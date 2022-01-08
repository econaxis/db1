use std::fmt::{Debug, Formatter};
use std::io::{Cursor, Read, Seek, Write};
use std::ops::RangeBounds;

use crate::{BytesSerialize, ChunkHeader, FromReader, QueryableDataType, Range, SuitableDataType};
use crate::compressor;
use crate::heap_writer;
use crate::heap_writer::default_mem_writer;
use crate::table_manager::assert_no_dups;

impl<T: Ord + Clone> Default for TableBase<T> {
    fn default() -> Self {
        Self {
            heap: Vec::new(),
            limits: Range::new(None),
            data: Vec::new(),
            is_sorted: true,
        }
    }
}

impl<T: PartialEq> PartialEq for TableBase<T> {
    fn eq(&self, other: &Self) -> bool {
        self.data.eq(&other.data)
    }
}

// Raw database instance for storing data, getting min/max of data, and querying data.
// Only in-memory operations supported.
pub struct TableBase<T> {
    data: Vec<T>,
    limits: Range<T>,
    is_sorted: bool,
    heap: Vec<u8>,
}

impl<T: SuitableDataType> Debug for TableBase<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbBase")
            .field("data", &self.data)
            .field("limits", &self.limits)
            .finish()
    }
}

fn read_to_vec<R: Read>(mut r: R, len: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(len);
    unsafe {
        let uninit_slice = std::slice::from_raw_parts_mut(buf.as_mut_ptr(), len);
        r.read_exact(uninit_slice).unwrap();
        buf.set_len(len);
    }
    buf
}

impl<T: SuitableDataType> FromReader for TableBase<T> {
    fn from_reader_and_heap<R: Read>(mut r: R, _heap: &[u8]) -> Self {
        assert_eq!(_heap.len(), 0);
        let chunk_header = ChunkHeader::<T>::from_reader_and_heap(&mut r, _heap);


        let mut buf = read_to_vec(&mut r, chunk_header.calculate_total_size());
        let (real_data, real_heap) = {
            let (data_unchecked, heap_unchecked) = buf.split_at_mut(chunk_header.calculate_heap_offset());
            if chunk_header.compressed() {
                (compressor::decompress::<T>(data_unchecked), compressor::decompress_heap(heap_unchecked))
            } else {
                (data_unchecked.to_vec(), heap_unchecked.to_vec())
            }
        };
        let heap = heap_writer::check(&real_heap).to_vec();
        let mut data_cursor = Cursor::new(real_data);

        let mut db = Self {
            data: vec![],
            limits: Range::default(),
            is_sorted: true,
            heap,
        };
        for _ in 0..chunk_header.length {
            let val = T::from_reader_and_heap(&mut data_cursor, &db.heap);
            db.store(val);
        }

        db
    }
}

#[cfg(test)]
impl<T: SuitableDataType> TableBase<T> {
    pub fn get_data(&self) -> &Vec<T> {
        &self.data
    }
    pub fn store_many(&mut self, t: &[T]) {
        for elem in t {
            self.store(elem.clone());
        }
    }
    pub fn heap(&self) -> &[u8] {
        &self.heap
    }
}


const USE_COMPRESSION: bool = true;

impl<T: SuitableDataType> TableBase<T> {
    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    // Sort by primary key
    pub fn sort_self(&mut self) {
        self.is_sorted = true;
        self.data.sort_by(|a, b| a.partial_cmp(b).unwrap())
    }

    // Store tuple into self
    pub(crate) fn store(&mut self, t: T) {
        debug_assert!(self.data.iter().find(|x| x == &&t).is_none());
        self.limits.add(&t);
        self.data.push(t);
        self.is_sorted = false;
    }

    pub(crate) fn store_and_replace(&mut self, t: T) -> Option<T> {
        if let Some(found) = self.data.iter_mut().find(|x| x == &&t) {
            Some(std::mem::replace(found, t))
        } else {
            self.store(t);
            None
        }
    }

    // Get the chunk header of current in-memory data

    pub(crate) fn get_chunk_header(&self, heap_size: u64) -> ChunkHeader<T> {
        ChunkHeader::<T> {
            type_size: T::TYPE_SIZE as u32,
            length: self.data.len() as u32,
            heap_size: heap_size as u64,
            limits: self.limits.clone(),
            compressed_size: 0,
        }
    }

    // Clear in-memory contents and flush to disk
    // Flushes like this: header - data - heap
    // We have to serialize to data + heap first (in a separate buffer), so we can calculate the data length and heap offset.
    // Then, we put data length + heap offset into the header and serialize that.

    pub(crate) fn force_flush<W: Write>(mut self, mut w: W) -> (ChunkHeader<T>, Vec<T>) {
        self.sort_self();
        println!("Limits {:?}", self.limits);

        let mut heap = heap_writer::default_heap_writer();
        let mut data = default_mem_writer();
        self.data
            .iter()
            .for_each(|a| a.serialize_with_heap(&mut data, &mut heap));
        let mut header = self.get_chunk_header(heap.stream_position().unwrap());
        if USE_COMPRESSION {
            let compressed_buf = compressor::compress::<T>(data.get_ref());
            let compressed_heap = compressor::compress_heap(heap.get_ref());
            header.heap_size = compressed_heap.len() as u64;
            header.compressed_size = compressed_buf.len() as u64;
            header.serialize_with_heap(&mut w, default_mem_writer());
            w.write_all(&compressed_buf).unwrap();
            w.write_all(&compressed_heap).unwrap();
        } else {
            header.serialize_with_heap(&mut w, default_mem_writer());
            w.write_all(data.get_ref()).unwrap();
            w.write_all(heap.get_ref()).unwrap();
        }
        w.flush().unwrap();
        let vec = std::mem::take(&mut self.data);

        (header, vec)
    }
}

impl<T: QueryableDataType> TableBase<T> {
    // Get slice corresponding to a primary key range

    pub(crate) fn key_range<RB: RangeBounds<u64>>(&mut self, range: &RB) -> &[T] {
        use std::ops::Bound::*;
        if self.is_sorted {
            debug_assert!(self.data.is_sorted());
        } else {
            assert!(self.data.is_sorted());
        }
        let start_idx = self.data.partition_point(|a| match range.start_bound() {
            Included(x) => a < x,
            Excluded(x) => a <= x,
            Unbounded => false,
        });
        let end_idx = self.data.partition_point(|a| match range.end_bound() {
            Included(x) => a <= x,
            Excluded(x) => a < x,
            Unbounded => true,
        });
        assert!(start_idx <= end_idx);

        let slice = self.data.get_mut(start_idx..end_idx).unwrap();
        if T::REQUIRES_HEAP {
            for s in slice.iter_mut() {
                s.resolve(&self.heap);
            }
        }

        slice
    }
}

#[test]
fn key_range_test() {
    use crate::DataType;
    let mut db = TableBase::<DataType>::default();
    let vec = vec![
        DataType(0, 0, 0),
        DataType(1, 1, 1),
        DataType(2, 2, 2),
        DataType(3, 3, 3),
        DataType(4, 4, 4),
    ];
    db.store_many(&vec);
    for i in 0..4 {
        for j in i..4 {
            assert_eq!(db.key_range(&(i..j)), &vec[i as usize..j as usize]);
            assert_eq!(db.key_range(&(i..=j)), &vec[i as usize..=j as usize]);
        }
    }
}
