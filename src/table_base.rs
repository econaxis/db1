use std::fmt::{Debug, Formatter};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};

use std::ops::{RangeBounds};

use heap_writer::default_heap_writer;

use crate::compressor;
use crate::heap_writer;
use crate::heap_writer::default_mem_writer;
use crate::table_traits::BasicTable;
use crate::{BytesSerialize, ChunkHeader, FromReader, Range, SuitableDataType};

impl<T> Default for TableBase<T> {
    fn default() -> Self {
        Self {
            ty: 1,
            heap: default_heap_writer().into_inner(),
            limits: Range::new(None, None),
            data: Vec::new(),
            is_sorted: true,
        }
    }
}

impl<T> TableBase<T> {
    pub fn writable_heap(&mut self) -> Cursor<&mut Vec<u8>> {
        let mut ret = Cursor::new(&mut self.heap);
        ret.seek(SeekFrom::End(0)).unwrap();
        ret
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
    pub limits: Range<u64>,
    is_sorted: bool,
    heap: Vec<u8>,
    ty: u64,
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
    buf.resize(len, 0);
    r.read_exact(&mut buf).unwrap();
    buf
}

pub fn read_to_buf<R: Read, const N: usize>(mut r: R) -> [u8; N] {
    let mut buf = [0u8; N];
    r.read_exact(&mut buf).unwrap();
    buf
}

impl<T: SuitableDataType> FromReader for TableBase<T> {
    fn from_reader_and_heap<R: Read>(mut r: R, _heap: &[u8]) -> Self {
        assert_eq!(_heap.len(), 0);
        let chunk_header = ChunkHeader::from_reader_and_heap(&mut r, _heap);

        let mut buf = read_to_vec(&mut r, (chunk_header.tot_len + chunk_header.heap_size) as usize);
        let (real_data, real_heap) = {
            let (data_unchecked, heap_unchecked) =
                buf.split_at_mut(chunk_header.tot_len as usize);
            if chunk_header.compressed() {
                (
                    compressor::decompress::<T>(data_unchecked),
                    compressor::decompress_heap(heap_unchecked),
                )
            } else {
                (data_unchecked.to_vec(), heap_unchecked.to_vec())
            }
        };
        let heap = heap_writer::check(&real_heap).to_vec();
        let mut data_cursor = Cursor::new(real_data);

        let mut db = Self {
            ty: chunk_header.ty,
            data: vec![],
            limits: Range::default(),
            is_sorted: true,
            heap,
        };
        for _ in 0..chunk_header.tuple_count {
            let val = T::from_reader_and_heap(&mut data_cursor, &db.heap);
            db.store_and_replace(val);
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
            self.store_and_replace(elem.clone());
        }
    }
}

const USE_COMPRESSION: bool = false;

impl<T: SuitableDataType> TableBase<T> {
    fn get_chunk_header(&self, tuple_count: u32, data_size: u64, heap_size: u64) -> ChunkHeader {
        ChunkHeader {
            ty: self.ty,
            type_size: T::TYPE_SIZE as u32,
            tot_len: data_size as u32,
            heap_size: heap_size as u32,
            tuple_count,
            limits: self.limits.clone(),
            compressed_size: 0,
        }
    }

    // Get slice corresponding to a primary key range
    pub(crate) fn key_range_resolved<RB: RangeBounds<u64>>(&self, range: RB) -> Vec<T> {
        use std::ops::Bound::*;
        debug_assert!(self
            .data
            .is_sorted_by(|a, b| a.first().partial_cmp(&b.first())));
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

        self.data
            .get(start_idx..end_idx)
            .unwrap()
            .iter()
            .map(|a| {
                let mut a = a.clone();
                a.resolve_item(&self.heap, u8::MAX);
                a
            })
            .collect()
    }
}

pub struct TableBaseRangeIterator<'a, T> {
    range: (usize, usize),
    data_index: usize,
    pub heap: &'a [u8],
    vec: &'a Vec<T>,
}

impl<'a, T> Iterator for TableBaseRangeIterator<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.data_index < self.range.1 {
            self.data_index += 1;
            Some(&self.vec[self.data_index - 1])
        } else {
            None
        }
    }
}

impl<T: SuitableDataType> BasicTable<T> for TableBase<T> {
    fn heap(&self) -> &[u8] {
        &self.heap
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    // Sort by primary key
    fn sort_self(&mut self) {
        self.is_sorted = true;
        self.data.sort_by_key(|a| a.first())
    }

    fn store_and_replace(&mut self, t: T) -> Option<T> {
        if let Some(found) = self.data.iter_mut().find(|x| x.first() == t.first()) {
            Some(std::mem::replace(found, t))
        } else {
            debug_assert!(self.data.iter().find(|x| x.first() == t.first()).is_none());
            self.limits.add(t.first());
            self.data.push(t);
            self.is_sorted = false;
            None
        }
    }

    // Clear in-memory contents and flush to disk
    // Flushes like this: header - data - heap
    // We have to serialize to data + heap first (in a separate buffer), so we can calculate the data length and heap offset.
    // Then, we put data length + heap offset into the header and serialize that.
    fn force_flush<W: Write>(&mut self, mut w: W) -> (ChunkHeader, Vec<T>) {
        // Get the chunk header of current in-memory data
        self.sort_self();

        let heap = std::mem::replace(&mut self.heap, default_heap_writer().into_inner());
        let mut heap = Cursor::new(heap);
        heap.seek(SeekFrom::End(0)).unwrap();
        let mut data = default_mem_writer();
        self.data
            .iter()
            .for_each(|a| a.serialize_with_heap(&mut data, &mut heap));
        let mut header = self.get_chunk_header(
            self.data.len() as u32,
            data.stream_position().unwrap(),
            heap.stream_position().unwrap(),
        );

        if USE_COMPRESSION {
            let compressed_buf = compressor::compress::<T>(data.get_ref());
            let compressed_heap = compressor::compress_heap(heap.get_ref());
            header.heap_size = compressed_heap.len() as u32;
            header.compressed_size = compressed_buf.len() as u32;
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

    // Get slice corresponding to a primary key range
    fn key_range(&self, range: Option<u64>) -> Vec<&T> {
        
        if range.is_none() {
            return self.data.iter().collect();
        }
        let range = range.unwrap();
        if !self.limits.overlaps(&(range..=range)) {
            return Vec::new();
        }
        debug_assert!(self
            .data
            .is_sorted_by(|a, b| a.first().partial_cmp(&b.first())));
        let start_idx = self.data.partition_point(|a| a < &range);
        let end_idx = self.data.partition_point(|a| a <= &range);
        assert!(start_idx <= end_idx);

        self.data.get(start_idx..end_idx).unwrap().iter().collect()
    }

    fn key_range_iterator<RB: RangeBounds<u64>>(&self, range: RB) -> TableBaseRangeIterator<'_, T> {
        use std::ops::Bound::*;
        if !self.limits.overlaps(&range) {
            return TableBaseRangeIterator {
                heap: &self.heap,
                range: (0, 0),
                data_index: 0,
                vec: &self.data,
            };
        }
        debug_assert!(self
            .data
            .is_sorted_by(|a, b| a.first().partial_cmp(&b.first())));
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

        TableBaseRangeIterator {
            heap: &self.heap,
            range: (start_idx, end_idx),
            data_index: start_idx,
            vec: &self.data,
        }
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
            assert_eq!(db.key_range_resolved(i..j), &vec[i as usize..j as usize]);
            assert_eq!(db.key_range_resolved(i..=j), &vec[i as usize..=j as usize]);
        }
    }
}
