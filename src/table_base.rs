use std::fmt::{Debug, Formatter};
use std::io::{Read, Write, Cursor, Seek, SeekFrom};
use std::ops::RangeBounds;

use crate::bytes_serializer::{BytesSerialize, FromReader};
use crate::chunk_header::ChunkHeader;
use crate::Range;
use crate::suitable_data_type::{SuitableDataType};
use crate::table_manager::assert_no_dups;

impl<T: Ord + Clone> Default for TableBase<T> {
    fn default() -> Self {
        Self { limits: Range::new(None), data: Vec::new(), is_sorted: true, heap: Default::default() }
    }
}

impl<T: PartialEq> PartialEq for TableBase<T> {
    fn eq(&self, other: &Self) -> bool {
        self.data.eq(&other.data)
    }
}


// Raw database instance for storing data, getting min/max of data, and querying data.
pub struct TableBase<T> {
     data: Vec<T>,
    limits: Range<T>,
    heap: Vec<u8>,
    is_sorted: bool,
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
        let mut uninit_slice = std::slice::from_raw_parts_mut(buf.as_mut_ptr(), len);
        r.read_exact(uninit_slice).unwrap();
        buf.set_len(len);
    }
    buf
}

impl<T: SuitableDataType> FromReader for TableBase<T> {


    // Read bytes into a DbBase instance
    fn from_reader_and_heap<R: Read>(mut r: R, heap: &[u8]) -> Self {
        assert_eq!(heap, &[]);
        //todo: add heap offset to chunk header, implement rest of functions, test heap requiring struct
        let chunk_header = ChunkHeader::<T>::from_reader_and_heap(&mut r, heap);

        let mut buf = read_to_vec(&mut r, chunk_header.calculate_total_size());
        let (data, heap) = buf.split_at_mut(chunk_header.calculate_heap_offset());
        let mut data_cursor = Cursor::new(data);

        // Sanity checks
        if !T::REQUIRES_HEAP {
            assert!(heap.is_empty());
        }

        let mut db = Self { is_sorted: true, ..Default::default() };

        for _ in 0..chunk_header.length {
            let val = T::from_reader_and_heap(&mut data_cursor, heap);
            db.store(val);
        }
        db.sort_self();

        db
    }
}

struct EmptyWriter;

impl Write for EmptyWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        todo!()
    }

    fn flush(&mut self) -> std::io::Result<()> {
        todo!()
    }
}
impl Seek for EmptyWriter {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        todo!()
    }
}

impl<T: SuitableDataType> TableBase<T> {
    #[cfg(test)]
    pub fn get_data(&self) -> &Vec<T> {
        &self.data
    }
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
        self.limits.add(&t);
        self.data.push(t);
        self.is_sorted = false;
    }

    pub(crate) fn store_and_replace(&mut self, t: T) -> Option<T> {
        if let Some(found) = self.data.iter_mut().find(|x| **x == t) {
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
        }
    }

    // Clear in-memory contents and flush to disk
    pub(crate) fn force_flush(&mut self, mut w: impl Write) -> (ChunkHeader<T>, Vec<T>) {
        assert!(!self.data.is_empty());
        self.sort_self();
        debug_assert!(assert_no_dups(&self.data));
        let mut heap = Cursor::new(Vec::new());
        let mut data = Cursor::new(Vec::new());
        self.data.iter().for_each(|a| a.serialize_with_heap(&mut data, &mut heap));

        let header = self.get_chunk_header(heap.stream_position().unwrap());

        header.serialize_with_heap(&mut w, Cursor::new(Vec::new()));
        dbg!(&header, &data, &heap);
        self.limits = Range::new(None);


        w.write_all(&data.into_inner()).unwrap();
        w.write_all(&heap.into_inner()).unwrap();
        w.flush().unwrap();
        let vec = std::mem::take(&mut self.data);
        (header, vec)
    }


    // Get slice corresponding to a primary key range
    pub(crate) fn key_range<RB: RangeBounds<u64>>(&self, range: &RB) -> &[T] {
        use std::ops::Bound::*;
        if self.is_sorted {
            debug_assert!(self.data.is_sorted());
        } else {
            assert!(self.data.is_sorted());
        }
        let start_idx = self.data.partition_point(|a| match range.start_bound() {
            Included(x) => a < x,
            Excluded(x) => a <= x,
            Unbounded => false
        });
        let end_idx = self.data.partition_point(|a| match range.end_bound() {
            Included(x) => a <= x,
            Excluded(x) => a < x,
            Unbounded => true
        });
        assert!(start_idx <= end_idx);
        self.data.get(start_idx..end_idx).unwrap()
    }
}
