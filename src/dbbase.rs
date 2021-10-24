use std::fmt::{Debug, Formatter};
use std::io::{Read, Write};
use std::ops::RangeBounds;

use crate::bytes_serializer::{BytesSerialize, FromReader};
use crate::chunk_header::ChunkHeader;
use crate::Range;
use crate::suitable_data_type::SuitableDataType;
use crate::main_db::assert_no_dups;

impl<T: Ord + Clone> Default for DbBase<T> {
    fn default() -> Self {
        Self { limits: Range::new(None), data: Vec::new(), is_sorted: true }
    }
}

impl<T: PartialEq> PartialEq for DbBase<T> {
    fn eq(&self, other: &Self) -> bool {
        self.data.eq(&other.data)
    }
}


// Raw database instance for storing data, getting min/max of data, and querying data.
pub struct DbBase<T> {
    pub(crate) data: Vec<T>,
    limits: Range<T>,
    is_sorted: bool,
}

impl<T: SuitableDataType> Debug for DbBase<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbBase")
            .field("data", &self.data)
            .field("limits", &self.limits)
            .finish()
    }
}

impl <T: SuitableDataType> FromReader for DbBase<T> {
    // Read bytes into a DbBase instance
    fn from_reader<R: Read>(r: &mut R) -> Self {
        let chunk_header = ChunkHeader::<T>::from_reader(r);

        let mut db = Self { limits: Range::new(None), data: Vec::with_capacity(chunk_header.length as usize), is_sorted: true };

        for _ in 0..chunk_header.length {
            let val = T::from_reader(r);
            db.store(val);
        }
        db.sort_self();

        db
    }
}

impl<T: SuitableDataType> DbBase<T> {
    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    // Sort by primary key
    pub fn sort_self(&mut self) {
        self.is_sorted =true;
        self.data.sort_by(|a, b| a.partial_cmp(b).unwrap())
    }

    // Store tuple into self
    pub(crate) fn store(&mut self, t: T) {
        self.limits.add(&t);
        self.data.push(t);
        self.is_sorted = false;
    }

    // Get the chunk header of current in-memory data
    pub(crate) fn get_chunk_header(&self) -> ChunkHeader<T> {
        ChunkHeader::<T> {
            type_size: std::mem::size_of::<T>() as u32,
            length: self.data.len() as u32,
            limits: self.limits.clone(),
        }
    }

    // Clear in-memory contents and flush to disk
    pub(crate) fn force_flush(&mut self, mut w: impl Write) -> Vec<T> {
        if self.data.is_empty() {
            return Vec::new();
        }
        let header = self.get_chunk_header();

        self.limits = Range::new(None);
        self.sort_self();
        debug_assert!(assert_no_dups(&self.data));

        let mut vec = std::mem::take(&mut self.data);
        header.serialize(&mut w);
        vec.iter().for_each(|a| T::serialize(a, &mut w));
        w.flush().unwrap();
        vec
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
