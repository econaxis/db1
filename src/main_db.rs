#![feature(write_all_vectored)]
#![feature(is_sorted)]
#![feature(cursor_remaining)]

use std::cmp::Ordering;
use std::collections::Bound;
use std::fmt::{Debug, Formatter};
use std::io::{Read, Write};
use std::ops::{FnOnce, RangeBounds};

use crate::chunk_header::ChunkHeader;
use crate::bytes_serializer::{BytesSerialize, FromReader};
use crate::suitable_data_type::SuitableDataType;


// todo: implement generations to facilitate editing

#[allow(unused)]
fn setup_logging() {
    env_logger::init();
}


#[derive(Clone)]
pub struct Range<T> {
    pub(crate) min: Option<T>,
    pub(crate) max: Option<T>,
}

impl<T: Ord + Clone> Range<T> {
    fn new(init: Option<T>) -> Self {
        Self { min: init.clone(), max: init }
    }
    // Returns comp(lhs, rhs) if both are defined value, else returns True
    fn check_else_true<A, F: FnOnce(A, A) -> bool>(lhs: Option<A>, rhs: Option<A>, comp: F) -> bool {
        match lhs.is_some() && rhs.is_some() {
            false => true,
            true => comp(lhs.unwrap(), rhs.unwrap())
        }
    }
    pub fn add(&mut self, new_elt: &T) {
        if Self::check_else_true(Some(new_elt), self.min.as_ref(), |new, min| new < min) {
            self.min = Some(new_elt.clone());
        }
        if Self::check_else_true(Some(new_elt), self.max.as_ref(), |new, max| new > max) {
            self.max = Some(new_elt.clone());
        }
    }
}


const CHECK_SEQUENCE: u8 = 98;

impl<T: SuitableDataType> BytesSerialize for Range<T> {
    fn serialize<W: Write>(&self, mut w: W) {
        w.write(&CHECK_SEQUENCE.to_le_bytes()).unwrap();
        self.min.as_ref().unwrap().serialize(&mut w);
        self.max.as_ref().unwrap().serialize(w);
    }
}

impl<T: SuitableDataType> FromReader for Range<T> {
    fn from_reader<R: Read>(r: &mut R) -> Self {
        let mut check = [0u8; 1];
        r.read_exact(&mut check).unwrap();
        assert_eq!(check[0], CHECK_SEQUENCE);
        let min = T::from_reader(r);
        let max = T::from_reader(r);
        Self { min: Some(min), max: Some(max) }
    }
}


impl<T: SuitableDataType> Range<T> {
    pub fn overlaps<RB: RangeBounds<u64>>(&self, rb: &RB) -> bool {
        match (&self.min, &self.max) {
            (Some(min), Some(max)) => {
                let min_in = match rb.start_bound() {
                    Bound::Included(start) => max >= start,
                    Bound::Excluded(start) => max > start,
                    Bound::Unbounded => true
                };
                let max_in = match rb.end_bound() {
                    Bound::Included(end) => min <= end,
                    Bound::Excluded(end) => min < end,
                    Bound::Unbounded => true
                };

                max_in && min_in
            }
            _ => panic!("Must not be None to compare")
        }
    }
}


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

const FLUSH_CUTOFF: usize = 5;

pub struct DbManager<T: SuitableDataType> {
    db: DbBase<T>,
    previous_headers: Vec<(usize, ChunkHeader<T>)>,
    output_stream: Vec<u8>,
    counter: u64,
}

impl<T: SuitableDataType> DbManager<T> {
    pub fn new(db: DbBase<T>) -> Self {
        Self { db, previous_headers: Vec::default(), output_stream: Vec::default(), counter: 0 }
    }
    pub fn store(&mut self, t: T) {
        self.db.store(t);

        self.counter += 1;
        if self.db.len() >= FLUSH_CUTOFF {
            let header = self.db.get_chunk_header();
            self.previous_headers.push((self.output_stream.len(), header));
            self.db.force_flush(&mut self.output_stream);

            assert_eq!(self.previous_headers.len() * FLUSH_CUTOFF, self.counter as usize);
        }
    }
    pub fn current_key_range<RB: RangeBounds<u64>>(&self, range: &RB) -> &[T] {
        self.db.key_range(range)
    }

    pub fn get_in_all<RB: RangeBounds<u64>>(&self, range: RB) -> Vec<T> {
        let ok_chunks = self.previous_headers.iter().filter_map(|(pos, h)|
            h.limits.overlaps(&range).then(|| pos));

        let mut vec = Vec::new();
        for pos in ok_chunks {
            let slice = &self.output_stream[*pos..];
            let db = DbBase::<T>::from_reader(slice);
            let range = db.key_range(&range);
            vec.extend_from_slice(range);
        };
        vec.extend_from_slice(self.current_key_range(&range));
        vec
    }
}


pub struct DbBase<T> {
    pub(crate) data: Vec<T>,
    limits: Range<T>,
    is_sorted: bool,
}

impl<T: SuitableDataType> Debug for Range<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Range")
            .field(&self.min)
            .field(&self.max)
            .finish()
    }
}

impl<T: SuitableDataType> Debug for DbBase<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbBase")
            .field("data", &self.data)
            .field("limits", &self.limits)
            .finish()
    }
}

impl<T: SuitableDataType> Debug for DbManager<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbManager")
            .field("current_data", &self.db)
            .field("prev_headers", &self.previous_headers)
            .finish()
    }
}

fn ord_range<Data: PartialOrd<Int>, Int>(b: Bound<Int>, data: &Data, default: Ordering) -> Ordering {
    match b {
        Bound::Included(x) | Bound::Excluded(x) => data.partial_cmp(&x).unwrap(),
        Bound::Unbounded => default
    }
}

impl<T: SuitableDataType> DbBase<T> {
    fn len(&self) -> usize {
        self.data.len()
    }
    fn sort_self(&mut self) {
        self.data.sort_by(|a, b| a.partial_cmp(b).unwrap())
    }
    pub(crate) fn from_reader(mut r: impl Read) -> Self {
        let chunk_header = ChunkHeader::<T>::from_reader(&mut r);

        let mut db = Self { limits: Range::new(None), data: Vec::with_capacity(chunk_header.length as usize), is_sorted: true };
        for _ in 0..chunk_header.length {
            let val = T::from_reader(&mut r);
            db.store(val);
        }
        db.sort_self();

        db
    }
    pub(crate) fn store(&mut self, t: T) {
        self.limits.add(&t);
        self.data.push(t);
        self.is_sorted = false;
    }

    fn get_chunk_header(&self) -> ChunkHeader<T> {
        ChunkHeader::<T> {
            type_size: std::mem::size_of::<T>() as u32,
            length: self.data.len() as u32,
            limits: self.limits.clone(),
        }
    }
    pub(crate) fn force_flush(&mut self, mut w: impl Write) -> Vec<T> {
        if self.data.is_empty() {
            return Vec::new();
        }
        let header = self.get_chunk_header();

        self.limits = Range::new(None);

        let mut vec = std::mem::take(&mut self.data);
        vec.sort_by(|a, b| a.partial_cmp(b).unwrap());
        header.serialize(&mut w);
        vec.iter().for_each(|a| T::serialize(a, &mut w));
        vec
    }

    pub(crate) fn key_lookup(&self, key: u64) -> Option<T> {
        assert!(self.data.is_sorted());
        let result = self.data.binary_search_by(|a| a.partial_cmp(&key).unwrap());

        result.map(|index| self.data[index].clone()).ok()
    }



    pub(crate) fn key_range<RB: RangeBounds<u64>>(&self, range: &RB) -> &[T] {
        use std::ops::Bound::*;

        assert!(self.data.is_sorted());
        let start_idx = self.data.partition_point(|a| match range.start_bound() {
            Included(x) => a < x,
            Excluded(x) => a <= x,
            Unbounded => false
        });
        let mut end_idx = self.data.partition_point(|a| match range.end_bound() {
            Included(x) => a <= x,
            Excluded(x) => a < x,
            Unbounded => true
        });
        self.data.get(start_idx..end_idx).unwrap()
    }
}


// fn main() {
//     println!("Hello, world!");
// }
