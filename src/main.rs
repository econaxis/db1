#![feature(write_all_vectored)]
#![feature(is_sorted)]
#![feature(cursor_remaining)]

use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::io::{Cursor, IoSlice, Read, Seek, SeekFrom, Write};

use rand::Rng;
use std::ops::Range as stdRange;

use chunk_header::ChunkHeader;
use suitable_data_type::SuitableDataType;
use log;
use env_logger;
use std::ops::{FnOnce, RangeBounds};

use crate::bytes_serializer::{BytesSerialize, FromReader};
use std::slice::SliceIndex;
use std::collections::Bound;
use crate::suitable_data_type::DataType;

mod bytes_serializer;
mod chunk_header;
mod suitable_data_type;

// todo: implement generations to facilitate editing

fn setup_logging() {
    env_logger::init();
}


const CHECK_BYTES: u64 = 0x8e3ea4b6d509c660;

#[derive(Clone)]
pub struct Range<T> {
    min: Option<T>,
    max: Option<T>,
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
        w.write(&CHECK_SEQUENCE.to_le_bytes());
        self.min.as_ref().unwrap().serialize(&mut w);
        self.max.as_ref().unwrap().serialize(w);
    }
}

impl<T: SuitableDataType> FromReader for Range<T> {
    fn from_reader<R: Read>(r: &mut R) -> Self {
        let mut check = [0u8; 1];
        r.read_exact(&mut check);
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

#[test]
fn test_range() {
    let test_range = Range { min: Some(DataType(3, 3, 3)), max: Some(DataType(10, 10, 10)) };
    assert!(!test_range.overlaps(&(15..20)));
    assert!(test_range.overlaps(&(7..20)));
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

struct DbManager<T: SuitableDataType> {
    db: DbBase<T>,
    previous_headers: Vec<(usize, ChunkHeader<T>)>,
    output_stream: Vec<u8>,
    counter: u64,
}

impl<T: SuitableDataType> DbManager<T> {
    fn new(db: DbBase<T>) -> Self {
        Self { db, previous_headers: Vec::default(), output_stream: Vec::default(), counter: 0 }
    }
    fn store(&mut self, t: T) {
        self.db.store(t);

        self.counter += 1;
        if self.db.len() >= FLUSH_CUTOFF {
            let header = self.db.get_chunk_header();
            self.previous_headers.push((self.output_stream.len(), header));
            self.db.force_flush(&mut self.output_stream);

            assert_eq!(self.previous_headers.len() * FLUSH_CUTOFF, self.counter as usize);
        }
    }
    fn get_in_current<I>(&self, range: I) -> I::Output where I: SliceIndex<[T]>, <I as SliceIndex<[T]>>::Output: Sized + Clone {
        self.db.data.get(range).unwrap().clone()
    }

    fn get_in_all<RB: RangeBounds<u64>>(&self, range: RB) -> Vec<T> {
        let ok_chunks = self.previous_headers.iter().filter_map(|(pos, h)|
            h.limits.overlaps(&range).then(|| pos));

        let mut vec = Vec::new();
        for pos in ok_chunks {
            let slice = &self.output_stream[*pos..];
            let db = DbBase::<T>::from_reader(slice);
            let range = db.key_range(&range);
            vec.extend_from_slice(range);
        };

        vec
    }
}

#[test]
fn test_dbmanager() {
    let mut dbm: DbManager<DataType> = DbManager::new(DbBase::default());
    let range: stdRange<u64> = 200..250;
    let mut expecting = Vec::new();
    for i in 0..255 {
        let rand = i as u64;
        let rand = (rand * rand * rand + 103238) % 255;
        let rand = rand as u8;
        dbm.store(DataType(rand, i, i));

        if range.contains(&(rand as u64)) {
            expecting.push(DataType(rand, i, i));
        }
    }

    let mut res = dbm.get_in_all(range.clone());
    res.sort();
    expecting.sort();
    assert_eq!(res, expecting);
}

struct DbBase<T> {
    data: Vec<T>,
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
    fn from_reader(mut r: impl Read) -> Self {
        let chunk_header = ChunkHeader::<T>::from_reader(&mut r);

        let mut db = Self { limits: Range::new(None), data: Vec::with_capacity(chunk_header.length as usize), is_sorted: true };
        for _ in 0..chunk_header.length {
            let val = T::from_reader(&mut r);
            db.store(val);
        }
        db.sort_self();

        return db;
    }
    fn store(&mut self, t: T) {
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
    fn force_flush(&mut self, mut w: impl Write) -> Vec<T> {
        if self.data.is_empty() {
            return Vec::new();
        }
        let header = self.get_chunk_header();

        self.limits = Range::new(None);

        let mut vec = std::mem::replace(&mut self.data, Vec::new());
        vec.sort_by(|a, b| a.partial_cmp(b).unwrap());
        header.serialize(&mut w);
        vec.iter().for_each(|a| T::serialize(a, &mut w));
        vec
    }

    fn key_lookup(&self, key: u64) -> Option<T> {
        assert!(self.data.is_sorted());
        let result = self.data.binary_search_by(|a| a.partial_cmp(&key).unwrap());

        result.map(|index| self.data[index].clone()).ok()
    }

    fn key_range<RB: RangeBounds<u64>>(&self, range: &RB) -> &[T] {
        assert!(self.data.is_sorted());
        let result_extractor = |a: Result<usize, usize>| -> usize {
            match a {
                Ok(x) => x,
                Err(x) => x
            }
        };


        let start_idx = self.data.binary_search_by(|a|
            ord_range(range.start_bound().cloned(), a, Ordering::Greater));
        let end_idx = self.data.binary_search_by(|a|
            ord_range(range.end_bound().cloned(), a, Ordering::Less)
        );

        let start_idx = result_extractor(start_idx);
        let end_idx = result_extractor(end_idx);

        self.data.get(start_idx..end_idx).unwrap()
    }
}

#[cfg(test)]
#[test]
fn test_key_lookup() {
    use rand::thread_rng;
    use suitable_data_type::DataType;
    let mut db = DbBase::<DataType>::default();

    let mut rng = thread_rng();
    for i in 0..10 {
        db.store(DataType(i * 4, rng.gen(), rng.gen()));
    }

    dbg!(db.key_lookup(8));
    dbg!(db.key_range(&(2..30)));
}

#[cfg(test)]
#[test]
fn test1() {
    use rand::thread_rng;
    use suitable_data_type::DataType;
    let mut db = DbBase::<DataType>::default();

    let mut rng = thread_rng();
    for i in 10u8..40u8 {
        let mult: u8 = rand::random();
        db.store(DataType(mult, i, i));
    }
    let mut buffer: Vec<u8> = Vec::new();
    let old_data = db.force_flush(&mut buffer);

    println!("Hex: {:?}", buffer);

    let reader = buffer.as_slice();
    let db1 = DbBase::<DataType>::from_reader(reader);
    assert_eq!(old_data, db1.data);
    dbg!(db1);
}

#[cfg(test)]
#[test]
fn test2() {
    use rand::thread_rng;
    use suitable_data_type::DataType;

    let mut buffer: Vec<u8> = Vec::new();
    let mut dbs = Vec::new();
    for _ in 0..150 {
        let mut db = DbBase::<DataType>::default();

        let mut rng = thread_rng();
        for _ in 0..10 {
            db.store(DataType(rng.gen(), rng.gen(), rng.gen()));
        }
        let old_data = db.force_flush(&mut buffer);
        dbs.push(old_data);
    }

    let mut reader = Cursor::new(&buffer);


    for d in dbs {
        let db1 = DbBase::<DataType>::from_reader(&mut reader);
        assert_eq!(d, db1.data);
    }
}

#[cfg(test)]
#[test]
fn test3() {
    use rand::thread_rng;
    use chunk_header::ChunkHeaderIndex;
    use suitable_data_type::DataType;

    let mut buffer: Vec<u8> = Vec::new();
    let mut dbs = Vec::new();
    for i in 0..150 {
        let mut db = DbBase::<DataType>::default();

        let mut rng = thread_rng();
        for _ in 0..10 {
            db.store(DataType(i, rng.gen(), rng.gen()));
        }
        let old_data = db.force_flush(&mut buffer);
        dbs.push(old_data);
    }

    let mut reader = Cursor::new(&buffer);

    let res = ChunkHeaderIndex::<DataType>::from_reader(&mut reader);

    assert_eq!(res.0.len(), dbs.len());
}


fn main() {
    println!("Hello, world!");
}
