#![feature(write_all_vectored)]
#![feature(is_sorted)]
#![feature(cursor_remaining)]

use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::io::{Cursor, IoSlice, Read, Seek, SeekFrom, Write};

use rand::Rng;

use chunk_header::ChunkHeader;
use suitable_data_type::SuitableDataType;

use crate::bytes_serializer::{BytesSerialize, FromReader};

mod bytes_serializer;
mod chunk_header;
mod suitable_data_type;

// todo: implement generations to facilitate editing



const CHECK_BYTES: u64 = 0x8e3ea4b6d509c660;

#[derive(Debug)]
struct DB<T> {
    data: Vec<T>,
    is_sorted: bool
}


impl<T> Default for DB<T> {
    fn default() -> Self {
        Self { data: Vec::new(), is_sorted: true }
    }
}

impl<T: PartialEq> PartialEq for DB<T> {
    fn eq(&self, other: &Self) -> bool {
        self.data.eq(&other.data)
    }
}

impl<T: SuitableDataType> DB<T> {
    fn sort_self(&mut self) {
        self.data.sort_by(|a, b| a.partial_cmp(b).unwrap())
    }
    fn from_reader(mut r: impl Read) -> Self {
        let chunk_header = ChunkHeader::<T>::from_reader(&mut r);

        let mut db = Self { data: Vec::with_capacity(chunk_header.length as usize), is_sorted: true };

        for _ in 0..chunk_header.length {
            db.data.push(T::from_reader(&mut r))
        }
        db.sort_self();

        return db;
    }
    fn store(&mut self, t: T) {
        self.data.push(t);
        self.is_sorted = false;
    }

    fn force_flush(&mut self, w: &mut impl Write) -> Vec<T> {
        let mut vec = std::mem::replace(&mut self.data, Vec::new());
        vec.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let header = ChunkHeader::<T> {
            type_size: std::mem::size_of::<T>() as u32,
            length: vec.len() as u32,
            start: vec.first().unwrap().clone(),
            end: vec.last().unwrap().clone()
        };
        header.serialize(w);
        vec.iter().for_each(|a| T::serialize(a, w));
        vec
    }

    fn key_lookup(&self, key: u64) -> Option<T> {
        assert!(self.data.is_sorted());
        let result = self.data.binary_search_by(|a| a.partial_cmp(&key).unwrap());

        result.map(|index| self.data[index].clone()).ok()
    }

    fn key_range(&self, range: (u64, u64)) -> &[T] {
        assert!(self.data.is_sorted());
        let result_extractor = |a: Result<usize, usize>| -> usize {
            match a {
                Ok(x) => x,
                Err(x) => x
            }
        };

        let start_idx = self.data.binary_search_by(|a| a.partial_cmp(&range.0).unwrap());
        let end_idx = self.data.binary_search_by(|a| a.partial_cmp(&range.1).unwrap());

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
    let mut db = DB::<DataType>::default();

    let mut rng = thread_rng();
    for i in 0..10 {
        db.store(DataType(i * 4, rng.gen(), rng.gen()));
    }

    dbg!(db.key_lookup(8));
    dbg!(db.key_range((2, 30)));
}
#[cfg(test)]
#[test]
fn test1() {
    use rand::thread_rng;
    use suitable_data_type::DataType;
    let mut db = DB::<DataType>::default();

    let mut rng = thread_rng();
    for _ in 0..10 {
        db.store(DataType(1, 2, 3));
    }
    let mut buffer: Vec<u8> = Vec::new();
    let old_data = db.force_flush(&mut buffer);

    println!("Hex: {:?}", buffer);

    let reader = buffer.as_slice();
    let db1 = DB::<DataType>::from_reader(reader);
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
        let mut db = DB::<DataType>::default();

        let mut rng = thread_rng();
        for _ in 0..10 {
            db.store(DataType(rng.gen(), rng.gen(), rng.gen()));
        }
        let old_data = db.force_flush(&mut buffer);
        dbs.push(old_data);
    }

    let mut reader = Cursor::new(&buffer);


    for d in dbs {
        let db1 = DB::<DataType>::from_reader(&mut reader);
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
        let mut db = DB::<DataType>::default();

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
