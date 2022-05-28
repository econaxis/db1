use std::collections::hash_map::{DefaultHasher, Entry};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io::{Read, Seek, SeekFrom, Write};
use Range;

use crate::{
    from_reader, gen_suitable_data_type_impls, BytesSerialize, ChunkHeader, FromReader,
    SuitableDataType,
};

#[derive(Clone, Debug, PartialEq, Hash)]
pub struct IndexKey {
    pub(crate) hash: u64,
    pub(crate) pointer: u64,
}

gen_suitable_data_type_impls!(IndexKey);
impl SuitableDataType for IndexKey {}

impl BytesSerialize for IndexKey {}

from_reader!(IndexKey);
#[derive(Debug, PartialEq)]
pub struct HashDb {
    hash: HashMap<u64, IndexKey>,
}

impl Default for HashDb {
    fn default() -> Self {
        Self {
            hash: HashMap::default(),
        }
    }
}

pub struct InvalidWriter;

impl Read for InvalidWriter {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        panic!()
    }
}

impl Write for InvalidWriter {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        panic!()
    }

    fn flush(&mut self) -> std::io::Result<()> {
        panic!()
    }
}

impl Seek for InvalidWriter {
    fn seek(&mut self, _pos: SeekFrom) -> std::io::Result<u64> {
        panic!()
    }
}

pub(crate) fn hash<T: Hash>(t: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    t.hash(&mut hasher);
    hasher.finish()
}

impl HashDb {
    pub fn store<T: Hash>(&mut self, value: T, location: u64) {
        let ikey = IndexKey {
            hash: hash(&value),
            pointer: location,
        };
        self.store_by_hash(ikey);
    }

    pub fn get<T: Hash>(&mut self, look_for: T) -> Vec<u64> {
        let hash = hash(&look_for);
        self.get_by_hash(hash)
    }

    fn store_by_hash(&mut self, t: IndexKey) {
        let mut value = t.hash;

        loop {
            if let Entry::Vacant(ent) = self.hash.entry(value) {
                ent.insert(t);
                break;
            }
            value += 1;
        }
    }

    fn get_by_hash(&mut self, hash: u64) -> Vec<u64> {
        let mut result_buffer = Vec::new();
        let mut check_hash = hash;
        while let Some(x) = self.hash.get(&check_hash) {
            if x.hash == hash {
                result_buffer.push(x.pointer);
            }
            check_hash += 1;
        }
        result_buffer
    }
}

