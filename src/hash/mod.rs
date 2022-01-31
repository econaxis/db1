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

impl HashDb {
    pub fn serialize<W: Write>(&self, mut data: W) -> ChunkHeader {
        let chunk_header = self.get_chunk_header();
        chunk_header.serialize_with_heap(&mut data, InvalidWriter);

        for j in self.hash.values() {
            j.serialize_with_heap(&mut data, InvalidWriter);
        }
        chunk_header
    }
}

impl FromReader for HashDb {
    fn from_reader_and_heap<R: Read>(mut r: R, heap: &[u8]) -> Self {
        assert_eq!(heap, &[]);
        let chunk_header = ChunkHeader::from_reader_and_heap(&mut r, heap);
        log::debug!("Hash db chunk header {:?}", chunk_header);
        let mut ret = HashDb::default();
        for _ in 0..chunk_header.tuple_count {
            let t = IndexKey::from_reader_and_heap(&mut r, heap);
            ret.store_by_hash(t);
        }
        ret
    }
}

impl HashDb {
    fn get_chunk_header(&self) -> ChunkHeader {
        ChunkHeader {
            ty: 2,
            type_size: 0,
            tuple_count: self.hash.len() as u32,
            tot_len: (self.hash.len() * std::mem::size_of::<IndexKey>()) as u32,
            heap_size: 0,
            limits: Range {
                min: Some(0),
                max: Some(0)
            },
            compressed_size: 0,
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

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use hash::HashDb;
    use FromReader;

    #[test]
    fn test_hash_collision() {
        let mut db = HashDb::default();

        db.store("hello world", 1);
        db.store("abcdef", 2);
        db.store("hello world", 3);

        assert_eq!(db.get("hello world"), [1, 3]);
        assert_eq!(db.get("abcdef"), [2]);
    }

    #[test]
    fn test_serialize() {
        let mut db = HashDb::default();
        db.store("hello world", 1);
        db.store("hfdsafdello world", 10);
        db.store("hello worl232d", 100);
        db.store("hello wdsavcxorld", 1000);

        let mut f: Cursor<Vec<u8>> = Cursor::default();
        db.serialize(&mut f);

        f.set_position(0);
        let db1 = HashDb::from_reader_and_heap(f, &[]);
        assert_eq!(db, db1);
    }
}
