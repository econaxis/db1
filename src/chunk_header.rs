
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{Cursor, Read, Seek, Write};
use dynamic_tuple::{TypeData};
use table_base2::TableType;



use crate::bytes_serializer::{BytesSerialize, FromReader};
use crate::range::Range;

const CH_CHECK_SEQUENCE: u64 = 0x32aa842f80ad9;

impl BytesSerialize for ChunkHeader {
    fn serialize_with_heap<W: Write, W1: Write + Seek>(&self, mut w: W, mut _heap: W1) {
        // w.write_all(&CH_CHECK_SEQUENCE.to_le_bytes()).unwrap();
        let mut rc = ReadContainer {
            check_sequence: CH_CHECK_SEQUENCE,
            ty: self.ty,
            tot_len: self.tot_len,
            type_size: self.type_size,
            tuple_count: self.tuple_count,
            heap_size: self.heap_size,
            compressed_size: self.compressed_size,
            table_type: self.table_type.to_u8(),
        };
        w.write_all(slice_from_type(&mut rc)).unwrap();

        let mut heap: Cursor<Vec<u8>> = Cursor::default();
        self.limits.serialize_with_heap(&mut w, &mut heap);
        w.write_all(&heap.stream_len().unwrap().to_le_bytes());
        w.write_all(heap.get_ref().as_slice());
    }
}

// Describes a chunk of tuples, such as min/max ranges (for binary searches), size of the tuple, and how many tuples
// Will be serialized along with the data itself for quicker searches.
#[derive(PartialEq, Clone, Debug)]
#[repr(C)]
pub struct ChunkHeader {
    pub ty: u64,
    pub tot_len: u32,
    pub type_size: u32,
    pub tuple_count: u32,
    pub heap_size: u32,
    pub limits: Range<TypeData>,
    pub compressed_size: u32,
    pub table_type: TableType,
}

#[derive(Default, Debug)]
#[repr(C)]
struct ReadContainer {
    check_sequence: u64,
    ty: u64,
    tot_len: u32,
    type_size: u32,
    tuple_count: u32,
    heap_size: u32,
    compressed_size: u32,
    table_type: u8,
}

pub fn slice_from_type<T: Sized>(t: &mut T) -> &mut [u8] {
    unsafe { std::slice::from_raw_parts_mut(t as *mut T as *mut u8, std::mem::size_of::<T>()) }
}

impl FromReader for Option<ChunkHeader> {
    fn from_reader_and_heap<R: Read>(mut r: R, heap: &[u8]) -> Self {
        assert_eq!(heap.len(), 0);

        let mut rc = ReadContainer::default();
        r.read_exact(slice_from_type(&mut rc)).ok()?;
        if rc.check_sequence != CH_CHECK_SEQUENCE {
            println!("Check sequence doesn't match {:?}", rc);
            return None;
        }
        let mut limits = Range::from_reader_and_heap(&mut r, &[]);

        let mut ch_heap_len = 0u64;
        r.read_exact(slice_from_type(&mut ch_heap_len)).unwrap();
        let mut ch_heap = Vec::default();
        ch_heap.resize(ch_heap_len as usize, 0);
        r.read_exact(&mut ch_heap);

        limits.resolve(&ch_heap);

        Some(ChunkHeader {
            ty: rc.ty,
            tot_len: rc.tot_len,
            type_size: rc.type_size,
            limits,
            tuple_count: rc.tuple_count,
            heap_size: rc.heap_size,
            compressed_size: rc.compressed_size,
            table_type: TableType::from_u8(rc.table_type),
        })
    }
}


impl ChunkHeader {
    pub const MAXTYPESIZE: u64 = 60;
    pub(crate) fn compressed(&self) -> bool {
        self.compressed_size > 0
    }
    pub fn calculate_total_size(&self) -> usize {
        if self.compressed() {
            (self.compressed_size + self.heap_size) as usize
        } else {
            (self.tot_len) as usize
        }
    }
    pub fn calculate_heap_offset(&self) -> usize {
        (self.calculate_total_size() - self.heap_size as usize) as usize
    }
}

// Represents a collection of ChunkHeaders, along with their location in a file for latter searches
#[derive(Debug, PartialEq, Clone)]
pub struct CHValue {
    pub ch: ChunkHeader,
    pub location: u64,
}

impl Default for CHValue {
    fn default() -> Self {
        Self {
            ch: ChunkHeader {
                ty: 0,
                tot_len: 0,
                type_size: 0,
                tuple_count: 0,
                heap_size: 0,
                limits: Default::default(),
                compressed_size: 0,
                table_type: TableType::Data,
            },
            location: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord)]
pub struct MinKey {
    ty: u16,
    pkey: TypeData,
}

impl PartialOrd for MinKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.ty.cmp(&other.ty).then(self.pkey.cmp(&other.pkey)))
    }
}

impl MinKey {
    pub fn start_ty(&self) -> MinKey {
        MinKey {
            ty: self.ty,
            pkey: TypeData::Null,
        }
    }
    pub fn new(ty: u64, pkey: TypeData) -> MinKey {
        MinKey {
            ty: ty as u16,
            pkey,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct ChunkHeaderIndex(pub BTreeMap<MinKey, CHValue>);

impl Default for ChunkHeaderIndex {
    fn default() -> Self {
        ChunkHeaderIndex(Default::default())
    }
}

impl ChunkHeaderIndex {
    pub fn remove(&mut self, ty: u64, pkey: TypeData) -> u64 {
        let mk = MinKey::new(ty, pkey);
        let k = self.0.range(&mk.start_ty()..=&mk).rev().next().unwrap();
        let location = k.1.location;
        let first = k.0.clone();
        let _result = self.0.remove(&first).unwrap();

        location
    }

    pub fn get_in_one_it<'a>(&'a self, ty: u64, pkey: Option<TypeData>) -> impl DoubleEndedIterator<Item=(&'a MinKey, &'a CHValue)> {
        if let Some(pkey) = pkey {
            let mk = MinKey::new(ty, pkey);
            
            self.0.range(mk.start_ty()..=mk)
        } else {
            let mk = MinKey::new(ty, TypeData::Null);
            let mk_next = MinKey::new(ty + 1, TypeData::Null);
            
            self.0.range(mk.start_ty()..mk_next)
        }
    }
    pub fn get_in_one_mut(&mut self, ty: u64, pkey: TypeData) -> impl DoubleEndedIterator<Item=(&MinKey, &mut CHValue)> {
        let mk = MinKey::new(ty, pkey);
        let left = self.0.range_mut(mk.start_ty()..=mk).rev();
        left
    }

    pub fn push(&mut self, pos: u64, chunk_header: ChunkHeader) {
        let min_value = chunk_header.limits.min.clone().unwrap();
        let mk = MinKey::new(chunk_header.ty, min_value);
        self.0.insert(
            mk,
            CHValue {
                ch: chunk_header,
                location: pos,
            },
        );
    }
    pub fn reset_limits(&mut self, ty: u64, old_min: TypeData, new_limit: Range<TypeData>) {
        let mk = MinKey::new(ty, old_min);
        let mut prev = self.0.remove(&mk).unwrap();
        prev.ch.limits = new_limit;
        self.push(prev.location, prev.ch);
    }
    pub fn update_limits(&mut self, ty: u64, loc: u64, pkey: TypeData) {
        let x = self.get_in_one_mut(ty, pkey.clone()).next().unwrap();
        assert_eq!(x.1.location, loc);
        let x0 = x.0.clone();

        // Since we're changing the lower bound, have to reindex in CH (as that btree is sorted by lower bound)
        if x.0.pkey > pkey {
            let mut new_limit = x.1.ch.limits.clone();
            new_limit.add(&pkey);
            let mut value = self.0.remove(&x0).unwrap();
            value.ch.limits = new_limit.clone();
            let mk = MinKey::new(ty, new_limit.min.unwrap());
            self.0.insert(mk, value);
        } else if !x.1.ch.limits.overlaps(&(&pkey..=&pkey)) {
            // Just update the value
            x.1.ch.limits.add(&pkey);
        }
    }
}

impl FromReader for ChunkHeader {
    fn from_reader_and_heap<R: Read>(r: R, heap: &[u8]) -> Self {
        Option::<Self>::from_reader_and_heap(r, heap).unwrap()
    }
}
