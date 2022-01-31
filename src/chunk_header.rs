use std::collections::BTreeMap;
use std::fmt::{Debug};
use std::io::{Read, Seek, Write};


use crate::bytes_serializer::{BytesSerialize, FromReader};
use crate::range::Range;

const CH_CHECK_SEQUENCE: u32 = 0x32aa8429;

impl BytesSerialize for ChunkHeader {
    fn serialize_with_heap<W: Write, W1: Write + Seek>(&self, mut w: W, mut _heap: W1) {
        w.write_all(&CH_CHECK_SEQUENCE.to_le_bytes()).unwrap();
        w.write_all(&self.ty.to_le_bytes()).unwrap();
        w.write_all(&self.tot_len.to_le_bytes()).unwrap();
        w.write_all(&self.type_size.to_le_bytes()).unwrap();
        w.write_all(&self.tuple_count.to_le_bytes()).unwrap();
        w.write_all(&self.heap_size.to_le_bytes()).unwrap();
        w.write_all(&self.compressed_size.to_le_bytes()).unwrap();
        self.limits.serialize_with_heap(w, _heap);
    }
}

pub fn slice_from_type<T: Sized>(t: &mut T) -> &mut [u8] {
    unsafe { std::slice::from_raw_parts_mut(t as *mut T as *mut u8, std::mem::size_of::<T>()) }
}

impl FromReader for Option<ChunkHeader> {
    fn from_reader_and_heap<R: Read>(mut r: R, heap: &[u8]) -> Self {
        assert_eq!(heap.len(), 0);

        let mut check_sequence: u32 = 0;
        let mut ty: u64 = 0;
        let mut type_size: u32 = 0;
        let mut tot_len: u32 = 0;
        let mut tuple_count: u32 = 0;
        let mut heap_size: u32 = 0;
        let mut compressed_size: u32 = 0;
        r.read_exact(slice_from_type(&mut check_sequence)).ok()?;
        if check_sequence != CH_CHECK_SEQUENCE {
            println!("Check sequence doesn't match");
            return None;
        }
        r.read_exact(slice_from_type(&mut ty)).unwrap();
        r.read_exact(slice_from_type(&mut tot_len)).unwrap();
        r.read_exact(slice_from_type(&mut type_size)).unwrap();
        r.read_exact(slice_from_type(&mut tuple_count)).unwrap();
        r.read_exact(slice_from_type(&mut heap_size)).unwrap();
        r.read_exact(slice_from_type(&mut compressed_size)).unwrap();
        let limits = Range::from_reader_and_heap(r, heap);

        Some(ChunkHeader {
            ty,
            tot_len,
            type_size,
            limits,
            tuple_count,
            heap_size,
            compressed_size,
        })
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
    pub limits: Range<u64>,
    pub compressed_size: u32,
}

impl ChunkHeader {
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
            },
            location: 0,
        }
    }
}


#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MinKey {
    ty: u16,
    pkey: u64,
}

impl MinKey {
    pub fn start_ty(&self) -> MinKey {
        MinKey {
            ty: self.ty,
            pkey: 0,
        }
    }
    pub fn new(ty: u64, pkey: u64) -> MinKey {
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
    pub fn remove(&mut self, ty: u64, pkey: u64) -> u64 {

        let mk = MinKey::new(ty, pkey);
        let k = self.0.range(mk.start_ty()..=mk).rev().next().unwrap();
        let location = k.1.location;
        let first = *k.0;
        let _result = self.0.remove(&first).unwrap();

        location
    }

    pub fn get_in_one(&self, ty: u64, pkey: u64) -> Option<(&'_ MinKey, &'_ CHValue)> {
        
        let mk = MinKey::new(ty, pkey);
        let mut left = self.0.range(mk.start_ty()..=mk).rev();

        
        left.next()
    }

    pub fn push(&mut self, pos: u64, chunk_header: ChunkHeader) {
        // Check
        debug_assert!({
            let mut prev_limits = Vec::new();
            for i in self.0.iter().filter(|a| a.1.ch.ty == chunk_header.ty) {
                assert!(prev_limits.iter().all(|a: &Range<u64>| !a.overlaps(&i.1.ch.limits)));
                prev_limits.push(i.1.ch.limits.clone());
            }
            true
        });

        let min_value = chunk_header.limits.min.unwrap();
        let mk = MinKey::new(chunk_header.ty, min_value);
        self.0.insert(mk, CHValue {
            ch: chunk_header,
            location: pos,
        });
    }
    pub fn reset_limits(&mut self, ty: u64, old_min: u64, new_limit: Range<u64>) {
        let mk = MinKey::new(ty, old_min);
        assert_eq!(self.0.range(mk..=mk).filter(|a| a.1.ch.ty == ty).count(), 1);
        let mut prev = self.0.remove(&mk).unwrap();
        prev.ch.limits = new_limit;
        self.push(prev.location, prev.ch);
    }
    pub fn update_limits(&mut self, ty: u64, loc: u64, pkey: u64) {
        let x = self.get_in_one(ty, pkey).unwrap();
        assert_eq!(x.1.location, loc);
        let x0 = *x.0;
        if !x.1.ch.limits.overlaps(&(pkey..=pkey)) {
            let mut new_limit = x.1.ch.limits.clone();
            new_limit.add(pkey);
            let mut value = self.0.remove(&x0).unwrap();
            value.ch.limits = new_limit.clone();
            let mk = MinKey::new(ty, new_limit.min.unwrap());
            self.0.insert(mk, value);
        }
    }
}

impl FromReader for ChunkHeader {
    fn from_reader_and_heap<R: Read>(r: R, heap: &[u8]) -> Self {
        Option::<Self>::from_reader_and_heap(r, heap).unwrap()
    }
}
