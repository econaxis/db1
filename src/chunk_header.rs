use std::fmt::{Debug, Formatter};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::ops::RangeBounds;

use serializer::PageSerializer;
use table_base::read_to_buf;

use crate::{ SuitableDataType};
use crate::bytes_serializer::{BytesSerialize, FromReader};
use crate::range::Range;

const CH_CHECK_SEQUENCE: u32 = 0x32aa8429;

impl BytesSerialize for ChunkHeader
{
    fn serialize_with_heap<W: Write, W1: Write + Seek>(&self, mut w: W, mut _heap: W1) {
        w.write_all(&CH_CHECK_SEQUENCE.to_le_bytes()).unwrap();
        w.write_all(&self.ty.to_le_bytes()).unwrap();
        w.write_all(&self.type_size.to_le_bytes()).unwrap();
        w.write_all(&self.length.to_le_bytes()).unwrap();
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
        let mut ty: u8 = 0;
        let mut type_size: u32 = 0;
        let mut length: u32 = 0;
        let mut heap_size: u64 = 0;
        let mut compressed_size: u64 = 0;
        r.read_exact(slice_from_type(&mut check_sequence)).ok()?;
        if check_sequence != CH_CHECK_SEQUENCE {
            return None;
        }
        r.read_exact(slice_from_type(&mut ty)).unwrap();
        r.read_exact(slice_from_type(&mut type_size)).unwrap();
        r.read_exact(slice_from_type(&mut length)).unwrap();
        r.read_exact(slice_from_type(&mut heap_size)).unwrap();
        r.read_exact(slice_from_type(&mut compressed_size)).unwrap();
        let limits = Range::from_reader_and_heap(r, heap);

        Some(ChunkHeader {
            ty,
            type_size,
            length,
            limits,
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
    pub ty: u8,
    pub type_size: u32,
    pub length: u32,
    pub heap_size: u64,
    pub limits: Range<u64>,
    pub compressed_size: u64,
}

impl ChunkHeader {
    pub(crate) fn compressed(&self) -> bool {
        self.compressed_size > 0
    }
    pub fn calculate_total_size(&self) -> usize {
        if self.compressed() {
            (self.compressed_size + self.heap_size) as usize
        } else {
            (self.type_size * self.length + self.heap_size as u32) as usize
        }
    }
    pub fn calculate_heap_offset(&self) -> usize {
        (self.calculate_total_size() - self.heap_size as usize) as usize
    }
}

// Represents a collection of ChunkHeaders, along with their location in a file for latter searches
#[derive(Debug, PartialEq)]
pub struct ChunkHeaderIndex(pub Vec<(u64, ChunkHeader)>);

impl Default for ChunkHeaderIndex {
    fn default() -> Self {
        ChunkHeaderIndex(Vec::default())
    }
}

pub struct ChunkHeaderIterator<R> {
    stream: R,
}

impl<R: Read + Seek> ChunkHeaderIterator<R> {
    fn next(&mut self) -> u64 {
        u64::from_le_bytes(read_to_buf(&mut self.stream))
    }
    fn skip(&mut self, skip: SeekFrom) {
        self.stream.seek(skip).unwrap();
    }
}


impl ChunkHeaderIndex {
    fn new() -> Self {
        Self(Vec::new())
    }
    // Iterate all the previously flushed chunk headers and look for all tuples contained in range `RB`
    pub fn get_in_all<RB: RangeBounds<u64>>(&self, ty: u8, range: &RB) -> Vec<u64>
        where
    {
        self.0
            .iter()
            .filter_map(|(pos, h)| (h.limits.overlaps(range) && h.ty == ty).then(|| *pos))
            .collect()
    }
    pub fn push(&mut self, pos: u64, chunk_header: ChunkHeader) {
        self.0.push((pos, chunk_header));
    }
}


impl FromReader for ChunkHeader {
    fn from_reader_and_heap<R: Read>(r: R, heap: &[u8]) -> Self {
        Option::<Self>::from_reader_and_heap(r, heap).unwrap()
    }
}
