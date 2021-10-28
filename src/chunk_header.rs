use std::fmt::{Debug, Formatter};
use std::io::{Read, Seek, Write};

use crate::bytes_serializer::{BytesSerialize, FromReader};
use crate::range::Range;
use crate::suitable_data_type::SuitableDataType;

const CH_CHECK_SEQUENCE: u32 = 0x32aa8429;

impl<T: SuitableDataType> BytesSerialize for ChunkHeader<T> where T: BytesSerialize {
    fn serialize_with_heap<W: Write, W1: Write + Seek>(&self, mut w: W, mut _heap: W1)  {
        w.write_all(&CH_CHECK_SEQUENCE.to_le_bytes()).unwrap();
        w.write_all(&self.type_size.to_le_bytes()).unwrap();
        w.write_all(&self.length.to_le_bytes()).unwrap();
        w.write_all(&self.heap_size.to_le_bytes()).unwrap();
        self.limits.serialize_with_heap(w, _heap);
    }
}

pub fn slice_from_type<T: Sized>(t: &mut T) -> &mut [u8] {
    unsafe {std::slice::from_raw_parts_mut(t as *mut T as *mut u8, std::mem::size_of::<T>())}
}


impl<T: SuitableDataType> FromReader for ChunkHeader<T> {
    fn from_reader_and_heap<R: Read>(mut r: R, heap: &[u8]) -> Self {
        assert_eq!(heap, &[]);

        let mut check_sequence: u32 = 0;
        let mut type_size: u32 = 0;
        let mut length: u32 = 0;
        let mut heap_size: u64 = 0;
        r.read_exact(slice_from_type(&mut check_sequence)).unwrap();
        r.read_exact(slice_from_type(&mut type_size)).unwrap();
        r.read_exact(slice_from_type(&mut length)).unwrap();
        r.read_exact(slice_from_type(&mut heap_size)).unwrap();

        assert_eq!(check_sequence, CH_CHECK_SEQUENCE);

        let limits = Range::from_reader_and_heap(r, heap);

        Self {type_size, length, limits, heap_size}
    }
}

// Describes a chunk of tuples, such as min/max ranges (for binary searches), size of the tuple, and how many tuples
// Will be serialized along with the data itself for quicker searches.
#[repr(C)]
pub struct ChunkHeader<T: SuitableDataType> {
    pub type_size: u32,
    pub length: u32,
    pub heap_size: u64,
    pub limits: Range<T>
}

impl<T: SuitableDataType> ChunkHeader<T> {
    pub fn calculate_total_size(&self) -> usize {
        (self.type_size * self.length + self.heap_size as u32) as usize
    }
    pub fn calculate_heap_offset(&self) -> usize {
        (self.type_size * self.length) as usize
    }
}

// todo: refactor table_manager to use chunkheaderindex
// Represents a collection of ChunkHeaders, along with their location in a file for latter searches
#[derive(Debug)]
pub struct ChunkHeaderIndex<T: SuitableDataType>(pub Vec<(ChunkHeader<T>, u64)>);

impl <T: SuitableDataType> ChunkHeaderIndex<T> {
    fn new() -> Self {
        Self(Vec::new())
    }
}
//
// impl <T: SuitableDataType> FromReader for ChunkHeaderIndex<T> {
//     fn from_reader_and_heap<R: Read>(mut r: R, mut heap: R1) -> Self {
//         let mut s = Self::new();
//         let vec = {
//             let mut vec = Vec::new();
//             r.read_to_end(&mut vec).unwrap();
//             vec
//         };
//
//         let mut vec_cursor = Cursor::new(vec);
//         while !vec_cursor.is_empty() {
//             let position = vec_cursor.position();
//             let chunk_header =ChunkHeader::from_reader_and_heap(&mut vec_cursor, &mut );
//             let skip_forward = chunk_header.length * chunk_header.type_size;
//             s.0.push((chunk_header, position));
//             vec_cursor.seek(SeekFrom::Current(skip_forward as i64)).unwrap();
//         };
//         s
//     }
// }

impl<T: SuitableDataType> Debug for ChunkHeader<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("ChunkHeader {{ Heap: {}, Data Length: {}, {:?}}}", self.heap_size, self.length, self.limits))
    }
}
