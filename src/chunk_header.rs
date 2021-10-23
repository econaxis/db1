use std::fmt::{Debug, Formatter};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};

use crate::bytes_serializer::{BytesSerialize, FromReader};
use crate::{Range};
use crate::suitable_data_type::SuitableDataType;

const CH_CHECK_SEQUENCE: u32 = 0x32af8429;
impl<T: SuitableDataType> BytesSerialize for ChunkHeader<T> where T: BytesSerialize {
    fn serialize<W: Write>(&self, mut w: W)  {
        w.write(&CH_CHECK_SEQUENCE.to_le_bytes()).unwrap();
        w.write(&self.type_size.to_le_bytes()).unwrap();
        w.write(&self.length.to_le_bytes()).unwrap();
        self.limits.serialize(w);
    }
}

pub fn slice_from_type<T: Sized>(t: &mut T) -> &mut [u8] {
    unsafe {std::slice::from_raw_parts_mut(t as *mut T as *mut u8, std::mem::size_of::<T>())}
}


impl<T: SuitableDataType> FromReader for ChunkHeader<T> {
    fn from_reader<R: Read>(r: &mut R) -> Self {
        let mut check_sequence: u32 = 0;
        let mut type_size: u32 = 0;
        let mut length: u32 = 0;
        r.read_exact(slice_from_type(&mut check_sequence)).unwrap();
        r.read_exact(slice_from_type(&mut type_size)).unwrap();
        r.read_exact(slice_from_type(&mut length)).unwrap();

        assert_eq!(check_sequence, CH_CHECK_SEQUENCE);

        let limits = Range::from_reader(r);

        Self {type_size, length, limits}
    }
}

#[repr(C)]
pub struct ChunkHeader<T: SuitableDataType> {
    pub type_size: u32,
    pub length: u32,
    pub limits: Range<T>
}

#[derive(Debug)]
pub struct ChunkHeaderIndex<T: SuitableDataType>(pub Vec<(ChunkHeader<T>, u64)>);

impl <T: SuitableDataType> ChunkHeaderIndex<T> {
    fn new() -> Self {
        Self(Vec::new())
    }
}

impl <T: SuitableDataType> FromReader for ChunkHeaderIndex<T> {
    fn from_reader<R: Read>(r: &mut R) -> Self {
        let mut s = Self::new();
        let vec = {
            let mut vec = Vec::new();
            r.read_to_end(&mut vec).unwrap();
            vec
        };

        let mut vec_cursor = Cursor::new(vec);
        while !vec_cursor.is_empty() {
            let position = vec_cursor.position();
            let chunk_header =ChunkHeader::from_reader(&mut vec_cursor);
            let skip_forward = chunk_header.length * chunk_header.type_size;
            s.0.push((chunk_header, position));
            vec_cursor.seek(SeekFrom::Current(skip_forward as i64)).unwrap();
        };
        s
    }
}

impl<T: SuitableDataType> Debug for ChunkHeader<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("ChunkHeader {{ {}, {}, {:?}}}", self.type_size, self.length, self.limits))
    }
}
