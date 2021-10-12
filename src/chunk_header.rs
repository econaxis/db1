use std::fmt::{Debug, Formatter};
use std::io::{Cursor, IoSlice, Read, Seek, SeekFrom, Write};

use crate::bytes_serializer::{BytesSerialize, FromReader};
use crate::CHECK_BYTES;
use crate::suitable_data_type::SuitableDataType;

impl<T: SuitableDataType> BytesSerialize for ChunkHeader<T> where T: BytesSerialize {
    fn serialize<W: Write>(&self, w: &mut W)  {
        let my_bytes = IoSlice::new(unsafe {
            std::slice::from_raw_parts(self as *const Self as *const u8, std::mem::size_of::<Self>())
        });

        let check_bytes = IoSlice::new(unsafe {
                std::slice::from_raw_parts(&CHECK_BYTES as *const u64 as *const u8, 8)
            });

        w.write_all_vectored(&mut [my_bytes, check_bytes]).unwrap();
    }
}

impl<T: SuitableDataType> FromReader for ChunkHeader<T> {
    fn from_reader<R: Read>(r: &mut R) -> Self {
        let mut buffer: Self = unsafe {std::mem::MaybeUninit::uninit().assume_init()};
        let mut slice = unsafe {std::slice::from_raw_parts_mut(&mut buffer as *mut Self as *mut u8, std::mem::size_of::<Self>())};
        r.read_exact(&mut slice).unwrap();

        let mut check_bytes = [0u8;8];

        r.read_exact(&mut check_bytes);

        assert_eq!(unsafe {std::ptr::read(check_bytes.as_ptr() as *const u64)}, CHECK_BYTES);

        buffer
    }
}

#[repr(C)]
pub struct ChunkHeader<T: SuitableDataType> {
    pub type_size: u32,
    pub length: u32,
    pub start: T,
    pub end: T,
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
            vec_cursor.seek(SeekFrom::Current(skip_forward as i64));
        };
        s
    }
}

impl<T: SuitableDataType> Debug for ChunkHeader<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("ChunkHeader {{ {}, {}, {:?}, {:?}}}", self.type_size, self.length, self.start, self.end))
    }
}
