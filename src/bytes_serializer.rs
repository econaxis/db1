use std::io::{Write, Read, Seek};

pub trait FromReader {
    fn from_reader_and_heap<R: Read>(r: R, heap: &[u8]) -> Self;
}

pub trait BytesSerialize: Sized {
    fn serialize_with_heap<W: Write, W1: Write + Seek>(&self, mut data: W, mut heap: W1) {
        let bytes = unsafe {
            std::slice::from_raw_parts(self as *const Self as *const u8, std::mem::size_of::<Self>())
        };

        data.write_all(bytes).unwrap();
    }
}

#[macro_export]
macro_rules! bytes_serializer {
    ($x: ty) => {
        impl crate::bytes_serializer::BytesSerialize for $x {}
    };
}

#[macro_export]
macro_rules! from_reader {
    ($x: ty) => {
        impl crate::bytes_serializer::FromReader for $x {
            fn from_reader_and_heap<R: Read>(mut r: R, mut data: &[u8]) -> Self {
                let mut buffer = [0u8; std::mem::size_of::<$x>()];
                r.read_exact(&mut buffer).unwrap();

                unsafe {std::mem::transmute(buffer)}
            }
        }
    }
}