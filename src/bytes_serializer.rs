use std::io::Write;

pub trait FromReader {
    fn from_reader<R: std::io::Read>(r: &mut R) -> Self;
}

pub trait BytesSerialize: Sized {
    fn serialize<W: Write>(&self, w: &mut W) {
        let bytes = unsafe {
            std::slice::from_raw_parts(self as *const Self as *const u8, std::mem::size_of::<Self>())
        };

        w.write_all(bytes).unwrap();
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
            fn from_reader<R: std::io::Read>(r: &mut R) -> Self {
                let mut buffer = [0u8; std::mem::size_of::<$x>()];
                r.read_exact(&mut buffer).unwrap();

                unsafe {std::mem::transmute(buffer)}
            }
        }
    }
}