
use std::io::{Read};










fn read_to_vec<R: Read>(mut r: R, len: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(len);
    buf.resize(len, 0);
    r.read_exact(&mut buf).unwrap();
    buf
}

pub fn read_to_buf<R: Read, const N: usize>(mut r: R) -> [u8; N] {
    let mut buf = [0u8; N];
    r.read_exact(&mut buf).unwrap();
    buf
}

