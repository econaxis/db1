use std::fmt::{Debug, Formatter};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};

use std::ops::RangeBounds;

use heap_writer::default_heap_writer;

use crate::compressor;
use crate::heap_writer;
use crate::heap_writer::default_mem_writer;
use crate::{BytesSerialize, ChunkHeader, FromReader, Range, SuitableDataType};

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

