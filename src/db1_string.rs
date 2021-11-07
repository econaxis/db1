use std::io::{Read, Seek, Write};

use crate::{BytesSerialize, FromReader};
use crate::chunk_header::slice_from_type;

#[derive(Clone, PartialOrd, Ord, Eq, Debug)]
#[repr(C)]
pub enum Db1String {
    Unresolved(u64, u64),
    Resolved(*const u8, u64),
    Resolvedo(String),
}

impl PartialEq for Db1String {
    fn eq(&self, other: &Self) -> bool {
        let s = self.as_string();

        let o = other.as_string();
        if s.is_some() && o.is_some() {
            s == o
        } else {
            match (self, other) {
                (Self::Unresolved(a1, a2), Self::Unresolved(b1, b2)) => {
                    a1 == b1 && a2 == b2
                }
                _ => false
            }
        }
    }
}

impl Default for Db1String {
    fn default() -> Self {
        Self::Resolvedo(Default::default())
    }
}

impl Db1String {
    const TYPE_SIZE: u64 = 18;
    const STRING_CHECK_SEQ: u16 = 0x72a0;
    pub fn resolve(&mut self, heap: &[u8]) {
        match &self {
            Self::Resolved(_, _) | Self::Resolvedo(_) => {}
            Self::Unresolved(loc, len) => {
                let buffer_slice = &heap[*loc as usize..(loc + len) as usize];
                let buffer_slice = buffer_slice.as_ptr();
                *self = Self::Resolved(buffer_slice, *len);
            }
        }
    }
    pub fn as_string(&self) -> Option<&str> {
        match self {
            Self::Resolved(s, len) => {
                let slice = unsafe { std::slice::from_raw_parts(*s, *len as usize) };
                std::str::from_utf8(slice).ok()
            }
            Self::Resolvedo(s) => Some(&s),
            _ => None
        }
    }
}

impl From<String> for Db1String {
    fn from(s: String) -> Self {
        Self::Resolvedo(s)
    }
}

impl BytesSerialize for Db1String {
    fn serialize_with_heap<W: Write, W1: Write + Seek>(&self, mut data: W, mut heap: W1) {
        match self.as_string() {
            Some(s) => {
                let slice = s.as_bytes();
                let heap_position = heap.stream_position().unwrap();
                data.write_all(&Self::STRING_CHECK_SEQ.to_le_bytes()).unwrap();
                data.write_all(&heap_position.to_le_bytes()).unwrap();
                data.write_all(&slice.len().to_le_bytes()).unwrap();
                heap.write_all(slice).unwrap();
            }
            None => {
                panic!("Called serialized on a read Db1String")
            }
        }
    }
}

impl FromReader for Db1String {
    fn from_reader_and_heap<R: Read>(mut r: R, heap: &[u8]) -> Self {
        let mut check_sequence: u16 = 0;
        let mut loc: u64 = 0;
        let mut len: u64 = 0;
        r.read_exact(slice_from_type(&mut check_sequence)).unwrap();
        r.read_exact(slice_from_type(&mut loc)).unwrap();
        r.read_exact(slice_from_type(&mut len)).unwrap();

        assert_eq!(check_sequence, Self::STRING_CHECK_SEQ);

        if (loc == 0 && len == 0) || heap.is_empty() {
            return Self::default();
        }
        Self::Unresolved(loc, len)
    }
}
