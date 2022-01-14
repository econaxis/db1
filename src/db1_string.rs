use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::io::{Read, Seek, Write};
use std::os::raw::c_char;

use crate::{BytesSerialize, FromReader};
use crate::chunk_header::slice_from_type;

#[derive(Clone, Eq)]
#[repr(C)]
pub enum Db1String {
    Unresolved(u64, u64),
    Resolvedo(Vec<u8>, bool),
}

impl From<(*const c_char, u64)> for Db1String {
    fn from((ptr, len): (*const c_char, u64)) -> Self {
        let vec = unsafe {std::slice::from_raw_parts(ptr as *const u8, len as usize)}.to_vec();
        Self::Resolvedo(vec, false)
    }
}

impl PartialEq<&str> for Db1String {
    fn eq(&self, other: &&str) -> bool {
        self.as_buffer().eq(other.as_bytes())
    }
}

impl Hash for Db1String {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if let Self::Resolvedo(v, _) = self {
            v.hash(state)
        } else {
            panic!()
        }
    }
}

impl Debug for Db1String {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Resolvedo(v, _) if v.len() > 0 => f.write_fmt(format_args!("Document {}", std::str::from_utf8(v).unwrap_or("non-utf8"))),
            Self::Resolvedo(v, _)  => f.write_fmt(format_args!("Empty Document")),
            Self::Unresolved(a, b) => f.write_fmt(format_args!("Document unknown ind {} len {}", *a, *b))
        }
    }
}

impl PartialEq for Db1String {
    fn eq(&self, other: &Self) -> bool {
        self.as_buffer() == other.as_buffer()
        // if s.is_some() && o.is_some() {
        //     s == o
        // } else {
        //     match (self, other) {
        //         (Self::Unresolved(a1, a2), Self::Unresolved(b1, b2)) => {
        //             a1 == b1 && a2 == b2
        //         }
        //         _ => false
        //     }
        // }
    }
}

impl Default for Db1String {
    fn default() -> Self {
        Self::Resolvedo(Default::default(), false)
    }
}

impl Db1String {
    pub const TYPE_SIZE: u64 = 18;
    const STRING_CHECK_SEQ: u16 = 0x72a0;
    pub fn as_buffer(&self) -> &[u8] {
        match self {
            Self::Resolvedo(s, _) => {
                s
            }
            _ => panic!()
        }
    }
    pub fn as_ptr(&self) -> *const c_char {
        self.as_buffer().as_ptr() as *const c_char
    }
    pub fn len(&self) -> u64 {
        self.as_buffer().len() as u64
    }
    pub fn as_ptr_allow_unresolved(&self) -> (*const u8, u64) {
        match self {
            Self::Resolvedo(a, _) => (a.as_ptr(), a.len() as u64),
            Self::Unresolved(_, _) => (std::ptr::null(), 0)
        }
    }
    pub fn resolve_item(&mut self, heap: &[u8]) {
        match self {
            Self::Resolvedo(v, true) => {panic!("Shouldn't happen")},
            Self::Resolvedo(v, false) => {}
            Self::Unresolved(ind, len) => {
                *self = Self::Resolvedo(heap[*ind as usize..(*ind + *len) as usize].to_vec(), true)
            }
        }
    }
}


impl From<String> for Db1String {
    fn from(s: String) -> Self {
        Self::Resolvedo(s.into_bytes(), false)
    }
}

impl<'a> From<&'a str> for Db1String {
    fn from(s: &'a str) -> Self {
        Self::from(s.to_string())
    }
}

impl From<Vec<u8>> for Db1String {
    fn from(s: Vec<u8>) -> Self {
        Self::Resolvedo(s, false)
    }
}

impl BytesSerialize for Db1String {
    fn serialize_with_heap<W: Write, W1: Write + Seek>(&self, mut data: W, mut heap: W1) {
        let slice = self.as_buffer();
        let heap_position = heap.stream_position().unwrap();
        data.write_all(&Self::STRING_CHECK_SEQ.to_le_bytes()).unwrap();
        data.write_all(&heap_position.to_le_bytes()).unwrap();
        data.write_all(&slice.len().to_le_bytes()).unwrap();
        heap.write_all(slice).unwrap();
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
