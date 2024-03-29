use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::io::{Read, Seek, Write};
use std::os::raw::c_char;

use crate::chunk_header::slice_from_type;
use crate::{BytesSerialize, FromReader};

#[derive(Clone, Eq)]
#[repr(C)]
pub enum Db1String {
    // Offset + length to heap array
    Unresolved(u64, u64),
    Ptr(*const u8, usize),
    // Owned byte array
    Resolvedo(Vec<u8>),
}

impl PartialOrd for Db1String {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_buffer().partial_cmp(other.as_buffer())
    }
}

impl Ord for Db1String {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl From<(*const c_char, u64)> for Db1String {
    fn from((ptr, len): (*const c_char, u64)) -> Self {
        let vec = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) }.to_vec();
        Self::Resolvedo(vec)
    }
}

impl PartialEq<&str> for Db1String {
    fn eq(&self, other: &&str) -> bool {
        self.as_buffer().eq(other.as_bytes())
    }
}

impl Hash for Db1String {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if let Self::Resolvedo(v) = self {
            v.hash(state)
        } else {
            panic!()
        }
    }
}

impl Debug for Db1String {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Resolvedo(v) if v.len() >= 100 => f.write_str("Db1string over 100"),
            Self::Resolvedo(v) if !v.is_empty() => f.write_fmt(format_args!(
                "db1\"{}\"",
                std::str::from_utf8(v).unwrap_or("non-utf8")
            )),
            Self::Resolvedo(_v) => f.write_fmt(format_args!("Empty Document")),
            Self::Unresolved(a, b) => {
                f.write_fmt(format_args!("Document unknown ind {} len {}", *a, *b))
            }
            Self::Ptr(x, y) => {
                let slice = std::str::from_utf8(unsafe { std::slice::from_raw_parts(*x, *y) }).unwrap_or("non-utf8");
                f.write_fmt(format_args!("db1-{}", slice))
            }
        }
    }
}

impl PartialEq for Db1String {
    fn eq(&self, other: &Self) -> bool {
        self.as_buffer() == other.as_buffer()
    }
}

impl Default for Db1String {
    fn default() -> Self {
        Self::Resolvedo(Default::default())
    }
}

impl Db1String {
    pub const TYPE_SIZE: u64 = 1 + 4 + 4;
    const STRING_CHECK_SEQ: u8 = 0xa7;
    pub fn as_buffer(&self) -> &[u8] {
        match self {
            Self::Resolvedo(s) => s,
            Self::Ptr(ptr, len) => unsafe {
                std::slice::from_raw_parts(*ptr, *len)
            }
            _ => panic!(),
        }
    }
    pub fn to_ptr(self, heap: &[u8]) -> Self {
        match self {
            Db1String::Unresolved(offset, len) => unsafe {
                Db1String::Ptr(heap.as_ptr().add(offset as usize), len as usize)
            },
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
            Self::Resolvedo(a) => (a.as_ptr(), a.len() as u64),
            Self::Ptr(ptr, len) => (*ptr, *len as u64),
            Self::Unresolved(_, _) => (std::ptr::null(), 0),
        }
    }
    pub fn resolve_item(&mut self, heap: &[u8]) {
        match self {
            Self::Resolvedo(_v) => {}
            Self::Unresolved(ind, len) => {
                *self = Self::Resolvedo(heap[*ind as usize..(*ind + *len) as usize].to_vec())
            }
            Self::Ptr(_, _) => panic!()
        }
    }
}

impl From<String> for Db1String {
    fn from(s: String) -> Self {
        Self::Resolvedo(s.into_bytes())
    }
}

impl<'a> From<&'a str> for Db1String {
    fn from(s: &'a str) -> Self {
        Self::from(s.to_string())
    }
}

impl From<Vec<u8>> for Db1String {
    fn from(s: Vec<u8>) -> Self {
        Self::Resolvedo(s)
    }
}

fn as_bytes<T: 'static>(t: &T) -> &[u8] {
    let ptr = t as *const T as *const u8;
    let len = std::mem::size_of::<T>();
    unsafe { std::slice::from_raw_parts(ptr, len) }
}

impl BytesSerialize for Db1String {
    fn serialize_with_heap<W: Write, W1: Write + Seek>(&self, mut data: W, mut heap: W1) {
        let slice = self.as_buffer();

        let slicelen = slice.len() as u32;
        let heap_position = heap.stream_position().unwrap() as u32;

        data.write_all(&Self::STRING_CHECK_SEQ.to_le_bytes()).unwrap();
        data.write_all(&heap_position.to_le_bytes()).unwrap();
        data.write_all(&slicelen.to_le_bytes()).unwrap();
        heap.write_all(slice).unwrap();
    }
}

impl FromReader for Db1String {
    fn from_reader_and_heap<R: Read>(mut r: R, _heap: &[u8]) -> Self {
        let mut check_sequence: u8 = 0;
        let mut loc: u32 = 0;
        let mut len: u32 = 0;
        r.read_exact(slice_from_type(&mut check_sequence)).unwrap();
        r.read_exact(slice_from_type(&mut loc)).unwrap();
        r.read_exact(slice_from_type(&mut len)).unwrap();

        assert_eq!(check_sequence, Self::STRING_CHECK_SEQ);

        if loc == 0 && len == 0 {
            return Self::default();
        }
        Self::Unresolved(loc as u64, len as u64)
    }
}
