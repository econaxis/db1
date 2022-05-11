use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::io::{IoSlice, Read, Seek, Write};
use std::os::raw::c_char;

use crate::chunk_header::slice_from_type;
use crate::{BytesSerialize, FromReader};

#[derive(Clone, Eq)]
#[repr(C)]
pub enum Db1String {
    // Offset + length to heap array
    Unresolved(u64, u64),

    // Owned byte array
    Resolvedo(Vec<u8>),

    // Pointer + length to memory location
    Ptr(*const u8, u64),
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
            Self::Ptr(ptr, len) => {
                let slice = unsafe { std::slice::from_raw_parts(*ptr, *len as usize) };
                let str = std::str::from_utf8(slice);
                f.write_fmt(format_args!("Db1 {:?}", str))
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
            Self::Ptr(ptr, len) => unsafe { std::slice::from_raw_parts(*ptr, *len as usize) },
            _ => panic!(),
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
            Self::Unresolved(_, _) => (std::ptr::null(), 0),
            Self::Ptr(ptr, len) => (*ptr, *len),
        }
    }
    pub fn owned(&mut self) {
        match self {
            Self::Ptr(ptr, len) => {
                let slice = unsafe { std::slice::from_raw_parts(*ptr, *len as usize) };
                let vec = slice.to_vec();
                *self = Self::Resolvedo(vec);
            }
            _ => {}
        }
    }
    pub fn resolve_item(&mut self, heap: &[u8]) {
        match self {
            Self::Resolvedo(_v) => {
                panic!("Shouldn't happen")
            }
            Self::Resolvedo(_v) => {}
            Self::Unresolved(ind, len) => {
                *self = Self::Resolvedo(heap[*ind as usize..(*ind + *len) as usize].to_vec())
            }
            Self::Ptr(..) => {}
        }
    }

    pub fn read_to_ptr(data: impl Read, heap: &[u8]) -> Self {
        let mut s = Self::from_reader_and_heap(data, heap);
        match s {
            Db1String::Unresolved(loc, len) => {
                s = Db1String::Ptr(heap[loc as usize..].as_ptr(), len);
            }
            _ => panic!(),
        }
        s
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
        let buf1 = IoSlice::new(as_bytes(&Self::STRING_CHECK_SEQ));
        let buf2 = IoSlice::new(as_bytes(&heap_position));
        let buf3 = IoSlice::new(as_bytes(&slicelen));
        data.write_all_vectored([buf1, buf2, buf3].as_mut_slice())
            .unwrap();
        heap.write_all(slice).unwrap();
    }
}

impl FromReader for Db1String {
    fn from_reader_and_heap<R: Read>(mut r: R, heap: &[u8]) -> Self {
        let mut check_sequence: u8 = 0;
        let mut loc: u32 = 0;
        let mut len: u32 = 0;
        r.read_exact(slice_from_type(&mut check_sequence)).unwrap();
        r.read_exact(slice_from_type(&mut loc)).unwrap();
        r.read_exact(slice_from_type(&mut len)).unwrap();

        assert_eq!(check_sequence, Self::STRING_CHECK_SEQ);

        if (loc == 0 && len == 0) || heap.is_empty() {
            return Self::default();
        }
        Self::Unresolved(loc as u64, len as u64)
    }
}
