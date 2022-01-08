#![feature(cursor_remaining)]
#![feature(write_all_vectored)]
#![feature(is_sorted)]
#![feature(map_first_last)]
#![allow(clippy::manual_strip)]
#![allow(clippy::assertions_on_constants)]
#![allow(unused_unsafe)]

use std::cmp::Ordering;

use std::ffi::{CStr, CString};
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

use std::os::raw::c_char;



pub use range::Range;

pub use crate::{bytes_serializer::BytesSerialize, bytes_serializer::FromReader,
                chunk_header::ChunkHeader,
                chunk_header::ChunkHeaderIndex, suitable_data_type::DataType, suitable_data_type::QueryableDataType,
                suitable_data_type::SuitableDataType, table_base::TableBase, table_manager::TableManager};
use crate::chunk_header::slice_from_type;
use crate::db1_string::Db1String;

mod buffer_pool;
mod bytes_serializer;
mod chunk_header;
mod heap_writer;
mod range;
mod suitable_data_type;
mod table_base;
mod table_manager;
mod tests;
mod db1_string;
mod compressor;


#[repr(C)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Document {
    id: u32,
    name: Db1String,
    document: Db1String,
}


impl Document {
    pub fn hash(&self) -> u64 {
        self.id as u64
    }
}

impl QueryableDataType for Document {
    fn clone1(&self, heap: &[u8]) -> Self {
        Self {
            id: self.id,
            name: self.name.clone1(heap),
            document: self.document.clone1(heap)
        }
    }
}

impl PartialEq<u64> for Document {
    fn eq(&self, other: &u64) -> bool {
        self.hash().eq(other)
    }
}

impl PartialOrd<u64> for Document {
    fn partial_cmp(&self, other: &u64) -> Option<Ordering> {
        self.hash().partial_cmp(other)
    }
}

impl PartialOrd for Document {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.hash().partial_cmp(&other.hash())
    }
}

impl Ord for Document {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl SuitableDataType for Document {
    const REQUIRES_HEAP: bool = true;
    const TYPE_SIZE: u64 = (8 + 8 + 2) * 2 + 4;
    fn first(&self) -> u64 {
        todo!()
    }
    fn resolve(&mut self, heap: &[u8]) {
        self.name.resolve(heap);
        self.document.resolve(heap);
    }
}

impl BytesSerialize for Document {
    fn serialize_with_heap<W: Write, W1: Write + Seek>(&self, mut data: W, mut heap: W1) {
        data.write_all(&self.id.to_le_bytes()).unwrap();
        self.name.serialize_with_heap(&mut data, &mut heap);
        self.document.serialize_with_heap(data, heap);
    }
}

impl FromReader for Document {
    fn from_reader_and_heap<R: Read>(mut r: R, heap: &[u8]) -> Self {
        let mut id: u32 = 0;
        r.read_exact(slice_from_type(&mut id)).unwrap();
        let name = Db1String::from_reader_and_heap(&mut r, heap);
        let document = Db1String::from_reader_and_heap(r, heap);
        Self { id, name, document }
    }
}

#[no_mangle]
pub unsafe extern "C" fn db1_store(
    db: *mut TableManager<Document, File>,
    id: u32,
    name: *const c_char,
    name_len: u32,
    document: *const c_char,
    document_len: u32,
) {
    let name: Db1String = unsafe { std::slice::from_raw_parts(name as *const u8, name_len as usize) }.to_vec().into();
    let document: Db1String = unsafe { std::slice::from_raw_parts(document as *const u8, document_len as usize) }.to_vec().into();

    (&mut *db).store_and_replace(Document { id, name, document });
}

#[repr(C)]
pub struct StrFatPtr {
    ptr: *const c_char,
    len: u64,
}

impl Debug for StrFatPtr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let str = unsafe { std::slice::from_raw_parts(self.ptr as *const u8, self.len as usize) };
        let str = std::str::from_utf8(str).unwrap();
        f.write_fmt(format_args!("{}", str))
    }
}

impl StrFatPtr {
    fn as_str(&self) -> &str {
        let str = unsafe { std::slice::from_raw_parts(self.ptr as *const u8, self.len as usize) };
        if let Ok(s) = std::str::from_utf8(str) {
            s
        } else {
            panic!("String error! {:?}", str);
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn db1_get(
    db: *mut TableManager<Document, File>,
    id: u32,
    field: u8,
) -> StrFatPtr {
    let id = id as u64;
    let mut result = (&mut *db).get_in_all(id..=id);

    if let Some(result) = result.first() {
        let document = match field {
            0 => &result.name,
            1 => &result.document,
            _ => panic!()
        };
        match document.as_buffer() {
            Some(str) => {
                StrFatPtr { ptr: str.as_ptr() as *const c_char, len: str.len() as u64 }
            }
            _ => panic!()
        }
    } else {
        StrFatPtr { ptr: std::ptr::null(), len: 0 }
    }
}

#[no_mangle]
pub unsafe extern "C" fn free_char_p(f: *mut c_char) {
    let _todrop = CString::from_raw(f);
}

#[no_mangle]
pub unsafe extern "C" fn db1_new(filename: *const c_char) -> *mut TableManager<Document, File> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .write(true)
        .open(CStr::from_ptr(filename).to_str().unwrap())
        .unwrap();
    let file1 = File::open(CStr::from_ptr(filename).to_str().unwrap()).unwrap();
    file.seek(SeekFrom::End(0)).unwrap();
    let db = TableManager::<Document, File>::read_from_file(file1, file);
    Box::leak(Box::new(db))
}

#[no_mangle]
pub unsafe extern "C" fn db1_persist(db: *mut TableManager<Document, File>) {
    (&mut *db).force_flush();
}


#[test]
fn test_document() {
    const PATH: &[u8] = b"/tmp/test1\0";
    std::fs::remove_file("/tmp/test1").unwrap();
    unsafe {
        let dbm = db1_new(CStr::from_bytes_with_nul(PATH).unwrap().as_ptr());
        dbg!(&mut *dbm);
        let name = CString::new("fdsafsvcx").unwrap();
        let document = CString::new(" fdsafsaduf sa hdsapuofhs f").unwrap();
        db1_store(dbm, 3, name.clone().into_raw(), name.as_bytes().len() as u32, document.clone().into_raw(), document.as_bytes().len() as u32);

        let res = db1_get(dbm, 3, 1);
        println!("{:?}", res.ptr);
        assert_eq!(res.as_str().as_bytes(), document.as_bytes());

        dbg!(db1_get(dbm, 3, 0));
        db1_persist(dbm);


        let dbm = db1_new(CStr::from_bytes_with_nul(PATH).unwrap().as_ptr());
        dbg!(db1_get(dbm, 3, 1));
    }
}

#[test]
fn test_debug() {
    unsafe {
        let dbm = db1_new(CStr::from_bytes_with_nul(b"/tmp/test1\0").unwrap().as_ptr());
        dbg!(db1_get(dbm, 3, 0));
    }
}
