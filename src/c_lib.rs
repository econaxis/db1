use crate::{BytesSerialize, FromReader, QueryableDataType, SuitableDataType, TableManager};
use std::cmp::Ordering;
use crate::chunk_header::slice_from_type;
use std::collections::hash_map::DefaultHasher;
use std::ffi::{CStr, CString};
use std::hash::{Hash, Hasher};
use std::io::{Read, Seek, Write, SeekFrom};
use std::os::raw::c_char;
use std::fs::File;
use crate::db1_string::Db1String;

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

impl QueryableDataType for Document {}

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
    document: *const c_char,
) {
    let name = CStr::from_ptr(name).to_owned().to_string_lossy().into_owned().into();
    let document = CStr::from_ptr(document).to_owned().to_string_lossy().into_owned().into();

    (&mut *db).store(Document { id, name, document });
}

#[repr(C)]
pub struct StrFatPtr {
    ptr: *const c_char,
    len: u64
}

#[no_mangle]
pub unsafe extern "C" fn db1_get(
    db: *mut TableManager<Document, File>,
    id: u32,
    field: u8,
) -> StrFatPtr {
    let id = id as u64;
    let mut result = (&mut *db).get_in_all(id..=id);

    if let Some(result) = result.first_mut() {
        let document = match field {
            0 => std::mem::take(&mut result.name),
            1 => std::mem::take(&mut result.document),
            _ => panic!()
        };
        match document {
            Db1String::Resolved(ptr, len) => StrFatPtr {ptr: ptr as *const c_char, len},
            _ => panic!()
        }
    } else {
        StrFatPtr {ptr: std::ptr::null(), len: 0}
    }
}

#[no_mangle]
pub unsafe extern "C" fn free_char_p(f: *mut c_char) {
    CString::from_raw(f);
}

#[no_mangle]
pub unsafe extern "C" fn db1_new(filename: *const c_char) -> *mut TableManager<Document, File> {
    let file = File::with_options()
        .create(true)
        .read(true)
        .write(true)
        .open(CStr::from_ptr(filename).to_str().unwrap())
        .unwrap();

    let mut file1 = file.try_clone().unwrap();
    file1.seek(SeekFrom::Start(0)).unwrap();
    let db = TableManager::<Document, File>::read_from_file(file1, file);
    Box::leak(Box::new(db))
}

#[no_mangle]
pub unsafe extern "C" fn db1_persist(db: *mut TableManager<Document, File>) {
    (&mut *db).force_flush();
}

#[test]
fn test_document() {
    unsafe {
        let dbm = db1_new(CStr::from_bytes_with_nul(b"/tmp/test1\0").unwrap().as_ptr());
        dbg!(&mut *dbm);
        let name = CString::new("fdsafsvcx").unwrap();
        let document = CString::new(" fdsafsaduf sa hdsapuofhs f").unwrap();
        db1_store(dbm, 3, name.clone().into_raw(), document.clone().into_raw());

        let res = db1_get(dbm, 3, 1);
        let str = CString::from_raw(res);
        assert_eq!(str.as_bytes(), document.as_bytes());

        dbg!(CString::from_raw(db1_get(dbm, 3, 0)));
        db1_persist(dbm);


        let dbm = db1_new(CStr::from_bytes_with_nul(b"/tmp/test1\0").unwrap().as_ptr());
        dbg!(db1_get(dbm, 3, 1));
    }
}
