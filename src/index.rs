// Application specific

use std::collections::HashSet;
use std::ffi::CStr;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::Cursor;
use std::io::{Read, Seek, Write};

use std::os::raw::c_char;

use serializer::PageSerializer;

use crate::db1_string::Db1String;
use crate::gen_suitable_data_type_impls;
use crate::hash::HashDb;
use crate::{BytesSerialize, FromReader, SuitableDataType, TableManager};

// use tests::rand_string;

// use tests::rand_string;

#[derive(Clone, Debug, PartialEq)]
pub struct ImageDocument {
    id: u64,
    filename: Db1String,
    description: Db1String,
    data: Db1String,
}

#[derive(Clone)]
#[repr(C)]
pub struct FFIImageDocument {
    id: u64,
    filename: *const c_char,
    filename_len: u64,
    description: *const c_char,
    description_len: u64,
    data: *const c_char,
    data_len: u64,
}

impl From<&FFIImageDocument> for ImageDocument {
    fn from(a: &FFIImageDocument) -> Self {
        Self {
            id: a.id,
            filename: Db1String::from((a.filename, a.filename_len)),
            description: Db1String::from((a.description, a.description_len)),
            data: Db1String::from((a.data, a.data_len)),
        }
    }
}

impl Debug for FFIImageDocument {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let tmp: ImageDocument = self.into();
        tmp.fmt(f)
    }
}

#[repr(C)]
pub struct FFIDocumentArray {
    ptr: *const FFIImageDocument,
    len: u64,
}

impl Debug for FFIDocumentArray {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let slice = unsafe { std::slice::from_raw_parts(self.ptr, self.len as usize) };
        f.write_fmt(format_args!("{:?}", slice))
    }
}

impl ImageDocument {
    pub fn get_ffi(&self) -> FFIImageDocument {
        let filename = self.filename.as_ptr_allow_unresolved();
        let description = self.description.as_ptr_allow_unresolved();
        let data = self.data.as_ptr_allow_unresolved();
        FFIImageDocument {
            id: self.id,
            filename: filename.0 as *const c_char,
            filename_len: filename.1,
            description: description.0 as *const c_char,
            description_len: description.1,
            data: data.0 as *const c_char,
            data_len: data.1,
        }
    }
    pub fn new(id: u64, filename: String, description: String, data: String) -> Self {
        Self {
            id,
            filename: Db1String::from(filename),
            description: Db1String::from(description),
            data: Db1String::from(data),
        }
    }
}

impl Hash for ImageDocument {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}
gen_suitable_data_type_impls!(ImageDocument);

impl BytesSerialize for ImageDocument {
    fn serialize_with_heap<W: Write, W1: Write + Seek>(&self, mut data: W, mut heap: W1) {
        data.write_all(&self.id.to_le_bytes()).unwrap();
        self.filename.serialize_with_heap(&mut data, &mut heap);
        self.description.serialize_with_heap(&mut data, &mut heap);
        self.data.serialize_with_heap(&mut data, &mut heap);
    }
}

impl FromReader for ImageDocument {
    fn from_reader_and_heap<R: Read>(mut r: R, heap: &[u8]) -> Self {
        let mut buf = [0u8; 8];
        r.read_exact(&mut buf).unwrap();
        let id = u64::from_le_bytes(buf);

        let filename = Db1String::from_reader_and_heap(&mut r, heap);
        let description = Db1String::from_reader_and_heap(&mut r, heap);
        let data = Db1String::from_reader_and_heap(&mut r, heap);
        Self {
            id,
            filename,
            description,
            data,
        }
    }
}

impl SuitableDataType for ImageDocument {
    const REQUIRES_HEAP: bool = true;
    const TYPE_SIZE: u64 = 8 + 3 * Db1String::TYPE_SIZE;
    fn first(&self) -> u64 {
        self.id
    }
    fn resolve_item(&mut self, heap: &[u8], index: u8) {
        match index {
            1 => self.filename.resolve_item(heap),
            2 => self.description.resolve_item(heap),
            3 => self.data.resolve_item(heap),
            _ => {}
        };
    }
}

#[derive(Debug)]
pub struct ImageDb<Writer: Write + Seek + Read = File> {
    pub db: TableManager<ImageDocument, Writer>,
    index: HashDb,
    output_buf: Vec<ImageDocument>,
    output_buf_ffi: Vec<FFIImageDocument>,
}

impl ImageDb<Cursor<Vec<u8>>> {
    fn open_from_buf(b: Cursor<Vec<u8>>) -> Self {
        ImageDb {
            db: TableManager::read_from_file(b),
            ..Default::default()
        }
    }
}

impl ImageDb<File> {
    pub fn open_from_file(f: File) -> ImageDb {
        let mut s = ImageDb {
            db: TableManager::read_from_file(f),
            index: HashDb::default(),
            output_buf: Vec::<ImageDocument>::default(),
            output_buf_ffi: Vec::<FFIImageDocument>::default(),
        };
        s.load_index();
        s
    }
    pub fn new_from_file(f: File) -> ImageDb {
        ImageDb {
            db: TableManager::new(f),
            index: HashDb::default(),
            output_buf: Vec::<ImageDocument>::default(),
            output_buf_ffi: Vec::<FFIImageDocument>::default(),
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn db2_new(path: *const c_char) -> *mut ImageDb {
    let path = CStr::from_ptr(path).to_str().unwrap();
    let file = File::options()
        .write(true)
        .append(true)
        .read(true)
        .open(path);
    match file {
        Ok(f) => Box::leak(Box::new(ImageDb::open_from_file(f))),
        Err(e) => {
            println!("Making {} because {:?}", path, e);
            Box::leak(Box::new(ImageDb::new_from_file(
                File::options()
                    .write(true)
                    .truncate(true)
                    .read(true)
                    .create(true)
                    .open(path)
                    .unwrap(),
            )))
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn db2_store(
    db: *mut ImageDb,
    id: u64,
    filename: *const c_char,
    filename_len: u64,
    description: *const c_char,
    descr_len: u64,
    data: *const c_char,
    data_len: u64,
) {
    let im = ImageDocument {
        id,
        filename: Db1String::from((filename, filename_len)),
        description: Db1String::from((description, descr_len)),
        data: Db1String::from((data, data_len)),
    };
    log::debug!("Storing im: {:?}", im);
    ImageDb::setup_pointer(db).store(im);
}

#[no_mangle]
pub unsafe extern "C" fn db2_get(db: *mut ImageDb, id: u64, load_mask: u8) -> FFIDocumentArray {
    let db = ImageDb::setup_pointer(db);
    let result = db.get(id, load_mask);

    if let Some(result) = result {
        let fi = result.get_ffi();
        db.output_buf_ffi.push(fi);

        FFIDocumentArray {
            ptr: &db.output_buf_ffi[0] as *const _,
            len: 1,
        }
    } else {
        FFIDocumentArray {
            ptr: std::ptr::null(),
            len: 0,
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn db2_persist(db: *mut ImageDb) {
    let db = ImageDb::setup_pointer(db);
    db.flush_db();
}

#[no_mangle]
pub unsafe extern "C" fn db2_drop(db: *mut ImageDb) {
    let _a = Box::from_raw(db);
}

#[no_mangle]
pub unsafe extern "C" fn db2_get_all(db: *mut ImageDb, mask: u8) -> FFIDocumentArray {
    let db = ImageDb::setup_pointer(db);
    let res = db.db.get_in_all(None, mask);

    println!("Get all res {:?}", res);
    for j in res {
        db.output_buf_ffi.push(j.get_ffi());
    }
    FFIDocumentArray {
        ptr: db.output_buf_ffi.as_ptr(),
        len: db.output_buf_ffi.len() as u64,
    }
}

#[no_mangle]
pub unsafe extern "C" fn db2_get_by_name<'a>(
    db: *mut ImageDb,
    name: *const c_char,
) -> FFIDocumentArray {
    let name = CStr::from_ptr(name).to_str().unwrap();
    let db = ImageDb::setup_pointer(db);
    db.get_by_name(name);

    for j in &db.output_buf {
        db.output_buf_ffi.push(j.get_ffi());
    }
    FFIDocumentArray {
        ptr: db.output_buf_ffi.as_ptr(),
        len: db.output_buf_ffi.len() as u64,
    }
}

impl<T: Default + Write + Seek + Read> Default for ImageDb<T> {
    fn default() -> ImageDb<T> {
        ImageDb {
            db: TableManager::new(T::default()),
            index: HashDb::default(),
            output_buf: Vec::default(),
            output_buf_ffi: Vec::default(),
        }
    }
}

impl<W: Write + Seek + Read> ImageDb<W> {
    pub fn store(&mut self, d: ImageDocument) {
        println!("Storing {:?}", d);
        self.index.store(d.filename.as_buffer(), d.id);
        self.db.store_and_replace(d);
    }

    pub fn load_index(&mut self) {
        let index_spot = self.db.serializer().get_in_all(2, None).unwrap();
        let page = self.serializer().get_page(index_spot);
        self.index = HashDb::from_reader_and_heap(page, &[]);
    }

    pub fn get(&mut self, id: u64, mask: u8) -> Option<&ImageDocument> {
        let result = self.db.get_one(id, mask);
        if let Some(result) = result {
            self.output_buf.push(result);
            Some(&self.output_buf[0])
        } else {
            None
        }
    }

    pub fn get_by_name(&mut self, name: &str) -> &[ImageDocument] {
        self.output_buf.clear();
        let mut seen = HashSet::new();
        let pkeys = self.index.get(name.as_bytes());
        for pkey in pkeys {
            let res = self.db.get_one(pkey, u8::MAX);
            if let Some(exists) = res {
                if exists.filename == name && seen.insert(exists.id) {
                    self.output_buf.push(exists);
                }
            }
        }
        &self.output_buf
    }

    pub fn flush_db(&mut self) {
        self.db.force_flush();

        let mut buf: Cursor<Vec<u8>> = Cursor::default();
        let ch = self.index.serialize(&mut buf);
        buf.set_position(0);
        let len = buf.stream_len().unwrap();
        self.db.serializer().add_page(buf.into_inner(), len, ch);

        self.db.serializer().flush();
    }

    pub fn serializer(&mut self) -> &mut PageSerializer<W> {
        self.db.serializer()
    }
}

impl ImageDb {
    unsafe fn setup_pointer<'a>(db: *mut ImageDb) -> &'a mut Self {
        let reference = unsafe { &mut *db };
        reference.output_buf.clear();
        reference.output_buf_ffi.clear();
        reference
    }
}

#[test]
fn test_persistence() {
    const PATH: &[u8] = b"/tmp/test_persistence\0";
    unsafe {
        let db = db2_new(CStr::from_bytes_with_nul(PATH).unwrap().as_ptr());
        (&mut *db).store(ImageDocument {
            id: 1,
            filename: "fdsa".into(),
            description: "fdsa".into(),
            data: "fds adsa".into(),
        });
        (&mut *db).flush_db();
        db2_drop(db);
        let db = db2_new(CStr::from_bytes_with_nul(PATH).unwrap().as_ptr());
        dbg!(db2_get(db, 1, u8::MAX));
    }
}

#[test]
fn test_name_lookup() {
    let mut imdb: ImageDb<Cursor<_>> = ImageDb::<Cursor<Vec<u8>>>::default();

    let im1 = ImageDocument {
        id: 0,
        filename: "test.jpg".into(),
        description: "fdsa f80da8 408fdsa".into(),
        data: "fdsa f80da8 408fdsa".into(),
    };
    let im2 = ImageDocument {
        id: 1,
        filename: "test.png".into(),
        description: "fdsa pfdsakf80da8 408fdsa".into(),
        data: "fdsa f80da8 408fdsa".into(),
    };
    let im3 = ImageDocument {
        id: 10,
        filename: "test.png".into(),
        description: "fdfdsa 08fdsa8 sa pfdsakf80da8 408fdsa".into(),
        data: "fdsa f80da8 408fdsaf d8a0f8sa".into(),
    };

    imdb.store(im1.clone());
    imdb.store(im2.clone());
    imdb.store(im3.clone());
    imdb.flush_db();

    assert_eq!(imdb.get_by_name("test.jpg"), [im1]);
    assert_eq!(imdb.get_by_name("test.png"), [im2, im3]);
}

fn test_serialize(mut i: ImageDb<Cursor<Vec<u8>>>) -> ImageDb<Cursor<Vec<u8>>> {
    i.flush_db();
    let mut ser = i.serializer().replace_inner(Cursor::default());
    ser.set_position(0);

    ImageDb::open_from_buf(ser)
}

#[test]
fn test_long() {
    use hash::hash;
    use tests::rand_string;
    const TOTAL_LEN: usize = 100;
    let mut imdb = ImageDb::<Cursor<Vec<u8>>>::default();

    for i in 0..TOTAL_LEN {
        let im1 = ImageDocument {
            id: i as u64,
            filename: format!("test{}", hash(&i) % 100).into(),
            description: rand_string(100).into(),
            data: rand_string(100).into(),
        };
        imdb.store(im1);
        imdb.flush_db();
    }

    println!("done");
    let mut imdb = test_serialize(imdb);
    imdb.load_index();

    let mut seen = vec![0u8; TOTAL_LEN];
    for i in 0..100 {
        for img in imdb.get_by_name(&format!("test{}", i)) {
            seen[img.id as usize] += 1;
            println!("got image {} for {}", img.id, i);
            assert_eq!(hash(&img.id) % 100, i);
        }
    }
    assert!(seen.iter().all(|a| *a == 1));
}

#[test]
fn test_c_api() {
    use std::ffi::CString;
    use tests::rand_string;
    unsafe {
        let _ = std::fs::remove_file("/tmp/test_c_api_file");
        let p = CString::new("/tmp/test_c_api_file").unwrap();
        let db = db2_new(p.as_ptr());

        let mut imdbs = Vec::new();
        for id in 0..10000 {
            imdbs.push(ImageDocument {
                id,
                filename: rand_string(1000).into(),
                description: rand_string(10).into(),
                data: rand_string(10).into(),
            });
            let last = imdbs.last().unwrap();
            db2_store(
                db,
                last.id,
                last.filename.as_ptr(),
                last.filename.len(),
                last.description.as_ptr(),
                last.description.len(),
                last.data.as_ptr(),
                last.data.len(),
            );
        }

        for v in imdbs {
            let res = db2_get(db, v.id, u8::MAX);
            let first = unsafe { &*res.ptr };
            assert_eq!(<&FFIImageDocument as Into<ImageDocument>>::into(first), v);

            let names =
                db2_get_by_name(db, CString::new(v.filename.as_buffer()).unwrap().into_raw());
            assert!(names.len >= 1);
            let first = unsafe { &*names.ptr };
            assert_eq!(<&FFIImageDocument as Into<ImageDocument>>::into(first), v);
        }
    }
}

#[test]
fn test_load_description_only() {
    use tests::rand_string;
    let mut im: ImageDb = ImageDb::new_from_file(
        File::options()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open("/tmp/test_load_descr")
            .unwrap(),
    );
    for i in 0..1000 {
        im.store(ImageDocument {
            id: i,
            filename: rand_string(10).into(),
            description: rand_string(10).into(),
            data: rand_string(10).into(),
        });
    }
    let _result = im.get(50, 0b1010);
    let result = unsafe { db2_get(&mut im as *mut _, 50, 0b1010) };
    dbg!(result);
}
