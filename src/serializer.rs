use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::option::Option::None;

use chunk_header::ChunkHeaderIndex;
use table_base::read_to_buf;
use table_base2::TableBase2;
use {ChunkHeader, FromReader};
use dynamic_tuple::TypeData;

#[derive(Debug)]
pub struct PageSerializer<W: Read + Write + Seek> {
    pub file: W,
    pub previous_headers: ChunkHeaderIndex,
    deleted: Vec<(u64, u64)>,
    pinned: HashSet<u64>,
    pub cache: HashMap<u64, TableBase2>,
    constant_size: Option<u64>,
}


impl Default for PageSerializer<Cursor<Vec<u8>>> {
    fn default() -> Self {
        Self::create(Cursor::default(), Some(16000))
    }
}

pub struct LimitedReader<W>(W, usize);

impl<W> LimitedReader<W> {
    pub(crate) fn size(&self) -> usize {
        self.1
    }
    pub fn new(w: W, size: usize) -> LimitedReader<W> {
        assert!(size != 0);
        LimitedReader(w, size)
    }
}

pub struct PageData<'a, W> {
    w: &'a mut W,
    pos: u64,
    len: u64,
    nextpos: u64,
}

impl<'a, W> Debug for PageData<'a, W> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageData")
            .field("pos", &self.pos)
            .field("len", &self.len)
            .field("nextpos", &self.nextpos)
            .finish()
    }
}

enum PageResult<'a, W> {
    Good(PageData<'a, W>),
    Deleted(PageData<'a, W>),
    Eof,
}

impl<'a, W> Debug for PageResult<'a, W> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Good(x) => f.write_fmt(format_args!("Good {:?}", x)),
            Self::Deleted(x) => f.write_fmt(format_args!("Deleted {:?}", x)),
            Self::Eof => f.write_str("Eof"),
        }
    }
}

impl<W: Write + Read + Seek> PageSerializer<W> {
    const CHECK_SEQ: u64 = 3180343028731803290;
    const WORKING_PAGE: u16 = 31920;
    const PAGEOVERHEAD: u64 = 6;
    const DELETED_PAGE: u16 = 21923;

    pub fn maximum_serialized_len(&self) -> usize {
        self.constant_size.unwrap_or(16000) as usize
    }
    pub fn replace_inner(&mut self, w: W) -> W {
        std::mem::replace(&mut self.file, w)
    }

    pub fn free_page(&mut self, ty: u64, pkey: TypeData) {
        // Check that page is still valid
        let p = self.previous_headers.remove(ty, pkey);
        if let PageResult::Good(pd) = Self::page_checked(&mut self.file, Some(p)) {
            assert_eq!(pd.pos, p);
            pd.w.seek(SeekFrom::Start(p)).unwrap();
            pd.w.write_all(&Self::DELETED_PAGE.to_le_bytes()).unwrap();

            println!("Deleting page with pos {} len {}", pd.pos, pd.len);

            self.deleted.push((p, pd.len + Self::PAGEOVERHEAD));
        } else {
            panic!()
        }
    }

    pub fn flush(&mut self) {
        self.file.flush().unwrap();
    }

    fn check_is_valid(r: &mut W) -> bool {
        assert_eq!(r.stream_position().unwrap(), 0);
        let mut u64_buf = [0u8; 8];
        if r.read_exact(&mut u64_buf).is_err() {
            return false;
        }
        let check_seq = u64::from_le_bytes(u64_buf);
        check_seq == Self::CHECK_SEQ
    }

    fn iter_pages(r: &mut W) -> (Vec<(u64, ChunkHeader)>, Vec<(u64, u64)>) {
        assert!(Self::check_is_valid(r));
        let mut v = Vec::new();
        let mut deleted = Vec::new();

        loop {
            match Self::page_checked(r, None) {
                PageResult::Good(pd) => {
                    let len = pd.len;
                    let mut reader = LimitedReader::new(pd.w, len as usize);
                    let ch = Option::<ChunkHeader>::from_reader_and_heap(&mut reader, &[]);
                    if let Some(ch) = ch {
                        v.push((pd.pos, ch));
                    }
                    let skip = pd.nextpos;
                    r.seek(SeekFrom::Start(skip)).unwrap();
                }
                PageResult::Deleted(pd) => {
                    let skip = pd.len;
                    deleted.push((pd.pos, skip as u64));

                    let skip = pd.nextpos;
                    r.seek(SeekFrom::Start(skip)).unwrap();
                }
                PageResult::Eof => break,
            };
        }
        (v, deleted)
    }
    pub fn create_from_reader(mut w: W, constant_size: Option<u64>) -> Self {
        w.seek(SeekFrom::Start(0)).unwrap();
        let (pages, deleted) = PageSerializer::iter_pages(&mut w);
        let mut ch = ChunkHeaderIndex(BTreeMap::default());
        for p in pages {
            ch.push(p.0, p.1);
        }
        Self {
            file: w,
            previous_headers: ch,
            deleted,
            pinned: Default::default(),
            cache: Default::default(),
            constant_size,
        }
    }
    pub fn clone_headers(&self) -> ChunkHeaderIndex {
        self.previous_headers.clone()
    }
    pub fn smart_create(mut w: W) -> Self {
        if Self::check_is_valid(&mut w) {
            Self::create_from_reader(w, None)
        } else {
            Self::create(w, None)
        }
    }

    fn page_checked(mut file: &mut W, position: Option<u64>) -> PageResult<'_, W> {
        let pos = if let Some(pos) = position {
            file.seek(SeekFrom::Start(pos)).unwrap()
        } else {
            file.seek(SeekFrom::Current(0)).unwrap()
        };
        let mut u16_bytes = [0u8; 2];
        match file.read_exact(&mut u16_bytes) {
            Ok(_) => {
                let len = u32::from_le_bytes(read_to_buf(&mut file));
                let check_val = u16::from_le_bytes(u16_bytes);
                match check_val {
                    PageSerializer::<W>::WORKING_PAGE => PageResult::Good(PageData {
                        w: file,
                        pos,
                        len: len as u64,
                        nextpos: pos + 2 + 4 + len as u64,
                    }),
                    PageSerializer::<W>::DELETED_PAGE => PageResult::Deleted(PageData {
                        w: file,
                        pos,
                        len: len as u64,
                        nextpos: pos + 2 + 4 + len as u64,
                    }),
                    _ => panic!("Tried to load page incorrectly at {:?}", pos),
                }
            }
            Err(e) => {
                log::debug!(
                    "Load page wrong {:?} {:?} Total len {}",
                    position,
                    e,
                    file.stream_len().unwrap()
                );
                // println!("Load page wrong {:?} {:?}", position, e);
                PageResult::Eof
            }
        }
    }
    pub fn create(mut w: W, constant_size: Option<u64>) -> Self {
        w.seek(SeekFrom::Start(0)).unwrap();

        w.write_all(&Self::CHECK_SEQ.to_le_bytes()).unwrap();
        Self {
            deleted: Vec::new(),
            file: w,
            previous_headers: ChunkHeaderIndex::default(),
            pinned: Default::default(),
            cache: Default::default(),
            constant_size,
        }
    }
    pub fn load_page_cached(&mut self, p: u64) -> &mut TableBase2 {
        const BPOOLSIZE: usize = 5000;
        if self.cache.len() >= BPOOLSIZE {
            let mut unload_count = self.cache.len() - BPOOLSIZE;
            let keys: Vec<_> = self.cache.keys().cloned().collect();
            for k in keys {
                if unload_count == 0 {
                    break;
                }
                if !self.pinned.contains(&k) && k != p {
                    self.unload_page(k);
                    unload_count -= 1;
                }
            }
        }

        self.pinned.insert(p);

        let file = &mut self.file;
        let table = self.cache.entry(p).or_insert_with(|| {
            let page_reader = Self::file_get_page(file, p);
            let mut page = TableBase2::from_reader_and_heap(page_reader, &[]);
            page.loaded_location = Some(p);
            page
        });
        table
    }
    pub fn file_get_page(file: &mut W, position: u64) -> LimitedReader<&mut W> {
        match PageSerializer::<W>::page_checked(file, Some(position)) {
            PageResult::Good(pd) => {
                let size = pd.len;

                if size == 0 {
                    println!("Tried to load zero-sized page")
                }

                log::debug!("Yielding page {} {}", position, pd.len);
                LimitedReader::new(pd.w, pd.len as usize)
            }
            x => {
                panic!("Got page {:?}", x)
            }
        }
    }

    pub fn move_file(&mut self) -> W
    where
        W: Default,
    {
        self.unload_all();
        self.previous_headers.0.clear();
        std::mem::take(&mut self.file)
    }
    fn unload_page(&mut self, p: u64) {
        println!("Unloading page {p}");
        let mut page = self.cache.remove(&p).unwrap();
        if page.dirty {
            page.force_flush(self);
        }
    }
    pub fn unload_all(&mut self) {
        if self.file.stream_len().unwrap() == 0 {
            return;
        }
        // Not sure why we're loading all the pages and unloading them here...
        // for i in &self.previous_headers.0.clone() {
        //     self.load_page_cached(i.1.location);
        //     self.unpin_page(i.1.location);
        // }

        let keys: Vec<_> = self.cache.keys().cloned().collect();
        for i in keys {
            self.unload_page(i);
        }
        assert!(self.pinned.is_empty());
        self.file.flush().unwrap();
    }
    pub fn unpin_page(&mut self, page: u64) {
        assert!(self.pinned.remove(&page));
    }

    pub fn get_in_all_insert(&self, ty: u64, pkey: TypeData) -> Option<u64> {
        let left = self.previous_headers.get_in_one_it(ty, pkey).next_back();

        left.map(|a| a.1.location)
    }
}

impl<W: Read> Read for LimitedReader<W> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.1 < buf.len() {
            panic!();
        }
        self.1 -= buf.len();
        self.0.read(buf)
    }
}


impl<W: Write + Seek + Read> PageSerializer<W> {
    pub fn add_page(&mut self, mut buf: Vec<u8>, ch: ChunkHeader) -> u64 {
        if let Some(sz) = self.constant_size {
            assert!(buf.len() < sz as usize);
            buf.resize((sz) as usize, 0);
        }
        // Check for deleted pages
        let new_pos = {
            if self.constant_size.is_some() && !self.deleted.is_empty() {
                let pos = self.deleted.pop().unwrap().0;
                self.file.seek(SeekFrom::Start(pos)).unwrap()
            } else {
                self.file.seek(SeekFrom::End(0)).unwrap()
            }
        };
        self.file
            .write_all(&PageSerializer::<W>::WORKING_PAGE.to_le_bytes())
            .unwrap();
        self.file
            .write_all(&(self.constant_size.unwrap_or(buf.len() as u64) as u32).to_le_bytes())
            .unwrap();
        self.file.write_all(&buf).unwrap();

        self.previous_headers.push(new_pos, ch);

        new_pos
    }

    pub fn get_page(&mut self, position: u64) -> LimitedReader<&'_ mut W> {
        Self::file_get_page(&mut self.file, position)
    }

    pub fn get_in_all(&self, ty: u64, r: Option<TypeData>) -> impl DoubleEndedIterator<Item = u64> + '_ {
        let candidate_pages = self
            .previous_headers
            // TODO(hn): r::MAX, r::MIN
            .get_in_one_it(ty, r.clone().unwrap_or(TypeData::Null));

        candidate_pages.filter_map(move |x| {
            let ch = x.1;
            if r.is_some() && !ch.ch.limits.overlaps(&(r.clone().unwrap()..=r.clone().unwrap())) {
                None
            } else {
                Some(ch.location)
            }
        })
    }
}

impl<W: Read + Write + Seek> Drop for PageSerializer<W> {
    fn drop(&mut self) {
        // assert!(
        //     self.pinned.is_empty(),
        //     "Failed to unpin pages: {:?}",
        //     self.pinned
        // );
        // self.unload_all()
    }
}

#[test]
fn serializer_works() {
    use Range;

    let default_ch = ChunkHeader {
        ty: 0,
        tot_len: 0,
        type_size: 0,
        tuple_count: 0,
        heap_size: 0,
        limits: Range {
            min: Some(TypeData::Int(0)),
            max: Some(TypeData::Int(0)),
        },
        compressed_size: 0,
    };
    let mut ps = PageSerializer::default();
    ps.add_page(vec![0, 1, 2, 3, 4, 5],  default_ch.clone());
    ps.add_page(vec![5, 6, 9, 1, 2, 3],  default_ch);

    let mut f = std::mem::take(&mut ps.file);
    f.set_position(0);
    let ps1 = PageSerializer::create_from_reader(f, None);
    dbg!(&ps1.previous_headers);
}

#[test]
fn delete_works() {
    use Range;
    let mut ps = PageSerializer::default();
    let loc = ps.add_page(
        vec![1u8; 100],
        ChunkHeader {
            ty: 0,
            tot_len: 0,
            type_size: 0,
            tuple_count: 0,
            heap_size: 0,
            limits: Range {
                min: Some(TypeData::Int(3)),
                max: Some(TypeData::Int(3)),
            },
            compressed_size: 0,
        },
    );

    assert_eq!(ps.get_in_all(0, Some(TypeData::Int(3))).next(), Some(loc));
    ps.free_page(0, TypeData::Int(3));
    assert_eq!(ps.get_in_all(0, Some(TypeData::Int(3))).next(), None);

    let ps1 = PageSerializer::create_from_reader(std::mem::take(&mut ps.file), None);
    assert_eq!(ps1.get_in_all(0, Some(TypeData::Int(3))).next(), None);
}
