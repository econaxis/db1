use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::convert::TryInto;
use std::fmt::{Debug, Formatter};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::option::Option::None;

use {ChunkHeader, Range};
use {FromReader, SuitableDataType};
use BytesSerialize;
use db1_string::Db1String;
use dynamic_tuple::{DynamicTuple, DynamicTupleInstance, TupleBuilder};
use dynamic_tuple::RWS;
use hash::InvalidWriter;
use serializer::{PageSerializer};
use table_base::read_to_buf;

pub struct TableBase2 {
    pub ty: u64,
    data: Vec<u8>,
    pub limits: Range<u64>,
    type_size: usize,
    heap: Heap,
    pub dirty: bool,
    pub loaded_location: Option<u64>,
}

pub struct Heap(Cursor<Vec<u8>>, BinaryHeap<(u32, u32)>);

impl Heap {
    fn new(mut p0: Cursor<Vec<u8>>, p1: BinaryHeap<(u32, u32)>) -> Heap {
        p0.seek(SeekFrom::End(0)).unwrap();
        Heap(p0, p1)
    }
}

impl Default for Heap {
    fn default() -> Self {
        Self(Cursor::new(Vec::with_capacity(16000)), Default::default())
    }
}
impl Write for &mut Heap {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

impl Heap {
    pub fn as_slice(&self) -> &[u8] {
        self.0.get_ref()
    }
    pub fn free(&mut self, loc: u64, len: u64) {
        self.1.push((loc as u32, len as u32));
    }

    #[allow(unused)]
    fn vacuum(&mut self) {
        let mut new_len = self.0.get_ref().len();
        while let Some(a) = self.1.pop() {
            if a.0 + a.1 == new_len as u32 {
                new_len = a.0 as usize;
            }
        }
        self.0.get_mut().resize(new_len, 0);
        self.0.set_position(new_len as u64);
    }
    pub fn len(&self) -> u64 {
        self.0.get_ref().len() as u64
    }
}

impl Debug for TableBase2 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableBase2")
            .field("ty", &self.ty)
            .field("data", &self.data.len())
            .field("type_size", &self.type_size)
            .field("heap", &self.heap.len())
            .field("limits", &self.limits)
            .finish()
    }
}


/*
Named table supports functions:

insert tuple
get tuple from all

by having access to a buffer pool and a page serializer.

Insert  - seek out all the pages with specific ID, matching specific range, corresponding to a named table.
        - get the first page, load it into memory, and insert a tuple into that page.
        - keep it in the buffer pool.

Get from all     - seek out all pages with specific ID, matching range


 */
impl TableBase2 {
    const TABLEBASE2: u64 = 0xf6c4f2fcf200310e;
    pub fn new(ty: u64, type_size: usize) -> Self {
        Self {
            ty,
            data: Vec::with_capacity(16000),
            limits: Default::default(),
            type_size,
            heap: Default::default(),
            dirty: true,
            loaded_location: None,
        }
    }
    pub fn heap_mut(&mut self) -> &mut Cursor<Vec<u8>> {
        &mut self.heap.0
    }
    pub fn heap(&self) -> &Cursor<Vec<u8>> {
        &self.heap.0
    }

    pub fn chunk_header(&self) -> ChunkHeader {
        ChunkHeader {
            ty: self.ty,
            tot_len: (self.data.len() + self.heap.len() as usize) as u32,
            type_size: self.type_size as u32,
            tuple_count: 0,
            heap_size: self.heap.len() as u32,
            limits: self.limits.clone(),
            compressed_size: 0,
        }
    }
    pub fn load_pkey(&self, ind: usize) -> u64 {
        u64::from_le_bytes(self.data[ind..ind + 8].try_into().unwrap())
    }
    pub fn load_value(&self, ind: usize) -> &[u8] {
        &self.data[ind..ind + self.type_size]
    }
    pub fn load_index(&self, ind: usize) -> &[u8] {
        self.load_value(ind * self.type_size)
    }
    pub fn len(&self) -> u64 {
        (self.data.len() / self.type_size) as u64
    }
    pub fn serialized_len(&self) -> usize {
        self.data.len() + self.heap.len() as usize + ChunkHeader::MAXTYPESIZE as usize + std::mem::size_of_val(&Self::TABLEBASE2)
    }
    pub fn heap_size(&self) -> u64 {
        self.heap.len()
    }
    pub fn binary_search(&self, a: u64) -> Option<u64> {
        let len = self.len();
        let mut left = 0;
        let mut right = len;
        loop {
            let middle = (left + right) / 2;
            if middle >= len {
                return Some(middle);
            }
            match self.load_pkey(middle as usize * self.type_size).cmp(&a) {
                Ordering::Less => {
                    left = middle;
                }
                Ordering::Equal => {
                    right = middle;
                }
                Ordering::Greater => {
                    right = middle;
                }
            }
            if (left as i64 - right as i64).abs() <= 1 {
                return if self.load_pkey(left as usize * self.type_size) >= a {
                    Some(left)
                } else {
                    Some(right)
                };
            }
        }
    }
    pub fn insert(&mut self, t: DynamicTupleInstance) {
        assert_eq!(t.len, self.type_size);
        self.dirty = true;
        let position = self
            .binary_search(t.first())
            .unwrap_or((self.data.len() / self.type_size) as u64) as usize * self.type_size;
        let len = self.data.len();
        if self.data.capacity() < len + self.type_size {
            self.data.reserve(len + 200 * self.type_size);
        }
        self.data.resize(len + self.type_size, 0);

        self.data
            .copy_within(position..len, position + self.type_size);
        self.data[position..position + self.type_size].copy_from_slice(&t.data[0..self.type_size]);

        self.limits.add(t.first());
    }
    pub fn assert_sorted(&self) -> Vec<u64> {
        assert!((0..self.len() as usize).map(|i| self.load_pkey(i * self.type_size)).is_sorted());
        (0..self.len() as usize).map(|i| self.load_pkey(i * self.type_size)).collect()
    }

    pub fn insert_tb(&mut self, tb: TupleBuilder) {
        let inst = tb.build(self.heap_mut());
        self.insert(inst);
    }

    fn find_split_point(&self, mut v: usize) -> usize {
        assert!(v >= 1);
        while v < self.len() as usize && self.load_pkey(v * self.type_size) == self.load_pkey((v - 1) * self.type_size) {
            v += 1;
        };
        if v == self.len() as usize {
            panic!("Too many tuples of similar length")
        }
        v
    }

    pub fn split(&mut self, splitter: &DynamicTuple) -> Option<Self> {
        assert!(self.len() >= 2);
        debug_assert!(self.assert_sorted().is_empty() || true);
        // Split exactly at middle
        let middle = self.find_split_point(self.len() as usize / 2) * self.type_size;

        let mut new_heap = Heap::default();
        let mut new_heap1 = Heap::default();
        let mut new_range = Range::new(None, None);
        let mut new_range1 = Range::new(None, None);
        for i in (0..self.data.len()).step_by(self.type_size) {
            let (used_heap, used_range) = if i >= middle {
                (&mut new_heap1, &mut new_range1)
            } else {
                (&mut new_heap, &mut new_range)
            };

            let tuple =
                splitter.read_tuple(&self.data[i..i + self.type_size], u64::MAX, self.heap.0.get_mut());
            let new_tuple = tuple.build(&mut used_heap.0);

            assert_eq!(new_tuple.len, self.type_size);
            self.data[i..i + self.type_size].copy_from_slice(&new_tuple.data[0..self.type_size]);
            used_range.add(self.load_pkey(i));
        }


        self.heap = new_heap;
        self.limits = new_range;

        let mut new_data = vec![0u8; self.data.len() - middle];
        new_data.copy_from_slice(&self.data[middle..]);

        self.dirty = true;
        self.data.resize(middle, 0);

        Some(Self {
            ty: self.ty,
            data: new_data,
            limits: new_range1,
            type_size: self.type_size,
            heap: new_heap1,
            dirty: true,
            loaded_location: None,
        })
    }


    pub fn force_flush<W: Write + Read + Seek>(&mut self, ps: &mut PageSerializer<W>) -> u64 {
        println!("Forcing flush");
        if std::thread::panicking() {
            return 0;
        }

        if self.loaded_location.is_some() {
            ps.free_page(self.ty, self.limits.min.unwrap());
        }


        let mut buf: Cursor<Vec<u8>> = Cursor::default();
        let ch = self.chunk_header();
        ch.serialize_with_heap(&mut buf, InvalidWriter);

        buf.write_all(&self.data).unwrap();
        buf.write_all(self.heap.0.get_ref()).unwrap();
        buf.write_all(&(Self::TABLEBASE2).to_le_bytes()).unwrap();

        let buf = buf.into_inner();
        let len = buf.len();

        let new_pos = ps.add_page(buf, len as u64, ch);
        println!("Loaded loc: {}; Len: {}", new_pos, len);
        self.loaded_location = Some(new_pos);
        self.dirty = false;
        new_pos
    }
    pub fn search_value(&self, value: u64) -> Vec<&[u8]> {
        let mut ans = Vec::new();
        if let Some(mut location) = self.binary_search(value) {
            loop {
                let index = location as usize * self.type_size;
                if index + 8 >= self.data.len() || self.load_pkey(index) != value {
                    break;
                }
                ans.push(&self.data[index..index + self.type_size]);
                location += 1;
            }
            assert!(self.limits.overlaps(&(value..=value)) || ans.is_empty());
            ans
        } else {
            vec![]
        }
    }
}

impl FromReader for TableBase2 {
    fn from_reader_and_heap<R: Read>(mut r: R, _heap: &[u8]) -> Self {
        let ch = ChunkHeader::from_reader_and_heap(&mut r, &[]);

        let data_size = ch.tot_len - ch.heap_size;
        let heap_size = ch.heap_size;

        let mut data = vec![0u8; data_size as usize];
        let mut heap = vec![0u8; heap_size as usize];

        // why is this even here??
        data.reserve(data.len().saturating_sub(16000));
        heap.reserve(heap.len().saturating_sub(16000));

        r.read_exact(&mut data).unwrap();
        r.read_exact(&mut heap).unwrap();

        assert_eq!(u64::from_le_bytes(read_to_buf(&mut r)), Self::TABLEBASE2);

        Self {
            ty: ch.ty,
            data,
            limits: ch.limits,
            type_size: ch.type_size as usize,
            heap: Heap::new(Cursor::new(heap), Default::default()),
            dirty: false,
            loaded_location: None,
        }
    }
}

#[test]
fn works() {
    use ::dynamic_tuple::Type;
    let mut db = TableBase2::new(19, (Db1String::TYPE_SIZE * 2 + 8) as usize);
    let mut ps = PageSerializer::create(Cursor::new(Vec::new()), None);


    let v: Vec<u64> = (0..1000).map(|a| (a * (a + 1000)) % 30).collect();
    for i in &v {
        let tup = TupleBuilder::default()
            .add_int(*i)
            .add_string("hello")
            .add_string("world");
        let inst = tup.build(&mut db.heap.0);
        db.insert(inst);
    }

    db.force_flush(&mut ps);

    let page = ps.get_in_all(19, None).unwrap();
    let page = ps.get_page(page);

    let db1 = TableBase2::from_reader_and_heap(page, &[]);

    let index = db1.binary_search(v[30]).unwrap() * db1.type_size as u64;
    println!("Loading {}", index);
    let tup = db1.load_value(index as usize);

    let dyntuple = DynamicTuple::new(vec![Type::Int, Type::String, Type::String]);
    let tup = dyntuple.read_tuple(tup, 0, db1.heap.0.get_ref());
    dbg!(&tup);
    assert_eq!(tup.extract_int(0), v[30]);
    assert_eq!(tup.extract_string(1), b"hello");
    assert_eq!(tup.extract_string(2), b"world");

    let mut split_db = db.split(&dyntuple).unwrap();
    println!("Split result {:?} {:?}", db, split_db);


    dbg!(dyntuple.read_tuple(
        db.search_value((5 * 1005) % 30).first().unwrap(),
        0,
        db.heap.0.get_ref(),
    ));
    dbg!(dyntuple.read_tuple(
        db.search_value(*v.iter().min().unwrap()).first().unwrap(),
        0,
        split_db.heap.0.get_ref(),
    ));

    db.force_flush(&mut ps);
    split_db.force_flush(&mut ps);

    let mut f = std::mem::take(&mut ps.file);
    f.set_position(0);

    let ps1 = PageSerializer::create_from_reader(f, None);
    assert!(ps1.get_in_all(19, None).is_some());
}


#[test]
fn bp_works() {
    let mut ps = PageSerializer::default();

    for _ in 0..100 {
        let mut table = TableBase2::new(1, Db1String::TYPE_SIZE as usize * 2 + 8);
        for i in 0..40 {
            let ty = TupleBuilder::default()
                .add_int(i)
                .add_string("hi")
                .add_string("world");
            let tup = ty.build(&mut table.heap_mut());
            table.insert(tup)
        }
        table.force_flush(&mut ps);
    }

    let file = std::mem::take(&mut ps.file);
    let ps = PageSerializer::create_from_reader(file, None);
    dbg!(&ps.clone_headers());
}
