// todo: compression, secondary indexes

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::ops::{RangeBounds, RangeFull};

use serializer::{DbPageManager, PageSerializer};

use crate::buffer_pool::BufferPool;
use crate::bytes_serializer::FromReader;
use crate::chunk_header::ChunkHeaderIndex;
use crate::ChunkHeader;
use crate::heap_writer::default_mem_writer;
use crate::suitable_data_type::SuitableDataType;
use crate::table_base::TableBase;
use crate::table_traits::BasicTable;

#[allow(unused)]
fn setup_logging() {
    env_logger::init();
}

// Provides higher level database API's -- automated flushing to disk, query functions for previously flushed chunks
pub struct TableManager<T, TableT = TableBase<T>, Writer = Cursor<Vec<u8>>> {
    db: TableT,
    buffer_pool: BufferPool<TableT>,
    pub output_stream: PageSerializer<Writer>,
    result_buffer: Vec<T>,
}


impl<T: SuitableDataType, TableT: BasicTable<T>, Writer: Write + Seek + Read> TableManager<T, TableT, Writer> {
    // Maximum tuples we can hold in memory. After this amount, we empty to disk.
    #[cfg(test)]
    pub const FLUSH_CUTOFF: usize = 10;

    #[cfg(not(test))]
    pub const FLUSH_CUTOFF: usize = 2500;
    // Constructs a DbManager instance from a DbBase and an output writer (like a file)
    pub fn new(writer: Writer) -> Self {
        Self {
            output_stream: PageSerializer::create(writer),
            db: Default::default(),
            buffer_pool: Default::default(),
            result_buffer: Default::default(),
        }
    }

    pub fn inner_stream(self) -> Writer {
        self.output_stream.file
    }

    // Check if exceeded in memory capacity and write to disk
    fn check_should_flush(&mut self) {
        if self.db.len() >= Self::FLUSH_CUTOFF {
            self.force_flush();
        }
    }

    // Store tuple into the database, flushing to disk if the in-memory database exceeds FLUSH_CUTOFF
    pub fn store(&mut self, t: T) {
        self.db.store(t);
        self.check_should_flush();
    }

    pub fn store_and_replace(&mut self, t: T) -> Option<T> {
        let val = self.db.store_and_replace(t);
        self.check_should_flush();
        val
    }
    pub fn get_output_buffer_first(&mut self) -> T {
        self.result_buffer.remove(0)
    }


    fn process_items(db: &TableT, item: &T, mask: u8) -> T {
        let mut cloned = item.clone();
        for i in 0..7 {
            if (mask & (1 << i)) != 0 {
                cloned.resolve_item(db.heap(), i);
            }
        }
        cloned
    }

    pub fn get_one(&mut self, id: u64, load_mask: u8) -> Option<T> {
        self.db.sort_self();
        if let Some(j) = self.db.key_range(id..=id).first() {
            let cloned = Self::process_items(&self.db, *j, load_mask);
            return Some(cloned);
        }

        let mut loads = 0;
        let mut ok_chunks = self.output_stream.previous_headers.get_in_all(1, &(id..=id));
        ok_chunks.reverse();
        for pos in ok_chunks {
            let db = self.load_page(pos);
            loads += 1;
            // Loading page has no effect on the db, so have to workaround the borrow checker
            if let Some(j) = db.key_range(id..=id).first() {
                let cloned = Self::process_items(db, j, load_mask);
                println!("Found item in {}", loads);
                return Some(cloned);
            }
        }
        None
    }
    pub fn get_in_all<RB: RangeBounds<u64> + Clone>(&mut self, range: RB, load_mask: u8) -> &mut Vec<T>
    {
        self.result_buffer.clear();
        let ok_chunks = self.output_stream.previous_headers.get_in_all(1, &range);
        let mut cln = HashMap::new();
        for pos in ok_chunks {
            let db = self.load_page(pos);
            // Loading page has no effect on the db, so have to workaround the borrow checker
            for j in db.key_range(range.clone()) {
                let cloned = Self::process_items(db, j, load_mask);
                cln.insert(cloned.first(), cloned);
            }
        }
        // Now search in the current portion
        self.db.sort_self();
        for j in self.db.key_range(range) {
            let cloned = Self::process_items(&self.db, j, load_mask);
            cln.insert(cloned.first(), cloned);
        }
        self.result_buffer = cln.into_values().collect();
        self.result_buffer.sort_by_key(T::first);
        &mut self.result_buffer
    }

    fn load_page(&mut self, page_loc: u64) -> &mut TableT {
        let output_stream = &mut self.output_stream;
        let loader = move || {
            let page = output_stream.get_page(page_loc);
            TableT::from_reader_and_heap(page, &[])
        };

        self.buffer_pool.load_page(page_loc, loader)
    }

    // Constructs instance of database from a file generated previously.
    // Binary format should be consecutive array of DbBase flushes.
    pub fn read_from_file(r: Writer) -> Self {
        Self {
            db: TableT::default(),
            buffer_pool: BufferPool::default(),
            result_buffer: Default::default(),
            output_stream: PageSerializer::create_from_reader(r),
        }
    }

    pub fn serializer(&mut self) -> &mut PageSerializer<Writer> {
        &mut self.output_stream
    }


    pub fn compact(&mut self) {
        let mut seen_keys = HashSet::new();
        let mut storage = Vec::new();
        self.force_flush();
        for stream_pos in self.output_stream.previous_headers.get_in_all(1, &RangeFull) {
            let db = self.load_page(stream_pos);
            for j in db.key_range(RangeFull) {
                if seen_keys.insert(j.first()) {
                    storage.push(Self::process_items(db, j, u8::MAX));
                }
            }
            self.output_stream.free_page(stream_pos);
        }
        storage.sort_by_key(|a| a.first());
        println!("Got keys {:?}", storage);
        storage.dedup_by_key(|a| a.first());
        for j in storage {
            self.store(j);
        }
    }
    pub fn force_flush(&mut self) -> Option<(ChunkHeader, Vec<T>)> {
        if self.db.len() == 0 {
            log::debug!("Not flushing because len 0");
            return None;
        }
        let mut writer: Cursor<Vec<u8>> = Cursor::default();
        let mut db = std::mem::take(&mut self.db);
        let (header, rest_data) = db.force_flush(&mut writer);
        writer.set_position(0);
        let stream_len = writer.stream_len().unwrap();
        let stream_pos = self.output_stream.add_page(writer, stream_len, header.clone());
        log::debug!("Flushed to {} {}", stream_pos, stream_len);
        Some((header, rest_data))
    }
}

impl<T: SuitableDataType, TableT, Writer: Write + Seek + Read> Debug for TableManager<T, TableT, Writer> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbManager")
            // .field("current_data", &self.db)
            .field("prev_headers", &self.output_stream.previous_headers)
            .finish()
    }
}

impl<T: SuitableDataType> Default for TableManager<T, TableBase<T>> {
    fn default() -> Self {
        let db = TableBase::default();
        Self {
            db,
            output_stream: PageSerializer::create(Cursor::new(Vec::new())),
            buffer_pool: Default::default(),
            result_buffer: Default::default(),
        }
    }
}

// Checks that slice has no duplicate values
pub fn assert_no_dups<T: PartialOrd + Debug>(a: &[T]) -> bool {
    debug_assert!(a.is_sorted());
    for window in a.windows(2) {
        match window {
            [i, j] if i.partial_cmp(j) == Some(Ordering::Less) => {}
            _ => {
                panic!("Duplicated primary key value {:?}", window);
            }
        }
    }
    true
}

