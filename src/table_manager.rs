// todo: compression, secondary indexes

use std::cmp::Ordering;
use std::collections::{BTreeSet};
use std::fmt::{Debug, Formatter};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::ops::RangeBounds;

use crate::buffer_pool::BufferPool;
use crate::bytes_serializer::FromReader;
use crate::chunk_header::ChunkHeaderIndex;
use crate::ChunkHeader;
use crate::heap_writer::default_mem_writer;
use crate::suitable_data_type::{QueryableDataType, SuitableDataType};
use crate::table_base::TableBase;

#[allow(unused)]
fn setup_logging() {
    env_logger::init();
}

// Provides higher level database API's -- automated flushing to disk, query functions for previously flushed chunks
pub struct TableManager<T: SuitableDataType, Writer: Write + Seek + Read = Cursor<Vec<u8>>> {
    db: TableBase<T>,
    previous_headers: ChunkHeaderIndex<T>,
    buffer_pool: BufferPool<T>,
    output_stream: Writer,
}


impl<T: SuitableDataType, Writer: Write + Seek + Read> TableManager<T, Writer> {
    // Maximum tuples we can hold in memory. After this amount, we empty to disk.
    #[cfg(test)]
    pub const FLUSH_CUTOFF: usize = 10;

    #[cfg(not(test))]
    pub const FLUSH_CUTOFF: usize = 250;
}



impl<T: SuitableDataType, Writer: Write + Seek + Read> TableManager<T, Writer> {
    // Constructs a DbManager instance from a DbBase and an output writer (like a file)
    pub fn new(writer: Writer) -> Self {
        Self {
            output_stream: writer,
            previous_headers: Default::default(),
            db: Default::default(),
            buffer_pool: Default::default(),
        }
    }

    // Check if exceeded in memory capacity and write to disk
    fn check_should_flush(&mut self) {
        if self.db.len() >= Self::FLUSH_CUTOFF {
            println!("Flushing {} tuples", self.db.len());
            let stream_pos = self.output_stream.stream_position().unwrap();
            let db = std::mem::take(&mut self.db);
            let (header, _) = db.force_flush(&mut self.output_stream);
            self.previous_headers.push(stream_pos, header);
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
    pub fn get_in_all<RB: RangeBounds<u64>>(&mut self, range: RB) -> Vec<&T>
        where
            T: QueryableDataType,
    {
        let ok_chunks = self.previous_headers.get_in_all(&range);
        let mut cln = BTreeSet::new();
        self.buffer_pool.freeze();
        for pos in ok_chunks {
            let db = self.load_page(pos);
            // Loading page has no effect on the db, so have to workaround the borrow checker
            let db = unsafe { &mut *db };
            for j in db.key_range(&range) {
                cln.replace(j);
            }
        }
        self.buffer_pool.unfreeze();


        // Now search in the current portion
        self.db.sort_self();
        for j in self.db.key_range(&range) {
            cln.replace(j);
        }
        cln.into_iter().collect()
    }

    fn load_page(&mut self, page_loc: u64) -> *mut TableBase<T> {
        let loader = || {
            self.output_stream.seek(SeekFrom::Start(page_loc)).unwrap();
            TableBase::<T>::from_reader_and_heap(&mut self.output_stream, &[])
        };

        self.buffer_pool.load_page(page_loc, loader)
    }

    // Constructs instance of database from a file generated previously.
    // Binary format should be consecutive array of DbBase flushes.
    pub fn read_from_file<R: Read>(r: R, output_stream: Writer) -> Self {
        Self {
            previous_headers: ChunkHeaderIndex::from_reader_and_heap(r, &[]),
            db: TableBase::default(),
            buffer_pool: BufferPool::default(),
            output_stream,
        }
    }

    #[cfg(test)]
    pub fn get_prev_headers(&self) -> &ChunkHeaderIndex<T> {
        &self.previous_headers
    }
    #[cfg(test)]
    pub fn get_output_stream_len(&mut self) -> usize {
        self.output_stream.stream_position().unwrap() as usize
    }
    #[cfg(test)]
    pub fn get_data(&self) -> &Vec<T> {
        self.db.get_data()
    }

    pub fn force_flush(&mut self) -> Option<(ChunkHeader<T>, Vec<T>)> {
        if self.db.len() == 0 {
            return None;
        }

        let stream_pos = self.output_stream.stream_position().unwrap();
        let db = std::mem::take(&mut self.db);
        let (header, res) = db.force_flush(&mut self.output_stream);
        self.previous_headers.push(stream_pos, header.clone());
        println!("Flushed to {}", stream_pos);
        Some((header, res))
    }
}

impl<T: SuitableDataType, Writer: Write + Seek + Read> Debug for TableManager<T, Writer> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbManager")
            .field("current_data", &self.db)
            .field("prev_headers", &self.previous_headers)
            .finish()
    }
}

impl<T: SuitableDataType> Default for TableManager<T> {
    fn default() -> Self {
        let db = TableBase::default();
        Self {
            db,
            output_stream: default_mem_writer(),
            buffer_pool: Default::default(),
            previous_headers: Default::default(),
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


#[test]
fn test_empty_file() {
    use crate::DataType;
    let empty: &[u8] = &[];
    dbg!(TableManager::<DataType>::read_from_file(empty, Cursor::new(Vec::new())));
}