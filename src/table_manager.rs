// todo: compression, secondary indexes

use std::cmp::Ordering;
use std::collections::{BTreeSet};
use std::fmt::{Debug, Formatter};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};

use std::ops::RangeBounds;

use crate::bytes_serializer::{FromReader};
use crate::chunk_header::{ChunkHeaderIndex};
use crate::table_base::TableBase;

use crate::suitable_data_type::{SuitableDataType, QueryableDataType};
use crate::buffer_pool::BufferPool;
use crate::heap_writer::default_mem_writer;

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
    const FLUSH_CUTOFF: usize = 50;

    // Constructs a DbManager instance from a DbBase and an output writer (like a file)
    pub fn new(writer: Writer) -> Self {
        Self { output_stream: writer, previous_headers: Default::default(), db: Default::default(), buffer_pool: Default::default() }
    }


    // Check if exceeded in memory capacity and write to disk
    fn check_should_flush(&mut self) {
        if self.db.len() >= Self::FLUSH_CUTOFF {
            let stream_pos = self.output_stream.stream_position().unwrap();
            let (header, _) = self.db.force_flush(&mut self.output_stream);
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
    pub fn get_in_all<RB: RangeBounds<u64>>(&mut self, range: RB) -> Vec<T> where T: QueryableDataType {
        let ok_chunks = self.previous_headers.get_in_all(&range);
        let mut cln = BTreeSet::new();
        for pos in ok_chunks {
            let db = self.load_page(pos);
            for j in db.key_range(&range) {
                cln.replace(j.clone());
            }
        };
        self.db.sort_self();

        for j in self.db.key_range(&range) {
            cln.replace(j.clone());
        }

        cln.into_iter().collect()
    }

    fn load_page(&mut self, page_loc: u64) -> &mut TableBase<T> {
        let loader = || {
            self.output_stream.seek(SeekFrom::Start(page_loc)).unwrap();
            TableBase::<T>::from_reader_and_heap(&mut self.output_stream, &[])
        };

        self.buffer_pool.load_page(page_loc, loader)
    }

    // Constructs instance of database from a file generated previously.
    // Binary format should be consecutive array of DbBase flushes.
    pub fn read_from_file<R: Read>(r: R, output_stream: Writer) -> Self {
        Self { previous_headers: ChunkHeaderIndex::from_reader_and_heap(r, &[]), db: TableBase::default(), buffer_pool: BufferPool::default(), output_stream }
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
    pub fn force_flush(&mut self) {
        let stream_pos = self.output_stream.stream_position().unwrap();
        let (header, _) = self.db.force_flush(&mut self.output_stream);
        self.previous_headers.push(stream_pos, header);
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
        Self { db, output_stream: default_mem_writer(), buffer_pool: Default::default(), previous_headers: Default::default() }
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