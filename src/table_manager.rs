// todo: compression, secondary indexes, heap



use std::cmp::Ordering;
use std::collections::{BTreeSet};
use std::fmt::{Debug, Formatter};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};

use std::ops::RangeBounds;

use crate::bytes_serializer::{FromReader};
use crate::chunk_header::ChunkHeader;
use crate::table_base::TableBase;

use crate::suitable_data_type::SuitableDataType;
use crate::buffer_pool::BufferPool;

#[allow(unused)]
fn setup_logging() {
    env_logger::init();
}


// Provides higher level database API's -- automated flushing to disk, query functions for previously flushed chunks
pub struct TableManager<T: SuitableDataType, Writer: Write + Seek + Read = Cursor<Vec<u8>>> {
    db: TableBase<T>,
    previous_headers: Vec<(u64, ChunkHeader<T>)>,
    buffer_pool: BufferPool<T>,
    output_stream: Writer,
}

impl<T: SuitableDataType, Writer: Write + Seek + Read> TableManager<T, Writer> {
    // Maximum tuples we can hold in memory. After this amount, we empty to disk.
    const FLUSH_CUTOFF: usize = 5;

    // Constructs a DbManager instance from a DbBase and an output writer (like a file)
    pub fn new(writer: Writer) -> Self {
        Self { output_stream: writer, previous_headers: Vec::default(), db: Default::default(), buffer_pool: Default::default() }
    }

    fn check_should_flush(&mut self) {
        if self.db.len() >= Self::FLUSH_CUTOFF {
            let stream_pos = self.output_stream.stream_position().unwrap();
            let (header, _) = self.db.force_flush(&mut self.output_stream);
            self.previous_headers.push((stream_pos, header));
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

    // Filter all chunk headers that can possibly satisfy range, and return their locations in the stream
    fn chunks_in_range<RB: RangeBounds<u64>>(headers: &[(u64, ChunkHeader<T>)], range: &RB) -> Vec<u64> {
        headers.iter().filter_map(|(pos, h)|
            h.limits.overlaps(range).then(|| *pos)).collect()
    }

    fn load_page(&mut self, page_loc: u64) -> &mut TableBase<T> {
        let loader = || {
            self.output_stream.seek(SeekFrom::Start(page_loc)).unwrap();
            TableBase::<T>::from_reader_and_heap(&mut self.output_stream, &[])
        };

        self.buffer_pool.load_page(page_loc, loader)
    }
    // Iterate through all the previously flushed chunk headers and look for all tuples contained in range `RB`
    pub fn get_in_all<RB: RangeBounds<u64>>(&mut self, range: RB) -> Vec<T> {
        let ok_chunks = Self::chunks_in_range(&self.previous_headers, &range);
        let mut cln = BTreeSet::new();
        for pos in ok_chunks {
            let db = self.load_page(pos);
            let range = db.key_range(&range);

            for j in range {
                cln.replace(j.clone());
            }
        };
        self.db.sort_self();

        for j in self.db.key_range(&range) {
            cln.replace(j.clone());
        }

        cln.into_iter().collect()
    }

    #[cfg(test)]
    pub fn get_output_stream_len(&mut self) -> usize {
        self.output_stream.stream_position().unwrap() as usize
    }
    #[cfg(test)]
    pub fn force_flush(&mut self) {
        let stream_pos = self.output_stream.stream_position().unwrap();
        let (header, _) = self.db.force_flush(&mut self.output_stream);
        self.previous_headers.push((stream_pos, header));
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
        Self { db, output_stream: Cursor::new(Vec::new()), buffer_pool: Default::default(), previous_headers: Default::default() }
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