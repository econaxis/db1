// todo: buffer pool, compression, deletion, secondary indexes
// implement generations to facilitate editing



use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::ops::RangeBounds;

use crate::bytes_serializer::{BytesSerialize, FromReader};
use crate::chunk_header::ChunkHeader;
use crate::dbbase::DbBase;
use crate::range::Range;
use crate::suitable_data_type::SuitableDataType;

#[allow(unused)]
fn setup_logging() {
    env_logger::init();
}


// Provides higher level database API's -- automated flushing to disk, query functions for previously flushed chunks
pub struct DbManager<T: SuitableDataType, Writer: Write + Seek + Read = Cursor<Vec<u8>>> {
    db: DbBase<T>,
    previous_headers: Vec<(u64, ChunkHeader<T>)>,
    output_stream: Writer,
}



impl<T: SuitableDataType, Writer: Write + Seek + Read> DbManager<T, Writer> {
    // Maximum tuples we can hold in memory. After this amount, we empty to disk.
    const FLUSH_CUTOFF: usize = 5;

    // Constructs a DbManager instance from a DbBase and an output writer (like a file)
    pub fn new(db: DbBase<T>, writer: Writer) -> Self {
        Self { db, output_stream: writer, previous_headers: Vec::default() }
    }


    // Store tuple into the database, flushing to disk if the in-memory database exceeds FLUSH_CUTOFF
    pub fn store(&mut self, t: T) {
        self.db.store(t);

        if self.db.len() >= Self::FLUSH_CUTOFF {
            let header = self.db.get_chunk_header();
            self.previous_headers.push((self.output_stream.stream_position().unwrap(), header));
            self.db.force_flush(&mut self.output_stream);
        }
    }

    // Iterate through all the previously flushed chunk headers and look for all tuples contained in range `RB`
    pub fn get_in_all<RB: RangeBounds<u64>>(&mut self, range: RB) -> Vec<T> {
        let ok_chunks: Vec<_> = self.previous_headers.iter().filter_map(|(pos, h)|
            h.limits.overlaps(&range).then(|| pos)).collect();
        let mut vec = Vec::new();
        for pos in ok_chunks {
            self.output_stream.seek(SeekFrom::Start(*pos));
            let db = DbBase::<T>::from_reader(&mut self.output_stream);
            let range = db.key_range(&range);
            vec.extend_from_slice(range);
        };
        self.db.sort_self();
        vec.extend_from_slice(self.db.key_range(&range));

        vec.sort();
        assert_no_dups(&vec);

        vec
    }

    #[cfg(test)]
    pub fn get_output_stream_len(&mut self) -> usize {
        self.output_stream.stream_position().unwrap() as usize
    }

}

impl<T: SuitableDataType, Writer: Write + Seek + Read> Debug for DbManager<T, Writer> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbManager")
            .field("current_data", &self.db)
            .field("prev_headers", &self.previous_headers)
            .finish()
    }
}

impl<T: SuitableDataType, Writer: Write + Seek + Read + Default> Default for DbManager<T, Writer> {
    fn default() -> Self {
        let db = DbBase::default();
        Self { db, previous_headers: Vec::default(), output_stream: Writer::default() }
    }
}


// Checks that slice has no duplicate values
pub fn assert_no_dups<T: PartialOrd + Debug>(a: &[T]) -> bool {
    debug_assert!(a.is_sorted());
    for window in a.windows(2) {
        match window {
            [i, j] if i.partial_cmp(j) == Some(Ordering::Less) => {},
            _ => {
                panic!("Duplicated primary key value {:?}", window);
            }
        }
    }
    true
}