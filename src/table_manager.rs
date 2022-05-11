// todo: compression, secondary indexes

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::io::{Cursor, Read, Seek, Write};
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::option::Option::None;

use serializer::PageSerializer;
use table_base::TableBaseRangeIterator;
use FromReader;

use crate::buffer_pool::BufferPool;
use crate::suitable_data_type::SuitableDataType;
use crate::table_base::TableBase;
use crate::table_traits::BasicTable;
use crate::ChunkHeader;

#[allow(unused)]
fn setup_logging() {
    env_logger::init();
}

// Provides higher level database API's -- automated flushing to disk, query functions for previously flushed chunks
pub struct TableManager<T, Writer: Read + Write + Seek = Cursor<Vec<u8>>> {
    pub db: TableBase<T>,
    buffer_pool: BufferPool<TableBase<T>>,
    output_stream: PageSerializer<Writer>,
    result_buffer: Vec<T>,
}

pub struct ManagerFns<T, Writer> {
    phantom: PhantomData<T>,
    phantom1: PhantomData<Writer>,
}

impl<T: SuitableDataType, Writer: Write + Seek + Read> TableManager<T, Writer> {
    // Constructs a DbManager instance from a DbBase and an output writer (like a file)
    pub fn new(writer: Writer) -> Self {
        Self {
            output_stream: PageSerializer::create(writer, None),
            db: Default::default(),
            buffer_pool: Default::default(),
            result_buffer: Default::default(),
        }
    }
    pub fn serializer(&mut self) -> &mut PageSerializer<Writer> {
        &mut self.output_stream
    }

    pub fn store_and_replace(&mut self, t: T) -> Option<T> {
        ManagerFns::store_and_replace(&mut self.db, &mut self.output_stream, t)
    }

    pub fn get_one(&mut self, id: u64, load_mask: u8) -> Option<T> {
        ManagerFns::get_one(
            &mut self.db,
            &mut self.output_stream,
            &mut self.buffer_pool,
            id,
            load_mask,
        )
    }

    pub fn range_iterating<
        F: FnMut(TableBaseRangeIterator<'_, T>, &TableBase<T>) -> Result<(), String>,
        RB: RangeBounds<u64> + Clone,
    >(
        &mut self,
        id: RB,
        mut function: F,
    ) -> Result<(), String> {
        self.db.sort_self();
        function(self.db.key_range_iterator(id.clone()), &self.db)?;

        ManagerFns::range_iterating(
            &mut self.output_stream,
            &mut self.buffer_pool,
            1,
            id,
            function,
        )
    }
    pub fn get_in_all(&mut self, range: Option<u64>, load_mask: u8) -> &Vec<T> {
        self.result_buffer.clear();
        self.result_buffer = ManagerFns::get_in_all(
            &mut self.db,
            &mut self.output_stream,
            &mut self.buffer_pool,
            range,
            load_mask,
        );
        &self.result_buffer
    }
    // Constructs instance of database from a file generated previously.
    // Binary format should be consecutive array of DbBase flushes.
    pub fn read_from_file(r: Writer) -> Self {
        Self {
            db: TableBase::default(),
            buffer_pool: BufferPool::default(),
            result_buffer: Default::default(),
            output_stream: PageSerializer::create_from_reader(r, None),
        }
    }

    // pub fn compact(&mut self) {
    //     let mut seen_keys = HashSet::new();
    //     let mut storage = Vec::new();
    //     self.force_flush();
    //     for stream_pos in self.output_stream.previous_headers.get_in_all(1, &RangeFull) {
    //         let db = self.load_page(stream_pos);
    //         for j in db.key_range(RangeFull) {
    //             if seen_keys.insert(j.first()) {
    //                 storage.push(Self::process_items(db, j, u8::MAX));
    //             }
    //         }
    //         self.output_stream.free_page(stream_pos);
    //     }
    //     storage.sort_by_key(|a| a.first());
    //     println!("Got keys {:?}", storage);
    //     storage.dedup_by_key(|a| a.first());
    //     for j in storage {
    //         self.store_and_replace(j);
    //     }
    // }
    pub fn force_flush(&mut self) -> Option<(ChunkHeader, Vec<T>)> {
        ManagerFns::force_flush(&mut self.db, &mut self.output_stream)
    }
}

impl<T: SuitableDataType, Writer: Write + Seek + Read> ManagerFns<T, Writer> {
    // Maximum tuples we can hold in memory. After this amount, we empty to disk.
    #[cfg(test)]
    pub const FLUSH_CUTOFF: usize = 10;

    #[cfg(not(test))]
    pub const FLUSH_CUTOFF: usize = 2500;

    // Check if exceeded in memory capacity and write to disk
    fn check_should_flush(db: &TableBase<T>) -> bool {
        db.len() >= Self::FLUSH_CUTOFF
    }

    pub fn store_and_replace(
        db: &mut TableBase<T>,
        _os: &mut PageSerializer<Writer>,
        t: T,
    ) -> Option<T> {
        // if Self::check_should_flush(db) {
        //     Self::force_flush(db, os);
        // }
        db.store_and_replace(t)
    }

    fn process_items(db: &TableBase<T>, item: &T, mask: u8) -> T {
        let mut cloned = item.clone();
        for i in 0..7 {
            if (mask & (1 << i)) != 0 {
                cloned.resolve_item(db.heap(), i);
            }
        }
        cloned
    }

    pub fn get_one(
        db: &mut TableBase<T>,
        os: &mut PageSerializer<Writer>,
        bp: &mut BufferPool<TableBase<T>>,
        id: u64,
        load_mask: u8,
    ) -> Option<T> {
        db.sort_self();
        if let Some(j) = db.key_range(Some(id)).first() {
            let cloned = Self::process_items(db, *j, load_mask);
            return Some(cloned);
        }

        let mut loads = 0;
        let ok_chunks: Vec<_> = os.get_in_all(1, Some(id)).collect();
        for pos in ok_chunks {
            let db = Self::load_page(os, bp, pos);
            loads += 1;
            // Loading page has no effect on the db, so have to workaround the borrow checker
            if let Some(j) = db.key_range(Some(id)).first() {
                let cloned = Self::process_items(db, j, load_mask);
                println!("Found item in {}", loads);
                return Some(cloned);
            }
        }
        None
    }

    pub fn range_iterating<
        F: FnMut(TableBaseRangeIterator<'_, T>, &TableBase<T>) -> Result<(), String>,
        RB: RangeBounds<u64> + Clone,
    >(
        _os: &mut PageSerializer<Writer>,
        _bp: &mut BufferPool<TableBase<T>>,
        _ty: u64,
        _range: RB,
        _function: F,
    ) -> Result<(), String> {
        panic!()
        // let mut ok_chunks = os.get_in_all(ty, range.clone());
        // ok_chunks.reverse();
        // for pos in ok_chunks {
        //     let db = Self::load_page(os, bp, pos);
        //     function(db.key_range_iterator(range.clone()), db)?;
        // }
        // Ok(())
    }
    pub fn get_in_all(
        db: &mut TableBase<T>,
        output_stream: &mut PageSerializer<Writer>,
        bp: &mut BufferPool<TableBase<T>>,
        range: Option<u64>,
        load_mask: u8,
    ) -> Vec<T> {
        let ok_chunks: Vec<_> = output_stream.get_in_all(1, range).collect();
        let mut cln = HashMap::new();
        for pos in ok_chunks {
            let db = Self::load_page(output_stream, bp, pos);
            // Loading page has no effect on the db, so have to workaround the borrow checker
            for j in db.key_range(range) {
                let cloned = Self::process_items(db, j, load_mask);
                cln.insert(cloned.first(), cloned);
            }
        }
        // Now search in the current portion
        db.sort_self();
        for j in db.key_range(range) {
            let cloned = Self::process_items(db, j, load_mask);
            cln.insert(cloned.first(), cloned);
        }
        let mut result_buffer: Vec<T> = cln.into_values().collect();
        result_buffer.sort_by_key(T::first);
        result_buffer
    }

    fn load_page<'a>(
        ps: &mut PageSerializer<Writer>,
        bp: &'a mut BufferPool<TableBase<T>>,
        page_loc: u64,
    ) -> &'a mut TableBase<T> {
        let loader = move || {
            let page = ps.get_page(page_loc);
            TableBase::from_reader_and_heap(page, &[])
        };

        bp.load_page(page_loc, loader)
    }

    // pub fn compact(&mut self) {
    //     let mut seen_keys = HashSet::new();
    //     let mut storage = Vec::new();
    //     self.force_flush();
    //     for stream_pos in self.output_stream.previous_headers.get_in_all(1, &RangeFull) {
    //         let db = self.load_page(stream_pos);
    //         for j in db.key_range(RangeFull) {
    //             if seen_keys.insert(j.first()) {
    //                 storage.push(Self::process_items(db, j, u8::MAX));
    //             }
    //         }
    //         self.output_stream.free_page(stream_pos);
    //     }
    //     storage.sort_by_key(|a| a.first());
    //     println!("Got keys {:?}", storage);
    //     storage.dedup_by_key(|a| a.first());
    //     for j in storage {
    //         self.store_and_replace(j);
    //     }
    // }
    pub fn force_flush(
        db: &mut TableBase<T>,
        output_stream: &mut PageSerializer<Writer>,
    ) -> Option<(ChunkHeader, Vec<T>)> {
        if db.len() == 0 {
            log::debug!("Not flushing because len 0");
            return None;
        }
        let mut writer: Cursor<Vec<u8>> = Cursor::default();
        let mut db = std::mem::take(db);
        let (header, rest_data) = db.force_flush(&mut writer);
        writer.set_position(0);
        let stream_len = writer.stream_len().unwrap();
        let stream_pos = output_stream.add_page(writer.into_inner(), stream_len, header.clone());
        log::debug!("Flushed to {} {}", stream_pos, stream_len);
        Some((header, rest_data))
    }
}

impl<T: SuitableDataType, Writer: Write + Seek + Read> Debug for TableManager<T, Writer> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbManager")
            .field("current_data", &self.db)
            // .field("output_stream", &self.output_stream)
            .finish()
    }
}

impl<T: SuitableDataType> Default for TableManager<T> {
    fn default() -> Self {
        let db = TableBase::default();
        Self {
            db,
            output_stream: PageSerializer::create(Cursor::new(Vec::new()), None),
            buffer_pool: Default::default(),
            result_buffer: Default::default(),
        }
    }
}
