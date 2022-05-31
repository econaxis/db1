#![feature(is_sorted)]
#![feature(assert_matches)]
#![allow(clippy::manual_strip)]
#![allow(clippy::assertions_on_constants)]
#![allow(unused_unsafe)]
#![feature(trait_alias)]
#![feature(seek_stream_len)]
#![feature(test)]
#![feature(entry_insert)]
#![feature(write_all_vectored)]
#![allow(clippy::derive_hash_xor_eq)]
extern crate core;
extern crate rand;
extern crate test;






use std::io::{Read};


pub use range::Range;

use crate::chunk_header::slice_from_type;
use crate::db1_string::Db1String;
pub use crate::{
    bytes_serializer::BytesSerialize, bytes_serializer::FromReader, chunk_header::ChunkHeader,
    suitable_data_type::DataType, suitable_data_type::SuitableDataType,
};

mod buffer_pool;
mod bytes_serializer;
mod chunk_header;
mod compressor;
mod db1_string;
mod dynamic_tuple;
mod hash;
mod heap_writer;
mod index;
mod query_data;
mod range;
mod serializer;
mod suitable_data_type;
mod table_base;
mod table_base2;
mod table_manager;
mod table_traits;
mod tests;
mod ra_ops;
mod secondary_index;
mod typed_table;
mod named_tables;
