use std::io::Write;
use std::ops::RangeBounds;
use table_base::TableBaseRangeIterator;

use crate::{ChunkHeader, FromReader, SuitableDataType};

pub trait BasicTable<T: SuitableDataType>: FromReader + Default {
    fn heap(&self) -> &[u8];
    fn len(&self) -> usize;
    // Sort by primary key
    fn sort_self(&mut self);
    // Store tuple into self
    fn store_and_replace(&mut self, t: T) -> Option<T>;
    fn force_flush<W: Write>(&mut self, w: W) -> (ChunkHeader, Vec<T>);

    fn key_range(&self, range: Option<u64>) -> Vec<&T>;
    fn key_range_iterator<RB: RangeBounds<u64>>(&self, range: RB) -> TableBaseRangeIterator<'_, T>;
}
