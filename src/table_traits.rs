use std::any::TypeId;
use std::io::{Write};
use std::ops::RangeBounds;

use crate::{ChunkHeader, FromReader,  SuitableDataType};

pub trait BasicTable<T: SuitableDataType>: FromReader + Default {
    fn heap(&self) -> &[u8];
    fn len(&self) -> usize;
    // Sort by primary key
    fn sort_self(&mut self);
    // Store tuple into self
    fn store(&mut self, t: T);
    fn store_and_replace(&mut self, t: T) -> Option<T>;
    fn force_flush<W: Write>(&mut self, w: W) -> (ChunkHeader, Vec<T>);

    fn key_range<RB: RangeBounds<u64>>(&self, range: RB) -> Vec<&T>;
}
