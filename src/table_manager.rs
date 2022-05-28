// todo: compression, secondary indexes

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::io::{Cursor, Read, Seek, Write};
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::option::Option::None;

use serializer::PageSerializer;
use FromReader;

use crate::suitable_data_type::SuitableDataType;
use crate::ChunkHeader;

#[allow(unused)]
fn setup_logging() {
    env_logger::init();
}


