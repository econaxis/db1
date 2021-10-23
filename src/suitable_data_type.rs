use std::cmp::{Ordering, Ord};
use std::fmt::{Debug, Formatter};

use crate::bytes_serializer::{BytesSerialize, FromReader};

#[derive(Clone)]
#[repr(C)]
pub struct DataType(pub u8, pub u8, pub u8);

impl Debug for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("DataType({},{},{})", self.0, self.1, self.2))
    }
}

impl DataType {
    pub(crate) fn first(&self) -> u64 {
        self.0 as u64
    }
}

use crate::{bytes_serializer, from_reader};
bytes_serializer!(DataType);
from_reader!(DataType);
pub trait SuitableDataType: PartialEq<u64> + PartialOrd<u64> + Ord + Clone + Debug + BytesSerialize + FromReader + 'static {
    fn first(&self) -> u64;
}

impl SuitableDataType for DataType {
    fn first(&self) -> u64 {
        return self.0 as u64;
    }
}
#[macro_export]
macro_rules! gen_suitable_data_type_impls {
    ($t:ty) => {

impl PartialOrd<u64> for $t {
    fn partial_cmp(&self, other: &u64) -> Option<Ordering> {
        self.first().partial_cmp(&(*other))
    }
}

impl Eq for $t {}

impl PartialOrd for $t {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.partial_cmp(&(other.first() as u64))
    }
}
impl Ord for $t {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl PartialEq<u64> for $t {
    fn eq(&self, other: &u64) -> bool {
        self.first().eq(&(*other as u64))
    }
}
impl PartialEq for $t {
    fn eq(&self, other: &Self) -> bool {
        self.eq(&(other.first()))
    }
}


    };
}

gen_suitable_data_type_impls!(DataType);