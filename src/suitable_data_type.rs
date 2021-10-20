use std::cmp::{Ordering, Ord};
use std::fmt::{Debug, Formatter};

use crate::bytes_serializer::{BytesSerialize, FromReader};

#[derive(PartialEq, Clone)]
#[repr(C)]
pub struct DataType(pub u8, pub u8, pub u8);

impl Debug for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("DataType({},{},{})", self.0, self.1, self.2))
    }
}

use crate::{bytes_serializer, from_reader};
bytes_serializer!(DataType);
from_reader!(DataType);
pub trait SuitableDataType : PartialEq<u64> + PartialOrd<u64> + Ord + Clone + Debug + BytesSerialize + FromReader + 'static {}

impl SuitableDataType for DataType {}

impl PartialEq<u64> for DataType {
    fn eq(&self, other: &u64) -> bool {
        self.0.eq(&(*other as u8))
    }
}

impl PartialOrd<u64> for DataType {
    fn partial_cmp(&self, other: &u64) -> Option<Ordering> {
        self.0.partial_cmp(&(*other as u8))
    }
}

impl Eq for DataType {
}

impl PartialOrd for DataType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.partial_cmp(&(other.0 as u64))
    }
}
impl Ord for DataType {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}
