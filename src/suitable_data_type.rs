use std::cmp::Ordering;
use std::fmt::Debug;

use crate::bytes_serializer::{BytesSerialize, FromReader};

#[derive(Debug, PartialEq, Clone)]
#[repr(C)]
pub struct DataType(pub u8, pub u8, pub u8);

use crate::{bytes_serializer, from_reader};
bytes_serializer!(DataType);
from_reader!(DataType);
pub trait SuitableDataType : PartialEq<u64> + PartialOrd<u64> + PartialOrd<Self> + Clone + Debug + BytesSerialize + FromReader + 'static {}

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


impl PartialOrd for DataType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.partial_cmp(&(other.0 as u64))
    }
}
