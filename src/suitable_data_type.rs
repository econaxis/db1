use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::io::Read;

use crate::bytes_serializer::{BytesSerialize, FromReader};
use crate::from_reader;

#[derive(Clone, Default, PartialEq)]
#[repr(C)]
pub struct DataType(pub u8, pub u8, pub u8);

impl Hash for DataType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.0 as u64).hash(state)
    }
}

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

impl BytesSerialize for DataType {}
from_reader!(DataType);

pub trait SuitableDataType:
    Clone + Debug + BytesSerialize + FromReader + PartialOrd<u64> + PartialEq<u64> + 'static
{
    const REQUIRES_HEAP: bool = false;
    const TYPE_SIZE: u64 = std::mem::size_of::<Self>() as u64;
    // Get the primary key that will be used for comparisons, sorting, and duplicate checks.
    fn first(&self) -> u64 {
        todo!()
    }
    fn resolve_item(&mut self, _heap: &[u8], _index: u8) {}
}

impl SuitableDataType for DataType {
    fn first(&self) -> u64 {
        self.0 as u64
    }
}

#[macro_export]
macro_rules! gen_suitable_data_type_impls {
    ($t:ty) => {
        impl PartialOrd<u64> for $t {
            fn partial_cmp(&self, other: &u64) -> Option<std::cmp::Ordering> {
                self.first().partial_cmp(&(*other))
            }
        }

        impl PartialEq<u64> for $t {
            fn eq(&self, other: &u64) -> bool {
                self.first().eq(&(*other as u64))
            }
        }
    };
}

gen_suitable_data_type_impls!(DataType);
