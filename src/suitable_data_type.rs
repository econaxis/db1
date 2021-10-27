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
use std::io::{Write, Read, Seek, SeekFrom};
use std::fs::read;
use std::process::Output;
use crate::chunk_header::slice_from_type;
bytes_serializer!(DataType);
from_reader!(DataType);


const STRING_CHECK_SEQ: u16 = 0x72a0;
impl FromReader for String {
    fn from_reader_and_heap<R: Read>(mut r: R, heap: &[u8]) -> Self {

        let mut check_sequence: u16 = 0;
        let mut loc: u64 = 0;
        let mut len: u64 = 0;
        r.read_exact(slice_from_type(&mut check_sequence));
        r.read_exact(slice_from_type(&mut loc));
        r.read_exact(slice_from_type(&mut len));

        assert_eq!(check_sequence, STRING_CHECK_SEQ);

        if (loc == 0 && len == 0) || heap == &[] {
            return String::new();
        }
        let buffer_slice = &heap[loc as usize..(loc + len) as usize];
        String::from_utf8(Vec::from(buffer_slice)).unwrap()
    }
}

impl BytesSerialize for String {
    fn serialize_with_heap<W: Write, W1: Write + Seek> (&self, mut data: W, mut heap: W1) {
        let slice = self.as_bytes();
        let heap_position = heap.stream_position().unwrap();
        data.write_all(&STRING_CHECK_SEQ.to_le_bytes()).unwrap();
        data.write_all(&heap_position.to_le_bytes()).unwrap();
        data.write_all(&slice.len().to_le_bytes()).unwrap();
        heap.write_all(slice).unwrap();
    }
}

// impl<T: Sized + FromReader> VariableLength<T> {
//     pub fn load_value<R: Read + Seek>(&mut self, reader:&mut R) {
//         match self {
//             Self::RealValue(_) => {},
//             Self::Pointer(loc) => {
//                 reader.seek(SeekFrom::Start(*loc));
//                 *self = Self::RealValue(T::from_reader_and_heap(reader));
//             }
//         }
//     }
// }

pub trait QueryableDataType: SuitableDataType + PartialOrd<u64> + PartialEq<u64> {}
pub trait SuitableDataType: Ord +  Clone + Debug + BytesSerialize + FromReader + 'static {
    const REQUIRES_HEAP: bool = false;
    const TYPE_SIZE: u64 = std::mem::size_of::<Self>() as u64;
    // Get the primary key that will be used for comparisons, sorting, and duplicate checks.
    fn first(&self) -> u64;
}

impl SuitableDataType for DataType {
    fn first(&self) -> u64 {
        self.0 as u64
    }
}
impl QueryableDataType for DataType {}
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