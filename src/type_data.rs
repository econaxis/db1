use std::cmp::Ordering;
use std::io::{Read, Seek, Write};
use ::{BytesSerialize, Db1String};
use ::{FromReader, slice_from_type};

impl Into<TypeData> for u64 {
    fn into(self) -> TypeData {
        TypeData::Int(self)
    }
}

impl PartialOrd for TypeData {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let result = match (self, other) {
            (TypeData::Int(x), TypeData::Int(y)) => x.partial_cmp(y),
            (TypeData::String(x), TypeData::String(y)) => x.partial_cmp(y),
            (TypeData::Null, TypeData::Null) => Some(Ordering::Equal),
            (TypeData::Null, _other) => Some(Ordering::Less),
            (_self_, TypeData::Null) => Some(Ordering::Greater),
            (TypeData::Int(u64::MAX), _) => Some(Ordering::Greater),
            (_, TypeData::Int(u64::MAX)) => Some(Ordering::Less),
            _ => panic!("Invalid comparison between {:?} {:?}", self, other)
        };
        result
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Type {
    Int = 1,
    String = 2,
}

impl From<u64> for Type {
    fn from(i: u64) -> Self {
        match i {
            1 => Type::Int,
            2 => Type::String,
            _ => panic!(),
        }
    }
}

#[derive(Debug, Eq, Clone)]
pub enum TypeData {
    Int(u64),
    String(Db1String),
    Null,
}

impl Ord for TypeData {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl PartialEq for TypeData {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (TypeData::Int(x), TypeData::Int(y)) => x.eq(y),
            (TypeData::String(x), TypeData::String(y)) => x.eq(y),
            (TypeData::Null, TypeData::Null) => true,
            _ => false,
        }
    }
}

impl TypeData {
    const INT_TYPE: u8 = 1;
    const STRING_TYPE: u8 = 2;
    const NULL_TYPE: u8 = 0;
    fn get_type_code(&self) -> u8 {
        match self {
            TypeData::Int(_) => TypeData::INT_TYPE,
            TypeData::String(_) => TypeData::STRING_TYPE,
            TypeData::Null => TypeData::NULL_TYPE,
        }
    }
}

impl FromReader for TypeData {
    fn from_reader_and_heap<R: Read>(mut r: R, heap: &[u8]) -> Self {
        let mut type_code: u8 = 0;
        r.read_exact(slice_from_type(&mut type_code)).unwrap();

        match type_code {
            TypeData::INT_TYPE => {
                let mut int: u64 = 0;
                r.read_exact(slice_from_type(&mut int)).unwrap();
                TypeData::Int(int)
            }
            TypeData::STRING_TYPE => {
                TypeData::String(Db1String::from_reader_and_heap(&mut r, heap))
            }
            TypeData::NULL_TYPE => {
                TypeData::Null
            }
            _ => panic!("Invalid type code got {}", type_code)
        }
    }
}

impl BytesSerialize for TypeData {
    fn serialize_with_heap<W: Write, W1: Write + Seek>(&self, mut data: W, heap: W1) {
        data.write_all(&self.get_type_code().to_le_bytes()).unwrap();
        match self {
            TypeData::Int(i) => data.write_all(&i.to_le_bytes()).unwrap(),
            TypeData::String(s) => s.serialize_with_heap(&mut data, heap),
            TypeData::Null => {}
        }
    }
}

impl From<&'_ str> for TypeData {
    fn from(i: &'_ str) -> Self {
        Self::String(i.to_string().into())
    }
}
