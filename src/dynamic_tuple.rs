



use std::ffi::{CStr, CString};
use std::fmt::{Debug, Write as OW};
use std::fs::File;
use std::io::{Cursor, Read, Seek, Write};


use std::os::raw::c_char;

use std::sync::Once;


use db1_string::Db1String;


use serializer::PageSerializer;
use table_base::read_to_buf;

use FromReader;
use {BytesSerialize, SuitableDataType};







#[derive(Default, Clone, Debug)]
pub struct DynamicTuple {
    pub fields: Vec<Type>,
}

#[derive(Default, Debug, PartialEq, Eq, Clone)]
pub struct TupleBuilder {
    pub fields: Vec<TypeData>,
}

impl TupleBuilder {
    pub fn append(mut self, other: TupleBuilder) -> Self {
        self.fields.extend(other.fields.into_iter());
        TupleBuilder {
            fields: self.fields
        }
    }
    pub fn first(&self) -> u64 {
        match &self.fields[0] {
            TypeData::Int(i) => *i,
            _ => panic!(),
        }
    }
    pub fn first_v2(&self) -> &TypeData {
        &self.fields[0]
    }
    pub fn type_check(&self, ty: &DynamicTuple) -> bool {
        assert_eq!(self.fields.len(), ty.fields.len());
        for a in self.fields.iter().zip(ty.fields.iter()) {
            match a {
                (TypeData::Int(..), Type::Int) | (TypeData::String(..), Type::String) => {}
                _ => return false,
            }
        }
        true
    }
    pub fn extract_int(&self, ind: usize) -> u64 {
        match &self.fields[ind] {
            TypeData::Int(i) => *i,
            _ => panic!(),
        }
    }
    pub fn extract_string(&self, ind: usize) -> &[u8] {
        match &self.fields[ind] {
            TypeData::String(i) => i.as_buffer(),
            _ => panic!("{:?}", self),
        }
    }
    pub fn extract(&self, ind: usize) -> &TypeData {
        &self.fields[ind]
    }
    pub fn add_int(mut self, i: u64) -> Self {
        self.fields.push(TypeData::Int(i));
        self
    }
    pub fn add_string<S: Into<String>>(mut self, s: S) -> Self {
        let s = Db1String::from(s.into());
        self.fields.push(TypeData::String(s));
        self
    }
    pub fn build<W: Write + Seek>(&self, mut heap: W) -> DynamicTupleInstance {
        let mut buf = [0u8; 400];
        let mut writer: Cursor<&mut [u8]> = Cursor::new(&mut buf);

        for i in &self.fields {
            match i {
                TypeData::Int(int) => {
                    writer.write_all(&int.to_le_bytes()).unwrap();
                }
                TypeData::String(s) => {
                    s.serialize_with_heap(&mut writer, &mut heap);
                }
                _ => panic!(),
            }
        }
        let len = writer.position();
        DynamicTupleInstance {
            data: buf,
            len: len as usize,
        }
    }
}

impl DynamicTuple {
    pub fn new(v: Vec<Type>) -> Self {
        assert!(v.len() < 64);
        Self { fields: v }
    }
    pub fn size(&self) -> u64 {
        self.fields
            .iter()
            .map(|v| match v {
                Type::Int => 8,
                Type::String => Db1String::TYPE_SIZE,
            })
            .sum()
    }
    pub fn read_tuple(&self, a: &[u8], mut load_columns: u64, heap: &[u8]) -> TupleBuilder {
        if load_columns == 0 {
            load_columns = u64::MAX;
        }
        let mut slice = Cursor::new(a);
        let mut answer = Vec::with_capacity(self.fields.len());

        for index in 0..self.fields.len() {
            let fully_load = ((1 << index) & load_columns) > 0;
            let t = self.fields[index as usize];
            match t {
                Type::Int => {
                    let data = TypeData::Int(u64::from_le_bytes(read_to_buf(&mut slice)));
                    if fully_load {
                        answer.push(data);
                    } else {
                        answer.push(TypeData::Null)
                    }
                }
                Type::String => {
                    let mut data = Db1String::from_reader_and_heap(&mut slice, heap);
                    if fully_load {
                        data.resolve_item(heap);
                        answer.push(TypeData::String(data));
                    } else {
                        answer.push(TypeData::Null)
                    }
                }
            }
        }
        TupleBuilder { fields: answer }
    }
}



pub trait RWS = Read + Write + Seek;


use crate::named_tables::NamedTables;
use crate::parser;


use crate::type_data::{Type, TypeData};

pub struct DynamicTable<W: Read + Write + Seek = Cursor<Vec<u8>>> {
    table: NamedTables,
    ps: PageSerializer<W>,
}

static ENVLOGGER: Once = Once::new();

#[no_mangle]
pub unsafe extern "C" fn sql_new(path: *const c_char) -> *mut DynamicTable<File> {
    ENVLOGGER.call_once(env_logger::init);
    let path = CStr::from_ptr(path).to_str().unwrap();
    let file = File::options()
        .create(true)
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    Box::leak(Box::new(DynamicTable::new(file)))
}

#[no_mangle]
pub unsafe extern "C" fn sql_exec(
    ptr: *mut DynamicTable<File>,
    query: *const c_char,
) -> *const c_char {
    let db = &mut *ptr;
    let query = CStr::from_ptr(query).to_string_lossy();

    let result = parser::parse_lex_sql(query.as_ref(), &mut db.table, &mut db.ps);
    if let Some(x) = result {
        let x = x.results();
        let mut output_string = "[".to_string();
        let mut first_tup = true;
        for tuple in x {
            if !first_tup {
                output_string.write_str(",[").unwrap();
            } else {
                output_string.write_str("[").unwrap();
                first_tup = !first_tup;
            }
            let mut first = true;
            for field in tuple.fields {
                if !first {
                    output_string.write_str(",").unwrap();
                } else {
                    first = !first;
                }

                match field {
                    TypeData::Int(i) => output_string.write_fmt(format_args!("{}", i)).unwrap(),
                    TypeData::String(s) => output_string
                        .write_fmt(format_args!(
                            "\"{}\"",
                            std::str::from_utf8(s.as_buffer()).unwrap()
                        ))
                        .unwrap(),
                    TypeData::Null => {
                        output_string.write_fmt(format_args!("{}", 0)).unwrap()
                    }
                };
            }
            output_string.write_str("]").unwrap();
        }
        output_string.write_char(']').unwrap();
        CString::new(output_string).unwrap().into_raw()
    } else {
        std::ptr::null_mut()
    }
}

impl<W: RWS> DynamicTable<W> {
    fn new(w: W) -> Self {
        let mut ps = PageSerializer::smart_create(w);
        Self {
            table: NamedTables::new(&mut ps),
            ps,
        }
    }
}

// Dynamic tuples automatically take up 400 bytes
#[derive(Clone, Debug)]
pub struct DynamicTupleInstance {
    pub data: [u8; 400],
    pub len: usize,
}

impl DynamicTupleInstance {
    fn from_vec(v: Vec<u8>) -> Self {
        assert!(v.len() < 400);
        let mut se = Self {
            data: [0u8; 400],
            len: v.len(),
        };
        se.data[0..v.len()].copy_from_slice(&v);
        se
    }
}

impl BytesSerialize for DynamicTupleInstance {
    fn serialize_with_heap<W: Write, W1: Write + Seek>(&self, mut data: W, _heap: W1) {
        data.write_all(&(self.len as u32).to_le_bytes()).unwrap();
        data.write_all(&self.data[0..self.len]).unwrap();
    }
}

impl FromReader for DynamicTupleInstance {
    fn from_reader_and_heap<R: Read>(mut r: R, _heap: &[u8]) -> Self {
        let mut se = Self::from_vec(Vec::new());
        let len = u32::from_le_bytes(read_to_buf(&mut r)) as usize;
        r.read_exact(&mut se.data[0..len]).unwrap();
        se.len = len;
        se
    }
}
