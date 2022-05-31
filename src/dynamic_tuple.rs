use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::HashMap;

use std::ffi::{CStr, CString};
use std::fmt::{Debug, Write as OW};
use std::fs::File;
use std::io::{Cursor, Read, Seek, Write};

use std::option::Option::None;
use std::os::raw::c_char;

use std::sync::Once;


use db1_string::Db1String;
use dynamic_tuple::TypeData::Null;
use slice_from_type;
use serializer::PageSerializer;
use table_base::read_to_buf;
use table_base2::{TableBase2, TableType};
use FromReader;
use {BytesSerialize, SuitableDataType};
use serializer;


use crate::query_data::QueryData;

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

impl Into<TypeData> for u64 {
    fn into(self) -> TypeData {
        TypeData::Int(self)
    }
}

#[derive(Default, Clone, Debug)]
pub struct DynamicTuple {
    pub(crate) fields: Vec<Type>,
}

// todo: TupleBuilder but without malloc -- just a schema for tuples
#[derive(Default, Debug, PartialEq, Clone)]
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

// #[test]
// fn build_tuple_w_string() {
//     let mut cursor = Cursor::<Vec<u8>>::default();
//     {
//         let mut tb = DynamicTable::new(&mut cursor, false);
//
//         let tuple = TupleBuilder::default()
//             .add_int(30)
//             .add_int(10)
//             .add_string("hello world")
//             .add_string("fdjsakf;ld saflkdsa;j fdavcx");
//         tb.store(tuple);
//
//         let tuple = TupleBuilder::default()
//             .add_int(60)
//             .add_int(20)
//             .add_string("60 hello world")
//             .add_string("60 fdjsakf;ld saflkdsa;j fdavcx");
//         tb.store(tuple);
//         tb.force_flush();
//     }
//
//     cursor.set_position(0);
//     let _tb1 = TypedTable {
//         ty: DynamicTuple {
//             fields: vec![Type::Int, Type::Int, Type::String, Type::String],
//         },
//         column_map: Default::default(),
//         id_ty: 1,
//     };
//     // let mut ps =
//     // tb1.get_in_all(None, )
//     // let mut tb1 = DynamicTable::new(&mut cursor, , true);
//     // tb1.get(..);
// }



pub trait RWS = Read + Write + Seek;

pub struct TableCursor<'a> {
    locations: Vec<u64>,
    ty: &'a DynamicTuple,
    // current_tuples: Vec<TupleBuilder>,
    current_index: u64,
    end_index_exclusive: u64,
    pkey: Option<TypeData>,
    load_columns: u64,
}

impl<'a> TableCursor<'a> {
    pub(crate) fn new< W: RWS>(locations: Vec<u64>, ps: & mut PageSerializer<W>, ty: &'a DynamicTuple, pkey: Option<TypeData>, load_columns: u64) -> Self {
        let mut se = Self {
            locations,
            ty,
            current_index: 0,
            end_index_exclusive: 0,
            pkey,
            load_columns,
        };
        if !se.locations.is_empty() {
            se.reset_index_iterator(ps);
        }
        se
    }
    fn reset_index_iterator<W: RWS>(&mut self, ps: &mut PageSerializer<W>) {
        // Reload the index iterator for the new table
        let table = ps.load_page_cached(*self.locations.last().unwrap());
        let range = if let Some(pk) = &self.pkey {
            table.get_ranges(pk..=pk)
        } else {
            0..table.len()
        };
        self.current_index = range.start;
        self.end_index_exclusive = range.end;
    }
}

use ra_ops::RANodeIterator;
use crate::named_tables::NamedTables;
use crate::secondary_index::SecondaryIndices;
use crate::typed_table::TypedTable;

impl<W: RWS> RANodeIterator<W> for TableCursor<'_> {
    fn next(&mut self, ps: &mut PageSerializer<W>) -> Option<TupleBuilder> {
        if self.current_index < self.end_index_exclusive {
            // Work on self.current_index
            let location = self.locations.last()?;
            let table = ps.load_page_cached(*location);

            let bytes = table.load_index(self.current_index as usize);
            let tuple = self.ty.read_tuple(bytes, self.load_columns, table.heap().get_ref());

            self.current_index += 1;
            Some(tuple)
        } else if self.locations.len() > 1 {
            self.locations.pop().unwrap();
            self.reset_index_iterator(ps);
            self.next(ps)
        } else if self.locations.len() == 1 {
            None
        } else {
            None
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct CreateTable {
    pub(crate) tbl_name: String,
    pub(crate) fields: Vec<(String, Type)>,
}

#[derive(PartialEq, Debug, Clone)]
enum Token<'a> {
    Identifier(&'a str),
    String(String),
    Empty,
    Number(u64),
    LParens,
    RParens,
    Comma,
    End,
}

#[derive(Debug)]
struct TokenStream<'a> {
    a: Vec<Token<'a>>,
    ind: Cell<usize>,
}

impl<'a> TokenStream<'a> {
    fn next(&self) -> &Token {
        self.ind.set(self.ind.get() + 1);
        &self.a[self.ind.get() - 1]
    }
    pub fn peek(&self) -> &Token {
        &self.a[self.ind.get()]
    }
    pub fn extract_identifier(&self) -> &str {
        match self.next() {
            Token::Identifier(s) => s,
            other => panic!("Expected identifier, got {:?}", other),
        }
    }
    pub fn extract(&self, a: Token) -> &Token {
        match self.next() {
            match_a if match_a == &a => match_a,
            _ => panic!(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct InsertValues {
    pub(crate) values: Vec<Vec<TypeData>>,
    pub(crate) tbl_name: String,
}

type TokenStreamRef<'a, 'b> = &'a TokenStream<'b>;

fn parse_user_data(str: TokenStreamRef) -> TypeData {
    match str.next() {
        Token::String(s) => TypeData::String(s.clone().into()),
        Token::Number(i) => {
            let i = *i;
            TypeData::Int(i)
        }
        _x => {
            panic!("Remaining: {:?}", str)
        }
    }
}

fn parse_insert_values(str: TokenStreamRef) -> InsertValues {
    let tbl_name = str.extract_identifier();
    let mut td = Vec::new();

    assert_eq!(str.extract_identifier(), "VALUES");

    loop {
        str.extract(Token::LParens);
        let tuple = parse_comma_delimited_list(str, parse_user_data);
        str.extract(Token::RParens);
        td.push(tuple);
        if str.peek() != &Token::Comma {
            break;
        } else {
            str.extract(Token::Comma);
        }
    }
    InsertValues {
        values: td,
        tbl_name: tbl_name.to_string(),
    }
}

fn parse_create_table(str: TokenStreamRef) -> CreateTable {
    let tbl_name = str.extract_identifier();
    assert_eq!(str.next(), &Token::LParens);

    let mut fields = Vec::new();
    loop {
        let name = str.extract_identifier();
        let ty = str.extract_identifier();
        let ty = match ty {
            "INT" | "int" => Type::Int,
            "STRING" | "string" => Type::String,
            _ => panic!(),
        };
        fields.push((name.to_string(), ty));

        match str.next() {
            Token::Comma => {}
            Token::RParens => break,
            _ => panic!(),
        }
    }

    CreateTable {
        tbl_name: tbl_name.to_string(),
        fields,
    }
}

fn lex(str: &str) -> TokenStream {
    let mut tokens = Vec::new();
    let mut prev_index = 0;
    let mut split = Vec::new();
    for (index, matched) in str.match_indices(&[',', ' ', '(', ')', '"', '\n', '\\']) {
        if prev_index != index {
            split.push(&str[prev_index..index]);
        }
        if !matched.is_empty() {
            split.push(matched);
        }
        prev_index = index + 1;
    }
    if prev_index < str.len() {
        split.push(&str[prev_index..]);
    }
    let mut escaped = false;
    let mut in_string: Option<String> = None;
    for s in split {
        // Filter out whitespace
        let token = match s {
            "\\" => {
                escaped = true;
                assert!(in_string.is_some());
                Token::Empty
            }
            "\"" if !escaped => {
                if let Some(str) = in_string.take() {
                    Token::String(str)
                } else {
                    in_string = Some("".to_string());
                    Token::Empty
                }
            }
            x if in_string.is_some() => {
                escaped = false;
                in_string.as_mut().unwrap().push_str(x);
                Token::Empty
            }
            " " | "\n" | "\r" if in_string.is_none() => continue,
            "," => Token::Comma,
            "(" => Token::LParens,
            ")" => Token::RParens,
            a => {
                assert!(
                    a.chars()
                        .all(|a| a.is_alphanumeric() || a == '_' || a == '*'),
                    "{}",
                    a
                );

                if a.chars().all(|a| a.is_numeric()) {
                    Token::Number(a.parse::<u64>().unwrap())
                } else {
                    Token::Identifier(a)
                }
            }
        };
        if token != Token::Empty {
            tokens.push(token);
        }
    }
    tokens.push(Token::End);
    TokenStream {
        a: tokens,
        ind: Cell::new(0),
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum Filter {
    Equals(String, TypeData),
}

#[derive(Debug, PartialEq)]
pub(crate) struct Select {
    pub(crate) tbl_name: String,
    pub(crate) columns: Vec<String>,
    pub(crate) filter: Vec<Filter>,
}

fn parse_comma_delimited_list<'a, 'b, T: 'b, F: Fn(TokenStreamRef<'a, 'b>) -> T>(
    str: TokenStreamRef<'a, 'b>,
    f: F,
) -> Vec<T> {
    let mut a = Vec::new();
    loop {
        a.push(f(str));

        match str.peek() {
            Token::Comma => {
                str.next();
            }
            _ => break,
        };
    }
    a
}

fn parse_where(str: TokenStreamRef) -> Vec<Filter> {
    let column_name = str.extract_identifier();
    assert_eq!(str.extract_identifier(), "EQUALS");
    let data = parse_user_data(str);

    vec![Filter::Equals(column_name.to_string(), data)]
}

fn parse_select(str: TokenStreamRef) -> Select {
    let columns = parse_comma_delimited_list(str, |a| a.extract_identifier());
    assert_eq!(str.extract_identifier(), "FROM");
    let tbl_name = str.extract_identifier();

    let filters = match str.next() {
        Token::Identifier(s) if *s == "WHERE" => parse_where(str),
        _ => {
            vec![]
        }
    };

    Select {
        tbl_name: tbl_name.to_string(),
        columns: columns.iter().map(|a| a.to_string()).collect(),
        filter: filters,
    }
}

#[derive(Debug, PartialEq)]
enum SQL {
    CreateTable(CreateTable),
    Insert(InsertValues),
    Select(Select),
    Flush,
}

fn parse_sql(str: TokenStreamRef) -> SQL {
    match str.extract_identifier() {
        "CREATE" => {
            assert_eq!(str.extract_identifier(), "TABLE");
            SQL::CreateTable(parse_create_table(str))
        }
        "INSERT" => {
            assert_eq!(str.extract_identifier(), "INTO");
            SQL::Insert(parse_insert_values(str))
        }
        "SELECT" => SQL::Select(parse_select(str)),
        "FLUSH" => SQL::Flush,
        _ => panic!(),
    }
}

fn parse_lex_sql<'a, W: RWS>(
    str: &str,
    table: &'a mut NamedTables,
    ps: &'a mut PageSerializer<W>,
) -> Option<QueryData<'a, W>> {
    let mut lexed = lex(str);
    let parsed = parse_sql(&mut lexed);
    match parsed {
        SQL::Insert(iv) => {
            table.execute_insert(iv, ps);
            None
        }
        SQL::Select(se) => Some(table.execute_select(se, ps)),
        SQL::CreateTable(cr) => {
            table.insert_table(cr, ps);
            None
        }
        SQL::Flush => {
            ps.unload_all();
            None
        }
    }
}

#[test]
fn select() {
    let mut ts = lex(r#"
    SELECT col1, col2, col3 FROM tbl WHERE col1 EQUALS 5
    "#);
    dbg!(parse_sql(&mut ts));
}

#[test]
fn create_table() {
    let mut ts = lex(r#"CREATE TABLE tbl_name (
        ID int,
        filename STRING,
        contents STRING
        )
    "#);
    assert_eq!(
        parse_sql(&mut ts),
        SQL::CreateTable(CreateTable {
            tbl_name: "tbl_name".to_string(),
            fields: vec![
                ("ID".to_string(), Type::Int),
                ("filename".to_string(), Type::String),
                ("contents".to_string(), Type::String),
            ],
        })
    );
}

#[test]
fn insert_values() {
    let mut ts = lex(r#"INSERT INTO test VALUES (3, 4, 5), (4, 5, 6), ("hello", "world", 1, 2)"#);
    assert_eq!(
        parse_sql(&mut ts),
        SQL::Insert(InsertValues {
            values: vec![
                vec![TypeData::Int(3), TypeData::Int(4), TypeData::Int(5)],
                vec![TypeData::Int(4), TypeData::Int(5), TypeData::Int(6)],
                vec![
                    TypeData::String("hello".into()),
                    TypeData::String("world".into()),
                    TypeData::Int(1),
                    TypeData::Int(2),
                ],
            ],
            tbl_name: "test".to_string(),
        })
    );
}

#[test]
fn test_index_type_table2() {
    let mut ps = PageSerializer::default();
    let mut tt = TypedTable::new(DynamicTuple::new(vec![Type::String, Type::String]), 10, &mut ps, vec!["a", "b"]);

    for i in 0..3_111_100u64 {
        let ty = TupleBuilder::default().add_string(i.to_string()).add_string((i * 10000).to_string());
        tt.store_raw(ty, &mut ps);
    }
}

#[test]
fn typed_table_cursors() {
    let mut ps = PageSerializer::default();
    let tt = TypedTable::new(
        DynamicTuple::new(vec![Type::Int, Type::String, Type::String]),
        10,
        &mut ps,
        vec!["id", "name", "content"],
    );

    let mut i = 0;
    while ps.get_in_all(tt.id_ty, None).len() < 10 {
        i += 1;
        let tb = TupleBuilder::default()
            .add_int(i)
            .add_string(format!("hello{i}"))
            .add_string(format!("world{i}"));
        tt.store_raw(tb, &mut ps);
    }
    // Now test the iterator API
    let mut result1 = tt.get_in_all_iter(None, u64::MAX, &mut ps);
    let mut result1: Vec<_> = result1.collect(&mut ps);

    let mut cursor = tt.get_in_all_iter(None, u64::MAX, &mut ps);
    let mut cursor: Vec<_> = cursor.collect(&mut ps);
    assert_eq!(result1, cursor);
}

#[test]
fn typed_table_test() {
    let mut ps = PageSerializer::default();
    let tt = TypedTable::new(
        DynamicTuple::new(vec![Type::Int, Type::String, Type::String]),
        10,
        &mut ps,
        vec!["id", "name", "content"],
    );
    let tt1 = TypedTable::new(
        DynamicTuple::new(vec![Type::Int, Type::String]),
        11,
        &mut ps,
        vec!["id", "name"],
    );

    for i in 30..=90 {
        let tb = TupleBuilder::default()
            .add_int(i)
            .add_string(format!("hello{i}"))
            .add_string(format!("world{i}"));
        tt.store_raw(tb, &mut ps);

        let tb1 = TupleBuilder::default()
            .add_int(i)
            .add_string(format!("tb1{i}"));
        tt1.store_raw(tb1, &mut ps);
    }

    for i in (30..=90).rev() {
        assert_eq!(
            tt.get_in_all_iter(Some(TypeData::Int(i)), 0, &mut ps).collect(&mut ps),
            vec![TupleBuilder::default()
                .add_int(i)
                .add_string(format!("hello{i}"))
                .add_string(format!("world{i}"))],
            "{}",
            i
        );
        assert_eq!(
            tt1.get_in_all_iter(Some(TypeData::Int(i)), 0, &mut ps).collect(&mut ps),
            vec![TupleBuilder::default()
                .add_int(i)
                .add_string(format!("tb1{i}"))]
        );
    }
}

#[test]
fn onehundred_typed_tables() {
    let mut ps = PageSerializer::default();
    let mut tables = Vec::new();
    let tt = TypedTable::new(
        DynamicTuple::new(vec![Type::Int, Type::String, Type::String]),
        10,
        &mut ps,
        vec!["id", "name", "content"],
    );
    tables.resize(100, tt);

    for i in 0..2000usize {
        println!("Inseting {i}");
        let tb = TupleBuilder::default()
            .add_int(i as u64)
            .add_string(format!("hello{i}"))
            .add_string(format!("world{i}"));
        tables[i % 100].store_raw(tb, &mut ps);
    }

    ps.unload_all();
    let mut ps1 = PageSerializer::create_from_reader(ps.file.clone(), None);
    for i in (0..2000).rev() {
        assert_eq!(
            tables[i % 100].get_in_all_iter(Some(TypeData::Int(i as u64)), 0, &mut ps).collect(&mut ps),
            vec![TupleBuilder::default()
                .add_int(i as u64)
                .add_string(format!("hello{i}"))
                .add_string(format!("world{i}"))]
        );
        assert_eq!(
            tables[i % 100].get_in_all_iter(Some(TypeData::Int(i as u64)), 0, &mut ps1).collect(&mut ps),
            vec![TupleBuilder::default()
                .add_int(i as u64)
                .add_string(format!("hello{i}"))
                .add_string(format!("world{i}"))]
        );
    }
}

#[test]
fn test_sql_all() {
    let mut ps = PageSerializer::create(Cursor::new(Vec::new()), Some(serializer::MAX_PAGE_SIZE));
    let mut nt = NamedTables::new(&mut ps);

    parse_lex_sql(
        "CREATE TABLE tbl (id INT, name STRING, telephone STRING)",
        &mut nt,
        &mut ps,
    );
    parse_lex_sql(
        r#"INSERT INTO tbl VALUES (3, "hello3 world", "30293204823")"#,
        &mut nt,
        &mut ps,
    );
    parse_lex_sql(
        r#"INSERT INTO tbl VALUES (4, "hello4 world", "3093204823")"#,
        &mut nt,
        &mut ps,
    );
    parse_lex_sql(
        r#"INSERT INTO tbl VALUES (5, "hello5 world", "3293204823")"#,
        &mut nt,
        &mut ps,
    );
    parse_lex_sql(
        "CREATE TABLE tbl1 (id INT, name STRING, fax INT)",
        &mut nt,
        &mut ps,
    );
    parse_lex_sql(
        r#"INSERT INTO tbl VALUES (6, "hello6 world", "0293204823")"#,
        &mut nt,
        &mut ps,
    );
    parse_lex_sql(
        r#"INSERT INTO tbl1 VALUES (7, "hello7 world", 293204823), (9, "hellfdsoa f", 3209324830294)"#,
        &mut nt,
        &mut ps,
    );
    parse_lex_sql(
        r#"INSERT INTO tbl1 VALUES (8, "hello8 world", 3209324830294)"#,
        &mut nt,
        &mut ps,
    );
    let answer1 = parse_lex_sql(
        r#"SELECT id, name, telephone FROM tbl WHERE id EQUALS 4"#,
        &mut nt,
        &mut ps,
    )
        .unwrap()
        .results();
    let answer2 = parse_lex_sql(
        r#"SELECT id, fax FROM tbl1 WHERE fax EQUALS 3209324830294 "#,
        &mut nt,
        &mut ps,
    )
        .unwrap()
        .results();
    dbg!(&answer1, &answer2);

    let mut ps = PageSerializer::create_from_reader(ps.move_file(), Some(serializer::MAX_PAGE_SIZE));
    let mut nt = NamedTables::new(&mut ps);
    assert_eq!(
        parse_lex_sql(
            r#"SELECT id, name, telephone FROM tbl WHERE id EQUALS 4 "#,
            &mut nt,
            &mut ps,
        )
            .unwrap()
            .results(),
        answer1
    );
    assert_eq!(
        parse_lex_sql(
            r#"SELECT id, fax FROM tbl1 WHERE fax EQUALS 3209324830294 "#,
            &mut nt,
            &mut ps,
        )
            .unwrap()
            .results(),
        answer2
    );
}

#[bench]
fn test_selects(b: &mut test::Bencher) -> impl std::process::Termination {
    use rand::seq::SliceRandom;
    use rand::thread_rng;
    ENVLOGGER.call_once(env_logger::init);
    let file = File::options()
        .truncate(true)
        .create(true)
        .read(true)
        .write(true)
        .open("/tmp/test_selects")
        .unwrap();
    let mut ps = PageSerializer::create(file, Some(serializer::MAX_PAGE_SIZE));
    let mut nt = NamedTables::new(&mut ps);

    parse_lex_sql(
        "CREATE TABLE tbl (id INT, name STRING, telephone STRING)",
        &mut nt,
        &mut ps,
    );

    let mut indices: Vec<u64> = (0..100_000).collect();
    indices.shuffle(&mut thread_rng());
    let mut j = indices.iter().cycle();
    for _ in 0..10_000 {
        let j = *j.next().unwrap();
        let i = j + 10;
        parse_lex_sql(
            &format!(
                r#"INSERT INTO tbl VALUES ({i}, "hello{i} world", "{i}"), ({j}, "hello{j} world", "{j}")"#
            ),
            &mut nt,
            &mut ps,
        );
    }
    for _ in 0..1000 {
        let j = *j.next().unwrap();
        let res1 = parse_lex_sql(
            &format!("SELECT * FROM tbl WHERE id EQUALS {j}"),
            &mut nt,
            &mut ps,
        );
        if let Some(r) = res1 {
            r.results();
        }
    }
}

#[test]
fn test_inserts() {
    use rand::seq::SliceRandom;
    use rand::SeedableRng;
    ENVLOGGER.call_once(env_logger::init);
    let mut a = rand_chacha::ChaCha20Rng::seed_from_u64(1);
    let file = File::options()
        .truncate(true)
        .create(true)
        .read(true)
        .write(true)
        .open("/tmp/test-inserts")
        .unwrap();
    let mut ps = PageSerializer::create(file, Some(serializer::MAX_PAGE_SIZE));
    let mut nt = NamedTables::new(&mut ps);

    parse_lex_sql(
        "CREATE TABLE tbl (id INT, name STRING, telephone STRING)",
        &mut nt,
        &mut ps,
    );

    let mut indices: Vec<u64> = (0..100_000).collect();
    indices.shuffle(&mut a);
    let mut j = indices.iter().cycle();
    for _ in 0..1_0000 {
        let j = j.next().unwrap();
        let i = j + 10;
        parse_lex_sql(
            &format!(
                r#"INSERT INTO tbl VALUES ({i}, "hello{i} world", "{i}"), ({j}, "hello{j} world", "{j}")"#
            ),
            &mut nt,
            &mut ps,
        );
    }
}

#[test]
fn lots_inserts() {
    use rand::seq::SliceRandom;
    use rand::SeedableRng;
    ENVLOGGER.call_once(env_logger::init);
    let mut a = rand_chacha::ChaCha20Rng::seed_from_u64(1);
    let file = File::options()
        .truncate(true)
        .create(true)
        .read(true)
        .write(true)
        .open("/tmp/test-lots-inserts")
        .unwrap();
    let mut ps = PageSerializer::create(file, None);
    let mut nt = NamedTables::new(&mut ps);

    parse_lex_sql(
        "CREATE TABLE tbl (id INT, name STRING, telephone STRING, description STRING)",
        &mut nt,
        &mut ps,
    );

    let mut indices: Vec<u64> = (0..1_000_000).collect();
    indices.shuffle(&mut a);
    let desc_string = String::from_utf8(vec![b'a'; 10]).unwrap();
    for j in indices {
        let i = j + 10;
        let insert = InsertValues {
            values: vec![vec![TypeData::Int(i), TypeData::String(format!("hello{i} world").into()), TypeData::String(format!("{i}").into()), TypeData::String(format!("{desc_string}").into())]],
            tbl_name: "tbl".to_string(),
        };
        nt.execute_insert(insert, &mut ps);
    }

    // for _ in 0..1_000_000 {
    //     let j = *j.next().unwrap();
    //     let i = j + 10;
    //     parse_lex_sql(&format!(r#"INSERT INTO tbl VALUES ({i}, "hello{i} world", "{i}"), ({j}, "hello{j} world", "{j}")"#), &mut nt, &mut ps);
    // }
    //
    // println!("Done inserting");
    // let mut i = 0;
    // b.iter(|| {
    //     i += 1;
    //     i %= 1000000;
    //     let res1 = parse_lex_sql(&format!("SELECT * FROM tbl WHERE id EQUALS {i}"), &mut nt, &mut ps);
    //     if let Some(r) = res1 {
    //         r.results();
    //     }
    // });
}

#[test]
fn named_table_exec_insert() {
    ENVLOGGER.call_once(env_logger::init);

    let mut ps = PageSerializer::default();
    let mut nt = NamedTables::new(&mut ps);
    nt.insert_table(
        CreateTable {
            tbl_name: "tbl_name".to_string(),
            fields: vec![
                ("id".to_string(), Type::Int),
                ("name".to_string(), Type::String),
            ],
        },
        &mut ps,
    );
    nt.execute_insert(
        InsertValues {
            values: vec![
                vec![TypeData::Int(3), TypeData::String("hello".into())],
                vec![TypeData::Int(4), TypeData::String("hello4".into())],
                vec![TypeData::Int(5), TypeData::String("hello4".into())],
            ],
            tbl_name: "tbl_name".to_string(),
        },
        &mut ps,
    );

    dbg!(nt
        .execute_select(
            Select {
                tbl_name: "tbl_name".to_string(),
                columns: vec![],
                filter: vec![Filter::Equals("id".to_string(), TypeData::Int(2))],
            },
            &mut ps,
        )
        .results());
    dbg!(nt
        .execute_select(
            Select {
                tbl_name: "tbl_name".to_string(),
                columns: vec![],
                filter: vec![Filter::Equals(
                    "name".to_string(),
                    TypeData::String("hello4".into()),
                )],
            },
            &mut ps,
        )
        .results());

    ps.unload_all();
    let prev_headers = ps.clone_headers();
    let mut ps1 = PageSerializer::create_from_reader(ps.move_file(), None);
    assert_eq!(ps1.clone_headers(), prev_headers);
    let mut nt = NamedTables::new(&mut ps1);
    dbg!(nt
        .execute_select(
            Select {
                tbl_name: "tbl_name".to_string(),
                columns: vec![],
                filter: vec![Filter::Equals(
                    "name".to_string(),
                    TypeData::String("hello4".into()),
                )],
            },
            &mut ps1,
        )
        .results());

    nt.insert_table(
        CreateTable {
            tbl_name: "tbl1".into(),
            fields: vec![
                ("pkey".to_string(), Type::Int),
                ("name".to_string(), Type::String),
                ("mimetype".to_string(), Type::String),
                ("contents".to_string(), Type::String),
            ],
        },
        &mut ps1,
    );
    for i in 0..100 {
        nt.execute_insert(
            InsertValues {
                values: vec![vec![
                    i.into(),
                    TypeData::String(format!("file{i}.jpeg").into()),
                    "application/pdf".into(),
                    "0f80a8ds8 vcx08".into(),
                ]],
                tbl_name: "tbl1".to_string(),
            },
            &mut ps1,
        );
    }
    for i in 0..100 {
        let res = nt
            .execute_select(
                Select {
                    tbl_name: "tbl1".to_string(),
                    columns: vec!["name".to_string()],
                    filter: vec![Filter::Equals(
                        "name".to_string(),
                        TypeData::String(format!("file{i}.jpeg").into()),
                    )],
                },
                &mut ps1,
            )
            .results();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].extract_string(1), format!("file{i}.jpeg").as_bytes());
    }
}

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

    let result = parse_lex_sql(query.as_ref(), &mut db.table, &mut db.ps);
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
                        // TODO: write Null instead of Int(0). Need to fix also in the Python parser module.
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

#[test]
fn test_sql_c_api() {
    unsafe {
        let tb = sql_new(
            CStr::from_bytes_with_nul(b"/tmp/test_sql_c_api.db\0")
                .unwrap()
                .as_ptr(),
        );
        let q1 = CString::new("CREATE TABLE tbl1 (pkey INT, telephone INT, a STRING)").unwrap();
        let q2 = CString::new(r#"INSERT INTO tbl1 VALUES (1, 90328023, "hello"), (2, 32084432, "world"), (3, 32084432, "world"), (4, 32084432, "world")"#).unwrap();
        let q3 = CString::new(r#"SELECT pkey, a FROM tbl1 WHERE a EQUALS "world""#).unwrap();
        let q4 = CString::new("SELECT pkey, a FROM tbl1").unwrap();
        sql_exec(tb, q1.as_ptr());
        sql_exec(tb, q2.as_ptr());
        println!(
            "{}",
            CStr::from_ptr(sql_exec(tb, q3.as_ptr() as *const c_char))
                .to_str()
                .unwrap()
        );
        println!(
            "{}",
            CStr::from_ptr(sql_exec(tb, q4.as_ptr() as *const c_char))
                .to_str()
                .unwrap()
        );
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
// TODO: change TableBase2 insertion API to support `Write` interface to avoid malloc
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
