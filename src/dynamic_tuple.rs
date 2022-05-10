use std::cell::Cell;
use std::collections::HashMap;
use std::convert::TryInto;
use std::ffi::{CStr, CString};
use std::fmt::{format, Debug, Write as OW};
use std::fs::File;
use std::io::{Cursor, Read, Seek, Write};
use std::option::Option::None;
use std::os::raw::c_char;
use std::sync::Once;

use db1_string::Db1String;
use dynamic_tuple::TypeData::Null;
use gen_suitable_data_type_impls;
use serializer::PageSerializer;
use table_base::read_to_buf;
use table_base2::{Heap, TableBase2};
use FromReader;
use {BytesSerialize, SuitableDataType};

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

#[derive(Debug, PartialEq)]
pub enum TypeData {
    Int(u64),
    String(Db1String),
    Null,
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
    fields: Vec<Type>,
}

#[derive(Default, Debug, PartialEq)]
pub struct TupleBuilder {
    pub fields: Vec<TypeData>,
}

impl TupleBuilder {
    pub fn owned(&mut self) {
        for f in &mut self.fields {
            match f {
                TypeData::String(str) => str.owned(),
                _ => {}
            }
        }
    }
    pub fn first(&self) -> u64 {
        match &self.fields[0] {
            TypeData::Int(i) => *i,
            _ => panic!(),
        }
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
    pub fn add_int(mut self, i: u64) -> Self {
        self.fields.push(TypeData::Int(i));
        self
    }
    pub fn add_string<S: Into<String>>(mut self, s: S) -> Self {
        let s = Db1String::from(s.into());
        self.fields.push(TypeData::String(s));
        self
    }
    pub fn build<W: Write + Seek>(self, mut heap: W) -> DynamicTupleInstance {
        let mut buf = [0u8; 400];
        let mut writer: Cursor<&mut [u8]> = Cursor::new(&mut buf);

        for i in self.fields {
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
                    let mut data = Db1String::read_to_ptr(&mut slice, heap);
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

    pub fn read_tuple_bytes(&self, a: &[u8], heap: &mut Heap) -> TupleBuilder {
        let mut slice = Cursor::new(a);
        let mut answer = Vec::new();
        for t in &self.fields {
            match t {
                Type::Int => {
                    answer.push(TypeData::Int(u64::from_le_bytes(read_to_buf(&mut slice))));
                }
                Type::String => {
                    let mut db1 = Db1String::from_reader_and_heap(&mut slice, heap.as_slice());

                    db1.resolve_item(heap.as_slice());
                    answer.push(TypeData::String(db1));
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

struct NamedTables {
    tables: HashMap<String, TypedTable>,
    largest_id: u64,
}

#[derive(Clone, Debug)]
struct TypedTable {
    ty: DynamicTuple,
    id_ty: u64,
    column_map: HashMap<String, u32>,
}

pub trait RWS = Read + Write + Seek;

impl TypedTable {
    fn get_in_all<'a, W: RWS>(
        &self,
        pkey: u64,
        load_columns: u64,
        ps: &'a mut PageSerializer<W>,
    ) -> QueryData<'a, W> {
        let mut answer = Vec::new();
        let pages = ps.get_in_all(self.id_ty, Some(pkey));
        for location in &pages {
            let table = ps.load_page_cached(*location);
            for bytes in table.search_value(pkey) {
                let tuple = self
                    .ty
                    .read_tuple(bytes, load_columns, table.heap().get_ref());
                answer.push(tuple);
            }
        }
        QueryData::new(answer, pages, ps)
    }

    fn exists_in_page_serializer(&self, ps: &PageSerializer<impl RWS>) -> bool {
        ps.get_in_all(self.id_ty, None).is_some()
    }

    fn get_all<'a, W: RWS>(
        &self,
        col_mask: u64,
        ps: &'a mut PageSerializer<W>,
    ) -> QueryData<'a, W> {
        let mut answer = Vec::new();
        let pages = ps.get_in_all(self.id_ty, None);
        for location in &pages {
            let table = ps.load_page_cached(*location);
            for i in 0..table.len() {
                let bytes = table.load_index(i as usize);
                let tuple = self.ty.read_tuple(bytes, col_mask, table.heap().get_ref());
                answer.push(tuple);
            }
        }
        QueryData::new(answer, pages, ps)
    }

    fn store_raw(&self, t: TupleBuilder, ps: &mut PageSerializer<impl RWS>) {
        assert!(t.type_check(&self.ty));
        let pkey = t.first();
        let (location, page) = match ps.get_in_all_insert(self.id_ty, pkey) {
            Some(location) => {
                ps.previous_headers
                    .update_limits(self.id_ty, location, pkey);
                let page = ps.load_page_cached(location);
                page.insert_tb(t);
                (location, page)
            }
            None => {
                let mut new_page = TableBase2::new(self.id_ty, self.ty.size() as usize);
                new_page.insert_tb(t);
                let location = new_page.force_flush(ps);
                (location, ps.load_page_cached(location))
            }
        };

        // If estimated flush size is >= 16000, then we should split page to avoid going over page size limit
        if page.serialized_len() >= 16000 {
            let old_min_limits = page.limits.min.unwrap();
            let newpage = page.split(&self.ty);
            if let Some(mut x) = newpage {
                assert!(!x.limits.overlaps(&page.limits));
                let page_limits = page.limits.clone();
                ps.previous_headers
                    .reset_limits(self.id_ty, old_min_limits, page_limits);
                x.force_flush(ps);
            }
        }

        ps.unpin_page(location);
    }

    fn new<W: Write + Read + Seek>(
        ty: DynamicTuple,
        id: u64,
        _ps: &mut PageSerializer<W>,
        columns: Vec<impl Into<String>>,
    ) -> Self {
        assert_eq!(columns.len(), ty.fields.len());
        println!("Creating dyntable {:?}", ty);

        Self {
            ty,
            id_ty: id,
            column_map: columns
                .into_iter()
                .enumerate()
                .map(|(ind, a)| (a.into(), ind as u32))
                .collect(),
        }
    }
}

#[derive(Debug, PartialEq)]
struct CreateTable {
    tbl_name: String,
    fields: Vec<(String, Type)>,
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
struct InsertValues {
    values: Vec<Vec<TypeData>>,
    tbl_name: String,
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
enum Filter {
    Equals(String, TypeData),
}

#[derive(Debug, PartialEq)]
struct Select {
    tbl_name: String,
    columns: Vec<String>,
    filter: Vec<Filter>,
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
            tt.get_in_all(i, 0, &mut ps).results(),
            vec![TupleBuilder::default()
                .add_int(i)
                .add_string(format!("hello{i}"))
                .add_string(format!("world{i}"))],
            "{}",
            i
        );
        assert_eq!(
            tt1.get_in_all(i, 0, &mut ps).results(),
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
            tables[i % 100].get_in_all(i as u64, 0, &mut ps).results(),
            vec![TupleBuilder::default()
                .add_int(i as u64)
                .add_string(format!("hello{i}"))
                .add_string(format!("world{i}"))]
        );
        assert_eq!(
            tables[i % 100].get_in_all(i as u64, 0, &mut ps1).results(),
            vec![TupleBuilder::default()
                .add_int(i as u64)
                .add_string(format!("hello{i}"))
                .add_string(format!("world{i}"))]
        );
    }
}

impl NamedTables {
    fn new(s: &mut PageSerializer<impl RWS>) -> Self {
        // Load schema table first
        let schema = TypedTable {
            ty: DynamicTuple {
                // TableID (64 bit type id), TableName, Column Name, Column Type,
                fields: vec![Type::Int, Type::String, Type::String, Type::Int],
            },
            id_ty: 2,
            column_map: Default::default(),
        };

        let mut tables = HashMap::new();

        let mut entry = tables.entry("schema".to_string()).insert_entry(schema);
        let schema = entry.get_mut();
        let mut large_id = 3;

        for tup in schema.get_all(0, s).results().into_iter().rev() {
            let id = tup.extract_int(0);
            let table_name = std::str::from_utf8(tup.extract_string(1)).unwrap();
            let column_name = std::str::from_utf8(tup.extract_string(2)).unwrap();
            let column_type = Type::from(tup.extract_int(3));

            let r = tables
                .entry(table_name.to_string())
                .or_insert_with(|| TypedTable {
                    ty: DynamicTuple::default(),
                    column_map: Default::default(),
                    id_ty: id,
                });
            println!("Adding column {} {}", table_name, column_name);
            r.column_map
                .insert(column_name.to_string(), r.ty.fields.len() as u32);
            r.ty.fields.push(column_type);
            large_id = large_id.max(id);
        }

        Self {
            tables,
            largest_id: large_id,
        }
    }

    fn insert_table(
        &mut self,
        CreateTable {
            tbl_name: name,
            fields: columns,
        }: CreateTable,
        ps: &mut PageSerializer<impl RWS>,
    ) {
        self.largest_id += 1;
        let table_id = self.largest_id;
        // First insert to schema table

        let schema_table = self.tables.get_mut("schema").unwrap();
        for (colname, col) in &columns {
            println!("Insert col {colname}");
            let tup = TupleBuilder::default()
                .add_int(table_id)
                .add_string(name.clone())
                .add_string(colname.clone())
                .add_int(*col as u64);
            schema_table.store_raw(tup, ps);
        }

        let types = columns.iter().map(|a| a.1).collect();
        let names = columns.into_iter().map(|a| a.0).collect();
        self.tables.insert(
            name,
            TypedTable::new(DynamicTuple { fields: types }, table_id, ps, names),
        );
    }

    fn execute_insert(&mut self, insert: InsertValues, ps: &mut PageSerializer<impl RWS>) {
        let table = self.tables.get_mut(&insert.tbl_name).unwrap();
        for t in insert.values {
            let tuple = TupleBuilder { fields: t };
            tuple.type_check(&table.ty);
            table.store_raw(tuple, ps);
        }
    }

    fn calculate_column_mask(table: &TypedTable, fields: &[String]) -> u64 {
        let mut mask = 0;
        if fields.is_empty() {
            return u64::MAX;
        }
        for f in fields {
            if f == "*" {
                mask = u64::MAX;
                return mask;
            }
            let index = table.column_map[f];
            assert!(index < 64);
            mask |= 1 << index;
        }
        mask
    }

    fn execute_select<'a, W: RWS>(
        &mut self,
        select: Select,
        ps: &'a mut PageSerializer<W>,
    ) -> QueryData<'a, W> {
        let table = self.tables.get_mut(&select.tbl_name).unwrap();
        let col_mask = Self::calculate_column_mask(table, &select.columns);

        let filter = select.filter;

        // TODO: only supports the first filter condition for now
        match filter.first() {
            Some(Filter::Equals(colname, TypeData::Int(icomp))) => {
                match table.column_map[colname] {
                    0 => table.get_in_all(*icomp, col_mask, ps),
                    colindex => {
                        let mut query_result = table.get_all(col_mask, ps);

                        query_result.filter(|i| match i.fields[colindex as usize] {
                            TypeData::Int(int) => int == *icomp,
                            TypeData::String(_) | TypeData::Null => panic!(),
                        });
                        query_result
                    }
                }
            }
            Some(Filter::Equals(colname, TypeData::String(s))) => {
                let colindex = table.column_map[colname];
                let mut qr = table.get_all(col_mask, ps);
                qr.filter(|i| match &i.fields[colindex as usize] {
                    TypeData::String(s1) => s1 == s,
                    _ => panic!(),
                });
                qr
            }
            None | Some(Filter::Equals(_, Null)) => table.get_all(col_mask, ps),
        }
    }
}

#[test]
fn test_sql_all() {
    let mut ps = PageSerializer::create(Cursor::new(Vec::new()), Some(16000));
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

    let mut ps = PageSerializer::create_from_reader(ps.move_file(), Some(16000));
    let mut nt = NamedTables::new(&mut ps);
    assert_eq!(
        parse_lex_sql(
            r#"SELECT id, name, telephone FROM tbl WHERE id EQUALS 4 "#,
            &mut nt,
            &mut ps
        )
        .unwrap()
        .results(),
        answer1
    );
    assert_eq!(
        parse_lex_sql(
            r#"SELECT id, fax FROM tbl1 WHERE fax EQUALS 3209324830294 "#,
            &mut nt,
            &mut ps
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
    let mut ps = PageSerializer::create(file, None);
    let mut nt = NamedTables::new(&mut ps);

    parse_lex_sql(
        "CREATE TABLE tbl (id INT, name STRING, telephone STRING)",
        &mut nt,
        &mut ps,
    );

    let mut indices: Vec<u64> = (0..1_000_00).collect();
    indices.shuffle(&mut thread_rng());
    let mut j = indices.iter().cycle();
    for _ in 0..1_000_00 {
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
    b.iter(|| {
        let j = *j.next().unwrap();
        let res1 = parse_lex_sql(
            &format!("SELECT * FROM tbl WHERE id EQUALS {j}"),
            &mut nt,
            &mut ps,
        );
        if let Some(r) = res1 {
            r.results();
        }
    });
}

#[test]
fn test_lots_inserts() {
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
    let mut ps = PageSerializer::create(file, Some(16000));
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

#[bench]
fn lots_inserts(b: &mut test::Bencher) -> impl std::process::Termination {
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

    let mut indices: Vec<u64> = (0..2_000_000).collect();
    indices.shuffle(&mut a);
    let mut j = indices.iter().cycle();
    let desc_string = String::from_utf8(vec![b'a'; 10]).unwrap();
    b.iter(|| {
        let j = j.next().unwrap();
        let i = j + 10;
        parse_lex_sql(&format!(r#"INSERT INTO tbl VALUES ({i}, "hello{i} world", "{i}" ,"{desc_string}"), ({j}, "hello{j} world", "{j}", "{desc_string}")"#), &mut nt, &mut ps);
    });

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

// Dynamic tuples automatically take up 100 bytes
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

gen_suitable_data_type_impls!(DynamicTupleInstance);
impl SuitableDataType for DynamicTupleInstance {
    fn first(&self) -> u64 {
        u64::from_le_bytes(self.data[0..8].try_into().unwrap())
    }
}
