use std::cell::Cell;
use dynamic_tuple::RWS;
use named_tables::NamedTables;
use query_data::QueryData;
use serializer::PageSerializer;
use type_data::{Type, TypeData};

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

pub fn parse_lex_sql<'a, W: RWS>(
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
