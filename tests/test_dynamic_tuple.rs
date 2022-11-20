extern crate db2;
extern crate rand;

use db2::ra_ops::RANodeIterator;
use std::ffi::{c_char, CStr, CString};
use std::fs::File;
use std::io::Cursor;
use rand::prelude::SliceRandom;
use rand::{SeedableRng, thread_rng};
use db2::{dynamic_tuple, DynamicTuple, MAX_PAGE_SIZE, NamedTables, PageSerializer, parser, TupleBuilder, Type, TypeData, TypedTable};
use db2::parser::{CreateTable, Filter, InsertValues, Select};

#[test]
fn test_index_type_table2() {
    let mut ps = PageSerializer::default();
    let mut tt = TypedTable::new(DynamicTuple::new(vec![Type::String, Type::String]), 10, &mut ps, vec!["a", "b"]);

    for i in 0..11_100u64 {
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
    let mut ps = PageSerializer::create(Cursor::new(Vec::new()), Some(MAX_PAGE_SIZE));
    let mut nt = NamedTables::new(&mut ps);

    parser::parse_lex_sql(
        "CREATE TABLE tbl (id INT, name STRING, telephone STRING)",
        &mut nt,
        &mut ps,
    );
    parser::parse_lex_sql(
        r#"INSERT INTO tbl VALUES (3, "hello3 world", "30293204823")"#,
        &mut nt,
        &mut ps,
    );
    parser::parse_lex_sql(
        r#"INSERT INTO tbl VALUES (4, "hello4 world", "3093204823")"#,
        &mut nt,
        &mut ps,
    );
    parser::parse_lex_sql(
        r#"INSERT INTO tbl VALUES (5, "hello5 world", "3293204823")"#,
        &mut nt,
        &mut ps,
    );
    parser::parse_lex_sql(
        "CREATE TABLE tbl1 (id INT, name STRING, fax INT)",
        &mut nt,
        &mut ps,
    );
    parser::parse_lex_sql(
        r#"INSERT INTO tbl VALUES (6, "hello6 world", "0293204823")"#,
        &mut nt,
        &mut ps,
    );
    parser::parse_lex_sql(
        r#"INSERT INTO tbl1 VALUES (7, "hello7 world", 293204823), (9, "hellfdsoa f", 3209324830294)"#,
        &mut nt,
        &mut ps,
    );
    parser::parse_lex_sql(
        r#"INSERT INTO tbl1 VALUES (8, "hello8 world", 3209324830294)"#,
        &mut nt,
        &mut ps,
    );
    let answer1 = parser::parse_lex_sql(
        r#"SELECT id, name, telephone FROM tbl WHERE id EQUALS 4"#,
        &mut nt,
        &mut ps,
    )
        .unwrap()
        .results();
    let answer2 = parser::parse_lex_sql(
        r#"SELECT id, fax FROM tbl1 WHERE fax EQUALS 3209324830294 "#,
        &mut nt,
        &mut ps,
    )
        .unwrap()
        .results();
    dbg!(&answer1, &answer2);

    let mut ps = PageSerializer::create_from_reader(ps.move_file(), Some(MAX_PAGE_SIZE));
    let mut nt = NamedTables::new(&mut ps);
    assert_eq!(
        parser::parse_lex_sql(
            r#"SELECT id, name, telephone FROM tbl WHERE id EQUALS 4 "#,
            &mut nt,
            &mut ps,
        )
            .unwrap()
            .results(),
        answer1
    );
    assert_eq!(
        parser::parse_lex_sql(
            r#"SELECT id, fax FROM tbl1 WHERE fax EQUALS 3209324830294 "#,
            &mut nt,
            &mut ps,
        )
            .unwrap()
            .results(),
        answer2
    );
}

#[test]
fn test_selects() {

    let file = File::options()
        .truncate(true)
        .create(true)
        .read(true)
        .write(true)
        .open("/tmp/test_selects")
        .unwrap();
    let mut ps = PageSerializer::create(file, Some(MAX_PAGE_SIZE));
    let mut nt = NamedTables::new(&mut ps);

    parser::parse_lex_sql(
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
        parser::parse_lex_sql(
            &format!(
                r#"INSERT INTO tbl VALUES ({i}, "hello{i} world", "{i}"), ({j}, "hello{j} world", "{j}")"#
            ),
            &mut nt,
            &mut ps,
        );
    }
    for _ in 0..1000 {
        let j = *j.next().unwrap();
        let res1 = parser::parse_lex_sql(
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

    let mut a = rand_chacha::ChaCha20Rng::seed_from_u64(1);
    let file = File::options()
        .truncate(true)
        .create(true)
        .read(true)
        .write(true)
        .open("/tmp/test-inserts")
        .unwrap();
    let mut ps = PageSerializer::create(file, Some(MAX_PAGE_SIZE));
    let mut nt = NamedTables::new(&mut ps);

    parser::parse_lex_sql(
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
        parser::parse_lex_sql(
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

    parser::parse_lex_sql(
        "CREATE TABLE tbl (id INT, name STRING, telephone STRING, description STRING)",
        &mut nt,
        &mut ps,
    );

    let mut indices: Vec<u64> = (0..0_111_000).collect();
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

#[test]
fn test_sql_c_api() {
    unsafe {
        let tb = dynamic_tuple::sql_new(
            CStr::from_bytes_with_nul(b"/tmp/test_sql_c_api.db\0")
                .unwrap()
                .as_ptr(),
        );
        let q1 = CString::new("CREATE TABLE tbl1 (pkey INT, telephone INT, a STRING)").unwrap();
        let q2 = CString::new(r#"INSERT INTO tbl1 VALUES (1, 90328023, "hello"), (2, 32084432, "world"), (3, 32084432, "world"), (4, 32084432, "world")"#).unwrap();
        let q3 = CString::new(r#"SELECT pkey, a FROM tbl1 WHERE a EQUALS "world""#).unwrap();
        let q4 = CString::new("SELECT pkey, a FROM tbl1").unwrap();
        dynamic_tuple::sql_exec(tb, q1.as_ptr());
        dynamic_tuple::sql_exec(tb, q2.as_ptr());
        println!(
            "{}",
            CStr::from_ptr(dynamic_tuple::sql_exec(tb, q3.as_ptr() as *const c_char))
                .to_str()
                .unwrap()
        );
        println!(
            "{}",
            CStr::from_ptr(dynamic_tuple::sql_exec(tb, q4.as_ptr() as *const c_char))
                .to_str()
                .unwrap()
        );
    }
}
