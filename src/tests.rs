#![cfg(test)]

use std::io::{Cursor, Write, Seek};
use std::ops::{Range as stdRange, RangeBounds};
use crate::*;
use crate::suitable_data_type::DataType;
use rand::random;
use std::fs::File;
use rand::prelude::SliceRandom;

#[test]
fn test_works_with_std_file() {
    let file = File::with_options().create(true).read(true).write(true).open("/tmp/test.db").unwrap();
    let db = DbManager::new(DbBase::<DataType>::default(), file);
    run_test_with_db(db);
}


#[test]
fn test_crash_in_middle() {
    let mut buf = Vec::new();
    let mut cursor = Cursor::new(&mut buf);
    let mut db = DbManager::new(DbBase::<DataType>::default(), cursor);

    let mut last_lens: Vec<usize> = Vec::new();
    for i in generate_int_range(0, 110) {
        db.store(DataType(i, i, i));

        if Some(&db.get_output_stream_len()) != last_lens.last() {
            last_lens.push(db.get_output_stream_len() as usize);
        }
    };
    println!("Last lens {}", last_lens.len());
    // Need at least 10 elements for the test to be effective
    assert!(last_lens.len() >= 10);
    std::mem::drop(db);
    let mut tuples = 0;
    for j in last_lens {
        if j == 0 {
            continue;
        }
        let mut current_tuples = 0;
        let mut b = Cursor::new(&buf[0..j]);

        while !b.is_empty() {
            let db = DbBase::<DataType>::from_reader(&mut b);
            current_tuples += db.data.len();
        }
        assert_eq!(current_tuples, tuples + 5);
        tuples = current_tuples;
    }
    assert_eq!(tuples, 110);
}

#[test]
fn test_range() {
    let test_range = Range { min: Some(DataType(3, 3, 3)), max: Some(DataType(10, 10, 10)) };
    assert!(!test_range.overlaps(&(15..20)));
    assert!(test_range.overlaps(&(7..20)));
}

#[test]
fn test_all_findable() {
    let mut solutions = Vec::new();
    let mut dbm = DbManager::new(DbBase::default(), Cursor::new(Vec::<u8>::new()));
    for i in generate_int_range(0, 200) {
        let val = DataType(i, random(), random());
        solutions.push(val.clone());
        dbm.store(val);
    }
    solutions.sort();

    for (iter, j) in solutions.iter().enumerate() {
        for (_iter1, j1) in solutions[iter..].iter().enumerate() {
            let range = j.first()..=j1.first();
            let mut res = dbm.get_in_all(j.first()..=j1.first());
            res.sort();
            assert_eq!(res, solutions.iter().filter_map(|a| range.contains(&a.first()).then(|| a.clone())).collect::<Vec<_>>());
        }
    }
}

#[test]
fn test_db_manager_vecu8() {
    let mut dbm: DbManager<DataType> = DbManager::new(DbBase::default(), Cursor::new(Vec::<u8>::new()));
    run_test_with_db(dbm);
}

// Generate Vec of unique, random integers in range [min, max)
fn generate_int_range<T>(min: T, max: T) -> Vec<T> where stdRange<T>: Iterator<Item=T> {
    let mut vec: Vec<_> = (min..max).collect();
    vec.shuffle(&mut rand::thread_rng());
    vec
}

fn run_test_with_db<T: Write + Read + Seek>(mut dbm: DbManager<DataType, T>) {
    let range: stdRange<u64> = 200..250;
    let mut expecting = Vec::new();

    for i in generate_int_range(0u8, 255u8) {
        println!("{}", i);
        let rand = i;
        dbm.store(DataType(rand, i, i));

        if range.contains(&(rand as u64)) {
            expecting.push(DataType(rand, i, i));
        }
    }

    let mut res = dbm.get_in_all(range);
    res.sort();
    expecting.sort();
    assert_eq!(res, expecting);
}

#[test]
fn test_key_lookup() {
    use rand::{thread_rng, Rng};
    use suitable_data_type::DataType;
    let mut db = DbBase::<DataType>::default();

    let mut rng = thread_rng();
    for i in generate_int_range(0, 10) {
        db.store(DataType(i * 4, rng.gen(), rng.gen()));
    }
    db.sort_self();
    dbg!(db.key_range(&(2..30)));
}

#[test]
fn test1() {
    use rand::{thread_rng};
    use suitable_data_type::DataType;
    let mut db = DbBase::<DataType>::default();

    let _rng = thread_rng();
    for i in generate_int_range(1, 40) {
        db.store(DataType(i, i, i));
    }
    let mut buffer: Vec<u8> = Vec::new();
    let old_data = db.force_flush(&mut buffer);

    println!("Hex: {:?}", buffer);

    let reader = buffer.as_slice();
    let mut reader_cursor = Cursor::new(reader);
    let db1 = DbBase::<DataType>::from_reader(&mut reader_cursor);
    assert_eq!(old_data, db1.data);
    dbg!(db1);
}

#[test]
fn test2() {
    use rand::{thread_rng, Rng};
    use suitable_data_type::DataType;
    use std::io::Cursor;

    let mut buffer: Vec<u8> = Vec::new();
    let mut dbs = Vec::new();
    for _ in 0..150 {
        let mut db = DbBase::<DataType>::default();

        let mut rng = thread_rng();
        for i in generate_int_range(0, 10) {
            db.store(DataType(i as u8, rng.gen(), rng.gen()));
        }
        let old_data = db.force_flush(&mut buffer);
        dbs.push(old_data);
    }

    let mut reader = Cursor::new(&buffer);


    for d in dbs {
        let db1 = DbBase::<DataType>::from_reader(&mut reader);
        assert_eq!(d, db1.data);
    }
}

#[test]
fn test3() {
    use rand::{thread_rng, Rng};
    use chunk_header::ChunkHeaderIndex;
    use suitable_data_type::DataType;

    let mut buffer: Vec<u8> = Vec::new();
    let mut dbs = Vec::new();
    for i in generate_int_range(0, 150) {
        let mut db = DbBase::<DataType>::default();

        let mut rng = thread_rng();
        for j in 0..10 {
            db.store(DataType(j, rng.gen(), rng.gen()));
        }
        let old_data = db.force_flush(&mut buffer);
        dbs.push(old_data);
    }

    let mut reader = Cursor::new(&buffer);

    let res = ChunkHeaderIndex::<DataType>::from_reader(&mut reader);

    assert_eq!(res.0.len(), dbs.len());
}
