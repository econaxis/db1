#![cfg(test)]

use std::io::{Cursor, Write, Seek, SeekFrom};
use std::ops::{Range as stdRange};
use crate::*;
use crate::suitable_data_type::{DataType};
use rand::random;
use std::fs::File;
use rand::prelude::SliceRandom;
use crate::table_base::TableBase;

#[test]
fn test_heap_struct() {
    #[derive(PartialEq, PartialOrd, Eq, Ord, Clone, Debug)]
    struct HeapTest {
        a: String
    }
    impl SuitableDataType for HeapTest {
        const REQUIRES_HEAP: bool = true;
        const TYPE_SIZE: u64 = 18;
        fn first(&self) -> u64 {
            todo!()
        }
    }
    impl PartialEq<u64> for HeapTest {
        fn eq(&self, _other: &u64) -> bool {
            todo!()
        }
    }
    impl PartialOrd<u64> for HeapTest {
        fn partial_cmp(&self, _other: &u64) -> Option<Ordering> {
            todo!()
        }
    }
    impl BytesSerialize for HeapTest {
        fn serialize_with_heap<W: Write, W1: Write + Seek>(&self, data: W, heap: W1) {
            self.a.serialize_with_heap(data, heap)
        }
    }
    impl FromReader for HeapTest {
        fn from_reader_and_heap<R: Read>(r: R, heap: &[u8]) -> Self {
            Self {a: String::from_reader_and_heap(r, heap)}
        }
    }

    let mut db = TableBase::<HeapTest>::default();
    db.store(HeapTest {a: "abcdef12356".to_string()});
    db.store(HeapTest {a: "fdasfdsa".to_string()});

    let mut c = Cursor::new(Vec::new());
    let (_, result)  = db.force_flush(&mut c);

    c.seek(SeekFrom::Start(0)).unwrap();
    let d = TableBase::<HeapTest>::from_reader_and_heap(&mut c, &[]);
    assert_eq!(&result, &[HeapTest {a: "abcdef12356".to_string()}, HeapTest {a: "fdasfdsa".to_string()}]);
    assert_eq!(&result, d.get_data());
}

#[test]
fn test_editable() {
    let mut db = TableManager::default();
    db.store(DataType(0, 0, 0));
    db.force_flush();
    db.store_and_replace(DataType(1, 2, 2));
    db.store_and_replace(DataType(0, 1, 1));
    db.force_flush();
    assert_eq!(db.get_in_all(0..=1), [DataType(0, 1, 1), DataType(1, 2, 2)]);
}

#[test]
fn test_works_with_std_file() {
    let file = File::with_options().create(true).read(true).write(true).open("/tmp/test.db").unwrap();
    let db = TableManager::new(file);
    run_test_with_db(db);
}


#[test]
fn test_crash_in_middle() {
    let mut buf = Vec::new();
    let cursor = Cursor::new(&mut buf);
    let mut db = TableManager::new(cursor);

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
            let db = TableBase::<DataType>::from_reader_and_heap(&mut b, &[]);
            current_tuples += db.len();
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
    let mut dbm = TableManager::default();
    for i in generate_int_range(0, 100) {
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
    let dbm: TableManager<DataType> = TableManager::default();
    run_test_with_db(dbm);
}


// Generate Vec of unique, random integers in range [min, max)
fn generate_int_range<T>(min: T, max: T) -> Vec<T> where stdRange<T>: Iterator<Item=T> {
    let mut vec: Vec<_> = (min..max).collect();
    vec.shuffle(&mut rand::thread_rng());
    vec
}

fn run_test_with_db<T: Write + Read + Seek>(mut dbm: TableManager<DataType, T>) {
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
    let mut db = TableBase::<DataType>::default();

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
    let mut db = TableBase::<DataType>::default();

    let _rng = thread_rng();
    for i in generate_int_range(1, 40) {
        db.store(DataType(i, i, i));
    }
    let mut buffer: Vec<u8> = Vec::new();
    let (_, old_data) = db.force_flush(&mut buffer);

    println!("Hex: {:?}", buffer);

    let reader = buffer.as_slice();
    let mut reader_cursor = Cursor::new(reader);
    let db1 = TableBase::<DataType>::from_reader_and_heap(&mut reader_cursor, &[]);
    assert_eq!(&old_data, db1.get_data());
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
        let mut db = TableBase::<DataType>::default();

        let mut rng = thread_rng();
        for i in generate_int_range(0, 10) {
            db.store(DataType(i as u8, rng.gen(), rng.gen()));
        }
        let old_data = db.force_flush(&mut buffer);
        dbs.push(old_data.1);
    }

    let mut reader = Cursor::new(&buffer);


    for d in dbs {
        let db1 = TableBase::<DataType>::from_reader_and_heap(&mut reader, &[]);
        assert_eq!(&d, db1.get_data());
    }
}

#[test]
fn test3() {
    use rand::{thread_rng, Rng};
    use chunk_header::ChunkHeaderIndex;
    use suitable_data_type::DataType;

    let mut buffer: Vec<u8> = Vec::new();
    let mut dbs = Vec::new();
    for _ in generate_int_range(0, 150) {
        let mut db = TableBase::<DataType>::default();

        let mut rng = thread_rng();
        for j in 0..10 {
            db.store(DataType(j, rng.gen(), rng.gen()));
        }
        let old_data = db.force_flush(&mut buffer);
        dbs.push(old_data);
    }

    let mut reader = Cursor::new(&buffer);

    let res = ChunkHeaderIndex::<DataType>::from_reader_and_heap(&mut reader, &[]);

    assert_eq!(res.0.len(), dbs.len());
    assert_eq!(res.0.len(), 150);


    fn dt_full_compare(one: &Option<DataType>, two: &Option<DataType>) -> bool {
        let one = one.as_ref().unwrap();
        let two = two.as_ref().unwrap();
        one.0 == two.0 && one.1 == two.1 && one.2 == two.2
    }
    for ((_pos, chunk1), (chunk2, _data)) in res.0.iter().zip(dbs.iter()) {
        assert!(dt_full_compare(&chunk1.limits.min, &chunk2.limits.min));
        assert!(dt_full_compare(&chunk1.limits.max, &chunk2.limits.max));
    }

    reader.seek(SeekFrom::Start(0)).unwrap();
    let mut test_out_stream = heap_writer::heap_writer();
    let tbm = TableManager::<DataType>::read_from_file(reader, test_out_stream);
    assert_eq!(tbm.get_prev_headers(), &res);
    dbg!(tbm);
}
