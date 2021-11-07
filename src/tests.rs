#![cfg(test)]

use std::fs::File;
use std::io::{Cursor, Seek, SeekFrom, Write};
use std::ops::Range as stdRange;

use rand::{random, Rng};
use rand::distributions::Alphanumeric;
use rand::prelude::SliceRandom;

use crate::*;
use crate::db1_string::Db1String;
use crate::suitable_data_type::DataType;
use crate::table_base::TableBase;

fn rand_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}
#[test]
fn test_heap_struct() {
    #[derive(PartialEq, PartialOrd, Eq, Ord, Clone, Debug)]
    struct HeapTest {
        a: Db1String,
    }
    impl SuitableDataType for HeapTest {
        const REQUIRES_HEAP: bool = true;
        const TYPE_SIZE: u64 = 18;
        fn first(&self) -> u64 {
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
            Self {
                a: Db1String::from_reader_and_heap(r, heap),
            }
        }
    }

    let mut db = TableBase::<HeapTest>::default();

    let mut s = Vec::new();
    for _ in 0..10 {
        let val = HeapTest {
            a: rand_string(rand::thread_rng().gen_range(0..10)).into(),
        };
        db.store_and_replace(val.clone());
        if s.iter().find(|a| &&val == a) == None {
            s.push(val);
        }
    }
    s.sort();

    let mut c = Cursor::new(Vec::new());
    let (_, result) = db.force_flush(&mut c);
    assert_eq!(&result, s.as_slice());

    c.seek(SeekFrom::Start(0)).unwrap();
    let mut d = TableBase::<HeapTest>::from_reader_and_heap(&mut c, &[]);
    let heap = d.heap.as_slice();
    let mut d = d.get_data().clone();
    for elem in &mut d {
        elem.a.resolve(heap);
    }

    assert_eq!(&result, &d);
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
    let file = File::with_options()
        .create(true)
        .read(true)
        .write(true)
        .open("/tmp/test.db")
        .unwrap();
    let db = TableManager::new(file);
    run_test_with_db(db);
}

#[test]
fn test_crash_in_middle() {
    let mut buf = Vec::new();
    let cursor = Cursor::new(&mut buf);
    let mut db = TableManager::new(cursor);
    let flush_size = TableManager::<DataType>::FLUSH_CUTOFF;

    let mut last_lens: Vec<usize> = Vec::new();
    for i in generate_int_range(0, 5 * flush_size) {
        let i = i as u8;
        db.store_and_replace(DataType(i, i, i));

        if Some(&db.get_output_stream_len()) != last_lens.last() {
            last_lens.push(db.get_output_stream_len() as usize);
        }
    }
    println!("Last lens {}", last_lens.len());
    assert!(
        last_lens.len() >= 4,
        "Number of different chunks must be larger than 10 for test to be effective"
    );
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
        assert_eq!(current_tuples, tuples + flush_size as usize);
        tuples = current_tuples;
    }
}

#[test]
fn test_range() {
    let test_range = Range {
        min: Some(DataType(3, 3, 3)),
        max: Some(DataType(10, 10, 10)),
    };
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
            assert_eq!(
                res,
                solutions
                    .iter()
                    .filter_map(|a| range.contains(&a.first()).then(|| a.clone()))
                    .collect::<Vec<_>>()
            );
        }
    }
}

#[test]
fn test_db_manager_vecu8() {
    let dbm: TableManager<DataType> = TableManager::default();
    run_test_with_db(dbm);
}

// Generate Vec of unique, random integers in range [min, max)
fn generate_int_range<T>(min: T, max: T) -> Vec<T>
where
    stdRange<T>: Iterator<Item = T>,
{
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
    use rand::thread_rng;
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
    use std::io::Cursor;
    use suitable_data_type::DataType;

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
    use chunk_header::ChunkHeaderIndex;
    use rand::{thread_rng, Rng};
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
    let test_out_stream = heap_writer::default_mem_writer();
    let tbm = TableManager::<DataType>::read_from_file(reader, test_out_stream);
    assert_eq!(tbm.get_prev_headers(), &res);
    dbg!(tbm);
}

fn rand_range(max: u8) -> u8 {
    rand::random::<u8>() % max
}

#[test]
fn test_edits_valid() {
    const LENGTH: u8 = 255;
    let mut possible_values = [0u8; LENGTH as usize];
    let mut dbm = TableManager::<DataType>::default();

    for i in 0..possible_values.len() {
        let i = i as u8;
        dbm.store(DataType(i, 0, 0));
    }
    for _ in 0..10000 {
        let new_value = DataType(rand_range(LENGTH), rand_range(LENGTH), 0);
        possible_values[new_value.0 as usize] = new_value.1;
        dbm.store_and_replace(new_value);
    }

    for (index, i) in possible_values.iter().enumerate() {
        let index = index as u64;
        let val = dbm.get_in_all(index..=index);
        match val.as_slice() {
            [a] => {
                assert_eq!(a.1, *i)
            }
            _ => panic!(),
        }
    }
}
