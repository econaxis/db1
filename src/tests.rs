#![cfg(test)]

use std::cell::RefCell;
use std::collections::HashSet;
use std::hash::Hash;
use std::io::{Cursor, Seek, SeekFrom, Write};
use std::ops::{Range as stdRange, RangeFull};

use rand::{random, Rng};
use rand::distributions::Alphanumeric;
use rand::prelude::SliceRandom;
use rand::SeedableRng;
use serializer::{DbPageManager, PageSerializer};

use crate::*;
use crate::db1_string::Db1String;
use crate::index::ImageDocument;
use crate::suitable_data_type::DataType;
use crate::table_base::TableBase;
use crate::table_traits::{BasicTable};

pub fn rand_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn vec_equals<T: PartialEq>(a: &Vec<T>, b: &Vec<&T>) -> bool {
    if a.len() != b.len() {
        return false;
    }
    for i in 0..a.len() {
        if &a[i] != b[i] { return false; }
    }
    true
}


#[test]
fn test_editable() {
    let mut db = TableManager::default();
    db.store(DataType(0, 0, 0));
    db.force_flush();
    db.store_and_replace(DataType(1, 2, 2));
    db.store_and_replace(DataType(0, 1, 1));
    db.force_flush();
    assert_eq!(&vec![DataType(0, 1, 1), DataType(1, 2, 2)], db.get_in_all(0..=1, u8::MAX));
}

#[test]
fn test_works_with_std_file() {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open("/tmp/test.db")
        .unwrap();
    let db = TableManager::new(file);
    run_test_with_db(db);
}

// #[test]
// fn test_crash_in_middle() {
//     let mut buf = Vec::new();
//     let cursor = Cursor::new(&mut buf);
//     let mut db = TableManager::new(cursor);
//     let flush_size = TableManager::<DataType>::FLUSH_CUTOFF;
//
//     let mut last_lens: HashSet<usize> = HashSet::new();
//     for i in generate_int_range(0, 5 * flush_size) {
//         let i = i as u8;
//         db.store_and_replace(DataType(i, i, i));
//
//         last_lens.insert(db.get_output_stream_len() as usize);
//     }
//     println!("Last lens {}", last_lens.len());
//     assert!(
//         last_lens.len() >= 4,
//         "Number of different chunks must be larger than 10 for test to be effective"
//     );
//     std::mem::drop(db);
//     let mut tuples = 0;
//     for j in last_lens {
//         if j == 0 {
//             continue;
//         }
//         let mut current_tuples = 0;
//         let mut b = Cursor::new(&buf[0..j]);
//
//         while !b.is_empty() {
//             let db = TableBase::<DataType>::from_reader_and_heap(&mut b, &[]);
//             current_tuples += db.len();
//         }
//         assert_eq!(current_tuples, tuples + flush_size as usize);
//         tuples = current_tuples;
//     }
// }

#[test]
fn test_range() {
    let test_range = Range {
        min: Some(3),
        max: Some(13),
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
    solutions.sort_by_key(DataType::first);

    for (iter, j) in solutions.iter().enumerate() {
        for (_iter1, j1) in solutions[iter..].iter().enumerate() {
            let range = j.first()..=j1.first();
            let mut res = dbm.get_in_all(j.first()..=j1.first(), u8::MAX).clone();
            res.sort_by_key(DataType::first);
            assert_eq!(
                solutions
                    .iter()
                    .filter_map(|a| range.contains(&a.first()).then(|| a.clone()))
                    .collect::<Vec<_>>(),
                res
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
        stdRange<T>: Iterator<Item=T>,
{
    let mut vec: Vec<_> = (min..max).collect();
    vec.shuffle(&mut rand::thread_rng());
    vec
}

fn run_test_with_db<T: Write + Read + Seek>(mut dbm: TableManager<DataType, TableBase<DataType>, T>) {
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

    let mut res = dbm.get_in_all(range, u8::MAX).clone();
    res.sort_by_key(|a| a.first());
    expecting.sort_by_key(DataType::first);

    assert_eq!(expecting, res);
}

pub fn mess_up<W: Read + Write + Seek>(a: &mut PageSerializer<W>) {
    let len: usize = rand::random::<u8>() as usize + 100;
    let bytes = rand_string(len).into_bytes();
    let mut buf = Cursor::new(bytes);
    a.add_page(buf, len as u64, ChunkHeader {
        ty: 3,
        type_size: 0,
        length: 0,
        heap_size: 0,
        limits: Default::default(),
        compressed_size: 0
    });
}

fn data_type_test<T: SuitableDataType + Hash + PartialEq, Table>(mut creator: Box<dyn FnMut(u64) -> T>)
    where Table: BasicTable<T> {
    let mut writer = Cursor::default();
    let mut dbm = TableManager::<T, Table, &mut Cursor<Vec<u8>>>::new(&mut writer);

    for i in 0..1000 {
        dbm.store_and_replace(creator(i));
        mess_up(dbm.serializer());
    }

    dbm.force_flush();
    let mut dbm_data = dbm.get_in_all(RangeFull, u8::MAX).clone();
    std::mem::drop(dbm);
    println!("Writer size: {}", writer.position());
    writer.seek(SeekFrom::Start(0));
    let writer1 = writer.clone();
    let mut dbm1 = TableManager::<T, Table>::read_from_file(writer1);

    let mut dbm1_data = dbm1.get_in_all(RangeFull, u8::MAX).clone();

    dbm_data.sort_by_key(T::first);
    dbm1_data.sort_by_key(T::first);
    for (a, b) in dbm1_data.iter().zip(dbm_data.iter()) {
        assert_eq!(a, b);
    }
    assert_eq!(dbm_data, dbm1_data);
    dbg!(dbm_data, dbm1_data);
}

#[test]
fn image_doc_test() {
    data_type_test::<ImageDocument, TableBase<ImageDocument>>(Box::new(|i| ImageDocument::new(i, rand_string(30), rand_string(30), rand_string(30))));
}

#[test]
fn document_doc_test() {
    data_type_test::<Document, TableBase<Document>>(Box::new(|i| Document {
        id: i as u32,
        name: "hfel".into(),
        document: "fdlksaf sa".into(),
    }));
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
    dbg!(db.key_range_resolved(2..30));
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


thread_local! {
    // pub static RAND: RefCell<ChaCha20Rng> = RefCell::new(ChaCha20Rng::from_entropy());
    pub static RAND: RefCell<rand_chacha::ChaCha20Rng> = RefCell::new(rand_chacha::ChaCha20Rng::seed_from_u64(1));
}
fn rand_range(max: u8) -> u8 {
    RAND.with(|rand| {
        let mut r = rand.borrow_mut();
        r.gen::<u8>() % max
    })
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
    for _ in 0..1000 {
        let new_value = DataType(rand_range(LENGTH), rand_range(LENGTH), 0);
        possible_values[new_value.0 as usize] = new_value.1;
        dbm.store_and_replace(new_value);
    }
    println!("Checking");
    for (index, i) in possible_values.iter().enumerate() {
        let index = index as u64;


        let val = dbm.get_in_all(index..=index, u8::MAX);
        match val.as_slice() {
            [a] => {
                assert_eq!(a.1, *i)
            }
            _ => {
                dbg!("{:?}", val);
                panic!()
            }
        }
    }
}


#[test]
fn compaction() {
    let mut tbm = TableManager::<Document>::default();
    for i in 0..3000 {
        tbm.store(Document {
            id: i % 1000,
            name: Db1String::from("hello world"),
            document: Db1String::from("hfdalkd salfd"),
        });
        tbm.force_flush();
    }

    tbm.compact();
    dbg!(tbm.get_in_all(0..3, u8::MAX));
    let mut stream = tbm.inner_stream();

    stream.set_position(0);
    let mut tbm = TableManager::<Document>::read_from_file(stream);
    dbg!(tbm.get_in_all(0..3, u8::MAX));
}

#[test]
fn use_table_manager_with_hash() {
    let mut tbm = TableManager::<DataType, TableBase<DataType>>::new(Cursor::default());

    for i in 0..1000 {
        tbm.store(DataType((i % 255) as u8, 1, 1));
    }
    for i in 0..1000 {
        let i = (i % 255) as u8;
        assert_eq!(tbm.get_in_all(i as u64..=i as u64, u8::MAX), &[DataType(i, 1, 1)]);
    }
}