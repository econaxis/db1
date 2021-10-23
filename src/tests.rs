#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::ops::{Range as stdRange, Bound};
    use crate::*;
    use crate::suitable_data_type::DataType;
    use rand::random;
    use std::collections::BTreeSet;

    #[test]
    fn test_range() {
        let test_range = Range { min: Some(DataType(3, 3, 3)), max: Some(DataType(10, 10, 10)) };
        assert!(!test_range.overlaps(&(15..20)));
        assert!(test_range.overlaps(&(7..20)));
    }

    #[test]
    fn test_all_findable() {
        let mut solutions = Vec::new();
        let mut dbm = DbManager::new(DbBase::default());
        for _ in 0..200 {
            let val = DataType(random::<u8>() % 80, random(), random());
            solutions.push(val.clone());
            dbm.store(val);
        }
        solutions.sort();

        for (iter, j) in solutions.iter().enumerate() {
            for (iter1, j1) in solutions[iter..].iter().enumerate() {
                let range = j.first()..=j1.first();
                let mut res = dbm.get_in_all(j.first()..=j1.first());
                res.sort();
                assert_eq!(res, solutions.iter().filter_map(|a| range.contains(&a.first()).then(|| a.clone())).collect::<Vec<_>>());
            }
        }
    }

    #[test]
    fn test_dbmanager() {
        let mut dbm: DbManager<DataType> = DbManager::new(DbBase::default());
        let range: stdRange<u64> = 200..250;
        let mut expecting = Vec::new();
        for i in 0..255 {
            let rand = i as u64;
            let rand = (rand * rand * rand + 103238) % 255;
            let rand = rand as u8;
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
        for i in 0..10 {
            db.store(DataType(i * 4, rng.gen(), rng.gen()));
        }

        dbg!(db.key_lookup(8));
        dbg!(db.key_range(&(2..30)));
    }

    #[test]
    fn test1() {
        use rand::{thread_rng};
        use suitable_data_type::DataType;
        let mut db = DbBase::<DataType>::default();

        let _rng = thread_rng();
        for i in 10u8..40u8 {
            let mult: u8 = rand::random();
            db.store(DataType(mult, i, i));
        }
        let mut buffer: Vec<u8> = Vec::new();
        let old_data = db.force_flush(&mut buffer);

        println!("Hex: {:?}", buffer);

        let reader = buffer.as_slice();
        let db1 = DbBase::<DataType>::from_reader(reader);
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
            for _ in 0..10 {
                db.store(DataType(rng.gen(), rng.gen(), rng.gen()));
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
        for i in 0..150 {
            let mut db = DbBase::<DataType>::default();

            let mut rng = thread_rng();
            for _ in 0..10 {
                db.store(DataType(i, rng.gen(), rng.gen()));
            }
            let old_data = db.force_flush(&mut buffer);
            dbs.push(old_data);
        }

        let mut reader = Cursor::new(&buffer);

        let res = ChunkHeaderIndex::<DataType>::from_reader(&mut reader);

        assert_eq!(res.0.len(), dbs.len());
    }
}
