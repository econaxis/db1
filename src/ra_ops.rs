use std::io::Cursor;
use dynamic_tuple::{DynamicTuple, RWS, TupleBuilder, Type, TypeData};
use serializer::PageSerializer;
use crate::typed_table::TypedTable;


struct Where<'a, W: RWS> {
    source: &'a mut dyn RANodeIterator<W>,
    condition: fn(&TupleBuilder) -> bool,
}

struct WhereByPkey<'a> {
    source: &'a mut TypedTable,
    pkey: Option<TypeData>
}


struct NestedLoopInnerJoin<'a, 'b, W: RWS> {
    left: &'a mut dyn RANodeIterator<W>,
    right: &'b mut dyn RANodeIterator<W>,
    left_col: u64,
    right_col: u64,
    result: Option<Vec<TupleBuilder>>
}

impl<'a, 'b, W: RWS> RANodeIterator<W> for NestedLoopInnerJoin<'a, 'b, W>{
    fn next(&mut self, ps: &mut PageSerializer<W>) -> Option<TupleBuilder> {
        if self.result.is_none() {
            let mut output = Vec::new();
            let right = self.right.collect(ps);

            while let Some(l) = self.left.next(ps) {
                for r in &right {
                    let left_id = l.extract(self.left_col as usize);
                    let right_id = r.extract(self.right_col as usize);
                    if left_id == right_id {
                        output.push(l.clone().append(r.clone()))
                    }
                }
            };
            self.result = Some(output);
        }

        self.result.as_mut().unwrap().pop()
    }
}

pub(crate) trait RANodeIterator<W: RWS> {
    fn next(&mut self, ps: &mut PageSerializer<W>) -> Option<TupleBuilder>;
    fn collect(&mut self, ps: &mut PageSerializer<W>) -> Vec<TupleBuilder> {
        let mut vec = Vec::new();
        while let Some(x) = self.next(ps) {
            vec.push(x);
        }
        vec
    }
}

impl<'a, W: RWS> RANodeIterator<W> for WhereByPkey<'a> {
    fn next(&mut self, ps: &mut PageSerializer<W>) -> Option<TupleBuilder> {
        if self.pkey.is_some() {
            let pk = self.pkey.take().unwrap();
            let mut cursor = self.source.get_in_all_iter(Some(pk), u64::MAX, ps);
            cursor.next(ps)
        } else {
            None
        }
    }
}



impl<'a, W: RWS> RANodeIterator<W> for Where<'a, W> {
    fn next(&mut self, ps: &mut PageSerializer<W>) -> Option<TupleBuilder> {
        while let Some(i) = self.source.next(ps) {
            if (self.condition)(&i) {
                return Some(i);
            }
        }
        None
    }
}


#[test]
fn where_by_pkey() {
    let (mut ps, mut tt) = init_test_table();
    let mut where_by_pkey = WhereByPkey {
        source: &mut tt,
        pkey: Some(TypeData::Int(300))
    };

    loop {
        match where_by_pkey.next(&mut ps) {
            Some(i) => dbg!(i),
            None => break
        };
    }
}

#[test]
fn test_where_operator() {
    let (mut ps, mut tt) = init_test_table();
    let mut it = tt.get_in_all_iter(None, u64::MAX,&mut ps);
    let mut whereclause = Where::<Cursor<Vec<u8>>> {
        source: &mut it,
        condition: |a: &TupleBuilder| -> bool {
            let str = a.extract_string(2);
            let numbers = &str[5..];
            let number: i32 = std::str::from_utf8(numbers).unwrap().parse().unwrap();
            number % 3 == 0
        },
    };
    let whereclause2 = Where {
        source: &mut whereclause,
        condition: |a: &TupleBuilder| -> bool {
            let str = a.extract_string(2);
            let digit = &str[5..6];
            let number: i32 = std::str::from_utf8(digit).unwrap().parse().unwrap();
            number == 1 || number == 7
        },
    };
}

#[test]
fn nested_loop() {
    let (mut ps, mut tt) = init_test_table();

    let tt1 = TypedTable::new(DynamicTuple::new(vec![Type::Int, Type::String]), 11, &mut ps, vec!["id", "content"]);

    for i in 0..2000 {
        tt1.store_raw(TupleBuilder::default().add_int(i).add_string(format!("hello{}", i * 13)), &mut ps);
    }

    let mut nl = NestedLoopInnerJoin {
        left: &mut tt.get_in_all_iter(None, u64::MAX, &mut ps),
        right: &mut tt1.get_in_all_iter(None, u64::MAX, &mut ps),
        left_col: 0,
        right_col: 0,
        result: None
    };

    dbg!(nl.collect(&mut ps));
}
fn init_test_table() -> (PageSerializer<Cursor<Vec<u8>>>, TypedTable) {
    let mut ps = PageSerializer::default();
    let tt = TypedTable::new(
        DynamicTuple::new(vec![Type::Int, Type::String, Type::String]),
        10,
        &mut ps,
        vec!["id", "name", "content"],
    );
    let mut i = 0;
    while ps.get_in_all(tt.id_ty, None).len() < 4 {
        i += 1;
        let tb = TupleBuilder::default()
            .add_int(i)
            .add_string(format!("hello{i}"))
            .add_string(format!("world{i}"));
        tt.store_raw(tb, &mut ps);
    }
    (ps, tt)
}