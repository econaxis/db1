use std::path::Iter;
use dynamic_tuple::{DynamicTuple, TupleBuilder, Type, TypedTable};
use serializer::PageSerializer;
use table_base2::TableBase2;

struct Where<'a> {
    source: &'a mut dyn Iterator<Item=TupleBuilder>,
    condition: fn(&TupleBuilder) -> bool,
}

struct WhereByPkey<'a> {
    source: &'a mut TypedTable,
    pkey: u64
}

//
// impl<'a> Iterator for WhereByPkey<'a> {
//     type Item = TupleBuilder;
//
//     fn next(&mut self) -> Option<Self::Item> {
//
//     }
// }

impl<'a> Iterator for Where<'a> {
    type Item = TupleBuilder;

    fn next(&mut self) -> Option<Self::Item> {
        for i in &mut self.source {
            if (self.condition)(&i) {
                return Some(i);
            }
        }
        return None;
    }
}

#[test]
fn test_where_operator() {
    let mut ps = PageSerializer::default();
    let tt = TypedTable::new(
        DynamicTuple::new(vec![Type::Int, Type::String, Type::String]),
        10,
        &mut ps,
        vec!["id", "name", "content"],
    );
    let mut i = 0;
    while ps.get_in_all(tt.id_ty, None).count() < 2 {
        i += 1;
        let tb = TupleBuilder::default()
            .add_int(i)
            .add_string(format!("hello{i}"))
            .add_string(format!("world{i}"));
        tt.store_raw(tb, &mut ps);
    }
    let mut it = tt.get_in_all_iter(None, u64::MAX,&mut ps);
    let mut whereclause = Where {
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
    for i in whereclause2 {
        dbg!(i);
    };
}