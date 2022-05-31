use dynamic_tuple::{RWS, TupleBuilder, Type, TypeData};
use serializer::PageSerializer;
use crate::dynamic_tuple::CreateTable;
use crate::named_tables::NamedTables;
use crate::typed_table::TypedTable;

#[derive(Clone, Debug)]
struct IndexDescriptor {
    on_column: u64,
    raw_table: TypedTable,
}


#[derive(Default, Clone, Debug)]
pub struct SecondaryIndices {
    indices: Vec<IndexDescriptor>,
}

#[test]
fn secondaryindices_works() {
    let mut ps = PageSerializer::default();
    let ps = &mut ps;
    let mut nt = NamedTables::new(ps);

    let mut si = SecondaryIndices::default();
    si.append_secondary_index(ps, &mut nt, "SI", Type::String, Type::Int, 1);

    for i in 0..350_000 {
        si.store(ps, TupleBuilder::default().add_int(i).add_string(i.to_string()));
    }
    for i in 0..350_000 {
        assert_eq!(si.query(ps, 1, TypeData::String(i.to_string().into()))[0], TypeData::Int(i))
    }
}

impl SecondaryIndices {
    // fn init<W: RWS>(ps: &mut PageSerializer<W>, nt: &mut NamedTables)
    fn append_secondary_index<W: RWS>(&mut self, ps: &mut PageSerializer<W>, nt: &mut NamedTables, name: &str, value_ty: Type, pkey_ty: Type, on_column: u64) {
        let cr = CreateTable {
            tbl_name: name.to_string(),
            fields: vec![("value".to_string(), value_ty), ("row".to_string(), pkey_ty)],
        };
        nt.insert_table(cr, ps);

        let raw_table = nt.tables.get(name).unwrap().clone();
        self.indices.push(IndexDescriptor {
            on_column,
            raw_table,
        })
    }
    fn store<W: RWS>(&mut self, ps: &mut PageSerializer<W>, tuple: TupleBuilder) {
        let pkey = tuple.first_v2().clone();
        for indice in &self.indices {
            let indexed_col = tuple.extract(indice.on_column as usize).clone();
            let index_tuple = TupleBuilder {
                fields: vec![indexed_col, pkey.clone()]
            };
            indice.raw_table.store_raw(index_tuple, ps);
        }
    }

    fn query<W: RWS>(&self, ps: &mut PageSerializer<W>, column: u64, equal: TypeData) -> Vec<TypeData> {
        let ind = self.indices.iter().find(|a| a.on_column == column).expect("Column is not indexed");
        let mut table_iter = ind.raw_table.get_in_all_iter(Some(equal), u64::MAX, ps);
        table_iter.collect(ps).into_iter().map(|a| a.extract(1).clone()).collect()
    }
}
