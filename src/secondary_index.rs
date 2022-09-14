use dynamic_tuple::{RWS, TupleBuilder};
use ra_ops::RANodeIterator;
use serializer::PageSerializer;
use crate::parser::CreateTable;
use crate::named_tables::NamedTables;
use crate::type_data::{Type, TypeData};
use crate::typed_table::TypedTable;

#[derive(Clone, Debug)]
pub struct IndexDescriptor {
    pub(crate) on_column: u64,
    pub(crate) raw_table: TypedTable,
}


#[derive(Default, Clone, Debug)]
pub struct SecondaryIndices {
    pub indices: Vec<IndexDescriptor>,
}

// #[test]
// fn secondaryindices_works() {
//     let mut ps = PageSerializer::default();
//     let ps = &mut ps;
//     let mut nt = NamedTables::new(ps);
//
//     let mut si = SecondaryIndices::default();
//     si.append_secondary_index(ps, &mut nt, "SI", Type::String, Type::Int, 1);
//
//     for i in 0..350_000 {
//         si.store(ps, TupleBuilder::default().add_int(i).add_string(i.to_string()));
//     }
//     for i in 0..350_000 {
//         assert_eq!(si.query(ps, 1, TypeData::String(i.to_string().into()))[0], TypeData::Int(i))
//     }
// }

impl SecondaryIndices {
    // fn init<W: RWS>(ps: &mut PageSerializer<W>, nt: &mut NamedTables)
    fn append_secondary_index2<W: RWS>(nt: &mut NamedTables, base_table_name: &str, on_column: u64, idx_name: String, ps: &mut PageSerializer<W>) {

        let base_table = nt.tables.get_mut(base_table_name).unwrap();

        let value_type = base_table.ty.fields[on_column as usize];
        let pkey_type = base_table.ty.fields[0];
        let cr = CreateTable {
            tbl_name: idx_name,
            fields: vec![("value".to_string(), value_type), ("row".to_string(), pkey_type)],
        };

        let idx_table = nt.insert_table(cr, ps);
        let idx_id = idx_table.id_ty;
        let idx = IndexDescriptor {
            on_column, raw_table: idx_table.clone()
        };

        let base_table_id = nt.tables.get_mut(base_table_name).unwrap().id_ty;

        nt.append_secondary_index(ps, &idx, idx_id,base_table_id);

        let base_table = nt.tables.get_mut(base_table_name).unwrap();


        base_table.attached_indices.indices.push(idx);
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
