use std::collections::HashMap;
use dynamic_tuple::{CreateTable, DynamicTuple, RWS, TupleBuilder, Type, TypeData};
use dynamic_tuple::TypeData::Null;
use query_data::QueryData;
use ra_ops::RANodeIterator;
use serializer::PageSerializer;
use typed_table::TypedTable;
use crate::dynamic_tuple::{Filter, InsertValues, Select};

enum DbOtherObjectType {
    SecondaryIndex(SecondaryIndexSchemaInfo)
}

struct SecondaryIndexSchemaInfo {
    // ID of the particular secondary index
    attach_to_table: String,
    index_name: String,
    on_column: u64,
}

pub(crate) struct NamedTables {
    pub tables: HashMap<String, TypedTable>,
    pub other_structures: Vec<DbOtherObject>,
    largest_id: u64,
}

const DATA_TABLE_ID: u64 = 2;
const INDEX_TABLE_ID: u64 = 3;

impl NamedTables {
    /* TODO(index-schema-storage): implement storage for secondary indices in the schema table
        Subtasks:
            - how to store secondary index information in tables
            - when adding a secondary index in code, also propagate those changes to the schema table
            - abstract schema table + table info table to a separate struct
     */
    // pub fn init_secondary_indices(ps: &mut PageSerializer<impl RWS>) {
    //     let indices_schema = TypedTable::new(
    //         DynamicTuple {
    //             fields: vec![
    //                 Type::String, // table name that the index attaches to
    //                 Type::String, // name of the index
    //                 Type::Int,     // on column of table
    //             ]
    //         },INDEX_TABLE_ID, ps, vec!["table_name", "index_name", "on_column"]);
    //
    //     for tup in indices_schema.get_in_all_iter(None, )
    //     }
    // }
    pub(crate) fn new(s: &mut PageSerializer<impl RWS>) -> Self {
        // Load schema table first
        /*
        TODO(table-schema): abstract schema table to separate class
            - use that class to persist `insert_table` code
         */
        let schema = TypedTable {
            ty: DynamicTuple {
                // TableID (64 bit type id), TableName, Column Name, Column Type,
                fields: vec![Type::Int, Type::String, Type::String, Type::Int],
            },
            id_ty: 2,
            column_map: Default::default(),
            attached_indices: Default::default(),
        };

        let mut tables = HashMap::new();

        let mut entry = tables.entry("schema".to_string()).insert_entry(schema);
        let schema = entry.get_mut();
        let mut large_id = 3;

        for tup in schema.get_in_all_iter(None, 0, s).collect(s).into_iter().rev() {
            let id = tup.extract_int(0);
            let table_name = std::str::from_utf8(tup.extract_string(1)).unwrap();
            let column_name = std::str::from_utf8(tup.extract_string(2)).unwrap();
            let column_type = Type::from(tup.extract_int(3));

            let r = tables
                .entry(table_name.to_string())
                .or_insert_with(|| TypedTable {
                    ty: DynamicTuple::default(),
                    column_map: Default::default(),
                    id_ty: id,
                    attached_indices: Default::default(),
                });
            println!("Adding column {} {}", table_name, column_name);
            r.column_map
                .insert(column_name.to_string(), r.ty.fields.len() as u32);
            r.ty.fields.push(column_type);
            large_id = large_id.max(id);
        }

        Self {
            tables,
            largest_id: large_id,
        }
    }

    pub(crate) fn insert_table(
        &mut self,
        CreateTable {
            tbl_name: name,
            fields: columns,
        }: CreateTable,
        ps: &mut PageSerializer<impl RWS>,
    ) {
        self.largest_id += 1;
        let table_id = self.largest_id;
        // First insert to schema table

        let schema_table = self.tables.get_mut("schema").unwrap();

        for (colname, col) in &columns {
            println!("Insert col {colname}");
            let tup = TupleBuilder::default()
                .add_int(table_id)
                .add_string(name.clone())
                .add_string(colname.clone())
                .add_int(*col as u64);
            schema_table.store_raw(tup, ps);
        }

        let types = columns.iter().map(|a| a.1).collect();
        let names = columns.into_iter().map(|a| a.0).collect();
        self.tables.insert(
            name,
            TypedTable::new(DynamicTuple { fields: types }, table_id, ps, names),
        );
    }

    fn execute_insert(&mut self, insert: InsertValues, ps: &mut PageSerializer<impl RWS>) {
        let table = self.tables.get_mut(&insert.tbl_name).unwrap();
        for t in insert.values {
            let tuple = TupleBuilder { fields: t };
            tuple.type_check(&table.ty);
            table.store_raw(tuple, ps);
        }
    }

    fn calculate_column_mask(table: &TypedTable, fields: &[String]) -> u64 {
        let mut mask = 0;
        if fields.is_empty() {
            return u64::MAX;
        }
        for f in fields {
            if f == "*" {
                mask = u64::MAX;
                return mask;
            }
            let index = table.column_map[f];
            assert!(index < 64);
            mask |= 1 << index;
        }
        mask
    }

    fn execute_select<'a, W: RWS>(
        &mut self,
        select: Select,
        ps: &'a mut PageSerializer<W>,
    ) -> QueryData<'a, W> {
        let table = self.tables.get_mut(&select.tbl_name).unwrap();
        let col_mask = Self::calculate_column_mask(table, &select.columns);

        let filter = select.filter;

        // TODO: only supports the first filter condition for now
        let results: Vec<_> = match filter.first() {
            Some(Filter::Equals(colname, TypeData::Int(icomp))) => {
                match table.column_map[colname] {
                    0 => table.get_in_all_iter(Some(TypeData::Int(*icomp)), col_mask, ps).collect(ps),
                    colindex => {
                        todo!()
                        // println!("Warning: using inefficient table scan");
                        // let query_result = table.get_in_all_iter(None, col_mask, ps);
                        //
                        // query_result.filter(|i| match i.fields[colindex as usize] {
                        //     TypeData::Int(int) => int == *icomp,
                        //     _ => panic!(),
                        // }).collect()
                    }
                }
            }
            Some(Filter::Equals(colname, TypeData::String(s))) => {
                todo!()
                // println!("Warning: using inefficient table scan");
                //
                // let colindex = table.column_map[colname];
                // let qr = table.get_in_all_iter(None, col_mask, ps);
                // qr.filter(|i| match &i.fields[colindex as usize] {
                //     TypeData::String(s1) => s1 == s,
                //     _ => panic!(),
                // }).collect()
            }
            None | Some(Filter::Equals(_, Null)) => table.get_in_all_iter(None, col_mask, ps).collect(ps),
        };

        QueryData::new(results, vec![], ps)
    }
}
