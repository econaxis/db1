use dynamic_tuple::{DynamicTuple, RWS, TupleBuilder};
use serializer::PageSerializer;
use std::io::{Read, Seek, Write};
use std::collections::HashMap;
use secondary_index::SecondaryIndices;
use table_base2::{TableBase2, TableType};
use crate::table_cursor::TableCursor;
use crate::type_data::{Type, TypeData};

#[derive(Clone, Debug)]
pub struct TypedTable {
    pub(crate) ty: DynamicTuple,
    pub(crate) id_ty: u64,
    pub(crate) column_map: HashMap<String, u32>,
    /* TODO(index-on-insert): run inserts through secondary indices */
    pub(crate) attached_indices: SecondaryIndices,
}

impl TypedTable {
    pub fn get_in_all_iter<W: RWS>(&self, pkey: Option<TypeData>, load_columns: u64, ps: & mut PageSerializer<W>) -> TableCursor<'_> {
        let location_iter = ps.get_in_all(self.id_ty, pkey.clone());
        TableCursor::new(location_iter, ps, &self.ty, pkey, load_columns)
    }

    pub(crate) fn store_raw(&self, t: TupleBuilder, ps: &mut PageSerializer<impl RWS>) {
        assert!(t.type_check(&self.ty));
        let max_page_len = ps.maximum_serialized_len();
        let pkey = t.first_v2().clone();
        let (_location, page) = match ps.get_in_all_insert(self.id_ty, pkey.clone()) {
            Some(location) => {
                let page = ps.load_page_cached(location);
                if !page.limits.overlaps(&(&pkey..=&pkey)) {
                    ps.previous_headers
                        .update_limits(self.id_ty, location, pkey);
                }

                // Have to load page again because of the damn borrow checker...
                let page = ps.load_page_cached(location);
                page.insert_tb(t);
                (location, page)
            }
            None => {
                let table_type = if self.ty.fields[0] == Type::Int {
                    TableType::Data
                } else {
                    TableType::Index(Type::String)
                };

                let mut new_page = TableBase2::new(self.id_ty, self.ty.size() as usize, table_type);
                new_page.insert_tb(t);
                let location = new_page.force_flush(ps);
                (location, ps.load_page_cached(location))
            }
        };

        // If estimated flush size is >= 16000, then we should split page to avoid going over page size limit
        if page.serialized_len() >= max_page_len {
            let old_min_limits = page.limits.min.clone().unwrap();
            let newpage = page.split(&self.ty);
            if let Some(mut x) = newpage {
                assert!(!x.limits.overlaps(&page.limits), "{:?} {:?}", &x.limits, &page.limits);
                let page_limits = page.limits.clone();
                ps.previous_headers
                    .reset_limits(self.id_ty, old_min_limits, page_limits);
                x.force_flush(ps);
            }
        }
    }


    pub(crate) fn new<W: Write + Read + Seek>(
        ty: DynamicTuple,
        id: u64,
        _ps: &mut PageSerializer<W>,
        columns: Vec<impl Into<String>>,
    ) -> Self {
        assert_eq!(columns.len(), ty.fields.len());
        println!("Creating dyntable {:?}", ty);

        Self {
            ty,
            id_ty: id,
            column_map: columns
                .into_iter()
                .enumerate()
                .map(|(ind, a)| (a.into(), ind as u32))
                .collect(),
            attached_indices: Default::default(),
        }
    }
}
