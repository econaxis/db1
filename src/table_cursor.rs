use dynamic_tuple::{DynamicTuple, RWS, TupleBuilder};
use ra_ops::RANodeIterator;
use serializer::PageSerializer;
use crate::type_data::TypeData;

pub struct TableCursor<'a> {
    locations: Vec<u64>,
    ty: &'a DynamicTuple,
    // current_tuples: Vec<TupleBuilder>,
    current_index: u64,
    end_index_exclusive: u64,
    pkey: Option<TypeData>,
    load_columns: u64,
}

impl<'a> TableCursor<'a> {
    pub fn new< W: RWS>(locations: Vec<u64>, ps: & mut PageSerializer<W>, ty: &'a DynamicTuple, pkey: Option<TypeData>, load_columns: u64) -> Self {
        let mut se = Self {
            locations,
            ty,
            current_index: 0,
            end_index_exclusive: 0,
            pkey,
            load_columns,
        };
        if !se.locations.is_empty() {
            se.reset_index_iterator(ps);
        }
        se
    }
    fn reset_index_iterator<W: RWS>(&mut self, ps: &mut PageSerializer<W>) {
        // Reload the index iterator for the new table
        let table = ps.load_page_cached(*self.locations.last().unwrap());
        let range = if let Some(pk) = &self.pkey {
            table.get_ranges(pk..=pk)
        } else {
            0..table.len()
        };
        self.current_index = range.start;
        self.end_index_exclusive = range.end;
    }
}

impl<W: RWS> RANodeIterator<W> for TableCursor<'_> {
    fn next(&mut self, ps: &mut PageSerializer<W>) -> Option<TupleBuilder> {
        if self.current_index < self.end_index_exclusive {
            // Work on self.current_index
            let location = self.locations.last()?;
            let table = ps.load_page_cached(*location);

            let bytes = table.load_index(self.current_index as usize);
            let tuple = self.ty.read_tuple(bytes, self.load_columns, table.heap().get_ref());

            self.current_index += 1;
            Some(tuple)
        } else if self.locations.len() > 1 {
            self.locations.pop().unwrap();
            self.reset_index_iterator(ps);
            self.next(ps)
        } else if self.locations.len() == 1 {
            None
        } else {
            None
        }
    }
}
