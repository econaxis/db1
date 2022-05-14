use dynamic_tuple::{TupleBuilder, RWS};
use serializer::PageSerializer;

#[derive(Debug)]
pub struct QueryData<'a, W: RWS> {
    results: Vec<TupleBuilder>,
    accessed_pages: Option<u64>,
    ps: &'a mut PageSerializer<W>,
}

impl<'a, W: RWS> QueryData<'a, W> {
    pub fn new(
        results: Vec<TupleBuilder>,
        accessed_pages: Vec<u64>,
        ps: &'a mut PageSerializer<W>,
    ) -> Self {
        // assert!(accessed_pages.len() <= 1);
        Self {
            results,
            accessed_pages: accessed_pages.first().copied(),
            ps,
        }
    }
    pub fn results(mut self) -> Vec<TupleBuilder> {
        for page in std::mem::take(&mut self.accessed_pages) {
            self.ps.unpin_page(page);
        }
        std::mem::take(&mut self.results)
    }
    pub fn filter<F: FnMut(&TupleBuilder) -> bool>(&mut self, f: F) {
        let res = std::mem::take(&mut self.results);
        self.results = res.into_iter().filter(f).collect();
    }
}

impl<'a, W: RWS> Drop for QueryData<'a, W> {
    fn drop(&mut self) {
        assert!(self.results.is_empty());
        assert!(self.accessed_pages.is_none());
    }
}
