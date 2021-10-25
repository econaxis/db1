use std::collections::HashMap;

use crate::table_base::TableBase;
use crate::suitable_data_type::SuitableDataType;
use std::iter::FromIterator;

pub struct BufferPool<T: SuitableDataType> {
    // Maps from location -> number of uses since last eviction
    lru: HashMap<u64, u32>,

    // Maps from location -> actual in-memory database
    buffer_pool: HashMap<u64, TableBase<T>>,
}

impl<T: SuitableDataType> Default for BufferPool<T> {
    fn default() -> Self {
        Self { lru: Default::default(), buffer_pool: Default::default() }
    }
}

impl<T: SuitableDataType> BufferPool<T> {
    const MAX_BUFFERPOOL_SIZE: usize = 10;

    pub fn load_page<Loader: FnOnce() -> TableBase<T>>(&mut self, location: u64, loader: Loader) -> &mut TableBase<T> {
        self.evict_if_necessary();
        self.lru.entry(location).and_modify(|e| { *e += 1 }).or_insert(1);
        self.buffer_pool.entry(location).or_insert_with(loader)
    }

    pub fn evict_if_necessary(&mut self) {
        if self.buffer_pool.len() > Self::MAX_BUFFERPOOL_SIZE {
            // Find least recently used items
            let mut lru = Vec::from_iter(self.lru.iter().map(|(a, b)| (*a, *b)));
            lru.sort_by_key(|(_loc, uses)| *uses);
            for (loc_to_remove, _) in lru {
                if self.buffer_pool.len() > Self::MAX_BUFFERPOOL_SIZE {
                    self.lru.remove(&loc_to_remove);
                    self.buffer_pool.remove(&loc_to_remove);
                } else {
                    break;
                }
            }
        }
    }
}


#[cfg(test)]
mod test {
    use crate::*;
    use super::*;

    fn default_loader() -> TableBase<DataType> {
        TableBase::<DataType>::default()
    }

    #[test]
    fn test_buffer_pool1() {
        type MyBufferPool = BufferPool<DataType>;
        // Required for this test to work
        assert!(MyBufferPool::MAX_BUFFERPOOL_SIZE >= 5);
        assert!(MyBufferPool::MAX_BUFFERPOOL_SIZE < 1000000);

        let mut buffer_pool = MyBufferPool::default();

        // Load max size + 1 elements into pool
        for i in 0..MyBufferPool::MAX_BUFFERPOOL_SIZE {
            buffer_pool.load_page(i as u64, default_loader);
        }

        // Loader should not be called, as there should be no evictions
        buffer_pool.load_page(0_u64, || panic!());
        buffer_pool.load_page(1_u64, || panic!());

        let mut called = false;
        buffer_pool.load_page(1000000, || {
            called = true;
            default_loader()
        });
        assert!(called);

        // 1 should still be in pool, because it was most recently used
        buffer_pool.load_page(1_u64, || panic!());
        buffer_pool.load_page(0_u64, || panic!());

        for i in 0..10 {
            buffer_pool.load_page(i + 10000000, default_loader);
        }
        assert!(buffer_pool.buffer_pool.len() <= MyBufferPool::MAX_BUFFERPOOL_SIZE + 1);
    }
}