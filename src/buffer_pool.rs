use std::collections::HashMap;
use std::iter::FromIterator;

pub struct BufferPool<TableT> {
    // Maps from location -> last use time
    last_use: HashMap<u64, u64>,
    time: u64,
    // Maps from location -> actual in-memory database
    buffer_pool: HashMap<u64, TableT>,
}

impl<TableT> Default for BufferPool<TableT> {
    fn default() -> Self {
        Self {
            last_use: Default::default(),
            buffer_pool: Default::default(),
            time: 0,
        }
    }
}

impl<TableT> BufferPool<TableT> {
    const MAX_BUFFERPOOL_SIZE: usize = 200;

    pub fn load_page<Loader: FnOnce() -> TableT>(
        &mut self,
        location: u64,
        loader: Loader,
    ) -> &mut TableT {
        self.time += 1;
        self.evict_if_necessary();
        self.last_use.insert(location, self.time);
        self.buffer_pool.entry(location).or_insert_with(|| {
            log::debug!("Loading buffer pool {}", location);
            loader()
        })
    }

    pub fn evict_if_necessary(&mut self) {
        if self.buffer_pool.len() > Self::MAX_BUFFERPOOL_SIZE {
            // Find least recently used items
            let mut lru = Vec::from_iter(self.last_use.iter().map(|(a, b)| (*a, *b)));
            lru.sort_by_key(|(_loc, uses)| *uses);
            for (loc_to_remove, _) in lru {
                if self.buffer_pool.len() > Self::MAX_BUFFERPOOL_SIZE {
                    self.last_use.remove(&loc_to_remove);
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
        type MyBufferPool = BufferPool<TableBase<DataType>>;
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
