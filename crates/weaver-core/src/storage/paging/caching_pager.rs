//! A caching pager stores pages, keeping some amount of non-modified pages in memory

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use lru::LruCache;
use parking_lot::Mutex;

use crate::monitoring::{Monitor, MonitorCollector, Monitorable, Stats};
use crate::storage::paging::traits::Page;
use crate::storage::Pager;

#[derive(Debug)]
pub struct LruCachingPager<P: Pager> {
    delegate: P,
    lru: Mutex<LruCache<usize, CachedPage>>,
    monitor: OnceLock<LruCacheMonitor>,
}

impl<P: Pager> LruCachingPager<P> {
    /// Creates a new lru caching pager with a given capacity
    pub fn new(pager: P, capacity: usize) -> Self {
        Self {
            delegate: pager,
            lru: Mutex::new(LruCache::new(
                NonZeroUsize::new(capacity).expect("non-zero capacity"),
            )),
            monitor: Default::default(),
        }
    }
}

#[derive(Clone, Default, Debug)]
struct LruCacheMonitor {
    hits: Arc<AtomicUsize>,
    misses: Arc<AtomicUsize>,
}

impl Monitor for LruCacheMonitor {
    fn name(&self) -> &str {
        "LruCache"
    }

    fn stats(&mut self) -> Stats {
        Stats::from_iter([
            ("hits", self.hits.load(Ordering::Relaxed) as i64),
            ("misses", self.misses.load(Ordering::Relaxed) as i64),
        ])
    }
}

impl<P: Pager> Monitorable for LruCachingPager<P> {
    fn monitor(&self) -> Box<dyn Monitor> {
        let mut collection = MonitorCollector::new();
        collection.push_monitorable(&self.delegate);
        collection.push(self.monitor.get_or_init(LruCacheMonitor::default).clone());
        Box::new(collection.into_monitor("LruCachingPager"))
    }
}

impl<P: Pager> Pager for LruCachingPager<P> {
    type Page<'a> = CachedPage where P: 'a;
    type PageMut<'a> = P::PageMut<'a> where P: 'a;
    type Err = P::Err;

    fn page_size(&self) -> usize {
        self.delegate.page_size()
    }

    fn get(&self, index: usize) -> Result<Self::Page<'_>, Self::Err> {
        let mut lock = self.lru.lock();
        if lock.contains(&index) {
            if let Some(monitor) = self.monitor.get() {
                monitor.hits.fetch_add(1, Ordering::Relaxed);
            }
            Ok(lock.get(&index).unwrap().clone())
        } else {
            lock.try_get_or_insert(index, || {
                let orig = self.delegate.get(index)?;
                if let Some(monitor) = self.monitor.get() {
                    monitor.misses.fetch_add(1, Ordering::Relaxed);
                }
                Ok(CachedPage::from_page(orig))
            }).cloned()
        }
    }

    fn get_mut(&self, index: usize) -> Result<Self::PageMut<'_>, Self::Err> {
        self.delegate.get_mut(index).inspect(|_| {
            // on success, remove from cache
            let _ = self.lru.lock().pop(&index);
            if let Some(monitor) = self.monitor.get() {
                monitor.misses.fetch_add(1, Ordering::Relaxed);
            }
        })
    }

    fn new(&self) -> Result<(Self::PageMut<'_>, usize), Self::Err> {
        if let Some(monitor) = self.monitor.get() {
            monitor.misses.fetch_add(1, Ordering::Relaxed);
        }
        self.delegate.new()
    }

    fn free(&self, index: usize) -> Result<(), Self::Err> {
        let _ = self.lru.lock().pop(&index);
        self.delegate.free(index)
    }

    fn len(&self) -> usize {
        self.delegate.len()
    }

    fn reserved(&self) -> usize {
        self.delegate.reserved()
    }
}

#[derive(Debug, Clone)]
pub struct CachedPage {
    bytes: Arc<Vec<u8>>,
}

impl CachedPage {
    /// Create a cached page from a page
    pub fn from_page<'a, P: Page<'a>>(page: P) -> Self {
        Self {
            bytes: Arc::new(Vec::from(page.as_slice())),
        }
    }
}

impl<'a> Page<'a> for CachedPage {
    fn len(&self) -> usize {
        self.bytes.len()
    }

    fn as_slice(&self) -> &[u8] {
        self.bytes.as_slice()
    }
}
