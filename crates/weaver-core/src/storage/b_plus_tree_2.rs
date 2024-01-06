//! The second version of the B+ tree


use std::cmp::Ordering;
use std::ops::Bound;
use std::sync::OnceLock;
use parking_lot::RwLock;
use crate::data::row::{OwnedRow, OwnedRowRefIter, Row};
use crate::error::Error;
use crate::key::{KeyData, KeyDataRange};
use crate::storage::abstraction::Paged;
use crate::storage::cells::{Cell, PageId};
use crate::storage::slotted_page_2::{PageType, SlottedPageAllocator};

/// A BPlusTree that uses a given pager
#[derive(Debug)]
pub struct BPlusTree<P : Paged> {
    allocator: RwLock<SlottedPageAllocator<P>>,
    /// determined initially by scanning up parents
    root: RwLock<Option<PageId>>,
}

impl<P: Paged> BPlusTree<P>
    where Error: From<P::Err>
{

    /// Creates a new bplus tree around a pager
    pub fn new(pager: P) -> Self {
        let mut allocator = SlottedPageAllocator::new(pager);
        let root = if allocator.len() > 0 {
            let mut ptr = Paged::get(&allocator, 0).unwrap_or_else(|_| panic!("should not fail because len > 0"));
            while let Some(parent) = ptr.parent() {
                ptr = allocator.get(parent).expect("parent set but does not exist")
            }
            Some(ptr.page_id())
        } else {
            None
        };

        Self {
            allocator: RwLock::new(allocator),
            root: RwLock::new(root),
        }
    }

    /// Inserts into bplus tree.
    ///
    /// Uses a immutable reference, as locking is performed at a node level. This should make
    /// insertions more efficient as space increases.
    pub fn insert<K : Into<KeyData>, V : Into<OwnedRow>>(&self, k: K, v: V) -> Result<(),Error> {
        let key = k.into();
        let value = v.into();

        if self.root.read().is_none() {
            let (page, _) = self.allocator.write().new_with_type(PageType::KeyValue)?;
            *self.root.write() = Some(page.page_id());
        }

        let leaf = self.find_leaf(&key)?;

        todo!()
    }



    /// Finds the leaf node that can contain the given key
    fn find_leaf(&self, key_data: &KeyData) -> Result<PageId, Error> {
        let mut ptr = self.root.read().expect("no root set");
        loop {
            let page = self.allocator.read().get(ptr).unwrap();
            match page.page_type() {
                PageType::Key => {
                    let cells = to_ranges(page.all()?);
                    let found = cells.binary_search_by(|(kdr, _)| {
                        if kdr.contains(key_data) {
                            Ordering::Equal
                        } else {
                            match kdr.start_bound() {
                                Bound::Included(i) | Bound::Excluded(i) => { i.cmp(key_data) }
                                Bound::Unbounded => { Ordering::Greater}
                            }
                        }
                    });
                    match found {
                        Ok(good) => {
                            let cell = &cells[good].1;
                            let Cell::Key(key) = cell else {
                                unreachable!("key cell pages only contain key cells")
                            };
                            ptr = key.page_id()
                        }
                        Err(close) => {

                        }
                    }
                }
                PageType::KeyValue => {
                    break
                }
            }
        }
        Ok(ptr)
    }
}

fn to_ranges(cells: Vec<Cell>) -> Vec<(KeyDataRange, Cell)> {
    cells
        .into_iter()
        .scan(Bound::Unbounded, |prev, cell| {
            let data = cell.key_data();
            let lower = prev.clone();
            let upper = Bound::Included(data.clone());
            *prev = Bound::Excluded(data);
            Some((KeyDataRange(lower, upper), cell))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::storage::abstraction::VecPaged;
    use super::*;
    #[test]
    fn create_b_plus_tree() {
        let _ = BPlusTree::new(VecPaged::new(1028));

    }

    #[test]
    fn insert_into_b_plus_tree() {
        let btree = BPlusTree::new(VecPaged::new(1028));
        btree.insert([1], [1, 2, 3]).expect("could not insert");
    }
}