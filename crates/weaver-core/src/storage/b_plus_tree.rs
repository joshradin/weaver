//! The second version of the B+ tree

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::ops::Bound;

use parking_lot::RwLock;
use ptree::{write_tree, TreeBuilder};

use crate::data::row::OwnedRow;
use crate::error::Error;
use crate::key::{KeyData, KeyDataRange};
use crate::storage::abstraction::Paged;
use crate::storage::cells::{Cell, KeyCell, KeyValueCell, PageId};
use crate::storage::slotted_page::{PageType, SlottedPageAllocator};
use crate::storage::{ReadDataError, WriteDataError};

/// A BPlusTree that uses a given pager
pub struct BPlusTree<P: Paged> {
    allocator: RwLock<SlottedPageAllocator<P>>,
    /// determined initially by scanning up parents
    root: RwLock<Option<PageId>>,
}

impl<P: Paged> Debug for BPlusTree<P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut mapping = BTreeMap::new();
        let guard = self.allocator.read();
        for i in 0..guard.len() {
            if let Ok(page) = Paged::get(&*guard, i) {
                mapping.insert(
                    page.page_id(),
                    (page.page_type(), page.parent(), page.all()),
                );
            }
        }
        f.debug_struct("BPlusTree")
            .field("root", &self.root.read())
            .field("pages", &mapping)
            .finish()
    }
}

impl<P: Paged> BPlusTree<P>
where
    Error: From<P::Err>,
{
    /// Creates a new bplus tree around a pager
    pub fn new(pager: P) -> Self {
        let mut allocator = SlottedPageAllocator::new(pager);
        let root = if allocator.len() > 0 {
            let mut ptr = Paged::get(&allocator, 0)
                .unwrap_or_else(|_| panic!("should not fail because len > 0"));
            while let Some(parent) = ptr.parent() {
                ptr = allocator
                    .get(parent)
                    .expect("parent set but does not exist")
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
    pub fn insert<K: Into<KeyData>, V: Into<OwnedRow>>(&self, k: K, v: V) -> Result<(), Error> {
        let key = k.into();
        let value = v.into();

        if self.root.read().is_none() {
            let (page, _) = self.allocator.write().new_with_type(PageType::KeyValue)?;
            *self.root.write() = Some(page.page_id());
        }

        let leaf = self.find_leaf(&key)?;
        let cell: Cell = KeyValueCell::new(key.clone(), value).into();
        if self.insert_cell(cell.clone(), leaf)? {
            // split occurred, retry
            let leaf = self.find_leaf(&key)?;
            self.insert_cell(cell, leaf).map(|_| ())
        } else {
            Ok(())
        }
    }

    fn insert_cell(&self, cell: Cell, leaf: PageId) -> Result<bool, Error> {
        let mut leaf_page = self.allocator.read().get(leaf).expect("no page found");
        match leaf_page.insert(cell.clone()) {
            Ok(()) => Ok(false),
            Err(Error::WriteDataError(WriteDataError::InsufficientSpace)) => {
                // insufficient space requires a split
                let id = leaf_page.page_id();
                drop(leaf_page);
                self.split(id)?;
                Ok(true)
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    /// splits the page given by a specified ID
    fn split(&self, page_id: PageId) -> Result<(), Error> {
        let mut page = self.allocator.read().get(page_id)?;
        page.lock()?;
        let page_type = page.page_type();
        let (mut split_page, _) = self.allocator.write().new_with_type(page_type)?;
        split_page.set_right_sibling(page_id);
        split_page.set_left_sibling(page.left_sibling());
        page.set_left_sibling(split_page.page_id());

        let full_count = page.count();
        if full_count == 0 {
            return Ok(());
        }
        let orig_split_page_count = full_count / 2;
        let key = page.index(orig_split_page_count).unwrap().key_data();

        let cells = page.drain(..=key)?;
        let max_key = cells.iter().map(|cell| cell.key_data()).max();

        let Some(max_key) = max_key else {
            return Ok(());
        };

        println!(
            "splitting into pages with maxes of {:?} and {:?}",
            max_key,
            page.max_key()
        );

        for cell in cells {
            split_page.insert(cell)?;
        }

        let split_page_id = split_page.page_id();

        let parent = match page.parent() {
            None => {
                let (mut new_root, _) = self.allocator.write().new_with_type(PageType::Key)?;
                let root_id = new_root.page_id();
                let _ = self.root.write().insert(root_id);
                page.set_parent(root_id);
                let max_key = page
                    .max_key()?
                    .expect("page split resulted in 0 cells in new page");
                new_root.insert(Cell::Key(KeyCell::new(page_id.as_u32(), max_key)))?;
                root_id
            }
            Some(parent) => parent,
        };

        split_page.set_parent(parent);
        let ptr_cell = Cell::Key(KeyCell::new(split_page_id.as_u32(), max_key));
        self.insert_cell(ptr_cell, parent)?;
        Ok(())
    }

    /// Tries to get a matching record based on the given key data.
    ///
    /// Only returns an error if something went wrong trying to find the data, and returns `Ok(None` if no
    /// problems occurred but an associated record was not present.
    pub fn get(&self, key_data: &KeyData) -> Result<Option<Box<[u8]>>, Error> {
        let leaf = self.find_leaf(key_data)?;
        let leaf = self.allocator.read().get(leaf)?;
        let cell = leaf.get(key_data)?;
        match cell {
            None => Ok(None),
            Some(Cell::Key(_)) => {
                return Err(Error::CellTypeMismatch {
                    expected: PageType::KeyValue,
                    actual: PageType::Key,
                });
            }
            Some(Cell::KeyValue(value)) => Ok(Some(Box::from(value.record()))),
        }
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
                        println!("checking if {:?} contains {:?}", kdr, key_data);
                        if kdr.contains(key_data) {
                            Ordering::Equal
                        } else {
                            match kdr.start_bound() {
                                Bound::Included(i) | Bound::Excluded(i) => i.cmp(key_data),
                                Bound::Unbounded => Ordering::Greater,
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
                            if close == cells.len() {
                                match page.right_sibling() {
                                    None => {
                                        let last = cells.last().unwrap().1.as_key_cell().unwrap();
                                        self.increase_max(last.page_id(), key_data)?;
                                        return Ok(last.page_id());
                                    }
                                    Some(right) => {
                                        panic!("got wrong leaf. This leaf {ptr:?} was found but it has right sibling {right:?} but key data is not within range")
                                    }
                                }
                            } else {
                                panic!("no good index found, but could insert a new key cell at index {close}.")
                            }
                        }
                    }
                }
                PageType::KeyValue => {
                    break;
                }
            }
        }
        Ok(ptr)
    }

    /// Increases the max
    fn increase_max(&self, leaf: PageId, new_max: &KeyData) -> Result<(), Error> {
        let mut prev = leaf;
        let mut ptr = self.allocator.read().get(leaf)?.parent();
        while let Some(parent) = ptr {
            let mut parent_page = self.allocator.read().get(parent)?;
            parent_page.lock()?;
            let old_cell = parent_page
                .all()?
                .into_iter()
                .filter_map(|cell| cell.into_key_cell())
                .find(|cell| cell.page_id() == prev)
                .ok_or_else(|| Error::ReadDataError(ReadDataError::PageNotFound(prev)))?;
            let old_key_data = old_cell.key_data();
            if &old_key_data < new_max {
                println!("setting pointer to page {prev:?} in page {parent:?} to use key {new_max:?} instead of {old_key_data:?}");
                let removed = parent_page.delete(&old_key_data)?;
                if let Some(removed) = removed {
                    println!("removed cell ({})", removed.into_key_cell().unwrap());
                } else {
                    panic!("should've removed an old cell")
                }
                let cell = KeyCell::new(prev.as_u32(), new_max.clone());
                println!("inserting ({})", cell);
                parent_page.insert(cell.into())?;
            }
            prev = parent;
            ptr = parent_page.parent();
            drop(parent_page);
        }
        Ok(())
    }

    pub fn print(&self) -> Result<(), Error> {
        let mut builder = TreeBuilder::new("btree".to_string());
        if let Some(root) = self.root.read().clone() {
            self.print_(root, &mut builder)?;
        }
        let built = builder.build();
        let mut vec = vec![];
        write_tree(&built, &mut vec).expect("could not write");
        println!("{}", String::from_utf8_lossy(&vec));
        Ok(())
    }

    pub fn print_(&self, page: PageId, builder: &mut TreeBuilder) -> Result<(), Error> {
        let page = self.allocator.read().get(page)?;
        builder.begin_child(format!("({:?}) {:?}", page.page_type(), page.page_id()));
        match page.page_type() {
            PageType::Key => {
                for cell in page.all()? {
                    if let Some(key_cell) = cell.into_key_cell() {
                        builder.begin_child(format!("<={:?}", key_cell.key_data()));
                        self.print_(key_cell.page_id(), builder)?;
                        builder.end_child();
                    }
                }
            }
            PageType::KeyValue => {
                builder.add_empty_child(format!(
                    "min: {:?}, max: {:?}, len: {}",
                    page.min_key(),
                    page.max_key(),
                    page.count()
                ));
            }
        }
        builder.end_child();
        Ok(())
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
    use crate::storage::ram_file::{PagedFile, RandomAccessFile};
    use tempfile::tempfile;

    use super::*;

    #[test]
    fn create_b_plus_tree() {
        let _ = BPlusTree::new(VecPaged::new(1028));
    }

    #[test]
    fn insert_into_b_plus_tree() {
        let btree = BPlusTree::new(VecPaged::new(128));
        btree.insert([1], [1, 2, 3]).expect("could not insert");
        let raw = btree.get(&[1].into()).unwrap().unwrap();
        println!("raw: {:x?}", raw);
        assert_eq!(raw.len(), 24);
    }

    #[test]
    fn insert_into_b_plus_tree_many() {
        let paged = RandomAccessFile::with_file(tempfile().expect("could not open")).unwrap();
        let btree = BPlusTree::new(PagedFile::new(paged, 512));

        for i in 1..=100 {
            btree.insert([i], [1 + i, 2 * i]).expect("could not insert");
        }

        let raw = btree.get(&[1].into()).unwrap().unwrap();
        println!("raw: {:x?}", raw);
        assert_eq!(raw.len(), 16);
        btree.print().expect("could not print");

        for i in 1..=100 {
            let gotten = btree
                .get(&[i].into())
                .unwrap()
                .unwrap_or_else(|| panic!("could not get record for key {i}"));
            assert_eq!(gotten.len(), 16);
        }
    }
}
