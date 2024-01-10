//! The second version of the B+ tree

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::io::Write;
use std::ops::Bound;
use std::sync::Arc;

use parking_lot::RwLock;
use ptree::{TreeBuilder, write_tree};
use tracing::{error, trace};

use crate::data::row::OwnedRow;
use crate::error::Error;
use crate::key::{KeyData, KeyDataRange};
use crate::storage::{ReadDataError, WriteDataError};
use crate::storage::abstraction::Paged;
use crate::storage::cells::{Cell, KeyCell, KeyValueCell, PageId};
use crate::storage::slotted_page::{PageType, SlottedPageAllocator};

/// A BPlusTree that uses a given pager
pub struct BPlusTree<P: Paged> {
    allocator: Arc<SlottedPageAllocator<P>>,
    /// determined initially by scanning up parents
    root: RwLock<Option<PageId>>,
}

impl<P: Paged> Debug for BPlusTree<P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut mapping = BTreeMap::new();
        let guard = &self.allocator;
        for i in 0..guard.len() {
            if let Ok(page) = Paged::get(&**guard, i) {
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
            allocator: Arc::new(allocator),
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
            let guard = &self.allocator;
            let (page, _) = guard.new_with_type(PageType::KeyValue)?;
            *self.root.write() = Some(page.page_id());
        }

        let leaf = self.find_leaf(&key, true)?;
        let cell: Cell = KeyValueCell::new(key.clone(), value).into();
        let insert_result = self.insert_cell(cell.clone(), leaf);
        let split = match insert_result {
            Ok(split) => { split }
            Err(e) => {
                error!("error occurred during insert: {e}");
                return Err(e);
            }
        };
        if split {
            // split occurred, retry
            let leaf = self.find_leaf(&key, true)?;
            self.insert_cell(cell, leaf).map(|_| ())
                .map_err(|err| {
                    error!("error occurred during insert after split: {err}");
                    err
                })
        } else {
            Ok(())
        }
    }

    fn insert_cell(&self, cell: Cell, leaf: PageId) -> Result<bool, Error> {
        let mut leaf_page = self.allocator.get_mut(leaf).expect("no page found");
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

        let mut allocator = &self.allocator;
        let mut page = allocator.get_mut(page_id)?;
        page.lock()?;
        let page_type = page.page_type();
        let (mut split_page, _) = allocator.new_with_type(page_type)?;
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

        for cell in cells {
            split_page.insert(cell)?;
        }

        let split_page_id = split_page.page_id();

        let parent = match page.parent() {
            None => {
                let (mut new_root, _) = allocator.new_with_type(PageType::Key)?;
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
        let key_ptr_cell = KeyCell::new(split_page_id.as_u32(), max_key);
        let ptr_cell = Cell::Key(key_ptr_cell.clone());
        drop(page);
        drop(split_page);
        let emit = if self.insert_cell(ptr_cell.clone(), parent)? {
            // panic!("split occurred while inserting ptr cell: {}", key_ptr_cell);
            self.insert_cell(ptr_cell, parent)
                .map(|_| ())
        } else {
            Ok(())
        };

        emit
    }

    /// Tries to get a matching record based on the given key data.
    ///
    /// Only returns an error if something went wrong trying to find the data, and returns `Ok(None` if no
    /// problems occurred but an associated record was not present.
    pub fn get(&self, key_data: &KeyData) -> Result<Option<Box<[u8]>>, Error> {
        let leaf = self.find_leaf(key_data, false)?;
        let allocator = &self.allocator;
        let leaf = allocator.get(leaf)?;
        let cell = leaf.get(key_data)?;
        match cell {
            None => Ok(None),
            Some(Cell::Key(_)) => {
                return Err(Error::CellTypeMismatch {
                    page_id: leaf.page_id(),
                    expected: PageType::KeyValue,
                    actual: PageType::Key,
                });
            }
            Some(Cell::KeyValue(value)) => Ok(Some(Box::from(value.record()))),
        }
    }

    /// Gets the set of rows from a given range
    pub fn range<T : Into<KeyDataRange>>(&self, key_data_range: T) -> Result<Vec<Box<[u8]>>, Error> {
        let range = key_data_range.into();
        let Some(root) = self.root.read().clone() else {
            return Ok(vec![])
        };
        let start_node = match range.start_bound() {
            Bound::Included(k) | Bound::Excluded(k)=> {
                self.find_leaf(k, false)?
            }
            Bound::Unbounded => {
                self.left_most(root)?
            }
        };
        let end_node = match range.end_bound() {
            Bound::Included(k) | Bound::Excluded(k)=> {
                self.find_leaf(k, false)?
            }
            Bound::Unbounded => {
                self.right_most(root)?
            }
        };
        let mut pages = vec![];
        let mut page_ptr = start_node;

        loop {
            pages.push(page_ptr);
            if page_ptr == end_node {
                break;
            }
            let page = self.allocator.get(page_ptr)?;
            let right = page.right_sibling().expect("siblings should always be set");
            page_ptr = right;
        }

        pages.into_iter()
            .try_fold(vec![], |mut vec, page_id| {
                let page = self.allocator.get(page_id)?;
                let page_range = page.key_range()?;
                if let Some(on_page) = page_range.intersection(&range) {
                    let page_cells = page.get_range(on_page)?
                        .into_iter()
                        .flat_map(|cell| cell.into_key_value_cell())
                        .map(|cell| Box::from(cell.record()));
                    vec.extend(page_cells);
                }
                Ok(vec)
            })
    }

    /// Gets all rows
    #[inline]
    pub fn all(&self) -> Result<Vec<Box<[u8]>>, Error> {
        self.range(..)
    }

    /// Gets the minimum key stored in this btree
    pub fn min_key(&self) -> Result<Option<KeyData>, Error> {
        if let Some(root) = self.root.read().as_ref() {
            let left = self.left_most(*root)?;
            let page = self.allocator.get(left)?;
            page.min_key()
        } else {
            Ok(None)
        }
    }

    /// Gets the maximum key stored in this btree
    pub fn max_key(&self) -> Result<Option<KeyData>, Error> {
        if let Some(root) = self.root.read().as_ref() {
            let left = self.right_most(*root)?;
            let page = self.allocator.get(left)?;
            page.max_key()
        } else {
            Ok(None)
        }
    }

    /// Finds the leaf node that can contain the given key
    fn find_leaf(&self, key_data: &KeyData, expand: bool) -> Result<PageId, Error> {
        let Some(mut ptr) = self.root.read().clone() else {
            return Err(Error::NotFound(key_data.clone()))
        };
        loop {
            let page = self.allocator.get(ptr).unwrap();
            match page.page_type() {
                PageType::Key => {
                    let cells = to_ranges(page.all()?);
                    let found = cells.binary_search_by(|(kdr, _)| {
                        if kdr.contains(key_data) {
                            Ordering::Equal
                        } else {
                            match kdr.start_bound() {
                                Bound::Included(i) => i.cmp(key_data),
                                Bound::Excluded(i) => match i.cmp(key_data) {
                                    Ordering::Equal => {
                                        Ordering::Greater
                                    },
                                    v => v
                                }
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
                            if expand && close == cells.len() {
                                match page.right_sibling() {
                                    None => {
                                        let last = cells.last().unwrap().1.as_key_cell().unwrap();
                                        drop(page);
                                        self.increase_max(last.page_id(), key_data)?;
                                        ptr = last.page_id();
                                    }
                                    Some(right) => {
                                        panic!("got wrong leaf. This leaf {ptr:?} was found but it has right sibling {right:?} but key data is not within range")
                                    }
                                }
                            } else if expand {
                                panic!("no good index found, but could insert a new key cell at index {close}.")
                            } else {
                                return Err(Error::NotFound(key_data.clone()))
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
        let mut prev = self.right_most(leaf)?;
        let mut ptr = self.allocator.get(prev)?.parent();

        while let Some(parent) = ptr {
            let mut parent_page = self.allocator.get_mut(parent)?;
            parent_page.lock()?;
            let old_cell = parent_page
                .all()?
                .into_iter()
                .filter_map(|cell| cell.into_key_cell())
                .find(|cell| cell.page_id() == prev)
                .ok_or_else(|| Error::ReadDataError(ReadDataError::PageNotFound(prev)))?;
            let old_key_data = old_cell.key_data();
            if &old_key_data < new_max {
                let removed = parent_page.delete(&old_key_data)?;
                if let Some(removed) = removed {
                    // removed cell successfully
                } else {
                    panic!("should've removed an old cell")
                }
                let cell = KeyCell::new(prev.as_u32(), new_max.clone());
                parent_page.insert(cell.into())?;
            }
            prev = parent;
            ptr = parent_page.parent();
            drop(parent_page);
        }
        Ok(())
    }

    /// Right most (max) page
    fn right_most(&self, node: PageId) -> Result<PageId, Error> {
        let page = self.allocator.get(node)?;
        match page.page_type() {
            PageType::Key => {
                if let Some(ref max) = page.max_key()? {
                    let max = page.get(max)?.expect("max cell").into_key_cell().expect("is always key cell").page_id();
                    self.right_most(max)
                } else {
                    return Ok(node)
                }
            }
            PageType::KeyValue => { Ok(node)}
        }
    }

    /// left most (min) page
    fn left_most(&self, node: PageId) -> Result<PageId, Error> {
        let page = self.allocator.get(node)?;
        match page.page_type() {
            PageType::Key => {
                if let Some(ref min) = page.min_key()? {
                    let max = page.get(min)?.expect("min cell").into_key_cell().expect("is always key cell").page_id();
                    self.left_most(max)
                } else {
                    return Ok(node)
                }
            }
            PageType::KeyValue => { Ok(node)}
        }
    }

    /// Verifies the integrity of the tree
    pub fn verify_integrity(&self) -> Result<(), Error>{
        let root = self.root.read().clone();
        match root {
            None => { Ok(())}
            Some(root) => {
                self.verify_integrity_(root)
            }
        }
    }

    /// Verifies the integrity of the tree
    fn verify_integrity_(&self, page_id: PageId) -> Result<(), Error> {
        let page = self.allocator.get(page_id)?;
        match page.page_type() {
            PageType::Key => {
                let ranges = to_ranges(page.all()?);
                for (range, node) in ranges {
                    let key_cell = node.into_key_cell().unwrap();
                    let child = self.allocator.get(key_cell.page_id())?;

                    if let Some(ref min) = child.min_key()? {
                        if !range.contains(min) {
                            panic!("verify failed. range does not contain minimum. range = {:?}, min={:?}", range, min)
                        }
                    }

                    if let Some(ref max) = child.max_key()? {
                        if !range.contains(max) {
                            panic!("verify failed because max not in range")
                        }
                        if range.1.as_ref() != Bound::Included(max) {
                            panic!("max should always be max key")
                        }
                    }
                    self.verify_integrity_(key_cell.page_id())?;
                }
                Ok(())
            }
            PageType::KeyValue => { Ok(()) }
        }
    }

    pub fn print(&self) -> Result<(), Error> {
        let mut builder = TreeBuilder::new("btree".to_string());
        if let Some(root) = self.root.read().clone() {
            self.print_(root, &mut builder, None)?;
        }
        let built = builder.build();
        let mut vec = vec![];
        write_tree(&built, &mut vec)?;
        println!("{}", String::from_utf8_lossy(&vec));
        Ok(())
    }

    pub fn write<W : Write>(&self, writer: W) -> Result<(), Error> {
        let mut builder = TreeBuilder::new("btree".to_string());
        if let Some(root) = self.root.read().clone() {
            self.print_(root, &mut builder, None)?;
        }
        let built = builder.build();
        write_tree(&built, writer)?;
        Ok(())
    }

    pub fn print_(&self, page_id: PageId, builder: &mut TreeBuilder, prev: Option<&KeyData>) -> Result<(), Error> {
        let page = self.allocator.get(page_id)?;
        builder.begin_child(format!("({:?}) {:?}. nodes {}", page.page_type(), page.page_id(), self.nodes(page_id)?));
        match page.page_type() {
            PageType::Key => {
                let mut prev = None;
                for cell in page.all()? {
                    if let Some(key_cell) = cell.into_key_cell() {
                        let title = match &prev {
                            None => {format!("(,{:?}]", key_cell.key_data())}
                            Some(prev) => {
                                format!("({prev:?},{:?}]", key_cell.key_data())
                            }
                        };
                        builder.begin_child(title);
                        self.print_(key_cell.page_id(), builder, prev.as_ref())?;
                        prev = Some(key_cell.key_data());
                        builder.end_child();
                    }
                }
            }
            PageType::KeyValue => {
                builder.add_empty_child(format!(
                    "min: {:?}, max: {:?}, len: {}, keys: {:?}",
                    page.min_key(),
                    page.max_key(),
                    page.count(),
                    page.all()?
                        .into_iter()
                        .map(|cell| cell.key_data())
                        .collect::<Vec<_>>()
                ));
            }
        }
        builder.end_child();
        Ok(())
    }

    /// Gets the number of nodes
    pub fn nodes(&self, page_id: PageId) -> Result<usize, Error> {
        let page = self.allocator.get(page_id)?;
        match page.page_type() {
            PageType::Key => {
                Ok(page.all()?
                    .into_iter()
                    .filter_map(Cell::into_key_cell)
                    .map(|cell| {
                        self.nodes(cell.page_id())
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .sum::<usize>() + 1)
            }
            PageType::KeyValue => { Ok(1) }
        }
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
    use rand::distributions::Alphanumeric;
    use rand::Rng;
    use crate::data::values::Value;

    use crate::storage::abstraction::VecPaged;

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
        trace!("raw: {:x?}", raw);
        assert_eq!(raw.len(), 24);
    }

    #[test]
    fn insert_into_b_plus_tree_many() {
        let btree = BPlusTree::new(VecPaged::new(180));

        const MAX: i64 = 256;
        for i in 0..MAX {
            if let Err(e) =btree.insert([i], [1 + i, 2 * i]) {
                btree.print().expect("could not print");
                panic!("error occurred: {e}");
            }
        }
        btree.print().expect("could not print");
        btree.verify_integrity().expect("verify failed");
        let raw = btree.get(&[1].into()).unwrap().unwrap();
        trace!("raw: {:x?}", raw);
        assert_eq!(raw.len(), 16);


        for i in 0..MAX {
            let gotten = btree
                .get(&[i].into())
                .unwrap()
                .unwrap_or_else(|| panic!("could not get record for key {i}"));
            assert_eq!(gotten.len(), 16);
        }
    }

    #[test]
    fn insert_into_b_plus_tree_many_string() {
        let btree = BPlusTree::new(VecPaged::new(1028));

        const MAX: i64 = 512;
        let mut strings = vec![];
        for i in 0..MAX {
            let s: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(rand::thread_rng().gen_range(5..=15))
                .map(char::from)
                .collect();
            strings.push(s.clone());

            if let Err(e) =btree.insert([s.clone()], [Value::from(s), (2 * i).into()]) {
                btree.print().expect("could not print");
                panic!("error occurred: {e}");
            }
        }
        btree.print().expect("could not print");
        // btree.verify_integrity().expect("verify failed");


        for i in &strings {
            let gotten = btree
                .get(&[i].into())
                .unwrap()
                .unwrap_or_else(|| panic!("could not get record for key {i}"));
            println!("gotten: {:x?}", gotten);
        }


    }

    #[test]
    fn insert_into_b_plus_tree_many_rand() {
        let btree = BPlusTree::new(VecPaged::new(2048));
        for i in 1..=(1024) {
            if let Err(e) =btree.insert([rand::thread_rng().gen_range(-256000..=256000)], [1 + i, 2 * i]) {
                btree.print().expect("could not print");
                panic!("error occurred: {e}");
            }
        }

        btree.print().expect("could not print");
    }

}
