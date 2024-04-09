//! The second version of the B+ tree

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::io::Write;
use std::ops::Bound;
use std::sync::atomic::AtomicUsize;
use std::sync::{atomic, Arc, OnceLock};
use std::time::Instant;

use parking_lot::{Mutex, RwLock};
use ptree::{print_tree, write_tree, TreeBuilder};
use tracing::{error, warn};

use crate::data::row::OwnedRow;
use crate::error::WeaverError;
use crate::key::{KeyData, KeyDataRange};
use crate::monitoring::{Monitor, Monitorable, Stats};
use crate::storage::cells::{Cell, KeyCell, KeyValueCell, PageId};
use crate::storage::paging::slotted_pager::{PageType, SlottedPager};
use crate::storage::paging::traits::Pager;
use crate::storage::{ReadDataError, WriteDataError};

/// A BPlusTree that uses a given pager.
///
/// This is used for primary indices.
pub struct BPlusTree<P: Pager> {
    allocator: Arc<SlottedPager<P>>,
    /// determined initially by scanning up parents
    root: RwLock<Option<PageId>>,
    monitor: OnceLock<BPlusTreeMonitor>,
}

impl<P: Pager> Debug for BPlusTree<P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut mapping = BTreeMap::new();
        let guard = &self.allocator;
        for i in 0..guard.allocated() {
            if let Ok(page) = Pager::get(&**guard, i) {
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

impl<P: Pager> BPlusTree<P>
where
    WeaverError: From<P::Err>,
{
    /// Creates a new bplus tree around a pager
    pub fn new(pager: P) -> Self {
        let allocator = SlottedPager::new(pager);
        let root = if allocator.allocated() > 0 {
            let mut ptr = Pager::get(&allocator, 0)
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
            monitor: OnceLock::new(),
        }
    }

    pub fn allocator(&self) -> &impl Pager {
        self.allocator.as_ref()
    }

    /// Inserts into bplus tree.
    ///
    /// Uses a immutable reference, as locking is performed at a node level. This should make
    /// insertions more efficient as space increases.
    pub fn insert<K: Into<KeyData> + Debug, V: Into<OwnedRow> + Debug>(
        &self,
        k: K,
        v: V,
    ) -> Result<(), WeaverError> {
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
            Ok(split) => split,
            Err(e) => {
                error!("error occurred during insert: {e}");
                return Err(e);
            }
        };
        if split {
            // split occurred, retry
            let leaf = self.find_leaf(&key, true)?;
            self.insert_cell(cell, leaf).map(|_| ()).map_err(|err| {
                error!("error occurred during insert after split: {err}");
                err
            })
        } else {
            Ok(())
        }
    }

    fn insert_cell(&self, cell: Cell, page_id: PageId) -> Result<bool, WeaverError> {
        let mut page = self.allocator.get_mut(page_id).expect("no page found");
        match page.insert(cell.clone()) {
            Ok(()) => {
                if let Some(monitor) = self.monitor.get() {
                    monitor.inserts.fetch_add(1, atomic::Ordering::Relaxed);
                }
                Ok(false)
            }
            Err(WeaverError::WriteDataError(WriteDataError::AllocationFailed { .. })) => {
                // insufficient space requires a split
                let id = page.page_id();
                drop(page);
                self.split(id)?;
                Ok(true)
            }
            Err(e) => Err(e),
        }
    }

    /// splits the page given by a specified ID
    fn split(&self, page_id: PageId) -> Result<(), WeaverError> {
        self.verify_integrity();
        let allocator = &self.allocator;
        let mut page = allocator.get_mut(page_id)?;
        let page_type = page.page_type();
        let (mut split_page, _) = allocator.new_with_type(page_type)?;
        split_page.set_right_sibling(page_id);
        split_page.set_left_sibling(page.left_sibling());
        if let Some(left_sibling) = page.left_sibling() {
            let mut left_sibling_page = allocator.get_mut(left_sibling)?;
            left_sibling_page.set_right_sibling(split_page.page_id());
        }
        page.set_left_sibling(split_page.page_id());

        let full_count = page.count();
        if full_count == 0 {
            return Ok(());
        }
        let median_key = page.median_key()?.expect("median key must be defined");

        let cells = page.drain(..=median_key.clone())?;

        #[cfg(debug_assertions)]
        {
            let upper = page
                .all()
                .expect("could not get cells")
                .iter()
                .map(|c| c.key_data())
                .collect::<BTreeSet<_>>();
            let lower = cells.iter().map(|c| c.key_data()).collect::<BTreeSet<_>>();
            /* trace!(
                "split page {page_id:?} into\nlower {:#?}\nmedian: {median_key:?}\nupper {:#?}",
                lower,
                upper
            ); */
            assert_eq!(upper.intersection(&lower).count(), 0);
            assert!(upper.iter().all(|u| lower.iter().all(|l| l < u)));
        }
        for cell in cells {
            split_page.insert(cell)?;
        }

        let split_page_id = split_page.page_id();

        let parent = match page.parent() {
            None => {
                let (mut new_root, _) = allocator.new_with_type(PageType::Key)?;
                /* trace!("creating new root {}", new_root.page_id()); */
                let root_id = new_root.page_id();
                let _ = self.root.write().insert(root_id);
                page.set_parent(root_id);
                let max_key = page
                    .max_key()?
                    .expect("page split resulted in 0 cells in new page");
                let ptr_cell = KeyCell::new(page_id.as_u32(), max_key);
                new_root.insert(Cell::Key(ptr_cell))?;
                root_id
            }
            Some(parent) => parent,
        };

        split_page.set_parent(parent);
        let key_ptr_cell = KeyCell::new(split_page_id.as_u32(), median_key.clone());
        /* trace!("created ptr {}", key_ptr_cell); */
        let ptr_cell = Cell::Key(key_ptr_cell.clone());

        drop(page);
        drop(split_page);

        let parent = self.get_new_parent(&median_key, parent)?;

        /* trace!("inserting split page into {parent:?}"); */

        let emit = if self.insert_cell(ptr_cell.clone(), parent)? {
            let parent = self.get_new_parent(&median_key, parent)?;
            self.insert_cell(ptr_cell, parent).map(|split| {
                if split {
                    panic!("second split")
                }
            })
        } else {
            Ok(())
        };
        self.verify_integrity();
        if let Some(monitor) = self.monitor.get() {
            monitor.splits.fetch_add(1, atomic::Ordering::Relaxed);
        }
        emit
    }

    fn get_new_parent(&self, median_key: &KeyData, parent: PageId) -> Result<PageId, WeaverError> {
        let parent_cell = self.allocator.get(parent)?;
        let parent = if let Some(left_parent) = parent_cell.left_sibling() {
            let left_parent_cell = self.allocator.get(left_parent)?;
            /* trace!(
                "parent max: l(new)={:?}, r(orig)={:?}",
                left_parent_cell.max_key(),
                parent_cell.max_key()
            ); */

            let use_left = if let Some(ref left_max) = left_parent_cell.max_key()? {
                median_key <= left_max
            } else {
                false
            };

            if use_left {
                left_parent
            } else {
                parent
            }
        } else {
            parent
        };
        let parent = if let Some(right_sibling) = parent_cell.right_sibling() {
            let right_parent_cell = self.allocator.get(right_sibling)?;
            /* trace!(
                "parent max: l(orig)={:?}, r(new)={:?}",
                parent_cell.max_key(),
                right_parent_cell.max_key(),
            ); */

            let use_right = if let Some(ref right_min) = right_parent_cell.min_key()? {
                median_key >= right_min
            } else {
                false
            };

            if use_right {
                right_sibling
            } else {
                parent
            }
        } else {
            parent
        };
        Ok(parent)
    }

    /// Tries to get a matching record based on the given key data.
    ///
    /// Only returns an error if something went wrong trying to find the data, and returns `Ok(None` if no
    /// problems occurred but an associated record was not present.
    pub fn get(&self, key_data: &KeyData) -> Result<Option<Box<[u8]>>, WeaverError> {
        if let Some(monitor) = self.monitor.get() {
            monitor.reads.fetch_add(1, atomic::Ordering::Relaxed);
        }
        let leaf = self.find_leaf(key_data, false)?;
        let allocator = &self.allocator;
        let leaf = allocator.get(leaf)?;
        let cell = leaf.get(key_data)?;
        match cell {
            None => Ok(None),
            Some(Cell::Key(_)) => Err(WeaverError::CellTypeMismatch {
                page_id: leaf.page_id(),
                expected: PageType::KeyValue,
                actual: PageType::Key,
            }),
            Some(Cell::KeyValue(value)) => Ok(Some(Box::from(value.record()))),
        }
    }
    pub fn count<T: Into<KeyDataRange>>(&self, key_data_range: T) -> Result<u64, WeaverError> {
        let range = key_data_range.into();
        let Some(root) = *self.root.read() else {
            return Ok(0);
        };
        let start_node = match range.start_bound() {
            Bound::Included(k) | Bound::Excluded(k) => self.find_leaf(k, false)?,
            Bound::Unbounded => self.left_most(root)?,
        };
        let end_node = match range.end_bound() {
            Bound::Included(k) | Bound::Excluded(k) => self.find_leaf(k, false)?,
            Bound::Unbounded => self.right_most(root)?,
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

        pages.into_iter().try_fold(0_u64, |accum, page_id| {
            let page = self.allocator.get(page_id)?;
            let page_range = page.key_range()?;
            if let Some(on_page) = page_range.intersection(&range) {
                let page_cells = page.get_range(on_page)?.len();
                Ok(accum + page_cells as u64)
            } else {
                Ok(accum)
            }
        })
    }

    /// Gets the set of rows from a given range
    pub fn range<T: Into<KeyDataRange>>(
        &self,
        key_data_range: T,
    ) -> Result<Vec<Box<[u8]>>, WeaverError> {
        let range = key_data_range.into();
        let Some(root) = *self.root.read() else {
            return Ok(vec![]);
        };
        let start_node = match range.start_bound() {
            Bound::Included(k) | Bound::Excluded(k) => self.find_leaf(k, false)?,
            Bound::Unbounded => self.left_most(root)?,
        };
        let end_node = match range.end_bound() {
            Bound::Included(k) | Bound::Excluded(k) => self.find_leaf(k, false)?,
            Bound::Unbounded => self.right_most(root)?,
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

        pages
            .into_iter()
            .try_fold(vec![], |mut vec, page_id| {
                let page = self.allocator.get(page_id)?;
                let page_range = page.key_range()?;
                if let Some(on_page) = page_range.intersection(&range) {
                    let page_cells = page
                        .get_range(on_page)?
                        .into_iter()
                        .flat_map(|cell| cell.into_key_value_cell())
                        .map(|cell| Box::from(cell.record()));
                    vec.extend(page_cells);
                }
                Ok(vec)
            })
            .inspect(|cells| {
                if let Some(monitor) = self.monitor.get() {
                    monitor
                        .reads
                        .fetch_add(cells.len(), atomic::Ordering::Relaxed);
                }
            })
    }

    /// Gets all rows
    #[inline]
    pub fn all(&self) -> Result<Vec<Box<[u8]>>, WeaverError> {
        self.range(..)
    }

    /// Gets the minimum key stored in this btree
    pub fn min_key(&self) -> Result<Option<KeyData>, WeaverError> {
        if let Some(root) = self.root.read().as_ref() {
            let left = self.left_most(*root)?;
            let page = self.allocator.get(left)?;
            page.min_key()
        } else {
            Ok(None)
        }
    }

    /// Gets the maximum key stored in this btree
    pub fn max_key(&self) -> Result<Option<KeyData>, WeaverError> {
        if let Some(root) = self.root.read().as_ref() {
            let left = self.right_most(*root)?;
            let page = self.allocator.get(left)?;
            page.max_key()
        } else {
            Ok(None)
        }
    }

    /// Finds the leaf node that can contain the given key
    fn find_leaf(&self, key_data: &KeyData, expand: bool) -> Result<PageId, WeaverError> {
        let Some(mut ptr) = *self.root.read() else {
            return Err(WeaverError::NotFound(key_data.clone()));
        };
        let mut traversal = vec![];
        loop {
            let page = self.allocator.get(ptr).unwrap();
            match page.page_type() {
                PageType::Key => {
                    let cells = to_ranges(page.all()?);
                    let found = cells.binary_search_by(|(kdr, _)| {
                        if kdr.contains(key_data) {
                            Ordering::Equal
                        } else {
                            match kdr.end_bound() {
                                Bound::Included(i) => i.cmp(key_data),
                                Bound::Excluded(i) => match i.cmp(key_data) {
                                    Ordering::Equal => Ordering::Less,
                                    v => v,
                                },
                                Bound::Unbounded => Ordering::Less,
                            }
                        }
                    });
                    match found {
                        Ok(good) => {
                            let cell = &cells[good].1;
                            let Cell::Key(key) = cell else {
                                unreachable!("key cell pages only contain key cells")
                            };
                            traversal.push((cells[good].0.clone(), key.page_id()));
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
                                warn!("could not get key {key_data:?} in {cells:#?}");
                                return Err(WeaverError::NotFound(key_data.clone()));
                            }
                        }
                    }
                }
                PageType::KeyValue => {
                    break;
                }
            }
        }
        // trace!("find leaf for key {key_data:?} had traversal: {traversal:#?}");
        Ok(ptr)
    }

    /// Finds the leaf node that can contain the given key
    fn _find_internal(&self, key_data: &KeyData) -> Result<PageId, WeaverError> {
        let Some(mut ptr) = *self.root.read() else {
            return Err(WeaverError::NotFound(key_data.clone()));
        };
        let mut traversal = vec![];
        loop {
            let page = self.allocator.get(ptr).unwrap();
            match page.page_type() {
                PageType::Key => {
                    let cells = to_ranges(page.all()?);
                    if cells.iter().any(|(_range, cell)| match cell {
                        Cell::Key(cell) => {
                            let child = cell.page_id();
                            let child_page =
                                self.allocator.get(child).expect("could not get child");
                            child_page.page_type() == PageType::KeyValue
                        }
                        Cell::KeyValue(_) => true,
                    }) {
                        // breaks when at the lowest level before key values
                        break;
                    }
                    let found = cells.binary_search_by(|(kdr, _)| {
                        if kdr.contains(key_data) {
                            Ordering::Equal
                        } else {
                            match kdr.end_bound() {
                                Bound::Included(i) => i.cmp(key_data),
                                Bound::Excluded(i) => match i.cmp(key_data) {
                                    Ordering::Equal => Ordering::Less,
                                    v => v,
                                },
                                Bound::Unbounded => Ordering::Less,
                            }
                        }
                    });
                    match found {
                        Ok(good) => {
                            let cell = &cells[good].1;
                            let Cell::Key(key) = cell else {
                                unreachable!("key cell pages only contain key cells")
                            };
                            traversal.push((cells[good].0.clone(), key.page_id()));
                            ptr = key.page_id()
                        }
                        Err(_close) => {
                            return Err(WeaverError::NotFound(key_data.clone()));
                        }
                    }
                }
                PageType::KeyValue => {
                    break;
                }
            }
        }
        // trace!("find leaf for key {key_data:?} had traversal: {traversal:#?}");
        Ok(ptr)
    }

    /// Increases the max
    fn increase_max(&self, leaf: PageId, new_max: &KeyData) -> Result<(), WeaverError> {
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
                .ok_or_else(|| WeaverError::ReadDataError(ReadDataError::PageNotFound(prev)))?;
            let old_key_data = old_cell.key_data();
            if &old_key_data < new_max {
                let removed = parent_page.delete(&old_key_data)?;
                if let Some(_removed) = removed {
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
    fn right_most(&self, node: PageId) -> Result<PageId, WeaverError> {
        let page = self.allocator.get(node)?;
        match page.page_type() {
            PageType::Key => {
                if let Some(ref max) = page.max_key()? {
                    let max = page
                        .get(max)?
                        .expect("max cell")
                        .into_key_cell()
                        .expect("is always key cell")
                        .page_id();
                    self.right_most(max)
                } else {
                    Ok(node)
                }
            }
            PageType::KeyValue => Ok(node),
        }
    }

    /// left most (min) page
    fn left_most(&self, node: PageId) -> Result<PageId, WeaverError> {
        let page = self.allocator.get(node)?;
        match page.page_type() {
            PageType::Key => {
                if let Some(ref min) = page.min_key()? {
                    let max = page
                        .get(min)?
                        .expect("min cell")
                        .into_key_cell()
                        .expect("is always key cell")
                        .page_id();
                    self.left_most(max)
                } else {
                    Ok(node)
                }
            }
            PageType::KeyValue => Ok(node),
        }
    }

    /// Verifies the integrity of the tree.
    ///
    /// Only runs when debug assertions are enabled
    #[inline]
    pub fn verify_integrity(&self) {
        #[cfg(debug_assertions)]
        {
            let root = *self.root.read();
            match root {
                None => {}
                Some(root) => self.verify_integrity_(root).unwrap(),
            }
        }
    }

    /// Verifies the integrity of the tree
    fn verify_integrity_(&self, page_id: PageId) -> Result<(), WeaverError> {
        let page = self.allocator.get(page_id)?;
        match page.page_type() {
            PageType::Key => {
                let ranges = to_ranges(page.all()?);
                for (range, node) in ranges {
                    let key_cell = node.into_key_cell().unwrap();
                    let child = self.allocator.get(key_cell.page_id())?;

                    if let Some(ref min) = child.min_key()? {
                        if !range.contains(min) {
                            let _ = self.print();
                            error!("verify failed, check backtrace for details");
                            panic!("verify failed. range does not contain minimum. range = {:?}, min={:?}. page {page_id} -> {}", range, min, child.page_id())
                        }
                    }

                    if let Some(ref max) = child.max_key()? {
                        if !range.contains(max) {
                            let _ = self.print();
                            error!("verify failed, check backtrace for details");
                            panic!(
                                "verify failed because max ({:?}) not in range ({:?}). page {page_id} -> {}",
                                max, range, child.page_id()
                            );
                        }
                        match range.1.as_ref() {
                            Bound::Included(i) => {
                                if max > i {
                                    let _ = self.print();
                                    error!("verify failed, check backtrace for details");
                                    panic!(
                                        "upper limit in bound ({:?}) should always be at least max key ({:?}). page {page_id} -> {}",
                                        range.1, max, child.page_id()
                                    );
                                }
                            }
                            Bound::Excluded(i) => {
                                if max > i {
                                    let _ = self.print();
                                    error!("verify failed, check backtrace for details");
                                    panic!(
                                        "upper limit in bound ({:?}) should always be at least max key ({:?}). page {page_id} -> {}",
                                        range.1, max, child.page_id()
                                    );
                                }
                            }
                            Bound::Unbounded => {}
                        }
                        if range.1.as_ref() != Bound::Included(max) {}
                    }
                    self.verify_integrity_(key_cell.page_id())?;
                }
                Ok(())
            }
            PageType::KeyValue => Ok(()),
        }
    }

    pub fn print(&self) -> Result<(), WeaverError> {
        let mut builder = TreeBuilder::new("btree".to_string());
        if let Some(root) = *self.root.read() {
            self.print_(root, &mut builder, None)?;
        }
        let built = builder.build();
        print_tree(&built)?;
        /* trace!("{}", String::from_utf8_lossy(&vec)); */
        Ok(())
    }

    pub fn write<W: Write>(&self, writer: W) -> Result<(), WeaverError> {
        let mut builder = TreeBuilder::new("btree".to_string());
        if let Some(root) = *self.root.read() {
            self.print_(root, &mut builder, None)?;
        }
        let built = builder.build();
        write_tree(&built, writer)?;
        Ok(())
    }

    fn print_(
        &self,
        page_id: PageId,
        builder: &mut TreeBuilder,
        _prev: Option<&KeyData>,
    ) -> Result<(), WeaverError> {
        let page = self.allocator.get(page_id)?;
        builder.begin_child(format!(
            "({:?}) {:?}. nodes {} (l: {:?}, r: {:?})",
            page.page_type(),
            page.page_id(),
            self.nodes_from_page(page_id)?,
            page.left_sibling(),
            page.right_sibling()
        ));
        match page.page_type() {
            PageType::Key => {
                let mut prev = None;
                for cell in page.all()? {
                    if let Some(key_cell) = cell.into_key_cell() {
                        let title = match &prev {
                            None => {
                                format!("(,{:?}]", key_cell.key_data())
                            }
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
    pub fn nodes_from_page(&self, page_id: PageId) -> Result<usize, WeaverError> {
        let page = self.allocator.get(page_id)?;
        match page.page_type() {
            PageType::Key => Ok(page
                .all()?
                .into_iter()
                .filter_map(Cell::into_key_cell)
                .map(|cell| self.nodes_from_page(cell.page_id()))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .sum::<usize>()
                + 1),
            PageType::KeyValue => Ok(1),
        }
    }

    pub fn nodes(&self) -> Result<usize, WeaverError> {
        let root = *self.root.read();
        if let Some(root) = root {
            self.nodes_from_page(root)
        } else {
            Ok(0)
        }
    }
}

impl<P: Pager> Monitorable for BPlusTree<P> {
    fn monitor(&self) -> Box<dyn Monitor> {
        Box::new(
            self.monitor
                .get_or_init(|| {
                    let monitor = self.allocator.monitor();
                    BPlusTreeMonitor::new(monitor)
                })
                .clone(),
        )
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

#[derive(Clone)]
struct BPlusTreeMonitor {
    pager_monitor: Arc<Mutex<Box<dyn Monitor>>>,
    start_time: Instant,
    reads: Arc<AtomicUsize>,
    inserts: Arc<AtomicUsize>,
    updates: Arc<AtomicUsize>,
    deletes: Arc<AtomicUsize>,
    splits: Arc<AtomicUsize>,
}

impl Debug for BPlusTreeMonitor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnbufferedTableMonitor")
            .finish_non_exhaustive()
    }
}

impl BPlusTreeMonitor {
    fn new(pager: Box<dyn Monitor>) -> Self {
        Self {
            pager_monitor: Arc::new(Mutex::new(pager)),
            start_time: Instant::now(),
            reads: Arc::new(Default::default()),
            inserts: Arc::new(Default::default()),
            updates: Arc::new(Default::default()),
            deletes: Arc::new(Default::default()),
            splits: Arc::new(Default::default()),
        }
    }
}

impl Monitor for BPlusTreeMonitor {
    fn name(&self) -> &str {
        "BPlusTree"
    }
    fn stats(&mut self) -> Stats {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let r = self.reads.load(std::sync::atomic::Ordering::Relaxed) as f64 / elapsed;
        let w = self.inserts.load(std::sync::atomic::Ordering::Relaxed) as f64 / elapsed;
        let u = self.updates.load(std::sync::atomic::Ordering::Relaxed) as f64 / elapsed;
        let d = self.deletes.load(std::sync::atomic::Ordering::Relaxed) as f64 / elapsed;
        let splits = self.splits.load(std::sync::atomic::Ordering::Relaxed) as f64 / elapsed;

        Stats::from_iter([
            ("pager", self.pager_monitor.lock().stats()),
            ("reads", Stats::Throughput(r)),
            ("writes", Stats::Throughput(w)),
            ("updates", Stats::Throughput(u)),
            ("deletes", Stats::Throughput(d)),
            ("splits", Stats::Throughput(splits)),
        ])
    }
}

#[cfg(test)]
mod tests {
    use rand::distributions::Alphanumeric;
    use rand::Rng;

    use crate::data::serde::deserialize_data_untyped;
    use crate::data::types::Type;
    use crate::data::values::DbVal;
    use crate::storage::paging::file_pager::FilePager;
    use crate::storage::paging::traits::VecPager;

    use super::*;

    #[test]
    fn create_b_plus_tree() {
        let _ = BPlusTree::new(VecPager::new(1028));
    }

    #[test]
    fn insert_into_b_plus_tree() {
        let btree = BPlusTree::new(VecPager::new(128));
        btree.insert([1], [1, 2, 3]).expect("could not insert");
        let raw = btree.get(&[1].into()).unwrap().unwrap();
        /* trace!("raw: {:x?}", raw); */
        let read =
            deserialize_data_untyped(raw, vec![Type::Integer; 3]).expect("could not deserialize");
        assert_eq!(&read[0], &1.into());
        assert_eq!(&read[1], &2.into());
        assert_eq!(&read[2], &3.into());
    }

    #[test]
    fn recover_state_from_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("btree");
        {
            let btree = BPlusTree::new(FilePager::open_or_create(&path).unwrap());
            const MAX: i64 = 256;
            for i in 0..MAX {
                if let Err(e) = btree.insert([i], [1 + i, 2 * i]) {
                    btree.print().expect("could not print");
                    panic!("error occurred on loop {i}: {e}");
                }
            }
        }
        {
            let btree = BPlusTree::new(FilePager::open(&path).unwrap());
            assert_eq!(btree.all().expect("could not get all").len(), 256);
            const MAX: i64 = 256;
            for i in 0..MAX {
                let v = btree.get(&KeyData::from([i])).unwrap();
                assert!(matches!(v, Some(_)), "should have id")
            }
        }
    }

    #[test]
    fn insert_into_b_plus_tree_many() {
        let btree = BPlusTree::new(VecPager::new(180));

        const MAX: i64 = 256;
        for i in 0..MAX {
            if let Err(e) = btree.insert([i], [1 + i, 2 * i]) {
                btree.print().expect("could not print");
                panic!("error occurred: {e}");
            }
        }
        btree.print().expect("could not print");
        btree.verify_integrity();

        for i in 0..MAX {
            let raw = btree
                .get(&[i].into())
                .unwrap()
                .unwrap_or_else(|| panic!("could not get record for key {i}"));
            let read = deserialize_data_untyped(raw, vec![Type::Integer; 3])
                .expect("could not deserialize");
            assert_eq!(&read[0], &(i + 1).into());
            assert_eq!(&read[1], &(2 * i).into());
        }
    }

    #[test]
    fn insert_into_b_plus_tree_many_rand_string() {
        let btree = BPlusTree::new(VecPager::new(1028));

        const MAX: i64 = 512;
        let mut strings = vec![];
        for i in 0..MAX {
            let s: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(rand::thread_rng().gen_range(5..=15))
                .map(char::from)
                .collect();
            strings.push(s.clone());

            if let Err(e) = btree.insert(
                [DbVal::string(s.clone(), 16)],
                [DbVal::from(s), (2 * i).into()],
            ) {
                btree.print().expect("could not print");
                panic!("error occurred: {e}");
            }
        }
        btree.print().expect("could not print");
        btree.verify_integrity();

        for i in &strings {
            let _gotten = btree
                .get(&[i].into())
                .unwrap()
                .unwrap_or_else(|| panic!("could not get record for key {i}"));
            /* trace!("gotten: {:x?}", gotten); */
        }
    }

    #[test]
    fn insert_into_b_plus_tree_many_inc_string() {
        let btree = BPlusTree::new(VecPager::new(4096));

        const MAX: i64 = 512;
        let mut strings = vec![];
        for i in (0..MAX).rev() {
            let s: String = i.to_string();
            strings.push(s.clone());

            if let Err(e) = btree.insert([s.clone()], [DbVal::from(s)]) {
                btree.print().expect("could not print");
                panic!("error occurred: {e}");
            }
        }
        btree.print().expect("could not print");
        btree.verify_integrity();

        for i in &strings {
            let raw = btree
                .get(&[i].into())
                .unwrap()
                .unwrap_or_else(|| panic!("could not get record for key {i}"));
            let read = deserialize_data_untyped(raw, vec![Type::String(128); 1])
                .expect("could not deserialize");
            let s = read[0].to_string();
            assert_eq!(&s, i);
        }
    }

    #[test]
    fn insert_into_b_plus_tree_many_rand() {
        let btree = BPlusTree::new(VecPager::new(2048));
        for i in 1..=(1024) {
            if let Err(e) = btree.insert(
                [rand::thread_rng().gen_range(-256000..=256000)],
                [1 + i, 2 * i],
            ) {
                btree.print().expect("could not print");
                panic!("error occurred: {e}");
            }
        }

        btree.print().expect("could not print");
    }

    #[test]
    fn insert_into_b_plus_tree_many_rand_floats() {
        let btree = BPlusTree::new(VecPager::new(2048));
        let mut rng = rand::thread_rng();
        for _i in 1..=(1024) {
            let k = rng.gen_range(-1000.0..1000.0);
            if let Err(e) = btree.insert([k], [k]) {
                btree.print().expect("could not print");
                panic!("error occurred: {e}");
            }
        }

        btree.print().expect("could not print");
    }
}
