//! # Slotted B-Trees

use std::borrow::Cow;
use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io::Write;
use std::num::NonZeroU32;
use std::ops::{Bound, Deref, RangeBounds};
use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;
use ptree::{print_tree, Style, TreeBuilder, TreeItem};
use tracing::{error, trace, warn};

use crate::common::batched::to_n_batches;
use crate::data::row::{OwnedRow, Row};
use crate::error::Error;
use crate::key::{KeyData, KeyDataRange};
use crate::storage::cells::{Cell, KeyCell, KeyValueCell};
use crate::storage::ram_file::RandomAccessFile;
use crate::storage::slotted_page::{
    KeySlottedPage, KeyValueSlottedPage, PageType, SlottedPage, PAGE_SIZE,
};
use crate::storage::WriteDataError;

/// A disk backed b-tree
#[derive(Debug)]
pub struct DiskBTree {
    ram: Arc<RwLock<RandomAccessFile>>,
    flat_tree: Vec<RwLock<BTreeNode>>,
}

impl DiskBTree {
    /// Opens/creates a disk b tree at a given location
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Self::new(file)
    }

    /// Opens/creates a disk b tree at a given location
    pub fn new(file: File) -> Result<Self, Error> {
        let ram = Arc::new(RwLock::new(RandomAccessFile::with_file(file, false)?));
        let mut pages = SlottedPage::open_vector(&ram)?;
        let mut nodes = vec![];
        if pages.is_empty() {
            // empty means no saved data
            // create a new btree
            nodes.push(RwLock::new(BTreeNode::RootNode {
                page: SlottedPage::init_shared(
                    &ram,
                    NonZeroU32::new(1).unwrap(),
                    PageType::KeyValue,
                    0,
                )?,
                children: None,
            }));
        }

        Ok(Self {
            ram,
            flat_tree: nodes,
        })
    }

    /// Insert a row
    pub fn insert(&mut self, key_data: KeyData, row: OwnedRow) -> Result<(), Error> {
        let split_id = {
            let root = self.root_node();
            // attempt 1:
            match BTreeNode::insert_rw(root, key_data.clone(), &row, &self.flat_tree) {
                Err(Error::WriteDataError(WriteDataError::AllocationFailed { page_id, size })) => {
                    trace!("failed to allocate enough space in page {}", page_id);
                    if size > PAGE_SIZE - (PAGE_SIZE / 4) {
                        warn!(
                            "Can not store row {:?} because size is larger than .75 * PAGE_SIZE",
                            row
                        );
                        return Err(Error::WriteDataError(WriteDataError::AllocationFailed {
                            page_id,
                            size,
                        }));
                    }
                    page_id
                }
                Err(Error::OutOfRange) => {
                    panic!("out of range")
                }
                other => return other,
            }
        };
        // split node
        self.split_node(split_id)?;
        // attempt 2; after split has occurred
        let root = &self.flat_tree[0];
        BTreeNode::insert_rw(root, key_data.clone(), &row, &self.flat_tree)
    }

    ///  Gets the max depth of this btree
    pub fn depth(&self) -> usize {
        self.root_node().read().depth(&self.flat_tree)
    }

    ///  Gets the number of nodes in this btree
    pub fn nodes(&self) -> usize {
        self.root_node().read().nodes(&self.flat_tree)
    }

    /// Approximates optimal depth based on number of nodes
    pub fn optimal_depth(&self) -> f64 {
        (self.nodes() as f64).log(3.0)
    }

    fn root_node(&self) -> &RwLock<BTreeNode> {
        &self.flat_tree[0]
    }

    fn get_node_by_page(&self, page_id: u32) -> Option<&RwLock<BTreeNode>> {
        self.flat_tree
            .iter()
            .find(|node| node.read().page_id() == page_id)
    }

    /// Splits a node into one key node and three key value nodes
    fn split_node(&mut self, page_id: u32) -> Result<(), Error> {
        let cells = {
            let node = self
                .get_node_by_page(page_id)
                .expect("gave page id that doesn't exist in this btree");
            let node = node.read();
            SplitData::split_data(node.page())?
        };

        let SplitData {
            keys: [k1, k2, k3],
            cells: [split1, split2, split3],
            left_sibling,
            right_sibling,
        } = cells;

        let insert_all = |cells: Vec<Cell>, page: &mut SlottedPage| -> Result<(), Error> {
            for cell in cells {
                page.insert(cell)?;
            }
            Ok(())
        };


        let mut node_1 =
            SlottedPage::init_last_shared(&self.ram, self.next_id(), PageType::KeyValue)?;
        let node_1_id = node_1.page_id();
        insert_all(split1, &mut node_1)?;
        self.flat_tree.push(RwLock::new(BTreeNode::LeafNode {
            page: KeyValueSlottedPage::try_from(node_1)?,
            parent: page_id,
            left_sibling,
            right_sibling: None,
        }));
        let mut node_2 =
            SlottedPage::init_last_shared(&self.ram, self.next_id(), PageType::KeyValue)?;
        let node_2_id = node_2.page_id();
        self.flat_tree.last().unwrap().write().set_right_sibling(node_2_id);
        insert_all(split2, &mut node_2)?;
        self.flat_tree.push(RwLock::new(BTreeNode::LeafNode {
            page: KeyValueSlottedPage::try_from(node_2)?,
            parent: page_id,
            left_sibling: Some(node_1_id),
            right_sibling: None,
        }));
        let mut node_3 =
            SlottedPage::init_last_shared(&self.ram, self.next_id(), PageType::KeyValue)?;
        let node_3_id = node_3.page_id();
        self.flat_tree.last().unwrap().write().set_right_sibling(node_3_id);
        insert_all(split3, &mut node_3)?;
        self.flat_tree.push(RwLock::new(BTreeNode::LeafNode {
            page: KeyValueSlottedPage::try_from(node_3)?,
            parent: page_id,
            left_sibling: Some(node_2_id),
            right_sibling,
        }));



        let mut key_node =
            SlottedPage::init_last_shared(&self.ram, page_id.try_into().unwrap(), PageType::Key)?;
        key_node.insert(Cell::from(KeyCell::new(node_1_id, k1)))?;
        key_node.insert(Cell::from(KeyCell::new(node_2_id, k2)))?;
        key_node.insert(Cell::from(KeyCell::new(node_3_id, k3)))?;

        let mut node = self
            .get_node_by_page(page_id)
            .expect("gave page id that doesn't exist in this btree");
        let is_root = match &*node.read() {
            BTreeNode::RootNode { .. } => true,
            _ => false,
        };

        let new_node = if is_root {
            BTreeNode::root_from(key_node)?
        } else {
            let parent = match &*node.read() {
                BTreeNode::LeafNode { parent, .. } => *parent,
                BTreeNode::InternalNode { parent, .. } => *parent,
                _ => unreachable!(),
            };
            BTreeNode::new(key_node, parent)?
        };

        *node.write() = new_node;

        if !self.is_balanced() {
            self.rebalance(page_id)?
        }

        Ok(())
    }

    /// Re-balances the tree, starting at a given page
    fn rebalance(&mut self, page_id: u32) -> Result<(), Error> {
        print_structure(self);
        todo!("node split caused inbalance: {:#?}", self.balance_factor());
    }

    fn next_id(&self) -> NonZeroU32 {
        NonZeroU32::new(
            self.flat_tree
                .iter()
                .map(|node| node.read().page_id())
                .max()
                .map(|max| max + 1)
                .unwrap_or(1),
        )
        .unwrap()
    }


    /// Gets the right-most page id
    fn rightmost(&self) -> Result<u32, Error> {
        let mut ptr = self.root_node();
        loop {
            let page_type =  ptr.read().page().page_type();
            match page_type {
                PageType::Key => {
                    let children = ptr.read().children().expect("key should always have keys");
                    let last_child = children.last().unwrap();
                    ptr = self.get_node_by_page(*last_child).unwrap();
                }
                PageType::KeyValue => {
                    break;
                }
            }
        }
        Ok(ptr.read().page_id())
    }

    /// Gets the left-most page id
    fn leftmost(&self) -> Result<u32, Error> {
        let mut ptr = self.root_node();
        loop {
            let page_type =  ptr.read().page().page_type();
            match page_type {
                PageType::Key => {
                    let children = ptr.read().children().expect("key should always have keys");
                    let last_child = children.first().unwrap();
                    ptr = self.get_node_by_page(*last_child).unwrap();
                }
                PageType::KeyValue => {
                    break;
                }
            }
        }
        Ok(ptr.read().page_id())
    }

    /// Gets the balance factors of all the nodes in this tree.
    ///
    /// Balance factor is defined as the maximum height minus
    /// the minimum height of the sub trees of a given node.
    pub fn balance_factor(&self) -> BTreeMap<u32, usize> {
        self.flat_tree
            .iter()
            .map(|node| node.read().page_id())
            .map(|node_id| (node_id, self.node_balance_factor(node_id)))
            .collect()
    }

    /// Checks if this tree is balanced
    pub fn is_balanced(&self) -> bool {
        self.balance_factor()
            .iter()
            .all(|(_, &bf)| {
                bf == 0 || bf == 1
            })
    }

    fn node_balance_factor(&self, id: u32) -> usize {
        let node = self.get_node_by_page(id).expect("no node found");
        match node.read().children() {
            None => { 0 }
            Some(children) => {
                let child_depths = children.into_iter()
                    .map(|node| self.get_node_by_page(node).expect("node must exist").read().depth(&self.flat_tree))
                    .collect::<Vec<_>>();
                child_depths.iter().max().zip(child_depths.iter().min())
                    .map(|(&max, &min)| {
                        max - min
                    })
                    .unwrap_or(0)
            }
        }
    }

    pub fn get(&self, key_data: &KeyData) -> Result<Option<OwnedRow>, Error> {
        todo!()
    }

    pub fn delete(&self, key_data: &KeyData) -> Result<Option<OwnedRow>, Error> {
        todo!()
    }

    pub fn range<R: RangeBounds<KeyData>>(&self, range: R) -> Result<Vec<OwnedRow>, Error> {
        self.range_(KeyDataRange::from(range))
    }

    /// Gets the range
    fn range_(&self, range: KeyDataRange) -> Result<Vec<OwnedRow>, Error> {
        todo!()
    }

    pub fn all(&self) -> Result<Vec<OwnedRow>, Error> {
        self.range(..)
    }
}

#[derive(Debug)]
enum BTreeNode {
    RootNode {
        page: SlottedPage,
        /// When present, this node is a key_node
        children: Option<Vec<(KeyDataRange, u32)>>,
    },
    InternalNode {
        page: KeySlottedPage,
        parent: u32,
        left_sibling: Option<u32>,
        right_sibling: Option<u32>,
        ranges: Vec<(KeyDataRange, u32)>,
    },
    LeafNode {
        page: KeyValueSlottedPage,
        parent: u32,
        left_sibling: Option<u32>,
        right_sibling: Option<u32>,
    },
}

impl BTreeNode {
    fn root_from(page: SlottedPage) -> Result<Self, Error> {
        match page.page_type() {
            PageType::Key => {
                let children = page.range(&KeyDataRange::from(..))?;
                let ranges = children
                    .iter()
                    .filter_map(|cell| match cell {
                        Cell::Key(key) => Some(key),
                        _ => None,
                    })
                    .scan(Bound::Unbounded, |bound, cell| {
                        let key_data = cell.key_data();
                        let range = KeyDataRange(bound.clone(), Bound::Included(key_data.clone()));
                        *bound = Bound::Excluded(key_data);
                        Some((range, cell.page_id()))
                    })
                    .collect::<Vec<_>>();
                Ok(Self::RootNode {
                    page,
                    children: Some(ranges),
                })
            }
            PageType::KeyValue => Ok(BTreeNode::RootNode {
                page,
                children: None,
            }),
        }
    }
    fn new(page: SlottedPage, parent: u32) -> Result<Self, Error> {
        match page.page_type() {
            PageType::Key => {
                let children = page.range(&KeyDataRange::from(..))?;
                let ranges = children
                    .iter()
                    .filter_map(|cell| match cell {
                        Cell::Key(key) => Some(key),
                        _ => None,
                    })
                    .scan(Bound::Unbounded, |bound, cell| {
                        let right_bound = cell.key_data();
                        let range = KeyDataRange(bound.clone(), Bound::Included(right_bound.clone()));
                        *bound = Bound::Excluded(right_bound);
                        Some((range, cell.page_id()))
                    })
                    .collect::<Vec<_>>();
                Ok(Self::InternalNode {
                    page: KeySlottedPage::try_from(page)?,
                    parent,
                    left_sibling: None,
                    right_sibling: None,
                    ranges,
                })
            }
            PageType::KeyValue => Ok(BTreeNode::LeafNode {
                page: KeyValueSlottedPage::try_from(page)?,
                parent,
                left_sibling: None,
                right_sibling: None,
            }),
        }
    }

    fn right_sibling(&self) -> Option<&u32> {
        match self {
            BTreeNode::RootNode { .. } => { None }
            BTreeNode::InternalNode { right_sibling, .. } => { right_sibling.as_ref() }
            BTreeNode::LeafNode { right_sibling, .. } => {  right_sibling.as_ref()}
        }
    }

    fn set_right_sibling(&mut self, id: impl Into<Option<u32>>) {
        match self {
            BTreeNode::RootNode { .. } => { }
            BTreeNode::InternalNode { right_sibling, .. } => { *right_sibling = id.into() }
            BTreeNode::LeafNode { right_sibling, .. } => {  *right_sibling = id.into() }
        }
    }

    fn left_sibling(&self) -> Option<&u32> {
        match self {
            BTreeNode::RootNode { .. } => { None }
            BTreeNode::InternalNode { left_sibling, .. } => { left_sibling.as_ref() }
            BTreeNode::LeafNode { left_sibling, .. } => {  left_sibling.as_ref()}
        }
    }
    fn set_left_sibling(&mut self, id: impl Into<Option<u32>>) {
        match self {
            BTreeNode::RootNode { .. } => { }
            BTreeNode::InternalNode { left_sibling, .. } => { *left_sibling = id.into() }
            BTreeNode::LeafNode { left_sibling, .. } => {  *left_sibling = id.into() }
        }
    }

    fn page_id(&self) -> u32 {
        match self {
            BTreeNode::RootNode { page, .. } => page.page_id(),
            BTreeNode::InternalNode { page, .. } => page.page_id(),
            BTreeNode::LeafNode { page, .. } => page.page_id(),
        }
    }

    fn children(&self) -> Option<Vec<u32>> {
        match self {
            BTreeNode::RootNode { children: Some(children), .. } | BTreeNode::InternalNode { ranges: children, .. } => {
                Some(children
                    .iter()
                    .map(|(range, id)| *id)
                    .collect())

            }
            _ => None
        }
    }

    fn get(
        &self,
        key_data: &KeyData,
        nodes: &[RwLock<BTreeNode>],
    ) -> Result<Option<KeyValueCell>, Error> {
        match self {
            BTreeNode::RootNode { page, children } => match children {
                None => Self::get_key_value_cell(key_data, page),
                Some(ranges) => Self::get_ranged(key_data, nodes, ranges),
            },
            BTreeNode::InternalNode { ranges, .. } => Self::get_ranged(key_data, nodes, ranges),
            BTreeNode::LeafNode { page, .. } => Self::get_key_value_cell(key_data, page),
        }
    }

    /// Gets all rows within a key data range
    fn range(
        &self,
        key_data: &KeyDataRange,
        nodes: &[RwLock<BTreeNode>],
    ) -> Result<Vec<Cell>, Error> {
        match self {
            BTreeNode::RootNode { page, children } => match children {
                None => page.range(key_data),
                Some(ranges) => Self::range_ranged(key_data, nodes, ranges),
            },
            BTreeNode::InternalNode { ranges, .. } => Self::range_ranged(key_data, nodes, ranges),
            BTreeNode::LeafNode { page, .. } => page.range(key_data),
        }
    }

    fn range_ranged(
        key_data: &KeyDataRange,
        nodes: &[RwLock<BTreeNode>],
        ranges: &[(KeyDataRange, u32)],
    ) -> Result<Vec<Cell>, Error> {
        let mut output = vec![];
        for (range, child) in ranges {
            if range.overlaps(&key_data) {
                let mut child_node = nodes[*child as usize].read();
                output.extend(child_node.range(key_data, nodes)?);
            }
        }
        Ok(output)
    }

    fn insert(
        &mut self,
        key_data: KeyData,
        row: &Row,
        nodes: &[RwLock<BTreeNode>],
    ) -> Result<(), Error> {
        match self {
            BTreeNode::RootNode { page, children } => match children {
                None => page.insert(KeyValueCell::new(key_data, row.to_owned())),
                Some(ranges) => Self::insert_ranged(key_data, row, nodes, ranges),
            },
            BTreeNode::InternalNode { ranges, .. } => {
                Self::insert_ranged(key_data, row, nodes, ranges)
            }
            BTreeNode::LeafNode { page, .. } => {
                page.insert(KeyValueCell::new(key_data, row.to_owned()))
            }
        }
    }

    fn insert_rw(
        node: &RwLock<BTreeNode>,
        key_data: KeyData,
        row: &Row,
        nodes: &[RwLock<BTreeNode>],
    ) -> Result<(), Error> {
        let ranges = {
            let mut guard = node.write();
            let ranges = match &mut *guard {
                BTreeNode::RootNode { page, children } => match children {
                    None => return page.insert(KeyValueCell::new(key_data, row.to_owned())),
                    Some(ranges) => ranges.clone(),
                },
                BTreeNode::InternalNode { ranges, .. } => {
                    ranges.clone()
                }
                BTreeNode::LeafNode { page, .. } => {
                    return page.insert(KeyValueCell::new(key_data, row.to_owned()));
                }
            };
            drop(guard);
            ranges
        };
        Self::insert_ranged(key_data, row, nodes, &ranges)
    }

    fn insert_ranged(
        key_data: KeyData,
        row: &Row,
        nodes: &[RwLock<BTreeNode>],
        ranges: &Vec<(KeyDataRange, u32)>,
    ) -> Result<(), Error> {
        for (index, &(ref range, child_id)) in ranges.iter().enumerate() {
            if range.contains(&key_data) {
                trace!("key {:?} for page {} in range {:?}", key_data, child_id, range);
                let option = Self::get_node(nodes, child_id);
                let Some(mut node) = option else {
                    error!("page {} is unavailable", child_id);
                    return Err(Error::ChildNotFound(child_id));
                };
                trace!("inserting into page {}", child_id);
                let mut child_node = node.write();
                return child_node.insert(key_data, row, nodes);
            } else if index == ranges.len() - 1 && range.is_greater(&key_data) {
                let page = Self::get_node(nodes, child_id).unwrap_or_else(|| panic!("page {} should always exist", child_id));
                let right_sibling = page.read().right_sibling().copied();
                match right_sibling {
                    None => {
                        let right_most_id = page.read().right_most_child(nodes);
                        let mut breadcrumbs = VecDeque::new();
                        let mut id = right_most_id;
                        loop {
                            breadcrumbs.push_back(id);
                            let node = Self::get_node(nodes, id).unwrap_or_else(|| panic!("page {} should always exist", id));
                            let Some(parent) = node.read().parent() else {
                                break;
                            };
                            id = parent;
                        }
                        let right_most= Self::get_node(nodes, right_most_id).unwrap_or_else(|| panic!("page {} should always exist", child_id));
                        right_most.try_write().unwrap_or_else(|| {
                            panic!("could not get write access to page {} (locked: {}, x-locked: {})", right_most_id, right_most.is_locked(), right_most.is_locked_exclusive())
                        }).insert(key_data.clone(), row, nodes)?;
                        while let Some(id) = breadcrumbs.pop_front() {
                            let node = Self::get_node(nodes, id).unwrap_or_else(|| panic!("page {} should always exist", child_id));
                            let mut node = node.write();
                            node.set_new_max(key_data.clone()).unwrap_or_else(|e| {
                                panic!("failed to new new max for page {}: {}", id, e);
                            })
                        }

                        return Ok(())
                    }
                    Some(right_sibling) => {
                        // in case of right sibling existing
                        trace!("inserting into right sibling {}", right_sibling);
                        let node = Self::get_node(nodes, right_sibling).unwrap_or_else(|| panic!("right sibling page {} should always exist", child_id));
                        return node.write().insert(key_data, row, nodes);
                    }
                }
            }
        }
        Err(Error::OutOfRange)
    }

    fn set_new_max(&mut self, value: KeyData) -> Result<(), Error> {
        match self {
            BTreeNode::RootNode { page, children: Some(ranges) } => {
                let (lk, cell) = page.last_key_value().unwrap();
                page.delete(&lk)?;
                let Cell::Key(key_cell) = cell else {
                    panic!("must be key cell")
                };
                page.insert(KeyCell::new(key_cell.page_id(), value.clone()))?;
                let (range, _) = ranges.last_mut().unwrap();
                range.1 = Bound::Included(value);
                Ok(())
            }
            BTreeNode::InternalNode {page, ranges, .. } => {
                let (lk, cell) = page.last_key_value().unwrap();
                page.delete(&lk)?;
                let Cell::Key(key_cell) = cell else {
                    panic!("must be key cell")
                };
                page.insert(KeyCell::new(key_cell.page_id(), value.clone()))?;
                let (range, _) = ranges.last_mut().unwrap();
                range.1 = Bound::Included(value);
                Ok(())
            }
            _ => {
                Ok(())
            }
        }
    }

    fn get_node(nodes: &[RwLock<BTreeNode>], child: u32) -> Option<&RwLock<BTreeNode>> {
        nodes.iter().find(|node| {
            node.try_read()
                .map(|node| node.page_id() == child)
                .unwrap_or(false)
        })
    }

    fn get_ranged(
        key_data: &KeyData,
        nodes: &[RwLock<BTreeNode>],
        ranges: &Vec<(KeyDataRange, u32)>,
    ) -> Result<Option<KeyValueCell>, Error> {
        for (range, child) in ranges {
            if range.contains(key_data) {
                let child_node = nodes[*child as usize].read();
                return child_node.get(key_data, nodes);
            }
        }
        unreachable!(
            "since leftmost and rightmost are unbounded, this should not be possible to reach"
        );
    }

    fn get_key_value_cell(
        key_data: &KeyData,
        page: &SlottedPage,
    ) -> Result<Option<KeyValueCell>, Error> {
        match page.get_cell(key_data) {
            None => Ok(None),
            Some(Cell::KeyValue(key_value)) => Ok(Some(key_value)),
            Some(_) => Err(Error::CellTypeMismatch {
                expected: PageType::KeyValue,
                actual: PageType::Key,
            }),
        }
    }

    /// Gets the key data range for this node
    fn key_data_range(&self) -> Result<KeyDataRange, Error> {
        match self {
            BTreeNode::RootNode { page, children } => {
                let kdr = page.key_data_range();
                Ok(kdr)
            }
            BTreeNode::InternalNode { ranges, .. } => {
                Ok(ranges.iter()
                    .map(|(range, _)| range)
                    .cloned()
                    .reduce(|accum, ref range| {
                        accum.union(range).expect("ranges should be connectable")
                    })
                    .expect("should always have >= 1 values")
                )
            }
            BTreeNode::LeafNode { page, .. } => {
                let kdr = page.key_data_range();
                Ok(kdr)
            }
        }
    }

    /// Gets the id of the right most child
    fn right_most_child(&self, nodes: &[RwLock<BTreeNode>]) -> u32 {
        match self.children() {
            None => {
                self.page_id()
            }
            Some(children) => {
                Self::get_node(nodes, *children.last().unwrap())
                    .unwrap()
                    .read()
                    .right_most_child(nodes)
            }
        }
    }

    /// Gets the id of the left most child
    fn left_most_child(&self, nodes: &[RwLock<BTreeNode>]) -> u32 {
        match self.children() {
            None => {
                self.page_id()
            }
            Some(children) => {
                Self::get_node(nodes, *children.first().unwrap())
                    .unwrap()
                    .read()
                    .left_most_child(nodes)
            }
        }
    }

    fn parent(&self) -> Option<u32> {
        match self {
            BTreeNode::RootNode { .. } => { None }
            &BTreeNode::InternalNode { parent, ..} => { Some(parent )}
            &BTreeNode::LeafNode { parent, .. } => { Some(parent)}
        }
    }

    fn page(&self) -> &SlottedPage {
        match self {
            BTreeNode::RootNode { page, .. } => page,
            BTreeNode::InternalNode { page, .. } => page.deref(),
            BTreeNode::LeafNode { page, .. } => page.deref(),
        }
    }

    /// Gets the depth that this node
    fn depth(&self, nodes: &[RwLock<BTreeNode>]) -> usize {
        match self.children() {
            None => { 1 }
            Some(children) => {
                children.into_iter()
                    .map(|page| Self::get_node(nodes, page).unwrap().read().depth(nodes))
                    .max()
                    .unwrap_or_default() + 1
            }
        }
    }

    /// Gets the total number of nodes in this part of the tree
    fn nodes(&self, nodes: &[RwLock<BTreeNode>]) -> usize {
        match self.children() {
            None => { 1 }
            Some(children) => {
                children.into_iter()
                        .map(|page| Self::get_node(nodes, page).unwrap().read().nodes(nodes))
                        .sum::<usize>() + 1
            }
        }
    }
}

struct SplitData {
    keys: [KeyData; 3],
    cells: [Vec<Cell>; 3],
    left_sibling: Option<u32>,
    right_sibling: Option<u32>,
}

impl SplitData {
    /// Split a slotted page into split data
    fn split_data(page: &SlottedPage) -> Result<SplitData, Error> {
        if page.page_type() != PageType::KeyValue {
            // Only key values
            return Err(Error::CellTypeMismatch {
                expected: PageType::KeyValue,
                actual: PageType::Key,
            });
        }

        let cells = page.range(&(..).into())?;
        let batches = to_n_batches(3, cells)
            .map(|batch| batch.collect::<Vec<_>>())
            .collect::<Vec<_>>();

        let cells: [Vec<Cell>; 3] = batches.try_into().unwrap();
        let keys: [KeyData; 3] = cells
            .iter()
            .take(3)
            .flat_map(|key| key.iter().map(|i| i.key_data()).max())
            .collect::<Vec<_>>()
            .try_into()
            .unwrap();
        Ok(SplitData { keys, cells, left_sibling: page.left_sibling_id(), right_sibling: page.right_sibling_id() })
    }
}

enum InsertCleanup {
    RangeExpanded,

}



pub fn print_structure(btree: &DiskBTree) {
    let root = btree.root_node();
    let node = root.read();
    let mut builder = TreeBuilder::new("btree".to_string());
    print_node(&mut builder, &*node, &btree.flat_tree[..]);
    print_tree(&builder.build()).expect("could not print");
}

fn print_node(builder: &mut TreeBuilder, btree: &BTreeNode, nodes: &[RwLock<BTreeNode>]) {
    match btree {
        BTreeNode::RootNode { page, children } => {
            match children {
                None => {
                    builder.add_empty_child(format!("pg {}: values={}", btree.page_id(), page.len()));
                }
                Some(ranges) => {
                    for (range, child) in ranges {
                        if let Some(next) = BTreeNode::get_node(nodes, *child) {
                            builder.begin_child(format!("pg {}->{}: {range:?}", btree.page_id(), child));
                            print_node(builder, &*next.read(), nodes);
                            builder.end_child();
                        }
                    }
                }
            }
        }
        BTreeNode::InternalNode { ranges, .. } => {
            for (range, child) in ranges {
                if let Some(next) = BTreeNode::get_node(nodes, *child) {
                    builder.begin_child(format!("pg {}->{}: {range:?}", btree.page_id(), child));
                    print_node(builder, &*next.read(), nodes);
                    builder.end_child();
                }
            }
        }
        BTreeNode::LeafNode { page, .. } => {
            builder.add_empty_child(format!("pg {}: values={}", btree.page_id(), page.len()));
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::{random, Rng};
    use tempfile::tempfile;

    use crate::data::row::Row;
    use crate::data::values::Value;
    use crate::key::KeyData;
    use crate::storage::b_tree::{DiskBTree, print_structure};

    fn insert_rand(count: usize) {
        insert((0..count).into_iter().map(|_| rand::thread_rng().gen_range(-10_000..=10_000)))
    }

    fn insert<I: IntoIterator<Item = i64>>(iter: I) {
        let temp = tempfile().unwrap();
        let mut btree = DiskBTree::new(temp).expect("couldn't create btree");

        let result = iter.into_iter()
            .try_for_each(|id: i64| {
                btree
                    .insert(
                        KeyData::from([Value::from(id)]),
                        Row::from([Value::from(id), Value::from(id)]).to_owned(),
                    )
            });
        println!("final depth: {}", btree.depth());
        println!("final node count: {}", btree.nodes());
        println!("target depth: {}", btree.optimal_depth());
        println!("balance factors: {:#?}", btree.balance_factor());
        print_structure(&btree);
        let _ = result.expect("failed");
        assert!(btree.is_balanced(), "btree is not balanced");
    }

    #[test]
    fn insert_100() {
        insert_rand(100);
    }

    #[test]
    fn insert_1000() {
        insert_rand(1000);
    }

    #[test]
    fn insert_10000() {
        insert_rand(10000);
    }

    #[test]
    fn insert_0_to_10000() {
        insert(0..=10000);
    }

    #[test]
    fn insert_10000_to_0() {
        insert((0..=10000).into_iter().rev());
    }
}
