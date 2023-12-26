//! # Slotted B-Trees

use std::fs::File;
use std::num::NonZeroU32;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::Arc;

use crate::common::batched::{to_batches, to_n_batches};
use parking_lot::RwLock;
use tracing::{trace, warn};

use crate::common::ram_file::RandomAccessFile;
use crate::data::row::{OwnedRow, Row};
use crate::error::Error;
use crate::key::{KeyData, KeyDataRange};
use crate::storage::cells::{Cell, KeyCell, KeyValueCell};
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
            let mut root = self.flat_tree[0].write();
            // attempt 1:
            match root.insert(key_data.clone(), &row, &self.flat_tree) {
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
                other => return other,
            }
        };
        // split node
        self.split_node(split_id)?;
        // attempt 2; after split has occurred
        let mut root = self.flat_tree[0].write();
        root.insert(key_data, &row, &self.flat_tree)
    }

    fn get_node_by_page(&self, page_id: u32) -> Option<&RwLock<BTreeNode>> {
        self.flat_tree
            .iter()
            .find(|node| node.read().page_id() == page_id)
    }

    /// Splits a node into one key node and three key value nodes
    fn split_node(&mut self, page_id: u32) -> Result<(), Error> {
        let mut cells = {
            let node = self
                .get_node_by_page(page_id)
                .expect("gave page id that doesn't exist in this btree");
            let node = node.read();
            node.range(&KeyDataRange::from(..), &self.flat_tree)?
        };
        let cell_count = dbg!(cells.len());
        let per_split = cell_count / 3;

        let split_1 = cells.drain(..per_split).collect::<Vec<_>>();
        let split_2 = cells.drain(..per_split).collect::<Vec<_>>();
        let split_3 = cells;

        let k1 = split_1.iter().map(|cell| cell.key_data()).max().unwrap();
        let k2 = split_2.iter().map(|cell| cell.key_data()).max().unwrap();
        let k3 = split_3.iter().map(|cell| cell.key_data()).max().unwrap();

        let insert_all = |cells: Vec<Cell>, page: &mut SlottedPage| -> Result<(), Error> {
            for cell in cells {
                page.insert(cell)?;
            }
            Ok(())
        };

        let mut node_1 =
            SlottedPage::init_last_shared(&self.ram, self.next_id(), PageType::KeyValue)?;
        let node_1_id = node_1.page_id();
        insert_all(split_1, &mut node_1)?;
        self.flat_tree.push(RwLock::new(BTreeNode::LeafNode {
            page: KeyValueSlottedPage::try_from(node_1)?,
            parent: page_id,
        }));
        let mut node_2 =
            SlottedPage::init_last_shared(&self.ram, self.next_id(), PageType::KeyValue)?;
        let node_2_id = node_2.page_id();
        insert_all(split_2, &mut node_2)?;
        self.flat_tree.push(RwLock::new(BTreeNode::LeafNode {
            page: KeyValueSlottedPage::try_from(node_2)?,
            parent: page_id,
        }));
        let mut node_3 =
            SlottedPage::init_last_shared(&self.ram, self.next_id(), PageType::KeyValue)?;
        let node_3_id = node_3.page_id();
        insert_all(split_3, &mut node_3)?;
        self.flat_tree.push(RwLock::new(BTreeNode::LeafNode {
            page: KeyValueSlottedPage::try_from(node_3)?,
            parent: page_id,
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
        println!("this: {:#?}", self);

        Ok(())
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
    },
}

fn to_ranges(keys: &[KeyData], pointers: &[u32]) -> Vec<(KeyDataRange, u32)> {
    debug_assert!(
        pointers.len() == keys.len() + 1,
        "should always provide one more pointer than keys"
    );
    let mut vector = vec![];
    let mut target_len = pointers.len();
    let mut keys = keys.iter();
    let mut current_key = None;
    for (i, &pointer) in (0..target_len).into_iter().zip(pointers) {
        match i {
            0 => {
                let key = keys.next().unwrap().clone();
                vector.push((KeyDataRange::from(..key.clone()), pointer));
                current_key = Some(key);
            }
            _ => {
                let key_l = current_key.take().unwrap();
                let key_r = keys.next().unwrap().clone();
                vector.push((KeyDataRange::from(key_l..key_r.clone()), pointer));
                current_key = Some(key_r);
            }
        }
    }

    vector
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
                        let range = KeyDataRange(bound.clone(), Bound::Included(cell.key_data()));
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
            }),
        }
    }

    fn page_id(&self) -> u32 {
        match self {
            BTreeNode::RootNode { page, .. } => page.page_id(),
            BTreeNode::InternalNode { page, .. } => page.page_id(),
            BTreeNode::LeafNode { page, .. } => page.page_id(),
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

    fn insert_ranged(
        key_data: KeyData,
        row: &Row,
        nodes: &[RwLock<BTreeNode>],
        ranges: &mut Vec<(KeyDataRange, u32)>,
    ) -> Result<(), Error> {
        for (i, (range, child)) in ranges.iter().enumerate() {
            if range.contains(&key_data) || i == ranges.len() - 1 {
                println!("key {:?} for page {} in range {:?}", key_data, child, range);
                let option = nodes.iter().find(|node| {
                    node.try_read()
                        .map(|node| node.page_id() == *child)
                        .unwrap_or(false)
                });
                let Some(mut node) = option else {
                    eprintln!("page {} is unavailable", child);
                    return Err(Error::ChildNotFound(*child));
                };
                println!("inserting into page {}", child);
                let mut child_node = node.write();
                return child_node.insert(key_data, row, nodes);
            }
        }
        unreachable!(
            "since leftmost and rightmost are unbounded, this should not be possible to reach"
        );
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
}

struct SplitData {
    keys: [KeyData; 3],
    cells: [Vec<Cell>; 4],
}

impl SplitData {
    /// Split a slotted page into split data
    fn split_data(page: SlottedPage) -> Result<SplitData, Error> {
        if page.page_type() != PageType::KeyValue {
            // Only key values
            return Err(Error::CellTypeMismatch {
                expected: PageType::KeyValue,
                actual: PageType::Key,
            });
        }

        let cells = page.range(&(..).into())?;
        let batches = to_n_batches(4, cells);

        todo!()
    }
}

#[cfg(test)]
mod tests {
    use rand::random;
    use tempfile::tempfile;

    use crate::data::row::Row;
    use crate::data::values::Value;
    use crate::key::KeyData;
    use crate::storage::b_tree::DiskBTree;

    fn insert_rand(count: usize) {
        insert((0..count).into_iter().map(|_| random()))
    }

    fn insert<I: IntoIterator<Item = i64>>(iter: I) {
        let temp = tempfile().unwrap();
        let mut btree = DiskBTree::new(temp).expect("couldn't create btree");
        for id in iter {
            btree
                .insert(
                    KeyData::from([Value::from(id)]),
                    Row::from([Value::from(id), Value::from(id)]).to_owned(),
                )
                .expect("could not insert cell");
        }
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
    fn insert_0_to_10000() {
        insert(0..=10000);
    }
}
