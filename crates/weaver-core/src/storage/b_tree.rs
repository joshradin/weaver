//! # Slotted B-Trees

use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::common::ram_file::RandomAccessFile;
use crate::data::row::OwnedRow;
use crate::error::Error;
use crate::key::KeyData;
use crate::storage::slotted_page::SlottedPage;

/// A disk backed b-tree
#[derive(Debug)]
pub struct DiskBTree {
    ram: Arc<Mutex<RandomAccessFile>>,
    tree: Option<DiskBTreeNode>
}

impl DiskBTree {
    /// Opens/creates a disk b tree at a given location
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let ram = Arc::new(Mutex::new(RandomAccessFile::open(path, true)?));
        let lock = ram.lock();
        if lock.metadata()?.len() > 0 {
            drop(lock);
            Ok(DiskBTree {
                ram,
                tree: None,
            })
        } else {
            drop(lock);
            Ok(DiskBTree {
                ram,
                tree: None,
            })
        }
    }

    /// Insert a row
    pub fn insert(&self, key_data: KeyData, row: OwnedRow) {

    }

    pub fn get(&self, key_data: &KeyData) -> Option<OwnedRow> {
        todo!()
    }

    pub fn delete(&self, key_data: &KeyData) -> Option<OwnedRow> {
        todo!()
    }

    pub fn range<R : RangeBounds<KeyData>>(&self, range: R) -> Option<()> {
        todo!()
    }
}

#[derive(Debug)]
enum DiskBTreeNode {
    Closed { id: u32, index: usize },
    Open(SlottedPage),
}

impl DiskBTreeNode {
    fn open(&mut self, file: &Arc<Mutex<RandomAccessFile>>) -> Result<(), Error> {
        if let &mut Self::Closed { id, index } = self {
            *self = Self::Open(SlottedPage::open_mutex(file.clone(), index)?);
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), Error> {
        if let Self::Open(open) = self {
            let id = open.page_id();
            let index = open.index();
            *self = Self::Closed { id, index }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {


}