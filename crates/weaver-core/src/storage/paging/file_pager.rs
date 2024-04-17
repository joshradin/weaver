//! The base pager of a real files.
//!
//! This pager wraps around a [`RandomAccessFile`](RandomAccessFile)

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::io;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::trace;

use crate::common::hex_dump::HexDump;
use crate::common::track_dirty::Mad;
use crate::error::WeaverError;
use crate::monitoring::{monitor_fn, Monitor, MonitorCollector, Monitorable};
use crate::storage::devices::ram_file::RandomAccessFile;
use crate::storage::devices::StorageDevice;
use crate::storage::paging::traits::{Page, PageMut};
use crate::storage::{Pager, PAGE_SIZE};

/// Provides a paged abstraction over a [RandomAccessFile]
#[derive(Debug)]
pub struct FilePager<F: StorageDevice> {
    raf: Arc<RwLock<F>>,
    usage_map: Arc<RwLock<HashMap<usize, Arc<AtomicI32>>>>,
    page_len: usize,
}

impl FilePager<RandomAccessFile> {
    /// Creates a new paged file
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WeaverError> {
        let path = path.as_ref();
        let ram = RandomAccessFile::open(path)?;
        Ok(Self::with_file_and_page_len(ram, PAGE_SIZE))
    }

    /// Creates a new paged file
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, WeaverError> {
        let path = path.as_ref();
        let ram = RandomAccessFile::create(path)?;
        Ok(Self::with_file_and_page_len(ram, PAGE_SIZE))
    }

    /// Creates a new paged file
    pub fn open_or_create<P: AsRef<Path>>(path: P) -> Result<Self, WeaverError> {
        let path = path.as_ref();
        let ram = RandomAccessFile::open_or_create(path)?;
        Ok(Self::with_file_and_page_len(ram, PAGE_SIZE))
    }
}

impl<F: StorageDevice> FilePager<F> {
    /// Creates a new paged file
    pub fn with_file_and_page_len(file: F, page_len: usize) -> Self {
        let current_file_len = file.len() as usize;
        let pages = current_file_len.div_ceil(page_len);
        let usage_map = HashMap::from_iter((0..pages).map(|i| (i, Default::default())));

        Self {
            raf: Arc::new(RwLock::new(file)),
            usage_map: Arc::new(RwLock::new(usage_map)),
            page_len,
        }
    }

    /// Creates a new paged file
    pub fn with_file(file: F) -> Self {
        Self::with_file_and_page_len(file, PAGE_SIZE)
    }
}

impl<F: StorageDevice> From<F> for FilePager<F> {
    fn from(value: F) -> Self {
        Self::with_file(value)
    }
}

impl<F: StorageDevice> Monitorable for FilePager<F> {
    fn monitor(&self) -> Box<dyn Monitor> {
        let file_monitor = self.raf.read().monitor();
        let collector = MonitorCollector::from_iter([file_monitor]);

        Box::new(monitor_fn("file_pager", move || collector.all()))
    }
}

impl<F: StorageDevice> Pager for FilePager<F> {
    type Page<'a> = FilePage where F: 'a;
    type PageMut<'a> = FilePageMut<F> where F: 'a;
    type Err = WeaverError;

    fn page_size(&self) -> usize {
        self.page_len
    }

    fn get(&self, index: usize) -> Result<Self::Page<'_>, Self::Err> {
        let offset = index * self.page_len;
        if offset + self.page_len
            > self
                .raf
                .try_read()
                .ok_or_else(|| {
                    io::Error::new(
                        ErrorKind::WouldBlock,
                        "would block because already borrowed mutably",
                    )
                })?
                .len() as usize
                + self.page_len
        {
            return Err(io::Error::new(ErrorKind::InvalidInput, "out of bounds").into());
        }
        let mut usage_map = self.usage_map.write();
        let token = usage_map.entry(offset).or_default().clone();
        token
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |old| {
                if old >= 0 {
                    Some(old + 1)
                } else {
                    None
                }
            })
            .map_err(|used| {
                WeaverError::caused_by(
                    "Failed to get page",
                    io::Error::new(
                        ErrorKind::WouldBlock,
                        format!(
                            "Would block, page at offset {} already in use (used: {used})",
                            offset
                        ),
                    ),
                )
            })?;

        let buf = self
            .raf
            .try_read()
            .ok_or_else(|| {
                io::Error::new(
                    ErrorKind::WouldBlock,
                    "would block because already borrowed mutably",
                )
            })?
            .read_exact(offset as u64, self.page_len as u64)?
            .into_boxed_slice();
        let page = FilePage {
            buf,
            usage_token: token,
        };
        trace!("created {}", std::any::type_name_of_val(&page));
        Ok(page)
    }

    fn get_mut(&self, index: usize) -> Result<Self::PageMut<'_>, Self::Err> {
        let offset = index * self.page_len;
        if offset + self.page_len
            > self
                .raf
                .try_read()
                .ok_or_else(|| {
                    io::Error::new(
                        ErrorKind::WouldBlock,
                        "would block because already borrowed mutably",
                    )
                })?
                .len() as usize
                + self.page_len
        {
            return Err(io::Error::new(ErrorKind::InvalidInput, "out of bounds").into());
        }
        let mut usage_map = self.usage_map.write();
        let token = usage_map.entry(offset).or_default().clone();
        token
            .compare_exchange(0, -1, Ordering::SeqCst, Ordering::Relaxed)
            .map_err(|val| {
                WeaverError::caused_by(
                    "Failed to get page",
                    io::Error::new(
                        ErrorKind::WouldBlock,
                        format!(
                            "Would block, page at offset {} already in use (used: {val})",
                            offset
                        ),
                    ),
                )
            })?;

        let buf = self
            .raf
            .try_read()
            .ok_or_else(|| {
                io::Error::new(
                    ErrorKind::WouldBlock,
                    "would block because already borrowed mutably",
                )
            })?
            .read_exact(offset as u64, self.page_len as u64)?
            .into_boxed_slice();
        let page_mut = FilePageMut {
            file: self.raf.clone(),
            usage_token: token,
            buffer: Mad::new(buf),
            offset: offset as u64,
            len: self.page_len as u64,
        };
        trace!("created {}", std::any::type_name_of_val(&page_mut));
        Ok(page_mut)
    }

    fn new_page(&self) -> Result<(Self::PageMut<'_>, usize), Self::Err> {
        let new_index = {
            let mut guard = self.raf.try_write().ok_or_else(|| {
                io::Error::new(
                    ErrorKind::WouldBlock,
                    "would block because already borrowed immutably",
                )
            })?;
            let new_index = guard.len() as usize / self.page_len;
            let new_len = guard.len() + self.page_len as u64;
            guard.set_len(new_len)?;
            new_index
        };
        self.get_mut(new_index).map(|page| (page, new_index))
    }

    fn free(&self, index: usize) -> Result<(), Self::Err> {
        let mut old = self.get_mut(index)?;
        old.as_mut_slice().fill(0);
        Ok(())
    }

    fn allocated(&self) -> usize {
        self.usage_map.read().iter().count()
    }

    fn reserved(&self) -> usize {
        self.raf.read().len() as usize
    }


}

#[derive(Debug)]
pub struct FilePage {
    buf: Box<[u8]>,
    usage_token: Arc<AtomicI32>,
}

impl Drop for FilePage {
    fn drop(&mut self) {
        trace!("dropping {}", std::any::type_name_of_val(self));
        self.usage_token.fetch_sub(1, Ordering::SeqCst);
    }
}

impl<'a> Page<'a> for FilePage {
    fn len(&self) -> usize {
        self.buf.len()
    }
    fn as_slice(&self) -> &[u8] {
        &self.buf
    }
}

/// A page from a random access fille
pub struct FilePageMut<F: StorageDevice> {
    file: Arc<RwLock<F>>,
    usage_token: Arc<AtomicI32>,
    buffer: Mad<Box<[u8]>>,
    offset: u64,
    len: u64,
}

impl<F: StorageDevice> Debug for FilePageMut<F> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilePageMut")
            .field("buffer", &HexDump::new(&*self.buffer))
            .finish()
    }
}

impl<'a, F: StorageDevice> PageMut<'a> for FilePageMut<F> {
    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buffer.to_mut().as_mut()
    }
}

impl<'a, F: StorageDevice> Page<'a> for FilePageMut<F> {
    fn len(&self) -> usize {
        self.len as usize
    }
    fn as_slice(&self) -> &[u8] {
        &self.buffer
    }
}

impl<F: StorageDevice> Drop for FilePageMut<F> {
    fn drop(&mut self) {
        trace!("dropping {}", std::any::type_name_of_val(self));
        let res = self.file.write().write(self.offset, &self.buffer[..]);
        trace!("write on drop for file page mut resulted in {res:?}");
        if self
            .usage_token
            .compare_exchange(-1, 0, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            panic!("atomic usage token should be -1")
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::monitoring::Monitorable;
    use tempfile::{tempdir, tempfile};

    use crate::storage::devices::ram_file::RandomAccessFile;
    use crate::storage::paging::file_pager::{FilePageMut, FilePager};
    use crate::storage::paging::traits::{Page, PageMut};
    use crate::storage::{Pager, VecPager};
    use std::io::Write;
    use crate::storage::paging::virtual_pager::VirtualPagerTable;

    #[test]
    fn paged() {
        let temp = tempfile().expect("could not create tempfile");
        let ram = RandomAccessFile::with_file(temp).expect("could not create ram file");
        let paged = FilePager::with_file_and_page_len(ram, 4096);
        let (mut page, index): (FilePageMut<_>, _) = paged.new_page().unwrap();
        let slice = page.get_mut(..128).unwrap();
        slice[..6].copy_from_slice(&[0, 1, 2, 3, 4, 5]);
        drop(page);
        let page = paged.get(index).unwrap();
        let slice = page.get(..6).unwrap();
        assert_eq!(slice, &[0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn get_stats() {
        let temp = tempfile().expect("could not create tempfile");
        let ram = RandomAccessFile::with_file(temp).expect("could not create ram file");
        let paged = FilePager::with_file_and_page_len(ram, 4096);
        let mut monitor = paged.monitor();

        for i in 0..16 {
            let (mut page, _) = paged.new_page().expect("could not get next page");
            write!(page.as_mut_slice(), "hello page {}", i).expect("could not write");
        }

        let stats = monitor.stats();
        println!("{}: {stats:#?}", monitor.name());
    }

    #[test]
    fn file_pager_is_reusable() {
        let dir = tempdir().unwrap();
        let file_path = dir.as_ref().join("tempfile.vpt");

        {
            let pager = FilePager::open_or_create(&file_path).unwrap();
            let (mut page, 0) = pager.new_page().expect("could not create a new page") else {
                panic!("expected 0 id for page")
            };
            // write a known magic number at a given offset
            page.write_u64(0xDEADBEEF, 16);
        }
        assert!(std::fs::metadata(&file_path).unwrap().len() > 0, "file should contain contents");
        {
            let pager = FilePager::open(&file_path).unwrap();
            let page = pager.get(0).expect("could not create a new page");
            // reada known magic number at a given offset
            let magic = page.read_u64(16).expect("magic number should be present");
            assert_eq!(magic, 0xDEADBEEF);
        }
    }
}
