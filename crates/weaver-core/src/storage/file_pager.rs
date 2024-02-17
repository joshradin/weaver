//! The base pager of a real files.
//!
//! This pager wraps around a [`RandomAccessFile`](RandomAccessFile)

use super::ram_file::RandomAccessFile;
use crate::common::track_dirty::Mad;
use crate::error::Error;
use crate::storage::abstraction::{Page, PageMut};
use crate::storage::{Pager, PAGE_SIZE};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::io;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use crate::common::hex_dump::HexDump;

/// Provides a paged abstraction over a [RandomAccessFile]
#[derive(Debug)]
pub struct FilePager {
    raf: Arc<RwLock<RandomAccessFile>>,
    usage_map: Arc<RwLock<HashMap<usize, Arc<AtomicI32>>>>,
    page_len: usize,
}

impl FilePager {
    /// Creates a new paged file
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let path = path.as_ref();
        let ram = RandomAccessFile::create(path)?;
        Ok(Self::with_page_len(ram, PAGE_SIZE))
    }
    /// Creates a new paged file
    pub fn with_page_len(file: RandomAccessFile, page_len: usize) -> Self {
        Self {
            raf: Arc::new(RwLock::new(file)),
            usage_map: Default::default(),
            page_len,
        }
    }
}

impl Pager for FilePager {
    type Page<'a> = FilePage;
    type PageMut<'a> = FilePageMut;
    type Err = Error;

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
                Error::caused_by(
                    "Failed to get page",
                    io::Error::new(
                        ErrorKind::WouldBlock,
                        format!("Would block, page at offset {} already in use (used: {used})", offset),
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
        Ok(FilePage {
            buf,
            usage_token: token,
        })
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
                Error::caused_by(
                    "Failed to get page",
                    io::Error::new(
                        ErrorKind::WouldBlock,
                        format!("Would block, page at offset {} already in use (used: {val})", offset),
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
        Ok(FilePageMut {
            file: self.raf.clone(),
            usage_token: token,
            buffer: Mad::new(buf),
            offset: offset as u64,
            len: self.page_len as u64,
        })
    }

    fn new(&self) -> Result<(Self::PageMut<'_>, usize), Self::Err> {
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

    fn len(&self) -> usize {
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
        self.usage_token.fetch_sub(1, Ordering::SeqCst);
    }
}

impl<'a> Page<'a> for FilePage {
    fn len(&self) -> usize {
        self.buf.len()
    }
    fn as_slice(&self) -> &[u8] {
        &*self.buf
    }
}



/// A page from a random access fille
pub struct FilePageMut {
    file: Arc<RwLock<RandomAccessFile>>,
    usage_token: Arc<AtomicI32>,
    buffer: Mad<Box<[u8]>>,
    offset: u64,
    len: u64,
}

impl Debug for FilePageMut {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilePageMut")
            .field("buffer", &HexDump::new(&*self.buffer))
        .finish()
    }
}

impl<'a> PageMut<'a> for FilePageMut {
    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buffer.to_mut().as_mut()
    }
}

impl<'a> Page<'a> for FilePageMut {
    fn len(&self) -> usize {
        self.len as usize
    }
    fn as_slice(&self) -> &[u8] {
        &*self.buffer
    }
}

impl Drop for FilePageMut {
    fn drop(&mut self) {
        let _ = self.file.write().write(self.offset, &self.buffer[..]);
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
    use crate::storage::abstraction::{Page, PageMut};
    use crate::storage::file_pager::{FilePageMut, FilePager};
    use crate::storage::ram_file::RandomAccessFile;
    use crate::storage::Pager;
    use tempfile::tempfile;

    #[test]
    fn paged() {
        let temp = tempfile().expect("could not create tempfile");
        let mut ram = RandomAccessFile::with_file(temp).expect("could not create ram file");
        let mut paged = FilePager::with_page_len(ram, 4096);
        let (mut page, index): (FilePageMut, _) = paged.new().unwrap();
        let slice = page.get_mut(..128).unwrap();
        slice[..6].copy_from_slice(&[0, 1, 2, 3, 4, 5]);
        drop(page);
        let page = paged.get(index).unwrap();
        let slice = page.get(..6).unwrap();
        assert_eq!(slice, &[0, 1, 2, 3, 4, 5]);
    }
}
