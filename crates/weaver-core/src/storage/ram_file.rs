use std::collections::HashMap;
use std::fs::{File, Metadata};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::ops::Index;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;
use std::{io, iter};

use parking_lot::RwLock;

use crate::common::track_dirty::Mad;
use crate::error::Error;
use crate::storage::abstraction::{Page, PageMut, PageMutWithHeader, Paged};
use crate::storage::PAGE_SIZE;

#[derive(Debug)]
pub struct RandomAccessFile {
    file: File,
    buffer: Vec<u8>,
    dirty: bool,
    length: u64,
}

impl RandomAccessFile {
    pub fn with_file(file: File) -> io::Result<Self> {
        let length = file.metadata()?.len();
        Ok(Self {
            file,
            buffer: Default::default(),
            dirty: false,
            length,
        })
    }

    pub fn create<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        File::create(path).and_then(|file| Self::with_file(file))
    }

    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        File::open(path).and_then(|file| Self::with_file(file))
    }

    pub fn metadata(&self) -> io::Result<Metadata> {
        self.file.metadata()
    }

    /// Sets the new length of the file, either extending it or truncating it
    pub fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)?;
        self.sync()?;
        if self.buffer.len() < len as usize {
            self.buffer
                .extend(vec![0; len as usize - self.buffer.len()])
        } else if self.buffer.len() > len as usize {
            drop(self.buffer.drain((len as usize)..));
        }
        Ok(())
    }

    /// Write data at a given offset
    pub fn write(&mut self, offset: u64, data: &[u8]) -> io::Result<()> {
        if offset > self.length {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                "can't seek past end of file",
            ));
        }
        self.dirty = true;
        let new_len = offset + (data.len() as u64);

        if offset == self.length {
            self.buffer.extend_from_slice(data);
        } else {
            if new_len > self.length {
                self.buffer
                    .extend(iter::repeat(0_u8).take((new_len - self.length) as usize));
            }
            let buf = &mut self.buffer[offset as usize..][..data.len()];
            buf.copy_from_slice(data);
        }
        if new_len > self.length {
            self.length = new_len;
        }

        Ok(())
    }

    /// Read data at given offset into a buffer
    pub fn read(&self, offset: u64, buffer: &mut [u8]) -> io::Result<u64> {
        if offset > self.length {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                "can't seek past end of file",
            ));
        }

        let fill_size = buffer.len().min((self.length - offset) as usize);

        buffer[..fill_size].copy_from_slice(&self.buffer[(offset as usize)..][..fill_size]);
        Ok(fill_size as u64)
    }

    /// Read an exact amount of data, returning an error if this can't be done
    pub fn read_exact(&self, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        if offset > self.length {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                "can't seek past end of file",
            ));
        }

        let mut vec = vec![0_u8; len as usize];
        vec.copy_from_slice(&self.buffer[offset as usize..][..len as usize]);
        Ok(vec)
    }

    /// Gets the length of the random access file
    pub fn len(&self) -> u64 {
        self.length
    }
    pub fn flush(&mut self) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&self.buffer)?;
        let new_len = self.buffer.len() as u64;
        if new_len > self.length {
            self.length = new_len;
        }
        Ok(())
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.flush()?;
        self.file.sync_all()?;
        self.length = self.file.metadata()?.len();

        Ok(())
    }
}

impl Drop for RandomAccessFile {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

impl TryFrom<File> for RandomAccessFile {
    type Error = io::Error;

    fn try_from(value: File) -> Result<Self, Self::Error> {
        RandomAccessFile::with_file(value)
    }
}

/// Provides a paged abstraction over a [RandomAccessFile]
#[derive(Debug)]
pub struct PagedFile {
    raf: Arc<RwLock<RandomAccessFile>>,
    usage_map: Arc<RwLock<HashMap<usize, Arc<AtomicI32>>>>,
    page_len: usize,
}

impl PagedFile {
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

impl Paged for PagedFile {
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
            .map_err(|_| {
                Error::caused_by(
                    "Failed to get page",
                    io::Error::new(
                        ErrorKind::WouldBlock,
                        format!("Would block, page at offset {} already in use", offset),
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
            .map_err(|_| {
                Error::caused_by(
                    "Failed to get page",
                    io::Error::new(
                        ErrorKind::WouldBlock,
                        format!("Would block, page at offset {} already in use", offset),
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
        dbg!(&old);
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
impl<'a> Page<'a> for FilePage {
    fn len(&self) -> usize {
        self.buf.len()
    }
    fn as_slice(&self) -> &[u8] {
        &*self.buf
    }
}

/// A page from a random access fille
#[derive(Debug)]
pub struct FilePageMut {
    file: Arc<RwLock<RandomAccessFile>>,
    usage_token: Arc<AtomicI32>,
    buffer: Mad<Box<[u8]>>,
    offset: u64,
    len: u64,
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
    use tempfile::tempfile;

    use crate::storage::abstraction::{Page, PageMut};

    use super::{FilePageMut, Paged, PagedFile, RandomAccessFile};

    #[test]
    fn write_to_ram_file() {
        let temp = tempfile().expect("could not create tempfile");
        let mut ram = RandomAccessFile::with_file(temp).expect("could not create ram file");
        let test = [1, 2, 3, 4, 5, 6];
        ram.write(0, &test).expect("could not write");
        let mut buffer = [0; 16];
        let read = ram.read(0, &mut buffer).expect("could not read");
        assert_eq!(&buffer[..read as usize], &test);
    }

    #[test]
    fn paged() {
        let temp = tempfile().expect("could not create tempfile");
        let mut ram = RandomAccessFile::with_file(temp).expect("could not create ram file");
        let mut paged = PagedFile::with_page_len(ram, 4096);
        let (mut page, index): (FilePageMut, _) = paged.new().unwrap();
        let slice = page.get_mut(..128).unwrap();
        slice[..6].copy_from_slice(&[0, 1, 2, 3, 4, 5]);
        drop(page);
        let page = paged.get(index).unwrap();
        let slice = page.get(..6).unwrap();
        assert_eq!(slice, &[0, 1, 2, 3, 4, 5]);
    }
}
