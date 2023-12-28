use std::collections::HashMap;
use std::fs::{File, Metadata};
use std::io;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::ops::Index;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use parking_lot::{Mutex, MutexGuard, RwLock};
use crate::storage::abstraction::{Page, Paged};

#[derive(Debug)]
pub struct RandomAccessFile {
    file: File,
    length: u64,
    auto_sync: bool,
}

impl RandomAccessFile {
    pub fn with_file(file: File, auto_sync: bool) -> io::Result<Self> {
        let length = file.metadata()?.len();
        Ok(Self {
            file,
            length,
            auto_sync,
        })
    }

    pub fn create<P: AsRef<Path>>(path: P, auto_sync: bool) -> io::Result<Self> {
        File::create(path).and_then(|file| Self::with_file(file, auto_sync))
    }

    pub fn open<P: AsRef<Path>>(path: P, auto_sync: bool) -> io::Result<Self> {
        File::open(path).and_then(|file| Self::with_file(file, auto_sync))
    }

    pub fn metadata(&self) -> io::Result<Metadata> {
        self.file.metadata()
    }

    /// Sets the new length of the file, either extending it or truncating it
    pub fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)?;
        self.sync()?;
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
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(data)?;
        let new_len = offset + (data.len() as u64);
        if new_len > self.length {
            self.length = new_len;
        }

        if self.auto_sync {
            self.sync()?;
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

        (&self.file).seek(SeekFrom::Start(offset))?;
        let fill_size = (self.length - offset).min(buffer.len() as u64) as usize;
        let inter = &mut buffer[..fill_size];
        (&self.file).read_exact(inter)?;
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

        (&self.file).seek(SeekFrom::Start(offset))?;
        let mut vec = vec![0_u8; len as usize];
        (&self.file).read_exact(&mut vec)?;
        Ok(vec)
    }

    /// Gets the length of the random access file
    pub fn len(&self) -> u64 {
        self.length
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.file.sync_all()?;
        self.length = self.file.metadata()?.len();
        Ok(())
    }
}

impl TryFrom<File> for RandomAccessFile {
    type Error = io::Error;

    fn try_from(value: File) -> Result<Self, Self::Error> {
        RandomAccessFile::with_file(value, false)
    }
}

/// Provides a paged abstraction over a [RandomAccessFile]
#[derive(Debug)]
pub struct PagedFile {
    raf: Arc<RwLock<RandomAccessFile>>,
    usage_map: Arc<RwLock<HashMap<usize, Arc<AtomicBool>>>>,
    page_len: usize
}

impl PagedFile {

    /// Creates a new paged file
    pub fn new(file: RandomAccessFile, page_len: usize) -> Self {
        Self { raf: Arc::new(RwLock::new(file)), usage_map: Default::default(), page_len }
    }
}

impl Paged for PagedFile {
    type Page<'a> = FilePage<'a>;
    type Err = io::Error;

    fn page_size(&self) -> usize {
        self.page_len
    }

    fn get<'a>(&'a self, index: usize) -> Result<Self::Page<'a>, Self::Err> {
        let offset = index * self.page_len;
        if offset + self.page_len > self.raf.try_read().ok_or_else(|| io::Error::new(ErrorKind::WouldBlock, "would block because already borrowed mutably"))?.len() as usize + self.page_len {
            return Err(io::Error::new(ErrorKind::InvalidInput, "out of bounds"));
        }
        let mut usage_map = self.usage_map.write();
        let token = usage_map.entry(offset)
                           .or_default()
                           .clone();
        token
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .map_err(|_| io::Error::new(ErrorKind::WouldBlock, ""))?;

        let buf = self.raf.try_read().ok_or_else(|| io::Error::new(ErrorKind::WouldBlock, "would block because already borrowed mutably"))?.read_exact(offset as u64, self.page_len as u64)?;
        Ok(FilePage {
            file: self.raf.clone(),
            usage_token: token,
            buffer: buf,
            offset: offset as u64,
            len: self.page_len as u64,
            _phantom: PhantomData,
        })
    }

    fn new(&mut self) -> Result<(Self::Page<'_>, usize), Self::Err> {
        let new_index = {
            let mut guard = self.raf.try_write().ok_or_else(|| io::Error::new(ErrorKind::WouldBlock, "would block because already borrowed immutably"))?;
            let new_index = guard.len() as usize / self.page_len;
            let new_len = guard.len() + self.page_len as u64;
            guard.set_len(new_len)?;
            new_index
        };
        self.get(new_index).map(|page| (page, new_index))
    }

    fn remove(&mut self, index: usize) -> Result<(), Self::Err> {
        todo!()
    }
}

/// A page from a random access fille
#[derive(Debug)]
pub struct FilePage<'a> {
    file: Arc<RwLock<RandomAccessFile>>,
    usage_token: Arc<AtomicBool>,
    buffer: Vec<u8>,
    offset: u64,
    len: u64,
    _phantom: PhantomData<&'a ()>
}

impl<'a> Page<'a> for FilePage<'a> {
    fn len(&self) -> usize {
        self.len as usize
    }

    fn as_slice(& self) -> &[u8] {
        self.buffer.as_slice()
    }

    fn as_mut_slice(& mut self) -> & mut [u8] {
        self.buffer.as_mut_slice()
    }
}

impl<'a> Drop for FilePage<'a> {
    fn drop(&mut self) {
        let _ = self.file.write().write(self.offset, &self.buffer[..]);
        self.usage_token.store(false, Ordering::SeqCst);
    }
}



#[cfg(test)]
mod tests {
    use tempfile::tempfile;
    use crate::storage::abstraction::Page;

    use super::{PagedFile, RandomAccessFile, Paged, FilePage};

    #[test]
    fn write_to_ram_file() {
        let temp = tempfile().expect("could not create tempfile");
        let mut ram = RandomAccessFile::with_file(temp, true).expect("could not create ram file");
        let test = [1, 2, 3, 4, 5, 6];
        ram.write(0, &test).expect("could not write");
        let mut buffer = [0; 16];
        let read = ram.read(0, &mut buffer).expect("could not read");
        assert_eq!(&buffer[..read as usize], &test);
    }

    #[test]
    fn paged() {
        let temp = tempfile().expect("could not create tempfile");
        let mut ram = RandomAccessFile::with_file(temp, true).expect("could not create ram file");
        let mut paged = PagedFile::new(ram, 4096);
        let (mut page, index): (FilePage, _) = paged.new().unwrap();
        let slice = page.get_mut(..128).unwrap();
        slice[..6].copy_from_slice(&[0, 1, 2, 3, 4, 5]);
        drop(page);
        let page = paged.get(index).unwrap();
        let slice = page.get(..6).unwrap();
        assert_eq!(slice, &[0, 1, 2, 3, 4, 5]);
    }
}
