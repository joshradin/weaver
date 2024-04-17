use std::collections::VecDeque;
use crate::common::hex_dump::HexDump;
use crate::common::pretty_bytes::PrettyBytes;
use crate::monitoring::{Monitor, Monitorable};
use crate::storage::devices::{StorageDevice, StorageDeviceMonitor};
use std::fmt::{Debug, Formatter};
use std::fs::{File, Metadata};
use std::io;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::iter::FusedIterator;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::OnceLock;
use tracing::trace;

/// A random access file allows for accessing the contents of a file
/// at any given point within the file.
///
/// Random access files are buffered, and only access the required data to back a given byte
/// when it's required.
///
/// It will also only flush to the file and modify if it's written to.
pub struct RandomAccessFile {
    file: File,
    length: u64,
    monitor: OnceLock<StorageDeviceMonitor>,
}

impl Debug for RandomAccessFile {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RandomAccessFile")
            .field("file", &self.file)
            .field("length", &self.length)
            .finish()
    }
}

impl RandomAccessFile {
    pub fn with_file(file: File) -> io::Result<Self> {
        let length = file.metadata()?.len();
        Ok(Self {
            file,
            length,
            monitor: OnceLock::new(),
        })
    }

    pub fn create<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        File::create(path).and_then(Self::with_file)
    }

    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        File::open(path).and_then(Self::with_file)
    }

    pub fn open_or_create<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        File::options()
            .create(true)
            .write(true)
            .read(true)
            .truncate(false)
            .open(path)
            .and_then(Self::with_file)
    }

    /// Gets an iterator of bytes over a random access file
    pub fn bytes(&self) -> Bytes {
        Bytes {
            len: self.len(),
            counted: 0,
            buffer: Default::default(),
            offset: 0,
            file: self,
        }
    }
}

impl Monitorable for RandomAccessFile {
    fn monitor(&self) -> Box<dyn Monitor> {
        Box::new(self.monitor.get_or_init(StorageDeviceMonitor::new).clone())
    }
}

impl StorageDevice for RandomAccessFile {
    fn metadata(&self) -> io::Result<Metadata> {
        self.file.metadata()
    }
    /// Sets the new length of the file, either extending it or truncating it
    fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)?;
        self.sync()?;
        Ok(())
    }
    /// Write data at a given offset
    fn write(&mut self, offset: u64, data: &[u8]) -> io::Result<()> {
        if offset > self.length || offset + data.len() as u64 > self.len() {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                "can't seek past end of file",
            ));
        }
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(data)?;
        trace!("wrote {:#x?} to file {:?}", HexDump::new(data), self.file);
        if let Some(stats) = self.monitor.get() {
            stats.flushes.fetch_add(1, Ordering::Relaxed);
            stats.writes.fetch_add(1, Ordering::Relaxed);
            stats.bytes_written.fetch_add(data.len(), Ordering::Relaxed);
        }
        Ok(())
    }
    /// Read data at given offset into a buffer
    fn read(&self, offset: u64, buffer: &mut [u8]) -> io::Result<u64> {
        if offset > self.length {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                "can't seek past end of file",
            ));
        }
        (&self.file).seek(SeekFrom::Start(0))?;
        let read = (&self.file).read(buffer).map(|u| u as u64)?;
        trace!(
            "read {:#x?} from file {:?}",
            HexDump::new(&buffer[..read as usize]),
            self.file
        );
        if let Some(stats) = self.monitor.get() {
            stats.reads.fetch_add(1, Ordering::Relaxed);
            stats.bytes_read.fetch_add(read as usize, Ordering::Relaxed);
        }

        Ok(read)
    }
    /// Read an exact amount of data, returning an error if this can't be done
    fn read_exact(&self, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        if offset > self.length || offset + len > self.len() {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                "can't seek past end of file",
            ));
        }

        let mut vec = vec![0_u8; len as usize];
        (&self.file).seek(SeekFrom::Start(offset))?;
        (&self.file).read_exact(&mut vec)?;
        trace!("read {:#x?} from file {:?}", HexDump::new(&vec), self.file);
        if let Some(stats) = self.monitor.get() {
            stats.reads.fetch_add(1, Ordering::Relaxed);
            stats.bytes_read.fetch_add(len as usize, Ordering::Relaxed);
        }
        Ok(vec)
    }
    /// Gets the length of the random access file
    fn len(&self) -> u64 {
        self.length
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()?;
        if let Some(stats) = self.monitor.get() {
            stats.flushes.fetch_add(1, Ordering::Relaxed);
        }
        self.file.sync_all()?;
        self.file.sync_data()?;
        Ok(())
    }
    fn sync(&mut self) -> io::Result<()> {
        self.flush()?;
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

/// A bytes iterator.
///
/// Buffers bytes
#[derive(Debug)]
pub struct Bytes<'a> {
    len: u64,
    counted: u64,
    buffer: VecDeque<u8>,
    offset: u64,
    file: &'a RandomAccessFile
}

impl<'a> Iterator for Bytes<'a> {
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        if self.counted == self.len {
            return None;
        }
        if self.buffer.is_empty() {
            let mut buffer = vec![0_u8; 256];
            let read = self.file.read(self.offset, &mut buffer).expect("failed to read bytes");
            self.offset += read;
            self.buffer.extend(&buffer[..read as usize]);
        }
        let last = self.buffer.pop_back();
        if last.is_some() {
            self.counted += 1;
        }
        last
    }
}

impl FusedIterator for Bytes<'_> {}



#[cfg(test)]
mod tests {
    use test_log::test;
    use tempfile::{tempdir, tempfile};

    use crate::storage::devices::StorageDevice;

    use crate::storage::devices::ram_file::RandomAccessFile;

    #[test]
    fn write_to_ram_file() {
        let temp = tempfile().expect("could not create tempfile");
        let mut ram = RandomAccessFile::with_file(temp).expect("could not create ram file");
        ram.set_len(128).unwrap();
        let test = [1, 2, 3, 4, 5, 6];
        ram.write(0, &test).expect("could not write");
        let mut buffer = [0; 16];
        let read = ram.read(0, &mut buffer).expect("could not read");
        assert_eq!(&buffer[..test.len()], &test);
    }

    #[test]
    fn debug_file() {
        let temp = tempfile().expect("could not create tempfile");
        let mut ram = RandomAccessFile::with_file(temp).expect("could not create ram file");
        const TEXT: &'static [u8; 1208] = br#"
Lorem ipsum dolor sit amet,
consectetur adipiscing elit. Integer
efficitur purus non orci pellentesque,
vitae varius nisi lobortis. Nam ac
congue nisi. Morbi vel dolor est. Proin
eget tortor tempus, lobortis orci at,
sodales urna. Donec vulputate convallis
tortor eu dictum. Vivamus nec rhoncus
odio. Integer risus est, venenatis ut
faucibus id, ultricies eu orci. Nullam
tortor tellus, dignissim sit amet ante
eu, tempus luctus velit.

Proin est erat, viverra sit amet dictum
eget, imperdiet in sem. Duis aliquam
pellentesque metus, vel pretium sapien
tristique id. Fusce ut ultricies
turpis, venenatis sagittis augue. Morbi
malesuada eros ut dolor congue
porttitor. Sed in ornare nisi, at
tristique elit. Praesent non lectus non
lectus efficitur cursus. Duis
pellentesque tortor mauris. Vivamus
sapien quam, varius ac facilisis ut,
molestie eu ligula. Ut non metus dolor.
Cras efficitur dictum viverra. Aenean
lectus diam, dictum sed velit at,
interdum interdum ligula. Aliquam nulla
mauris, aliquam a tempus dapibus,
ullamcorper sit amet nibh. Vestibulum
ultrices id quam sed maximus. Etiam
pellentesque, mi et malesuada bibendum,
orci tellus elementum nisi, tempus
aliquet magna lorem ac dolor.
        "#;
        ram.set_len((TEXT.len() + 16).try_into().unwrap())
            .expect("could not set len");
        ram.write(0, TEXT).expect("could not write");

        println!("ram: {ram:#?}");
    }

    #[test]
    fn writes_persist() {
        let tempdir = tempdir().unwrap();
        let file = tempdir.as_ref().join("test.txt");
        {
            let mut ram = RandomAccessFile::open_or_create(&file).expect("could not create ram file");
            ram.set_len(1000).unwrap();
            ram.write(0, b"hello, world").unwrap()
        }
        {
            let ram = RandomAccessFile::open(&file).expect("could not create ram file");
            let mut buffer = vec![0; b"hello, world".len()];
            ram.read(0, &mut buffer).unwrap();
            assert_eq!(buffer, b"hello, world");
        }
    }


}
