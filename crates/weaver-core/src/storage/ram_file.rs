use std::fmt::{Debug, Formatter};
use std::fs::{File, Metadata};
use std::io;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::ops::Index;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use nom::Parser;

use crate::monitoring::{Monitor, Monitorable, Stats};
use crate::storage::paging::traits::{Page, PageMut, PageMutWithHeader, Pager};
use crate::storage::StorageDevice;

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
    monitor: OnceLock<RAFMonitor>,
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
        File::create(path).and_then(|file| Self::with_file(file))
    }

    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        File::open(path).and_then(|file| Self::with_file(file))
    }

    pub fn open_or_create<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        File::options()
            .create(true)
            .write(true)
            .read(true)
            .truncate(false)
            .open(path)
            .and_then(|file| Self::with_file(file))
    }
}

impl Monitorable for RandomAccessFile {
    fn monitor(&self) -> Box<dyn Monitor> {
        Box::new(self.monitor.get_or_init(RAFMonitor::new).clone())
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
        if offset > self.length {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                "can't seek past end of file",
            ));
        }
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&data)?;
        self.file.flush()?;
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
        if let Some(stats) = self.monitor.get() {
            stats.reads.fetch_add(1, Ordering::Relaxed);
            stats.bytes_read.fetch_add(read as usize, Ordering::Relaxed);
        }

        Ok(read)
    }
    /// Read an exact amount of data, returning an error if this can't be done
    fn read_exact(&self, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        if offset > self.length {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                "can't seek past end of file",
            ));
        }

        let mut vec = vec![0_u8; len as usize];
        (&self.file).seek(SeekFrom::Start(offset))?;
        (&self.file).read_exact(&mut vec)?;
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

#[derive(Debug, Clone)]
struct RAFMonitor {
    start: Instant,
    reads: Arc<AtomicUsize>,
    bytes_read: Arc<AtomicUsize>,
    writes: Arc<AtomicUsize>,
    bytes_written: Arc<AtomicUsize>,
    flushes: Arc<AtomicUsize>,
}

impl RAFMonitor {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
            reads: Default::default(),
            bytes_read: Default::default(),
            writes: Default::default(),
            bytes_written: Default::default(),
            flushes: Default::default(),
        }
    }
}

impl Monitor for RAFMonitor {
    fn name(&self) -> &str {
        "RandomAccessFile"
    }

    fn stats(&mut self) -> Stats {
        let elapsed = self.start.elapsed().as_secs_f64();
        let flushes = self.flushes.load(Ordering::Relaxed) as f64;
        let reads = self.reads.load(Ordering::Relaxed) as f64;
        let bytes_read = self.bytes_read.load(Ordering::Relaxed) as f64;
        let writes = self.writes.load(Ordering::Relaxed) as f64;
        let bytes_written = self.bytes_written.load(Ordering::Relaxed);
        Stats::from_iter([
            ("flushes", Stats::Throughput(flushes / elapsed)),
            ("reads", Stats::Throughput(reads / elapsed)),
            ("bytes_read", Stats::Throughput(bytes_read / elapsed)),
            ("writes", Stats::Throughput(writes / elapsed)),
            (
                "bytes_written",
                Stats::Throughput(bytes_written as f64 / elapsed),
            ),
            ("total_bytes_written", Stats::Integer(bytes_written as i64)),
        ])
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempfile;

    use crate::storage::StorageDevice;

    use super::RandomAccessFile;

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
    fn debug_file() {
        let temp = tempfile().expect("could not create tempfile");
        let mut ram = RandomAccessFile::with_file(temp).expect("could not create ram file");

        ram.write(
            0,
            br#"
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
        "#,
        )
        .expect("could not write");

        println!("ram: {ram:#?}");
    }
}
