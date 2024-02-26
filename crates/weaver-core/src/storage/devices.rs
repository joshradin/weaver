//! Defines structs that implemented the [StorageDevice] trait

use std::time::Instant;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::io;
use std::fs::Metadata;
use std::fmt::Debug;
use static_assertions::assert_obj_safe;
use crate::monitoring::{Monitor, Monitorable, Stats};
use crate::storage::StorageDeviceDelegate;

pub mod ram_file;
pub mod mmap_file;

/// A file which allows for random access
pub trait StorageDevice: Debug + Monitorable {
    /// Gets the metadata of a storage file
    fn metadata(&self) -> io::Result<Metadata>;
    /// Sets the new length of the file, either extending it or truncating it
    fn set_len(&mut self, len: u64) -> io::Result<()>;
    /// Write data at a given offset
    fn write(&mut self, offset: u64, data: &[u8]) -> io::Result<()>;
    /// Read data at given offset into a buffer
    fn read(&self, offset: u64, buffer: &mut [u8]) -> io::Result<u64>;
    /// Read an exact amount of data, returning an error if this can't be done
    fn read_exact(&self, offset: u64, len: u64) -> io::Result<Vec<u8>>;
    /// Gets the length of the random access file
    fn len(&self) -> u64;
    fn flush(&mut self) -> io::Result<()>;
    fn sync(&mut self) -> io::Result<()>;

    /// Converts a [StorageDevice] into a [StorageDeviceDelegate], which wraps this storage file in an
    /// object-safe manner.
    fn into_delegate(self) -> StorageDeviceDelegate
    where
        Self: Sized + Send + Sync + 'static,
    {
        StorageDeviceDelegate::new(self)
    }
}

assert_obj_safe!(StorageDevice);

impl StorageDevice for StorageDeviceDelegate {
    fn metadata(&self) -> io::Result<Metadata> {
        self.delegate.metadata()
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.delegate.set_len(len)
    }

    fn write(&mut self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.delegate.write(offset, data)
    }

    fn read(&self, offset: u64, buffer: &mut [u8]) -> io::Result<u64> {
        self.delegate.read(offset, buffer)
    }

    fn read_exact(&self, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        self.delegate.read_exact(offset, len)
    }

    fn len(&self) -> u64 {
        self.delegate.len()
    }

    fn flush(&mut self) -> io::Result<()> {
        self.delegate.flush()
    }

    fn sync(&mut self) -> io::Result<()> {
        self.delegate.sync()
    }

    fn into_delegate(self) -> StorageDeviceDelegate
    where
        Self: Sized + Send + Sync + 'static,
    {
        self
    }
}

#[derive(Debug, Clone)]
pub struct StorageDeviceMonitor {
    pub start: Instant,
    pub reads: Arc<AtomicUsize>,
    pub bytes_read: Arc<AtomicUsize>,
    pub writes: Arc<AtomicUsize>,
    pub bytes_written: Arc<AtomicUsize>,
    pub flushes: Arc<AtomicUsize>,
}

impl StorageDeviceMonitor {
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

impl Monitor for StorageDeviceMonitor {
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


