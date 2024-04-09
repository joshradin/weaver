//! Storage device backed by mmap instead of files

use std::fs::{File, Metadata};
use std::io;
use std::io::{ErrorKind, Write};
use std::sync::atomic::Ordering;
use std::sync::OnceLock;

use memmap2::MmapMut;

use crate::error::WeaverError;
use crate::monitoring::{Monitor, Monitorable};
use crate::storage::devices::{StorageDevice, StorageDeviceMonitor};

/// A memory mapped file
#[derive(Debug)]
pub struct MMapFile {
    file: File,
    mmap: MmapMut,
    monitor: OnceLock<StorageDeviceMonitor>,
}

impl MMapFile {
    pub fn with_file(file: File) -> Result<Self, WeaverError> {
        unsafe {
            let mmap = MmapMut::map_mut(&file)?;
            Ok(Self {
                file,
                mmap,
                monitor: Default::default(),
            })
        }
    }
}

impl Monitorable for MMapFile {
    fn monitor(&self) -> Box<dyn Monitor> {
        Box::new(
            self.monitor
                .get_or_init(StorageDeviceMonitor::new)
                .clone(),
        )
    }
}

impl StorageDevice for MMapFile {
    fn metadata(&self) -> std::io::Result<Metadata> {
        self.file.metadata()
    }

    fn set_len(&mut self, len: u64) -> std::io::Result<()> {
        self.file.set_len(len)?;
        unsafe {
            self.mmap = MmapMut::map_mut(&self.file)?;
            assert_eq!(
                self.mmap.len(),
                len as usize,
                "mmap did not result in new size of {len}"
            );
        }
        self.file.sync_all()?;
        Ok(())
    }

    fn write(&mut self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        if offset > self.len() || offset + data.len() as u64 > self.len() {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                "can't seek past end of file when writing mmap",
            ));
        }
        self.mmap[(offset as usize)..][..data.len()].copy_from_slice(data);
        if let Some(stats) = self.monitor.get() {
            stats.flushes.fetch_add(1, Ordering::Relaxed);
            stats.writes.fetch_add(1, Ordering::Relaxed);
            stats.bytes_written.fetch_add(data.len(), Ordering::Relaxed);
        }
        Ok(())
    }

    fn read(&self, offset: u64, buffer: &mut [u8]) -> std::io::Result<u64> {
        if offset > self.len() {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                "can't seek past end of file when reading from mmap",
            ));
        }
        let ret = (&mut *buffer)
            .write(&self.mmap[offset as usize..])
            .map(|u| u as u64)?;
        if let Some(stats) = self.monitor.get() {
            stats.reads.fetch_add(1, Ordering::Relaxed);
            stats.bytes_read.fetch_add(ret as usize, Ordering::Relaxed);
        }
        Ok(ret)
    }

    fn read_exact(&self, offset: u64, len: u64) -> std::io::Result<Vec<u8>> {
        if offset > self.len() || offset + len > self.len() {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                format!("can't seek past end of file when reading exact from mmap (offset: {offset}, len: {len}, actual_len:{})", self.mmap.len()),
            ));
        }
        let ret = self.mmap[offset as usize..][..len as usize].to_vec();
        if let Some(stats) = self.monitor.get() {
            stats.reads.fetch_add(1, Ordering::Relaxed);
            stats.bytes_read.fetch_add(len as usize, Ordering::Relaxed);
        }
        Ok(ret)
    }

    fn len(&self) -> u64 {
        self.mmap.len() as u64
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.mmap.flush()?;
        if let Some(stats) = self.monitor.get() {
            stats.flushes.fetch_add(1, Ordering::Relaxed);
        }
        self.file.sync_all()?;
        self.file.sync_data()?;
        Ok(())
    }

    fn sync(&mut self) -> std::io::Result<()> {
        self.file.sync_all()?;
        self.file.sync_data()?;
        Ok(())
    }
}
