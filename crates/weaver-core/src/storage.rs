//! Storage primitives

use std::borrow::Borrow;
use std::fmt::Debug;
use std::io::Write;
use std::string::FromUtf8Error;

use nom::error::Error;

use thiserror::Error;

use devices::StorageDevice;
pub use paging::traits::{Pager, VecPager};

use crate::monitoring::{Monitor, Monitorable};
use crate::storage::cells::PageId;

pub mod b_plus_tree;
pub mod cells;
pub mod devices;
pub mod engine;
pub mod paging;
pub mod tables;

/// Gets the standard page size of 4096 bytes
pub static PAGE_SIZE: usize = 2 << 11;

pub type ReadResult<T> = Result<T, ReadDataError>;
pub type WriteResult<T> = Result<T, WriteDataError>;

#[derive(Debug, Error)]
pub enum ReadDataError {
    #[error("Page {0} was not found")]
    PageNotFound(PageId),
    #[error("No enough space to read data")]
    NotEnoughSpace,
    #[error("Could not read all the data required for this cell because EOF reach unexpectedly")]
    UnexpectedEof,
    #[error("Magic number did not exist")]
    BadMagicNumber,
    #[error("Unknown type discriminant: {0}")]
    UnknownTypeDiscriminant(u8),
    #[error("Need a type in order to continue deserialization")]
    NoTypeGiven,
    #[error(transparent)]
    FromUtf8Error(#[from] FromUtf8Error),
    #[error("Page {0} already locked")]
    PageLocked(PageId),
    #[error("{:?}: {:x?}", e.code, e.input)]
    NomError { e: nom::error::Error<Box<[u8]>> },
}

impl From<nom::error::Error<&[u8]>> for ReadDataError {
    fn from(value: Error<&[u8]>) -> Self {
        let e = nom::error::Error {
            input: Box::from(value.input),
            code: value.code,
        };
        Self::NomError { e }
    }
}

#[derive(Debug, Error)]
pub enum WriteDataError {
    #[error("Not enough space to store data for this cell on page")]
    InsufficientSpace,
    #[error("Failed to allocate {size} bytes on page {page_id}")]
    AllocationFailed { page_id: u32, size: usize },
}

pub trait StorageBackedData {
    type Owned: Borrow<Self>;

    /// Try to read a keycell
    fn read(buf: &[u8]) -> ReadResult<Self::Owned>;
    /// Write a key cell, returns the number of bytes written if successful

    fn write(&self, buf: &mut [u8]) -> WriteResult<usize>;
}

macro_rules! integer {
    ($int:ty) => {
        impl StorageBackedData for $int {
            type Owned = Self;

            fn read(buf: &[u8]) -> ReadResult<Self> {
                const SIZE: usize = std::mem::size_of::<$int>();
                let mut int_buf = [0u8; SIZE];
                int_buf.clone_from_slice(buf.get(..SIZE).ok_or(ReadDataError::UnexpectedEof)?);
                Ok(<$int>::from_be_bytes(int_buf))
            }

            fn write(&self, mut buf: &mut [u8]) -> WriteResult<usize> {
                use std::io::Write;
                buf.write_all(&self.to_be_bytes())
                    .map_err(|_e| WriteDataError::InsufficientSpace)?;
                Ok(std::mem::size_of::<$int>())
            }
        }
    };
}

integer!(u8);
integer!(u16);
integer!(u32);
integer!(u64);
integer!(i8);
integer!(i16);
integer!(i32);
integer!(i64);

macro_rules! optional_integer {
    ($int:ty) => {
        impl StorageBackedData for Option<$int> {
            type Owned = Self;

            fn read(buf: &[u8]) -> ReadResult<Self> {
                let r: $int = <$int as StorageBackedData>::read(buf)?;
                if r == 0 {
                    Ok(None)
                } else {
                    Ok(Some(r))
                }
            }

            fn write(&self, buf: &mut [u8]) -> WriteResult<usize> {
                self.unwrap_or(0).write(buf)
            }
        }
    };
}

optional_integer!(u8);
optional_integer!(u16);
optional_integer!(u32);
optional_integer!(u64);
optional_integer!(i8);
optional_integer!(i16);
optional_integer!(i32);
optional_integer!(i64);

impl StorageBackedData for [u8] {
    type Owned = Box<[u8]>;

    fn read(buf: &[u8]) -> ReadResult<Self::Owned> {
        let len = u64::read(buf)?;
        buf.get(8..)
            .ok_or(ReadDataError::UnexpectedEof)?
            .get(..len as usize)
            .ok_or(ReadDataError::UnexpectedEof)
            .map(Box::from)
    }

    fn write(&self, mut buf: &mut [u8]) -> WriteResult<usize> {
        let len_a = (self.len() as u64).write(buf)?;
        buf.write_all(self)
            .map_err(|_| WriteDataError::InsufficientSpace)?;
        Ok(len_a + self.len())
    }
}

impl StorageBackedData for str {
    type Owned = String;

    fn read(buf: &[u8]) -> ReadResult<String> {
        let read: Box<[u8]> = <[u8]>::read(buf)?;
        let bytes = String::from_utf8_lossy(&read);
        Ok(bytes.to_string())
    }

    fn write(&self, buf: &mut [u8]) -> WriteResult<usize> {
        self.as_bytes().write(buf)
    }
}

/// A storage file delagate wraps an arbitrary storage file implementation
#[derive(Debug)]
pub struct StorageDeviceDelegate {
    delegate: Box<dyn StorageDevice + Send + Sync>,
}

impl StorageDeviceDelegate {
    /// Creates a new storage file delegate from the given delegate
    fn new(delegate: impl StorageDevice + Send + Sync + 'static) -> Self {
        Self {
            delegate: Box::new(delegate),
        }
    }
}

impl Monitorable for StorageDeviceDelegate {
    fn monitor(&self) -> Box<dyn Monitor> {
        self.delegate.monitor()
    }
}
