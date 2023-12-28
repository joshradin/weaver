//! Storage primitives

use std::borrow::Cow;
use std::io::Write;
use std::string::FromUtf8Error;
use thiserror::Error;
pub mod b_tree;
pub mod cells;
pub mod slotted_page;
mod abstraction;
pub mod ram_file;

pub type ReadResult<T> = Result<T, ReadDataError>;
pub type WriteResult<T> = Result<T, WriteDataError>;

#[derive(Debug, Error)]
pub enum ReadDataError {
    #[error("Could not read all the data required for this cell")]
    UnexpectedEof,
    #[error("Magic number did not exist")]
    BadMagicNumber,
    #[error("Unknown type discriminant: {0}")]
    UnknownTypeDiscriminant(u8),
    #[error("Need a type in order to continue deserialization")]
    NoTypeGiven,
    #[error(transparent)]
    FromUtf8Error(#[from] FromUtf8Error),
}

#[derive(Debug, Error)]
pub enum WriteDataError {
    #[error("Not enough space to store data for this cell on page")]
    InsufficientSpace,
    #[error("Failed to allocate {size} bytes on page {page_id}")]
    AllocationFailed { page_id: u32, size: usize },
}

pub trait StorageBackedData<'a>: Sized {
    /// Try to read a keycell
    fn read(buf: &'a [u8]) -> ReadResult<Self>;
    /// Write a key cell, returns the number of bytes written if successful

    fn write(&'a self, buf: &mut [u8]) -> WriteResult<usize>;
}

macro_rules! integer {
    ($int:ty) => {
        impl<'a> StorageBackedData<'a> for $int {
            fn read(buf: &'a [u8]) -> ReadResult<Self> {
                const size: usize = std::mem::size_of::<$int>();
                let mut int_buf = [0u8; size];
                int_buf.clone_from_slice(buf.get(..size).ok_or(ReadDataError::UnexpectedEof)?);
                Ok(<$int>::from_be_bytes(int_buf))
            }

            fn write(&'a self, mut buf: &mut [u8]) -> WriteResult<usize> {
                use std::io::Write;
                buf.write_all(&self.to_be_bytes())
                    .map_err(|e| WriteDataError::InsufficientSpace)?;
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
        #[doc("treats 0 value as None")]
        impl<'a> StorageBackedData<'a> for Option<$int> {
            fn read(buf: &'a [u8]) -> ReadResult<Self> {
                let r: $int = <$int as StorageBackedData<'a>>::read(buf)?;
                if r == 0 {
                    Ok(None)
                } else {
                    Ok(Some(r))
                }
            }

            fn write(&'a self, buf: &mut [u8]) -> WriteResult<usize> {
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

impl<'a> StorageBackedData<'a> for &'a [u8] {
    fn read(buf: &'a [u8]) -> ReadResult<Self> {
        let len = u64::read(buf)?;
        buf.get(8..)
            .ok_or(ReadDataError::UnexpectedEof)?
            .get(..len as usize)
            .ok_or(ReadDataError::UnexpectedEof)
    }

    fn write(&'a self, mut buf: &mut [u8]) -> WriteResult<usize> {
        let len_a = (self.len() as u64).write(buf)?;
        buf.write_all(self)
            .map_err(|_| WriteDataError::InsufficientSpace)?;
        Ok(len_a + self.len())
    }
}

impl<'a> StorageBackedData<'a> for Cow<'a, str> {
    fn read(buf: &'a [u8]) -> ReadResult<Self> {
        let s = String::from_utf8_lossy(<&'a [u8]>::read(buf)?);
        Ok(s)
    }

    fn write(&'a self, buf: &mut [u8]) -> WriteResult<usize> {
        self.as_bytes().write(buf)
    }
}

impl<'a> StorageBackedData<'a> for String {
    fn read(buf: &'a [u8]) -> ReadResult<Self> {
        let s = Cow::read(buf)?;
        Ok(s.to_string())
    }

    fn write(&'a self, buf: &mut [u8]) -> WriteResult<usize> {
        self.as_bytes().write(buf)
    }
}
