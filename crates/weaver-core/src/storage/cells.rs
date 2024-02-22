//! Cells are parts of slotted pages, and are used to store data that we know can be variable in size
//! and shape.

use std::fmt::{Debug, Display, Formatter};
use std::io::Write;
use std::mem::size_of;

use crate::common::pretty_bytes::PrettyBytes;
use bitfield::bitfield;
use derive_more::{Display, From};
use std::num::NonZeroU32;

use crate::data::row::OwnedRow;
use crate::data::serde::{deserialize_data_typed, serialize_data_typed, serialize_data_untyped};
use crate::key::{KeyData, KeyDataRange};
use crate::storage::{ReadDataError, ReadResult, StorageBackedData, WriteDataError, WriteResult};

/// A cell can either just store a key, or a key-value
#[derive(Debug, PartialEq, Eq, Clone, From)]
pub enum Cell {
    Key(KeyCell),
    KeyValue(KeyValueCell),
}

impl Cell {
    pub fn len(&self) -> usize {
        match self {
            Cell::Key(k) => k.len(),
            Cell::KeyValue(kv) => kv.len(),
        }
    }

    /// Gets the key of this cell. Always present
    pub fn key_data(&self) -> KeyData {
        match self {
            Cell::Key(k) => k.key_data(),
            Cell::KeyValue(kv) => kv.key_data(),
        }
    }

    /// Gets this cell as a key cell
    pub fn as_key_cell(&self) -> Option<&KeyCell> {
        if let Cell::Key(key) = &self {
            Some(key)
        } else {
            None
        }
    }

    /// Gets this cell as a key value cell
    pub fn as_key_value_cell(&self) -> Option<&KeyValueCell> {
        if let Cell::KeyValue(kv) = &self {
            Some(kv)
        } else {
            None
        }
    }

    /// converts this cell as a key cell
    pub fn into_key_cell(self) -> Option<KeyCell> {
        if let Cell::Key(key) = self {
            Some(key)
        } else {
            None
        }
    }

    /// converts this cell as a key cell
    pub fn into_key_value_cell(self) -> Option<KeyValueCell> {
        if let Cell::KeyValue(key) = self {
            Some(key)
        } else {
            None
        }
    }
}

#[derive(PartialEq, Eq, Clone)]
pub struct KeyCell {
    /// The size of the key
    key_size: u32,
    /// The id of the child page this cell is pointing to
    page_id: u32,
    /// The key bytes
    bytes: Box<[u8]>,
}

impl Debug for KeyCell {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyCell")
            .field("page_id", &self.page_id)
            .field("bytes", &PrettyBytes(&self.bytes))
            .finish()
    }
}

impl Display for KeyCell {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} -> {}", self.key_data(), self.page_id)
    }
}

impl KeyCell {
    /// Create a key cell from a key index
    pub fn new(page_id: u32, key: KeyData) -> Self {
        let mut buffer: Vec<u8> = serialize_data_typed(&key);
        Self {
            key_size: buffer.len() as u32,
            page_id,
            bytes: buffer.into_boxed_slice(),
        }
    }

    pub fn key_data(&self) -> KeyData {
        KeyData::from(
            deserialize_data_typed(&self.bytes).expect("should be unfallible unless corrupted"),
        )
    }

    pub fn len(&self) -> usize {
        2 * size_of::<u32>() + self.bytes.len()
    }

    /// Gets the id of the page this cell points to.
    pub fn page_id(&self) -> PageId {
        PageId::new(self.page_id.try_into().expect("must always be > 0"))
    }
}

impl StorageBackedData for KeyCell {
    type Owned = Self;
    /// Try to read a keycell
    fn read(buf: &[u8]) -> ReadResult<Self> {
        let mut u32_buf = [0u8; 4];
        u32_buf.clone_from_slice(buf.get(..4).ok_or(ReadDataError::UnexpectedEof)?);
        let key_size = u32::from_be_bytes(u32_buf);
        let mut u32_buf = [0u8; 4];
        u32_buf.clone_from_slice(buf.get(4..8).ok_or(ReadDataError::UnexpectedEof)?);
        let page_id = u32::from_be_bytes(u32_buf);
        let key_data = buf
            .get(8..)
            .ok_or(ReadDataError::UnexpectedEof)?
            .get(..key_size as usize)
            .ok_or(ReadDataError::UnexpectedEof)?;
        Ok(KeyCell {
            key_size,
            page_id,
            bytes: Vec::from(key_data).into_boxed_slice(),
        })
    }
    /// Write a key cell, returns the number of bytes written if successful

    fn write(&self, mut buf: &mut [u8]) -> WriteResult<usize> {
        buf.write_all(&self.key_size.to_be_bytes())
            .map_err(|e| WriteDataError::InsufficientSpace)?;
        buf.write_all(&self.page_id.to_be_bytes())
            .map_err(|e| WriteDataError::InsufficientSpace)?;
        buf.write_all(&self.bytes)
            .map_err(|e| WriteDataError::InsufficientSpace)?;
        Ok(size_of::<u32>() * 2 + self.bytes.len())
    }
}

#[derive(PartialEq, Eq, Clone)]
pub struct KeyValueCell {
    flags: Flags,
    key_size: u32,
    value_size: u32,
    key: Box<[u8]>,
    data_record: Box<[u8]>,
}

impl Debug for KeyValueCell {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyValueCell")
            .field("flags", &self.flags)
            .field("key", &PrettyBytes(&self.key))
            .field("record", &PrettyBytes(&self.data_record))
            .finish()
    }
}

impl KeyValueCell {
    /// Creates a new key value cell from an owned row
    pub fn new(key: KeyData, record: OwnedRow) -> Self {
        let key_data = serialize_data_typed(&key);
        let record_data = serialize_data_untyped(&record);
        Self {
            flags: Flags(0),
            key_size: key_data.len() as u32,
            value_size: record_data.len() as u32,
            key: key_data.into_boxed_slice(),
            data_record: record_data.into_boxed_slice(),
        }
    }

    pub fn flags_mut(&mut self) -> &mut Flags {
        &mut self.flags
    }

    pub fn flags(&self) -> &Flags {
        &self.flags
    }

    pub fn len(&self) -> usize {
        size_of::<Flags>() + 2 * size_of::<u32>() + self.key.len() + self.data_record.len()
    }

    pub fn key_data(&self) -> KeyData {
        KeyData::from(deserialize_data_typed(&self.key).expect("should be valid"))
    }

    /// Returns the raw record.
    ///
    /// Records are stored untyped, and therefore we do not know what it's actually made of.
    pub fn record(&self) -> &[u8] {
        self.data_record.as_ref()
    }
}

bitfield! {
    #[derive(Copy, Clone, Eq, PartialEq)]
    #[repr(transparent)]
    pub struct Flags(u8);
    impl Debug;
}

impl StorageBackedData for KeyValueCell {
    type Owned = Self;
    fn read(buf: &[u8]) -> ReadResult<Self> {
        let flags = Flags(*buf.get(0).ok_or(ReadDataError::UnexpectedEof)?);
        let mut u32_buf = [0u8; 4];
        u32_buf.clone_from_slice(buf.get(1..5).ok_or(ReadDataError::UnexpectedEof)?);
        let key_size = u32::from_be_bytes(u32_buf);
        u32_buf.clone_from_slice(buf.get(5..9).ok_or(ReadDataError::UnexpectedEof)?);
        let value_size = u32::from_be_bytes(u32_buf);
        let key = buf
            .get(9..)
            .ok_or(ReadDataError::UnexpectedEof)?
            .get(..key_size as usize)
            .ok_or(ReadDataError::UnexpectedEof)?;
        let data_record = buf
            .get((9 + key_size as usize)..)
            .ok_or(ReadDataError::UnexpectedEof)?
            .get(..value_size as usize)
            .ok_or(ReadDataError::UnexpectedEof)?;
        Ok(KeyValueCell {
            flags,
            key_size,
            value_size,
            key: Vec::from(key).into_boxed_slice(),
            data_record: Vec::from(data_record).into_boxed_slice(),
        })
    }

    fn write(&self, mut buf: &mut [u8]) -> WriteResult<usize> {
        buf.write_all(&[self.flags.0])
            .map_err(|e| WriteDataError::InsufficientSpace)?;
        buf.write_all(&self.key_size.to_be_bytes())
            .map_err(|e| WriteDataError::InsufficientSpace)?;
        buf.write_all(&self.value_size.to_be_bytes())
            .map_err(|e| WriteDataError::InsufficientSpace)?;
        buf.write_all(&self.key)
            .map_err(|e| WriteDataError::InsufficientSpace)?;
        buf.write_all(&self.data_record)
            .map_err(|e| WriteDataError::InsufficientSpace)?;
        Ok(1 + size_of::<u32>() * 2 + self.key_size as usize + self.value_size as usize)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::cells::{Flags, KeyCell, KeyValueCell};
    use crate::storage::StorageBackedData;

    #[test]
    fn read_write_key_cell() {
        let key_cell = KeyCell {
            key_size: 4,
            page_id: 55,
            bytes: Box::new([1, 2, 3, 4]),
        };
        let mut buffer = vec![0_u8; 12];
        key_cell
            .write(&mut buffer)
            .expect("could not write key cell");
        println!("buffer: {:?}", buffer);
        assert!(buffer.iter().any(|b| *b > 0));
        let read_key_cell = KeyCell::read(&buffer).expect("could not read key cell");
        assert_eq!(read_key_cell, key_cell);
    }

    #[test]
    fn read_write_key_value_cell() {
        let key_cell = KeyValueCell {
            flags: Flags(55),
            key_size: 4,
            value_size: 6,
            key: Box::new([1, 2, 3, 4]),
            data_record: Box::new([1, 2, 3, 4, 5, 6]),
        };
        let mut buffer = vec![0_u8; 32];
        let len = key_cell
            .write(&mut buffer)
            .expect("could not write key cell");
        println!("buffer: {:?}", &buffer[..len]);
        assert!(buffer.iter().any(|b| *b > 0));
        let read_key_cell = KeyValueCell::read(&buffer).expect("could not read key cell");
        assert_eq!(read_key_cell, key_cell);
    }
}

/// A page id
#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Display)]
pub struct PageId(u32);

impl PageId {
    pub fn new(id: NonZeroU32) -> Self {
        Self(id.get())
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

impl StorageBackedData for PageId {
    type Owned = Self;
    fn read(buf: &[u8]) -> ReadResult<Self> {
        let inner: u32 = u32::read(buf)?;
        Ok(Self(inner))
    }

    fn write(&self, buf: &mut [u8]) -> WriteResult<usize> {
        self.0.write(buf)
    }
}
