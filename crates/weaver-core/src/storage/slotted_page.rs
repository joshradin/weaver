//! # Slotted Page
//!
//! Slotted pages are a mechanism for storing data on disk that can
//! - Store variable-size records with a minimal overhead
//! - Reclaim space occupied by the removal of records
//! - Reference records in the page without regard to their exact location

use derive_more::{Deref, DerefMut};
use std::collections::{BTreeMap, Bound};
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::mem::{size_of, size_of_val};
use std::num::NonZeroU32;
use std::os::unix::raw::off_t;
use std::path::Path;
use std::sync::Arc;

use digest::typenum::NonZero;
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::trace;

use crate::error::Error;
use crate::key::{KeyData, KeyDataRange};
use crate::storage::cells::{Cell, KeyCell, KeyValueCell};
use crate::storage::{ReadDataError, ReadResult, StorageBackedData, WriteDataError, WriteResult};
use crate::storage::ram_file::RandomAccessFile;

/// PAGE size is 16Kb
pub const PAGE_SIZE: usize = 1024 * 16;
const HEADER_SIZE: usize = size_of::<Header>();

/// A slotted page contains data
pub struct SlottedPage {
    file: Arc<RwLock<RandomAccessFile>>,
    index: usize,
    header: Header,
    slots: BTreeMap<KeyData, u64>,
    slots_end: u64,
    cells_start: u64,
    /// The free list is a mapping between size -> offset
    free_list: BTreeMap<usize, Vec<u64>>,
}

impl SlottedPage {
    /// Opens a page at a given path
    pub fn open_path<P: AsRef<Path>>(path: P, page_index: usize) -> Result<SlottedPage, Error> {
        let file = RandomAccessFile::with_file(File::open(path)?, false)?;
        Self::open(file, page_index)
    }

    pub fn open(file: RandomAccessFile, page_index: usize) -> Result<SlottedPage, Error> {
        Self::open_shared(&Arc::new(RwLock::new(file)), page_index)
    }

    /// Opens a file, returning a vector of slotted pages
    pub fn open_vector(file: &Arc<RwLock<RandomAccessFile>>) -> Result<Vec<SlottedPage>, Error> {
        let arc = file.clone();
        let lock = arc.read();
        let pages = (lock.len() / PAGE_SIZE as u64) as usize;
        (0..pages)
            .into_iter()
            .map(|index| Self::open_shared(&file, index))
            .collect()
    }

    /// Opens a page in a file at given offset
    ///
    /// The page offset is an array
    pub fn open_shared(
        file: &Arc<RwLock<RandomAccessFile>>,
        page_index: usize,
    ) -> Result<SlottedPage, Error> {
        let file = file.clone();
        if file.read().metadata()?.len() < (page_index as u64 + 1) * (PAGE_SIZE as u64) {
            return Err(Error::ReadDataError(ReadDataError::UnexpectedEof));
        }

        let file_start = page_index as u64 * (PAGE_SIZE as u64);

        let header = Header::read(&file.read().read_exact(file_start, HEADER_SIZE as u64)?)?;
        let cells = header.len as u64;
        let slots_ptr = file_start + HEADER_SIZE as u64;
        let mut ptrs = vec![];
        for i in 0..cells {
            let read = file.read().read_exact(
                slots_ptr + i * size_of::<u64>() as u64,
                size_of::<u64>() as u64,
            )?;
            let as_offset = u64::from_be_bytes(read.try_into().unwrap());
            ptrs.push(as_offset);
        }

        let mut cells = vec![];
        let mut slots = BTreeMap::new();
        let cells_start = ptrs.iter().copied().min().unwrap_or(1);
        for offset in ptrs {
            let cell = Self::get_cell_at(&mut file.read(), offset, &header.page_type)?;
            let key_data = cell.key_data();
            slots.insert(key_data, offset);
            cells.push(cell);
        }

        let slots_end = slots_ptr + (cells.len() * size_of::<u64>()) as u64;
        debug_assert!(
            slots_end < cells_start,
            "slots end must be less than cells start"
        );

        Ok(Self {
            file,
            index: page_index,
            header,
            slots,
            slots_end,
            cells_start,
            free_list: Default::default(),
        })
    }

    pub fn create<P: AsRef<Path>>(
        path: P,
        page_id: NonZeroU32,
        page_type: PageType,
        page_index: usize,
    ) -> Result<SlottedPage, Error> {
        let real_file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let file: RandomAccessFile = real_file.try_into()?;
        Self::init(file, page_id, page_type, page_index)
    }

    pub fn init(
        random_access_file: RandomAccessFile,
        page_id: NonZeroU32,
        page_type: PageType,
        page_index: usize,
    ) -> Result<SlottedPage, Error> {
        let mutex = Arc::new(RwLock::new(random_access_file));
        Self::init_shared(&mutex, page_id, page_type, page_index)
    }

    pub fn init_shared(
        random_access_file: &Arc<RwLock<RandomAccessFile>>,
        page_id: NonZeroU32,
        page_type: PageType,
        page_index: usize,
    ) -> Result<SlottedPage, Error> {
        let mut real_file = random_access_file.write();
        if real_file.metadata()?.len() < (page_index as u64 + 1) * PAGE_SIZE as u64 {
            real_file.set_len(PAGE_SIZE as u64 * (page_index as u64 + 1))?;
        }

        let header = Header {
            magic_number: HEADER_MAGIC_NUMBER,
            page_id: page_id.get(),
            left_page_id: None,
            right_page_id: None,
            page_type,
            len: 0,
        };

        let mut page = Self {
            file: random_access_file.clone(),
            index: page_index,
            header,
            slots: Default::default(),
            slots_end: (page_index * PAGE_SIZE) as u64 + Header::len() as u64,
            cells_start: ((page_index + 1) * PAGE_SIZE) as u64,
            free_list: Default::default(),
        };
        page._flush(&mut real_file)?;
        Ok(page)
    }

    /// Pushes a new slotted page at the end of the random access file
    pub fn init_last_shared(
        random_access_file: &Arc<RwLock<RandomAccessFile>>,
        page_id: NonZeroU32,
        page_type: PageType,
    ) -> Result<SlottedPage, Error> {
        let len = random_access_file.read().metadata()?.len();
        let pages = len as usize / PAGE_SIZE;
        Self::init_shared(random_access_file, page_id, page_type, pages)
    }
    pub fn page_id(&self) -> u32 {
        self.header.page_id
    }

    pub fn set_right_sibling(&mut self, id: Option<&SlottedPage>) {
        self.header.right_page_id = id.map(|page| page.page_id());
    }

    pub fn right_sibling_id(&self) -> Option<u32> {
        self.header.right_page_id
    }

    pub fn set_left_sibling(&mut self, id: Option<&SlottedPage>) {
        self.header.left_page_id = id.map(|page| page.page_id());
    }

    pub fn left_sibling_id(&self) -> Option<u32> {
        self.header.right_page_id
    }

    /// The page type
    pub fn page_type(&self) -> PageType {
        self.header.page_type
    }

    pub fn free_space(&self) -> u64 {
        (self.cells_start - self.slots_end)
            + self
                .free_list
                .iter()
                .map(|(&size, offsets)| (size * offsets.len()) as u64)
                .sum::<u64>()
            + size_of::<Header>() as u64
    }

    fn cell_length_at(&self, offset: u64) -> Result<u64, Error> {
        Self::_cell_length_at(&*self.file.read(), offset, &self.header.page_type)
    }

    fn _cell_length_at(ram: &RandomAccessFile, offset: u64, ty: &PageType) -> Result<u64, Error> {
        match ty {
            PageType::Key => {
                let buffer = ram.read_exact(offset, 4)?;
                let len = 2 * size_of::<u32>() as u64
                    + u32::from_be_bytes(buffer.try_into().unwrap()) as u64;
                Ok(len)
            }
            PageType::KeyValue => {
                let buffer = ram.read_exact(offset + 1, 8)?;
                let (key_size, value_size) = buffer.split_at(size_of::<u32>());
                let key_size = u32::from_be_bytes(key_size.try_into().unwrap()) as u64;
                let value_size = u32::from_be_bytes(value_size.try_into().unwrap()) as u64;
                Ok(1 + 2 * size_of::<u32>() as u64 + key_size + value_size)
            }
        }
    }

    fn get_cell_at(ram: &RandomAccessFile, offset: u64, ty: &PageType) -> Result<Cell, Error> {
        match ty {
            PageType::Key => {
                let len = Self::_cell_length_at(ram, offset, ty)?;
                Ok(Cell::Key(KeyCell::read(&ram.read_exact(offset, len)?)?))
            }
            PageType::KeyValue => {
                let len = Self::_cell_length_at(ram, offset, ty)?;
                Ok(Cell::KeyValue(KeyValueCell::read(
                    &ram.read_exact(offset, len)?,
                )?))
            }
        }
    }

    /// Gets a cell with the given key data
    pub fn get_cell(&self, key_data: &KeyData) -> Option<Cell> {
        self.slots.get(key_data).and_then(|&offset| {
            Self::get_cell_at(&*self.file.read(), offset, &self.header.page_type).ok()
        })
    }

    /// Gets all value cells in a given range
    pub fn range(&self, range: &KeyDataRange) -> Result<Vec<Cell>, Error> {
        Ok(self
            .slots
            .iter()
            .filter(|key| range.contains(&*key.0))
            .flat_map(|(key, _)| self.get_cell(key))
            .collect())
    }

    /// Gets the max key value. This should always be the maximum key value and its associated cell
    pub fn last_key_value(&self) -> Option<(KeyData, Cell)> {
        self.slots.last_key_value()
            .and_then(|(key, &offset)|
                self.get_cell(key)
                    .map(|cell| (key.clone(), cell))
            )
    }

    /// Checks if adding new entry with a given size is feasible
    fn can_alloc(&self, len: usize) -> bool {
        trace!(
            "checking if can fit len {len} when cell_start: {} and slot_end: {}",
            self.cells_start,
            self.slots_end
        );
        let new_cells_start = self.cells_start.checked_sub(len as u64);
        let new_slot_end = self.slots_end.checked_add(len as u64);
        new_slot_end
            .zip(new_cells_start)
            .map(|(new_slots_end, new_cells_start)| new_slots_end <= new_cells_start)
            .unwrap_or(false)
    }

    fn alloc(&mut self, len: usize) -> Result<(u64, u64), Error> {
        if !self.can_alloc(len) {
            return Err(Error::WriteDataError(WriteDataError::AllocationFailed {
                page_id: self.page_id(),
                size: len,
            }));
        }
        let slot_offset = self.slots_end;
        self.slots_end += 8;
        self.cells_start -= len as u64;
        Ok((slot_offset, self.cells_start))
    }

    fn free(&mut self, offset: u64) -> Result<(), Error> {
        let cell_length = self.cell_length_at(offset)?;
        if offset == self.cells_start {
            // expand
            self.cells_start += cell_length;
        } else {
            self.free_list
                .entry(cell_length as usize)
                .or_default()
                .push(offset);
        }

        let arc_clone = self.file.clone();
        let mut lock = arc_clone.write();
        let mut slots = self.raw_slots(&mut *lock)?;
        slots.retain(|&slot| slot != offset);
        self.write_raw_slots(slots, &mut *lock)?;
        Ok(())
    }

    /// Tries to insert,
    pub fn insert<T: Into<Cell>>(&mut self, cell: T) -> Result<(), Error> {
        let cell = cell.into();
        self.assert_cell_type(&cell)?;
        let key_data = cell.key_data();
        let cell_len = cell.len();
        let (slot_offset, cell_offset) = self.alloc(cell_len)?;

        let mut data = vec![0_u8; cell_len];
        match cell {
            Cell::Key(key) => {
                key.write(&mut data)?;
            }
            Cell::KeyValue(key_value) => {
                key_value.write(&mut data)?;
            }
        }
        let arc = self.file.clone();
        let mut lock = arc.write();

        // inserts the cell at the start of the cell block
        // and inserts the pointer at the end of the block

        lock.write(cell_offset, &data)?;
        lock.write(slot_offset, &u64::to_be_bytes(cell_offset))?;
        self.header.len += 1;
        self.slots.insert(key_data, cell_offset);
        self.write_slots(&mut *lock)?;
        self._flush(&mut lock)?;
        Ok(())
    }

    /// Delete a cell by the given key, if present
    pub fn delete(&mut self, key: &KeyData) -> Result<Option<Cell>, Error> {
        match self.slots.get(&key) {
            None => Ok(None),
            Some(&offset) => {
                let cell = self.get_cell(key).unwrap();
                self.free(offset)?;

                Ok(Some(cell))
            }
        }
    }

    fn write_slots(&self, lock: &mut RandomAccessFile) -> Result<(), Error> {
        let mut offset = Header::len() as u64 + (self.index * PAGE_SIZE) as u64;
        for value in self.slots.values() {
            lock.write(offset, &value.to_be_bytes())?;
            offset += 8;
        }
        Ok(())
    }

    fn raw_slots(&self, lock: &mut RandomAccessFile) -> Result<Vec<u64>, Error> {
        let mut offset = Header::len() as u64 + (self.index * PAGE_SIZE) as u64;
        let slots_in_use = self.len();
        let mut slots = vec![];
        for i in 0..slots_in_use {
            let read = lock.read_exact(
                offset + (i * size_of::<u64>()) as u64,
                size_of::<u64>() as u64,
            )?;
            let read_u64 = u64::from_be_bytes(read.try_into().unwrap());
            slots.push(read_u64);
        }
        Ok(slots)
    }

    fn write_raw_slots(&mut self, vec: Vec<u64>, lock: &mut RandomAccessFile) -> Result<(), Error> {
        let mut offset = Header::len() as u64 + (self.index * PAGE_SIZE) as u64;
        for &value in &vec {
            lock.write(offset, &value.to_be_bytes())?;
            offset += 8;
        }
        if offset != self.slots_end {
            self.slots_end = offset;
            self.header.len = vec.len() as u32;
            self.slots
                .iter()
                .filter(|(_, slot_offset)| !vec.contains(slot_offset))
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>()
                .into_iter()
                .for_each(|key| {
                    self.slots.remove(&key);
                });
        }
        Ok(())
    }

    fn assert_cell_type(&mut self, cell: &Cell) -> Result<(), Error> {
        match (cell, self.page_type()) {
            (Cell::Key(_), PageType::KeyValue) => {
                return Err(Error::CellTypeMismatch {
                    expected: PageType::KeyValue,
                    actual: PageType::Key,
                });
            }
            (Cell::KeyValue(_), PageType::Key) => {
                return Err(Error::CellTypeMismatch {
                    expected: PageType::Key,
                    actual: PageType::KeyValue,
                });
            }
            _ => Ok(()),
        }
    }

    /// Gets the number of cells stored in this header
    pub fn len(&self) -> usize {
        self.header.len as usize
    }

    pub fn flush(&mut self) -> Result<(), Error> {
        let ar = self.file.clone();
        let mut lock = ar.write();
        self._flush(&mut lock)?;
        Ok(())
    }

    fn _flush(&mut self, lock: &mut RwLockWriteGuard<RandomAccessFile>) -> Result<(), Error> {
        // write header
        let mut header_buffer = vec![0u8; size_of_val(&self.header)];
        self.header.write(&mut header_buffer)?;
        lock.write(self.index as u64 * PAGE_SIZE as u64, &header_buffer)?;

        Ok(())
    }

    fn bounds(&self) -> KeyDataRange {
        self.keys().fold(
            KeyDataRange(Bound::Unbounded, Bound::Unbounded),
            |mut accum, next| {
                match &accum.0 {
                    Bound::Included(acc) | Bound::Excluded(acc) => {
                        if next <= acc {
                            accum.0 = Bound::Included(next.clone());
                        }
                    }
                    Bound::Unbounded => {
                        accum.0 = Bound::Included(next.clone());
                    }
                }
                match &accum.1 {
                    Bound::Included(acc) | Bound::Excluded(acc) => {
                        if next >= acc {
                            accum.1 = Bound::Included(next.clone());
                        }
                    }
                    Bound::Unbounded => {
                        accum.1 = Bound::Included(next.clone());
                    }
                }
                accum
            },
        )
    }

    fn keys(&self) -> impl Iterator<Item = &KeyData> {
        self.slots.keys()
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn key_data_range(&self) -> KeyDataRange {
        KeyDataRange(
            self.slots.first_key_value()
                .map(|(k, v)| Bound::Included(k.clone()))
                .unwrap_or(Bound::Unbounded),
            self.slots.last_key_value()
                .map(|(k, v)| Bound::Included(k.clone()))
                .unwrap_or(Bound::Unbounded)
        )
    }
}

impl Debug for SlottedPage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlottedPage")
            .field("index", &self.index)
            .field("header", &self.header)
            .field("slots", &self.slots)
            .field("space_used", &(PAGE_SIZE as u64 - self.free_space()))
            .field("free_space", &self.free_space())
            .finish()
    }
}

impl Drop for SlottedPage {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

const HEADER_MAGIC_NUMBER: u64 =
    u64::from_be_bytes([b'W', b'E', b'A', b'V', b'E', b'R', b'D', b'B']);

#[derive(Debug)]
struct Header {
    magic_number: u64,
    page_id: u32,
    left_page_id: Option<u32>,
    right_page_id: Option<u32>,
    page_type: PageType,

    len: u32,
}

impl Header {
    const fn len() -> usize {
        size_of::<Header>()
    }
}

impl<'a> StorageBackedData<'a> for Header {
    fn read(buf: &'a [u8]) -> ReadResult<Self> {
        let magic = u64::read(buf)?;
        if magic != HEADER_MAGIC_NUMBER {
            return Err(ReadDataError::BadMagicNumber);
        }
        const U32_SIZE: usize = size_of::<u32>();
        let buf = &buf[8..];
        let page_id = u32::read(buf)?;
        let buf = buf.get(U32_SIZE..).ok_or(ReadDataError::UnexpectedEof)?;
        let left_page_id = <Option<u32>>::read(buf)?;
        let buf = buf.get(U32_SIZE..).ok_or(ReadDataError::UnexpectedEof)?;
        let right_page_id = <Option<u32>>::read(buf)?;
        let buf = buf.get(U32_SIZE..).ok_or(ReadDataError::UnexpectedEof)?;
        let page_type = PageType::read(buf)?;
        let buf = buf.get(1..).ok_or(ReadDataError::UnexpectedEof)?;
        let len = u32::read(buf)?;

        Ok(Header {
            magic_number: magic,
            page_id,
            left_page_id,
            right_page_id,
            page_type,
            len,
        })
    }

    fn write(&'a self, mut buf: &mut [u8]) -> WriteResult<usize> {
        let len = self.magic_number.write(buf)?;
        buf = &mut buf[len..];
        let len = self.page_id.write(buf)?;
        buf = &mut buf[len..];
        let len = self.left_page_id.write(buf)?;
        buf = &mut buf[len..];
        let len = self.right_page_id.write(buf)?;
        buf = &mut buf[len..];
        let len = self.page_type.write(buf)?;
        buf = &mut buf[len..];
        let len = self.len.write(buf)?;
        buf = &mut buf[len..];

        Ok(size_of::<Header>())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u8)]
pub enum PageType {
    Key = 1,
    KeyValue = 2,
}

impl<'a> StorageBackedData<'a> for PageType {
    fn read(buf: &'a [u8]) -> ReadResult<Self> {
        match buf.get(0) {
            Some(1) => Ok(PageType::Key),
            Some(2) => Ok(PageType::KeyValue),
            Some(_) => Err(ReadDataError::BadMagicNumber),
            None => Err(ReadDataError::UnexpectedEof),
        }
    }

    fn write(&'a self, buf: &mut [u8]) -> WriteResult<usize> {
        let b = *self as u8;
        b.write(buf)
    }
}

/// Invariant over Key slotted pages
#[derive(Debug, Deref, DerefMut)]
pub struct KeySlottedPage(SlottedPage);

impl TryFrom<SlottedPage> for KeySlottedPage {
    type Error = Error;

    fn try_from(value: SlottedPage) -> Result<Self, Self::Error> {
        if value.page_type() != PageType::Key {
            Err(Error::CellTypeMismatch {
                expected: PageType::Key,
                actual: PageType::KeyValue,
            })
        } else {
            Ok(KeySlottedPage(value))
        }
    }
}

/// Invariant over Key Value slotted pages
#[derive(Debug, Deref, DerefMut)]
pub struct KeyValueSlottedPage(SlottedPage);

impl TryFrom<SlottedPage> for KeyValueSlottedPage {
    type Error = Error;

    fn try_from(value: SlottedPage) -> Result<Self, Self::Error> {
        if value.page_type() != PageType::KeyValue {
            Err(Error::CellTypeMismatch {
                expected: PageType::KeyValue,
                actual: PageType::Key,
            })
        } else {
            Ok(KeyValueSlottedPage(value))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use crate::data::row::Row;
    use crate::error::Error;
    use crate::key::KeyData;
    use crate::storage::cells::KeyValueCell;
    use crate::storage::slotted_page::{PageType, SlottedPage, PAGE_SIZE};
    use crate::storage::ReadDataError;

    #[test]
    fn can_not_read_uninit_page() {
        let file = tempfile::tempfile().unwrap();
        file.set_len(PAGE_SIZE as u64).unwrap();
        let error = SlottedPage::open(file.try_into().unwrap(), 0).expect_err("should be error");
        assert!(
            matches!(error, Error::ReadDataError(ReadDataError::BadMagicNumber)),
            "error was {:?}",
            error
        );
    }

    #[test]
    fn can_init_page() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("temp.idb");
        let mut page =
            SlottedPage::create(&file, NonZeroU32::new(1).unwrap(), PageType::KeyValue, 0)
                .expect("should create file");
        page.insert(KeyValueCell::new(
            KeyData::from(Row::from([1.into()])),
            Row::from([1.into(), 2.into()]).to_owned(),
        ))
        .unwrap();
        drop(page);
        let page = SlottedPage::open_path(file, 0).unwrap();
        assert_eq!(page.header.page_id, 1);
        assert_eq!(page.len(), 1);
    }

    #[test]
    fn can_delete() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("temp.idb");
        let mut page =
            SlottedPage::create(&file, NonZeroU32::new(1).unwrap(), PageType::KeyValue, 0)
                .expect("should create file");
        let key_data = KeyData::from(Row::from([1.into()]));
        page.insert(KeyValueCell::new(
            key_data.clone(),
            Row::from([1.into(), 2.into()]).to_owned(),
        ))
        .unwrap();
        assert_eq!(page.len(), 1);
        page.delete(&key_data).unwrap();
        assert_eq!(page.len(), 0);
    }

    #[test]
    fn can_init_2nd_page() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("temp.idb");
        let mut page =
            SlottedPage::create(&file, NonZeroU32::new(1).unwrap(), PageType::KeyValue, 0)
                .expect("should create file");
        let cell_1_key = KeyData::from(Row::from([1.into()]));
        page.insert(KeyValueCell::new(
            cell_1_key.clone(),
            Row::from([1.into(), 2.into()]).to_owned(),
        ))
        .unwrap();
        drop(page);
        let page = SlottedPage::open_path(file.clone(), 0).unwrap();
        let mut page2 =
            SlottedPage::create(&file, NonZeroU32::new(2).unwrap(), PageType::KeyValue, 1).unwrap();
        let cell_2_key = KeyData::from(Row::from([4.into()]));
        page2
            .insert(KeyValueCell::new(
                cell_2_key.clone(),
                Row::from([5.into(), 7.into()]).to_owned(),
            ))
            .unwrap();
        assert_eq!(page2.header.page_id, 2);
        assert_eq!(page2.len(), 1);
        assert!(page.get_cell(&cell_1_key).is_some());
        assert!(page.get_cell(&cell_2_key).is_none());
        assert!(page2.get_cell(&cell_2_key).is_some());
    }

    #[test]
    fn insert_page() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("temp.idb");
        let mut page =
            SlottedPage::create(&file, NonZeroU32::new(1).unwrap(), PageType::KeyValue, 0)
                .expect("should create file");
        let key_data = KeyData::from(Row::from([]));
    }

    #[test]
    fn insert_cell() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("temp.idb");
        let mut page = SlottedPage::create(&file, NonZeroU32::new(1).unwrap(), PageType::Key, 0)
            .expect("should create file");
    }
}
