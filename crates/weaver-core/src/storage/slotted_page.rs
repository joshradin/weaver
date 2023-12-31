//! Second version of slotted pages, built over page abstractions

use std::collections::{BTreeMap, Bound, LinkedList, VecDeque};
use std::iter::FusedIterator;
use std::mem::{size_of, size_of_val};
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::{Mutex, RwLock};

use crate::common::track_dirty::Mad;
use crate::error::Error;
use crate::key::{KeyData, KeyDataRange};
use crate::storage::abstraction::{Page, PageWithHeader, Paged, SplitPage};
use crate::storage::cells::{Cell, KeyCell, KeyValueCell, PageId};
use crate::storage::{ReadDataError, ReadResult, StorageBackedData, WriteDataError, WriteResult};

impl StorageBackedData for Option<PageId> {
    type Owned = Self;
    fn read(buf: &[u8]) -> ReadResult<Self> {
        let inner: u32 = u32::read(buf)?;
        if inner == 0 {
            Ok(None)
        } else {
            Ok(Some(PageId::new(NonZeroU32::new(inner).unwrap())))
        }
    }

    fn write(&self, buf: &mut [u8]) -> WriteResult<usize> {
        match self {
            None => 0_u32.write(buf),
            Some(i) => i.write(buf),
        }
    }
}

struct CellPtr {
    /// ptr to the slot
    slot: usize,
    /// ptr to the cell
    cell: usize,
}

#[derive(Debug, Clone)]
struct FreeCell {
    /// The offset of the free area
    offset: usize,
    /// The length of the free area
    len: usize,
}

/// A slotted page implementation over a page
#[derive(Debug)]
pub struct SlottedPage<P: Page> {
    page: SplitPage<P, SlottedPageHeader>,
    header: Mad<SlottedPageHeader>,
    /// points to the end of the slots
    slot_ptr: usize,
    /// points to the beginning of the cells
    cell_ptr: usize,
    /// A list of free space
    free_list: LinkedList<FreeCell>,
    lock: OnceLock<Arc<AtomicBool>>,
    my_lock: bool,
}

impl<P: Page> SlottedPage<P> {
    /// Insert a cell into a slotted page. Must lock the cell
    pub fn insert(&mut self, cell: Cell) -> Result<(), Error> {
        self.assert_cell_type(&cell)?;
        self.lock()?;
        let ref key_data = cell.key_data();
        let cell_len = cell.len();
        if self.contains(key_data) {
            self.delete(key_data)?;
        }
        let Some(CellPtr {
            slot: _,
            cell: cell_ptr,
        }) = self.alloc(cell_len)
        else {
            return Err(WriteDataError::InsufficientSpace.into());
        };

        let mut data = &mut self.page.as_mut_slice()[cell_ptr..][..cell_len];

        match cell {
            Cell::Key(key) => {
                key.write(&mut data)?;
            }
            Cell::KeyValue(key_value) => {
                key_value.write(&mut data)?;
            }
        }

        self.sync_slots();

        Ok(())
    }

    /// Locks this page, granting exclusive access to it. Required when performing insertions or deletions.
    /// Repeatedly locking the page has no effect once successful. Unlocked upon dropping
    pub fn lock(&mut self) -> Result<(), Error> {
        if !self.my_lock {
            match self.lock.get() {
                None => {}
                Some(lock) => {
                    if lock
                        .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
                        .is_err()
                    {
                        return Err(Error::ReadDataError(ReadDataError::PageLocked(
                            self.page_id(),
                        )));
                    }
                    self.my_lock = true;
                }
            }
        }
        Ok(())
    }

    /// Unlocks this page if this page has exclusive ownership over the backing data.
    ///
    /// Has no effect if page is not locked
    pub fn unlock(&mut self) {
        if self.my_lock {
            match self.lock.get() {
                None => {}
                Some(lock) => {
                    let _ = lock.compare_exchange(true, false, Ordering::SeqCst, Ordering::Relaxed);
                    self.my_lock = false;
                }
            }
        }
    }

    /// Works similarly to how the binary search method works in the [`slice`][<\[_\]>::binary_search] primitive.
    ///
    /// If present, `Ok(index)` is returned, and if not present `Err(index)` is returned, where the index
    /// is where the key data could be inserted to maintain sort order.
    pub fn binary_search(&self, key_data: &KeyData) -> Result<Result<usize, usize>, Error> {
        let mut l: usize = 0;
        let mut r: usize = self.count().checked_sub(1).unwrap_or(0);

        while l <= r {
            let m = (l + r) / 2;
            let kd_search = self.get_key_data(m)?;
            if &kd_search < key_data {
                l = m + 1;
            } else if &kd_search > key_data {
                if m > 0 {
                    r = m - 1;
                } else {
                    break;
                }
            } else {
                return Ok(Ok(m));
            }
        }
        Ok(Err((l + r) / 2))
    }

    /// Checks if this page contains
    pub fn contains(&self, key_data: &KeyData) -> bool {
        match self.binary_search(key_data) {
            Ok(Ok(_)) => true,
            _ => false,
        }
    }

    /// Get a cell by key value
    pub fn get(&self, key_data: &KeyData) -> Result<Option<Cell>, Error> {
        let index = self.binary_search(key_data)?;
        match index {
            Ok(index) => self.get_cell(index).map(Some),
            Err(_) => Ok(None),
        }
    }

    /// Gets the cell at the given index, where indexes are relative to slots
    pub fn index(&self, index: usize) -> Option<Cell> {
        self.get_cell(index).ok()
    }

    /// Get cells within a range
    pub fn get_range<I: Into<KeyDataRange>>(&self, key_data: I) -> Result<Vec<Cell>, Error> {
        let range = key_data.into();
        let l = match range.start_bound() {
            Bound::Included(i) => match self.binary_search(i)? {
                Ok(ok) => ok,
                Err(err) => err,
            },
            Bound::Excluded(e) => match self.binary_search(e)? {
                Ok(ok) => ok + 1,
                Err(err) => err,
            },
            Bound::Unbounded => 0,
        };
        let r = match range.end_bound() {
            Bound::Included(i) => match self.binary_search(i)? {
                Ok(ok) => ok,
                Err(err) => err,
            },
            Bound::Excluded(e) => match self.binary_search(e)? {
                Ok(ok) => ok - 1,
                Err(err) => err,
            },
            Bound::Unbounded => self.count() - 1,
        };
        (l..=r)
            .into_iter()
            .map(|index| self.get_cell(index))
            .collect()
    }

    /// Gets all the cells within this page
    #[inline]
    pub fn all(&self) -> Result<Vec<Cell>, Error> {
        self.get_range(..)
    }

    /// Deletes the cell with a given key if present
    pub fn delete(&mut self, key_data: &KeyData) -> Result<Option<Cell>, Error> {
        if !self.contains(key_data) {
            return Ok(None);
        }
        self.lock()?;
        let slot = self.binary_search(key_data)?.unwrap();
        let cell_offset = self.get_cell_offset(slot)?;
        let slot_offset = self
            .get_slot_offset_from_cell_offset(cell_offset)?
            .expect("slot offset should exist");

        let read = self.get_cell_at_offset(cell_offset)?;
        self.free_slot(slot_offset)?;
        self.sync_slots();
        Ok(Some(read))
    }

    /// Drains the cells from this page that are within a given key data range
    pub fn drain<I: Into<KeyDataRange>>(&mut self, key_data: I) -> Result<Vec<Cell>, Error> {
        let range = key_data.into();
        let kds = self
            .get_range(range)?
            .into_iter()
            .map(|cell| cell.key_data());
        kds.map(|kd| self.delete(&kd))
            .flat_map(|res| match res {
                Ok(Some(cell)) => Some(Ok(cell)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }

    /// Gets the max key in this cell
    pub fn max_key(&self) -> Result<Option<KeyData>, Error> {
        if self.count() == 0 {
            return Ok(None);
        }
        self.get_cell(self.count() - 1)
            .map(|cell| Some(cell.key_data()))
    }

    /// Gets the min key in this cell
    pub fn min_key(&self) -> Result<Option<KeyData>, Error> {
        if self.count() == 0 {
            return Ok(None);
        }
        self.get_cell(0).map(|cell| Some(cell.key_data()))
    }

    /// allocate a given length within the slotted page
    ///
    /// If successful, returns the offset where the cell should be inserted, otherwise
    /// `None` is returned.
    ///
    /// # Error
    /// Will return `None` if and only if there isn't enough space to store both
    /// `size + sizeof::<u64>`
    fn alloc(&mut self, size: usize) -> Option<CellPtr> {
        let total_len = size + size_of::<u64>();
        let existing = self
            .free_list
            .iter()
            .enumerate()
            .filter(|(_, free_cell)| free_cell.len >= size)
            .min_by_key(|(_, free_cell)| free_cell.len)
            .map(|tuple| tuple.0);

        if self.slot_ptr + size_of::<u64>() >= self.cell_ptr {
            return None;
        }
        let cell_ptr = if let Some(existing) = existing {
            let mut tail = self.free_list.split_off(existing);
            let mut cell = tail.pop_front().expect("should contain one cell");
            let offset = cell.offset;
            if cell.len > size {
                cell.len -= size;
                cell.offset += size;
                self.free_list.push_back(cell);
            }

            self.free_list.append(&mut tail);
            offset
        } else if self.cell_ptr - self.slot_ptr >= total_len {
            self.cell_ptr -= size;
            let ptr = self.cell_ptr;
            ptr
        } else {
            return None;
        };
        let slot_ptr = self.slot_ptr;
        self.slot_ptr += size_of::<u64>();
        self.header.to_mut().size += 1;

        self.page.as_mut_slice()[slot_ptr..][..size_of::<u64>()]
            .copy_from_slice(&(cell_ptr as u64).to_be_bytes());

        Some(CellPtr {
            slot: slot_ptr,
            cell: cell_ptr,
        })
    }

    /// Frees the slot at the given offset
    fn free_slot(&mut self, slot_offset: usize) -> Result<(), Error> {
        if slot_offset >= self.slot_ptr {
            return Err(Error::WriteDataError(WriteDataError::InsufficientSpace));
        }
        let cell_ptr = self.read_ptr(slot_offset)?;
        let cell_len = self.get_cell_at_offset(cell_ptr)?.len();
        self.page.as_mut_slice()[cell_ptr..][..cell_len].fill(0);

        if self.slot_ptr == slot_offset {
            self.slot_ptr -= size_of::<u64>();
        } else {
            let end_ptr = self.slot_ptr - size_of::<u64>();
            let a = self.read_ptr(slot_offset)?;
            let b = self.read_ptr(end_ptr)?;
            self.write_ptr(slot_offset, b)?;
            self.write_ptr(end_ptr, a)?;

            self.slot_ptr -= size_of::<u64>();
        }
        self.write_ptr(self.slot_ptr, 0)?;
        self.header.to_mut().size -= 1;
        if self.cell_ptr == cell_ptr {
            // can just increase the cell ptr to ignore
            self.cell_ptr += cell_len;
        } else {
            // add to free list
            let free_cell = FreeCell {
                offset: cell_ptr,
                len: cell_len,
            };
            self.free_list.push_back(free_cell);
            self.merge_free_cells();
        }

        Ok(())
    }

    fn sync_slots(&mut self) {
        let key_to_cell_offset = (0..self.count())
            .into_iter()
            .map(|i| {
                (
                    self.get_cell(i).expect("could not get slot").key_data(),
                    self.get_cell_offset(i).unwrap(),
                )
            })
            .collect::<BTreeMap<_, _>>();

        let in_order = key_to_cell_offset
            .values()
            .map(|&index| index)
            .collect::<Vec<_>>();

        in_order
            .into_iter()
            .zip(self.slots_offsets())
            .collect::<Vec<_>>()
            .into_iter()
            .try_for_each(|(cell_offset, slot_offset)| -> Result<_, _> {
                self.write_ptr(slot_offset, cell_offset)
            })
            .expect("failed to sync slots in data");
    }

    fn merge_free_cells(&mut self) {
        let mut cells = Vec::from_iter(self.free_list.split_off(0));
        cells.sort_by_key(|cell| cell.offset);
        self.free_list.append(
            &mut cells.into_iter().fold(LinkedList::new(), |mut list, next| {
                let merged = if let Some(last) = list.back_mut() {
                    if last.offset + last.len == next.offset {
                        last.len += next.len;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };
                if !merged {
                    list.push_back(next);
                }
                list
            }),
        )
    }

    fn get_slot_offset_from_cell_offset(&self, cell_offset: usize) -> Result<Option<usize>, Error> {
        for slot_offset in self.slots_offsets() {
            let cell_offset_f = self.read_ptr(slot_offset)?;
            if cell_offset == cell_offset_f {
                return Ok(Some(slot_offset));
            }
        }
        Ok(None)
    }
    fn slots_offsets(&self) -> impl FusedIterator<Item = usize> + '_ {
        (0..self.count())
            .into_iter()
            .map(|i| self.get_slot_offset(i).expect("could not get slot"))
            .fuse()
    }

    fn cell_offsets(&self) -> impl FusedIterator<Item = usize> + '_ {
        (0..self.count())
            .into_iter()
            .map(|i| self.get_cell_offset(i).expect("could not get slot"))
            .fuse()
    }

    /// Gets the given cell at a known offset
    fn get_cell_at_offset(&self, offset: usize) -> Result<Cell, Error> {
        match self.page_type() {
            PageType::Key => Ok(KeyCell::read(&self.page.as_slice()[offset..])?.into()),
            PageType::KeyValue => Ok(KeyValueCell::read(&self.page.as_slice()[offset..])?.into()),
        }
    }

    /// Gets the given cell at the slot index
    fn get_cell(&self, slot: usize) -> Result<Cell, Error> {
        let cell_ptr = self.get_cell_offset(slot)?;
        self.get_cell_at_offset(cell_ptr)
    }

    /// Gets the key data at the slot index
    fn get_key_data(&self, slot: usize) -> Result<KeyData, Error> {
        self.get_cell(slot).map(|cell| cell.key_data())
    }

    /// Gets the cell offset of a slot
    fn get_cell_offset(&self, slot: usize) -> Result<usize, Error> {
        let slot_offset = self.get_slot_offset(slot)?;
        self.read_ptr(slot_offset)
    }

    /// Gets the offset of the slot at the given index
    fn get_slot_offset(&self, index: usize) -> Result<usize, Error> {
        if index >= self.count() {
            return Err(Error::ReadDataError(ReadDataError::UnexpectedEof));
        }
        Ok(index * size_of::<u64>())
    }

    /// Reads a pointer (offset from page) at a given offset
    fn read_ptr(&self, offset: usize) -> Result<usize, Error> {
        if offset > self.page.body_len() - size_of::<u64>() {
            return Err(ReadDataError::NotEnoughSpace.into());
        }
        Ok(u64::from_be_bytes(
            self.page.as_slice()[offset..][..size_of::<u64>()]
                .try_into()
                .expect("should be correct number of bytes"),
        ) as usize)
    }

    /// Writes a pointer (offset from page) at a given offset
    fn write_ptr(&mut self, offset: usize, ptr: usize) -> Result<(), Error> {
        if offset > self.page.body_len() - size_of::<u64>() {
            return Err(ReadDataError::NotEnoughSpace.into());
        }

        let buffer = &mut self.page.as_mut_slice()[offset..][..size_of::<u64>()];
        buffer.copy_from_slice(&(ptr as u64).to_be_bytes());

        Ok(())
    }

    fn assert_cell_type(&mut self, cell: &Cell) -> Result<(), Error> {
        match (cell, self.page_type()) {
            (Cell::Key(_), PageType::KeyValue) => {
                return Err(Error::CellTypeMismatch {
                    expected: crate::storage::slotted_page::PageType::KeyValue,
                    actual: crate::storage::slotted_page::PageType::Key,
                });
            }
            (Cell::KeyValue(_), PageType::Key) => {
                return Err(Error::CellTypeMismatch {
                    expected: crate::storage::slotted_page::PageType::Key,
                    actual: crate::storage::slotted_page::PageType::KeyValue,
                });
            }
            _ => Ok(()),
        }
    }

    pub fn page_id(&self) -> PageId {
        self.header.page_id
    }

    pub fn page_type(&self) -> PageType {
        self.header
            .page_type
            .expect("page type should be set at initialization")
    }

    /// Gets the page id of the right sibling of this page
    pub fn right_sibling(&self) -> Option<PageId> {
        self.header.right_page_id
    }
    pub fn set_right_sibling(&mut self, page_id: impl Into<Option<PageId>>) {
        self.header.to_mut().right_page_id = page_id.into()
    }

    /// Gets the page id of the left sibling of this page
    pub fn left_sibling(&self) -> Option<PageId> {
        self.header.left_page_id
    }

    pub fn set_left_sibling(&mut self, page_id: impl Into<Option<PageId>>) {
        self.header.to_mut().left_page_id = page_id.into()
    }

    pub fn parent(&self) -> Option<PageId> {
        self.header.parent_page_id
    }

    pub fn set_parent(&mut self, parent: impl Into<Option<PageId>>) {
        self.header.to_mut().parent_page_id = parent.into();
    }

    pub fn count(&self) -> usize {
        self.header.size() as usize
    }
}

impl<P: Page> Drop for SlottedPage<P> {
    fn drop(&mut self) {
        if self.header.is_dirty() {
            let _ = self.page.set_header(self.header.as_ref().clone());
        }
        if let Some(lock) = self.lock.get() {
            let _ = lock.compare_exchange(true, false, Ordering::SeqCst, Ordering::Relaxed);
        }
    }
}

impl<P: Page> Page for SlottedPage<P> {
    fn len(&self) -> usize {
        self.page.len()
    }

    fn as_slice(&self) -> &[u8] {
        self.page.as_slice()
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.page.as_mut_slice()
    }
}

const MAGIC: u64 = u64::from_be_bytes([b'W', b'E', b'A', b'V', b'E', b'R', b'D', b'B']);

/// The header of a slotted page
#[derive(Debug, Eq, PartialEq, Clone)]
struct SlottedPageHeader {
    magic_number: u64,
    page_id: PageId,
    left_page_id: Option<PageId>,
    right_page_id: Option<PageId>,
    parent_page_id: Option<PageId>,
    page_type: Option<PageType>,
    /// The number of cells stored in this page
    size: u32,
}

impl SlottedPageHeader {
    pub fn new(page_id: PageId) -> Self {
        Self {
            magic_number: MAGIC,
            page_id,
            left_page_id: None,
            right_page_id: None,
            parent_page_id: None,
            page_type: None,
            size: 0,
        }
    }

    pub fn magic_number(&self) -> u64 {
        self.magic_number
    }
    pub fn page_id(&self) -> PageId {
        self.page_id
    }
    pub fn left_page_id(&self) -> Option<PageId> {
        self.left_page_id
    }
    pub fn right_page_id(&self) -> Option<PageId> {
        self.right_page_id
    }
    pub fn parent_page_id(&self) -> Option<PageId> {
        self.parent_page_id
    }
    pub fn page_type(&self) -> Option<PageType> {
        self.page_type
    }

    pub fn size(&self) -> u32 {
        self.size
    }
    pub fn set_page_id(&mut self, page_id: PageId) {
        self.page_id = page_id;
    }
    pub fn set_left_page_id(&mut self, left_page_id: Option<PageId>) {
        self.left_page_id = left_page_id;
    }
    pub fn set_right_page_id(&mut self, right_page_id: Option<PageId>) {
        self.right_page_id = right_page_id;
    }
    pub fn set_parent_page_id(&mut self, parent_page_id: Option<PageId>) {
        self.parent_page_id = parent_page_id;
    }
    pub fn set_page_type(&mut self, page_type: PageType) {
        self.page_type = Some(page_type);
    }
    pub fn set_size(&mut self, size: u32) {
        self.size = size;
    }
}

impl StorageBackedData for SlottedPageHeader {
    type Owned = Self;
    fn read(buf: &[u8]) -> ReadResult<Self> {
        let magic = u64::read(buf)?;
        if magic != MAGIC {
            return Err(ReadDataError::BadMagicNumber);
        }
        const U32_SIZE: usize = size_of::<u32>();
        let buf = &buf[8..];
        let page_id = u32::read(buf)
            .and_then(|id| NonZeroU32::new(id).ok_or_else(|| ReadDataError::BadMagicNumber))
            .map(|u| PageId::new(u))?;
        let buf = buf.get(U32_SIZE..).ok_or(ReadDataError::UnexpectedEof)?;
        let left_page_id = <Option<PageId>>::read(buf)?;
        let buf = buf.get(U32_SIZE..).ok_or(ReadDataError::UnexpectedEof)?;
        let right_page_id = <Option<PageId>>::read(buf)?;
        let buf = buf.get(U32_SIZE..).ok_or(ReadDataError::UnexpectedEof)?;
        let parent_page_id = <Option<PageId>>::read(buf)?;
        let buf = buf.get(U32_SIZE..).ok_or(ReadDataError::UnexpectedEof)?;
        let page_type = PageType::read(buf)?;
        let buf = buf.get(1..).ok_or(ReadDataError::UnexpectedEof)?;
        let size = u32::read(buf)?;

        Ok(SlottedPageHeader {
            magic_number: magic,
            page_id,
            left_page_id,
            right_page_id,
            parent_page_id,
            page_type: Some(page_type),
            size,
        })
    }

    fn write(&self, mut buf: &mut [u8]) -> WriteResult<usize> {
        let len = self.magic_number.write(buf)?;
        buf = &mut buf[len..];
        let len = self.page_id.write(buf)?;
        buf = &mut buf[len..];
        let len = self.left_page_id.write(buf)?;
        buf = &mut buf[len..];
        let len = self.right_page_id.write(buf)?;
        buf = &mut buf[len..];
        let len = self.parent_page_id.write(buf)?;
        buf = &mut buf[len..];
        let len = self.page_type.unwrap().write(buf)?;
        buf = &mut buf[len..];
        let len = self.size.write(buf)?;
        buf = &mut buf[len..];

        Ok(size_of::<Self>())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u8)]
pub enum PageType {
    Key = 1,
    KeyValue = 2,
}

impl StorageBackedData for PageType {
    type Owned = Self;
    fn read(buf: &[u8]) -> ReadResult<Self> {
        match buf.get(0) {
            Some(1) => Ok(PageType::Key),
            Some(2) => Ok(PageType::KeyValue),
            Some(_) => Err(ReadDataError::BadMagicNumber),
            None => Err(ReadDataError::UnexpectedEof),
        }
    }

    fn write(&self, buf: &mut [u8]) -> WriteResult<usize> {
        let b = *self as u8;
        b.write(buf)
    }
}

/// Provides an allocator for slotted pages
#[derive(Debug)]
pub struct SlottedPageAllocator<P: Paged> {
    paged: P,
    next_page_id: AtomicU32,
    free_list: RwLock<VecDeque<usize>>,
    page_id_to_index: BTreeMap<PageId, usize>,
    usage: Mutex<BTreeMap<PageId, Arc<AtomicBool>>>,
}

impl<P: Paged> SlottedPageAllocator<P> {
    pub fn new(paged: P) -> Self {
        let mut paged = if paged.len() * paged.page_size() == paged.reserved() {
            Self {
                paged,
                next_page_id: AtomicU32::new(1),
                free_list: Default::default(),
                page_id_to_index: Default::default(),
                usage: Default::default(),
            }
        } else {
            let mut empty = vec![];
            let mut max = PageId::new(1.try_into().unwrap());
            {
                let mut iter = paged.iter();
                while let Some(Ok((mut page, index))) = iter.next() {
                    if !Self::has_magic(&page) {
                        empty.push(index);
                    }
                    let split = make_slotted(&mut page).page_id();
                    if split > max {
                        max = split;
                    }
                }
            }
            Self {
                paged,
                next_page_id: Default::default(),
                free_list: Default::default(),
                page_id_to_index: Default::default(),
                usage: Default::default(),
            }
        };
        if let Some(max) = (0..paged.len())
            .into_iter()
            .filter_map(|p| Paged::get(&paged, p).ok())
            .map(|p| p.page_id())
            .max()
        {
            paged.next_page_id = AtomicU32::new(max.as_u32());
        }

        for (page, index) in (0..paged.len())
            .into_iter()
            .filter_map(|p| Paged::get(&paged, p).ok().map(|page| (page.page_id(), p)))
            .collect::<Vec<_>>()
        {
            paged.page_id_to_index.insert(page, index);
        }

        paged
    }

    /// Checks if the given page has the magic number
    fn has_magic(page: &P::Page) -> bool {
        &page.as_slice()[0..size_of_val(&MAGIC)] == &MAGIC.to_be_bytes()
    }

    /// Gets the next page id
    fn next_page_id(&self) -> PageId {
        PageId::new(NonZeroU32::new(self.next_page_id.fetch_add(1, Ordering::SeqCst)).unwrap())
    }

    /// Creates a new page of a given type
    pub fn new_with_type(
        &mut self,
        page_type: PageType,
    ) -> Result<(SlottedPage<P::Page>, usize), P::Err> {
        let (mut new, index) = self.new()?;
        new.header.to_mut().set_page_type(page_type);
        self.page_id_to_index.insert(new.page_id(), index);
        Ok((new, index))
    }

    /// Gets the page by a given page_id
    pub fn get(&self, id: PageId) -> Result<SlottedPage<P::Page>, Error> {
        let lock = self.usage.lock().entry(id).or_default().clone();

        self.page_id_to_index
            .get(&id)
            .ok_or_else(|| Error::ReadDataError(ReadDataError::PageNotFound(id)))
            .and_then(|index| {
                Paged::get(self, *index)
                    .map_err(|e| Error::custom(format!("could not read page {}: {e}", index)))
            })
            .map(|page| {
                page.lock.set(lock).expect("lock should be empty");
                page
            })
    }
}

fn make_slotted<P: Page>(page: P) -> SlottedPage<P> {
    let split = SplitPage::<_, SlottedPageHeader>::new(page, size_of::<SlottedPageHeader>());
    let body_len = split.body_len();
    let header = split.header().expect("could not read header");
    let len = header.size as usize;
    let slot_ptr = len * size_of::<u64>();
    let mut min_offset = split.body_len();
    for i in 0..len {
        let slot_offset = i * size_of::<u64>();
        let ptr = &split.get(slot_offset..).expect("must exist")[..size_of::<u64>()];
        let offset = u64::from_be_bytes(ptr.try_into().expect("will be exactly 8 bytes")) as usize;
        if offset < min_offset {
            min_offset = offset;
        }
    }

    let cell_ptr = min_offset;
    let mut output = SlottedPage {
        page: split,
        header: Mad::new(header),
        slot_ptr,
        cell_ptr,
        free_list: Default::default(),
        lock: Default::default(),
        my_lock: false,
    };
    output.sync_slots();
    let mut cell_offsets = output.cell_offsets().collect::<Vec<_>>();
    cell_offsets.sort();
    for cell_index in 0..cell_offsets.len() {
        let cell_offset = cell_offsets[cell_index];
        let next_cell_offset = cell_offsets
            .get(cell_index + 1)
            .copied()
            .unwrap_or(body_len);
        let available_space = next_cell_offset.abs_diff(cell_offset);
        let cell_len = output.get_cell_at_offset(cell_offset).unwrap().len();

        if available_space > cell_len {
            let free_len = available_space - cell_len;
            let free_offset = cell_offset + cell_len;
            output.free_list.push_back(FreeCell {
                offset: free_offset,
                len: free_len,
            })
        }
    }

    output
}

impl<P: Paged> Paged for SlottedPageAllocator<P> {
    type Page = SlottedPage<P::Page>;
    type Err = P::Err;

    fn page_size(&self) -> usize {
        self.paged.page_size()
    }

    fn get(&self, index: usize) -> Result<Self::Page, Self::Err> {
        let page = self.paged.get(index)?;
        Ok(make_slotted(page))
    }

    fn new(&self) -> Result<(Self::Page, usize), Self::Err> {
        let id = self.next_page_id();
        let (zeroed_page, index) = if let Some(index) = self.free_list.write().pop_front() {
            (self.paged.get(index)?, index)
        } else {
            self.paged.new()?
        };
        let header = SlottedPageHeader::new(id);
        let page = SplitPage::new(zeroed_page, size_of_val(&header));
        let cell_ptr = page.body_len();
        Ok((
            SlottedPage {
                page,
                header: Mad::new(header),
                slot_ptr: 0,
                cell_ptr,
                free_list: Default::default(),
                lock: Default::default(),
                my_lock: false,
            },
            index,
        ))
    }

    fn free(&self, index: usize) -> Result<(), Self::Err> {
        self.paged.free(index)?;
        self.free_list.write().push_back(index);
        Ok(())
    }

    fn len(&self) -> usize {
        (0..self.paged.len())
            .filter_map(|index| {
                let page = self.paged.get(index).ok();
                page
            })
            .filter(|s| Self::has_magic(s))
            .count()
    }

    fn reserved(&self) -> usize {
        self.paged.reserved()
    }
}

impl<P: Paged> SlottedPageAllocator<P> {}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use tempfile::tempfile;

    use crate::data::values::Value;
    use crate::error::Error;
    use crate::key::KeyData;
    use crate::storage::abstraction::{Page, Paged, VecPaged};
    use crate::storage::cells::{Cell, KeyCell, PageId};
    use crate::storage::ram_file::{PagedFile, RandomAccessFile};
    use crate::storage::slotted_page::{PageType, SlottedPageAllocator, SlottedPageHeader};
    use crate::storage::WriteDataError;

    #[test]
    fn slotted_page() {
        let mut slotted_pager = SlottedPageAllocator::new(VecPaged::new(1028));
        {
            let slotted_page = slotted_pager.new_with_type(PageType::KeyValue).unwrap();
            let slotted_page2 = slotted_pager.new_with_type(PageType::Key).unwrap();
        }
        assert!(slotted_pager.reserved() > 0);
        assert_eq!(slotted_pager.len(), 2);
    }

    #[test]
    fn reuse_slotted_page_after_free() {
        let mut slotted_pager = SlottedPageAllocator::new(VecPaged::new(1028));
        {
            let (slotted_page, index) = slotted_pager.new_with_type(PageType::KeyValue).unwrap();
            let slotted_page2 = slotted_pager.new_with_type(PageType::Key).unwrap();
            slotted_pager.free(index).expect("could not free");
        }
        assert_eq!(slotted_pager.reserved(), 2 * 1028);
        assert_eq!(slotted_pager.len(), 1);
        let (slotted_page, index) = slotted_pager.new_with_type(PageType::KeyValue).unwrap();
        assert_eq!(index, 0, "should re-use 0 index");
    }

    #[test]
    fn reuse_slotted_page_after_free_file() {
        let temp = tempfile().expect("could not create file");
        let file = RandomAccessFile::with_file(temp).expect("could not create RAFile");
        let mut slotted_pager = SlottedPageAllocator::new(PagedFile::new(file, 1028));
        {
            let (slotted_page, index) = slotted_pager.new_with_type(PageType::KeyValue).unwrap();
            let slotted_page2 = slotted_pager.new_with_type(PageType::Key).unwrap();
            drop(slotted_page);
            slotted_pager.free(index).expect("could not free");
        }
        assert_eq!(slotted_pager.reserved(), 2 * 1028);
        assert_eq!(slotted_pager.len(), 1);
        let (slotted_page, index) = slotted_pager.new_with_type(PageType::KeyValue).unwrap();
        assert_eq!(index, 0, "should re-use 0 index");
    }

    #[test]
    fn insert_cell() {
        let mut slotted_pager = SlottedPageAllocator::new(VecPaged::new(1028));
        let (mut page, _) = slotted_pager.new_with_type(PageType::Key).unwrap();
        let key_data = KeyData::from([Value::from(1_i64)]);
        page.insert(KeyCell::new(15, key_data.clone()).into())
            .expect("could not insert into page");
        assert_eq!(page.count(), 1);
        let cell = page
            .get(&key_data)
            .expect("error occurred")
            .expect("cell not found");
        assert_eq!(&cell.key_data(), &key_data);
    }

    #[test]
    fn insert_cell_same_value() {
        let mut slotted_pager = SlottedPageAllocator::new(VecPaged::new(1028));
        let (mut page, _) = slotted_pager.new_with_type(PageType::Key).unwrap();
        let key_data = KeyData::from([Value::from(1_i64)]);
        page.insert(KeyCell::new(15, key_data.clone()).into())
            .expect("could not insert into page");
        page.insert(KeyCell::new(16, key_data.clone()).into())
            .expect("could not insert into page");
        assert_eq!(page.count(), 1);
        let cell = page
            .get(&key_data)
            .expect("error occurred")
            .expect("cell not found");
        assert_eq!(&cell.key_data(), &key_data);
    }

    #[test]
    fn insert_cell_into_full() {
        let mut slotted_pager =
            SlottedPageAllocator::new(VecPaged::new(size_of::<SlottedPageHeader>()));
        let (mut page, _) = slotted_pager.new_with_type(PageType::Key).unwrap();
        let key_data = KeyData::from([Value::from(1_i64)]);
        let err = page
            .insert(KeyCell::new(15, key_data.clone()).into())
            .expect_err("shouldn't be able to insert into page");
        assert!(
            matches!(
                err,
                Error::WriteDataError(WriteDataError::AllocationFailed { .. })
            ),
            "should be an allocation failed error"
        );
    }

    #[test]
    fn binary_search() {
        let mut slotted_pager = SlottedPageAllocator::new(VecPaged::new(1028));
        let (mut page, _) = slotted_pager.new_with_type(PageType::Key).unwrap();

        for i in 0..32 {
            let key_data = KeyData::from([Value::from(i)]);
            page.insert(KeyCell::new(15, key_data.clone()).into())
                .unwrap();
        }

        let page = page
            .binary_search(&KeyData::from([18]))
            .expect("should not fail");
        assert!(matches!(page, Ok(_)));
        assert_eq!(page.unwrap(), 18);
    }

    #[test]
    fn get_range() {
        let mut slotted_pager = SlottedPageAllocator::new(VecPaged::new(1028));
        let (mut page, _) = slotted_pager.new_with_type(PageType::Key).unwrap();

        for i in 0..32 {
            let key_data = KeyData::from([Value::from(i)]);
            page.insert(KeyCell::new(15, key_data.clone()).into())
                .unwrap();
        }

        assert_eq!(page.get_range(..).unwrap().len(), 32);
        assert_eq!(page.get_range(KeyData::from([16])..).unwrap().len(), 16);
        assert_eq!(
            page.get_range(KeyData::from([8])..KeyData::from([24]))
                .unwrap()
                .len(),
            16
        );
        let cells = page
            .get_range(KeyData::from([8])..=KeyData::from([24]))
            .unwrap();
        assert_eq!(cells.len(), 17);
        println!(
            "cells: {:#?}",
            cells.iter().map(Cell::key_data).collect::<Vec<_>>()
        );
    }

    #[test]
    fn reuse_cell() {
        let mut slotted_pager = SlottedPageAllocator::new(VecPaged::new(1028));
        let (mut page, _) = slotted_pager.new_with_type(PageType::Key).unwrap();
        let key_data1 = KeyData::from([Value::from(1_i64)]);
        let key_data2 = KeyData::from([Value::from(2_i64)]);
        page.insert(KeyCell::new(15, key_data1.clone()).into())
            .expect("could not insert into page");
        page.insert(KeyCell::new(16, key_data2.clone()).into())
            .expect("could not insert into page");
        page.insert(KeyCell::new(17, KeyData::from([Value::from(3_i64)])).into())
            .expect("could not insert into page");
        assert_eq!(page.count(), 3);
        page.delete(&key_data1).expect("could not delete");
        let removed = page.delete(&key_data2).expect("could not delete").unwrap();
        println!("free list: {:#?}", page.free_list);
        page.insert(KeyCell::new(15, key_data1.clone()).into())
            .expect("could not insert into page");
        println!("free list: {:#?}", page.free_list);
        assert!(!page.free_list.is_empty());
        assert_eq!(page.free_list.front().unwrap().len, removed.len());
    }

    #[test]
    fn merge_free_cells() {
        let mut slotted_pager = SlottedPageAllocator::new(VecPaged::new(1028));
        let (mut page, _) = slotted_pager.new_with_type(PageType::Key).unwrap();
        let key_data1 = KeyData::from([Value::from(1_i64)]);
        let key_data2 = KeyData::from([Value::from(2_i64)]);
        page.insert(KeyCell::new(15, key_data1.clone()).into())
            .expect("could not insert into page");
        page.insert(KeyCell::new(16, key_data2.clone()).into())
            .expect("could not insert into page");
        page.insert(KeyCell::new(17, KeyData::from([Value::from(3_i64)])).into())
            .expect("could not insert into page");
        assert_eq!(page.count(), 3);
        page.delete(&key_data2).expect("could not delete");
        println!("free list: {:#?}", page.free_list);
        let cell_ptr = page.cell_ptr;
        assert_eq!(page.free_list.len(), 1);
        let cell = page.free_list.front().unwrap().clone();
        page.delete(&key_data1).expect("could not delete");
        println!("free list: {:#?}", page.free_list);
        assert_eq!(
            page.free_list.len(),
            1,
            "free list cells should've combined: {:?}",
            page.free_list
        );
        let after_cell = page.free_list.front().unwrap().clone();
        assert_eq!(
            cell.offset, after_cell.offset,
            "offset should've stayed the same"
        );
        assert_ne!(cell.len, after_cell.len, "length should've changed");
        assert_eq!(page.cell_ptr, cell_ptr, "cell ptr should not have moved");
    }

    #[test]
    fn rebuild_free_cells() {
        let mut slotted_pager = SlottedPageAllocator::new(VecPaged::new(1028));
        {
            let (mut page, _) = slotted_pager.new_with_type(PageType::Key).unwrap();
            let key_data1 = KeyData::from([Value::from(1_i64)]);
            let key_data2 = KeyData::from([Value::from(2_i64)]);
            page.insert(KeyCell::new(15, key_data1.clone()).into())
                .expect("could not insert into page");
            page.insert(KeyCell::new(16, key_data2.clone()).into())
                .expect("could not insert into page");
            page.insert(KeyCell::new(17, KeyData::from([Value::from(3_i64)])).into())
                .expect("could not insert into page");
            page.delete(&key_data2).expect("could not delete");
            page.delete(&key_data1).expect("could not delete");
        }
        let page = slotted_pager
            .get(PageId::new(1.try_into().unwrap()))
            .unwrap();
        assert!(!page.free_list.is_empty(), "free list should not be empty");
    }

    #[test]
    fn rebuild_has_no_free_cells_if_no_deletions() {
        let mut slotted_pager = SlottedPageAllocator::new(VecPaged::new(1028));
        {
            let (mut page, _) = slotted_pager.new_with_type(PageType::Key).unwrap();
            let key_data1 = KeyData::from([Value::from(1_i64)]);
            let key_data2 = KeyData::from([Value::from(2_i64)]);
            page.insert(KeyCell::new(15, key_data1.clone()).into())
                .expect("could not insert into page");
            page.insert(KeyCell::new(16, key_data2.clone()).into())
                .expect("could not insert into page");
            page.insert(KeyCell::new(17, KeyData::from([Value::from(3_i64)])).into())
                .expect("could not insert into page");
        }
        let page = slotted_pager
            .get(PageId::new(1.try_into().unwrap()))
            .unwrap();
        assert!(page.free_list.is_empty(), "free list should not be empty");
    }
}
