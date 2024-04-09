//! Second version of slotted pages, built over page abstractions

use derive_more::{Deref, DerefMut};
use std::collections::{BTreeMap, Bound, LinkedList, VecDeque};

use std::mem::{size_of, size_of_val};
use std::num::NonZeroU32;

use std::sync::atomic::{AtomicI32, AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};

use crate::common::linked_list;
use parking_lot::{Mutex, RwLock};

use crate::common::track_dirty::Mad;
use crate::error::WeaverError;
use crate::key::{KeyData, KeyDataRange};
use crate::monitoring::{Monitor, Monitorable};
use crate::storage::cells::{Cell, KeyCell, KeyValueCell, PageId};
use crate::storage::paging::traits::{
    Page, PageMut, PageMutWithHeader, PageWithHeader, Pager, SplitPage,
};
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
    _slot: usize,
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
pub struct SlottedPageShared<'a, P: Page<'a>> {
    page: SplitPage<'a, P, SlottedPageHeader>,
    header: Mad<SlottedPageHeader>,
    /// points to the end of the slots
    slot_ptr: usize,
    /// points to the beginning of the cells
    cell_ptr: usize,
    /// A list of free space
    free_list: LinkedList<FreeCell>,
    lock: OnceLock<Arc<AtomicI32>>,
}

impl<'a, P: Page<'a>> SlottedPageShared<'a, P> {
    /// Works similarly to how the binary search method works in the [`slice`][<\[_\]>::binary_search] primitive.
    ///
    /// If present, `Ok(index)` is returned, and if not present `Err(index)` is returned, where the index
    /// is where the key data could be inserted to maintain sort order.
    pub fn binary_search(&self, key_data: &KeyData) -> Result<Result<usize, usize>, WeaverError> {
        let mut l: usize = 0;
        let mut r: usize = self.count().saturating_sub(1);

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
    pub fn get(&self, key_data: &KeyData) -> Result<Option<Cell>, WeaverError> {
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
    pub fn get_range<I: Into<KeyDataRange>>(&self, key_data: I) -> Result<Vec<Cell>, WeaverError> {
        if self.count() == 0 {
            return Ok(vec![]);
        }
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
            .map(|index| self.get_cell(index))
            .collect()
    }

    /// Gets all the cells within this page
    #[inline]
    pub fn all(&self) -> Result<Vec<Cell>, WeaverError> {
        self.get_range(..)
    }
    fn get_slot_offset_from_cell_offset(&self, cell_offset: usize) -> Result<Option<usize>, WeaverError> {
        for slot_offset in self.slots_offsets() {
            let cell_offset_f = self.read_ptr(slot_offset)?;
            if cell_offset == cell_offset_f {
                return Ok(Some(slot_offset));
            }
        }
        Ok(None)
    }
    fn slots_offsets(&self) -> Vec<usize> {
        (0..self.count())
            .map(|i| self.get_slot_offset(i).expect("could not get slot"))
            .fuse()
            .collect()
    }

    fn cell_offsets(&self) -> Vec<usize> {
        (0..self.count())
            .map(|i| self.get_cell_offset(i).expect("could not get slot"))
            .fuse()
            .collect()
    }

    /// Gets the given cell at a known offset
    fn get_cell_at_offset(&self, offset: usize) -> Result<Cell, WeaverError> {
        let slice = self.page.as_slice();
        let slice = Box::from(&slice[offset..]);
        match self.page_type() {
            PageType::Key => Ok(KeyCell::read(&slice)?.into()),
            PageType::KeyValue => Ok(KeyValueCell::read(&slice)?.into()),
        }
    }

    /// Gets the given cell at the slot index
    fn get_cell(&self, slot: usize) -> Result<Cell, WeaverError> {
        let cell_ptr = self.get_cell_offset(slot)?;
        self.get_cell_at_offset(cell_ptr)
    }

    /// Gets the key data at the slot index
    fn get_key_data(&self, slot: usize) -> Result<KeyData, WeaverError> {
        self.get_cell(slot).map(|cell| cell.key_data())
    }

    /// Gets the cell offset of a slot
    fn get_cell_offset(&self, slot: usize) -> Result<usize, WeaverError> {
        let slot_offset = self.get_slot_offset(slot)?;
        self.read_ptr(slot_offset)
    }

    /// Gets the offset of the slot at the given index
    fn get_slot_offset(&self, index: usize) -> Result<usize, WeaverError> {
        if index >= self.count() {
            return Err(WeaverError::ReadDataError(ReadDataError::UnexpectedEof));
        }
        Ok(index * size_of::<u64>())
    }

    /// Reads a pointer (offset from page) at a given offset
    fn read_ptr(&self, offset: usize) -> Result<usize, WeaverError> {
        if offset > self.page.body_len() - size_of::<u64>() {
            return Err(ReadDataError::NotEnoughSpace.into());
        }
        Ok(u64::from_be_bytes(
            self.page.as_slice()[offset..][..size_of::<u64>()]
                .try_into()
                .expect("should be correct number of bytes"),
        ) as usize)
    }

    fn assert_cell_type(&self, cell: &Cell) -> Result<(), WeaverError> {
        match (cell, self.page_type()) {
            (Cell::Key(_), PageType::KeyValue) => {
                Err(WeaverError::CellTypeMismatch {
                    page_id: self.page_id(),
                    expected: PageType::KeyValue,
                    actual: PageType::Key,
                })
            }
            (Cell::KeyValue(_), PageType::Key) => {
                Err(WeaverError::CellTypeMismatch {
                    page_id: self.page_id(),
                    expected: PageType::Key,
                    actual: PageType::KeyValue,
                })
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

    /// Gets the page id of the left sibling of this page
    pub fn left_sibling(&self) -> Option<PageId> {
        self.header.left_page_id
    }

    pub fn parent(&self) -> Option<PageId> {
        self.header.parent_page_id
    }

    /// Gets the count of cells within this page
    pub fn count(&self) -> usize {
        self.header.size() as usize
    }

    /// Gets the max key in this cell
    pub fn max_key(&self) -> Result<Option<KeyData>, WeaverError> {
        if self.count() == 0 {
            return Ok(None);
        }
        self.get_cell(self.count() - 1)
            .map(|cell| Some(cell.key_data()))
    }

    /// Gets the min key in this cell
    pub fn min_key(&self) -> Result<Option<KeyData>, WeaverError> {
        if self.count() == 0 {
            return Ok(None);
        }
        self.get_cell(0).map(|cell| Some(cell.key_data()))
    }

    /// Gets the median key in this cell
    pub fn median_key(&self) -> Result<Option<KeyData>, WeaverError> {
        let count = self.count();
        if count == 0 {
            return Ok(None);
        }
        let mid = count / 2;
        self.get_key_data(mid).map(Some)
    }

    /// Gets the range of the key data
    pub fn key_range(&self) -> Result<KeyDataRange, WeaverError> {
        let min = self
            .min_key()?
            .map(Bound::Included)
            .unwrap_or(Bound::Unbounded);
        let max = self
            .max_key()?
            .map(Bound::Included)
            .unwrap_or(Bound::Unbounded);
        Ok(KeyDataRange(min, max))
    }
}

impl<'a, P: PageMut<'a>> SlottedPageShared<'a, P> {
    /// Insert a cell into a slotted page. Must lock the cell
    pub fn insert(&mut self, cell: Cell) -> Result<(), WeaverError> {
        self.assert_cell_type(&cell)?;
        self.lock()?;
        let key_data = &cell.key_data();
        let cell_len = cell.len();
        if self.contains(key_data) {
            self.delete(key_data)?;
        }
        let Some(CellPtr {
            _slot: _,
            cell: cell_ptr,
        }) = self.alloc(cell_len)
        else {
            return Err(WriteDataError::AllocationFailed {
                page_id: self.page_id().as_u32(),
                size: cell_len,
            }
            .into());
        };

        let data = &mut self.page.as_mut_slice()[cell_ptr..][..cell_len];

        match cell {
            Cell::Key(key) => {
                key.write(data)?;
            }
            Cell::KeyValue(key_value) => {
                key_value.write(data)?;
            }
        }

        self.sync_slots();

        Ok(())
    }

    /// Locks this page, granting exclusive access to it. Required when performing insertions or deletions.
    /// Repeatedly locking the page has no effect once successful. Unlocked upon dropping
    pub fn lock(&mut self) -> Result<(), WeaverError> {
        Ok(())
    }

    /// Unlocks this page if this page has exclusive ownership over the backing data.
    ///
    /// Has no effect if page is not locked
    pub fn unlock(&mut self) {}

    /// Deletes the cell with a given key if present
    pub fn delete(&mut self, key_data: &KeyData) -> Result<Option<Cell>, WeaverError> {
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
    pub fn drain<I: Into<KeyDataRange>>(&mut self, key_data: I) -> Result<Vec<Cell>, WeaverError> {
        let range = key_data.into();
        let min = match &range.0 {
            Bound::Included(i) => match self.binary_search(i)? {
                Ok(exact) => exact,
                Err(not_present) => not_present,
            },
            Bound::Excluded(i) => match self.binary_search(i)? {
                Ok(exact) => exact + 1,
                Err(not_present) => not_present + 1,
            },
            Bound::Unbounded => 0,
        };
        let max = match &range.1 {
            Bound::Included(i) => match self.binary_search(i)? {
                Ok(exact) => exact,
                Err(not_present) => not_present,
            },
            Bound::Excluded(i) => match self.binary_search(i)? {
                Ok(exact) => exact - 1,
                Err(not_present) => not_present - 1,
            },
            Bound::Unbounded => self.count(),
        };
        // remove min..=max
        let cells = (min..=max)
            .map(|slot| self.get_cell(slot))
            .collect::<Result<Vec<_>, _>>()?;

        self.free_slot_chunk(min * size_of::<u64>(), (max + 1) * size_of::<u64>())?;
        self.sync_slots();
        Ok(cells)
    }

    /// Gets the space used by the header, the slots, and the cells
    pub fn used(&self) -> usize {
        self.all()
            .into_iter()
            .flatten()
            .map(|cell| cell.len())
            .sum::<usize>()
            + self.count() * size_of::<u64>()
            + size_of::<SlottedPageHeader>()
    }

    /// Gets the free space left over in this page
    pub fn free_space(&self) -> usize {
        self.len() - self.used()
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
            
            self.cell_ptr
        } else {
            return None;
        };
        let slot_ptr = self.slot_ptr;
        self.slot_ptr += size_of::<u64>();
        self.header.to_mut().size += 1;

        self.page.as_mut_slice()[slot_ptr..][..size_of::<u64>()]
            .copy_from_slice(&(cell_ptr as u64).to_be_bytes());

        Some(CellPtr {
            _slot: slot_ptr,
            cell: cell_ptr,
        })
    }

    /// Frees the slot at the given offset
    fn free_slot(&mut self, slot_offset: usize) -> Result<(), WeaverError> {
        if slot_offset >= self.slot_ptr {
            return Err(WeaverError::WriteDataError(WriteDataError::InsufficientSpace));
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

    /// Frees the slot at the given offset to an end slot, exclusuive
    fn free_slot_chunk(&mut self, slot_offset: usize, end_slot_offset: usize) -> Result<(), WeaverError> {
        if slot_offset >= self.slot_ptr || slot_offset > end_slot_offset {
            return Err(WeaverError::WriteDataError(WriteDataError::InsufficientSpace));
        }
        let chunk_size = end_slot_offset - slot_offset;
        assert_eq!(
            chunk_size % size_of::<u64>(),
            0,
            "chunk must be divisible by 8"
        );
        let slots = (end_slot_offset - slot_offset) / size_of::<u64>();
        let mut free_cells = vec![];

        for i in 0..slots {
            let slot_offset = slot_offset + i * size_of::<u64>();
            let cell_ptr = self.read_ptr(slot_offset)?;
            let cell_len = self.get_cell_at_offset(cell_ptr)?.len();
            self.page.as_mut_slice()[cell_ptr..][..cell_len].fill(0);
            free_cells.push(FreeCell {
                offset: cell_ptr,
                len: cell_len,
            })
        }

        if self.slot_ptr == end_slot_offset {
            self.slot_ptr = slot_offset;
        } else {
            for i in 0..slots {
                let slot_offset = slot_offset + i * size_of::<u64>();
                let end_ptr = self.slot_ptr - (size_of::<u64>() * (i + 1));
                let a = self.read_ptr(slot_offset)?;
                let b = self.read_ptr(end_ptr)?;
                self.write_ptr(slot_offset, b)?;
                self.write_ptr(end_ptr, a)?;
            }
            self.slot_ptr -= chunk_size;
        }

        for i in 0..slots {
            self.write_ptr(self.slot_ptr + i * size_of::<u64>(), 0)?;
        }
        self.header.to_mut().size -= slots as u32;

        self.free_list.extend(free_cells);
        self.merge_free_cells();

        Ok(())
    }

    fn sync_slots(&mut self) {
        let key_to_cell_offset = (0..self.count())
            .map(|i| {
                (
                    self.get_cell(i).expect("could not get slot").key_data(),
                    self.get_cell_offset(i).unwrap(),
                )
            })
            .collect::<BTreeMap<_, _>>();

        let in_order = key_to_cell_offset
            .values().copied()
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
        );
        // TODO: if free cell ptr == self.cell_ptr, then increase self.cell_ptr to free cell ptr + len and remove free cell.
        linked_list::sort_by_cached_key(&mut self.free_list, |cell| cell.offset);
        let cells = self.free_list.split_off(0);
        for cell in cells {
            if cell.offset == self.cell_ptr {
                self.cell_ptr = cell.offset + cell.len;
            } else {
                self.free_list.push_back(cell);
            }
        }
    }

    /// Writes a pointer (offset from page) at a given offset
    fn write_ptr(&mut self, offset: usize, ptr: usize) -> Result<(), WeaverError> {
        if offset > self.page.body_len() - size_of::<u64>() {
            return Err(ReadDataError::NotEnoughSpace.into());
        }

        let buffer = &mut self.page.as_mut_slice()[offset..][..size_of::<u64>()];
        buffer.copy_from_slice(&(ptr as u64).to_be_bytes());

        Ok(())
    }

    pub fn set_right_sibling(&mut self, page_id: impl Into<Option<PageId>>) {
        self.header.to_mut().right_page_id = page_id.into()
    }

    pub fn set_left_sibling(&mut self, page_id: impl Into<Option<PageId>>) {
        self.header.to_mut().left_page_id = page_id.into()
    }

    pub fn set_parent(&mut self, parent: impl Into<Option<PageId>>) {
        self.header.to_mut().parent_page_id = parent.into();
    }
}

impl<'a, P: PageMut<'a>> PageMut<'a> for SlottedPageShared<'a, P> {
    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.page.as_mut_slice()
    }
}

impl<'a, P: Page<'a>> Page<'a> for SlottedPageShared<'a, P> {
    fn len(&self) -> usize {
        self.page.len()
    }
    fn as_slice(&self) -> &[u8] {
        self.page.as_slice()
    }
}

#[derive(Debug, Deref, DerefMut)]
pub struct SlottedPageMut<'a, P: PageMut<'a>> {
    shared: SlottedPageShared<'a, P>,
}

impl<'a, P: PageMut<'a>> SlottedPageMut<'a, P> {
    /// Wraps a page, turning it into a slotted page
    pub fn new(page: P) -> Self {
        make_slotted_mut(page)
    }

    pub fn init(page: P, page_type: PageType) -> Result<Self, WeaverError> {
        let mut slotted = Self::new(page);
        if slotted.header.page_type.is_some() {
            return Err(WeaverError::custom("slotted page already initialized"));
        }
        slotted.header.to_mut().set_page_type(page_type);
        Ok(slotted)
    }
}

impl<'a, P: PageMut<'a>> Drop for SlottedPageMut<'a, P> {
    fn drop(&mut self) {
        if self.header.is_dirty() {
            let header = self.header.as_ref().clone();
            let _ = self.page.set_header(header);
        }
        if let Some(lock) = self.lock.get() {
            // println!("dropping slotted page {:?} with {} access", self.page_id(), if self.is_read { "r" } else { "r/w" });
            lock.compare_exchange(-1, 0, Ordering::SeqCst, Ordering::SeqCst)
                .expect("should be -1");
        }
    }
}

impl<'a, P: PageMut<'a>> Page<'a> for SlottedPageMut<'a, P> {
    fn len(&self) -> usize {
        self.shared.len()
    }

    fn as_slice(&self) -> &[u8] {
        self.shared.as_slice()
    }
}

impl<'a, P: PageMut<'a>> PageMut<'a> for SlottedPageMut<'a, P> {
    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.shared.as_mut_slice()
    }
}

#[derive(Debug, Deref)]
pub struct SlottedPage<'a, P: Page<'a>> {
    shared: SlottedPageShared<'a, P>,
}

impl<'a, P: Page<'a>> SlottedPage<'a, P> {
    /// Wraps a normal page into a slotted page
    pub fn new(page: P) -> Self {
        make_slotted(page)
    }
}

impl<'a, P: Page<'a>> Drop for SlottedPage<'a, P> {
    fn drop(&mut self) {
        if let Some(lock) = self.lock.get() {
            // println!("dropping slotted page {:?} with {} access", self.page_id(), if self.is_read { "r" } else { "r/w" });
            lock.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl<'a, P: Page<'a>> Page<'a> for SlottedPage<'a, P> {
    fn len(&self) -> usize {
        self.shared.len()
    }

    fn as_slice(&self) -> &[u8] {
        self.shared.as_slice()
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

#[allow(unused)]
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
            .map(PageId::new)?;
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
        self.size.write(buf)?;

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
        match buf.first() {
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
pub struct SlottedPager<P: Pager> {
    base_pager: P,
    next_page_id: AtomicU32,
    free_list: RwLock<VecDeque<usize>>,
    page_id_to_index: RwLock<BTreeMap<PageId, usize>>,
    usage: Mutex<BTreeMap<PageId, Arc<AtomicI32>>>,
}

impl<P: Pager> SlottedPager<P> {
    pub fn new(paged: P) -> Self {
        let mut paged = if paged.len() * paged.page_size() == paged.reserved() {
            Self {
                base_pager: paged,
                next_page_id: AtomicU32::new(1),
                free_list: Default::default(),
                page_id_to_index: Default::default(),
                usage: Default::default(),
            }
        } else {
            let mut empty = vec![];
            let mut max = PageId::new(1.try_into().unwrap());
            {
                let mut iter = paged.iter_mut();
                while let Some(Ok((page, index))) = iter.next() {
                    if !Self::has_magic(&page) {
                        empty.push(index);
                    }
                    let split = make_slotted_mut(page).page_id();
                    if split > max {
                        max = split;
                    }
                }
            }
            Self {
                base_pager: paged,
                next_page_id: Default::default(),
                free_list: Default::default(),
                page_id_to_index: Default::default(),
                usage: Default::default(),
            }
        };
        if let Some(max) = (0..paged.len())
            .filter_map(|p| Pager::get(&paged, p).ok())
            .map(|p| p.page_id())
            .max()
        {
            paged.next_page_id = AtomicU32::new(max.as_u32());
        }

        for (page, index) in (0..paged.len())
            .filter_map(|p| Pager::get(&paged, p).ok().map(|page| (page.page_id(), p)))
            .collect::<Vec<_>>()
        {
            paged.page_id_to_index.write().insert(page, index);
        }

        paged
    }

    /// Checks if the given page has the magic number
    fn has_magic<'a, Pg: Page<'a>>(page: &Pg) -> bool {
        &page.as_slice()[0..size_of_val(&MAGIC)] == &MAGIC.to_be_bytes()
    }

    /// Gets the next page id
    fn next_page_id(&self) -> PageId {
        PageId::new(NonZeroU32::new(self.next_page_id.fetch_add(1, Ordering::SeqCst)).unwrap())
    }

    /// Creates a new page of a given type
    pub fn new_with_type(
        &self,
        page_type: PageType,
    ) -> Result<(SlottedPageMut<P::PageMut<'_>>, usize), P::Err> {
        let (mut new, index) = Pager::new(self)?;
        new.header.to_mut().set_page_type(page_type);
        self.page_id_to_index.write().insert(new.page_id(), index);
        Ok((new, index))
    }

    /// Gets the page by a given page_id
    pub fn get(&self, id: PageId) -> Result<SlottedPage<P::Page<'_>>, WeaverError> {
        let lock = self.usage.lock().entry(id).or_default().clone();
        lock.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
            if v >= 0 {
                Some(v + 1)
            } else {
                None
            }
        })
        .map_err(|v| {
            eprintln!(
                "could not acquired slotted page {:?} with r access. (reads: {v})",
                id
            );
            WeaverError::ReadDataError(ReadDataError::PageLocked(id))
        })?;

        self.page_id_to_index
            .read()
            .get(&id)
            .ok_or_else(|| WeaverError::ReadDataError(ReadDataError::PageNotFound(id)))
            .and_then(|index| {
                Pager::get(self, *index)
                    .map_err(|e| WeaverError::caused_by(format!("could not read page {}", index), e))
            })
            .map(|page| {
                page.lock.set(lock).expect("lock should be empty");
                page
            })
    }

    /// Gets a mutable version of the page by a given page_id
    pub fn get_mut(&self, id: PageId) -> Result<SlottedPageMut<P::PageMut<'_>>, WeaverError> {
        let lock = self.usage.lock().entry(id).or_default().clone();
        lock.compare_exchange(0, -1, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|v| {
                eprintln!(
                    "could not acquired slotted page {:?} with r/w access. (reads: {})",
                    id, v
                );
                WeaverError::ReadDataError(ReadDataError::PageLocked(id))
            })?;

        self.page_id_to_index
            .read()
            .get(&id)
            .ok_or_else(|| WeaverError::ReadDataError(ReadDataError::PageNotFound(id)))
            .and_then(|index| {
                Pager::get_mut(self, *index)
                    .map_err(|e| WeaverError::caused_by(format!("could not read page {}", index), e))
            })
            .map(|page| {
                page.lock.set(lock).expect("lock should be empty");
                page
            })
    }
}

fn make_slotted<'a, P: Page<'a>>(page: P) -> SlottedPage<'a, P> {
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
    let mut output = SlottedPageShared {
        page: split,
        header: Mad::new(header),
        slot_ptr,
        cell_ptr,
        free_list: Default::default(),
        lock: Default::default(),
    };
    let mut cell_offsets = output.cell_offsets();
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

    SlottedPage { shared: output }
}
fn make_slotted_mut<'a, P: PageMut<'a>>(page: P) -> SlottedPageMut<'a, P> {
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
    let mut output = SlottedPageShared {
        page: split,
        header: Mad::new(header),
        slot_ptr,
        cell_ptr,
        free_list: Default::default(),
        lock: Default::default(),
    };
    output.sync_slots();
    let mut cell_offsets = output.cell_offsets();
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

    SlottedPageMut { shared: output }
}

impl<P: Pager> Monitorable for SlottedPager<P> {
    fn monitor(&self) -> Box<dyn Monitor> {
        self.base_pager.monitor()
    }
}

impl<P: Pager> Pager for SlottedPager<P> {
    type Page<'a> = SlottedPage<'a, P::Page<'a>> where P : 'a;
    type PageMut<'a> = SlottedPageMut<'a, P::PageMut<'a>> where P : 'a;
    type Err = P::Err;

    fn page_size(&self) -> usize {
        self.base_pager.page_size()
    }

    fn get(&self, index: usize) -> Result<Self::Page<'_>, Self::Err> {
        let page = self.base_pager.get(index)?;
        Ok(make_slotted(page))
    }

    fn get_mut(&self, index: usize) -> Result<Self::PageMut<'_>, Self::Err> {
        let page = self.base_pager.get_mut(index)?;
        Ok(make_slotted_mut(page))
    }

    fn new(&self) -> Result<(Self::PageMut<'_>, usize), Self::Err> {
        let id = self.next_page_id();
        let (zeroed_page, index) = if let Some(index) = self.free_list.write().pop_front() {
            (self.base_pager.get_mut(index)?, index)
        } else {
            self.base_pager.new()?
        };
        let header = SlottedPageHeader::new(id);
        let page = SplitPage::new(zeroed_page, size_of_val(&header));
        let cell_ptr = page.body_len();
        let page = SlottedPageMut {
            shared: SlottedPageShared {
                page,
                header: Mad::new(header),
                slot_ptr: 0,
                cell_ptr,
                free_list: Default::default(),
                lock: Default::default(),
            
            },
        };
        let lock = self.usage.lock().entry(id).or_default().clone();
        lock.compare_exchange(0, -1, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_v| WeaverError::ReadDataError(ReadDataError::PageLocked(id)))
            .expect("could not secure");
        let _ = page.lock.set(lock);
        Ok((page, index))
    }

    fn free(&self, index: usize) -> Result<(), Self::Err> {
        self.base_pager.free(index)?;
        self.free_list.write().push_back(index);
        Ok(())
    }

    fn len(&self) -> usize {
        (0..self.base_pager.len())
            .filter_map(|index| {
                let page = self.base_pager.get(index).ok();
                page
            })
            .filter(|s| Self::has_magic(s))
            .count()
    }

    fn reserved(&self) -> usize {
        self.base_pager.reserved()
    }
}

#[cfg(test)]
mod tests {
    use rand::seq::SliceRandom;
    use rand::thread_rng;
    use std::mem::size_of;

    use tempfile::tempfile;

    use crate::data::values::DbVal;
    use crate::error::WeaverError;
    use crate::key::KeyData;
    use crate::storage::cells::{Cell, KeyCell, PageId};
    use crate::storage::paging::file_pager::FilePager;
    use crate::storage::paging::slotted_pager::{PageType, SlottedPageHeader, SlottedPager};
    use crate::storage::paging::traits::{Pager, VecPager};
    use crate::storage::devices::ram_file::RandomAccessFile;
    use crate::storage::WriteDataError;

    #[test]
    fn slotted_page() {
        let slotted_pager = SlottedPager::new(VecPager::new(1028));
        {
            let _slotted_page = slotted_pager.new_with_type(PageType::KeyValue).unwrap();
            let _slotted_page2 = slotted_pager.new_with_type(PageType::Key).unwrap();
        }
        assert!(slotted_pager.reserved() > 0);
        assert_eq!(slotted_pager.len(), 2);
    }

    #[test]
    fn reuse_slotted_page_after_free() {
        let slotted_pager = SlottedPager::new(VecPager::new(1028));
        {
            let (_slotted_page, index) = slotted_pager.new_with_type(PageType::KeyValue).unwrap();
            let _slotted_page2 = slotted_pager.new_with_type(PageType::Key).unwrap();
            slotted_pager.free(index).expect("could not free");
        }
        assert_eq!(slotted_pager.reserved(), 2 * 1028);
        assert_eq!(slotted_pager.len(), 1);
        let (_slotted_page, index) = slotted_pager.new_with_type(PageType::KeyValue).unwrap();
        assert_eq!(index, 0, "should re-use 0 index");
    }

    #[test]
    fn reuse_slotted_page_after_free_file() {
        let temp = tempfile().expect("could not create file");
        let file = RandomAccessFile::with_file(temp).expect("could not create RAFile");
        let slotted_pager = SlottedPager::new(FilePager::with_file_and_page_len(file, 1028));
        {
            let (slotted_page, index) = slotted_pager.new_with_type(PageType::KeyValue).unwrap();
            let _slotted_page2 = slotted_pager.new_with_type(PageType::Key).unwrap();
            drop(slotted_page);
            println!("freeing index: {index}");
            slotted_pager.free(index).expect("could not free");
        }
        assert_eq!(slotted_pager.reserved(), 2 * 1028);
        assert_eq!(slotted_pager.len(), 1);
        let (_slotted_page, index) = slotted_pager.new_with_type(PageType::KeyValue).unwrap();
        assert_eq!(index, 0, "should re-use 0 index");
    }

    #[test]
    fn insert_cell() {
        let slotted_pager = SlottedPager::new(VecPager::new(1028));
        let (mut page, _) = slotted_pager.new_with_type(PageType::Key).unwrap();
        let key_data = KeyData::from([DbVal::from(1_i64)]);
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
        let slotted_pager = SlottedPager::new(VecPager::new(1028));
        let (mut page, _) = slotted_pager.new_with_type(PageType::Key).unwrap();
        let key_data = KeyData::from([DbVal::from(1_i64)]);
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
        let slotted_pager = SlottedPager::new(VecPager::new(size_of::<SlottedPageHeader>()));
        let (mut page, _) = slotted_pager.new_with_type(PageType::Key).unwrap();
        let key_data = KeyData::from([DbVal::from(1_i64)]);
        let err = page
            .insert(KeyCell::new(15, key_data.clone()).into())
            .expect_err("shouldn't be able to insert into page");
        assert!(
            matches!(
                err,
                WeaverError::WriteDataError(WriteDataError::AllocationFailed { .. })
            ),
            "should be an allocation failed error: {err:?}"
        );
    }

    #[test]
    fn binary_search() {
        let slotted_pager = SlottedPager::new(VecPager::new(1028));
        let (mut page, _) = slotted_pager.new_with_type(PageType::Key).unwrap();

        for i in 0..32 {
            let key_data = KeyData::from([DbVal::from(i)]);
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
        let slotted_pager = SlottedPager::new(VecPager::new(1028));
        let (mut page, _) = slotted_pager.new_with_type(PageType::Key).unwrap();

        for i in 0..32 {
            let key_data = KeyData::from([DbVal::from(i)]);
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
    fn drain_one_page() {
        let slotted_pager = SlottedPager::new(VecPager::new(12848));
        let (mut page, _) = slotted_pager.new_with_type(PageType::Key).unwrap();

        let mut values = Vec::from_iter(0..512);
        values.shuffle(&mut thread_rng());
        for i in values {
            let key_data = KeyData::from([DbVal::from(i)]);
            page.insert(KeyCell::new(15, key_data.clone()).into())
                .unwrap();
        }

        assert_eq!(
            slotted_pager.base_pager.len(),
            1,
            "only one page should've been allocated"
        );
        println!("page free space: {}", page.free_space());

        let cells = page
            .drain(KeyData::from([128])..KeyData::from([256]))
            .unwrap();
        println!(
            "cells: {:#?}",
            cells.iter().map(Cell::key_data).collect::<Vec<_>>()
        );
        assert_eq!(cells.len(), 128);
        assert_eq!(page.count(), 512 - 128);
        assert_eq!(
            cells.iter().map(|cell| cell.key_data()).min().unwrap(),
            KeyData::from([128])
        );
        assert_eq!(
            cells.iter().map(|cell| cell.key_data()).max().unwrap(),
            KeyData::from([255])
        );
        for cell in cells {
            assert!(
                !page.contains(&cell.key_data()),
                "page should not contain {cell:?} after drain"
            );
        }
        let max = page.max_key().expect("no max key").unwrap();
        assert_eq!(max, KeyData::from([511]));
    }

    #[test]
    fn reuse_cell() {
        let slotted_pager = SlottedPager::new(VecPager::new(1028));
        let (mut page, _) = slotted_pager.new_with_type(PageType::Key).unwrap();
        let key_data1 = KeyData::from([DbVal::from(1_i64)]);
        let key_data2 = KeyData::from([DbVal::from(2_i64)]);
        page.insert(KeyCell::new(15, key_data1.clone()).into())
            .expect("could not insert into page");
        page.insert(KeyCell::new(16, key_data2.clone()).into())
            .expect("could not insert into page");
        page.insert(KeyCell::new(17, KeyData::from([DbVal::from(3_i64)])).into())
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
        let slotted_pager = SlottedPager::new(VecPager::new(1028));
        let (mut page, _) = slotted_pager.new_with_type(PageType::Key).unwrap();
        let key_data1 = KeyData::from([DbVal::from(1_i64)]);
        let key_data2 = KeyData::from([DbVal::from(2_i64)]);
        page.insert(KeyCell::new(15, key_data1.clone()).into())
            .expect("could not insert into page");
        page.insert(KeyCell::new(16, key_data2.clone()).into())
            .expect("could not insert into page");
        page.insert(KeyCell::new(17, KeyData::from([DbVal::from(3_i64)])).into())
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
        let slotted_pager = SlottedPager::new(VecPager::new(1028));
        {
            let (mut page, _) = slotted_pager.new_with_type(PageType::Key).unwrap();
            let key_data1 = KeyData::from([DbVal::from(1_i64)]);
            let key_data2 = KeyData::from([DbVal::from(2_i64)]);
            page.insert(KeyCell::new(15, key_data1.clone()).into())
                .expect("could not insert into page");
            page.insert(KeyCell::new(16, key_data2.clone()).into())
                .expect("could not insert into page");
            page.insert(KeyCell::new(17, KeyData::from([DbVal::from(3_i64)])).into())
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
        let slotted_pager = SlottedPager::new(VecPager::new(1028));
        {
            let (mut page, _) = slotted_pager.new_with_type(PageType::Key).unwrap();
            let key_data1 = KeyData::from([DbVal::from(1_i64)]);
            let key_data2 = KeyData::from([DbVal::from(2_i64)]);
            page.insert(KeyCell::new(15, key_data1.clone()).into())
                .expect("could not insert into page");
            page.insert(KeyCell::new(16, key_data2.clone()).into())
                .expect("could not insert into page");
            page.insert(KeyCell::new(17, KeyData::from([DbVal::from(3_i64)])).into())
                .expect("could not insert into page");
        }
        let page = slotted_pager
            .get(PageId::new(1.try_into().unwrap()))
            .unwrap();
        assert!(page.free_list.is_empty(), "free list should not be empty");
    }
}
