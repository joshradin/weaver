//! Allow for a "virtual" address space, where many different paged objects are actually
//! stored on a single paged system

use std::borrow::Borrow;
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;
use std::mem::size_of;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use bitfield::bitfield;
use lru::LruCache;
use nom::Parser;
use parking_lot::Mutex;
use thiserror::Error;
use tracing::trace;

use crate::common::consistent_hasher::SeededHasherBuilder;
use crate::common::track_dirty::Mad;
use crate::error::WeaverError;
use crate::monitoring::{Monitor, Monitorable};
use crate::storage::paging::traits::{Page, PageMut};
use crate::storage::Pager;

/// A virtual paged table provides a level of indirection
#[derive(Debug)]
pub struct VirtualPagerTable<K: Eq + Hash, P: Pager> {
    shared: Arc<VirtualPagerShared<K, P>>,
}

impl<K: Eq + Hash, P: Pager> VirtualPagerTable<K, P> {
    pub fn new(paged: P) -> Result<Self, VirtualPagerError> {
        Ok(Self {
            shared: Arc::new(VirtualPagerShared::open(paged)?),
        })
    }

    /// Initializes a new member of the virtual pager table
    pub fn init(&self, key: K) -> Result<(), VirtualPagerError> {
        self.shared.add_root(key)
    }

    /// Gets a virtual pager by the given key
    pub fn get(&self, key: K) -> Result<Option<VirtualPager<K, P>>, VirtualPagerError> {
        if self.shared.contains_root(&key)? {
            Ok(Some(VirtualPager {
                key,
                parent: self.shared.clone(),
            }))
        } else {
            Ok(None)
        }
    }
    /// Gets a virtual pager by the given key, initializes it if the root doesn't already exist
    pub fn get_or_init(&self, key: K) -> Result<VirtualPager<K, P>, VirtualPagerError>
    where
        K: Clone,
    {
        if self.shared.contains_root(&key)? {
            self.get(key)
                .and_then(|o| o.ok_or(VirtualPagerError::RootUndefined))
        } else {
            self.init(key.clone())?;
            self.get(key)
                .and_then(|o| o.ok_or(VirtualPagerError::RootUndefined))
        }
    }

    fn print_roots(&self) {
        for (index, (hash, addr, len)) in self
            .shared
            .roots()
            .expect("could not get roots")
            .enumerate()
        {
            trace!("{index}: hash: {hash}, addr: {addr:?}, len: {len}");
        }
    }
}

static WEAVER_SEED: u64 = u64::from_be_bytes(*b"_WEAVER_");

#[derive(Debug)]
struct VirtualPagerShared<K: Hash, P: Pager> {
    backing_pager: P,
    hash_builder: SeededHasherBuilder,
    /// translation lookahead buffer,
    tlb: Mutex<LruCache<(u64, usize), usize>>,
    control_page_lock: Mutex<()>,
    _key_type: PhantomData<K>,
}

const ENTRY_SIZE: usize = 24;

impl<K: Hash, P: Pager> VirtualPagerShared<K, P> {
    fn open(paged: P) -> Result<Self, VirtualPagerError> {
        if paged.len() == 0 {
            let (_, _) = paged.new().map_err(VirtualPagerError::backing_error)?;
        }

        Ok(Self {
            backing_pager: paged,
            hash_builder: SeededHasherBuilder::with_seed(WEAVER_SEED),
            tlb: LruCache::new(NonZeroUsize::new(512).expect("512 is non zero")).into(),
            control_page_lock: Default::default(),
            _key_type: Default::default(),
        })
    }

    fn get_page_from_tlb<Q: ?Sized>(&self, root_key: &Q, page: usize) -> Option<usize>
    where
        K: Borrow<Q>,
        Q: Hash,
    {
        let hash = self.hash_builder.hash_one(root_key);
        self.tlb.lock().get(&(hash, page)).copied()
    }

    fn put_translation<Q: ?Sized>(&self, root_key: &Q, page: usize, translated: usize)
    where
        K: Borrow<Q>,
        Q: Hash,
    {
        let hash = self.hash_builder.hash_one(root_key);
        self.tlb.lock().put((hash, page), translated);
    }

    /// adds a new root
    fn add_root(&self, key: K) -> Result<(), VirtualPagerError> {
        if self.contains_root(&key)? {
            return Ok(());
        }
        // lock the control page for writing
        let _lock = self.control_page_lock.lock();
        let current_len = self.roots_count()?;
        let insert_at = current_len * ENTRY_SIZE + 8;

        let mut control_page_mut = self.get_control_page_mut()?;
        let buffer = &mut control_page_mut
            .get_mut(insert_at..)
            .ok_or(VirtualPagerError::InsufficientSpace)?[..ENTRY_SIZE];

        let (key_buffer, ptr_buffer) = buffer.split_at_mut(8);

        let hashed = self.hash_builder.hash_one(key);

        key_buffer.copy_from_slice(&hashed.to_be_bytes());
        ptr_buffer.fill(0);

        let new_len = current_len + 1;
        let new_len_bytes = new_len.to_be_bytes();
        control_page_mut
            .get_mut(0..8)
            .ok_or(VirtualPagerError::InsufficientSpace)?
            .copy_from_slice(&new_len_bytes);

        Ok(())
    }

    fn contains_root<Q: ?Sized>(&self, key: &Q) -> Result<bool, VirtualPagerError>
    where
        K: Borrow<Q>,
        Q: Hash,
    {
        let this_hashed = self.hash_builder.hash_one(key);
        Ok(self.roots()?.any(|root| root.0 == this_hashed))
    }

    fn get_root_index<Q: ?Sized>(&self, key: &Q) -> Result<Option<NonZeroU64>, VirtualPagerError>
    where
        K: Borrow<Q>,
        Q: Hash,
    {
        let this_hashed = self.hash_builder.hash_one(key);
        self.roots()?
            .find_map(|(hashed, ptr, len)| {
                if hashed == this_hashed {
                    Some(ptr)
                } else {
                    None
                }
            })
            .ok_or(VirtualPagerError::RootUndefined)
    }

    fn get_control_page(&self) -> Result<P::Page<'_>, VirtualPagerError> {
        self.backing_pager
            .get(0)
            .map_err(VirtualPagerError::backing_error)
    }

    fn get_control_page_mut(&self) -> Result<P::PageMut<'_>, VirtualPagerError> {
        self.backing_pager
            .get_mut(0)
            .map_err(VirtualPagerError::backing_error)
    }

    /// Gets the number of roots
    fn roots_count(&self) -> Result<usize, VirtualPagerError> {
        let buffer = Vec::from(
            self.backing_pager
                .get(0)
                .map_err(VirtualPagerError::backing_error)?
                .get(0..8)
                .unwrap(),
        );
        Ok(u64::from_be_bytes(buffer.try_into().expect("could not convert")) as usize)
    }

    fn root_details<Q: ?Sized>(
        &self,
        key: &Q,
    ) -> Result<Option<(u64, Option<NonZeroU64>, u64)>, VirtualPagerError>
    where
        K: Borrow<Q>,
        Q: Hash,
    {
        let key_hash = self.hash_builder.hash_one(key);
        Ok(self
            .roots()?
            .into_iter()
            .find(|(hash, _, _)| *hash == key_hash))
    }

    fn with_root_details<Q: ?Sized, F, R>(
        &self,
        key: &Q,
        callback: F,
    ) -> Result<Option<R>, VirtualPagerError>
    where
        F: FnOnce(&mut Mad<(u64, Option<NonZeroU64>, u64)>) -> R,
        K: Borrow<Q>,
        Q: Hash,
    {
        let root = self.root_details(key)?;
        if let Some(root) = root {
            let mut mad = Mad::new(root);
            let ret = callback(&mut mad);
            if mad.is_dirty() {
                trace!("root entry changed, writing to page (new root entry: {mad:?})");
                let key_hash = self.hash_builder.hash_one(key);
                let position = self
                    .roots()?
                    .position(|(hash, ..)| hash == key_hash)
                    .expect("should be present");
                let mut control_page = self.get_control_page_mut()?;
                if let Some(non_zero) = &mad.1 {
                    control_page.write_u64((*non_zero).into(), position * ENTRY_SIZE + 16);
                }
                control_page.write_u64(mad.2, position * ENTRY_SIZE + 24)
            }
            Ok(Some(ret))
        } else {
            Ok(None)
        }
    }

    fn roots(
        &self,
    ) -> Result<impl Iterator<Item = (u64, Option<NonZeroU64>, u64)>, VirtualPagerError> {
        let count = self.roots_count()?;
        let control_page = self.get_control_page()?;
        Ok((0..count)
            .map(|i| i * ENTRY_SIZE + 8) // to offset
            .flat_map(|offset| control_page.get(offset..).map(|slice| &slice[..ENTRY_SIZE]))
            .map(|slice| {
                let buf = Vec::from(slice);
                let array: [u8; ENTRY_SIZE] = buf.try_into().unwrap();
                let (hash, ptr_and_len) = array.split_at(8);
                let (ptr, len) = ptr_and_len.split_at(8);
                let ptr = u64::from_be_bytes(ptr.try_into().unwrap());
                (
                    u64::from_be_bytes(hash.try_into().unwrap()),
                    NonZeroU64::new(ptr),
                    u64::from_be_bytes(len.try_into().unwrap()),
                )
            })
            .collect::<Vec<_>>()
            .into_iter())
    }

    fn get_translated<Q: ?Sized>(
        &self,
        root_key: &Q,
        page: usize,
        create_if_missing: bool,
    ) -> Result<usize, VirtualPagerError>
    where
        Q: Hash,
        K: Borrow<Q>,
    {
        if let Some(translated) = self.get_page_from_tlb(root_key, page) {
            return Ok(translated);
        }

        let size = (self.backing_pager.page_size() / 8).ilog2();
        let mask = (1 << size) - 1;

        let (pml4_index, pmd_index, pdp_index, pd_index) = (
            (page >> size * 3) & mask,
            (page >> size * 2) & mask,
            (page >> size * 1) & mask,
            (page >> size * 0) & mask,
        );
        let root = self.get_or_init_root_index_page_map_directory(root_key)?;
        trace!("root: {root}, pml4_index: {pml4_index}");
        let page_map_directory = self.get_indirect_page(root, pml4_index, true)?;
        trace!("pmd: {page_map_directory}, pmd_index: {pmd_index}");
        let page_map = self.get_indirect_page(page_map_directory, pmd_index, true)?;
        trace!("pm: {page_map}, pdp_index: {pdp_index}");
        let page_directory = self.get_indirect_page(page_map, pdp_index, true)?;
        trace!("pd: {page_directory}, pd_index: {pd_index}");
        let translated_page =
            self.get_indirect_page(page_directory, pd_index, create_if_missing)?;
        trace!("actual page: {translated_page}");

        let hash = self.hash_builder.hash_one(root_key);
        self.tlb.lock().put((hash, page), translated_page);

        Ok(translated_page)
    }

    fn free_translated<Q: ?Sized>(&self, root_key: &Q, page: usize) -> Result<(), VirtualPagerError>
    where
        Q: Hash,
        K: Borrow<Q>,
    {
        let (pml4_index, pmd_index, pdp_index, pd_index) = (
            (page >> 27) & 0x1FF,
            (page >> 18) & 0x1FF,
            (page >> 9) & 0x1FF,
            (page >> 0) & 0x1FF,
        );
        let root = self.get_or_init_root_index_page_map_directory(root_key)?;
        trace!("root: {root}, pml4_index: {pml4_index}");
        let page_map_directory = self.get_indirect_page(root, pml4_index, true)?;
        trace!("pmd: {page_map_directory}, pmd_index: {pmd_index}");
        let page_map = self.get_indirect_page(page_map_directory, pmd_index, true)?;
        trace!("pm: {page_map}, pdp_index: {pdp_index}");
        let page_directory = self.get_indirect_page(page_map, pdp_index, true)?;
        trace!("pd: {page_directory}, pd_index: {pd_index}");
        let mut page_dir_mut = self
            .backing_pager
            .get_mut(page_directory)
            .map_err(VirtualPagerError::backing_error)?;
        page_dir_mut.write_u64(0, pd_index * 8);

        let hash = self.hash_builder.hash_one(root_key);
        self.tlb.lock().pop_entry(&(hash, page));

        Ok(())
    }

    fn get_or_init_root_index_page_map_directory<Q: ?Sized>(
        &self,
        key: &Q,
    ) -> Result<usize, VirtualPagerError>
    where
        K: Borrow<Q>,
        Q: Hash,
    {
        self.with_root_details::<_, _, Result<usize, _>>(key, |mut mad| {
            let ptr = &mut mad.to_mut().1;
            match ptr {
                &mut Some(address) => Ok(u64::from(address) as usize),
                none => {
                    trace!("creating page map directory...");
                    let (_, root_ptr) = self
                        .backing_pager
                        .new()
                        .map_err(VirtualPagerError::backing_error)?;
                    none.insert(NonZeroU64::new(root_ptr as u64).unwrap());
                    Ok(root_ptr)
                }
            }
        })
        .and_then(|address: Option<_>| address.ok_or(VirtualPagerError::RootUndefined))
        .and_then(|d| d)
    }

    fn get_indirect_page(
        &self,
        page: usize,
        index: usize,
        create_if_missing: bool,
    ) -> Result<usize, VirtualPagerError> {
        let offset = index * size_of::<u64>();
        let mut entry = {
            let page = self
                .backing_pager
                .get(page)
                .map_err(VirtualPagerError::backing_error)?;
            PageTableEntry(
                page.read_u64(offset)
                    .ok_or(VirtualPagerError::InsufficientSpace)?,
            )
        };
        let address = entry.address();
        if address == 0 {
            if create_if_missing {
                let (_, new_address) = self
                    .backing_pager
                    .new()
                    .map_err(VirtualPagerError::backing_error)?;
                entry.set_address(new_address as u64);
                let mut page = self
                    .backing_pager
                    .get_mut(page)
                    .map_err(VirtualPagerError::backing_error)?;

                page.write_u64(entry.0, offset);
                Ok(new_address)
            } else {
                Err(VirtualPagerError::NoPageAddress(index))
            }
        } else {
            Ok(address as usize)
        }
    }

    fn get_page<Q: ?Sized>(
        &self,
        root_key: &Q,
        page: usize,
    ) -> Result<P::Page<'_>, VirtualPagerError>
    where
        Q: Hash,
        K: Borrow<Q>,
    {
        let translated = self.get_translated(root_key, page, false)?;
        self.backing_pager
            .get(translated)
            .map_err(VirtualPagerError::backing_error)
    }

    fn get_page_mut<Q: ?Sized>(
        &self,
        root_key: &Q,
        page: usize,
    ) -> Result<P::PageMut<'_>, VirtualPagerError>
    where
        Q: Hash,
        K: Borrow<Q>,
    {
        let translated = self.get_translated(root_key, page, false)?;
        self.backing_pager
            .get_mut(translated)
            .map_err(VirtualPagerError::backing_error)
    }

    fn create_page<Q: ?Sized>(
        &self,
        root_key: &Q,
        page: usize,
    ) -> Result<P::PageMut<'_>, VirtualPagerError>
    where
        Q: Hash,
        K: Borrow<Q>,
    {
        let translated = self.get_translated(root_key, page, true)?;
        self.backing_pager
            .get_mut(translated)
            .map_err(VirtualPagerError::backing_error)
    }
}

bitfield! {
    struct PageTableEntry(u64);
    impl Debug;
    address, set_address: 47, 0;
}

#[derive(Debug)]
pub struct VirtualPager<K, P>
where
    K: Hash,
    P: Pager,
{
    key: K,
    parent: Arc<VirtualPagerShared<K, P>>,
}

impl<K, P> Monitorable for VirtualPager<K, P>
where
    K: Hash,
    P: Pager,
{
    fn monitor(&self) -> Box<dyn Monitor> {
        self.parent.backing_pager.monitor()
    }
}

impl<K, P> Pager for VirtualPager<K, P>
where
    K: Hash,
    P: Pager,
{
    type Page<'a> = P::Page<'a> where P: 'a, K: 'a, Self: 'a;
    type PageMut<'a> = P::PageMut<'a> where P: 'a, K: 'a, Self: 'a;
    type Err = VirtualPagerError;

    fn page_size(&self) -> usize {
        self.parent.backing_pager.page_size()
    }

    fn get(&self, index: usize) -> Result<Self::Page<'_>, Self::Err> {
        self.parent.get_page(&self.key, index)
    }

    fn get_mut(&self, index: usize) -> Result<Self::PageMut<'_>, Self::Err> {
        self.parent.get_page_mut(&self.key, index)
    }

    fn new(&self) -> Result<(Self::PageMut<'_>, usize), Self::Err> {
        let next_id = self.len();
        let ret = self
            .parent
            .create_page(&self.key, next_id)
            .map(|c| (c, next_id))?;
        self.parent.with_root_details(&self.key, |mad| {
            mad.to_mut().2 += 1;
        })?;
        Ok(ret)
    }

    fn free(&self, index: usize) -> Result<(), Self::Err> {
        let translated = self.parent.get_translated(&self.key, index, false)?;
        self.parent
            .backing_pager
            .free(translated)
            .map_err(VirtualPagerError::backing_error)?;
        self.parent.free_translated(&self.key, index)?;
        self.parent.with_root_details(&self.key, |mad| {
            mad.to_mut().2 -= 1;
        })?;
        Ok(())
    }

    fn len(&self) -> usize {
        self.parent
            .root_details(&self.key)
            .unwrap()
            .expect("virtual page root details should exist")
            .2 as usize
    }

    /// Virtual pagers only return the reserved length for this virtual pager
    fn reserved(&self) -> usize {
        self.len() * self.page_size()
    }
}

/// A virtual pager error
#[derive(Debug, Error)]
pub enum VirtualPagerError {
    #[error("No page address found for index {0}")]
    NoPageAddress(usize),
    #[error("Insufficient space in page")]
    InsufficientSpace,
    #[error("root not defined")]
    RootUndefined,
    #[error(transparent)]
    BackingPagerError(Box<WeaverError>),
}

impl VirtualPagerError {
    fn backing_error<E: Into<WeaverError>>(error: E) -> Self {
        Self::BackingPagerError(Box::new(error.into()))
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::paging::virtual_pager::VirtualPagerTable;
    use crate::storage::{Pager, VecPager};
    use std::collections::HashMap;

    #[test]
    fn virtual_pager_table() {
        let vp_table = VirtualPagerTable::<String, _>::new(VecPager::default()).unwrap();
        let one = vp_table.get("one".to_string()).expect("virtual page error");

        assert!(one.is_none());
        vp_table.init("one".to_string()).expect("could not init");
        vp_table.print_roots();
        let one = vp_table
            .get("one".to_string())
            .expect("no error")
            .expect("should exist now");
        vp_table.print_roots();
        println!("vp_table: {vp_table:#?}");
        let (_, index) = one.new().expect("could not create page");
        assert_eq!(index, 0, "new index should be 0");

        let (_, index) = one.new().expect("could not create page");
        assert_eq!(index, 1, "new index should be 1");
    }

    #[test]
    fn create_1000_pages() {
        let vp_table = VirtualPagerTable::<String, _>::new(VecPager::default()).unwrap();
        vp_table.init("_1".to_string()).expect("could not init");
        let one = vp_table
            .get("_1".to_string())
            .expect("virtual page error")
            .expect("could not get");
        for _ in 0..1000 {
            one.new()
                .unwrap_or_else(|e| panic!("could not create page: {e}"));
        }
    }

    #[test]
    fn create_1000_pages_small() {
        let vp_table = VirtualPagerTable::<String, _>::new(VecPager::new(512)).unwrap();
        vp_table.init("_1".to_string()).expect("could not init");
        let one = vp_table
            .get("_1".to_string())
            .expect("virtual page error")
            .expect("could not get");
        for _ in 0..1000 {
            one.new()
                .unwrap_or_else(|e| panic!("could not create page: {e}"));
        }
    }

    #[test]
    fn create_2_1000_pages() {
        let vp_table = VirtualPagerTable::<String, _>::new(VecPager::default()).unwrap();
        vp_table.init("_1".to_string()).expect("could not init");
        vp_table.init("_2".to_string()).expect("could not init");
        let one = vp_table
            .get("_1".to_string())
            .expect("virtual page error")
            .expect("could not get");
        let two = vp_table
            .get("_2".to_string())
            .expect("virtual page error")
            .expect("could not get");
        for id in 0..1000 {
            let id = id;
            let (_, new_id) = one
                .new()
                .unwrap_or_else(|e| panic!("could not create page: {e}"));
            assert_eq!(id, new_id);
            let (_, new_id) = two
                .new()
                .unwrap_or_else(|e| panic!("could not create page: {e}"));
            assert_eq!(id, new_id);
            assert!(vp_table.shared.backing_pager.len() >= id * 2);
        }
        println!(
            "tlb: {:#?}",
            vp_table.shared.tlb.lock().iter().collect::<HashMap<_, _>>()
        );
    }

    #[test]
    fn nested_virtual_table() {
        let vp_table1 = VirtualPagerTable::<usize, _>::new(VecPager::default()).unwrap();
        vp_table1.init(0).expect("could not init");
        let vp1 = vp_table1.get(0).unwrap().unwrap();
        let vp_table2 = VirtualPagerTable::<usize, _>::new(vp1).unwrap();
        vp_table2.init(0).expect("could not init inner layer");
        let vp2 = vp_table2.get(0).unwrap().unwrap();
        let (page, index) = vp2.new().expect("create new page");
        assert_eq!(index, 0);
        vp2.free(index).unwrap();
    }
}
