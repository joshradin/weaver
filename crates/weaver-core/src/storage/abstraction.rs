//! Creates abstractions that are used to build better storage-backed data structures

use std::collections::HashMap;
use std::convert::Infallible;
use std::error::Error;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::slice::SliceIndex;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::common::track_dirty::Mad;
use crate::storage::{ReadResult, StorageBackedData, WriteResult, PAGE_SIZE};

/// Allows for getting pages of a fix size
pub trait Pager {
    type Page<'a>: Page<'a>
    where
        Self: 'a;
    type PageMut<'a>: PageMut<'a>
    where
        Self: 'a;
    type Err: Error + Into<crate::error::Error> + Send + Sync + 'static;

    /// Gets the size of pages that are allocated by this paged type
    fn page_size(&self) -> usize;

    /// Gets the page at a given index, returns an error if not possible for
    /// whatever reason
    fn get(&self, index: usize) -> Result<Self::Page<'_>, Self::Err>;
    fn get_mut(&self, index: usize) -> Result<Self::PageMut<'_>, Self::Err>;

    /// Creates a new page, returning mutable reference to the page and the index
    /// it was created at
    fn new(&self) -> Result<(Self::PageMut<'_>, usize), Self::Err>;

    /// Removes the page at a given index, freeing the space it's allocated with.
    ///
    /// If removed, the same index can be reused later on.
    ///
    /// Because of potential re-use, pages should be zeroed after removal.
    fn free(&self, index: usize) -> Result<(), Self::Err>;

    /// Gets the total number of pages allocated
    fn len(&self) -> usize;

    /// Gets the total length of the memory reserved space, in bytes, of the paged object
    fn reserved(&self) -> usize;

    fn iter(&self) -> impl Iterator<Item = Result<(Self::Page<'_>, usize), Self::Err>> + '_ {
        (0..self.len())
            .into_iter()
            .map(|index| self.get(index).map(|page| (page, index)))
    }

    fn iter_mut(&self) -> impl Iterator<Item = Result<(Self::PageMut<'_>, usize), Self::Err>> + '_ {
        (0..self.len())
            .into_iter()
            .map(|index| self.get_mut(index).map(|page| (page, index)))
    }
}

/// Provides a view of page
pub trait PageMut<'a>: Page<'a> {
    /// Gets a mutable reference to this page as slice
    fn as_mut_slice(&mut self) -> &mut [u8];

    /// Returns a mutable raw pointer to the beginning of the page
    ///
    /// Usage of this pointer can only used in unsafe mechanisms
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.as_mut_slice().as_mut_ptr()
    }

    /// Returns a reference to an element or a subslice depending on the type of index
    fn get_mut<I>(&mut self, index: I) -> Option<&mut <I as SliceIndex<[u8]>>::Output>
    where
        I: SliceIndex<[u8]>,
    {
        self.as_mut_slice().get_mut(index)
    }
}

pub trait Page<'a>
where
    Self: Sized,
{
    /// Gets the total length of the page
    fn len(&self) -> usize;
    /// Gets this page as a slice
    fn as_slice(&self) -> &[u8];
    /// Returns a raw pointer to the beginning of the page.
    ///
    /// Usage of this pointer can only used in unsafe blocks
    fn as_ptr(&self) -> *const u8 {
        self.as_slice().as_ptr()
    }
    /// Returns a reference to an element or a subslice depending on the type of index
    fn get<I>(&self, index: I) -> Option<&<I as SliceIndex<[u8]>>::Output>
    where
        I: SliceIndex<[u8]>,
    {
        self.as_slice().get(index)
    }
}

impl<'a, P: PageMut<'a>> PageMut<'a> for &'a mut P {
    fn as_mut_slice(&mut self) -> &mut [u8] {
        (**self).as_mut_slice()
    }
}

impl<'a, P: Page<'a>> Page<'a> for &'a P {
    fn len(&self) -> usize {
        (**self).len()
    }
    fn as_slice(&self) -> &'a [u8] {
        (**self).as_slice()
    }
}

impl<'a, P: Page<'a>> Page<'a> for &'a mut P {
    fn len(&self) -> usize {
        (**self).len()
    }
    fn as_slice(&self) -> &[u8] {
        (**self).as_slice()
    }
}

pub trait PageWithHeader<'a>: Page<'a> {
    type Header: Clone + PartialEq + StorageBackedData;
    fn header(&self) -> ReadResult<Self::Header>;

    /// Gets the length of the header
    fn header_len(&self) -> usize;

    /// Gets the length of the body
    fn body_len(&self) -> usize {
        self.len() - self.header_len()
    }
}

/// A page with a header
pub trait PageMutWithHeader<'a>: PageWithHeader<'a> + PageMut<'a> {
    fn set_header(&mut self, header: Self::Header) -> WriteResult<()>;
}

/// Provides a simple implementation of a split page with a header
#[derive(Debug)]
pub struct SplitPage<'a, P, Header>
where
    P: Page<'a>,
    Header: Clone + PartialEq + StorageBackedData,
{
    page: P,
    header_len: usize,
    _header: PhantomData<&'a Header>,
}

impl<'a, P, Header> PageWithHeader<'a> for SplitPage<'a, P, Header>
where
    P: Page<'a>,
    Header: Clone + PartialEq + StorageBackedData<Owned = Header>,
{
    type Header = Header;

    fn header(&self) -> ReadResult<Self::Header> {
        Header::read(&self.page.as_slice()[..self.header_len])
    }

    fn header_len(&self) -> usize {
        self.header_len
    }
}

impl<'a, P, Header> SplitPage<'a, P, Header>
where
    P: Page<'a>,
    Header: Clone + PartialEq + StorageBackedData,
{
    pub fn new(page: P, header_len: usize) -> Self {
        Self {
            page,
            header_len,
            _header: Default::default(),
        }
    }
}

impl<'a, P, Header> PageMutWithHeader<'a> for SplitPage<'a, P, Header>
where
    P: PageMut<'a>,
    Header: Clone + PartialEq + StorageBackedData<Owned = Header>,
{
    fn set_header(&mut self, header: Self::Header) -> WriteResult<()> {
        let reference = &mut self.page.as_mut_slice()[..self.header_len];
        let result = header.write::<'_>(reference).map(move |_| ());
        drop(header);
        result
    }
}

impl<'a, P, Header> PageMut<'a> for SplitPage<'a, P, Header>
where
    P: PageMut<'a>,
    Header: Clone + PartialEq + StorageBackedData,
{
    fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.page.as_mut_slice()[self.header_len..]
    }
}

impl<'a, P, Header> Page<'a> for SplitPage<'a, P, Header>
where
    P: Page<'a>,
    Header: Clone + PartialEq + StorageBackedData,
{
    fn len(&self) -> usize {
        self.page.len()
    }
    fn as_slice(&self) -> &[u8] {
        &self.page.as_slice()[self.header_len..]
    }
}

/// An implementation over pages
#[derive(Debug)]
pub struct VecPager {
    pages: RwLock<Vec<Arc<RwLock<Box<[u8]>>>>>,
    usage: RwLock<HashMap<usize, Arc<AtomicI32>>>,
    page_len: usize,
}

impl Default for VecPager {
    fn default() -> Self {
        Self::new(PAGE_SIZE)
    }
}

impl VecPager {
    /// Creates a new vec-paged with a given page len
    pub fn new(page_len: usize) -> Self {
        Self {
            pages: Default::default(),
            usage: Default::default(),
            page_len,
        }
    }
}

impl Pager for VecPager {
    type Page<'a> = SharedPage<'a>;
    type PageMut<'a> = SharedPageMut<'a>;

    type Err = Infallible;

    fn page_size(&self) -> usize {
        self.page_len
    }

    fn get(&self, index: usize) -> Result<Self::Page<'_>, Self::Err> {
        let binding = self.pages.read();
        let page = binding[index].read().to_vec().into_boxed_slice();
        let usage = self.usage.read().get(&index).unwrap().clone();
        usage
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
                if v >= 0 {
                    Some(v + 1)
                } else {
                    None
                }
            })
            .expect("can not get immutable usage");

        Ok(SharedPage {
            buffer: page,
            _lf: Default::default(),
            usage,
        })
    }

    fn get_mut(&self, index: usize) -> Result<Self::PageMut<'_>, Self::Err> {
        let binding = self.pages.read();
        let arc = &binding[index];
        let page = arc.read().to_vec().into_boxed_slice();
        let usage = self.usage.read().get(&index).unwrap().clone();
        usage
            .compare_exchange(0, -1, Ordering::SeqCst, Ordering::Relaxed)
            .expect("can not get mutable usage");
        Ok(SharedPageMut {
            lock: arc.clone(),
            buffer: Mad::new(page),
            _lf: Default::default(),
            usage,
        })
    }

    fn new(&self) -> Result<(Self::PageMut<'_>, usize), Self::Err> {
        let index = self.pages.read().len();
        self.pages
            .write()
            .push(RwLock::new(vec![0; self.page_len].into_boxed_slice()).into());
        self.usage.write().insert(index, Default::default());
        let emit = self.get_mut(index)?;
        Ok((emit, index))
    }

    fn free(&self, index: usize) -> Result<(), Self::Err> {
        let mut pages = self.pages.write();
        let new = RwLock::new(vec![0; self.page_len].into_boxed_slice()).into();
        let old = std::mem::replace(&mut pages[index], new);
        let mut buff = old.write();
        buff.fill(0);
        dbg!(&buff);
        Ok(())
    }

    fn len(&self) -> usize {
        self.pages.read().iter().fuse().count()
    }

    fn reserved(&self) -> usize {
        self.pages.read().len() * self.page_len
    }
}

/// A simple shared page
#[derive(Debug)]
pub struct SharedPage<'a> {
    buffer: Box<[u8]>,
    usage: Arc<AtomicI32>,
    _lf: PhantomData<&'a ()>,
}

impl AsRef<[u8]> for SharedPage<'_> {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref()
    }
}

impl<'a> Page<'a> for SharedPage<'a> {
    fn len(&self) -> usize {
        self.as_ref().len()
    }
    fn as_slice(&self) -> &[u8] {
        &*self.buffer
    }
}

impl Drop for SharedPage<'_> {
    fn drop(&mut self) {
        self.usage.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A simple shared page
#[derive(Debug)]
pub struct SharedPageMut<'a> {
    lock: Arc<RwLock<Box<[u8]>>>,
    buffer: Mad<Box<[u8]>>,
    usage: Arc<AtomicI32>,
    _lf: PhantomData<&'a ()>,
}

impl AsRef<[u8]> for SharedPageMut<'_> {
    fn as_ref(&self) -> &[u8] {
        &*self.buffer
    }
}

impl AsMut<[u8]> for SharedPageMut<'_> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.buffer.to_mut()
    }
}

impl<'a> Page<'a> for SharedPageMut<'a> {
    fn len(&self) -> usize {
        self.as_ref().len()
    }
    fn as_slice(&self) -> &[u8] {
        self.as_ref()
    }
}

impl<'a> PageMut<'a> for SharedPageMut<'a> {
    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.as_mut()
    }
}

impl<'a> Drop for SharedPageMut<'a> {
    fn drop(&mut self) {
        let mut guard = self.lock.write();
        if self.buffer.is_dirty() {
            guard.copy_from_slice(&*self.buffer);
        }
        self.usage
            .compare_exchange(-1, 0, Ordering::SeqCst, Ordering::Relaxed)
            .expect("Usage reset failed");
    }
}
