//! Creates abstractions that are used to build better storage-backed data structures

use crate::common::track_dirty::Mad;
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockUpgradableReadGuard};
use std::collections::HashMap;
use std::convert::Infallible;
use std::error::Error;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::slice::SliceIndex;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::storage::{ReadResult, StorageBackedData, WriteResult};

/// Allows for getting pages of a fix size
pub trait Paged {
    type Page: Page;
    type Err: Error + 'static;

    /// Gets the size of pages that are allocated by this paged type
    fn page_size(&self) -> usize;

    /// Gets the page at a given index, returns an error if not possible for
    /// whatever reason
    fn get(&self, index: usize) -> Result<Self::Page, Self::Err>;

    /// Creates a new page, returning mutable reference to the page and the index
    /// it was created at
    fn new(&self) -> Result<(Self::Page, usize), Self::Err>;

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

    fn iter(&self) -> impl Iterator<Item = Result<(Self::Page, usize), Self::Err>> + '_ {
        (0..self.len())
            .into_iter()
            .map(|index| self.get(index).map(|page| (page, index)))
    }
}

/// Provides a view of page
pub trait Page
where
    Self: Sized,
{
    /// Gets the total length of the page
    fn len(&self) -> usize;

    /// Gets this page as a slice
    fn as_slice(&self) -> &[u8];

    /// Gets a mutable reference to this page as slice
    fn as_mut_slice(&mut self) -> &mut [u8];

    /// Returns a raw pointer to the beginning of the page.
    ///
    /// Usage of this pointer can only used in unsafe blocks
    fn as_ptr(&self) -> *const u8 {
        self.as_slice().as_ptr()
    }

    /// Returns a mutable raw pointer to the beginning of the page
    ///
    /// Usage of this pointer can only used in unsafe mechanisms
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.as_mut_slice().as_mut_ptr()
    }

    /// Returns a reference to an element or a subslice depending on the type of index
    fn get<I>(&self, index: I) -> Option<&<I as SliceIndex<[u8]>>::Output>
    where
        I: SliceIndex<[u8]>,
    {
        self.as_slice().get(index)
    }

    /// Returns a reference to an element or a subslice depending on the type of index
    fn get_mut<I>(&mut self, index: I) -> Option<&mut <I as SliceIndex<[u8]>>::Output>
    where
        I: SliceIndex<[u8]>,
    {
        self.as_mut_slice().get_mut(index)
    }
}

impl<'a, P: Page> Page for &'a mut P {
    fn len(&self) -> usize {
        (**self).len()
    }

    fn as_slice(&self) -> &[u8] {
        (**self).as_slice()
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        (**self).as_mut_slice()
    }
}

/// A page with a header
pub trait PageWithHeader: Page {
    type Header: Clone + PartialEq + StorageBackedData;

    fn header(&self) -> ReadResult<Self::Header>;
    fn set_header(&mut self, header: Self::Header) -> WriteResult<()>;

    /// Gets the length of the header
    fn header_len(&self) -> usize;

    /// Gets the length of the body
    fn body_len(&self) -> usize {
        self.len() - self.header_len()
    }
}

/// Provides a simple implementation of a split page with a header
#[derive(Debug)]
pub struct SplitPage<P, Header>
where
    P: Page,
    Header: Clone + PartialEq + StorageBackedData,
{
    page: P,
    header_len: usize,
    _header: PhantomData<Header>,
}

impl<P, Header> SplitPage<P, Header>
where
    P: Page,
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

impl<P, Header> PageWithHeader for SplitPage<P, Header>
where
    P: Page,
    Header: Clone + PartialEq + StorageBackedData<Owned = Header>,
{
    type Header = Header;

    fn header(&self) -> ReadResult<Self::Header> {
        Header::read(&self.page.as_slice()[..self.header_len])
    }

    fn set_header(&mut self, header: Self::Header) -> WriteResult<()> {
        let reference = &mut self.page.as_mut_slice()[..self.header_len];
        let result = header.write::<'_>(reference).map(move |_| ());
        drop(header);
        result
    }

    fn header_len(&self) -> usize {
        self.header_len
    }
}

impl<P, Header> Page for SplitPage<P, Header>
where
    P: Page,
    Header: Clone + PartialEq + StorageBackedData,
{
    fn len(&self) -> usize {
        self.page.len()
    }

    fn as_slice(&self) -> &[u8] {
        &self.page.as_slice()[self.header_len..]
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.page.as_mut_slice()[self.header_len..]
    }
}

/// An implementation over pages
#[derive(Debug)]
pub struct VecPaged {
    pages: RwLock<Vec<Arc<RwLock<Box<[u8]>>>>>,
    usage: RwLock<HashMap<usize, AtomicBool>>,
    page_len: usize,
}

impl VecPaged {
    /// Creates a new vec-paged with a given page len
    pub fn new(page_len: usize) -> Self {
        Self {
            pages: Default::default(),
            usage: Default::default(),
            page_len,
        }
    }
}

impl Paged for VecPaged {
    type Page = SharedPage;
    type Err = Infallible;

    fn page_size(&self) -> usize {
        self.page_len
    }

    fn get(&self, index: usize) -> Result<Self::Page, Self::Err> {
        let binding = self.pages.read();
        let page = &binding[index];
        let guard = (*page).clone();
        let buffer = Mad::new(guard.read().clone());
        Ok(SharedPage {
            lock: guard,
            buffer,
        })
    }

    fn new(&self) -> Result<(Self::Page, usize), Self::Err> {
        let index = self.pages.read().len();
        self.pages
            .write()
            .push(RwLock::new(vec![0; self.page_len].into_boxed_slice()).into());
        let emit = self.get(index)?;
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
pub struct SharedPage {
    lock: Arc<RwLock<Box<[u8]>>>,
    buffer: Mad<Box<[u8]>>,
}

impl AsRef<[u8]> for SharedPage {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref()
    }
}

impl AsMut<[u8]> for SharedPage {
    fn as_mut(&mut self) -> &mut [u8] {
        self.buffer.to_mut().as_mut()
    }
}

impl Page for SharedPage {
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn as_slice(&self) -> &[u8] {
        self.as_ref()
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.as_mut()
    }
}

impl Drop for SharedPage {
    fn drop(&mut self) {
        if self.buffer.is_dirty() {
            println!("copying {} bytes", self.buffer.len());
            self.lock.write().copy_from_slice(self.buffer.as_ref());
        }
    }
}
