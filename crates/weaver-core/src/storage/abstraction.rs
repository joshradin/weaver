//! Creates abstractions that are used to build better storage-backed data structures

use std::convert::Infallible;
use std::slice::SliceIndex;

/// Allows for getting pages of a fix size
pub trait Paged {
    type Page<'a> : Page<'a>
        where Self : 'a;
    type Err;


    /// Gets the size of pages that are allocated by this paged type
    fn page_size(&self) -> usize;

    /// Gets the page at a given index, returns an error if not possible for
    /// whatever reason
    fn get(&self, index: usize) -> Result<Self::Page<'_>, Self::Err>;


    /// Creates a new page, returning mutable reference to the page and the index
    /// it was created at
    fn new(&mut self) -> Result<(Self::Page<'_>, usize), Self::Err>;

    /// Removes the page at a given index, freeing the space it's allocated with.
    ///
    /// If removed, the same index can be reused later on.
    ///
    /// Because of potential re-use, pages should be zeroed after removal.
    fn remove(&mut self, index: usize) -> Result<(), Self::Err>;
}

/// Provides a view of page
pub trait Page<'a>
    where Self : 'a + Sized
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
    fn get<I>(&self, index: I) -> Option<& <I as SliceIndex<[u8]>>::Output>
        where I : SliceIndex<[u8]> {
        self.as_slice().get(index)
    }

    /// Returns a reference to an element or a subslice depending on the type of index
    fn get_mut<I>(&mut self, index: I) -> Option<& mut <I as SliceIndex<[u8]>>::Output>
        where I : SliceIndex<[u8]> {
        self.as_mut_slice().get_mut(index)
    }

}


///
pub trait HeaderPaged : Paged {



}