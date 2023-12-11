//! An opaque type doesn't allow any access to it's internal element. Used for
//! Maintaining lifetimes of values without allowing for access.

use std::fmt::{Debug, Formatter};

/// An opaque value permanently takes ownership of a value, and allows for no internal access
pub struct Opaque<T>(T);

impl<T> Opaque<T> {

    /// Creates a new opaque value
    pub fn new(value: T) -> Self {
        Self(value)
    }
}

impl<T> From<T> for Opaque<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T> Debug for Opaque<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Opaque").finish_non_exhaustive()
    }
}
