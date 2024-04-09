//! Read only provides read only access to a value, regardless of ownership

use std::ops::Deref;

/// Provides read-only access to some value.
///
/// Can not access the inner element with a mutable reference, even if this value is directly owned
/// or a mutable reference to the read only struct is owned.
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Default)]
pub struct ReadOnly<T>(T);

impl<T> ReadOnly<T> {
    /// Creates a new, read-only value
    pub fn new(value: T) -> Self {
        Self(value)
    }
}

impl<T> From<T> for ReadOnly<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T> AsRef<T> for ReadOnly<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl<T> Deref for ReadOnly<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
