//! Mark a value using dirty

use std::borrow::Borrow;
use std::fmt::{Display, Formatter};
use std::ops::Deref;

///  A type that allows for easily checking if a type is dirty
///
/// Stands for "Mark As Dirty"
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Hash, Default)]
pub struct Mad<T> {
    value: T,
    dirty: bool,
}

impl<T> Mad<T> {
    /// Creates a new mark-as-dirty with a given value
    pub fn new(value: T) -> Self {
        Self {
            value,
            dirty: false,
        }
    }

    /// Makes this Mad dirty, returning a mutable value
    pub fn to_mut(&mut self) -> &mut T {
        self.dirty = true;
        &mut self.value
    }

    /// Extracts the owned data.
    pub fn into_value(self) -> T {
        self.value
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }
}

impl<T> From<T> for Mad<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T> AsRef<T> for Mad<T> {
    fn as_ref(&self) -> &T {
        &self.value
    }
}

impl<T> Deref for Mad<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<T> Borrow<T> for Mad<T>
where
    T: ToOwned,
{
    fn borrow(&self) -> &T {
        self.as_ref()
    }
}

impl<T: Display> Display for Mad<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use crate::common::track_dirty::Mad;
    use std::collections::HashMap;

    #[test]
    fn mad_in_hashmap() {
        let mut hashset = HashMap::<u32, u32>::new();
        hashset.insert(4, 16);
        let &v = hashset.get(&Mad::new(4)).unwrap();
        assert_eq!(v, 16);
    }
}
