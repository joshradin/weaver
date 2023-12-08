//! A window over some rows

use std::iter::FromFn;
use crate::data::{OwnedRow, Row};
use crate::key::KeyData;
use std::ops::Bound;

#[derive(Debug, Clone)]
pub struct KeyIndex {
    kind: KeyIndexKind,
    limit: Option<usize>,
    offset: Option<usize>,
}

impl KeyIndex {

    pub fn all() -> Self {
        Self::new(KeyIndexKind::All, None, None)
    }

    /// Creates a new key index
    pub fn new(
        kind: KeyIndexKind,
        limit: impl Into<Option<usize>>,
        offset: impl Into<Option<usize>>,
    ) -> Self {
        Self {
            kind,
            limit: limit.into(),
            offset: offset.into(),
        }
    }

    /// Gets the key index kind
    pub fn kind(&self) -> &KeyIndexKind {
        &self.kind
    }

    /// Gets a limit
    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// Gets the offset
    pub fn offset(&self) -> Option<usize> {
        self.offset
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum KeyIndexKind {
    All,
    Range {
        low: Bound<KeyData>,
        high: Bound<KeyData>,
    },
    One(KeyData),
}

/// A rows result
pub trait Rows {
    fn next(&mut self) -> Option<OwnedRow>;
}

impl Rows for Box<dyn Rows> {
    fn next(&mut self) -> Option<OwnedRow> {
        (**self).next()
    }
}

impl<R : Rows + 'static> RowsExt for R{
    type IntoIter = Box<dyn Iterator<Item=OwnedRow>>;

    fn into_iter(mut self) -> Self::IntoIter {
        Box::new(std::iter::from_fn(move || {
            self.next()
        }))
    }
}
pub trait RowsExt : Rows {
    type IntoIter: Iterator<Item=OwnedRow>;
    fn into_iter(self) -> Self::IntoIter;
}
