//! A window over some rows

use crate::key::KeyData;
use std::ops::Bound;
use crate::data::row::Row;

#[derive(Debug, Clone)]
pub struct KeyIndex {
    key: String,
    kind: KeyIndexKind,
    limit: Option<usize>,
    offset: Option<usize>,
}

impl KeyIndex {
    pub fn all(key: impl AsRef<str>) -> Self {
        Self::new(key, KeyIndexKind::All, None, None)
    }

    /// Creates a new key index
    pub fn new(
        key: impl AsRef<str>,
        kind: KeyIndexKind,
        limit: impl Into<Option<usize>>,
        offset: impl Into<Option<usize>>,
    ) -> Self {
        Self {
            key: key.as_ref().to_string(),
            kind,
            limit: limit.into(),
            offset: offset.into(),
        }
    }

    pub fn key_name(&self) -> &str {
        &self.key
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
pub trait Rows<'t> {
    fn next(&mut self) -> Option<Row<'t>>;
}

impl<'t> Rows<'t> for Box<dyn Rows<'t> + 't> {
    fn next(&mut self) -> Option<Row<'t>> {
        (**self).next()
    }
}

// impl<'a, R: Rows<'a> + 'static> RowsExt<'a> for R {
//     type IntoIter = Box<dyn Iterator<Item = OwnedRow>>;
//
//     fn into_iter(mut self) -> Self::IntoIter {
//         Box::new(std::iter::from_fn(move || self.next()))
//     }
// }
// pub trait RowsExt<'a>: Rows<'a> {
//     type IntoIter: Iterator<Item = OwnedRow>;
//     fn into_iter(self) -> Self::IntoIter;
// }
