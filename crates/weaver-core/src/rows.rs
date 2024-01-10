//! A window over some rows

use crate::data::row::{OwnedRow, Row};
use crate::key::KeyData;
use crate::tables::table_schema::TableSchema;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::ops::Bound;

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
    fn schema(&self) -> &TableSchema;
    fn next(&mut self) -> Option<Row<'t>>;
}

pub trait RowsExt<'t>: Rows<'t> {
    fn to_owned(mut self) -> OwnedRows
    where
        Self: Sized,
    {
        let mut rows = VecDeque::new();
        while let Some(row) = self.next() {
            rows.push_back(row.to_owned());
        }

        OwnedRows {
            schema: self.schema().clone(),
            rows,
        }
    }
}

impl<'t, R: Rows<'t>> RowsExt<'t> for R {}

impl Debug for Box<dyn for<'a> Rows<'a> + Send> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoxedRow").finish_non_exhaustive()
    }
}

impl<'t> Rows<'t> for Box<dyn Rows<'t> + Send + 't> {
    fn schema(&self) -> &TableSchema {
        (**self).schema()
    }

    fn next(&mut self) -> Option<Row<'t>> {
        (**self).next()
    }
}
#[derive(Debug)]
pub struct OwnedRows {
    schema: TableSchema,
    rows: VecDeque<OwnedRow>,
}

impl OwnedRows {
    pub fn new<I>(schema: TableSchema, rows: I) -> Self
    where
        I: IntoIterator<Item = OwnedRow>,
    {
        Self {
            schema,
            rows: rows.into_iter().collect(),
        }
    }
}

impl<'t> Rows<'t> for OwnedRows {
    fn schema(&self) -> &TableSchema {
        &self.schema
    }

    fn next(&mut self) -> Option<Row<'t>> {
        self.rows.pop_front().map(|owned| owned.into())
    }
}

#[derive(Debug)]
pub struct DefaultRows<'a> {
    schema: TableSchema,
    rows: VecDeque<Row<'a>>,
}

impl<'a> Rows<'a> for DefaultRows<'a> {
    fn schema(&self) -> &TableSchema {
        &self.schema
    }

    fn next(&mut self) -> Option<Row<'a>> {
        self.rows.pop_front()
    }
}
