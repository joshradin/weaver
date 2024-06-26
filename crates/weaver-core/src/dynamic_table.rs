//! Defines storage engines

use std::fmt::{Debug, Display, Formatter};
use std::io;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::data::row::Row;

use crate::error::WeaverError;
use crate::monitoring::Monitorable;
use crate::rows::{KeyIndex, Rows};
use crate::storage::tables::bpt_file_table::B_PLUS_TREE_FILE_KEY;
use crate::storage::tables::in_memory_table::IN_MEMORY_KEY;

use crate::storage::tables::table_schema::TableSchema;
use crate::tx::Tx;

/// A column within a table
pub type Col<'a> = &'a str;

/// An owned column reference
pub type OwnedCol = String;

pub static ROW_ID_COLUMN: Col<'static> = "@@ROW_ID";

/// The main storage engine trait. Storage engines are provided
/// per table.
pub trait DynamicTable: Monitorable + HasSchema + Send + Sync {
    /// The next auto-incremented value for a given column
    ///
    /// Auto incremented values be always be unique.
    fn auto_increment(&self, col: Col) -> i64;

    /// Gets the next row id
    fn next_row_id(&self) -> i64;

    /// Commit any data modified during a transaction
    ///
    /// Only works on supporting tables.
    fn commit(&self, _tx: &Tx) {}

    /// Rollback the current transaction.
    ///
    /// Only works on supporting tables.
    fn rollback(&self, _tx: &Tx) {}

    /// Create a row. Fails if row's primary key is already present
    fn insert(&self, tx: &Tx, row: Row) -> Result<(), WeaverError>;

    /// Get by a key
    fn read<'tx, 'table: 'tx>(
        &'table self,
        tx: &'tx Tx,
        key: &KeyIndex,
    ) -> Result<Box<dyn Rows<'tx> + 'tx + Send>, WeaverError>;

    /// Shortcut for all rows
    fn all<'tx, 'table: 'tx>(
        &'table self,
        tx: &'tx Tx,
    ) -> Result<Box<dyn Rows<'tx> + 'tx + Send>, WeaverError> {
        self.read(tx, &self.schema().full_index()?)
    }

    /// Gets a size estimate for a given key index, returning an approximate amount
    /// of rows retrievable by said key. This is meant to be a quick operation, and not the exact
    /// number of rows stored in an index.
    ///
    /// Since this is approximate-based, there will also be no transaction support.
    fn size_estimate(&self, key_index: &KeyIndex) -> Result<u64, WeaverError>;

    /// Update an existing row. Fails if no row with primary key is already present
    fn update(&self, tx: &Tx, row: Row) -> Result<(), WeaverError>;

    /// Delete by key
    fn delete(&self, tx: &Tx, key: &KeyIndex) -> Result<Box<dyn Rows>, WeaverError>;
}

pub trait HasSchema {
    /// Gets the defining schema
    fn schema(&self) -> &TableSchema;
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error(transparent)]
    IoError(#[from] io::Error),
    #[error(transparent)]
    Custom(Box<dyn std::error::Error + Send + Sync>),
}

impl StorageError {
    /// Create a custom storage error
    pub fn custom<E: std::error::Error + Send + Sync + 'static>(custom: E) -> Self {
        Self::Custom(Box::new(custom))
    }
}

pub type Table = Box<dyn DynamicTable>;

impl Debug for Table {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Table")
            .field("schema", self.schema())
            .finish()
    }
}
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize, Hash)]
pub struct EngineKey(String);

impl EngineKey {
    pub fn new<S: AsRef<str>>(s: S) -> Self {
        Self(s.as_ref().to_string())
    }

    /// The "basic" engine key
    pub fn basic() -> Self {
        Self::new(B_PLUS_TREE_FILE_KEY)
    }

    /// An in memory table key. These are for volatile tables
    pub fn in_memory() -> Self {
        Self::new(IN_MEMORY_KEY)
    }
}

impl Display for EngineKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for EngineKey {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

#[derive(Debug, Error)]
pub enum OpenTableError {}
