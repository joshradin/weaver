//! Defines storage engines

use crate::data::row::Row;
use crate::error::Error;
use crate::rows::{KeyIndex, Rows};
use crate::tables::table_schema::TableSchema;
use crate::tx::Tx;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};
use std::io;
use thiserror::Error;

/// A column within a table
pub type Col<'a> = &'a str;

/// A column within a table
pub type TableCol = (String, String, String);

/// An owned column reference
pub type OwnedCol = String;

pub static ROW_ID_COLUMN: Col<'static> = "@@ROW_ID";

/// The main storage engine trait. Storage engines are provided
/// per table.
pub trait DynamicTable: Send + Sync {
    /// Gets the defining schema
    fn schema(&self) -> &TableSchema;

    /// The next auto-incremented value for a given column
    ///
    /// Auto incremented values be always be unique.
    fn auto_increment(&self, col: Col) -> i64;

    /// Gets the next row id
    fn next_row_id(&self) -> i64;

    /// Commit any data modified during a transaction
    ///
    /// Only works on supporting tables.
    fn commit(&self, tx: &Tx) {}

    /// Rollback the current transaction.
    ///
    /// Only works on supporting tables.
    fn rollback(&self, tx: &Tx) {}

    /// Create a row. Fails if row's primary key is already present
    fn insert(&self, tx: &Tx, row: Row) -> Result<(), Error>;

    /// Get by a key
    fn read<'tx, 'table: 'tx>(
        &'table self,
        tx: &'tx Tx,
        key: &KeyIndex,
    ) -> Result<Box<dyn Rows<'tx> + 'tx + Send>, Error>;

    /// Shortcut for all rows
    fn all<'tx, 'table: 'tx>(
        &'table self,
        tx: &'tx Tx,
    ) -> Result<Box<dyn Rows<'tx> + 'tx + Send>, Error> {
        self.read(tx, &self.schema().full_index()?)
    }

    /// Update an existing row. Fails if no row with primary key is already present
    fn update(&self, tx: &Tx, row: Row) -> Result<(), Error>;

    /// Delete by key
    fn delete(&self, tx: &Tx, key: &KeyIndex) -> Result<Box<dyn Rows>, Error>;
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

pub trait StorageEngineFactory: Send + Sync {
    fn open(&self, schema: &TableSchema) -> Result<Table, Error>;
}

impl<F: Fn(&TableSchema) -> Result<Table, Error> + Send + Sync> StorageEngineFactory for F {
    fn open(&self, schema: &TableSchema) -> Result<Table, Error> {
        (self)(schema)
    }
}

pub fn storage_engine_factory<
    F: Fn(&TableSchema) -> Result<Table, Error> + 'static + Send + Sync,
>(
    func: F,
) -> Box<dyn StorageEngineFactory> {
    Box::new(func)
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize, Hash)]
pub struct EngineKey(String);

impl EngineKey {
    pub fn new<S: AsRef<str>>(s: S) -> Self {
        Self(s.as_ref().to_string())
    }
}

pub const IN_MEMORY_KEY: &'static str = "IN_MEMORY";
pub const SYSTEM_TABLE_KEY: &'static str = "SYSTEM_TABLE";

impl EngineKey {
    pub fn all() -> impl Iterator<Item = EngineKey> {
        [
            EngineKey::new(IN_MEMORY_KEY),
            EngineKey::new(SYSTEM_TABLE_KEY),
        ]
        .into_iter()
    }
}

impl AsRef<str> for EngineKey {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

#[derive(Debug, Error)]
pub enum OpenTableError {}
