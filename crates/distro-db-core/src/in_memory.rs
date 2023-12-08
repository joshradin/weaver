//! An in-memory storage engine

use crate::data::{OwnedRow, Row};
use crate::key::KeyData;
use crate::rows::{KeyIndex, Rows};
use crate::storage_engine::{DynamicTable, StorageError};
use std::collections::{BTreeMap, HashMap};
use parking_lot::RwLock;
use crate::table_schema::TableSchema;

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash, Default)]
struct RowId(u64);

/// An in memory table
#[derive(Debug)]
pub struct InMemory {
    schema: TableSchema,
    main_buffer: RwLock<BTreeMap<KeyData, OwnedRow>>,
}

impl InMemory {
    pub fn new(schema: TableSchema) -> Self {
        Self { schema, main_buffer: RwLock::new(BTreeMap::new()) }
    }

    /// Gets the table schema
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }
}

impl DynamicTable for InMemory {
    fn schema(&self) -> &TableSchema {
        &self.schema
    }
    fn insert(&self, row: &Row) -> Result<(), StorageError> {
        todo!()
    }

    fn read(&self, key: &KeyIndex) -> Result<Box<dyn Rows>, StorageError> {
        todo!()
    }

    fn update(&self, row: &Row) -> Result<(), StorageError> {
        todo!()
    }

    fn delete(&self, key: &KeyIndex) -> Result<Box<dyn Rows>, StorageError> {
        todo!()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InMemoryError {}
