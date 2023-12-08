//! The dead simple engine is, well, dead simple.
//!
//! It stores tables data to a file by the same name, serializing and deserializing to json

use std::fs::File;
use std::io::Read;
use crate::data::Row;
use crate::in_memory::InMemory;
use crate::rows::{KeyIndex, Rows, RowsExt};
use crate::storage_engine::{DynamicTable, StorageError};
use crate::table_schema::TableSchema;

pub struct DeadSimple {
    in_memory: InMemory,
    file: File
}

impl DynamicTable for DeadSimple {
    fn schema(&self) -> &TableSchema {
        self.in_memory.schema()
    }

    fn insert(&self, row: &Row) -> Result<(), StorageError> {
        self.in_memory.insert(row)?;

        self.file.set_len(0)?;
        let all = self.in_memory.read(&KeyIndex::all())?.into_iter()
            .collect::<Vec<_>>();
        serde_json::to_writer(&self.file, &all).map_err(|e| StorageError::custom(e))?;

        Ok(())
    }

    fn read(&self, key: &KeyIndex) -> Result<Box<dyn Rows>, StorageError> {
        self.in_memory.read(key)
    }

    fn update(&self, row: &Row) -> Result<(), StorageError> {
        todo!()
    }

    fn delete(&self, key: &KeyIndex) -> Result<Box<dyn Rows>, StorageError> {
        todo!()
    }
}