//! The dead simple engine is, well, dead simple.
//!
//! It stores tables data to a file by the same name, serializing and deserializing to json

use crate::data::Row;
use crate::dynamic_table::{Col, DynamicTable, StorageError};
use crate::in_memory_table::InMemory;
use crate::rows::{KeyIndex, Rows, RowsExt};
use crate::table_schema::TableSchema;
use std::fs::File;
use std::io::Read;
use crate::error::Error;

pub struct DeadSimple {
    in_memory: InMemory,
    file: File,
}

impl DynamicTable for DeadSimple {
    fn schema(&self) -> &TableSchema {
        self.in_memory.schema()
    }

    fn auto_increment(&self, col: Col) -> i64 {
        todo!()
    }


    fn insert(&self, row: Row) -> Result<(), Error> {
        self.in_memory.insert(row)?;

        self.file.set_len(0)?;
        let all = self
            .in_memory
            .read(&KeyIndex::all(self.schema().primary_key()?.name()))?
            .into_iter()
            .collect::<Vec<_>>();
        serde_json::to_writer(&self.file, &all).map_err(|e| StorageError::custom(e))?;

        Ok(())
    }

    fn read(&self, key: &KeyIndex) -> Result<Box<dyn Rows>, Error> {
        self.in_memory.read(key)
    }

    fn update(&self, row: Row) -> Result<(), crate::error::Error> {
        todo!()
    }

    fn delete(&self, key: &KeyIndex) -> Result<Box<dyn Rows>, Error> {
        todo!()
    }
}
