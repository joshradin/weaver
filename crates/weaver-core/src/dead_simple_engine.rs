//! The dead simple engine is, well, dead simple.
//!
//! It stores tables data to a file by the same name, serializing and deserializing to json

use crate::data::row::Row;
use crate::dynamic_table::{Col, DynamicTable, StorageError};
use crate::in_memory_table::InMemoryTable;
use crate::rows::{KeyIndex, Rows};
use crate::table_schema::TableSchema;
use std::fs::File;
use std::io::Read;
use crate::error::Error;
use crate::tx::Tx;

pub struct DeadSimple {
    in_memory: InMemoryTable,
    file: File,
}

impl DynamicTable for DeadSimple {
    fn schema(&self) -> &TableSchema {
        self.in_memory.schema()
    }

    fn auto_increment(&self, col: Col) -> i64 {
        todo!()
    }

    fn next_row_id(&self) -> i64 {
        self.in_memory.next_row_id()
    }


    fn insert(&self, tx: &Tx, row: Row) -> Result<(), Error> {
        self.in_memory.insert(tx, row)?;
        Ok(())
    }

    fn read<'tx, 'table: 'tx>(&'table self, tx: &'tx Tx, key: &KeyIndex) -> Result<Box<dyn Rows + 'tx>, Error> {
        self.in_memory.read(tx, key)
    }


    fn update(&self, tx: &Tx, row: Row) -> Result<(), crate::error::Error> {
        todo!()
    }

    fn delete(&self, tx: &Tx, key: &KeyIndex) -> Result<Box<dyn Rows>, Error> {
        todo!()
    }
}
