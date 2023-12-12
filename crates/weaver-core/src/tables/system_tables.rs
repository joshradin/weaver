//! System tables provide information about the state of the system, and only exist as a "view" of
//! the internal state of the weaver

use crate::data::row::Row;
use crate::db::concurrency::DbSocket;
use crate::dynamic_table::{Col, DynamicTable, StorageEngineFactory, Table};
use crate::error::Error;
use crate::rows::{KeyIndex, Rows};
use crate::tables::table_schema::TableSchema;
use crate::tx::Tx;

/// Provide a system table
#[derive(Debug)]
pub struct SystemTable {
    table_schema: TableSchema,
}

impl DynamicTable for SystemTable {
    fn schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn auto_increment(&self, col: Col) -> i64 {
        unimplemented!("system tables shouldn't need auto increments")
    }

    fn next_row_id(&self) -> i64 {
        unimplemented!("system tables shouldn't need row_ids")
    }

    fn insert(&self, tx: &Tx, row: Row) -> Result<(), Error> {
        unimplemented!("can not insert into a system table")
    }

    fn read<'tx, 'table: 'tx>(&'table self, tx: &'tx Tx, key: &KeyIndex) -> Result<Box<dyn Rows + 'tx>, Error> {
        todo!()
    }

    fn update(&self, tx: &Tx, row: Row) -> Result<(), Error> {
        unimplemented!("can not update information in a system table")
    }

    fn delete(&self, tx: &Tx, key: &KeyIndex) -> Result<Box<dyn Rows>, Error> {
        unimplemented!("can not delete data from a system table")
    }
}

#[derive(Debug)]
pub(crate) struct SystemTableFactory {
    connection: DbSocket
}

impl SystemTableFactory {

    /// Creates system tables using an actual, live connection
    pub fn new(connection: DbSocket) -> Self {
        Self { connection }
    }
}

impl StorageEngineFactory for SystemTableFactory {
    fn open(&self, schema: &TableSchema) -> Result<Table, Error> {
        todo!()
    }
}
