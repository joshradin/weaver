//! A shared table allows for sharing tables over multiple threads

use crate::data::row::Row;
use crate::dynamic_table::{Col, DynamicTable, HasSchema, Table};
use crate::error::Error;
use crate::monitoring::{Monitor, Monitorable};
use crate::rows::{KeyIndex, Rows};
use crate::storage::tables::table_schema::TableSchema;
use crate::tx::Tx;
use std::sync::Arc;

/// A shared table
#[derive(Debug, Clone)]
pub struct SharedTable(Arc<Table>);

impl SharedTable {
    /// A shared table
    pub fn new(table: Table) -> Self {
        Self(Arc::new(table))
    }
}

impl HasSchema for SharedTable {
    fn schema(&self) -> &TableSchema {
        self.0.schema()
    }
}

impl Monitorable for SharedTable {
    fn monitor(&self) -> Box<dyn Monitor> {
        self.0.monitor()
    }
}

impl DynamicTable for SharedTable {
    fn auto_increment(&self, col: Col) -> i64 {
        self.0.auto_increment(col)
    }

    fn next_row_id(&self) -> i64 {
        self.0.next_row_id()
    }

    fn insert(&self, tx: &Tx, row: Row) -> Result<(), Error> {
        self.0.insert(tx, row)
    }

    fn read<'tx, 'table: 'tx>(
        &'table self,
        tx: &'tx Tx,
        key: &KeyIndex,
    ) -> Result<Box<dyn Rows<'tx> + 'tx + Send>, Error> {
        self.0.read(tx, key)
    }

    fn update(&self, tx: &Tx, row: Row) -> Result<(), Error> {
        self.0.update(tx, row)
    }

    fn delete(&self, tx: &Tx, key: &KeyIndex) -> Result<Box<dyn Rows>, Error> {
        self.0.delete(tx, key)
    }
}
