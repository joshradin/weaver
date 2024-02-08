//! An in-memory storage engine

use crate::data::row::Row;
use crate::dynamic_table::{Col, DynamicTable, HasSchema};
use crate::error::Error;
use crate::rows::{KeyIndex, Rows};
use crate::storage::PagedVec;
use crate::tables::table_schema::TableSchema;
use crate::tables::unbuffered_table::UnbufferedTable;
use crate::tx::{Tx, TX_ID_COLUMN};
use derive_more::Deref;

#[derive(Debug, Deref)]
pub struct InMemoryTable(UnbufferedTable<PagedVec>);

impl InMemoryTable {
    pub fn new(schema: TableSchema) -> Result<Self, Error> {
        Ok(InMemoryTable(UnbufferedTable::new(
            schema,
            PagedVec::default(),
            true,
        )?))
    }

    pub fn non_transactional(schema: TableSchema) -> Result<Self, Error> {
        Ok(InMemoryTable(UnbufferedTable::new(
            schema,
            PagedVec::default(),
            false,
        )?))
    }

    /// Creates an in-memory table from a set of rows and a given schema
    pub fn from_rows<'t>(mut schema: TableSchema, mut rows: impl Rows<'t>) -> Result<Self, Error> {
        if let Some(pos) = schema
            .sys_columns()
            .iter()
            .position(|col| &col.name() == &TX_ID_COLUMN)
        {
            schema.remove_sys_column(pos)?;
        }
        let mut table = Self::non_transactional(schema)?;
        let ref tx = Tx::default();
        while let Some(row) = rows.next() {
            table.insert(tx, row)?;
        }
        Ok(table)
    }
}

impl DynamicTable for InMemoryTable {
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

impl HasSchema for InMemoryTable {
    fn schema(&self) -> &TableSchema {
        self.0.schema()
    }
}

pub const IN_MEMORY_KEY: &'static str = "IN_MEMORY";
