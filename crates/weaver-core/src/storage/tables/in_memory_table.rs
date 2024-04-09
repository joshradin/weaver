//! An in-memory storage engine

use crate::data::row::Row;
use crate::db::core::WeaverDbCore;
use crate::dynamic_table::{Col, DynamicTable, HasSchema, Table};
use crate::dynamic_table_factory::DynamicTableFactory;
use crate::error::WeaverError;
use crate::monitoring::{monitor_fn, Monitor, Monitorable};
use crate::rows::{KeyIndex, Rows};
use crate::storage::tables::table_schema::TableSchema;
use crate::storage::tables::unbuffered_table::UnbufferedTable;
use crate::storage::VecPager;
use crate::tx::{Tx, TX_ID_COLUMN};
use derive_more::Deref;

#[derive(Debug, Deref)]
pub struct InMemoryTable(UnbufferedTable<VecPager>);

impl InMemoryTable {
    pub fn new(schema: TableSchema) -> Result<Self, WeaverError> {
        Ok(InMemoryTable(UnbufferedTable::new(
            schema,
            VecPager::default(),
            true,
        )?))
    }

    pub fn non_transactional(schema: TableSchema) -> Result<Self, WeaverError> {
        Ok(InMemoryTable(UnbufferedTable::new(
            schema,
            VecPager::default(),
            false,
        )?))
    }

    /// Creates an in-memory table from a set of rows and a given schema
    pub fn from_rows<'t>(mut schema: TableSchema, mut rows: impl Rows<'t>) -> Result<Self, WeaverError> {
        if let Some(pos) = schema
            .sys_columns()
            .iter()
            .position(|col| &col.name() == &TX_ID_COLUMN)
        {
            schema.remove_sys_column(pos)?;
        }
        let table = Self::non_transactional(schema)?;
        let ref tx = Tx::default();
        while let Some(row) = rows.next() {
            table.insert(tx, row)?;
        }
        Ok(table)
    }
}

impl Monitorable for InMemoryTable {
    fn monitor(&self) -> Box<dyn Monitor> {
        self.0.monitor()
    }
}

impl DynamicTable for InMemoryTable {
    fn auto_increment(&self, col: Col) -> i64 {
        self.0.auto_increment(col)
    }

    fn next_row_id(&self) -> i64 {
        self.0.next_row_id()
    }

    fn insert(&self, tx: &Tx, row: Row) -> Result<(), WeaverError> {
        self.0.insert(tx, row)
    }

    fn read<'tx, 'table: 'tx>(
        &'table self,
        tx: &'tx Tx,
        key: &KeyIndex,
    ) -> Result<Box<dyn Rows<'tx> + 'tx + Send>, WeaverError> {
        self.0.read(tx, key)
    }

    fn size_estimate(&self, key_index: &KeyIndex) -> Result<u64, WeaverError> {
        self.0.size_estimate(key_index)
    }

    fn update(&self, tx: &Tx, row: Row) -> Result<(), WeaverError> {
        self.0.update(tx, row)
    }

    fn delete(&self, tx: &Tx, key: &KeyIndex) -> Result<Box<dyn Rows>, WeaverError> {
        self.0.delete(tx, key)
    }
}

impl HasSchema for InMemoryTable {
    fn schema(&self) -> &TableSchema {
        self.0.schema()
    }
}

pub const IN_MEMORY_KEY: &'static str = "IN_MEMORY";

#[derive(Debug)]
pub struct InMemoryTableFactory;

impl Monitorable for InMemoryTableFactory {
    fn monitor(&self) -> Box<dyn Monitor> {
        Box::new(monitor_fn("InMemoryTableFactory", || {}))
    }
}

impl DynamicTableFactory for InMemoryTableFactory {
    fn open(&self, schema: &TableSchema, _core: &WeaverDbCore) -> Result<Table, WeaverError> {
        Ok(Box::new(InMemoryTable::new(schema.clone())?))
    }
}
