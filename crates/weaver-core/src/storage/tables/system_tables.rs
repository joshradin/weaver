//! System tables provide information about the state of the system, and only exist as a "view" of
//! the internal state of the weaver

use crate::data::row::Row;
use crate::db::server::socket::DbSocket;
use crate::dynamic_table::{Col, DynamicTable, HasSchema};
use crate::error::WeaverError;
use crate::monitoring::{monitor_fn, Monitor, Monitorable};
use crate::rows::{KeyIndex, Rows};
use crate::storage::tables::table_schema::TableSchema;
use crate::tx::Tx;
use std::fmt::Debug;
use std::sync::Arc;

/// Provide a system table
pub struct SystemTable {
    table_schema: TableSchema,
    connection: Arc<DbSocket>,
    on_read: Box<
        dyn for<'a> Fn(&'a DbSocket, &KeyIndex) -> Result<Box<dyn Rows<'a> + Send + 'a>, WeaverError>
            + Send
            + Sync,
    >,
}

impl SystemTable {
    pub fn new<F>(table_schema: TableSchema, connection: Arc<DbSocket>, on_read: F) -> Self
    where
        F: for<'a> Fn(&'a DbSocket, &KeyIndex) -> Result<Box<dyn Rows<'a> + Send + 'a>, WeaverError>
            + Send
            + Sync
            + 'static,
    {
        Self {
            table_schema,
            connection,
            on_read: Box::new(on_read),
        }
    }
}

impl Monitorable for SystemTable {
    fn monitor(&self) -> Box<dyn Monitor> {
        Box::new(monitor_fn("SystemTable", || {}))
    }
}

impl DynamicTable for SystemTable {
    fn auto_increment(&self, col: Col) -> i64 {
        unimplemented!("system tables shouldn't need auto increments")
    }

    fn next_row_id(&self) -> i64 {
        unimplemented!("system tables shouldn't need row_ids")
    }

    fn insert(&self, tx: &Tx, row: Row) -> Result<(), WeaverError> {
        unimplemented!("can not insert into a system table")
    }

    fn read<'tx, 'table: 'tx>(
        &'table self,
        _tx: &'tx Tx,
        key: &KeyIndex,
    ) -> Result<Box<dyn Rows<'tx> + 'tx + Send>, WeaverError> {
        let arc = &*self.connection;
        (self.on_read)(arc, key)
    }

    fn size_estimate(&self, key_index: &KeyIndex) -> Result<u64, WeaverError> {
        Ok(0)
    }

    fn update(&self, tx: &Tx, row: Row) -> Result<(), WeaverError> {
        unimplemented!("can not update information in a system table")
    }

    fn delete(&self, tx: &Tx, key: &KeyIndex) -> Result<Box<dyn Rows>, WeaverError> {
        unimplemented!("can not delete data from a system table")
    }
}

impl HasSchema for SystemTable {
    fn schema(&self) -> &TableSchema {
        &self.table_schema
    }
}

pub const SYSTEM_TABLE_KEY: &'static str = "SYSTEM_TABLE";
