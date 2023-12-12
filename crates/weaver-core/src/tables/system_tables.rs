//! System tables provide information about the state of the system, and only exist as a "view" of
//! the internal state of the weaver

use crate::data::row::Row;
use crate::data::values::Value;
use crate::db::concurrency::processes::WeaverProcessInfo;
use crate::db::concurrency::{DbReq, DbResp, DbSocket};
use crate::dynamic_table::{Col, DynamicTable, StorageEngineFactory, Table};
use crate::error::Error;
use crate::rows::{DefaultOwnedRows, KeyIndex, OwnedRowsExt, Rows};
use crate::tables::table_schema::TableSchema;
use crate::tx::Tx;
use std::fmt::Debug;
use std::sync::Arc;

/// Provide a system table
pub struct SystemTable {
    table_schema: TableSchema,
    connection: Arc<DbSocket>,
    on_read: Box<
        dyn for<'a> Fn(&'a DbSocket, &KeyIndex) -> Result<Box<dyn Rows<'a> + Send + 'a>, Error>
            + Send
            + Sync,
    >,
}

impl SystemTable {
    pub fn new<F>(table_schema: TableSchema, connection: Arc<DbSocket>, on_read: F) -> Self
    where
        F: for<'a> Fn(&'a DbSocket, &KeyIndex) -> Result<Box<dyn Rows<'a> + Send + 'a>, Error>
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

    fn read<'tx, 'table: 'tx>(
        &'table self,
        _tx: &'tx Tx,
        key: &KeyIndex,
    ) -> Result<Box<dyn Rows<'tx> + 'tx + Send>, Error> {
        let arc = &*self.connection;
        (self.on_read)(arc, key)
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
    connection: Arc<DbSocket>,
}

impl SystemTableFactory {
    /// Creates system tables using an actual, live connection
    pub fn new(connection: DbSocket) -> Self {
        Self {
            connection: Arc::new(connection),
        }
    }
}

impl StorageEngineFactory for SystemTableFactory {
    fn open(&self, schema: &TableSchema) -> Result<Table, Error> {
        let table = match schema.name() {
            "processes" => {
                let schema = schema.clone();
                Box::new(SystemTable::new(
                    schema.clone(),
                    self.connection.clone(),
                    move |socket, key_index| {
                        let schema = schema.clone();
                        let resp = socket.send(DbReq::on_server(move |full| {
                            let processes = full.with_process_manager(|pm| pm.processes());

                            let rows = processes.into_iter().map(
                                |WeaverProcessInfo {
                                     pid,
                                     age,
                                     state,
                                     info,
                                 }| {
                                    Row::from([
                                        Value::Integer(pid.into()),
                                        Value::Integer(age as i64),
                                        Value::String(format!("{state:?}")),
                                        Value::String(format!("{info}")),
                                    ])
                                    .to_owned()
                                },
                            );
                            Ok(DbResp::rows(DefaultOwnedRows::new(schema.clone(), rows)))
                        }))?;
                        match resp {
                            DbResp::Rows(rows) => Ok(rows.to_rows()),
                            _ => unreachable!(),
                        }
                    },
                ))
            }
            _unknown => {
                panic!("unknown system table: {}", _unknown);
            }
        };
        Ok(table)
    }
}
