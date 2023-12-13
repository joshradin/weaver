//! Users are what connect to the database

use weaver_core::data::row::Row;
use weaver_core::dynamic_table::{Col, DynamicTable};
use weaver_core::error::Error;
use weaver_core::rows::{KeyIndex, Rows};
use weaver_core::tables::table_schema::TableSchema;
use weaver_core::tx::Tx;

/// A user struct is useful for access control
#[derive(Debug)]
pub struct User {
    name: String,
    host: String,
}

#[derive(Default, Debug)]
pub struct UserTable {

}

impl DynamicTable for UserTable {
    fn schema(&self) -> &TableSchema {
        todo!()
    }

    fn auto_increment(&self, col: Col) -> i64 {
        todo!()
    }

    fn next_row_id(&self) -> i64 {
        todo!()
    }

    fn insert(&self, tx: &Tx, row: Row) -> Result<(), Error> {
        todo!()
    }

    fn read<'tx, 'table: 'tx>(&'table self, tx: &'tx Tx, key: &KeyIndex) -> Result<Box<dyn Rows<'tx> + 'tx + Send>, Error> {
        todo!()
    }

    fn update(&self, tx: &Tx, row: Row) -> Result<(), Error> {
        todo!()
    }

    fn delete(&self, tx: &Tx, key: &KeyIndex) -> Result<Box<dyn Rows>, Error> {
        todo!()
    }
}
