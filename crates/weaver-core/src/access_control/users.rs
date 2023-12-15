//! Users are what connect to the database

use crate::data::row::Row;
use crate::dynamic_table::{Col, DynamicTable};
use crate::error::Error;
use crate::rows::{KeyIndex, Rows};
use crate::tables::table_schema::TableSchema;
use crate::tx::Tx;

/// A user struct is useful for access control
#[derive(Debug, Clone)]
pub struct User {
    name: String,
    host: String,
}

impl User {

    /// Create a new user with a given name and host
    pub fn new(name: impl AsRef<str>, host: impl AsRef<str>) -> Self {
        Self { name: name.as_ref().to_string(), host: host.as_ref().to_string() }
    }


    /// Creates a new user with the host set to `localhost`
    pub fn localhost(name: impl AsRef<str>) -> Self {
        Self::new(name, "localhost")
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn host(&self) -> &str {
        &self.host
    }
}

/// The user table
#[derive(Debug)]
pub struct UserTable {
    schema: TableSchema
}

impl UserTable {
    pub fn new() -> Self {
        Self {
            schema: TableSchema::builder("weaver", "users")
                .build()
                .expect("failed to create users table schema"),
        }
    }
}

impl Default for UserTable {
    fn default() -> Self {
        Self::new()
    }
}

impl DynamicTable for UserTable {
    fn schema(&self) -> &TableSchema {
        &self.schema
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

    fn read<'tx, 'table: 'tx>(
        &'table self,
        tx: &'tx Tx,
        key: &KeyIndex,
    ) -> Result<Box<dyn Rows<'tx> + 'tx + Send>, Error> {
        todo!()
    }

    fn update(&self, tx: &Tx, row: Row) -> Result<(), Error> {
        todo!()
    }

    fn delete(&self, tx: &Tx, key: &KeyIndex) -> Result<Box<dyn Rows>, Error> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::access_control::users::User;

    #[test]
    fn detect_host() {
        let user = User::detect_host("root");
        assert_eq!(user.name(), "root");
        assert_ne!(user.host(), "localhost");
    }
}