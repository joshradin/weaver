//! Users are what connect to the database

use crate::data::row::Row;
use crate::data::types::Type;
use crate::db::WEAVER_SCHEMA;
use crate::dynamic_table::{Col, DynamicTable, EngineKey};
use crate::error::Error;
use crate::rows::{KeyIndex, Rows};
use crate::storage::VecPaged;
use crate::tables::table_schema::TableSchema;
use crate::tables::InMemoryTable;
use crate::tx::Tx;
use serde::{Deserialize, Serialize};

/// A user struct is useful for access control
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct User {
    name: String,
    host: String,
}

impl User {
    /// Create a new user with a given name and host
    pub fn new(name: impl AsRef<str>, host: impl AsRef<str>) -> Self {
        Self {
            name: name.as_ref().to_string(),
            host: host.as_ref().to_string(),
        }
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
    in_memory: InMemoryTable,
}

impl UserTable {
    pub fn new() -> Self {
        Self {
            in_memory: InMemoryTable::new(
                TableSchema::builder(WEAVER_SCHEMA, "users")
                    .column("host", Type::String, true, None, None)
                    .unwrap()
                    .column("user", Type::String, true, None, None)
                    .unwrap()
                    .column("auth_string", Type::String, false, None, None)
                    .unwrap()
                    .primary(&["host", "user"])
                    .unwrap()
                    .index("SK_user", &["user"], false)
                    .unwrap()
                    .engine(EngineKey::new("USER_TABLE"))
                    .build()
                    .expect("failed to create users table schema"),
            )
            .expect("couldn't create users table"),
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
        &self.in_memory.schema()
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
        self.in_memory.read(tx, key)
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
    #[test]
    fn detect_host() {
        // let user = User::detect_host("root");
        // assert_eq!(user.name(), "root");
        // assert_ne!(user.host(), "localhost");
    }
}
