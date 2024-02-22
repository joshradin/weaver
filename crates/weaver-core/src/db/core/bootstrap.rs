//! Bootstraps the weaver core

use std::path::Path;

use crate::data::types::Type;
use crate::data::values::DbVal;
use crate::db::core::WeaverDbCore;
use crate::dynamic_table::{DynamicTable, EngineKey};
use crate::error::Error;
use crate::rows::{KeyIndex, Rows, RowsExt};
use crate::tables::table_schema::TableSchema;
use crate::tx::Tx;

/// Bootstraps the weaver core, enabling persistence and table protections.
///
/// This should only ever run once. Unlike the functions found within the [crate::db::server::init] modules,
/// the only responsibility for this function is to make sure all _necessary_ files tables that require
/// _persistent_ storage are loaded. Consider functionality in this piece of code almost like the
///
///
/// Bootstrapping should follow the following process:
/// 1. Create the `weaver.schemata` table with a stored table schema within this program, then insert
///     the `weaver` schema with id 1
/// 2. Create the `weaver.tables` table with a stored table schema within this program.
/// 3.
///
pub fn bootstrap(core: &mut WeaverDbCore, weaver_schema_dir: &Path) -> Result<(), Error> {
    let ref tx = Tx::default(); // base transaction

    // STEP 2: Load weaver.tables
    let ref weaver_tables_schema = weaver_tables_schema()?;
    core.open_table(weaver_tables_schema)?;

    todo!("bootstrapping")
}

/// The `weaver.tables` schema
fn weaver_tables_schema() -> Result<TableSchema, Error> {
    TableSchema::builder("weaver", "tables")
        .column("id", Type::Integer, true, DbVal::from(1), 1)?
        .column("schema_id", Type::Integer, true, None, None)?
        .column("name", Type::String(255), true, None, None)?
        .column("table_ddl", Type::String(1 << 11), true, None, None)?
        .primary(&["id"])?
        .engine(EngineKey::basic())
        .build()
}

#[cfg(test)]
mod tests {
    use crate::db::core::bootstrap::weaver_tables_schema;
    use crate::db::core::{bootstrap, WeaverDbCore};
    use crate::error::Error;

    #[test]
    fn can_create_weaver_tables_schema() {
        let _table = weaver_tables_schema().expect("could not create table");
        println!("{_table:#?}");
    }

    #[test]
    fn test_bootstrap() -> Result<(), Error> {
        let (temp, mut core) = WeaverDbCore::in_temp_dir()?;
        bootstrap(&mut core, &temp.path().join("weaver"))?;

        Ok(())
    }
}
