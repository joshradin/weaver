//! Bootstraps the weaver core

use crate::data::row::Row;
use std::path::Path;
use tracing::error_span;
use weaver_ast::ToSql;

use crate::data::types::Type;
use crate::data::values::DbVal;
use crate::db::core::WeaverDbCore;
use crate::dynamic_table::{DynamicTable, EngineKey};
use crate::error::WeaverError;
use crate::rows::{KeyIndex, Rows};
use crate::storage::tables::table_schema::TableSchema;
use crate::tx::Tx;

/// Bootstraps the weaver core, enabling persistence and table protections.
///
/// This should only ever run once. Unlike the functions found within the [crate::db::server::init] modules,
/// the only responsibility for this function is to make sure all _necessary_ files tables that require
/// _persistent_ storage are loaded. Consider functionality in this piece of code almost like the
///
///
/// Bootstrapping should follow the following process:
/// 1. Create the `weaver.schemata` table with a stored table schema within this program,
/// 2. Insert the `weaver` schema with id 1
/// 3. Create the `weaver.tables` table with a stored table schema within this program.
/// 4. Insert `weaver.schemata` and `weaver.tables` entries into `weaver.tables` with protected flag on
///
pub fn bootstrap(core: &mut WeaverDbCore, weaver_schema_dir: &Path) -> Result<(), WeaverError> {
    let span = error_span!("bootstrap");
    let _enter = span.enter();
    let ref tx = Tx::default(); // base transaction

    std::fs::create_dir_all(weaver_schema_dir)?;

    // STEP 1: Load weaver.schemata
    let ref weaver_schemata_schema = weaver_schemata_schema()?;
    core.open_table(weaver_schemata_schema)?;
    let weaver_schemata = core.get_open_table("weaver", "schemata")?;
    weaver_schemata.insert(tx, Row::from([DbVal::from(1), "weaver".into()]))?;

    // STEP 2: Load weaver.tables
    let ref weaver_tables_schema = weaver_tables_schema()?;
    core.open_table(weaver_tables_schema)?;
    let weaver_tables = core.get_open_table("weaver", "tables")?;

    weaver_tables.insert(
        tx,
        Row::from([
            DbVal::Null,
            DbVal::from(1),
            DbVal::from("schemata"),
            DbVal::from(weaver_schemata_schema.to_sql()),
            DbVal::from(true),
        ]),
    )?;
    weaver_tables.insert(
        tx,
        Row::from([
            DbVal::Null,
            DbVal::from(1),
            DbVal::from("tables"),
            DbVal::from(weaver_tables_schema.to_sql()),
            DbVal::from(true),
        ]),
    )?;

    Ok(())
}

/// The `weaver.schemata` schema
fn weaver_schemata_schema() -> Result<TableSchema, WeaverError> {
    TableSchema::builder("weaver", "schemata")
        .column("id", Type::Integer, true, None, 1)?
        .column("name", Type::String(256), true, None, None)?
        .primary(&["id"])?
        .engine(EngineKey::basic())
        .build()
}

/// The `weaver.tables` schema
fn weaver_tables_schema() -> Result<TableSchema, WeaverError> {
    TableSchema::builder("weaver", "tables")
        .column("id", Type::Integer, true, None, 1)?
        .column("schema_id", Type::Integer, true, None, None)?
        .column("name", Type::String(255), true, None, None)?
        .column("table_ddl", Type::String(1 << 11), true, None, None)?
        .column("protected", Type::Boolean, true, DbVal::from(false), None)?
        .primary(&["id"])?
        .engine(EngineKey::basic())
        .build()
}

#[cfg(test)]
mod tests {
    use crate::db::core::bootstrap::weaver_tables_schema;
    use crate::db::core::{bootstrap, WeaverDbCore};
    use crate::error::WeaverError;

    #[test]
    fn can_create_weaver_tables_schema() {
        let _table = weaver_tables_schema().expect("could not create table");
        println!("{_table:#?}");
    }

    #[test]
    fn test_bootstrap() -> Result<(), WeaverError> {
        let (temp, mut core) = WeaverDbCore::in_temp_dir()?;
        bootstrap(&mut core, &temp.path().join("weaver"))?;

        Ok(())
    }
}
