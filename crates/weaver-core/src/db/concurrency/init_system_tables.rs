use std::time::Instant;
use tracing::{debug, info, info_span};
use crate::db::concurrency::{DbReq, DbResp, WeaverDb};
use crate::db::core::WeaverDbCore;
use crate::dynamic_table::{EngineKey, SYSTEM_TABLE_KEY};
use crate::error::Error;
use crate::tables::system_tables::SystemTableFactory;
use crate::tables::table_schema::TableSchema;

pub static SYSTEM_SCHEMA: &str = "system";

pub fn init_system_tables(db: &mut WeaverDb) -> Result<(), Error> {
    let start = Instant::now();
    let span = info_span!("init-system-tables");
    let _enter = span.enter();

    {
        let connection = db.connect();
        db.shared.db.write().insert_engine(EngineKey::new(SYSTEM_TABLE_KEY), SystemTableFactory::new(connection));
    }
    let connection = db.connect();
    connection.send(DbReq::full(|db| {
        add_process_list(db)?;

        Ok(DbResp::Ok)
    }))?;

    let duration = start.elapsed();
    debug!("finished initializing system tables in {:0.3} seconds", duration.as_secs_f32());
    Ok(())
}

fn add_process_list(weaver: &mut WeaverDbCore) -> Result<(), Error> {
    let schema = TableSchema::builder(SYSTEM_SCHEMA, "processes").build()?;
    weaver.open_table(&schema)?;
    Ok(())
}