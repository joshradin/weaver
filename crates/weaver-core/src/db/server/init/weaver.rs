//! initializes the weaver database
//!
//! This is persistent information that is stored between executions

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use tracing::{debug, info_span};

use crate::db::core::WeaverDbCore;
use crate::db::server::layers::packets::DbReq;
use crate::db::server::WeaverDb;
use crate::dynamic_table::EngineKey;
use crate::error::Error;
use crate::tables::file_table::B_PLUS_TREE_FILE_KEY;
use crate::tables::table_schema::TableSchema;

pub fn init_weaver_schema(db: &mut WeaverDb) -> Result<(), Error> {
    let start = Instant::now();
    let span = info_span!("init-weaver-schema");
    let _enter = span.enter();

    let connection = Arc::new(db.connect());
    connection
        .send(DbReq::on_core(move |core, cancel| -> Result<(), Error> {
            cost_table(core)?;
            Ok(())
        }))
        .join()??;

    drop(_enter);
    let duration = start.elapsed();
    debug!(
        "finished initializing weaver schema in {:0.3} seconds",
        duration.as_secs_f32()
    );
    Ok(())
}

fn cost_table(db: &mut WeaverDbCore) -> Result<(), Error> {
    db.open_table(
        &TableSchema::builder("weaver", "cost")
            .engine(EngineKey::new(B_PLUS_TREE_FILE_KEY))
            .build()?,
    )?;
    Ok(())
}
