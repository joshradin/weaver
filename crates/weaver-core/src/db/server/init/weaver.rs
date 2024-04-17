//! initializes the weaver database
//!
//! This is persistent information that is stored between executions

use std::time::Instant;

use crate::data::types::Type;
use tracing::{debug, info_span};

use crate::db::core::WeaverDbCore;

use crate::dynamic_table::{DynamicTable, EngineKey};
use crate::error::WeaverError;
use crate::queries::query_cost::CostTable;
use crate::storage::tables::bpt_file_table::B_PLUS_TREE_FILE_KEY;
use crate::storage::tables::table_schema::TableSchema;
use crate::tx::Tx;

pub fn init_weaver_schema(core: &mut WeaverDbCore) -> Result<(), WeaverError> {
    let start = Instant::now();
    let span = info_span!("init-weaver-schema");
    let _enter = span.enter();

    cost_table(core)?;

    drop(_enter);
    let duration = start.elapsed();
    debug!(
        "finished initializing weaver schema in {:0.3} seconds",
        duration.as_secs_f32()
    );
    Ok(())
}

fn cost_table(db: &mut WeaverDbCore) -> Result<(), WeaverError> {
    db.open_table(
        &TableSchema::builder("weaver", "cost")
            .column("key", Type::String(32), true, None, None)?
            .column("cost", Type::Float, true, None, None)?
            .column("row_factor", Type::Integer, true, None, None)?
            .column("row_log", Type::Integer, false, None, None)?
            .engine(EngineKey::new(B_PLUS_TREE_FILE_KEY))
            .build()?,
    )?;

    // gets the default cost table
    let cost_table = CostTable::default();
    let table = db.get_open_table("weaver", "cost")?;
    let tx = Tx::default();
    cost_table.flush_to_table(&table, &tx)?;
    table.commit(&tx);

    Ok(())
}
