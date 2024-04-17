//! Load tables

use tracing::{debug, info, trace};

use crate::db::core::{weaver_schemata_schema, weaver_tables_schema};
use crate::db::server::layers::packets::{DbReq, DbResp};
use crate::db::server::WeaverDb;
use crate::dynamic_table::DynamicTable;
use crate::error::WeaverError;
use crate::key::KeyData;
use crate::queries::query_cost::{cost_table_schema, CostTable};
use crate::rows::{KeyIndex, KeyIndexKind};
use crate::storage::tables::table_schema::TableSchema;

pub fn load_tables(db: &mut WeaverDb) -> Result<(), WeaverError> {
    let socket = db.connect();

    let tx = socket.start_tx()?;

    let r = socket
        .send(DbReq::on_core(move |core, _| -> Result<(), WeaverError> {
            let schemata = weaver_schemata_schema()?;
            core.open_table(&schemata)?;
            let tables_schema = weaver_tables_schema()?;
            core.open_table(&tables_schema)?;

            let schemata_table = core.get_open_table("weaver", "schemata")?;
            let tables_table = core.get_open_table("weaver", "tables")?;

            for row in schemata_table.all(&tx)? {
                let schema_name = &row[(&schemata, "name")];
                let schema_id = &row[(&schemata, "id")];
                info!("loading tables for schema {schema_name:?}");
                let tables = tables_table.all(&tx)?;

                for table in tables
                    .into_iter()
                    .filter(|row| &row[(&tables_schema, "schema_id")] == schema_id)
                {
                    let ddl = &table[(&tables_schema, "table_ddl_json")]
                        .string_value()
                        .expect("is not string value");
                    let parsed_schema: TableSchema =
                        serde_json::from_str(ddl).map_err(|e| WeaverError::custom(e))?;
                    trace!("row: {:?}", &table[0..4]);
                    trace!("schema: {:#?}", parsed_schema);
                    info!("opening table: {}.{}...", parsed_schema.schema(), parsed_schema.name());
                    core.open_table(&parsed_schema)?;
                }
            }

            tx.commit();
            Ok(())
        }))
        .join()??
        .to_result()?;

    Ok(())
}
