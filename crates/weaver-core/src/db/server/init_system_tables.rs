use std::sync::Arc;
use std::time::Instant;

use tracing::{debug, info_span};

use crate::access_control::users::UserTable;
use crate::data::row::Row;
use crate::data::types::Type;
use crate::data::values::Value;
use crate::db::core::WeaverDbCore;
use crate::db::server::layers::packets::{DbReq, DbReqBody, DbResp};
use crate::db::server::processes::WeaverProcessInfo;
use crate::db::server::socket::DbSocket;
use crate::db::server::WeaverDb;
use crate::dynamic_table::{EngineKey, SYSTEM_TABLE_KEY};
use crate::error::Error;
use crate::rows::{DefaultOwnedRows, OwnedRowsExt};
use crate::tables::system_tables::SystemTable;
use crate::tables::table_schema::TableSchema;

pub static SYSTEM_SCHEMA: &str = "system";

pub fn init_system_tables(db: &mut WeaverDb) -> Result<(), Error> {
    let start = Instant::now();
    let span = info_span!("init-system-tables");
    let _enter = span.enter();

    let connection = Arc::new(db.connect());
    let clone = connection.clone();
    connection
        .send(DbReq::on_core(move |core, cancel| -> Result<(), Error> {
            add_process_list(core, &clone)?;
            init_auth(core, &clone)?;
            Ok(())
        }))
        .join()??;

    let duration = start.elapsed();
    debug!(
        "finished initializing system tables in {:0.3} seconds",
        duration.as_secs_f32()
    );
    Ok(())
}

fn add_process_list(core: &mut WeaverDbCore, socket: &Arc<DbSocket>) -> Result<(), Error> {
    let schema = TableSchema::builder(SYSTEM_SCHEMA, "processes")
        .column("pid", Type::Integer, true, None, None)?
        .column("user", Type::String, true, None, None)?
        .column("host", Type::String, true, None, None)?
        .column("age", Type::Integer, true, None, None)?
        .column("state", Type::String, true, None, None)?
        .column("info", Type::String, true, None, None)?
        .engine(EngineKey::new(SYSTEM_TABLE_KEY))
        .build()?;
    let table = SystemTable::new(schema.clone(), socket.clone(), move |socket, key| {
        let schema = schema.clone();
        let resp = socket
            .send(DbReqBody::on_server(move |full, _| {
                let processes = full.with_process_manager(|pm| pm.processes());

                let rows = processes.into_iter().map(
                    |WeaverProcessInfo {
                         pid,
                         age,
                         state,
                         info,
                         user,
                         host,
                         using,
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
            }))
            .join()??;
        match resp {
            DbResp::Rows(rows) => Ok(rows.to_rows()),
            _ => unreachable!(),
        }
    });

    core.add_table(table)?;
    Ok(())
}

fn init_auth(core: &mut WeaverDbCore, socket: &Arc<DbSocket>) -> Result<(), Error> {
    let users = UserTable::default();
    core.add_table(users)?;
    Ok(())
}
