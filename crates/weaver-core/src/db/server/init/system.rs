use std::sync::Arc;
use std::time::Instant;

use tracing::{debug, info_span};

use crate::access_control::users::UserTable;
use crate::data::row::Row;
use crate::data::types::Type;
use crate::data::values::DbVal;
use crate::db::core::WeaverDbCore;
use crate::db::server::layers::packets::{DbReq, DbReqBody, DbResp};
use crate::db::server::processes::WeaverProcessInfo;
use crate::db::server::socket::DbSocket;
use crate::db::server::WeaverDb;
use crate::db::SYSTEM_SCHEMA;
use crate::dynamic_table::EngineKey;
use crate::error::WeaverError;
use crate::rows::OwnedRows;
use crate::storage::tables::system_tables::{SystemTable, SYSTEM_TABLE_KEY};
use crate::storage::tables::table_schema::TableSchema;

pub fn init_system_tables(db: &mut WeaverDb) -> Result<(), WeaverError> {
    let start = Instant::now();
    let span = info_span!("init-system-tables");
    let _enter = span.enter();

    let connection = Arc::new(db.connect());
    let clone = connection.clone();
    connection
        .send(DbReq::on_core(
            move |core, _cancel| -> Result<(), WeaverError> {
                add_process_list(core, &clone)?;
                init_auth(core, &clone)?;
                Ok(())
            },
        ))
        .join()??;

    let duration = start.elapsed();
    debug!(
        "finished initializing system tables in {:0.3} seconds",
        duration.as_secs_f32()
    );
    Ok(())
}

fn add_process_list(core: &mut WeaverDbCore, socket: &Arc<DbSocket>) -> Result<(), WeaverError> {
    let schema = TableSchema::builder(SYSTEM_SCHEMA, "processes")
        .column("pid", Type::Integer, true, None, None)?
        .column("user", Type::String(128), true, None, None)?
        .column("host", Type::String(128), true, None, None)?
        .column("age", Type::Integer, true, None, None)?
        .column("state", Type::String(128), true, None, None)?
        .column("info", Type::String(128), true, None, None)?
        .engine(EngineKey::new(SYSTEM_TABLE_KEY))
        .build()?;
    let table = SystemTable::new(schema.clone(), socket.clone(), move |socket, _key| {
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
                         using: _,
                     }| {
                        Row::from([
                            DbVal::Integer(pid.into()),
                            DbVal::String(user, 128),
                            DbVal::String(host, 128),
                            DbVal::Integer(age as i64),
                            DbVal::String(format!("{state:?}"), 128),
                            DbVal::String(info.to_string(), 128),
                        ])
                        .to_owned()
                    },
                );
                Ok(DbResp::rows(OwnedRows::new(schema.clone(), rows)))
            }))
            .join()??;
        match resp {
            DbResp::Rows(rows) => Ok(Box::new(rows)),
            _ => unreachable!(),
        }
    });

    core.add_table(table)?;
    Ok(())
}

fn init_auth(core: &mut WeaverDbCore, _socket: &Arc<DbSocket>) -> Result<(), WeaverError> {
    let users = UserTable::default();
    core.add_table(users)?;
    Ok(())
}
