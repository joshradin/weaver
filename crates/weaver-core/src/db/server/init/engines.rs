//! initializes engines

use crate::db::server::layers::packets::DbReq;
use crate::db::server::WeaverDb;
use crate::error::Error;
use crate::storage::engine::in_memory::InMemoryEngine;

pub fn init_engines(weaver_db: &mut WeaverDb) -> Result<(), Error> {
    let socket = weaver_db.connect();
    socket.send(DbReq::on_core(|core, _| {
        core.add_engine(InMemoryEngine::new());
    })).join()??;
    Ok(())
}