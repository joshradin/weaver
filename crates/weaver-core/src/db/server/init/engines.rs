//! initializes engines

use crate::db::server::layers::packets::DbReq;
use crate::db::server::WeaverDb;
use crate::error::Error;
use crate::storage::engine::in_memory::InMemoryEngine;
use crate::storage::engine::weave_bptf::WeaveBPTFEngine;

pub fn init_engines(weaver_db: &mut WeaverDb) -> Result<(), Error> {
    let socket = weaver_db.connect();
    socket.send(DbReq::on_core(|core, _| {
        core.add_engine(InMemoryEngine::new());
        core.add_engine(WeaveBPTFEngine::new(core.path()));
    })).join()??;
    Ok(())
}