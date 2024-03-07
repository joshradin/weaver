//! initializes engines

use crate::db::server::layers::packets::DbReq;
use crate::db::server::WeaverDb;
use crate::error::WeaverError;
use crate::storage::engine::in_memory::InMemoryEngine;
use crate::storage::engine::StorageEngine;
use crate::storage::engine::weave_bptf::WeaveBPTFEngine;

pub fn init_engines(weaver_db: &mut WeaverDb) -> Result<(), WeaverError> {
    let socket = weaver_db.connect();
    socket
        .send(DbReq::on_core(|core, _| {
            core.add_engine(InMemoryEngine::new());
            let weaver_bptf_engine = WeaveBPTFEngine::new(core.path());
            let key = weaver_bptf_engine.engine_key().clone();
            core.add_engine(weaver_bptf_engine);
            core.set_default_engine(key);
        }))
        .join()??;
    Ok(())
}
