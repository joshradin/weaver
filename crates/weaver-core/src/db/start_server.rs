use crate::db::concurrency::{DbReq, DbResp, WeaverDb, ShardSocketError};

/// Spins up the db
pub fn spin_up_shard(shard: &WeaverDb) -> Result<(), ShardSocketError> {
    let socket = shard.connect();
    socket.send(DbReq::full(|shard| Ok(DbResp::Ok)))?;

    Ok(())
}
