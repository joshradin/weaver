use crate::db::concurrency::{DbReq, DbResp, WeaverDb, ShardSocketError};
use crate::error::Error;

/// Spins up the db
pub fn spin_up_shard(shard: &WeaverDb) -> Result<(), Error> {
    let socket = shard.connect();
    socket.send(DbReq::full(|shard| Ok(DbResp::Ok)))?;

    Ok(())
}
