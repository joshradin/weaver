use crate::db::concurrency::{DbReq, DbResp, ShardSocketError, WeaverDb};
use crate::error::Error;

/// Spins up the db
pub fn spin_up_shard(shard: &WeaverDb) -> Result<(), Error> {
    let socket = shard.connect();
    socket.send(DbReq::on_core(|shard| Ok(DbResp::Ok)))?;

    Ok(())
}
