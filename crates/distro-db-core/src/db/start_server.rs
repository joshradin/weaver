use crate::db::{DbReq, DbResp, DistroDbServer, ShardSocketError};

/// Spins up the db
pub fn spin_up_shard(shard: &DistroDbServer) -> Result<(), ShardSocketError> {
    let socket = shard.connect();
    socket.send(DbReq::full(|shard| DbResp::Ok))?;

    Ok(())
}
