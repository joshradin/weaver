use crate::db::server::layers::packets::{DbReqBody, DbResp};
use crate::db::server::WeaverDb;
use crate::error::Error;

/// Spins up the db
pub fn spin_up_shard(shard: &WeaverDb) -> Result<(), Error> {
    let socket = shard.connect();
    socket.send(DbReqBody::on_core(|shard| Ok(DbResp::Ok)))?;

    Ok(())
}
