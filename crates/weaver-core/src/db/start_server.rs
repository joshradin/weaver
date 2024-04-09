
use crate::cnxn::{MessageStream};
use crate::common::stream_support::{internal_wstream};
use crate::db::server::layers::packets::{DbReqBody, DbResp};
use crate::db::server::WeaverDb;
use crate::error::WeaverError;
use std::thread;

/// Spins up the db
pub fn spin_up_shard(shard: &WeaverDb) -> Result<(), WeaverError> {
    let socket = shard.connect();
    socket
        .send(DbReqBody::on_core_write(|_shard, _cancel| Ok(DbResp::Ok)))
        .join()??;

    let (sx, rx) = internal_wstream();
    shard.handle_connection(rx)?;

    thread::spawn(move || {
        let _sx = sx;
        thread::park()
    });

    Ok(())
}
