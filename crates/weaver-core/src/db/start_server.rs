use std::thread;
use crate::cnxn::{MessageStream, RemoteDbReq};
use crate::cnxn::stream::WeaverStream;
use crate::common::stream_support::{internal_stream, internal_wstream};
use crate::db::server::layers::packets::{DbReqBody, DbResp};
use crate::db::server::WeaverDb;
use crate::error::Error;

/// Spins up the db
pub fn spin_up_shard(shard: &WeaverDb) -> Result<(), Error> {
    let socket = shard.connect();
    socket
        .send(DbReqBody::on_core_write(|shard, cancel| Ok(DbResp::Ok)))
        .join()??;

    let (sx, rx) = internal_wstream();
    shard.handle_connection(rx)?;

    thread::spawn(move || {
        let mut sx = sx;
        thread::park()
    });

    Ok(())
}
