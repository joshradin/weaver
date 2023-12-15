use std::time::Duration;
use tracing::level_filters::LevelFilter;
use weaver_core::cnxn::stream::WeaverStream;
use weaver_core::cnxn::{Message, MessageStream, RemoteDbReq, RemoteDbResp};
use weaver_core::db::server::layers::packets::{DbReqBody, DbResp};
use weaver_core::db::server::WeaverDb;
use weaver_core::error::Error;

#[test]
fn bind_to_tcp() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .with_thread_ids(true)
        .with_thread_names(true)
        .init();
    let mut server = WeaverDb::default();
    server.bind_tcp(("localhost", 0))?;
    let socket = server.local_addr().unwrap();

    let mut stream = WeaverStream::connect_timeout(socket, Duration::from_secs(1))?;
    stream.write(&Message::Req(RemoteDbReq::Ping.into()))?;
    let Message::Resp(RemoteDbResp::Pong) = stream.read()? else {
        panic!("must return pong")
    };
    let mut stream = WeaverStream::connect_timeout(socket, Duration::from_secs(1))?;
    stream.write(&Message::Req(RemoteDbReq::Ping.into()))?;
    let Message::Resp(RemoteDbResp::Pong) = stream.read()? else {
        panic!("must return pong")
    };

    Ok(())
}
