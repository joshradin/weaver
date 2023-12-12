use std::time::Duration;
use tracing::level_filters::LevelFilter;
use weaver_core::cnxn::tcp::WeaverTcpStream;
use weaver_core::cnxn::{Message, MessageStream};
use weaver_core::db::concurrency::{DbReq, DbResp, WeaverDb};
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

    let mut stream = WeaverTcpStream::connect_timeout(socket, Duration::from_secs(1))?;
    stream.write(&Message::Req(DbReq::Ping))?;
    let Message::Resp(DbResp::Pong) = stream.read()? else {
        panic!("must return pong")
    };
    let mut stream = WeaverTcpStream::connect_timeout(socket, Duration::from_secs(1))?;
    stream.write(&Message::Req(DbReq::Ping))?;
    let Message::Resp(DbResp::Pong) = stream.read()? else {
        panic!("must return pong")
    };

    Ok(())
}
