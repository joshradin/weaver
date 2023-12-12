use std::thread::sleep;
use std::time::Duration;
use tracing::metadata::LevelFilter;
use tracing::{info, info_span, instrument, trace};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{fmt, Layer};
use weaver_core::cnxn::DEFAULT_PORT;
use weaver_core::db::concurrency::{DbReq, DbResp, WeaverDb};
use weaver_core::error::Error;

mod cli;

fn main() -> Result<(), Error> {
    let subscriber =
        tracing_subscriber::registry().with(fmt::layer().with_filter(LevelFilter::DEBUG));
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let span = info_span!("main");
    let _enter = span.enter();

    info!("Starting weaver db...");
    let mut weaver = WeaverDb::default();

    weaver.bind_tcp(("localhost", DEFAULT_PORT))?;
    let cnxn = weaver.connect();
    loop {
        trace!("Checking if weaver db is alive...");
        let resp = cnxn.send(DbReq::Ping)?;
        if !matches!(resp, DbResp::Pong) {
            break;
        }
        trace!("weaver db still alive");
        sleep(Duration::from_secs(30));
    }

    Ok(())
}
