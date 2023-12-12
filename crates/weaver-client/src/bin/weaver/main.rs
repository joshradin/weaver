use std::fs::File;
use clap::Parser;
use log::{error, info, LevelFilter};
use simplelog::{ColorChoice, CombinedLogger, Config, TerminalMode, TermLogger, WriteLogger};
use weaver_core::cnxn::{Message, MessageStream};
use weaver_core::cnxn::tcp::WeaverTcpStream;
use weaver_core::db::concurrency::DbReq;
use weaver_core::error::Error;
use crate::cli::App;

mod cli;

fn main() -> eyre::Result<()> {
    let app = App::parse();
    let log_file = File::options().append(true).create(true).open(format!("{}.log", env!("CARGO_BIN_NAME")))?;
    CombinedLogger::init(vec![
        TermLogger::new(LevelFilter::Warn, Config::default(), TerminalMode::Stderr, ColorChoice::Auto),
        WriteLogger::new(LevelFilter::Trace, Config::default(), log_file)
    ])?;

    let addr = (app.host.as_str(), app.port);
    info!("connecting to weaver instance at {:?}", addr);
    let mut connection = match WeaverTcpStream::connect(addr) {
        Ok(cnxn) => {cnxn}
        Err(e) => {
            error!("Failed to connect to weaver instance at {:?} ({e})", addr);
            return Err(e.into());
        }
    };

    connection.write(&Message::Req(DbReq::ConnectionInfo))?;
    let resp = connection.read()?;
    println!("resp: {:#?}", resp);


    Ok(())
}