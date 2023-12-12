use crate::cli::App;
use clap::Parser;
use log::{error, info, LevelFilter};
use simplelog::{ColorChoice, CombinedLogger, Config, TermLogger, TerminalMode, WriteLogger};
use std::fs::File;
use std::io::stdout;
use std::thread::sleep;
use std::time::Duration;
use weaver_client::write_rows::write_rows;
use weaver_client::WeaverClient;
use weaver_core::cnxn::tcp::WeaverTcpStream;
use weaver_core::cnxn::{Message, MessageStream, RemoteDbReq, RemoteDbResp};
use weaver_core::db::concurrency::DbReq;
use weaver_core::error::Error;
use weaver_core::queries::ast::Query;

mod cli;

fn main() -> eyre::Result<()> {
    let app = App::parse();
    let log_file = File::options()
        .append(true)
        .create(true)
        .open(format!("{}.log", env!("CARGO_BIN_NAME")))?;
    CombinedLogger::init(vec![
        TermLogger::new(
            LevelFilter::Warn,
            Config::default(),
            TerminalMode::Stderr,
            ColorChoice::Auto,
        ),
        WriteLogger::new(LevelFilter::Trace, Config::default(), log_file),
    ])?;

    let addr = (app.host.as_str(), app.port);
    info!("connecting to weaver instance at {:?}", addr);
    let mut connection = match WeaverClient::connect(addr) {
        Ok(cnxn) => cnxn,
        Err(e) => {
            error!("Failed to connect to weaver instance at {:?} ({e})", addr);
            return Err(e.into());
        }
    };

    let query = Query::Select {
        columns: vec!["*".to_string()],
        table_ref: ("system".to_string(), "processes".to_string()),
        condition: None,
        limit: None,
        offset: None,
    };

    let (rows, duration) = connection.query(&query)?;
    write_rows(stdout(), rows, duration)?;
    sleep(Duration::from_secs(5));

    Ok(())
}
