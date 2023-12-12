use crate::cli::App;
use clap::Parser;
use log::{error, info, LevelFilter};
use simplelog::{ColorChoice, CombinedLogger, Config, TermLogger, TerminalMode, WriteLogger};
use std::fs::File;
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
    let mut connection = match WeaverTcpStream::connect(addr) {
        Ok(cnxn) => cnxn,
        Err(e) => {
            error!("Failed to connect to weaver instance at {:?} ({e})", addr);
            return Err(e.into());
        }
    };

    let resp = connection.send(&RemoteDbReq::ConnectionInfo)?;
    println!("resp: {:#?}", resp);

    let _ = connection.send(&RemoteDbReq::StartTransaction)?;

    let RemoteDbResp::Ok = connection.send(&RemoteDbReq::Query(Query::Select {
        columns: vec!["*".to_string()],
        table_ref: ("system".to_string(), "processes".to_string()),
        condition: None,
        limit: None,
        offset: None,
    }))?
    else {
        panic!("no ok");
    };

    let RemoteDbResp::Schema(schema) = connection.send(&RemoteDbReq::GetSchema)? else {
        panic!("should get schema");
    };
    println!("schema: {schema:#?}");

    loop {
        let resp = connection.send(&RemoteDbReq::GetRow)?;
        let RemoteDbResp::Row(Some(row)) = resp else {
            break;
        };
        println!("row");
    }

    connection.send(&RemoteDbReq::Commit)?;

    Ok(())
}
