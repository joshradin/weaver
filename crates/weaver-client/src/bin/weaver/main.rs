use std::fs::File;
use std::io::stdout;

use clap::Parser;
use log::{error, info, LevelFilter};
use simplelog::{ColorChoice, CombinedLogger, Config, TermLogger, TerminalMode, WriteLogger};

use weaver_ast::ast::Query;

use weaver_client::write_rows::write_rows;
use weaver_client::WeaverClient;
use weaver_core::access_control::auth::LoginContext;
use weaver_core::common::stream_support::Stream;

use crate::cli::App;

mod cli;

fn main() -> eyre::Result<()> {
    color_eyre::install()?;

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

    let mut login = LoginContext::new();
    if let Some(username) = &app.username {
        login.set_user(username);
    }


    match app.host.as_str() {
        "localhost" => {
            info!("connecting to weaver instance using socket file");
            let path = "weaver/weaverdb.socket";
            let client =  match WeaverClient::connect_localhost(path, login) {
                Ok(cnxn) => cnxn,
                Err(e) => {
                    error!("Failed to connect to weaver instance at {:?} ({e})", path);
                    return Err(e);
                }
            };
            client_repl(app, client)
        },
        other => {
            let addr = (other, app.port);
            info!("connecting to weaver instance at {:?}", addr);
            let client = match WeaverClient::connect(addr, login) {
                Ok(cnxn) => cnxn,
                Err(e) => {
                    error!("Failed to connect to weaver instance at {:?} ({e})", addr);
                    return Err(e);
                }
            };
            client_repl(app, client)
        }
    }
}

fn client_repl<T: Stream>(app: App, client: WeaverClient<T>) -> eyre::Result<()> {

    Ok(())
}





#[cfg(test)]
mod tests {
    #[test]
    fn empty() {}
}
