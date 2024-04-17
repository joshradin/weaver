use std::fs::File;
use std::io::stdout;
use std::path::Path;

use clap::Parser;
use log::{error, info, LevelFilter};
use rustyline::{Config, DefaultEditor, Editor};
use rustyline::completion::Completer;
use rustyline::config::Configurer;
use rustyline::error::ReadlineError;
use rustyline::highlight::{Highlighter, MatchingBracketHighlighter};
use rustyline::hint::Hinter;
use rustyline::history::History;
use rustyline::validate::{MatchingBracketValidator, ValidationContext, ValidationResult, Validator};
use simplelog::{ColorChoice, CombinedLogger, TerminalMode, TermLogger, WriteLogger};

use weaver_ast::ast::Query;
use weaver_ast::error::ParseQueryError;
use weaver_client::WeaverClient;
use weaver_client::write_rows::write_rows;
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
            app.log_level(),
            simplelog::Config::default(),
            TerminalMode::Stderr,
            ColorChoice::Auto,
        ),
        WriteLogger::new(LevelFilter::Trace, simplelog::Config::default(), log_file),
    ])?;

    let mut login = LoginContext::new();
    if let Some(username) = &app.username {
        login.set_user(username);
    }

    match app.host.as_str() {
        "localhost" => {
            info!("connecting to weaver instance using socket file");
            let path = "weaver/weaverdb.socket";
            let client = match WeaverClient::connect_localhost(path, login) {
                Ok(cnxn) => cnxn,
                Err(e) => {
                    error!("Failed to connect to weaver instance at {:?} ({e})", path);
                    return Err(e);
                }
            };
            client_repl(app, client)
        }
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

fn client_repl<T: Stream>(app: App, mut client: WeaverClient<T>) -> eyre::Result<()> {
    let mut rl = Editor::with_config(Config::builder().build())?;
    std::fs::create_dir_all("~/.weaver")?;
    if Path::new("~/.weaver/history").exists() {
        rl.load_history("~/.weaver/history")?;
    }
    rl.set_helper(Some(ReplHelper));

    while client.connected() {
        let line = match rl.readline("> ") {
            Ok(line) => line,
            Err(e) => match e {
                ReadlineError::Eof => {
                    break;
                }
                ReadlineError::Interrupted => {
                    continue;
                }
                e => return Err(e.into()),
            },
        };
        rl.history_mut().add(&line)?;
        let query = match Query::parse(&line) {
            Ok(q) => q,
            Err(err) => {
                eprintln!("failed to parse query: {err}");
                continue;
            }
        };

        match client.query(&query) {
            Ok((rows, duration)) => {
                write_rows(stdout(), rows, duration)?;
            }
            Err(e) => {
                eprintln!("got error: {}", e)
            }
        };
    }
    info!("saving history");
    rl.append_history("~/.weaver/history")?;

    Ok(())
}

struct ReplHelper;

impl Completer for ReplHelper {
    type Candidate = String;
}

impl Validator for ReplHelper {
    fn validate(&self, ctx: &mut ValidationContext) -> rustyline::Result<ValidationResult> {
        if ctx.input().contains(";") {
            Ok(ValidationResult::Valid(None))
        } else {
            Ok(ValidationResult::Incomplete)
        }
    }
}

impl Highlighter for ReplHelper {}

impl Hinter for ReplHelper {
    type Hint = String;
}

impl rustyline::Helper for ReplHelper {}

#[cfg(test)]
mod tests {
    #[test]
    fn empty() {}
}
