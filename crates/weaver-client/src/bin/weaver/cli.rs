use clap::{ArgAction, Parser, value_parser};
use std::path::PathBuf;
use log::LevelFilter;
use weaver_core::cnxn::DEFAULT_PORT;
#[derive(Debug, Parser)]
#[clap(
    version,
    author,
    about = "A client to connect to a running weaver instance"
)]
#[clap(disable_help_flag = true)]
pub struct App {
    #[clap(long, short, default_value = "localhost")]
    pub host: String,
    #[clap(long, short = 'P', default_value_t = DEFAULT_PORT)]
    pub port: u16,

    /// Sets the location of the key store
    #[clap(long)]
    pub key_store: Option<PathBuf>,

    /// The username to connect as
    #[clap(long, short)]
    pub username: Option<String>,

    /// Sets the verbosity of the application
    #[clap(short)]
    #[clap(action = ArgAction::Count, value_parser = value_parser!(u8).range(0..=2))]
    pub verbosity: u8,

    #[clap(long, action=ArgAction::HelpLong)]
    help: Option<bool>,
}
impl App {
    pub fn log_level(&self) -> LevelFilter {
        match self.verbosity {
            0 => LevelFilter::Info,
            1 => LevelFilter::Debug,
            2.. => LevelFilter::Trace,
        }
    }
}
