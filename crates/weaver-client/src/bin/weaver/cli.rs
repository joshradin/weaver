use clap::{ArgAction, Parser};
use std::path::PathBuf;
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


    #[clap(long, action=ArgAction::HelpLong)]
    help: Option<bool>,
}
