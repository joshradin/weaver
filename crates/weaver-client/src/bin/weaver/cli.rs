use clap::Parser;
use weaver_core::cnxn::DEFAULT_PORT;
#[derive(Debug, Parser)]
pub struct App {
    #[clap(default_value = "localhost")]
    pub host: String,
    #[clap(default_value_t = DEFAULT_PORT)]
    pub port: u16,
}
