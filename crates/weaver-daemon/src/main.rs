use clap::Parser;
use color_eyre::eyre;
use tracing_error::ErrorLayer;
use tracing_subscriber::layer::SubscriberExt;

use weaver_daemon::{App, run};

fn main() -> eyre::Result<()> {
    let app = App::parse();
    run(app)?;
    Ok(())
}
