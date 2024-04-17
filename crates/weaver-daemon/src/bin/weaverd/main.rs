use clap::Parser;
use color_eyre::eyre;
use tracing::metadata::LevelFilter;
use tracing_error::ErrorLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use weaver_daemon::cli::App;
use weaver_daemon::run;

fn main() -> eyre::Result<()> {
    let app = App::parse();
    init_tracing(&app.level_filter())?;
    run(app)?;
    Ok(())
}

fn init_tracing(app: &LevelFilter) -> eyre::Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_max_level(*app)
        .finish()
        .with(ErrorLayer::default())
        .init();
    Ok(())
}
