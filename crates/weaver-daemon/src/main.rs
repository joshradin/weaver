use clap::Parser;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{fmt, Layer};

use weaver_core::error::WeaverError;
use weaver_daemon::{run, App};

fn main() -> Result<(), WeaverError> {
    let app = App::parse();

    let subscriber = tracing_subscriber::registry().with(
        fmt::Layer::new()
            .with_thread_names(true)
            .with_filter(app.level_filter()),
    );
    tracing::subscriber::set_global_default(subscriber).unwrap();

    run(app)?;

    Ok(())
}
