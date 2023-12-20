mod cli;

pub use cli::App;
use std::path::PathBuf;
use std::thread::sleep;
use std::time::Duration;
use tracing::{info, info_span, trace};
use weaver_core::access_control::auth::init::AuthConfig;
use weaver_core::db::core::WeaverDbCore;
use weaver_core::db::server::layers::packets::DbReqBody;
use weaver_core::db::server::layers::packets::DbResp;
use weaver_core::db::server::WeaverDb;
use weaver_core::error::Error;

/// Starts the application
pub fn run(app: App) -> Result<(), Error> {
    let span = info_span!("main");
    let _enter = span.enter();

    info!("Starting weaver db...");
    let core = WeaverDbCore::new()?;

    let auth_config = AuthConfig {
        key_store: app.key_store.unwrap_or_else(|| PathBuf::from("./keys")),
        force_recreate: false,
    };

    let mut weaver = WeaverDb::new(
        app.num_workers.unwrap_or_else(num_cpus::get),
        core,
        auth_config,
    )?;

    weaver.bind_tcp((&*app.host, app.port))?;
    let cnxn = weaver.connect();
    loop {
        trace!("Checking if weaver db is alive...");
        let resp = cnxn.send(DbReqBody::Ping).join()??;
        if !matches!(resp, DbResp::Pong) {
            break;
        }
        trace!("weaver db still alive");
        sleep(Duration::from_secs(30));
    }
    Ok(())
}
