use tempfile::TempDir;
use tracing_subscriber::filter::LevelFilter;
use weaver_client::WeaverClient;
use weaver_core::access_control::auth::LoginContext;
use weaver_tests::{run_full_stack, start_server};

#[test]
fn secured_client_connection() -> eyre::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::DEBUG)
        .with_thread_ids(true)
        .init();
    let temp_dir = TempDir::new()?;
    run_full_stack(|server, client| Ok(()), temp_dir.path())?;

    Ok(())
}
