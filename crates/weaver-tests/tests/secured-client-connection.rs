use tempfile::TempDir;
use tracing_subscriber::filter::LevelFilter;
use weaver_client::WeaverClient;
use weaver_core::access_control::auth::LoginContext;
use weaver_tests::start_server;

#[test]
fn secured_client_connection() -> eyre::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .with_thread_ids(true)
        .init();
    let temp_dir = TempDir::new()?;
    let server = start_server(0, temp_dir.path(), None)?;
    let client = WeaverClient::connect(("localhost", server.port()), LoginContext::new())?;

    Ok(())
}
