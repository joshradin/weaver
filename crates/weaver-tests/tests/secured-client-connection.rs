use std::io::stdout;
use tempfile::TempDir;
use tracing_subscriber::filter::LevelFilter;
use weaver_client::WeaverClient;
use weaver_client::write_rows::write_rows;
use weaver_core::access_control::auth::LoginContext;
use weaver_core::queries::ast::Query;
use weaver_core::rows::Rows;
use weaver_tests::{run_full_stack, start_server};

#[test]
fn can_connect() -> eyre::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(LevelFilter::DEBUG)
        .with_thread_ids(true)
        .try_init();
    let temp_dir = TempDir::new()?;
    run_full_stack(temp_dir.path(), |server, client| Ok(()))?;

    Ok(())
}
#[test]
fn get_processes() -> eyre::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(LevelFilter::DEBUG)
        .with_thread_ids(true)
        .try_init();
    let temp_dir = TempDir::new()?;
    run_full_stack(temp_dir.path(), |server, client| {
        let (rows, elapsed) = client.query(&Query::select(&["*"], "system.processes", None))?;
        write_rows(stdout(), rows, elapsed).expect("could not write rows");


        Ok(())
    })?;

    Ok(())
}
#[test]
fn get_tables() -> eyre::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(LevelFilter::DEBUG)
        .with_thread_ids(true)
        .try_init();
    let temp_dir = TempDir::new()?;
    run_full_stack(temp_dir.path(), |server, client| {
        todo!("no tables");
        Ok(())
        })?;

    Ok(())
}

