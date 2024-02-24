use std::error::Error;
use std::io::stdout;

use tempfile::TempDir;
use tracing::info;
use tracing_subscriber::filter::LevelFilter;

use weaver_client::write_rows::write_rows;
use weaver_core::ast::Query;
use weaver_tests::run_full_stack;

fn init_tracing() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .with_thread_ids(true)
        .event_format(tracing_subscriber::fmt::format())
        .try_init()
}

#[test]
fn can_connect() -> eyre::Result<()> {
    let _ = init_tracing();
    let temp_dir = TempDir::new()?;
    run_full_stack(temp_dir.path(), |server, client| Ok(()))?;

    Ok(())
}


#[test]
fn get_processes() -> eyre::Result<()> {
    let _ = init_tracing();
    let temp_dir = TempDir::new()?;
    run_full_stack(temp_dir.path(), |server, client| {
        info!("trying to get system processes");
        let (rows, elapsed) = client.query(&Query::parse("select * from weaver.processes")?)?;
        write_rows(stdout(), rows, elapsed).expect("could not write rows");

        Ok(())
    })?;

    Ok(())
}

#[test]
fn get_tables() -> eyre::Result<()> {
    let _ = init_tracing();
    let temp_dir = TempDir::new()?;
    run_full_stack(temp_dir.path(), |server, client| {
        info!("trying to get tables");
        let (rows, elapsed) = client.query(&Query::parse("select * from weaver.tables")?)?;
        write_rows(stdout(), rows, elapsed).expect("could not write rows");

        Ok(())
    })?;

    Ok(())
}
