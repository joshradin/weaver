use std::io::stdout;
use tempfile::TempDir;
use tracing::{metadata, warn};
use weaver_client::write_rows::write_rows;
use weaver_core::ast::Query;
use weaver_tests::{init_tracing, run_full_stack_local_socket};

#[test]
fn reconnect() -> eyre::Result<()> {
    let _ = init_tracing(metadata::LevelFilter::DEBUG);
    let temp_dir = TempDir::new()?;
    run_full_stack_local_socket(temp_dir.path(), |_server, client| {
        warn!("successfully started weaver");
        let (rows, elapsed) = client.query(&Query::parse(
            r#"
            select * from weaver.cost
        "#,
        )?)?;
        write_rows(stdout(), rows, elapsed).expect("could not write rows");
        Ok(())
    })?;
    run_full_stack_local_socket(temp_dir.path(), |_server, client| {
        warn!("successfully reconnected");
        let (rows, elapsed) = client.query(&Query::parse(
            r#"
            select * from weaver.cost
        "#,
        )?)?;
        write_rows(stdout(), rows, elapsed).expect("could not write rows");

        Ok(())
    })?;
    Ok(())
}
