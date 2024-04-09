use std::io::stdout;
use tempfile::TempDir;
use tracing::warn;
use weaver_client::write_rows::write_rows;
use weaver_core::ast::Query;
use weaver_tests::{init_tracing, run_full_stack};

#[test]
fn reconnect() -> eyre::Result<()> {
    let _ = init_tracing(None);
    let temp_dir = TempDir::new()?;
    run_full_stack(temp_dir.path(), |_server, _client| {
        warn!("successfully started weaver");
        Ok(())
    })?;
    run_full_stack(temp_dir.path(), |_server, client| {
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
