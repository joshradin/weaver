use std::io::stdout;

use tempfile::TempDir;
use tracing::metadata::LevelFilter;
use tracing::warn;

use weaver_client::write_rows::write_rows;
use weaver_core::access_control::auth::LoginContext;
use weaver_core::rows::Rows;
use weaver_tests::{init_tracing, run_full_stack_port};

#[test]
fn connect_over_tcp() -> eyre::Result<()> {
    let _ = init_tracing(LevelFilter::DEBUG);
    let temp_dir = TempDir::new()?;
    const SEARCH_QUERY: &'static str = "select * from weaver.processes";
    run_full_stack_port(temp_dir.path(), |server, client| {
        warn!("successfully started weaver");

        let mut client2 = server.new_port_client(LoginContext::new())?;

        assert!(client2.connected());
        let (mut rows, _) = client.query(
            &format!(
                "SELECT pid FROM weaver.processes where pid != {}",
                client.pid()
            )
            .parse()?,
        )?;
        let other = rows.next().expect("no next row")[0]
            .int_value()
            .expect("expecting int");
        drop(rows);

        let (rows, duration) = client.query(&SEARCH_QUERY.parse()?)?;
        write_rows(stdout(), rows, duration)?;
        client.query(&format!("KILL {other}").parse()?)?;

        let (rows, ..) = client.query(&SEARCH_QUERY.parse()?)?;
        let schema = &rows.schema().clone();
        let rows = rows.to_owned();
        let row = rows
            .iter()
            .find(|s| s[(schema, "pid")].int_value() == Some(client.pid() as i64))
            .expect("client should still be present");

        let info = &row[(schema, "info")];
        assert_eq!(info.to_string(), SEARCH_QUERY);

        Ok(())
    })?;
    Ok(())
}
