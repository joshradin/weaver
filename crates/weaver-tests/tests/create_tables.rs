use std::io::stdout;

use tempfile::TempDir;
use tracing::info;

use weaver_client::write_rows::write_rows;
use weaver_core::ast::Query;
use weaver_tests::{init_tracing, run_full_stack};

#[test]
fn explain_create_table() -> eyre::Result<()> {
    let _ = init_tracing();
    let temp_dir = TempDir::new()?;
    run_full_stack(temp_dir.path(), |server, client| {
        info!("explain create table");
        let (rows, elapsed) = client.query(&Query::parse(r#"
            explain create table `schema`.`table` ( id INT auto_increment primary key, value FLOAT NOT NULL )
        "#)?)?;
        write_rows(stdout(), rows, elapsed).expect("could not write rows");

        Ok(())
    })?;

    Ok(())
}

#[test]
fn create_simple_table() -> eyre::Result<()> {
    let _ = init_tracing();
    let temp_dir = TempDir::new()?;
    run_full_stack(temp_dir.path(), |server, client| {
        info!("explain create table");
        let (rows, elapsed) = client.query(&Query::parse(r#"
            create table `schema`.`table` ( id INT auto_increment primary key, value float not null )
        "#)?)?;
        write_rows(stdout(), rows, elapsed).expect("could not write rows");

        Ok(())
    })?;

    Ok(())
}
