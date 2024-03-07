use std::error::Error;
use std::io::stdout;

use tempfile::TempDir;
use tracing::info;
use tracing_subscriber::filter::LevelFilter;

use weaver_client::write_rows::write_rows;
use weaver_core::ast::Query;
use weaver_tests::{init_tracing, run_full_stack};


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

#[test]
fn get_tables_with_schema() -> eyre::Result<()> {
    let _ = init_tracing();
    let temp_dir = TempDir::new()?;
    run_full_stack(temp_dir.path(), |server, client| {
        info!("trying to get tables");
        let (rows, elapsed) = client.query(&Query::parse(
            r"
        SELECT s.name, t.name, t.table_ddl
        FROM
            weaver.tables as t
        JOIN
            weaver.schemata as s ON t.schema_id = s.id
        WHERE
            s.name = 'weaver'
            ",
        )?)?;
        write_rows(stdout(), rows, elapsed).expect("could not write rows");

        Ok(())
    })?;

    Ok(())
}

#[test]
fn explain_get_tables_with_schema() -> eyre::Result<()> {
    let _ = init_tracing();
    let temp_dir = TempDir::new()?;
    run_full_stack(temp_dir.path(), |server, client| {
        info!("trying to get tables");
        let (rows, elapsed) = client.query(&Query::parse(
            r"
        EXPLAIN SELECT s.name as schema, t.name, t.table_ddl
        FROM
            weaver.tables as t
        JOIN
            weaver.schemata as s ON t.schema_id = s.id
        WHERE
            s.name = 'weaver'
            ",
        )?)?;
        write_rows(stdout(), rows, elapsed).expect("could not write rows");

        Ok(())
    })?;

    Ok(())
}