use std::io::stdout;
use std::path::Path;

use tempfile::{tempdir, TempDir};
use tracing::info;

use weaver_client::write_rows::write_rows;
use weaver_core::ast::Query;
use weaver_tests::{init_tracing, run_full_stack};

const DDL: &str = r#"
    CREATE TABLE `default`.`1brc` (
        id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL KEY,
        temperature FLOAT NOT NULL
    );
    "#;

const MAIN_QUERY: &'static str = r#"
        SELECT name, count(temperature) as count, min(temperature), max(temperature), avg(temperature) as avg_temperature
        FROM `default`.`1brc`
        GROUP BY name
        ORDER by count DESC, avg_temperature DESC
        LIMIT 25
        "#;

fn main() -> eyre::Result<()> {
    let _ = init_tracing();
    let data_dir = tempdir()?;
    let data_file = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("data")
        .join("48krc.csv");

    run_full_stack(&data_dir.path(), |server, client| {
        info!("trying to get tables");
        client.query(&Query::parse(DDL)?)?;

        let (rows, elapsed) = client.query(&Query::parse(&*format!("EXPLAIN {MAIN_QUERY}"))?)?;
        write_rows(stdout(), rows, elapsed).expect("could not write rows");
        client.query(&Query::parse(&*format!(
            r#"
                LOAD DATA INFILE {data_file:?} INTO TABLE `default`.`1brc` (name, temperature)
                FIELDS TERMINATED BY ';'
                "#,
        ))?)?;

        let (rows, elapsed) = client.query(&Query::parse(MAIN_QUERY)?)?;
        write_rows(stdout(), rows, elapsed).expect("could not write rows");
        Ok(())
    })?;

    Ok(())
}
