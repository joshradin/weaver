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

fn main() -> eyre::Result<()> {
    let _ = init_tracing();
    let data_dir = tempdir()?;
    let data_file = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("data")
        .join("1krc.csv");

    run_full_stack(&data_dir.path(), |server, client| {
        info!("trying to get tables");
        client.query(&Query::parse(DDL)?)?;
        client.query(&Query::parse(&*format!(
            r#"
                LOAD DATA INFILE {data_file:?} INTO TABLE `default`.`1brc` (name, temperature)
                FIELDS TERMINATED BY ';'
                "#,
        ))?)?;

        let (rows, elapsed) = client.query(&Query::parse(r#"
        EXPLAIN
        SELECT name, min(temperature), max(temperature), avg(temperature)
        FROM `default`.`1brc`
        GROUP BY name
        "#)?)?;
        write_rows(stdout(), rows, elapsed).expect("could not write rows");
        Ok(())
    })?;

    Ok(())
}
