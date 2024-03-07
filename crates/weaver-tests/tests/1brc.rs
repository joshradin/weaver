use std::io::stdout;
use std::path::Path;
use tempfile::TempDir;
use tracing::info;
use weaver_client::write_rows::write_rows;
use weaver_core::ast::Query;
use weaver_tests::{init_tracing, run_full_stack};

const DDL: &str = r#"
    CREATE TABLE default.`1brc` (
        id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL KEY,
        temperature FLOAT NOT NULL
    );
    "#;



/// performs the one billion row challege end-to-end
#[test]
fn one_billion_row_challenge() -> eyre::Result<()>{
    let _ = init_tracing();
    let temp_dir = TempDir::new()?;
    let data_file = Path::new(env!("CARGO_MANIFEST_DIR")).join("data").join("1brc.csv");

    run_full_stack(temp_dir.path(), |server, client| {
        info!("trying to get tables");
        client.query(&Query::parse(DDL)?)?;
        client.query(&Query::parse(&*format!("LOAD DATA INFILE {data_file:?} INTO TABLE `1brc` (name, temperature)", ))?)?;
        let (rows, elapsed) = client.query(&Query::parse("")?)?;
        write_rows(stdout(), rows, elapsed).expect("could not write rows");
        Ok(())
    })?;

    Ok(())
}
