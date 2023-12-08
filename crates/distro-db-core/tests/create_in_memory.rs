use distro_db_core::data::{Row, Type, Value};

use distro_db_core::db::DistroDb;
use distro_db_core::error::Error;
use distro_db_core::rows::{KeyIndex, RowsExt};
use distro_db_core::table_schema::{TableSchema, TableSchemaBuilder};

#[test]
fn create_in_memory() -> Result<(), Error>{
    let mut db = DistroDb::new().unwrap();
    let ref schema = TableSchema::builder("default", "in_mem")
        .column("id", Type::Integer, true, None, 0)?
        .column("name", Type::String, true, None, None)?
        .build()?;

    println!("schema: {:#?}", schema);

    db.open_table(schema)?;
    let table = db.get_table("default", "in_mem").unwrap();
    table
        .insert(Row::from([
            Value::Integer(0),
            Value::String("Hello".to_string()),
        ]))
        .expect("could not insert");

    println!("table: {:#?}", table.read(&schema.primary_key()?.all())?.into_iter().collect::<Vec<_>>());

    Ok(())
}
