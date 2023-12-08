use distro_db_core::data::{Row, Type, Value};

use distro_db_core::db::DistroDb;
use distro_db_core::error::Error;
use distro_db_core::rows::{KeyIndex, Rows};
use distro_db_core::table_schema::{TableSchema, TableSchemaBuilder};

#[test]
fn create_in_memory<'a>() -> Result<(), Error>{
    let mut db = DistroDb::new().unwrap();
    let ref schema = TableSchema::builder("default", "in_mem")
        .column("id", Type::Integer, true, None, 0)?
        .column("name", Type::String, true, None, None)?
        .build()?;

    println!("schema: {:#?}", schema);

    db.open_table(schema)?;
    let table = db.get_table("default", "in_mem").unwrap();
    let tx = db.start_transaction();
    {

        let tx = &tx;
        table
            .insert(tx, Row::from([
                Value::Integer(0),
                Value::String("Hello".to_string()),
            ]))
            .expect("could not insert");
        table
            .insert(tx, Row::from([
                Value::Integer(1),
                Value::String("Hello".to_string()),
            ]))
            .expect("could not insert");

        let mut x = &mut *table.read(tx, &schema.primary_key()?.all())?;
        while let Some(row) = x.next() {
            println!("row: {:?}", row);
        }

        // println!("table: {:#?}", x.into_iter().collect::<Vec<_>>());
    }
    let tx = tx;

    Ok(())
}
