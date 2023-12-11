use tracing::info;
use weaver_core::data::row::Row;
use weaver_core::data::types::Type;
use weaver_core::data::values::Value;

use weaver_core::db::core::WeaverDbCore;
use weaver_core::error::Error;
use weaver_core::rows::Rows;
use weaver_core::table_schema::TableSchema;

#[test]
fn create_in_memory() -> Result<(), Error>{
    let mut db = WeaverDbCore::new().unwrap();
    let ref schema = TableSchema::builder("default", "in_mem")
        .column("id", Type::Integer, true, None, 0)?
        .column("name", Type::String, true, None, None)?
        .build()?;

    info!("schema: {:#?}", schema);

    db.open_table(schema)?;
    let table = db.get_table("default", "in_mem").unwrap();
    {

        let tx1 = db.start_transaction();
        table
            .insert(&tx1, Row::from([
                Value::Integer(0),
                Value::String("Hello".to_string()),
            ]))
            .expect("could not insert");

        table
            .insert(&tx1, Row::from([
                Value::Integer(1),
                Value::String("Hello".to_string()),
            ]))
            .expect("could not insert");

        let mut x = table.read(&tx1, &schema.primary_key()?.all())?;
        while let Some(row) = x.next() {
            info!("row: {:?}", row);
        }



        // info!("table: {:#?}", x.into_iter().collect::<Vec<_>>());
    }

    Ok(())
}
