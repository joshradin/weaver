use distro_db_core::data::{Row, Type, Value};
use distro_db_core::db::DistroDb;
use distro_db_core::table_schema::{TableSchema, TableSchemaBuilder};

#[test]
fn create_in_memory() {
    let mut db = DistroDb::new().unwrap();
    let ref schema = TableSchema::builder("default", "in_mem")
        .column("id", Type::Number, true, None)
        .column("name", Type::String, true, None)
        .build();

    db.open_table(schema).expect("could not open table");
    let table = db.get_table("default", "in_mem").unwrap();
    table.insert(&Row::from([Value::Number(0), Value::String("Hello".to_string())])).expect("could not insert");

}