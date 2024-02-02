use tracing::info;
use tracing::level_filters::LevelFilter;
use weaver_core::data::row::Row;
use weaver_core::data::types::Type;
use weaver_core::data::values::Literal;

use weaver_core::db::core::WeaverDbCore;
use weaver_core::error::Error;
use weaver_core::rows::Rows;
use weaver_core::tables::table_schema::TableSchema;

#[test]
fn create_in_memory() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .with_thread_ids(true)
        .with_thread_names(true)
        .try_init();
    let mut db = WeaverDbCore::new().unwrap();
    let ref schema = TableSchema::builder("default", "in_mem")
        .column("id", Type::Integer, true, None, 0)
        .unwrap()
        .column("name", Type::String, true, None, None)
        .unwrap()
        .build()
        .expect("could not build schema");

    info!("schema: {:#?}", schema);

    db.open_table(schema).unwrap();
    let table = db.get_table("default", "in_mem").unwrap();
    {
        let tx1 = db.start_transaction();
        table
            .insert(
                &tx1,
                Row::from([Literal::Integer(0), Literal::String("Hello".to_string())]),
            )
            .expect("could not insert");

        table
            .insert(
                &tx1,
                Row::from([Literal::Integer(1), Literal::String("Hello".to_string())]),
            )
            .expect("could not insert");

        let mut x = table
            .read(&tx1, &schema.primary_key().unwrap().all())
            .expect("failed to get row");
        while let Some(row) = x.next() {
            info!("row: {:?}", row);
        }

        // info!("table: {:#?}", x.into_iter().collect::<Vec<_>>());
    }
}
