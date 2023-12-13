use tracing::info;
use tracing::level_filters::LevelFilter;
use weaver_core::data::row::Row;
use weaver_core::data::types::Type;
use weaver_core::data::values::Value;
use weaver_core::db::core::WeaverDbCore;
use weaver_core::db::server::layers::packets::{DbReqBody, DbResp};
use weaver_core::db::server::WeaverDb;
use weaver_core::error::Error;
use weaver_core::tables::table_schema::TableSchema;


#[test]
fn transactions_in_memory() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .with_thread_ids(true)
        .init();

    let mut db = WeaverDb::new(num_cpus::get(), WeaverDbCore::new()?)?;

    let socket = db.connect();
    socket
        .send(DbReqBody::on_core(|db| {
            let ref schema = TableSchema::builder("default", "in_mem")
                .column("id", Type::Integer, true, None, 0)?
                .column("name", Type::String, true, None, None)?
                .build()?;
            db.open_table(schema)?;
            let table = db.get_table("default", "in_mem").unwrap();
            {
                let tx1 = db.start_transaction();
                table
                    .insert(
                        &tx1,
                        Row::from([Value::Integer(0), Value::String("Hello".to_string())]),
                    )
                    .expect("could not insert");

                let tx2 = db.start_transaction();
                table
                    .insert(
                        &tx2,
                        Row::from([Value::Integer(1), Value::String("Hello".to_string())]),
                    )
                    .expect("could not insert");

                let mut x = table.read(&tx1, &schema.primary_key()?.all())?;

                info!("---- tx1 ----");
                while let Some(row) = x.next() {
                    info!("row: {:?}", row);
                }

                info!("---- tx2 ----");

                {
                    let mut x = table.read(&tx2, &schema.primary_key()?.all())?;
                    while let Some(row) = x.next() {
                        info!("row: {:?}", row);
                    }
                }
                tx2.commit();

                // info!("table: {:#?}", x.into_iter().collect::<Vec<_>>());
                Ok(DbResp::Ok)
            }
        }))
        .expect("socket failed");

    Ok(())
}
