use std::iter;
use tracing::info;
use tracing::level_filters::LevelFilter;
use weaver_core::access_control::auth::init::AuthConfig;
use weaver_core::data::row::Row;
use weaver_core::data::types::Type;
use weaver_core::data::values::DbVal;
use weaver_core::db::core::WeaverDbCore;
use weaver_core::db::server::layers::packets::{DbReqBody, DbResp, IntoDbResponse};
use weaver_core::db::server::WeaverDb;
use weaver_core::dynamic_table::DynamicTable;
use weaver_core::error::Error;
use weaver_core::rows::Rows;
use weaver_core::tables::table_schema::TableSchema;

#[test]
fn transactions_in_memory() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .with_thread_ids(true)
        .init();

    let mut db = WeaverDb::new(num_cpus::get(), WeaverDbCore::new()?, AuthConfig::default())?;

    let socket = db.connect();
    socket
        .send(DbReqBody::on_core_write(|db, _| {
            Ok((|| -> Result<_, Error> {
                let ref schema = TableSchema::builder("default", "in_mem")
                    .column("id", Type::Integer, true, None, 0)?
                    .column("name", Type::String(u16::MAX), true, None, None)?
                    .build()?;
                db.open_table(schema)?;
                let table = db.get_open_table("default", "in_mem").unwrap();
                {
                    let tx1 = db.start_transaction();
                    table
                        .insert(
                            &tx1,
                            Row::from([DbVal::Integer(0), DbVal::from("Hello".to_string())]),
                        )
                        .expect("could not insert");

                    let tx2 = db.start_transaction();
                    table
                        .insert(
                            &tx2,
                            Row::from([DbVal::Integer(1), DbVal::from("Hello".to_string())]),
                        )
                        .expect("could not insert");

                    let mut x = table.read(&tx1, &schema.primary_key()?.all())?;

                    let mut tx1_rows = vec![];
                    info!("---- tx1 ----");
                    while let Some(row) = x.next() {
                        info!("row: {:?}", row);
                        tx1_rows.push(row.to_owned());
                    }

                    info!("---- tx2 ----");
                    let mut tx2_rows = vec![];
                    {
                        let mut x = table.read(&tx2, &schema.primary_key()?.all())?;
                        while let Some(row) = x.next() {
                            info!("row: {:?}", row);
                            tx2_rows.push(row.to_owned()); // need to owned here because can't access rows after transaction dropped otherwise
                        }
                    }
                    tx2.commit();
                    drop(x);
                    tx1.commit();
                    assert_ne!(
                        tx1_rows, tx2_rows,
                        "rows in different open transactions should be invisible to each other"
                    );

                    let tx3 = db.start_transaction();
                    let mut all_rows = table.read(&tx3, &schema.primary_key()?.all())?;
                    let all_rows: Vec<_> =
                        iter::from_fn(|| all_rows.next().map(|r| r.to_owned())).collect();
                    assert_eq!(
                        all_rows,
                        tx1_rows
                            .iter()
                            .chain(&tx2_rows)
                            .cloned()
                            .collect::<Vec<_>>()
                    );

                    // info!("table: {:#?}", x.into_iter().collect::<Vec<_>>());
                    Ok(DbResp::Ok)
                }
            })()
            .into_db_resp())
        }))
        .join()
        .unwrap()
        .expect("socket failed");

    Ok(())
}
