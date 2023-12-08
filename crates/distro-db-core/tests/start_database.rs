use distro_db_core::db::DistroDb;

#[test]
fn start_database_creates_in_memory_schema_list() {
    let db = DistroDb::new().expect("could not create distro-db instance");
}
