use weaver_core::db::core::WeaverDbCore;

#[test]
fn start_database_creates_in_memory_schema_list() {
    let _db = WeaverDbCore::new().expect("could not create distro-db instance");
}
