//! Tables are a collection of data
pub mod file_table;
pub mod in_memory_table;
pub mod lss_table;
pub mod system_tables;
pub mod table_schema;
pub mod unbuffered_table;

pub use in_memory_table::InMemoryTable;

pub type TableRef = (String, String);
