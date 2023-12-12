//! Tables are a collection of data
pub mod table_schema;
pub mod in_memory_table;
pub mod system_tables;

pub use in_memory_table::InMemoryTable;

pub type TableRef = (String, String);

