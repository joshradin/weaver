use std::fmt::{Debug, Formatter};
use crate::db::core::WeaverDbCore;
use crate::dynamic_table::Table;
use crate::error::Error;
use crate::monitoring::{Monitor, Monitorable};
use crate::tables::table_schema::TableSchema;

pub trait DynamicTableFactory: Send + Sync + Monitorable {
    fn open(&self, schema: &TableSchema, core: &WeaverDbCore) -> Result<Table, Error>;
}

/// A delegated dynamic table that allows for object safe access over arbitrary types
pub struct DynamicTableFactoryDelegate {
    table_factory: Box<dyn DynamicTableFactory>
}

impl Monitorable for DynamicTableFactoryDelegate {
    fn monitor(&self) -> Box<dyn Monitor> {
        self.table_factory.monitor()
    }
}

impl DynamicTableFactory for DynamicTableFactoryDelegate {
    fn open(&self, schema: &TableSchema, core: &WeaverDbCore) -> Result<Table, Error> {
        self.table_factory.open(schema, core)
    }
}

impl DynamicTableFactoryDelegate {

    /// Create a new delegate
    pub fn new<T : DynamicTableFactory + 'static>(delegate: T) -> Self {
        Self { table_factory: Box::new(delegate)}
    }
}

impl Debug for DynamicTableFactoryDelegate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicTableDelegate")
            .finish_non_exhaustive()
    }
}
