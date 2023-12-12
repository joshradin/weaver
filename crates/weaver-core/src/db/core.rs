use tracing::{debug, info, info_span};
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use crate::db::start_db::start_db;
use crate::dynamic_table::{EngineKey, IN_MEMORY_KEY, storage_engine_factory, StorageEngineFactory, Table};
use crate::error::Error;
use crate::tables::InMemoryTable;
use crate::tables::system_tables::SystemTableFactory;
use crate::tables::table_schema::TableSchema;
use crate::tx::{Tx, TxId};
use crate::tx::coordinator::TxCoordinator;

/// A db core. Represents some part of a distributed db
pub struct WeaverDbCore {
    engines: HashMap<EngineKey, Box<dyn StorageEngineFactory>>,
    open_tables: RwLock<HashMap<(String, String), Arc<Table>>>,
    pub(crate) tx_coordinator: Option<TxCoordinator>,
}

impl Default for WeaverDbCore {
    fn default() -> Self {
        Self::new().unwrap()
    }
}
impl WeaverDbCore {
    pub fn new() -> Result<Self, Error> {
        let engines = EngineKey::all()
            .filter_map(|key| match key.as_ref() {
                IN_MEMORY_KEY => Some((
                    key,
                    storage_engine_factory(|schema: &TableSchema| {
                        Ok(Box::new(InMemoryTable::new(schema.clone())?))
                    }),
                )),
                _ => None,
            })
            .collect::<HashMap<_, _>>();

        let mut shard = Self {
            engines,
            open_tables: Default::default(),
            tx_coordinator: None,
        };
        start_db(&mut shard)?;
        Ok(shard)
    }

    /// Insert an engine
    pub fn insert_engine<T: StorageEngineFactory + 'static>(&mut self, engine_key: EngineKey, engine: T) {
        self.engines.insert(engine_key, Box::new(engine));
    }

    pub fn start_transaction(&self) -> Tx {
        match self.tx_coordinator {
            None => {
                Tx::default()
            }
            Some(ref tx_coordinator) => {
                tx_coordinator.next()
            }
        }
    }

    pub fn open_table(&self, schema: &TableSchema) -> Result<(), Error> {
        if self.open_tables.read().contains_key(&(schema.schema().to_string(), schema.name().to_string())) {
            return Ok(())
        }
        debug!("opening table {}.{} ...", schema.schema(), schema.name());
        let mut open_tables = self.open_tables.write();
        let engine = self
            .engines
            .get(schema.engine())
            .ok_or_else(|| Error::CreateTableError)?;
        let table = engine.open(schema)?;

        open_tables.insert(
            (schema.schema().to_string(), schema.name().to_string()),
            Arc::new(table),
        );

        Ok(())
    }

    /// Gets a table, if preset. The table is responsible for handling shared-access
    pub fn get_table(&self, schema: &str, name: &str) -> Option<Arc<Table>> {
        self.open_tables
            .read()
            .get(&(schema.to_string(), name.to_string()))
            .cloned()
    }
}

impl Drop for WeaverDbCore {
    fn drop(&mut self) {
        info!("Shutting down distro db core");
    }
}
