use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use fs2::FileExt;

use parking_lot::RwLock;
use tracing::{debug, info};

use crate::db::start_db::start_db;
use crate::dynamic_table::{
    storage_engine_factory, DynamicTable, EngineKey, StorageEngineFactory, Table,
};
use crate::error::Error;
use crate::tables::file_table::BptfTableFactory;
use crate::tables::table_schema::TableSchema;
use crate::tables::InMemoryTable;
use crate::tables::{file_table::B_PLUS_TREE_FILE_KEY, in_memory_table::IN_MEMORY_KEY};
use crate::tx::coordinator::TxCoordinator;
use crate::tx::Tx;

pub mod fs;

/// A db core. Represents some part of a distributed db
pub struct WeaverDbCore {
    path: PathBuf,
    lock_file: Option<File>,
    engines: HashMap<EngineKey, Box<dyn StorageEngineFactory>>,
    default_engine: Option<EngineKey>,
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
        Self::with_path(std::env::current_dir()?)
    }

    /// Opens the weaver db core at the given paths.
    ///
    /// All table are opened relative to this path
    pub fn with_path<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let path = path.as_ref().to_path_buf();
        let lock_file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(path.join("weaver.lock"))?;

        lock_file.lock_exclusive()?;


        let engines = EngineKey::all()
            .filter_map(|key| match key.as_ref() {
                IN_MEMORY_KEY => Some((
                    key,
                    storage_engine_factory(|schema: &TableSchema| {
                        Ok(Box::new(InMemoryTable::new(schema.clone())?))
                    }),
                )),
                B_PLUS_TREE_FILE_KEY => Some((key, Box::new(BptfTableFactory::new(&path)))),
                _ => None,
            })
            .collect::<HashMap<_, _>>();

        let mut shard = Self {
            path,
            lock_file: Some(lock_file),
            engines,
            default_engine: Some(EngineKey::new(B_PLUS_TREE_FILE_KEY)),
            open_tables: Default::default(),
            tx_coordinator: None,
        };
        start_db(&mut shard)?;
        Ok(shard)
    }

    /// Insert an engine
    pub fn insert_engine<T: StorageEngineFactory + 'static>(
        &mut self,
        engine_key: EngineKey,
        engine: T,
    ) {
        self.engines.insert(engine_key, Box::new(engine));
    }

    pub fn start_transaction(&self) -> Tx {
        match self.tx_coordinator {
            None => Tx::default(),
            Some(ref tx_coordinator) => tx_coordinator.next(),
        }
    }

    /// Add a table directly into the core
    pub fn add_table<T: DynamicTable + 'static>(&self, table: T) -> Result<(), Error> {
        debug!(
            "directly adding table {}.{} into core",
            table.schema().schema(),
            table.schema().name()
        );
        let table_schema = table.schema();
        let (schema, name) = (
            table_schema.schema().to_string(),
            table_schema.name().to_string(),
        );
        if self
            .open_tables
            .read()
            .contains_key(&(schema.clone(), name.clone()))
        {
            return Ok(());
        } else {
            let mut open_tables = self.open_tables.write();
            let table: Table = Box::new(table);
            open_tables.insert((schema, name), Arc::new(table));
            Ok(())
        }
    }
    pub fn open_table(&self, schema: &TableSchema) -> Result<(), Error> {
        if self
            .open_tables
            .read()
            .contains_key(&(schema.schema().to_string(), schema.name().to_string()))
        {
            return Ok(());
        }
        debug!("opening table {}.{} ...", schema.schema(), schema.name());
        let mut open_tables = self.open_tables.write();
        let engine = self
            .engines
            .get(schema.engine())
            .ok_or_else(|| Error::CreateTableError)?;
        let table = engine.open(schema, self)?;

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

    /// Gets the path this weaver db is open in
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for WeaverDbCore {
    fn drop(&mut self) {
        if let Some(lock_file) = self.lock_file.take() {
            drop(lock_file.unlock());
            let _ = std::fs::remove_file(&self.path.join("weaver.lock"));
        }
        info!("Shutting down distro db core");
    }
}
