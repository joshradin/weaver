use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use fs2::FileExt;
use parking_lot::RwLock;
use tracing::{debug, debug_span, field, info, trace};

use crate::data::row::Row;
use crate::data::values::DbVal;
pub use bootstrap::{bootstrap, weaver_schemata_schema, weaver_tables_schema};
use weaver_ast::ToSql;

use crate::db::start_db::start_db;
use crate::dynamic_table::{DynamicTable, EngineKey, HasSchema, Table};
use crate::dynamic_table_factory::DynamicTableFactory;
use crate::error::WeaverError;
use crate::monitoring::{monitor_fn, Monitor, MonitorCollector, Monitorable, Stats};
use crate::storage::engine::{StorageEngine, StorageEngineDelegate};
use crate::storage::tables::shared_table::SharedTable;
use crate::storage::tables::table_schema::TableSchema;
use crate::tx::coordinator::TxCoordinator;
use crate::tx::Tx;

mod bootstrap;

/// A db core. Represents some part of a distributed db
#[derive(Debug)]
pub struct WeaverDbCore {
    path: PathBuf,
    lock_file: Option<File>,
    engines: HashMap<EngineKey, StorageEngineDelegate>,
    default_engine: Option<EngineKey>,
    open_tables: RwLock<HashMap<(String, String), SharedTable>>,
    pub(crate) tx_coordinator: Option<TxCoordinator>,
    monitor: OnceLock<CoreMonitor>,
}

impl Default for WeaverDbCore {
    fn default() -> Self {
        Self::new().unwrap()
    }
}
impl WeaverDbCore {
    /// Creates a new weaver core in the current directory
    pub fn new() -> Result<Self, WeaverError> {
        Self::with_path(std::env::current_dir()?)
    }

    #[cfg(test)]
    pub fn in_temp_dir() -> Result<(tempfile::TempDir, Self), WeaverError> {
        use tempfile::TempDir;
        let tempdir = TempDir::new()?;
        Self::with_path(tempdir.as_ref()).map(|core| (tempdir, core))
    }

    /// Opens the weaver db core at the given paths.
    ///
    /// All table are opened relative to this path
    pub fn with_path<P: AsRef<Path>>(path: P) -> Result<Self, WeaverError> {
        let path = path.as_ref().to_path_buf();
        let lock_file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.join("weaver.lock"))?;

        #[cfg(not(miri))]
        {
            debug!("creating exclusive file lock");
            lock_file.try_lock_exclusive()?;
            debug!("exclusive file lock created");
        }

        debug!("starting core with config:");
        debug!(" - mmap: {}", cfg!(feature = "mmap"));

        let mut shard = Self {
            path,
            lock_file: Some(lock_file),
            engines: Default::default(),
            default_engine: None,
            open_tables: Default::default(),
            tx_coordinator: None,
            monitor: OnceLock::new(),
        };
        start_db(&mut shard)?;
        Ok(shard)
    }

    /// Insert an engine
    pub fn add_engine<T: StorageEngine + 'static>(&mut self, engine: T) {
        let engine_key = engine.engine_key().clone();
        trace!("registered storage engine {}", engine_key);
        self.engines
            .insert(engine_key, StorageEngineDelegate::new(engine));
    }

    /// Sets the default engine to use
    pub fn set_default_engine(&mut self, engine_key: EngineKey) {
        self.default_engine = Some(engine_key);
    }

    /// Gets the default engine key
    pub fn default_engine(&self) -> Option<&EngineKey> {
        self.default_engine.as_ref()
    }

    /// Start a transaction
    pub fn start_transaction(&self) -> Tx {
        match self.tx_coordinator {
            None => Tx::default(),
            Some(ref tx_coordinator) => tx_coordinator.next(),
        }
    }

    /// Add a table directly into the core
    pub fn add_table<T: DynamicTable + 'static>(&self, table: T) -> Result<(), WeaverError> {
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
            Ok(())
        } else {
            let mut open_tables = self.open_tables.write();
            let table: Table = Box::new(table);
            if let Some(monitor) = self.monitor.get() {
                monitor.collector.clone().push_monitorable(&*table);
            }
            open_tables.insert((schema, name), SharedTable::new(table));
            Ok(())
        }
    }

    /// Adds a table by a given schema
    pub fn open_table(&self, schema: &TableSchema) -> Result<(), WeaverError> {
        if self
            .open_tables
            .read()
            .contains_key(&(schema.schema().to_string(), schema.name().to_string()))
        {
            debug!(
                "open tables already contains {}.{}",
                schema.schema(),
                schema.name()
            );
            return Ok(());
        }

        let span = debug_span!(
            "open-table",
            schema = schema.schema(),
            table = schema.name(),
            engine = field::Empty
        );
        let enter = span.enter();

        debug!("opening table {}.{} ...", schema.schema(), schema.name());
        let mut open_tables = &mut self.open_tables.write();
        let engine = self
            .engines
            .get(schema.engine())
            .ok_or_else(|| WeaverError::UnknownStorageEngine(schema.engine().clone()))?;

        span.record("engine", engine.engine_key().to_string());

        if let Some(tables_table) = &open_tables.get(&("weaver".to_string(), "tables".to_string()))
        {
            let tx = Tx::default();
            let table_schema = &weaver_tables_schema()?;
            let schemata_schema = &weaver_schemata_schema()?;

            let schema_id = open_tables[&("weaver".to_string(), "schemata".to_string())]
                .all(&tx)?
                .into_iter()
                .find(|row| row[(schemata_schema, "name")].string_value() == Some(schema.schema()))
                .map(|row| row[(schemata_schema, "id")].clone().into_owned())
                .ok_or_else(|| WeaverError::SchemaNotFound(schema.schema().to_string()))?;

            if !tables_table.all(&tx)?.into_iter().any(|row| {
                *row[(table_schema, "schema_id")] == schema_id
                    && row[(table_schema, "name")].string_value() == Some(schema.name())
            }) {
                debug!("table {}.{} does not exist in weaver.tables, adding it now.", schema.schema(), schema.name());
                tables_table.insert(
                    &tx,
                    Row::from([
                        DbVal::Null,
                        schema_id,
                        DbVal::from(schema.name()),
                        DbVal::from(schema.to_sql()),
                        DbVal::from(serde_json::to_string(schema)?),
                        DbVal::Null,
                    ]),
                )?;
                tables_table.commit(&tx);
            }

            tx.commit();
        }
        let table = engine.factory().open(schema, self)?;

        if let Some(monitor) = self.monitor.get() {
            monitor.collector.clone().push_monitorable(&*table);
        }

        drop(enter);

        open_tables.insert(
            (schema.schema().to_string(), schema.name().to_string()),
            SharedTable::new(table),
        );

        Ok(())
    }

    /// Gets a table, if open. The table is responsible for handling shared-access.
    ///
    /// This method is not responsible for opening tables.
    pub fn get_open_table(&self, schema: &str, name: &str) -> Result<SharedTable, WeaverError> {
        self.open_tables
            .read()
            .get(&(schema.to_string(), name.to_string()))
            .cloned()
            .ok_or_else(|| WeaverError::NoTableFound {
                table: name.to_string(),
                schema: schema.to_string(),
            })
    }

    /// Gets all open tables
    pub fn get_open_tables(&self) -> impl Iterator<Item = SharedTable> {
        self.open_tables
            .read()
            .values()
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Closes a table
    pub fn close_table(&self, schema: &str, name: &str) -> Result<(), WeaverError> {
        self.open_tables
            .write()
            .remove(&(schema.to_string(), name.to_string()))
            .map(|_| ())
            .ok_or(WeaverError::NoTableFound {
                table: name.to_string(),
                schema: schema.to_string(),
            })
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
            let re = std::fs::remove_file(self.path.join("weaver.lock"));
            info!("lock file deleted: {}", re.is_ok());
        }
        info!("Shut down distro db core");
    }
}

impl Monitorable for WeaverDbCore {
    fn monitor(&self) -> Box<dyn Monitor> {
        Box::new(
            self.monitor
                .get_or_init(|| {
                    let mut monitor = CoreMonitor::default();
                    let guard = self.open_tables.read();
                    for (_, table) in guard.iter() {
                        let mut table_monitor = table.monitor();
                        monitor
                            .collector
                            .push(monitor_fn(table.schema().engine().clone(), move || {
                                table_monitor.stats()
                            }));
                    }

                    monitor
                })
                .clone(),
        )
    }
}

#[derive(Debug, Clone, Default)]
struct CoreMonitor {
    collector: MonitorCollector,
}

impl Monitor for CoreMonitor {
    fn name(&self) -> &str {
        "WeaverDbCore"
    }

    fn stats(&mut self) -> Stats {
        self.collector.all().mean()
    }
}
