//! Storage engines are responsible for being the in-between of a [DynamicTable](crate::dynamic_table::DynamicTable)
//! and some storage device.
//!
//! This storage device can be in memory or defined using [StorageDevice](crate::storage::StorageDevice).
//! Storage engines should have some associated [DynamicTableFactory](DynamicTableFactory) that can be
//! called upon from the weaver core.
//!
//! ```text
//!                    +---------+--------+
//!              - - > | factory | eng. A | - - > ( storage / memory )
//!              |     +---------+--------+
//! +--------+   |     +---------+--------+
//! |  core  | - + - > | factory | eng. B | - - > ( storage / memory )
//! +--------+   |     +---------+--------+
//!              |     +---------+--------+
//!              - - > | factory | eng. C | - - > ( storage / memory )
//!                    +---------+--------+
//!
//! ```

use std::fmt::{Debug, Formatter};

use static_assertions::assert_obj_safe;

use crate::dynamic_table::EngineKey;
use crate::dynamic_table_factory::{DynamicTableFactory, DynamicTableFactoryDelegate};
use crate::monitoring::{Monitor, Monitorable, SharedMonitor};
use crate::tables::in_memory_table::InMemoryTableFactory;

pub mod in_memory;
pub mod weave_bptf;

/// A storage engine provides a dynamic table factory with some storage/memory backend.
pub trait StorageEngine : Monitorable + Send + Sync {
    type Factory : DynamicTableFactory;

    /// Provides a factory for creating tables
    fn factory(&self) -> Self::Factory;

    /// the engine key
    fn engine_key(&self) -> &EngineKey;
}

assert_obj_safe!(StorageEngine<Factory=InMemoryTableFactory>);



/// A delegate to a storage engine, allowing for object save
pub struct StorageEngineDelegate {
    factory: Box<dyn Fn() -> DynamicTableFactoryDelegate + Send + Sync>,
    monitor: SharedMonitor,
    engine_key: EngineKey
}

impl Debug for StorageEngineDelegate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageEngineDelegate")
            .field("engine_key", &self.engine_key)
            .finish_non_exhaustive()
    }
}

impl StorageEngineDelegate {

    /// Create a new storage engine delegate from a storage engine
    pub fn new<T : StorageEngine + 'static>(storage_engine: T) -> Self {
        let engine_key = storage_engine.engine_key().clone();
        let monitor = SharedMonitor::from(storage_engine.monitor());
        let func = Box::new(move || {
            let factory = storage_engine.factory();
            DynamicTableFactoryDelegate::new(factory)
        });

        Self {
            factory: func,
            monitor,
            engine_key,
        }
    }
}

impl Monitorable for StorageEngineDelegate {
    fn monitor(&self) -> Box<dyn Monitor> {
        Box::new(self.monitor.clone())
    }
}

impl StorageEngine for StorageEngineDelegate {
    type Factory = DynamicTableFactoryDelegate;

    fn factory(&self) -> Self::Factory {
        (self.factory)()
    }

    fn engine_key(&self) -> &EngineKey {
        &self.engine_key
    }
}
