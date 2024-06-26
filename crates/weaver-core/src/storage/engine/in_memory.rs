//! The in_memory engine

use crate::dynamic_table::EngineKey;
use crate::monitoring::{monitor_fn, Monitor, Monitorable};
use crate::storage::engine::StorageEngine;
use crate::storage::tables::in_memory_table::{InMemoryTableFactory, IN_MEMORY_KEY};

/// In memory engine
#[derive(Debug)]
pub struct InMemoryEngine {
    key: EngineKey,
}

impl Default for InMemoryEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryEngine {
    pub fn new() -> Self {
        Self {
            key: EngineKey::new(IN_MEMORY_KEY),
        }
    }
}

impl Monitorable for InMemoryEngine {
    fn monitor(&self) -> Box<dyn Monitor> {
        Box::new(monitor_fn("InMemoryEngine", || {}))
    }
}

impl StorageEngine for InMemoryEngine {
    type Factory = InMemoryTableFactory;

    fn factory(&self) -> Self::Factory {
        InMemoryTableFactory
    }

    fn engine_key(&self) -> &EngineKey {
        &self.key
    }
}
