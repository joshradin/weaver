//! The in_memory engine

use crate::dynamic_table::EngineKey;
use crate::monitoring::{Monitor, monitor_fn, Monitorable};
use crate::storage::engine::StorageEngine;
use crate::tables::in_memory_table::{IN_MEMORY_KEY, InMemoryTableFactory};

/// In memory engine
#[derive(Debug)]
pub struct InMemoryEngine {
    key: EngineKey
}

impl InMemoryEngine {
    pub fn new() -> Self {
        Self { key: EngineKey::new(IN_MEMORY_KEY)}
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