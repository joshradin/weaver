//! `weaveBPTF` storage engine, providing for file-per-table b plus tree implementations
//!

use std::collections::HashSet;
use std::iter;
use std::path::{Path, PathBuf};

use crate::dynamic_table::EngineKey;
use crate::monitoring::{monitor_fn, Monitor, Monitorable, Stats};
use crate::storage::engine::StorageEngine;
use crate::storage::tables::bpt_file_table::{BptfTableFactory, B_PLUS_TREE_FILE_KEY};

/// The weave bptf engine
#[derive(Debug)]
pub struct WeaveBPTFEngine {
    engine_key: EngineKey,
    root: PathBuf,
    _alts: HashSet<PathBuf>,
}

impl WeaveBPTFEngine {
    /// Creates a new engine at a given root directory
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self::with_alts(root, iter::empty())
    }

    /// Creates a new engine with a given root, and can open tables that already exist in alternate
    /// paths.
    pub fn with_alts(root: impl AsRef<Path>, alts: impl IntoIterator<Item = PathBuf>) -> Self {
        Self {
            engine_key: EngineKey::new(B_PLUS_TREE_FILE_KEY),
            root: root.as_ref().to_path_buf(),
            _alts: alts.into_iter().collect(),
        }
    }
}

impl Monitorable for WeaveBPTFEngine {
    fn monitor(&self) -> Box<dyn Monitor> {
        Box::new(monitor_fn("WeaveBTPFEngine", || Stats::Null))
    }
}

impl StorageEngine for WeaveBPTFEngine {
    type Factory = BptfTableFactory;

    fn factory(&self) -> Self::Factory {
        BptfTableFactory::new(&self.root)
    }

    fn engine_key(&self) -> &EngineKey {
        &self.engine_key
    }
}
