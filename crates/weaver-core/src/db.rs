//! The db is responsible for building tables

use crate::dynamic_table::StorageEngineFactory;
use std::fmt::Debug;

pub mod server;
pub mod core;
mod start_db;
mod start_server;
