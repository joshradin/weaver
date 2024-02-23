//! The db is responsible for building tables

use crate::dynamic_table_factory::DynamicTableFactory;
use std::fmt::Debug;

pub mod core;
pub mod server;

mod start_db;
mod start_server;

pub static SYSTEM_SCHEMA: &'static str = "weaver";
