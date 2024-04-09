//! The db is responsible for building tables




pub mod core;
pub mod server;

mod start_db;
mod start_server;

pub static SYSTEM_SCHEMA: &str = "weaver";
