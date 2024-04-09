//! The mechanisms responsible for executing queries

pub mod evaluation;
pub mod executor;
pub mod strategies;

pub use executor::QueryExecutor;
