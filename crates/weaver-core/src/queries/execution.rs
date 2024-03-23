//! The mechanisms responsible for executing queries

pub mod executor;
pub mod strategies;
pub mod evaluation;

pub use executor::QueryExecutor;
