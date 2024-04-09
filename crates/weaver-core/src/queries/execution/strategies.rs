//! Strategies for the executors

pub mod join;

/// Base strategy trait
pub trait Strategy {
    /// Gets the name of the strategy
    fn name(&self) -> &str;
}
