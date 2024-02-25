//! Strategies for the executors

use std::fmt::{Display, Formatter};

pub mod join;

/// Base strategy trait
pub trait Strategy {
    /// Gets the name of the strategy
    fn name(&self) -> &str;
}
