//! Every action within a query has an associated cost.
//!
//! Every cost has a base value and a row factor, such that the cost
//! of doing an operation is `base * rows^row_factor`. For example, loading a table takes
//! the same cost regardless of amount of rows, so the row factor will be 0, a select would
//! have a row factor of 1, and a merge could have a row factor of 2.

use std::collections::HashMap;
use std::ops::Mul;

/// Represents the cost of an operation over some unknown amount of rows
#[derive(Debug, Copy, Clone)]
pub struct Cost {
    /// The base cost per `row^row_factor`
    pub base: f64,
    /// The exponent the rows are raised to
    pub row_factor: u32,
}

impl Cost {
    /// Gets the final cost. All values are saturated
    pub fn get_cost(&self, rows: usize) -> f64 {
        let row_cost = rows.saturating_pow(self.row_factor);
        self.base.mul(row_cost as f64)
    }
}

/// The cost table stores information about the cost of given operations
#[derive(Debug, Clone)]
pub struct CostTable {
    table: HashMap<String, Cost>,
}

static QUERY_COSTS: &[(&str, Cost)] = &[];

impl Default for CostTable {
    fn default() -> Self {
        Self {
            table: QUERY_COSTS
                .iter()
                .map(|&(key, cost)| (key.to_string(), cost))
                .collect(),
        }
    }
}

impl CostTable {
    /// Creates a cost table with default entries
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a cost within the table.
    ///
    /// Has no effect if the given key isn't already present
    pub fn set(&mut self, key: impl AsRef<str>, value: Cost) {
        if let Some(value_ref) = self.table.get_mut(key.as_ref()) {
            *value_ref = value;
        }
    }

    /// Gets a cost by key
    pub fn get(&self, key: impl AsRef<str>) -> Option<&Cost> {
        self.table.get(key.as_ref())
    }
}
