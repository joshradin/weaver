//! Every action within a query has an associated cost.
//!
//! Every cost has a base value and a row factor, such that the cost
//! of doing an operation is `base * rows^row_factor`. For example, loading a table takes
//! the same cost regardless of amount of rows, so the row factor will be 0, a select would
//! have a row factor of 1, and a merge could have a row factor of 2.

use crate::data::row::Row;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Mul;

use crate::data::values::DbVal;
use crate::dynamic_table::DynamicTable;
use crate::error::WeaverError;
use crate::tx::Tx;

/// Represents the cost of an operation over some unknown amount of rows
#[derive(Copy, Clone, Debug)]
pub struct Cost {
    /// The base cost per `(log)row^row_factor`
    pub base: f64,
    /// The exponent the rows are raised to
    pub row_factor: u32,
    /// if present, the number of rows are first ran through the log
    pub row_log: Option<u32>,
}

impl Cost {
    /// Create a new cost struct
    pub const fn new(base: f64, row_factor: u32, row_log: Option<u32>) -> Self {
        Self {
            base,
            row_factor,
            row_log,
        }
    }
    /// Gets the final cost. All values are saturated
    pub fn get_cost(&self, rows: usize) -> f64 {
        let rows: usize = if let Some(log_base) = self.row_log {
            rows.max(log_base as usize)
                .checked_ilog(log_base as usize)
                .unwrap_or(0) as usize
        } else {
            rows
        };

        let row_cost = rows.saturating_pow(self.row_factor);
        self.base.mul(row_cost as f64)
    }
}

impl PartialEq for Cost {
    fn eq(&self, other: &Self) -> bool {
        self.row_factor == other.row_factor && self.base.total_cmp(&other.base) == Ordering::Equal
    }
}

impl Eq for Cost {}
impl PartialOrd for Cost {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Cost {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.row_factor, self.row_log).cmp(&(other.row_factor, other.row_log)) {
            Ordering::Equal => self.base.total_cmp(&self.base),
            other => other,
        }
    }
}

/// The cost table stores information about the cost of given operations
#[derive(Debug, Clone, PartialEq)]
pub struct CostTable {
    table: HashMap<String, Cost>,
}

static QUERY_COSTS: &[(&str, Cost)] = &[
    ("LOAD_TABLE", Cost::new(1.4, 1, Some(16))),
    ("GROUP_BY", Cost::new(1.3, 1, None)),
    ("PROJECT", Cost::new(1.0, 1, None)),
    ("JOIN", Cost::new(1.0, 1, None)),
    ("FILTER", Cost::new(1.0, 1, None)),
    ("ORDER", Cost::new(1.0, 2, None)),
    ("LIMIT-OFFSET", Cost::new(1.0, 1, None)),
];

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

    pub fn from_table<T: DynamicTable + ?Sized>(table: &T, tx: &Tx) -> Self {
        let mut output = Self::new();
        let all = table.all(tx).expect("could not get all rows");
        for row in all.into_iter() {
            let DbVal::String(key, _) = &*row[0] else {
                panic!("first column is key")
            };
            let &DbVal::Float(base) = &*row[1] else {
                panic!("second column is base")
            };
            let &DbVal::Integer(row_factor) = &*row[2] else {
                panic!("third column is row factor")
            };
            let row_log = match &*row[2] {
                &DbVal::Integer(i) => Some(i as u32),
                DbVal::Null => None,
                _ => panic!("fourth column is optional log base"),
            };
            output.set(
                key,
                Cost {
                    base,
                    row_factor: row_factor as u32,
                    row_log,
                },
            )
        }
        output
    }

    pub fn flush_to_table<T: DynamicTable + ?Sized>(
        &self,
        table: &T,
        tx: &Tx,
    ) -> Result<(), WeaverError> {
        for (id, cost) in &self.table {
            let row = Row::from([
                DbVal::from(id),
                DbVal::from(cost.base),
                DbVal::from(cost.row_factor as i64),
                DbVal::from(cost.row_log.map(|i| i as i64)),
            ]);
            table.insert(tx, row)?;
        }
        Ok(())
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
