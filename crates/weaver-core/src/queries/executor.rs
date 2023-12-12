//! The mechanism responsible for executing queries

use std::sync::Weak;
use parking_lot::RwLock;

use crate::db::core::WeaverDbCore;
use crate::dynamic_table::Table;
use crate::error::Error;
use crate::queries::query_plan::{QueryPlan, QueryPlanKind};
use crate::tables::InMemoryTable;
use crate::tx::Tx;

/// The query executor is responsible for executing queries against the database
/// in performant ways.
///
/// Weaver Db's only have access to a finite amount of query executors, and are on the core level.
/// This means they only have access to the [`WeaverDbCore`](WeaverDbCore) object.
///
/// They are responsible for *just* executing query plans, and nothing more.
#[derive(Debug)]
pub struct QueryExecutor {
    core: Weak<RwLock<WeaverDbCore>>,
}

impl QueryExecutor {
    pub(crate) fn new(core: Weak<RwLock<WeaverDbCore>>) -> Self {
        Self { core }
    }
}

impl QueryExecutor {
    /// Executes a query
    pub fn execute(&self, tx: &Tx, plan: &QueryPlan) -> Result<Table, Error> {
        let root = plan.root();
        let core = self.core.upgrade().ok_or(Error::NoCoreAvailable)?;
        let mut stack = vec![root];
        let mut output: Option<Table> = None;

        while !stack.is_empty() {
            let node = stack.pop().unwrap();

            match &node.kind {
                QueryPlanKind::SelectByKey { table: (schema, name), key_index } => {
                    let core = core.read();
                    let table = core.get_table(schema, name).ok_or(Error::NoTableFound(schema.to_string(), name.to_string()))?;

                    let read = table.read(tx, &key_index[0])?;
                    output = Some(Box::new(InMemoryTable::from_rows(table.schema().clone(), read)?));
                }
                QueryPlanKind::Project { .. } => {}
            }
        }


        Ok(output.expect("no table"))
    }
}
