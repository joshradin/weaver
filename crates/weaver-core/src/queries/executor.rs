//! The mechanism responsible for executing queries

use parking_lot::RwLock;
use std::sync::{Arc, Weak};
use tracing::{error, event, info};

use crate::db::core::WeaverDbCore;
use crate::dynamic_table::{HasSchema, Table};
use crate::error::Error;
use crate::queries::query_plan::{QueryPlan, QueryPlanKind, QueryPlanNode};
use crate::rows::{OwnedRows, Rows, RowsExt};
use crate::storage::tables::InMemoryTable;
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
    pub fn execute(&self, tx: &Tx, plan: &QueryPlan) -> Result<OwnedRows, Error> {
        let root = plan.root();
        let core = self.core.upgrade().ok_or(Error::NoCoreAvailable)?;
        let mut output: Option<Table> = None;
        info!("executing query plan {plan:#?}");

        let final_table = self.execute_node(tx, root, &core)?;
        final_table.all(tx).map(|rows| {
            rows.map(|row| final_table.schema().public_only(row))
                .to_owned()
        })
    }

    fn execute_node(
        &self,
        tx: &Tx,
        node: &QueryPlanNode,
        core: &Arc<RwLock<WeaverDbCore>>,
    ) -> Result<Table, Error> {
        match &node.kind {
            QueryPlanKind::SelectByKey {
                to_select,
                key_index,
            } => {
                let table = self.execute_node(tx, to_select, core)?;

                let read = table
                    .read(tx, &key_index[0])?
                    .map(|row| table.schema().public_only(row));
                let in_memory = match InMemoryTable::from_rows(table.schema().clone(), read) {
                    Ok(table) => table,
                    Err(e) => {
                        error!("creating in memory table from select result failed: {e}");
                        if let Error::BadColumnCount { .. } = &e {
                            error!("table schema: {:#?}", table.schema())
                        }
                        return Err(e);
                    }
                };
                Ok(Box::new(in_memory))
            }
            QueryPlanKind::Project { .. } => {
                todo!("projection")
            }
            QueryPlanKind::LoadTable { schema, table } => {
                let core = core.read();

                let table = core.get_open_table(schema, table)?;
                Ok(Box::new(table))
            }
        }
    }
}
