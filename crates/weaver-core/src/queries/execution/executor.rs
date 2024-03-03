use std::sync::{Arc, Weak};

use parking_lot::RwLock;
use tracing::{error, info};

use crate::db::core::WeaverDbCore;
use crate::dynamic_table::{DynamicTable, HasSchema, Table};
use crate::error::WeaverError;
use crate::queries::execution::executor::expr_executor::ExpressionEvaluator;
use crate::queries::execution::strategies::join::JoinParameters;
use crate::queries::query_plan::{QueryPlan, QueryPlanKind, QueryPlanNode};
use crate::rows::Rows;
use crate::rows::{KeyIndex, OwnedRows};
use crate::storage::tables::in_memory_table::InMemoryTable;
use crate::tx::Tx;

mod expr_executor;

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
    pub fn new(core: Weak<RwLock<WeaverDbCore>>) -> Self {
        Self { core }
    }
}

impl QueryExecutor {
    /// Executes a query
    pub fn execute(&self, tx: &Tx, plan: &QueryPlan) -> Result<Box<dyn Rows<'_>>, WeaverError> {
        let root = plan.root();
        let core = self.core.upgrade().ok_or(WeaverError::NoCoreAvailable)?;
        info!("executing query plan {plan:#?}");
        let ref expression_evaluator = ExpressionEvaluator::compile(plan)?;
        let final_table = self.execute_node(tx, root, expression_evaluator, &core)?;
        Ok(Box::new(OwnedRows::from(final_table)))
    }

    fn execute_node(
        &self,
        tx: &Tx,
        node: &QueryPlanNode,
        expression_evaluator: &ExpressionEvaluator,
        core: &Arc<RwLock<WeaverDbCore>>,
    ) -> Result<Box<dyn Rows<'_>>, WeaverError> {
        match &node.kind {
            QueryPlanKind::TableScan {
                schema,
                table,
                keys,
            } => {
                let table = {
                    let core = core.read();
                    core.get_open_table(schema, table)?
                };
                let key_index = keys
                    .as_ref()
                    .and_then(|keys| keys.get(0).cloned())
                    .unwrap_or_else(|| {
                        table
                            .schema()
                            .full_index()
                            .expect("no way of getting all from table")
                    });
                let read = table
                    .read(tx, &key_index)?
                    .map(|row| table.schema().public_only(row))
                    .to_owned()
                    ;

                Ok(Box::new(read))
            }
            QueryPlanKind::Filter {
                filtered,
                condition,
            } => {
                let to_filter = self.execute_node(tx, &*filtered, expression_evaluator, core)?;


                todo!("filter")
            }
            QueryPlanKind::Project { columns, node } => {
                let to_project = self.execute_node(tx, &*node, expression_evaluator, core)?;

                todo!("projection")
            }
            QueryPlanKind::Join {
                left,
                right,
                join_kind,
                on,
                strategies,
            } => {
                let left = self.execute_node(tx, left, expression_evaluator, core)?;
                let right = self.execute_node(tx, right, expression_evaluator, core)?;

                let strategy = strategies.first().unwrap();

                strategy.try_join(JoinParameters {
                    op: join_kind.clone(),
                    left,
                    right,
                    constraint: on.clone(),
                })
            }
            _kind => {
                todo!("implement execution of {_kind:?}")
            }
        }
    }
}