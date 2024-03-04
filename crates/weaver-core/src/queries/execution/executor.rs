use std::borrow::Cow;
use std::sync::{Arc, Weak};

use parking_lot::RwLock;
use tracing::log::trace;
use tracing::{debug, debug_span, error, info};

use crate::data::row::{OwnedRow, Row};
use crate::data::values::DbVal;
use crate::db::core::WeaverDbCore;
use crate::dynamic_table::{DynamicTable, HasSchema, Table};
use crate::error::WeaverError;
use crate::queries::execution::executor::expr_executor::ExpressionEvaluator;
use crate::queries::execution::strategies::join::JoinParameters;
use crate::queries::query_plan::{QueryPlan, QueryPlanKind, QueryPlanNode};
use crate::rows::{KeyIndex, OwnedRows};
use crate::rows::{RefRows, Rows};
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
    pub fn execute(&self, tx: &Tx, plan: &QueryPlan) -> Result<OwnedRows, WeaverError> {
        if matches!(plan.root().kind, QueryPlanKind::Explain { .. }) {
            let explained = plan.as_rows();
            return Ok(explained);
        }

        let core = self.core.upgrade().ok_or(WeaverError::NoCoreAvailable)?;
        trace!("executing query plan {plan:#?}");
        let ref expression_evaluator = ExpressionEvaluator::compile(plan)?;
        self.execute_node_non_recursive(tx, plan.root(), expression_evaluator, &core)
    }

    /// executes the nodes in DFS post order traversal.
    ///
    /// Probably easiest to create the pre order list stack first, then just pop from the stack
    fn execute_node_non_recursive<'tx>(
        &self,
        tx: &'tx Tx,
        root: &QueryPlanNode,
        expression_evaluator: &ExpressionEvaluator,
        core: &Arc<RwLock<WeaverDbCore>>,
    ) -> Result<OwnedRows, WeaverError> {
        let mut stack = root.prefix_order();
        let mut row_stack: Vec<Box<dyn Rows>> = vec![];

        while let Some(node) = stack.pop() {
            match &node.kind {
                QueryPlanKind::TableScan {
                    schema,
                    table,
                    keys,
                } => {
                    debug_span!("table-scan").in_scope(|| -> Result<(), WeaverError> {
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
                            .to_owned();

                        row_stack.push(Box::new(read));
                        Ok(())
                    })?;
                }
                QueryPlanKind::Filter {
                    filtered,
                    condition,
                } => {
                    debug_span!("filter").in_scope(|| -> Result<(), WeaverError> {
                        let mut to_filter = row_stack.pop().expect("nothing to filter");
                        let mut owned = vec![];
                        while let Some(row) = to_filter.next() {
                            if expression_evaluator
                                .evaluate(condition, &row, to_filter.schema(), filtered.id())?
                                .bool_value()
                                == Some(true)
                            {
                                owned.push(row);
                            }
                        }

                        row_stack.push(Box::new(RefRows::new(node.schema.clone(), owned)));
                        Ok(())
                    })?;
                }
                QueryPlanKind::Project { columns, node: _ } => {
                    debug_span!("project").in_scope(|| -> Result<(), WeaverError> {
                        let mut to_project = row_stack.pop().expect("nothing to project");
                        let mut owned = vec![];
                        while let Some(row) = to_project.next() {
                            let mut new_row = Row::new(columns.len());
                            debug!("projecting row {row:?}");
                            for (idx, column_expr) in columns.iter().enumerate() {
                                debug!("evaluating {column_expr}");
                                let eval = expression_evaluator.evaluate(
                                    column_expr,
                                    &row,
                                    to_project.schema(),
                                    node.id(),
                                )?;
                                debug!("got {eval}");
                                new_row[idx] = Cow::Owned(eval.as_ref().clone());
                            }

                            owned.push(new_row);
                        }

                        row_stack.push(Box::new(RefRows::new(node.schema.clone(), owned)));
                        Ok(())
                    })?;
                }
                QueryPlanKind::Join {
                    left: _,
                    right: _,
                    join_kind,
                    on,
                    strategies,
                } => {
                    debug_span!("join").in_scope(|| -> Result<(), WeaverError> {
                        let left = row_stack.pop().expect("no left side of join");
                        let right = row_stack.pop().expect("no right side of join");

                        let (strategy, _) = strategies.first().unwrap();

                        let joined = strategy.try_join(JoinParameters {
                            op: join_kind.clone(),
                            left,
                            right,
                            constraint: on.clone(),
                            schema: node.schema.clone(),
                        })?;
                        row_stack.push(joined);
                        Ok(())
                    })?;
                }
                _kind => {
                    todo!("implement execution of {_kind:?}")
                }
            }
        }

        let result = row_stack.pop().expect("no row at top of stack");
        Ok(OwnedRows::from(result))
    }
}
