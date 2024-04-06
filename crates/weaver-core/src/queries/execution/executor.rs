use std::borrow::Cow;
use std::cmp::Reverse;
use std::str::FromStr;
use std::sync::{Arc, Weak};

use indexmap::IndexMap;
use nom::combinator::cond;
use parking_lot::RwLock;
use rayon::prelude::*;
use tracing::trace;
use tracing::{debug, debug_span, error, info};

use weaver_ast::ast::{CreateDefinition, CreateTable, Literal, LoadData, OrderDirection};
use weaver_ast::{ast, parse_literal};

use crate::data::row::{OwnedRow, Row};
use crate::data::values::DbVal;
use crate::db::core::WeaverDbCore;
use crate::dynamic_table::{DynamicTable, EngineKey, HasSchema, Table};
use crate::error::WeaverError;
use crate::queries::execution::evaluation::ExpressionEvaluator;
use crate::queries::execution::strategies::join::{
    HashJoinTableStrategy, JoinParameters, JoinStrategy,
};
use crate::queries::query_plan::{QueryPlan, QueryPlanKind, QueryPlanNode};
use crate::rows::{KeyIndex, OwnedRows};
use crate::rows::{RefRows, Rows};
use crate::storage::tables::bpt_file_table::B_PLUS_TREE_FILE_KEY;
use crate::storage::tables::in_memory_table::InMemoryTable;
use crate::storage::tables::table_schema::TableSchemaBuilder;
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
        let ref expression_evaluator = ExpressionEvaluator::compile(plan, None)?;
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
            debug!("executing {}", <&'static str>::from(&node.kind));
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
                            let evaluated = expression_evaluator
                                .evaluate_one_row(
                                    condition,
                                    &row,
                                    to_filter.schema(),
                                    filtered.id(),
                                )?
                                .bool_value();
                            trace!("{row:?} . {condition} = {evaluated:?}");
                            if evaluated
                                == Some(true)
                            {
                                owned.push(row);
                            }
                        }

                        row_stack.push(Box::new(RefRows::new(node.schema.clone(), owned)));
                        Ok(())
                    })?;
                }
                QueryPlanKind::Project {
                    columns,
                    projected: _,
                } => {
                    debug_span!("project").in_scope(|| -> Result<(), WeaverError> {
                        let mut to_project = row_stack.pop().expect("nothing to project");
                        let mut owned = vec![];
                        while let Some(row) = to_project.next() {
                            let mut new_row = Row::new(columns.len());
                            debug!("projecting row {row:?}");
                            for (idx, column_expr) in columns.iter().enumerate() {
                                debug!("evaluating {column_expr}");
                                let eval = expression_evaluator.evaluate_one_row(
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
                QueryPlanKind::HashJoin {
                    left: _,
                    right: _,
                    join_kind,
                    on,
                } => {
                    debug_span!("join").in_scope(|| -> Result<(), WeaverError> {
                        let left = row_stack.pop().expect("no left side of join");
                        let right = row_stack.pop().expect("no right side of join");

                        let joined = HashJoinTableStrategy.try_join(JoinParameters {
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
                QueryPlanKind::CreateTable { table_def } => {
                    let CreateTable {
                        schema,
                        name,
                        create_definitions,
                    } = table_def;

                    let mut schema_builder = TableSchemaBuilder::new(
                        schema.as_ref().ok_or(WeaverError::NoDefaultSchema)?,
                        name,
                    );

                    for create_def in create_definitions {
                        match create_def {
                            &CreateDefinition::Column(ast::ColumnDefinition {
                                ref id,
                                data_type,
                                non_null,
                                auto_increment,
                                unique,
                                key,
                                primary,
                            }) => {
                                schema_builder = schema_builder.column(
                                    id,
                                    data_type.into(),
                                    non_null,
                                    None,
                                    auto_increment.then_some(0),
                                )?;
                                if unique || key && !primary {
                                    schema_builder = schema_builder.index(
                                        &format!("SK_{}", id),
                                        &[id.as_ref()],
                                        unique,
                                    )?
                                } else if primary {
                                    schema_builder = schema_builder.primary(&[id.as_ref()])?
                                }
                            }
                            &CreateDefinition::Constraint(_) => {
                                todo!()
                            }
                        }
                    }

                    schema_builder = schema_builder.engine(
                        core.read()
                            .default_engine()
                            .expect("no default engine")
                            .clone(),
                    );

                    let schema = schema_builder.build()?;
                    trace!("created schema {schema:#?} from ddl");

                    let result = core.read().open_table(&schema);
                    trace!("open table resulted in {:?}", result);
                    let as_row = Box::new(QueryPlan::ddl_result(result.map(|()| "ok")));
                    row_stack.push(as_row);
                    trace!("core after open: {:#?}", core.read());
                }
                QueryPlanKind::LoadData { load_data } => {
                    let LoadData {
                        infile,
                        schema,
                        name,
                        terminated_by,
                        lines_start,
                        lines_terminated,
                        skip,
                        columns,
                    } = load_data;
                    debug!("reading from csv: {infile:?}");
                    let mut csv_builder_reader = csv::ReaderBuilder::new();
                    csv_builder_reader.comment(Some(b'#'));

                    if let Some(terminated_by) = terminated_by {
                        csv_builder_reader.delimiter(terminated_by.as_bytes()[0]);
                    }

                    let mut csv_reader = csv_builder_reader
                        .from_path(infile)
                        .map_err(|e| WeaverError::custom(e))?;

                    let table = core
                        .read()
                        .get_open_table(schema.as_ref().expect("no schema"), name)?;

                    let column_indexes_and_types = columns.iter().try_fold(
                        Vec::with_capacity(columns.len()),
                        |mut vec, next| -> Result<_, WeaverError> {
                            let column_idx = table
                                .schema()
                                .column_index(next.as_ref())
                                .ok_or_else(|| WeaverError::ColumnNotFound(next.to_string()))?;
                            let column_type = table.schema().columns()[column_idx].data_type();
                            vec.push((column_idx, column_type));
                            Ok(vec)
                        },
                    )?;

                    let mut iter = csv_reader.records();
                    let mut rows = iter
                        .into_iter()
                        .par_bridge()
                        .map(|line| {
                            let Ok(line) = line else {
                                return Err(WeaverError::custom(line.unwrap_err()));
                            };

                            let mut row = vec![DbVal::Null; table.schema().columns().len()];
                            column_indexes_and_types
                                .iter()
                                .zip(line.iter())
                                .try_for_each(
                                    |(&(col_idx, db_type), string)| -> Result<_, WeaverError> {
                                        let db_val = db_type.parse_value(string)?;

                                        row[col_idx] = db_val;
                                        Ok(())
                                    },
                                )?;
                            Ok(Row::from(row))
                        })
                        .collect::<Result<Vec<_>, WeaverError>>()?;
                    trace!("rows created: {}", rows.len());

                    let result = rows
                        .into_iter()
                        .map(|row| table.insert(tx, row))
                        .collect::<Result<Vec<_>, _>>();

                    let as_row = Box::new(QueryPlan::ddl_result(result.map(|vec| vec.len())));
                    row_stack.push(as_row);
                }
                QueryPlanKind::GroupBy {
                    grouped: _,
                    grouped_by,
                    result_columns,
                } => {
                    let mut grouped = row_stack.pop().expect("no rows to group");
                    let mut grouped_rows = IndexMap::<Vec<DbVal>, Vec<Row>>::new();

                    while let Some(row) = grouped.next() {
                        let grouping = grouped_by
                            .iter()
                            .map(|expr| {
                                expression_evaluator
                                    .evaluate_one_row(expr, &row, grouped.schema(), node.id())
                                    .map(|val| val.as_ref().to_owned())
                            })
                            .collect::<Result<Vec<_>, _>>()?;

                        grouped_rows.entry(grouping).or_default().push(row);
                    }

                    trace!("grouped rows: {:#?}", grouped_rows);
                    let mut owned = vec![];
                    for (grouping, rows) in grouped_rows {
                        let mut new_row = Row::new(result_columns.len());
                        for (idx, column_expr) in result_columns.iter().enumerate() {
                            trace!("evaluating {column_expr}");
                            let eval = expression_evaluator.evaluate_many_rows(
                                column_expr,
                                &rows,
                                grouped.schema(),
                                node.id(),
                            )?;
                            trace!("got {eval}");
                            new_row[idx] = Cow::Owned(eval.as_ref().clone());
                        }

                        owned.push(new_row);
                    }

                    row_stack.push(Box::new(RefRows::new(node.schema.clone(), owned)));
                }
                QueryPlanKind::OrderedBy { order, .. } => {
                    let mut ordered = row_stack.pop().expect("nothing to order");
                    let mut order_vec = vec![];

                    while let Some(row) = ordered.next() {
                        order_vec.push(row);
                    }

                    for (expr, order) in order.iter().rev() {
                        match order {
                            OrderDirection::Asc => order_vec.sort_by_cached_key(|row| {
                                expression_evaluator
                                    .evaluate_one_row(expr, row, ordered.schema(), node.id())
                                    .expect("couldn't evaluate")
                                    .as_ref()
                                    .clone()
                            }),
                            OrderDirection::Desc => order_vec.sort_by_cached_key(|row| {
                                Reverse(
                                    expression_evaluator
                                        .evaluate_one_row(expr, row, ordered.schema(), node.id())
                                        .expect("couldn't evaluate")
                                        .as_ref()
                                        .clone()
                                )
                            }),
                        }
                    }

                    row_stack.push(Box::new(RefRows::new(node.schema.clone(), order_vec)));
                }
                &QueryPlanKind::GetPage {
                    offset, limit, ..
                } => {
                    let mut base = row_stack.pop().expect("no rows to offset");
                    let mut done = false;

                    for _ in 0..offset {
                        if let Some(_) = base.next() {
                        } else {
                            done = true;
                            break;
                        }
                    }

                    if !done {
                        if let Some(limit) = limit {
                            let mut limited = vec![];
                            for _ in 0..limit {
                                if let Some(row) = base.next() {
                                    limited.push(row);
                                } else {
                                    break;
                                }
                            }
                            row_stack.push(Box::new(RefRows::new(node.schema.clone(), limited)));
                        } else {
                            row_stack.push(base);
                        }
                    } else {
                        row_stack.push(Box::new(RefRows::new(node.schema.clone(), vec![])));
                    }
                }
                _kind => {
                    todo!("implement execution of {_kind:#?}");
                }
            }
        }

        let result = row_stack.pop().expect("no rows object at top of stack");
        Ok(OwnedRows::from(result))
    }
}
