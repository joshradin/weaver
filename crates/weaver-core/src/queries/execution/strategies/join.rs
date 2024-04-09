//! Join strategies

use std::borrow::Cow;

use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use static_assertions::assert_obj_safe;
use tracing::{debug, instrument, trace, Level};

use weaver_ast::ast::{BinaryOp, Expr, JoinClause, JoinConstraint, JoinOperator};

use crate::data::row::Row;
use crate::data::values::DbVal;
use crate::db::server::WeakWeaverDb;
use crate::dynamic_table::{HasSchema};
use crate::error::WeaverError;

use crate::queries::execution::strategies::Strategy;
use crate::queries::query_cost::Cost;
use crate::queries::query_plan::{QueryPlanKind, QueryPlanNode};
use crate::rows::{RefRows, Rows};
use crate::storage::tables::table_schema::TableSchema;


/// A join strategy
pub trait JoinStrategy: Strategy {
    /// Sees if this join strategy can be run upon the given join clause.
    ///
    /// If it can, returns a [`Some(Cost)`](Cost) struct to determine how expensive the operation is, otherwise,
    /// [`None`](None) is returned.
    fn join_cost(&self, join_parameters: &JoinClause) -> Option<Cost>;

    /// Responsible for creating the join query node
    fn join_node(
        &self,
        rows: u64,
        left: QueryPlanNode,
        right: QueryPlanNode,
        join_clause: &JoinClause,
    ) -> Result<QueryPlanNode, WeaverError>;

    /// Attempts to perform a join
    fn try_join<'r>(
        &self,
        join_parameters: JoinParameters<'r>,
    ) -> Result<Box<dyn Rows<'r> + 'r>, WeaverError>;
}

impl Debug for dyn JoinStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "JoinStrategy[{}]", self.name())
    }
}

impl Display for dyn JoinStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

assert_obj_safe!(JoinStrategy);

#[derive(Debug)]
pub struct JoinParameters<'a> {
    pub op: JoinOperator,
    pub left: Box<dyn Rows<'a>>,
    pub right: Box<dyn Rows<'a>>,
    pub constraint: JoinConstraint,
    pub schema: TableSchema,
}

/// Responsible for selecting a join strategy
#[derive(Debug)]
pub struct JoinStrategySelector {
    db: WeakWeaverDb,
    strategies: Vec<Arc<dyn JoinStrategy>>,
}

impl JoinStrategySelector {
    /// Creates a new join strategy selector
    pub fn new(db: WeakWeaverDb) -> Self {
        Self {
            db,
            strategies: vec![],
        }
        .with_strategy(HashJoinTableStrategy)
    }

    /// Adds a strategy to the selector
    pub fn with_strategy<S: JoinStrategy + 'static>(mut self, strategy: S) -> Self {
        self.strategies
            .push(Arc::from(Box::new(strategy) as Box<dyn JoinStrategy>));
        self
    }

    /// Adds many strategies to the selector
    pub fn with_strategies<S: JoinStrategy + 'static, I: IntoIterator<Item = S>>(
        mut self,
        strategies: I,
    ) -> Self {
        self.strategies.extend(
            strategies
                .into_iter()
                .map(|s| Arc::from(Box::new(s) as Box<dyn JoinStrategy>)),
        );
        self
    }

    /// Gets all applicable strategies for a given join
    #[instrument(level = Level::TRACE, skip(self), fields(join=%join), ret, err)]
    pub fn get_strategies_for_join(
        &self,
        join: &JoinClause,
    ) -> Result<Vec<(Arc<dyn JoinStrategy>, Cost)>, WeaverError> {
        let mut vec = self
            .strategies
            .iter()
            .inspect(|strat| trace!("checking if {strat:?} is applicable"))
            .filter_map(|strat| strat.join_cost(join).map(|cost| (strat, cost)))
            .collect::<Vec<_>>();
        if vec.is_empty() {
            return Err(WeaverError::NoStrategyForJoin(join.clone()));
        }
        vec.sort_by_key(|c| c.1);
        Ok(vec
            .into_iter()
            .map(|(arc, cost)| (arc.clone(), cost))
            .collect())
    }
}

#[derive(Debug)]
pub struct HashJoinTableStrategy;

impl Strategy for HashJoinTableStrategy {
    fn name(&self) -> &str {
        "hash-eq"
    }
}

impl JoinStrategy for HashJoinTableStrategy {
    fn join_cost(&self, join_parameters: &JoinClause) -> Option<Cost> {
        if join_parameters.op != JoinOperator::Inner {
            return None;
        }

        let Expr::Binary {
            left,
            op: BinaryOp::Eq,
            right,
        } = &join_parameters.constraint.on
        else {
            return None;
        };

        let Expr::Column {
            column: left_column,
        } = &**left
        else {
            return None;
        };

        let Expr::Column {
            column: right_column,
        } = &**right
        else {
            return None;
        };

        debug!("can run a hash-join on {left_column} and {right_column}");
        Some(Cost::new(1.1, 1, None))
    }

    fn join_node(
        &self,
        rows: u64,
        left: QueryPlanNode,
        right: QueryPlanNode,
        join_clause: &JoinClause,
    ) -> Result<QueryPlanNode, WeaverError> {
        let JoinClause {
            op,
            constraint,
            ..
        } = join_clause;
        let target_schema = left.schema().join(right.schema());
        Ok(QueryPlanNode::builder()
            .cost(self.join_cost(join_clause).unwrap())
            .rows(rows)
            .kind(QueryPlanKind::HashJoin {
                left: Box::new(left),
                right: Box::new(right),
                join_kind: op.clone(),
                on: constraint.clone(),
            })
            .schema(target_schema)
            .build()?)
    }

    fn try_join<'r>(
        &self,
        join_parameters: JoinParameters<'r>,
    ) -> Result<Box<dyn Rows<'r> + 'r>, WeaverError> {
        let Expr::Binary {
            left,
            op: BinaryOp::Eq,
            right,
        } = &join_parameters.constraint.on
        else {
            unreachable!();
        };
        let Expr::Column {
            column: left_column,
        } = &**left
        else {
            unreachable!();
        };

        let Expr::Column {
            column: right_column,
        } = &**right
        else {
            unreachable!();
        };

        let mut left_table = join_parameters.left;
        let mut right_table = join_parameters.right;

        let mut hash_map = HashMap::<Cow<DbVal>, (bool, Vec<Row>)>::new();
        let left_column = left_column.resolved().expect("must be resolved");
        let left_idx = left_table
            .schema()
            .column_index_by_source(left_column)
            .unwrap_or_else(|| {
                panic!(
                    "could not get index of column {left_column} for left side {:?}",
                    left_table.schema().columns()
                )
            });
        let right_column = right_column.resolved().expect("must be resolved");
        let right_idx = right_table
            .schema()
            .column_index_by_source(right_column)
            .unwrap_or_else(|| {
                panic!(
                    "could not get index of column {right_column} for right side {:?}",
                    right_table.schema().columns()
                )
            });

        let mut i = 0;

        while let Some(row) = left_table.next() {
            let db_val = row[left_idx].clone();
            hash_map
                .entry(db_val)
                .or_insert_with(|| (false, vec![]))
                .1
                .push(row);
            i += 1;
        }
        while let Some(right_row) = right_table.next() {
            let db_val = right_row
                .get(right_idx)
                .unwrap_or_else(|| panic!("failed to get index {right_idx} of row {right_row:?}"));
            if let Some((used, rows)) = hash_map.get_mut(db_val) {
                *used = true;
                for left_row in rows {
                    let joined = Row::from_iter(left_row.iter().chain(right_row.iter()).cloned());
                    *left_row = joined;
                    i += 1;
                }
            }
            i += 1;
        }
        debug!("join completed in {i} iterations");

        let rows = hash_map
            .into_values()
            .into_iter()
            .filter_map(|(used, rows)| if used { Some(rows) } else { None })
            .flatten();

        Ok(Box::new(RefRows::new(join_parameters.schema, rows)))
    }
}
