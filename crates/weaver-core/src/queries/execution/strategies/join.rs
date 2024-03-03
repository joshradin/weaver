//! Join strategies

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use static_assertions::assert_obj_safe;
use tracing::{debug, instrument, trace, Level};

use weaver_ast::ast::{BinaryOp, Expr, JoinClause, JoinConstraint, JoinOperator};

use crate::data::row::Row;
use crate::data::values::DbVal;
use crate::db::server::WeakWeaverDb;
use crate::dynamic_table::Table;
use crate::error::WeaverError;
use crate::key::KeyData;
use crate::queries::execution::strategies::Strategy;
use crate::queries::query_cost::Cost;
use crate::rows::Rows;
use crate::storage::tables::InMemoryTable;

/// A join strategy
pub trait JoinStrategy: Strategy {
    /// Sees if this join strategy can be run upon the given join clause.
    ///
    /// If it can, returns a [`Some(Cost)`](Cost) struct to determine how expensive the operation is, otherwise,
    /// [`None`](None) is returned.
    fn join_cost(&self, join_parameters: &JoinClause) -> Option<Cost>;

    /// Attempts to perform a join
    fn try_join<'r>(&self, join_parameters: JoinParameters<'r>) -> Result<Box<dyn Rows<'r>>, WeaverError>;
}

impl Debug for dyn JoinStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "JoinStrategy[{}]", self.name())
    }
}

assert_obj_safe!(JoinStrategy);

#[derive(Debug)]
pub struct JoinParameters<'a> {
    pub op: JoinOperator,
    pub left: Box<dyn Rows<'a>>,
    pub right: Box<dyn Rows<'a>>,
    pub constraint: JoinConstraint,
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
    ) -> Result<Vec<Arc<dyn JoinStrategy>>, WeaverError> {
        let mut vec = self
            .strategies
            .iter()
            .inspect(|strat| trace!("checking if {strat:?} is applicable"))
            .filter_map(|strat| strat.join_cost(join).map(|cost| (cost, strat)))
            .collect::<Vec<_>>();
        if vec.is_empty() {
            return Err(WeaverError::NoStrategyForJoin(join.clone()));
        }
        vec.sort_by_key(|c| c.0);
        Ok(vec.into_iter().map(|(_, strat)| strat.clone()).collect())
    }
}

#[derive(Debug)]
struct HashJoinTableStrategy;

impl Strategy for HashJoinTableStrategy {
    fn name(&self) -> &str {
        "HashJoinTable"
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
        Some(Cost::new(1.1, 1))
    }

    fn try_join<'r>(&self, join_parameters: JoinParameters<'r>) -> Result<Box<dyn Rows<'r>>, WeaverError> {
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

        let left_table = join_parameters.left;
        let right_table = join_parameters.right;

        let mut hash_map = HashMap::<DbVal, Row>::new();
        let left_idx = left_table
            .schema()
            .column_index(left_column.resolved().expect("must be resolved").column())
            .expect("could not get index of column for left side");
        let right_idx = right_table
            .schema()
            .column_index(right_column.resolved().expect("must be resolved").column())
            .expect("could not get index of column for right side");

        todo!()
    }
}
