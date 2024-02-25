//! Join strategies

use crate::db::server::WeakWeaverDb;
use crate::dynamic_table::Table;
use crate::error::Error;
use crate::queries::execution::strategies::Strategy;
use crate::queries::query_cost::Cost;
use static_assertions::assert_obj_safe;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tracing::{instrument, trace, Level};
use weaver_ast::ast::{BinaryOp, Expr, JoinClause, JoinOperator};

/// A join strategy
pub trait JoinStrategy: Strategy {
    /// Sees if this join strategy can be run upon the given join clause.
    ///
    /// If it can, returns a [`Some(Cost)`](Cost) struct to determine how expensive the operation is, otherwise,
    /// [`None`](None) is returned.
    fn join_cost(&self, join_parameters: &JoinClause) -> Option<Cost>;

    /// Attempts to perform a join
    fn try_join(&self, join_parameters: JoinParameters) -> Result<Table, Error>;
}

impl Debug for dyn JoinStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "JoinStrategy[{}]", self.name())
    }
}

assert_obj_safe!(JoinStrategy);

#[derive(Debug)]
pub struct JoinParameters {
    op: JoinOperator,
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

    /// Tries to get a strategy for a given join clause
    #[instrument(level = Level::TRACE, skip(self), fields(join=%join), ret, err)]
    pub fn get_strategy_for_join(&self, join: &JoinClause) -> Result<Arc<dyn JoinStrategy>, Error> {
        self.strategies
            .iter()
            .inspect(|strat| trace!("checking if {strat:?} is applicable"))
            .filter_map(|strat| strat.join_cost(join).map(|cost| (cost, strat)))
            .inspect(|strat| {
                trace!(
                    "found applicable strategy {:?} with cost {:?}",
                    strat.1,
                    strat.0
                )
            })
            .reduce(|acc, next| match acc.0.cmp(&next.0) {
                Ordering::Less | Ordering::Equal => acc,
                Ordering::Greater => next,
            })
            .map(|(_, arc)| arc.clone())
            .ok_or_else(|| Error::NoStrategyForJoin(join.clone()))
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

        let Expr::Column { column } = &**left else {
            return None;
        };

        todo!()
    }

    fn try_join(&self, join_parameters: JoinParameters) -> Result<Table, Error> {
        todo!()
    }
}
