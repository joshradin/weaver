use std::fmt::{Debug, Formatter, Pointer};
use std::sync::Arc;

use weaver_ast::ast::{Expr, JoinConstraint, JoinOperator};

use crate::dynamic_table::HasSchema;
use crate::queries::execution::strategies::join::JoinStrategy;
use crate::queries::query_cost::Cost;
use crate::rows::KeyIndex;
use crate::storage::tables::table_schema::TableSchema;

#[derive(Debug)]
pub struct QueryPlan {
    root: QueryPlanNode,
}

impl QueryPlan {
    /// Create a new query plan with a given root
    pub fn new(root: QueryPlanNode) -> Self {
        Self { root }
    }
    /// Gets the root node
    pub fn root(&self) -> &QueryPlanNode {
        &self.root
    }
}


pub struct QueryPlanNode {
    pub cost: Cost,
    pub rows: u64,
    pub kind: QueryPlanKind,
    /// The table schema at this point
    pub schema: TableSchema,
    pub alias: Option<String>,
}

impl Debug for QueryPlanNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryPlanNode")
            .field("cost", &self.cost)
            .field("rows", &self.rows)
            .field("kind", &self.kind)
            .field("cols", &self.schema().columns())
            .finish()
    }
}

impl QueryPlanNode {
    /// Tries to find the plan node with a given alias. Aliases are shadowed.
    pub fn get_alias(&self, alias: impl AsRef<str>) -> Option<&QueryPlanNode> {
        let alias = alias.as_ref();
        if self
            .alias
            .as_ref()
            .map(|node_a| node_a == alias)
            .unwrap_or(false)
        {
            return Some(self);
        }
        match &self.kind {
            _ => None,
        }
    }

    /// Gets the actual cost of the query plan node
    pub fn cost(&self) -> f64 {
        self.cost.get_cost(self.rows as usize)
    }
}

impl HasSchema for QueryPlanNode {
    fn schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[derive(Debug)]
pub enum QueryPlanKind {
    /// Gets rows from a given table, this is usually used as a leaf node
    TableScan {
        schema: String,
        table: String,
        /// The keys that can be used
        keys: Option<Vec<KeyIndex>>,
    },
    Filter {
        filtered: Box<QueryPlanNode>,
        condition: Expr
    },
    Project {
        columns: Vec<usize>,
        node: Box<QueryPlanNode>,
    },
    Join {
        left: Box<QueryPlanNode>,
        right: Box<QueryPlanNode>,
        join_kind: JoinOperator,
        on: JoinConstraint,
        strategies: Vec<Arc<dyn JoinStrategy>>,
    },
}
