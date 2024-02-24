use crate::dynamic_table::HasSchema;
use crate::queries::query_cost::Cost;
use crate::rows::KeyIndex;
use crate::storage::tables::table_schema::{ColumnDefinition, TableSchema};
use crate::storage::tables::TableRef;
use std::collections::HashMap;

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

#[derive(Debug)]
pub struct QueryPlanNode {
    pub cost: Cost,
    pub rows: u64,
    pub kind: QueryPlanKind,
    /// The table schema at this point
    pub schema: TableSchema,
    pub alias: Option<String>,
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
            QueryPlanKind::SelectByKey { to_select, .. } => to_select.get_alias(alias),
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
    LoadTable {
        schema: String,
        table: String,
    },
    SelectByKey {
        to_select: Box<QueryPlanNode>,
        key_index: Vec<KeyIndex>,
    },
    Project {
        columns: Vec<usize>,
        node: Box<QueryPlanNode>,
    },
}
