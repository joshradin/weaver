use crate::rows::KeyIndex;
use crate::tables::TableRef;

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
    pub cost: f64,
    pub rows: u64,
    pub kind: QueryPlanKind
}

#[derive(Debug)]
pub enum QueryPlanKind {
    SelectByKey {
        table: TableRef,
        key_index: Vec<KeyIndex>,
    },
    Project {
        columns: Vec<usize>,
        node: Box<QueryPlanNode>
    }
}