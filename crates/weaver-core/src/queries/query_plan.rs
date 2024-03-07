use std::fmt::{Debug, Formatter, Pointer};
use std::sync::Arc;

use uuid::Uuid;

use weaver_ast::ast::{CreateTable, Expr, JoinConstraint, JoinOperator, ReferencesCols};

use crate::data::row::Row;
use crate::data::types::Type;
use crate::data::values::DbVal;
use crate::dynamic_table::HasSchema;
use crate::error::WeaverError;
use crate::queries::execution::strategies::join::JoinStrategy;
use crate::queries::query_cost::Cost;
use crate::rows::{KeyIndex, KeyIndexKind, OwnedRows, RefRows, Rows};
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
    pub fn root_mut(&mut self) -> &mut QueryPlanNode {
        &mut self.root
    }

    pub fn explain_schema() -> TableSchema {
        (|| -> Result<TableSchema, WeaverError> {
            TableSchema::builder("<explain>", "<explain>")
                .column("id", Type::Binary(16), true, None, None)?
                .column("select_type", Type::String(22), true, None, None)?
                .column("table", Type::String(255), true, None, None)?
                .column("type", Type::String(255), true, None, None)?
                .column("possible_keys", Type::String(255), true, None, None)?
                .column("columns", Type::String(255), true, None, None)?
                .column("rows", Type::Integer, true, None, None)?
                .column("cost", Type::Float, true, None, None)?
                // .column("id", Type::Binary(16), true, None, None)?
                .build()
        })()
        .expect("infallible")
    }

    pub fn ddl_result_schema() -> TableSchema {
        (|| -> Result<TableSchema, WeaverError> {
            TableSchema::builder("<query>", "<result>")
                .column("ok", Type::String(255), false, None, None)?
                .column("err", Type::String(255), false, None, None)?
                .build()
        })()
        .expect("infallible")
    }

    pub fn ddl_result<T, E>(result: Result<T, E>) -> impl Rows<'static>
    where
        T: ToString,
        E: ToString,
    {
        let row_values: [DbVal; 2] = match result {
            Ok(ok) => {
                [DbVal::string(ok.to_string(), None), DbVal::Null]
            }
            Err(err) => {
                [DbVal::Null, DbVal::string(err.to_string(), None)]
            }
        };
        let row = Row::from(row_values);
        RefRows::new(
            Self::ddl_result_schema(), [row]
        )
    }

    /// Converts this query plan into rows in postfix order
    pub fn as_rows(&self) -> OwnedRows {
        let mut rows = vec![];
        let mut in_order = self.root.postfix_order();
        in_order.pop();
        for node in in_order {
            let row = node.as_row().to_owned();
            rows.push(row);
        }

        OwnedRows::new(Self::explain_schema(), rows)
    }

    pub fn get(&self, id: &Uuid) -> Option<&QueryPlanNode> {
        let mut stack = vec![self.root()];
        while let Some(ptr) = stack.pop() {
            if &ptr.id == id {
                return Some(ptr);
            }
            stack.extend(ptr.children());
        }
        None
    }
    pub fn get_mut(&mut self, id: &Uuid) -> Option<&mut QueryPlanNode> {
        let mut stack = vec![self.root_mut()];
        while let Some(ptr) = stack.pop() {
            if &ptr.id == id {
                return Some(ptr);
            }
            stack.extend(ptr.children_mut());
        }
        None
    }
}

#[derive(Clone)]
pub struct QueryPlanNode {
    id: Uuid,
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
            .field("id", &self.id)
            .field("cost", &self.cost())
            .field("rows", &self.rows)
            .field("kind", &self.kind)
            .field("schema", &self.schema())
            .finish()
    }
}

impl QueryPlanNode {
    /// Gets a query plan node builder
    pub fn builder() -> QueryPlanNodeBuilder {
        QueryPlanNodeBuilder::default()
    }
    pub fn new(
        cost: Cost,
        rows: u64,
        kind: QueryPlanKind,
        schema: TableSchema,
        alias: Option<String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            cost,
            rows,
            kind,
            schema,
            alias,
        }
    }
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
        match &self.kind {
            QueryPlanKind::Join {
                strategies,
                left,
                right,
                ..
            } => {
                strategies.first().unwrap().1.get_cost(self.rows as usize)
                    + left.cost()
                    + right.cost()
            }
            QueryPlanKind::Filter { filtered, .. } => {
                self.cost.get_cost(self.rows as usize) + filtered.cost()
            }
            QueryPlanKind::Project { node, .. } => {
                self.cost.get_cost(self.rows as usize) + node.cost()
            }
            _ => self.cost.get_cost(self.rows as usize),
        }
    }

    /// Gets the uuid for this query plan
    pub fn id(&self) -> Uuid {
        self.id
    }

    fn as_row<'a>(&self) -> Row {
        let mut values: Vec<DbVal> = vec![];
        values.push(self.id.into());
        values.push("simple".into());

        match &self.kind {
            QueryPlanKind::TableScan {
                schema,
                table,
                keys,
            } => {
                values.push(table.into()); // table
                values.push(
                    keys.as_ref()
                        .and_then(|k| k.first())
                        .map(|k| match k.kind() {
                            KeyIndexKind::All => "ALL",
                            KeyIndexKind::Range { .. } => "range",
                            KeyIndexKind::One(_) => "const",
                        })
                        .unwrap_or("ALL")
                        .into(),
                ); // join kind
                values.push(
                    keys.as_ref()
                        .map(|keys| {
                            keys.iter()
                                .map(|k| k.key_name())
                                .collect::<Vec<_>>()
                                .join(",")
                        })
                        .unwrap_or_else(|| "PRIMARY".to_string())
                        .into(),
                ); // join kind
                values.push(
                    keys.as_ref()
                        .and_then(|k| k.first())
                        .and_then(|key| self.schema.get_key(key.key_name()).ok())
                        .map(|key| key.columns().join(","))
                        .unwrap_or_default()
                        .into(),
                )
            }
            QueryPlanKind::Filter {
                filtered,
                condition,
            } => {
                values.push("".into()); // table
                values.push("filter".into()); // join kind
                values.push("".into()); // possible keys
                values.push(
                    condition
                        .columns()
                        .into_iter()
                        .map(|i| i.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                        .into(),
                ); // columns
            }
            QueryPlanKind::Project { columns, .. } => {
                values.push("".into()); // table
                values.push("project".into()); // join kind
                values.push("".into()); // possible keys
                values.push(
                    columns
                        .into_iter()
                        .flat_map(|i| i.columns())
                        .map(|i| i.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                        .into(),
                ); // columns
            }
            QueryPlanKind::Join {
                strategies,
                on: JoinConstraint { on },
                ..
            } => {
                values.push("".into()); // table
                values.push(strategies.first().unwrap().0.to_string().into());
                values.push("".into()); // possible keys
                values.push(
                    on.columns()
                        .into_iter()
                        .map(|i| i.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                        .into(),
                ); // columns
            }
            QueryPlanKind::CreateTable { table_def } => {
                values.push(
                    format!("{}.{}", table_def.schema.as_ref().unwrap(), table_def.name).into(),
                ); // table
                values.push("create".into());
                values.push("".into()); // possible keys
                values.push("".into()); // columns
            }
            QueryPlanKind::Explain { .. } => {}
        }

        values.push((self.rows as i64).into());
        values.push(self.cost().into());

        Row::from(values)
    }

    /// Converts the query plan node tree into a pre order list. This is done
    /// recursively.
    pub fn prefix_order(&self) -> Vec<&QueryPlanNode> {
        let mut output = vec![];
        output.push(self);
        match &self.kind {
            QueryPlanKind::Filter { filtered, .. } => output.extend(filtered.prefix_order()),
            QueryPlanKind::Project { node, .. } => output.extend(node.prefix_order()),
            QueryPlanKind::Join { left, right, .. } => {
                output.extend(left.prefix_order());
                output.extend(right.prefix_order())
            }
            QueryPlanKind::Explain { explained } => output.extend(explained.prefix_order()),
            _ => {}
        }

        output
    }

    /// Converts the query plan node tree into a pre order list. This is done
    /// recursively.
    pub fn postfix_order(&self) -> Vec<&QueryPlanNode> {
        let mut output = vec![];
        match &self.kind {
            QueryPlanKind::Filter { filtered, .. } => output.extend(filtered.postfix_order()),
            QueryPlanKind::Project { node, .. } => output.extend(node.postfix_order()),
            QueryPlanKind::Join { left, right, .. } => {
                output.extend(left.postfix_order());
                output.extend(right.postfix_order())
            }
            QueryPlanKind::Explain { explained } => output.extend(explained.postfix_order()),
            _ => {}
        }
        output.push(self);

        output
    }
    /// Gets references to the children of this query plan node
    pub fn children(&self) -> Vec<&QueryPlanNode> {
        match &self.kind {
            QueryPlanKind::Filter { filtered, .. } => vec![&*filtered],
            QueryPlanKind::Project { node, .. } => vec![&*node],
            QueryPlanKind::Join { left, right, .. } => {
                vec![&*left, &*right]
            }
            QueryPlanKind::Explain { explained } => vec![&*explained],
            _ => {
                vec![]
            }
        }
    }
    /// Gets mutable reference to the children of this query plan node
    pub fn children_mut(&mut self) -> Vec<&mut QueryPlanNode> {
        match &mut self.kind {
            QueryPlanKind::Filter { filtered, .. } => vec![&mut *filtered],
            QueryPlanKind::Project { node, .. } => vec![&mut *node],
            QueryPlanKind::Join { left, right, .. } => {
                vec![&mut *left, &mut *right]
            }
            QueryPlanKind::Explain { explained } => vec![&mut *explained],
            _ => {
                vec![]
            }
        }
    }
}

impl HasSchema for QueryPlanNode {
    fn schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[derive(Debug, Default)]
pub struct QueryPlanNodeBuilder {
    cost: Option<Cost>,
    rows: Option<u64>,
    kind: Option<QueryPlanKind>,
    /// The table schema at this point
    schema: Option<TableSchema>,
    alias: Option<String>,
}

impl QueryPlanNodeBuilder {
    /// Creates a new query plan node builder
    pub fn new() -> Self {
        Self::default()
    }
    pub fn cost(&mut self, cost: Cost) -> &mut Self {
        let _ = self.cost.insert(cost);
        self
    }
    pub fn rows(&mut self, rows: u64) -> &mut Self {
        let _ = self.rows.insert(rows);
        self
    }
    pub fn kind(&mut self, kind: QueryPlanKind) -> &mut Self {
        let _ = self.kind.insert(kind);
        self
    }
    pub fn schema(&mut self, schema: TableSchema) -> &mut Self {
        let _ = self.schema.insert(schema);
        self
    }
    pub fn alias(&mut self, alias: impl Into<Option<String>>) -> &mut Self {
        self.alias = alias.into();
        self
    }
    pub fn build(&mut self) -> Result<QueryPlanNode, WeaverError> {
        let Self {
            cost: Some(cost),
            rows: Some(rest),
            kind: Some(kind),
            schema: Some(schema),
            alias,
        } = self
        else {
            let mut missing = vec![];

            missing.extend(self.cost.is_none().then_some("cost".to_string()));
            missing.extend(self.rows.is_none().then_some("rows".to_string()));
            missing.extend(self.kind.is_none().then_some("kind".to_string()));
            missing.extend(self.schema.is_none().then_some("schema".to_string()));

            return Err(WeaverError::BuilderIncomplete(
                "QueryPlanNodeBuilder".to_string(),
                missing,
            ));
        };

        Ok(QueryPlanNode::new(
            *cost,
            *rest,
            kind.clone(),
            schema.clone(),
            alias.clone(),
        ))
    }
}

#[derive(Debug, Clone)]
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
        condition: Expr,
    },
    Project {
        columns: Vec<Expr>,
        node: Box<QueryPlanNode>,
    },
    Join {
        left: Box<QueryPlanNode>,
        right: Box<QueryPlanNode>,
        join_kind: JoinOperator,
        on: JoinConstraint,
        strategies: Vec<(Arc<dyn JoinStrategy>, Cost)>,
    },
    Explain {
        explained: Box<QueryPlanNode>,
    },
    /// Creates a table
    CreateTable {
        table_def: CreateTable,
    },
}
