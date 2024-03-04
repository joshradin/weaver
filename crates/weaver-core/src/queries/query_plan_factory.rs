//! Creates an unoptimized [query plan](QueryPlan) from a [query](Query)

use std::borrow::Cow;
use std::cell::{OnceCell, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::From;
use std::marker::PhantomData;

use parking_lot::RwLock;
use rand::Rng;
use tracing::{debug, debug_span, error_span, trace};

use weaver_ast::ast;
use weaver_ast::ast::visitor::{visit_table_or_sub_query_mut, VisitorMut};
use weaver_ast::ast::{
    BinaryOp, ColumnRef, Expr, FromClause, Identifier, JoinClause, JoinConstraint, JoinOperator,
    Query, ReferencesCols, ResolvedColumnRef, ResultColumn, Select, TableOrSubQuery,
    UnresolvedColumnRef,
};

use crate::data::types::Type;
use crate::db::server::processes::WeaverProcessInfo;
use crate::db::server::socket::DbSocket;
use crate::db::server::WeakWeaverDb;
use crate::dynamic_table::{DynamicTable, HasSchema, Table};
use crate::error::WeaverError;
use crate::key::KeyData;
use crate::queries::execution::strategies::join::JoinStrategySelector;
use crate::queries::query_cost::{Cost, CostTable};
use crate::queries::query_plan::{QueryPlan, QueryPlanKind, QueryPlanNode, QueryPlanNodeBuilder};
use crate::rows::{KeyIndex, KeyIndexKind};
use crate::storage::tables::table_schema::{
    ColumnDefinition, Key, TableSchema, TableSchemaBuilder,
};
use crate::storage::tables::{table_schema, TableRef};
use crate::tx::Tx;

#[derive(Debug)]
pub struct QueryPlanFactory {
    db: WeakWeaverDb,
    join_strategy_selector: JoinStrategySelector,
    cost_table: RefCell<CostTable>,
}

impl QueryPlanFactory {
    pub fn new(db: WeakWeaverDb) -> Self {
        let selector = JoinStrategySelector::new(db.clone());
        Self {
            db,
            join_strategy_selector: selector,
            cost_table: Default::default(),
        }
    }

    /// Converts a given query to a plan
    pub fn to_plan<'a>(
        &self,
        tx: &Tx,
        query: &Query,
        plan_context: impl Into<Option<&'a WeaverProcessInfo>>,
    ) -> Result<QueryPlan, WeaverError> {
        error_span!("to_plan").in_scope(|| {
            let db = self.db.upgrade().ok_or(WeaverError::NoCoreAvailable)?;
            debug!("upgraded weaver db from weak");
            let socket = db.connect();
            debug!("connected socket to weaver db");
            debug!("getting cost table...");
            let cost_table = socket
                .get_table(&("weaver".into(), "cost".into()))
                .map_err(|_| WeaverError::CostTableNotLoaded)?;

            let cost_table = CostTable::from_table(&cost_table, tx);
            if &cost_table != &*self.cost_table.borrow() {
                *self.cost_table.borrow_mut() = cost_table;
            }

            self.to_plan_node(query, &socket, plan_context.into())
                .map(QueryPlan::new)
        })
    }

    pub fn get_involved_tables(
        &self,
        query: &Query,
        plan_context: Option<&WeaverProcessInfo>,
    ) -> Result<HashMap<TableRef, TableSchema>, WeaverError> {
        let involved = self.get_involved_table_refs(query, plan_context)?;
        let Some(core) = self.db.upgrade() else {
            return Err(WeaverError::NoCoreAvailable);
        };
        let db_socket = core.connect();
        let mut table = HashMap::new();
        for table_ref in involved {
            let schema = db_socket.get_table(&table_ref)?.schema().clone();
            table.insert(table_ref, schema);
        }
        Ok(table)
    }

    pub fn get_involved_table_refs(
        &self,
        query: &Query,
        plan_context: Option<&WeaverProcessInfo>,
    ) -> Result<Vec<TableRef>, WeaverError> {
        let mut emit = vec![];

        let mut stack = vec![query.clone()];

        while let Some(query) = stack.pop() {
            match query {
                Query::Select(Select {
                    from: Some(ast::FromClause(table_ref)),
                    ..
                }) => self.get_involved_table_refs_helper(
                    table_ref,
                    &mut emit,
                    &mut stack,
                    plan_context,
                )?,
                _ => {}
            }
        }

        Ok(emit)
    }
    fn get_involved_table_refs_helper(
        &self,
        table_ref: TableOrSubQuery,
        emit: &mut Vec<TableRef>,
        stack: &mut Vec<Query>,
        plan_context: Option<&WeaverProcessInfo>,
    ) -> Result<(), WeaverError> {
        match table_ref {
            TableOrSubQuery::Table {
                schema, table_name, ..
            } => match schema {
                None => emit.push(self.table_ref((None, table_name.as_ref()), plan_context)?),
                Some(schema) => emit.push((schema.to_string(), table_name.to_string())),
            },
            TableOrSubQuery::Select { select, .. } => stack.push(Query::Select(*select)),
            TableOrSubQuery::Multiple(many) => {
                for tsq in many {
                    self.get_involved_table_refs_helper(tsq, emit, stack, plan_context)?;
                }
            }
            TableOrSubQuery::JoinClause(JoinClause { left, right, .. }) => {
                self.get_involved_table_refs_helper(*left, emit, stack, plan_context)?;
                self.get_involved_table_refs_helper(*right, emit, stack, plan_context)?;
            }
        }
        Ok(())
    }

    fn get_cost(&self, key: impl AsRef<str>) -> Result<Cost, WeaverError> {
        let key = key.as_ref().to_string();
        self.cost_table
            .borrow()
            .get(&key)
            .ok_or_else(|| WeaverError::UnknownCostKey(key))
            .copied()
    }

    /// Converts a given query to a plan
    fn to_plan_node(
        &self,
        query: &Query,
        db: &DbSocket,
        plan_context: Option<&WeaverProcessInfo>,
    ) -> Result<QueryPlanNode, WeaverError> {
        debug!("creating query plan from {}", query);
        let tables = debug_span!("finding involved tables")
            .in_scope(|| self.get_involved_tables(query, plan_context))?;
        debug!("collected tables: {:?}", tables.keys());
        debug!("resolving all identifiers");
        let ref query = {
            let mut query = query.clone();

            let in_use = plan_context
                .and_then(|info| info.using.as_ref())
                .map(|s| Identifier::new(s));

            let mut resolved = IdentifierResolver::new(in_use, |column_ref| {
                self.resolve_column_ref(column_ref, &tables, plan_context)
            });
            debug_span!("column references resolver")
                .in_scope(|| resolved.visit_query_mut(&mut query))?;
            query
        };

        let node = match query {
            Query::Select(select) => self.select_to_plan_node(db, plan_context, &tables, select),
            Query::Explain(explained) => {
                let query = self.to_plan_node(explained, db, plan_context)?;
                QueryPlanNode::builder()
                    .rows(0)
                    .cost(Cost::new(1.0, 0))
                    .kind(QueryPlanKind::Explain { explained: Box::new(query) })
                    .schema(QueryPlan::explain_schema())
                    .build()
            }
            Query::QueryList(_) => {
                todo!("query list")
            }
        };
        node
    }

    fn from_to_plan_node(
        &self,
        db: &DbSocket,
        plan_context: Option<&WeaverProcessInfo>,
        real_tables: &HashMap<TableRef, TableSchema>,
        from: &FromClause,
    ) -> Result<QueryPlanNode, WeaverError> {
        let from = &from.0;
        self.table_or_sub_query_to_plan_node(db, plan_context, real_tables, from)
    }

    fn table_or_sub_query_to_plan_node(
        &self,
        db: &DbSocket,
        plan_context: Option<&WeaverProcessInfo>,
        real_tables: &HashMap<TableRef, TableSchema>,
        from: &TableOrSubQuery,
    ) -> Result<QueryPlanNode, WeaverError> {
        match from {
            TableOrSubQuery::Table {
                schema,
                table_name,
                alias,
            } => {
                let table_ref = self.table_ref(
                    (schema.as_ref().map(|s| s.as_ref()), table_name.as_ref()),
                    plan_context,
                )?;
                let mut table_schema = real_tables
                    .get(&table_ref)
                    .expect("could not get schema")
                    .clone();

                let rows = db
                    .get_table(&table_ref)?
                    .size_estimate(&table_schema.primary_key()?.all())?;

                let (schema, table) = table_ref;

                Ok(QueryPlanNode::builder()
                    .cost(self.get_cost("LOAD_TABLE")?)
                    .rows(rows)
                    .kind(QueryPlanKind::TableScan {
                        schema,
                        table,
                        keys: None,
                    })
                    .schema(table_schema)
                    .alias(alias.as_ref().map(|i| i.to_string()))
                    .build()?)
            }
            TableOrSubQuery::Select { select, alias } => {
                let node = self.select_to_plan_node(db, plan_context, real_tables, select)?;
                match alias {
                    None => Ok(node),
                    Some(alias) => {
                        unimplemented!("select query aliasing");
                    }
                }
            }
            TableOrSubQuery::Multiple(_) => {
                unimplemented!("FULL OUTER JOIN");
            }
            TableOrSubQuery::JoinClause(join_clause) => {
                self.join_to_plan_node(db, plan_context, real_tables, join_clause)
            }
        }
    }

    fn select_to_plan_node(
        &self,
        db: &DbSocket,
        plan_context: Option<&WeaverProcessInfo>,
        real_tables: &HashMap<TableRef, TableSchema>,
        select: &Select,
    ) -> Result<QueryPlanNode, WeaverError> {
        error_span!("SELECT").in_scope(|| -> Result<QueryPlanNode, WeaverError> {
            let Select {
                columns,
                from,
                condition,
                limit,
                offset,
            } = select;

            match from {
                None => {
                    todo!("no from")
                }
                Some(from) => {
                    let from_node = self.from_to_plan_node(db, plan_context, real_tables, from)?;
                    // let keys = self.get_keys_from_condition(plan_context, &real_tables, condition, &from_node)?;

                    let from_node_rows = from_node.rows;

                    let filtered = match condition {
                        None => from_node,
                        Some(condition) => {
                            let schema = from_node.schema().clone();
                            QueryPlanNode::builder()
                                .cost(self.get_cost("FILTER")?)
                                .rows(
                                    limit
                                        .map(|i| i.min(from_node_rows))
                                        .unwrap_or(from_node_rows),
                                )
                                .kind(QueryPlanKind::Filter {
                                    filtered: Box::new(from_node),
                                    condition: condition.clone(),
                                })
                                .schema(schema)
                                .build()?
                        }
                    };

                    // if columns.len() == 1 && matches!(columns[0], ResultColumn::Wildcard) {
                    //     // if just * wildcard then no projection is needed
                    //     return Ok(filtered);
                    // }

                    let (projected_schema, columns) =
                        self.table_schema_for_projection(columns, &filtered)?;

                    Ok(QueryPlanNode::builder()
                        .cost(self.get_cost("SELECT")?)
                        .rows(filtered.rows)
                        .kind(QueryPlanKind::Project {
                            columns: columns,
                            node: Box::new(filtered),
                        })
                        .schema(projected_schema)
                        .build()?)
                }
            }
        })
    }

    fn get_keys_from_condition(
        &self,
        plan_context: Option<&WeaverProcessInfo>,
        real_tables: &&HashMap<TableRef, TableSchema>,
        condition: &Option<Expr>,
        from_node: &QueryPlanNode,
    ) -> Result<Vec<KeyIndex>, WeaverError> {
        let mut applicable_keys =
            match condition
                .as_ref()
                .map(|condition| -> Result<VecDeque<&Key>, WeaverError> {
                    let condition: HashSet<ResolvedColumnRef> = condition
                        .columns()
                        .iter()
                        .map(|c| {
                            c.resolved()
                                .expect("all columns should be resolved at this point")
                                .clone()
                        })
                        .collect();
                    debug!("columns used in condition: {condition:?}");
                    Ok(from_node
                        .schema()
                        .keys()
                        .iter()
                        .filter_map(|key| {
                            debug!(
                                "checking if all of {:?} in condition columns {:?}",
                                key.columns(),
                                condition
                            );
                            if key.columns().iter().all(|column| {
                                let column_ref = self.parse_column_ref(column);

                                let resolved = match column_ref {
                                    ColumnRef::Unresolved(column_ref) => self
                                        .resolve_column_ref(&column_ref, real_tables, plan_context)
                                        .expect("could not resolve"),
                                    ColumnRef::Resolved(resolved) => resolved,
                                };

                                debug!("resolved key column: {resolved:?}");
                                debug!("does {condition:?} contain {resolved:?}?");
                                condition.contains(&resolved)
                            }) {
                                Some(key)
                            } else {
                                None
                            }
                        })
                        .collect::<VecDeque<_>>())
                }) {
                None => VecDeque::default(),
                Some(res) => res?,
            };

        if applicable_keys.is_empty() {
            applicable_keys.push_front(from_node.schema().primary_key()?);
        }
        let mut applicable_keys = Vec::from(applicable_keys);
        debug!("applicable keys: {applicable_keys:?}");
        applicable_keys.sort_by_cached_key(|key| {
            if key.primary() {
                -1_isize
            } else {
                key.columns().len() as isize
            }
        });
        Ok(applicable_keys
            .into_iter()
            .map(|key| self.to_key_index(key, condition.as_ref(), &real_tables, plan_context))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>())
    }

    fn table_schema_for_projection(
        &self,
        columns: &Vec<ResultColumn>,
        from_node: &QueryPlanNode,
    ) -> Result<(TableSchema, Vec<Expr>), WeaverError> {
        let source_schema = &from_node.schema;
        let mut schema_builder =
            TableSchemaBuilder::new("*", format!("table_{}", rand::random::<u64>()));
        let mut cols = vec![];

        for result_column in columns {
            match result_column {
                ResultColumn::Wildcard => {
                    // all columns from previous node
                    for col in from_node.schema.columns() {
                        let resolved = if let Some(source_column) = col.source_column() {
                            source_column.clone()
                        } else {
                            ResolvedColumnRef::new(
                                Identifier::new(source_schema.schema()),
                                Identifier::new(source_schema.name()),
                                Identifier::new(col.name()),
                            )
                        };

                        let mut col_def = col.clone();
                        col_def.set_source_column(resolved.clone());

                        schema_builder = schema_builder.column_definition(col_def);
                        let expr = Expr::Column {
                            column: resolved.into(),
                        };
                        cols.push(expr);
                    }
                }
                ResultColumn::TableWildcard(table) => {}
                ResultColumn::Expr { expr, alias } => {
                    let name = match alias {
                        None => expr.to_string(),
                        Some(alias) => alias.to_string(),
                    };
                    let non_null = match expr {
                        Expr::Column { column } => true,
                        _ => false,
                    };

                    cols.push(expr.clone());

                    let mut cd = ColumnDefinition::new(name, Type::Boolean, non_null, None, None)?;
                    if let Expr::Column { column } = expr {
                        if let Some(resolved) = column.resolved() {
                            cd.set_source_column(resolved.clone());
                        }
                    }

                    schema_builder = schema_builder.column_definition(cd);
                }
            }
        }
        Ok((schema_builder.build()?, cols))
    }

    fn join_to_plan_node(
        &self,
        db: &DbSocket,
        plan_context: Option<&WeaverProcessInfo>,
        real_tables: &HashMap<TableRef, TableSchema>,
        join_clause: &JoinClause,
    ) -> Result<QueryPlanNode, WeaverError> {
        error_span!("JOIN").in_scope(|| -> Result<QueryPlanNode, WeaverError> {
            let JoinClause {
                left,
                op,
                right,
                constraint,
            } = join_clause;

            let left = self.table_or_sub_query_to_plan_node(db, plan_context, real_tables, left)?;
            let right =
                self.table_or_sub_query_to_plan_node(db, plan_context, real_tables, right)?;

            let strategies = self
                .join_strategy_selector
                .get_strategies_for_join(join_clause)?;
            debug!("join strategies for {join_clause}: {strategies:#?}");

            let built_schema = left.schema().join(right.schema());

            let rows = match join_clause.op {
                JoinOperator::Left => left.rows,
                JoinOperator::Right => right.rows,
                JoinOperator::Full => left.rows.max(right.rows),
                JoinOperator::Inner => left.rows.max(right.rows),
                JoinOperator::Cross => left.rows * right.rows,
                JoinOperator::Outer => left.rows + right.rows,
            };

            Ok(QueryPlanNode::builder()
                .cost(self.get_cost("JOIN")?)
                .rows(rows)
                .kind(QueryPlanKind::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_kind: op.clone(),
                    on: constraint.clone(),
                    strategies,
                })
                .schema(built_schema)
                .build()?)
        })
    }

    /// When given some conditional expression `cond` and a known `key`, we can get key indices to query against the table
    /// in a more efficient manner than just doing an `all` search.
    ///
    /// This requires the `cond` condition follows certain patterns:
    /// - `{column} = literal` (and reverse)
    /// - `{column} < literal`
    pub fn to_key_index(
        &self,
        key: &Key,
        cond: Option<&Expr>,
        involved_tables: &HashMap<TableRef, TableSchema>,
        ctx: Option<&WeaverProcessInfo>,
    ) -> Result<Vec<KeyIndex>, WeaverError> {
        debug!("attempting to get key indices from key: {key:?} and cond: {cond:?}");
        match cond {
            None => Ok(vec![key.all()]),
            Some(cond) => match cond {
                Expr::Binary { left, op, right } => match op {
                    BinaryOp::Eq
                    | BinaryOp::Neq
                    | BinaryOp::Greater
                    | BinaryOp::Less
                    | BinaryOp::GreaterEq
                    | BinaryOp::LessEq => {
                        let (col, const_v) = if left.is_const() && !right.is_const() {
                            if let Expr::Column { column } = &**right {
                                (column, left)
                            } else {
                                return Ok(vec![]);
                            }
                        } else if right.is_const() && !left.is_const() {
                            if let Expr::Column { column } = &**left {
                                (column, right)
                            } else {
                                return Ok(vec![]);
                            }
                        } else {
                            return Ok(vec![]);
                        };

                        let col = match col {
                            ColumnRef::Unresolved(col) => {
                                self.resolve_column_ref(col, involved_tables, ctx)?
                            }
                            ColumnRef::Resolved(resolved) => resolved.clone(),
                        };

                        let Expr::Literal { literal: value } = &**const_v else {
                            panic!("expr is constant but not a literal: {left:?}")
                        };

                        if key.columns().len() == 1
                            && key.columns().contains(&col.column().to_string())
                        {
                            return Ok(vec![KeyIndex::new(
                                key.name(),
                                KeyIndexKind::One(KeyData::from([value.clone()])),
                                None,
                                None,
                            )]);
                        }
                        Ok(vec![])
                    }
                    BinaryOp::And | BinaryOp::Or => self
                        .to_key_index(key, Some(&*left), involved_tables, ctx)
                        .and_then(|mut left| {
                            self.to_key_index(key, Some(&*right), involved_tables, ctx)
                                .map(|right| {
                                    left.extend(right);
                                    left
                                })
                        }),
                    _ => Ok(vec![]),
                },
                _ => Ok(vec![]),
            },
        }
    }

    /// Convert to a table reference
    pub fn table_ref(
        &self,
        tb_ref: (Option<&str>, &str),
        ctx: Option<&WeaverProcessInfo>,
    ) -> Result<TableRef, WeaverError> {
        let in_use = ctx.and_then(|info| info.using.as_ref());
        trace!(
            "creating table ref from tb_ref {:?} and in_use {:?}",
            tb_ref,
            in_use
        );
        match tb_ref.0 {
            None => in_use
                .ok_or(WeaverError::UnQualifedTableWithoutInUseSchema)
                .map(|schema| (schema.to_string(), tb_ref.1.to_string())),
            Some(schema) => Ok((schema.to_string(), tb_ref.1.to_string())),
        }
    }

    pub fn parse_column_ref(&self, col: &str) -> ColumnRef {
        let split = col.split(".").collect::<Vec<_>>();
        match &split[..] {
            &[col] => UnresolvedColumnRef::with_column(Identifier::new(col)).into(),
            &[table, col] => {
                UnresolvedColumnRef::with_table(Identifier::new(table), Identifier::new(col)).into()
            }
            &[schema, table, col] => ResolvedColumnRef::new(
                Identifier::new(schema),
                Identifier::new(table),
                Identifier::new(col),
            )
            .into(),
            _ => panic!("column ref can not have more than three fields"),
        }
    }

    pub fn resolve_column_ref(
        &self,
        column_ref: &UnresolvedColumnRef,
        involved_tables: &HashMap<TableRef, TableSchema>,
        ctx: Option<&WeaverProcessInfo>,
    ) -> Result<ResolvedColumnRef, WeaverError> {
        match column_ref.as_tuple() {
            (None, None, col) => {
                debug!("finding column ?.?.{col}");
                let mut positives = vec![];
                for (table, schema) in involved_tables {
                    if schema.contains_column(col) {
                        positives.push(table);
                    }
                }
                match positives.as_slice() {
                    &[] => Err(WeaverError::ColumnNotFound(col.to_string())),
                    &[(schema, table)] => Ok(ResolvedColumnRef::new(
                        Identifier::new(schema),
                        Identifier::new(table),
                        Identifier::new(col),
                    )),
                    slice => Err(WeaverError::AmbiguousColumn {
                        col: col.to_string(),
                        positives: slice
                            .into_iter()
                            .map(|&(schema, table)| {
                                ResolvedColumnRef::new(
                                    Identifier::new(schema),
                                    Identifier::new(table),
                                    Identifier::new(col),
                                )
                            })
                            .collect(),
                    }),
                }
            }
            (None, Some(table), col) => {
                debug!("finding column ?.{table}.{col}");
                let mut positives = vec![];
                for (table_ref, schema) in involved_tables {
                    if table_ref.1 == table.as_ref() && schema.contains_column(col) {
                        positives.push(table_ref);
                    }
                }

                match positives.as_slice() {
                    &[] => Err(WeaverError::ColumnNotFound(col.to_string())),
                    &[(schema, table)] => Ok(ResolvedColumnRef::new(
                        Identifier::new(schema),
                        Identifier::new(table),
                        Identifier::new(col),
                    )),
                    slice => Err(WeaverError::AmbiguousColumn {
                        col: col.to_string(),
                        positives: slice
                            .into_iter()
                            .map(|&(schema, table)| {
                                ResolvedColumnRef::new(
                                    Identifier::new(schema),
                                    Identifier::new(table),
                                    Identifier::new(col),
                                )
                            })
                            .collect(),
                    }),
                }
            }
            (Some(schema), Some(table), col) => Ok(ResolvedColumnRef::new(
                schema.clone(),
                table.clone(),
                col.clone(),
            )),
            _ => unreachable!("<name>.?.<name> should not be possible"),
        }
    }
}

pub fn column_reference(table: &Table, column: &String) -> (String, String, String) {
    (
        table.schema().schema().to_string(),
        table.schema().name().to_string(),
        column.to_string(),
    )
}

pub fn to_col_ref(
    input: &(Option<String>, Option<String>, String),
) -> (Option<&str>, Option<&str>, &str) {
    let (schema, table, column) = input;
    (schema.as_deref(), table.as_deref(), column.as_str())
}

struct IdentifierResolver<'a, F>
where
    F: Fn(&UnresolvedColumnRef) -> Result<ResolvedColumnRef, WeaverError> + 'a,
{
    in_use_schema: Option<Identifier>,
    aliases: HashMap<Identifier, (Identifier, Identifier)>,
    resolver: F,
    _lf: PhantomData<&'a ()>,
}

impl<'a, F> IdentifierResolver<'a, F>
where
    F: Fn(&UnresolvedColumnRef) -> Result<ResolvedColumnRef, WeaverError> + 'a,
{
    fn new(in_use_schema: Option<Identifier>, resolver: F) -> Self {
        Self {
            in_use_schema,
            aliases: Default::default(),
            resolver,
            _lf: PhantomData,
        }
    }
}

impl<'a, F> VisitorMut for IdentifierResolver<'a, F>
where
    F: Fn(&UnresolvedColumnRef) -> Result<ResolvedColumnRef, WeaverError> + 'a,
{
    type Err = WeaverError;

    fn visit_column_ref_mut(&mut self, column: &mut ColumnRef) -> Result<(), Self::Err> {
        if let ColumnRef::Unresolved(ref unresolved) = column {
            if let Some((schema, table)) = unresolved.table().and_then(|i| self.aliases.get(i)) {
                debug!(
                    "found alias {}.{} for {}",
                    schema,
                    table,
                    unresolved.table().unwrap()
                );
                let resolved = ResolvedColumnRef::new(
                    schema.clone(),
                    table.clone(),
                    unresolved.column().clone(),
                );
                *column = ColumnRef::Resolved(resolved);
            } else {
                debug!("resolving column {unresolved:?}...");
                let resolved = (self.resolver)(unresolved)?;
                debug!("resolved = {resolved}");
                *column = ColumnRef::Resolved(resolved);
            }
        }

        Ok(())
    }

    fn visit_table_or_sub_query_mut(
        &mut self,
        table_or_sub_query: &mut TableOrSubQuery,
    ) -> Result<(), Self::Err> {
        match table_or_sub_query {
            TableOrSubQuery::Table {
                schema,
                table_name,
                alias: Some(alias),
            } => {
                let schema = schema
                    .as_ref()
                    .ok_or_else(|| WeaverError::UnQualifedTableWithoutInUseSchema)?;
                debug!("adding alias for {} -> {}.{}", alias, schema, table_name);
                self.aliases
                    .insert(alias.clone(), (schema.clone(), table_name.clone()));
            }
            TableOrSubQuery::Select { select, alias } => {
                panic!("don't know how to handle subquery and aliases");
            }
            _ => {}
        }

        visit_table_or_sub_query_mut(self, table_or_sub_query)
    }
}
