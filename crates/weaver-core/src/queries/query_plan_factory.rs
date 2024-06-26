//! Creates an unoptimized [query plan](QueryPlan) from a [query](Query)

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::convert::From;
use std::marker::PhantomData;

use tracing::{debug, debug_span, error_span, trace};

use weaver_ast::ast;
use weaver_ast::ast::visitor::{
    visit_result_column_mut, visit_select_mut, visit_table_or_sub_query_mut, VisitorMut,
};
use weaver_ast::ast::Select;
use weaver_ast::ast::{
    BinaryOp, ColumnRef, Create, Expr, FromClause, FunctionArgs, Identifier, JoinClause,
    JoinOperator, OrderBy, Query, ReferencesCols, ResolvedColumnRef, ResultColumn, TableOrSubQuery,
    UnresolvedColumnRef,
};

use crate::data::types::DbTypeOf;
use crate::db::server::processes::WeaverProcessInfo;
use crate::db::server::socket::DbSocket;
use crate::db::server::WeakWeaverDb;
use crate::dynamic_table::{DynamicTable, HasSchema, Table};
use crate::error::WeaverError;
use crate::key::KeyData;
use crate::queries::execution::evaluation::functions::FunctionRegistry;
use crate::queries::execution::evaluation::{find_function, FunctionKind};
use crate::queries::execution::strategies::join::JoinStrategySelector;
use crate::queries::query_cost::{Cost, CostTable};
use crate::queries::query_plan::{QueryPlan, QueryPlanKind, QueryPlanNode};
use crate::rows::{KeyIndex, KeyIndexKind};
use crate::storage::tables::table_schema::{
    ColumnDefinition, Key, TableSchema, TableSchemaBuilder,
};
use crate::storage::tables::TableRef;
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
        function_registry: &FunctionRegistry,
        plan_context: impl Into<Option<&'a WeaverProcessInfo>>,
    ) -> Result<QueryPlan, WeaverError> {
        debug_span!("to_plan").in_scope(|| {
            let db = self.db.upgrade().ok_or(WeaverError::NoCoreAvailable)?;
            debug!("upgraded weaver db from weak");
            let socket = db.connect();
            debug!("connected socket to weaver db");
            debug!("getting cost table...");
            let cost_table = socket
                .get_table(&("weaver".into(), "cost".into()))
                .map_err(|_| WeaverError::CostTableNotLoaded)?;

            let cost_table = CostTable::from_table(&cost_table, tx);
            if cost_table != *self.cost_table.borrow() {
                *self.cost_table.borrow_mut() = cost_table;
            }

            self.to_plan_node(query, &socket, function_registry, plan_context.into())
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
                Query::Explain(e) => {
                    stack.push(*e);
                }
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
        function_registry: &FunctionRegistry,
        plan_context: Option<&WeaverProcessInfo>,
    ) -> Result<QueryPlanNode, WeaverError> {
        debug!("creating query plan from {}", query);
        let tables = debug_span!("finding involved tables")
            .in_scope(|| self.get_involved_tables(query, plan_context))?;
        debug!("collected tables: {:?}", tables.keys());
        debug!("resolving all identifiers");
        let query = &{
            let mut query = query.clone();

            let in_use = plan_context
                .and_then(|info| info.using.as_ref())
                .map(Identifier::new);

            let mut resolved = IdentifierResolver::new(in_use, |column_ref| {
                self.resolve_column_ref(column_ref, &tables, plan_context)
            });
            debug_span!("column references resolver")
                .in_scope(|| resolved.visit_query_mut(&mut query))?;
            query
        };

        let node = match query {
            Query::Select(select) => {
                self.select_to_plan_node(db, plan_context, &tables, select, function_registry)
            }
            Query::Explain(explained) => {
                let query = self.to_plan_node(explained, db, function_registry, plan_context)?;
                QueryPlanNode::builder()
                    .rows(0)
                    .cost(Cost::new(1.0, 0, None))
                    .kind(QueryPlanKind::Explain {
                        explained: Box::new(query),
                    })
                    .schema(QueryPlan::explain_schema())
                    .build()
            }
            Query::QueryList(_) => {
                todo!("query list")
            }
            Query::Create(Create::Table(create_table)) => {
                let mut create_table = create_table.clone();
                if create_table.schema.is_none() {
                    if let Some(current) = plan_context.and_then(|ctx| ctx.using.as_ref()) {
                        create_table.schema = Some(Identifier::new(current))
                    } else {
                        return Err(WeaverError::NoDefaultSchema);
                    }
                }

                QueryPlanNode::builder()
                    .rows(0)
                    .cost(Cost::new(1.0, 0, None))
                    .kind(QueryPlanKind::CreateTable {
                        table_def: create_table,
                    })
                    .schema(QueryPlan::ddl_result_schema())
                    .build()
            }
            Query::LoadData(load_data) => {
                let mut load_data = load_data.clone();
                if load_data.schema.is_none() {
                    if let Some(current) = plan_context.and_then(|ctx| ctx.using.as_ref()) {
                        load_data.schema = Some(Identifier::new(current))
                    } else {
                        return Err(WeaverError::NoDefaultSchema);
                    }
                }
                QueryPlanNode::builder()
                    .rows(0)
                    .cost(Cost::new(2.0, 1, None))
                    .kind(QueryPlanKind::LoadData { load_data })
                    .schema(QueryPlan::ddl_result_schema())
                    .build()
            }
            Query::KillProcess(pid) => QueryPlanNode::builder()
                .rows(0)
                .cost(Cost::new(0.0, 0, None))
                .kind(QueryPlanKind::KillProcess { pid: *pid as u32 })
                .schema(QueryPlan::ddl_result_schema())
                .build(),
            #[allow(unreachable_patterns)]
            _other => {
                unimplemented!("{_other:?}")
            }
        };
        node
    }

    #[allow(clippy::wrong_self_convention)]
    fn from_to_plan_node(
        &self,
        db: &DbSocket,
        plan_context: Option<&WeaverProcessInfo>,
        real_tables: &HashMap<TableRef, TableSchema>,
        from: &FromClause,
        function_registry: &FunctionRegistry,
    ) -> Result<QueryPlanNode, WeaverError> {
        let from = &from.0;
        self.table_or_sub_query_to_plan_node(db, plan_context, real_tables, from, function_registry)
    }

    fn table_or_sub_query_to_plan_node(
        &self,
        db: &DbSocket,
        plan_context: Option<&WeaverProcessInfo>,
        real_tables: &HashMap<TableRef, TableSchema>,
        from: &TableOrSubQuery,
        function_registry: &FunctionRegistry,
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
                let table_schema = real_tables
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
                let node = self.select_to_plan_node(
                    db,
                    plan_context,
                    real_tables,
                    select,
                    function_registry,
                )?;
                match alias {
                    None => Ok(node),
                    Some(_alias) => {
                        unimplemented!("select query aliasing");
                    }
                }
            }
            TableOrSubQuery::Multiple(_) => {
                unimplemented!("FULL OUTER JOIN");
            }
            TableOrSubQuery::JoinClause(join_clause) => self.join_to_plan_node(
                db,
                plan_context,
                real_tables,
                join_clause,
                function_registry,
            ),
        }
    }

    fn select_to_plan_node(
        &self,
        db: &DbSocket,
        plan_context: Option<&WeaverProcessInfo>,
        real_tables: &HashMap<TableRef, TableSchema>,
        select: &Select,
        function_registry: &FunctionRegistry,
    ) -> Result<QueryPlanNode, WeaverError> {
        error_span!("SELECT").in_scope(|| -> Result<QueryPlanNode, WeaverError> {
            let Select {
                columns,
                from,
                condition,
                group_by,
                order_by,
                limit,
                offset,
            } = select;

            match from {
                None => {
                    todo!("no from")
                }
                Some(from) => {
                    let from_node = self.from_to_plan_node(
                        db,
                        plan_context,
                        real_tables,
                        from,
                        function_registry,
                    )?;
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

                    let mut outer = match group_by {
                        None => {
                            // no grouping allows for normal projection
                            let (projected_schema, columns) = self.table_schema_for_projection(
                                columns,
                                &filtered,
                                function_registry,
                            )?;

                            QueryPlanNode::builder()
                                .cost(self.get_cost("PROJECT")?)
                                .rows(filtered.rows)
                                .kind(QueryPlanKind::Project {
                                    columns,
                                    projected: Box::new(filtered),
                                })
                                .schema(projected_schema)
                                .build()?
                        }
                        Some(grouped) => self.group_by_to_plan_node(
                            columns,
                            grouped,
                            filtered,
                            function_registry,
                        )?,
                    };

                    if let Some(order) = order_by {
                        let outer_schema = outer.schema().clone();
                        outer = QueryPlanNode::builder()
                            .cost(self.get_cost("ORDER")?)
                            .rows(outer.rows)
                            .kind(QueryPlanKind::OrderedBy {
                                ordered: Box::new(outer),
                                order: order
                                    .iter()
                                    .map(|OrderBy(expr, dir)| {
                                        (expr.clone(), dir.unwrap_or_default())
                                    })
                                    .collect(),
                            })
                            .schema(outer_schema)
                            .build()?;
                    }

                    if let (&Some(limit), &offset) = (limit, offset) {
                        let outer_schema = outer.schema().clone();
                        outer = QueryPlanNode::builder()
                            .cost(self.get_cost("LIMIT-OFFSET")?)
                            .rows(limit)
                            .kind(QueryPlanKind::GetPage {
                                base: Box::new(outer),
                                offset: offset.unwrap_or(0) as usize,
                                limit: Some(limit as usize),
                            })
                            .schema(outer_schema)
                            .build()?;
                    }

                    Ok(outer)
                }
            }
        })
    }

    fn table_schema_for_projection(
        &self,
        columns: &Vec<ResultColumn>,
        from_node: &QueryPlanNode,
        function_registry: &FunctionRegistry,
    ) -> Result<(TableSchema, Vec<Expr>), WeaverError> {
        let source_schema = &from_node.schema;
        let mut schema_builder = TableSchemaBuilder::new("<query>", "<projection>");
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
                ResultColumn::TableWildcard(_table) => {}
                ResultColumn::Expr { expr, alias } => {
                    let name = match alias {
                        None => expr.to_string(),
                        Some(alias) => alias.to_string(),
                    };
                    let non_null = matches!(expr, Expr::Column { .. });

                    let db_type = expr.type_of(function_registry, Some(from_node.schema()))?;

                    let mut cd = ColumnDefinition::new(name, db_type, non_null, None, None)?;
                    if let Expr::Column { column } = expr {
                        if let Some(resolved) = column.resolved() {
                            cd.set_source_column(resolved.clone());
                        }
                    }

                    cols.push(expr.clone());
                    schema_builder = schema_builder.column_definition(cd);
                }
            }
        }
        Ok((schema_builder.build()?, cols))
    }

    fn group_by_to_plan_node(
        &self,
        columns: &Vec<ResultColumn>,
        groups: &[Expr],
        grouped: QueryPlanNode,
        function_registry: &FunctionRegistry,
    ) -> Result<QueryPlanNode, WeaverError> {
        for column in columns {
            match column {
                ResultColumn::Wildcard => {
                    return Err(WeaverError::WildcardIsNeverFunctionallyDependent)
                }
                ResultColumn::TableWildcard(_) => {
                    return Err(WeaverError::WildcardIsNeverFunctionallyDependent)
                }
                ResultColumn::Expr { expr, .. } => {
                    if !self.is_functionally_dependent(
                        grouped.schema(),
                        groups,
                        expr,
                        function_registry,
                    )? {
                        return Err(WeaverError::ExpressionNotFunctionallyDependentOnGroupBy(
                            expr.clone(),
                            groups.to_owned(),
                        ));
                    }
                }
            }
        }

        let _source_schema = &grouped.schema;
        let mut schema_builder = TableSchemaBuilder::new("<query>", "<grouped_by>");
        let mut cols = vec![];
        for result_column in columns {
            match result_column {
                ResultColumn::Expr { expr, alias } => {
                    let name = match alias {
                        None => expr.to_string(),
                        Some(alias) => alias.to_string(),
                    };
                    let non_null = matches!(expr, Expr::Column { column: _ });

                    let db_type = expr.type_of(function_registry, Some(grouped.schema()))?;

                    let mut cd = ColumnDefinition::new(name, db_type, non_null, None, None)?;
                    if let Expr::Column { column } = expr {
                        if let Some(resolved) = column.resolved() {
                            cd.set_source_column(resolved.clone());
                        }
                    }

                    cols.push(expr.clone());
                    schema_builder = schema_builder.column_definition(cd);
                }
                _ => return Err(WeaverError::WildcardIsNeverFunctionallyDependent),
            }
        }

        let schema = schema_builder.build()?;

        QueryPlanNode::builder()
            .cost(self.get_cost("GROUP_BY")?)
            .rows(grouped.rows)
            .kind(QueryPlanKind::GroupBy {
                grouped: Box::new(grouped),
                grouped_by: groups.to_owned(),
                result_columns: cols,
            })
            .schema(schema)
            .build()
    }

    /// checks if a given expressions is functionally dependent on another.
    /// This should mean that all references to columns not part of the `sources` parameter
    /// are within an aggregating function.
    ///
    /// # Example
    /// ```sql
    /// // ok because level is functionally dependent on level and avg is within an aggregating function
    /// SELECT level, AVG(age) + level ... GROUP BY level
    /// // not ok because age is not functionally dependent on level
    /// SELECT level, level + age ... GROUP BY level
    /// ```
    fn is_functionally_dependent(
        &self,
        schema: &TableSchema,
        sources: &[Expr],
        dependent: &Expr,
        function_registry: &FunctionRegistry,
    ) -> Result<bool, WeaverError> {
        fn is_functionally_dependent_helper(
            schema: &TableSchema,
            source_columns: &[ResolvedColumnRef],
            dependent: &Expr,
            function_registry: &FunctionRegistry,
        ) -> Result<bool, WeaverError> {
            Ok(match dependent {
                Expr::Column { column } => source_columns
                    .contains(column.resolved().expect("all columns must be resolved")),
                Expr::Literal { .. } => true,
                Expr::BindParameter { .. } => {
                    panic!("bind parameter in invalid locaction")
                }
                Expr::Unary { expr, .. } => is_functionally_dependent_helper(
                    schema,
                    source_columns,
                    expr,
                    function_registry,
                )?,
                Expr::Binary { left, right, .. } => {
                    is_functionally_dependent_helper(
                        schema,
                        source_columns,
                        left,
                        function_registry,
                    )? && is_functionally_dependent_helper(
                        schema,
                        source_columns,
                        right,
                        function_registry,
                    )?
                }
                Expr::FunctionCall { function, args } => {
                    let FunctionKind { normal, aggregate } =
                        find_function(function_registry, function, args, schema)?;

                    debug!("normal: {normal:?}, aggregate: {aggregate:?}");
                    match (normal, aggregate) {
                        (_, Some(_)) => true,
                        (Some(_), None) => match args {
                            FunctionArgs::Params { exprs, .. } => {
                                exprs.iter().try_fold(true, |state, expr| {
                                    is_functionally_dependent_helper(
                                        schema,
                                        source_columns,
                                        expr,
                                        function_registry,
                                    )
                                    .map(|output| output && state)
                                })?
                            }
                            FunctionArgs::Wildcard { .. } => false,
                        },
                        (None, None) => false,
                    }
                }
            })
        }

        let source_columns = sources
            .iter()
            .flat_map(|e| e.columns().into_iter().flat_map(|i| i.resolved().cloned()))
            .collect::<Vec<_>>();
        debug!(
            "seeing if {:?} is functionally dependent on {:?}",
            dependent, source_columns
        );
        is_functionally_dependent_helper(
            schema,
            source_columns.as_slice(),
            dependent,
            function_registry,
        )
    }

    fn join_to_plan_node(
        &self,
        db: &DbSocket,
        plan_context: Option<&WeaverProcessInfo>,
        real_tables: &HashMap<TableRef, TableSchema>,
        join_clause: &JoinClause,
        function_registry: &FunctionRegistry,
    ) -> Result<QueryPlanNode, WeaverError> {
        error_span!("JOIN").in_scope(|| -> Result<QueryPlanNode, WeaverError> {
            let JoinClause {
                left,
                op: _,
                right,
                constraint: _,
            } = join_clause;

            let left = self.table_or_sub_query_to_plan_node(
                db,
                plan_context,
                real_tables,
                left,
                function_registry,
            )?;
            let right = self.table_or_sub_query_to_plan_node(
                db,
                plan_context,
                real_tables,
                right,
                function_registry,
            )?;

            let strategies = self
                .join_strategy_selector
                .get_strategies_for_join(join_clause)?;
            debug!("join strategies for {join_clause}: {strategies:#?}");

            let (strategy, _) = strategies.first().expect("no applicable strategies");

            let rows = match join_clause.op {
                JoinOperator::Left => left.rows,
                JoinOperator::Right => right.rows,
                JoinOperator::Full => left.rows.max(right.rows),
                JoinOperator::Inner => left.rows.max(right.rows),
                JoinOperator::Cross => left.rows * right.rows,
                JoinOperator::Outer => left.rows + right.rows,
            };
            strategy.join_node(rows, left, right, join_clause)
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
                        .to_key_index(key, Some(left), involved_tables, ctx)
                        .and_then(|mut left| {
                            self.to_key_index(key, Some(right), involved_tables, ctx)
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
        let split = col.split('.').collect::<Vec<_>>();
        match split[..] {
            [col] => UnresolvedColumnRef::with_column(Identifier::new(col)).into(),
            [table, col] => {
                UnresolvedColumnRef::with_table(Identifier::new(table), Identifier::new(col)).into()
            }
            [schema, table, col] => ResolvedColumnRef::new(
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
        _ctx: Option<&WeaverProcessInfo>,
    ) -> Result<ResolvedColumnRef, WeaverError> {
        match (column_ref.table(), column_ref.column()) {
            (None, col) => {
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
                            .iter()
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
            (Some(table), col) => {
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
                            .iter()
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
    _in_use_schema: Option<Identifier>,
    select_level: usize,
    column_aliases: BTreeMap<usize, Vec<Identifier>>,
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
            _in_use_schema: in_use_schema,
            select_level: 0,
            column_aliases: Default::default(),
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
                let column_id = unresolved.column();
                if let Some(level_aliases) = self.column_aliases.get(&self.select_level) {
                    if level_aliases.contains(column_id) {
                        let resolved = ResolvedColumnRef::new(
                            "<select>",
                            format!("{}", self.select_level),
                            column_id.clone(),
                        );
                        debug!("resolved = {resolved}");
                        *column = ColumnRef::Resolved(resolved);
                        return Ok(());
                    }
                }

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
            TableOrSubQuery::Select {
                select: _,
                alias: _,
            } => {
                panic!("don't know how to handle subquery and aliases");
            }
            _ => {}
        }

        visit_table_or_sub_query_mut(self, table_or_sub_query)
    }

    fn visit_result_column_mut(
        &mut self,
        result_column: &mut ResultColumn,
    ) -> Result<(), Self::Err> {
        if let ResultColumn::Expr {
            expr: _,
            alias: Some(alias),
        } = result_column
        {
            // debug!("dealing with result column {expr} alias {alias}")
            self.column_aliases
                .entry(self.select_level)
                .or_default()
                .push(alias.clone());
        }

        visit_result_column_mut(self, result_column)
    }

    fn visit_select_mut(&mut self, select: &mut Select) -> Result<(), Self::Err> {
        self.select_level += 1;
        let ret = visit_select_mut(self, select);
        self.select_level -= 1;
        ret
    }
}
