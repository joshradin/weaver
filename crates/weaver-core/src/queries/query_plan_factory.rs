//! Creates an unoptimized [query plan](QueryPlan) from a [query](Query)

use rand::Rng;
use std::cell::{OnceCell, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::From;
use parking_lot::RwLock;

use tracing::{debug, error_span, trace};

use crate::data::types::Type;
use weaver_ast::ast;
use weaver_ast::ast::{
    BinaryOp, Expr, FromClause, Identifier, Query, ReferencesCols, ResultColumn, Select,
    TableOrSubQuery, UnresolvedColumnRef,
};

use crate::db::server::processes::WeaverProcessInfo;
use crate::db::server::socket::DbSocket;
use crate::db::server::WeakWeaverDb;
use crate::dynamic_table::{DynamicTable, HasSchema, Table, TableCol};
use crate::error::Error;
use crate::key::KeyData;
use crate::queries::query_cost::{Cost, CostTable};
use crate::queries::query_plan::{QueryPlan, QueryPlanKind, QueryPlanNode};
use crate::rows::{KeyIndex, KeyIndexKind};
use crate::tables::table_schema::{Key, TableSchema, TableSchemaBuilder};
use crate::tables::{table_schema, TableRef};
use crate::tx::Tx;

#[derive(Debug)]
pub struct QueryPlanFactory {
    db: WeakWeaverDb,
    cost_table: RefCell<CostTable>,
}

impl QueryPlanFactory {
    pub fn new(db: WeakWeaverDb) -> Self {
        Self {
            db,
            cost_table: Default::default(),
        }
    }

    /// Converts a given query to a plan
    pub fn to_plan<'a>(
        &self,
        tx: &Tx,
        query: &Query,
        plan_context: impl Into<Option<&'a WeaverProcessInfo>>,
    ) -> Result<QueryPlan, Error> {
        error_span!("to_plan").in_scope(|| {
            let db = self.db.upgrade().ok_or(Error::NoCoreAvailable)?;
            debug!("upgraded weaver db from weak");
            let socket = db.connect();
            debug!("connected socket to weaver db");
            debug!("getting cost table...");
            let cost_table = socket
                .get_table(&("weaver".into(), "cost".into()))
                .map_err(|_| Error::CostTableNotLoaded)?;

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
    ) -> Result<HashMap<TableRef, TableSchema>, Error> {
        let involved = self.get_involved_table_refs(query, plan_context)?;
        let Some(core) = self.db.upgrade() else {
            return Err(Error::NoCoreAvailable);
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
    ) -> Result<Vec<TableRef>, Error> {
        let mut emit = vec![];
        match query {
            Query::Select(Select {
                from: Some(ast::FromClause(table_ref)),
                ..
            }) => match table_ref {
                TableOrSubQuery::Table {
                    schema, table_name, ..
                } => match schema {
                    None => emit.push(self.table_ref((None, table_name.as_ref()), plan_context)?),
                    Some(schema) => emit.push((schema.to_string(), table_name.to_string())),
                },
                TableOrSubQuery::Select { .. } => {}
                TableOrSubQuery::Multiple(_) => {}
                TableOrSubQuery::JoinClause(_) => {}
            },
            _ => {}
        }
        Ok(emit)
    }

    fn get_cost(&self, key: impl AsRef<str>) -> Result<Cost, Error> {
        let key = key.as_ref().to_string();
        self.cost_table
            .borrow()
            .get(&key)
            .ok_or_else(|| Error::UnknownCostKey(key))
            .copied()
    }

    /// Converts a given query to a plan
    fn to_plan_node(
        &self,
        query: &Query,
        db: &DbSocket,
        plan_context: Option<&WeaverProcessInfo>,
    ) -> Result<QueryPlanNode, Error> {
        let tables = self.get_involved_tables(query, plan_context)?;
        debug!("collected tables: {:#?}", tables);

        let node = match query {
            Query::Select(select) => self.select_to_plan_node(db, plan_context, &tables, select),
            Query::Explain(_) => {
                todo!("explain")
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
    ) -> Result<QueryPlanNode, Error> {
        let from = &from.0;
        self.table_or_sub_query_to_plan_node(db, plan_context, real_tables, from)
    }

    fn table_or_sub_query_to_plan_node(
        &self,
        db: &DbSocket,
        plan_context: Option<&WeaverProcessInfo>,
        real_tables: &HashMap<TableRef, TableSchema>,
        from: &TableOrSubQuery,
    ) -> Result<QueryPlanNode, Error> {
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

                let (schema, table) = table_ref;

                Ok(QueryPlanNode {
                    cost: self.get_cost("LOAD_TABLE")?,
                    rows: u64::MAX,
                    kind: QueryPlanKind::LoadTable { schema, table },
                    schema: table_schema,
                    alias: alias.as_ref().map(|i| i.to_string()),
                })
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
            TableOrSubQuery::JoinClause(_) => {
                unimplemented!("JOIN");
            }
        }
    }

    fn select_to_plan_node(
        &self,
        db: &DbSocket,
        plan_context: Option<&WeaverProcessInfo>,
        real_tables: &HashMap<TableRef, TableSchema>,
        select: &Select,
    ) -> Result<QueryPlanNode, Error> {
        error_span!("SELECT").in_scope(|| -> Result<QueryPlanNode, Error> {
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

                    let mut applicable_keys =
                        match condition
                            .as_ref()
                            .map(|condition| -> Result<VecDeque<&Key>, Error> {
                                let condition: HashSet<(String, String, String)> = condition
                                    .columns()
                                    .iter()
                                    .map(|c| self.resolve_column_ref(c, &real_tables, plan_context))
                                    .collect::<Result<_, _>>()?;
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
                                            let ref column_ref = self.parse_column_ref(column);
                                            let resolved = self
                                                .resolve_column_ref(
                                                    column_ref,
                                                    real_tables,
                                                    plan_context,
                                                )
                                                .expect("could not resolve");
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

                    let keys = applicable_keys
                        .into_iter()
                        .map(|key| {
                            self.to_key_index(key, condition.as_ref(), &real_tables, plan_context)
                        })
                        .collect::<Result<Vec<_>, _>>()?
                        .into_iter()
                        .flatten()
                        .collect();

                    debug!("keys: {:?}", keys);

                    let schema =
                        if columns.len() == 1 && matches!(columns[0], ResultColumn::Wildcard) {
                            from_node.schema.clone()
                        } else {
                            let mut schema_builder = TableSchemaBuilder::new(
                                "*",
                                format!("table_{}", rand::random::<u64>()),
                            );

                            for result_column in columns {
                                match result_column {
                                    ResultColumn::Wildcard => {
                                        // all columns from previous node
                                        for col in from_node.schema.columns() {
                                            schema_builder =
                                                schema_builder.column_definition(col.clone());
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

                                        schema_builder = schema_builder.column(
                                            name,
                                            Type::Boolean,
                                            non_null,
                                            None,
                                            None,
                                        )?;
                                    }
                                }
                            }
                            schema_builder.build()?
                        };

                    Ok(QueryPlanNode {
                        cost: self.get_cost("SELECT")?,
                        rows: u64::MAX,
                        kind: QueryPlanKind::SelectByKey {
                            to_select: Box::new(from_node),
                            key_index: keys,
                        },
                        schema,
                        alias: None,
                    })
                }
            }
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
    ) -> Result<Vec<KeyIndex>, Error> {
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

                        let col = self.resolve_column_ref(col, involved_tables, ctx)?;

                        let Expr::Literal { literal: value } = &**const_v else {
                            panic!("expr is constant but not a literal: {left:?}")
                        };

                        if key.columns().len() == 1 && key.columns().contains(&col.2) {
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
    ) -> Result<TableRef, Error> {
        let in_use = ctx.and_then(|info| info.using.as_ref());
        trace!(
            "creating table ref from tb_ref {:?} and in_use {:?}",
            tb_ref,
            in_use
        );
        match tb_ref.0 {
            None => in_use
                .ok_or(Error::UnQualifedTableWithoutInUseSchema)
                .map(|schema| (schema.to_string(), tb_ref.1.to_string())),
            Some(schema) => Ok((schema.to_string(), tb_ref.1.to_string())),
        }
    }

    pub fn parse_column_ref(&self, col: &str) -> UnresolvedColumnRef {
        let split = col.split(".").collect::<Vec<_>>();
        match &split[..] {
            &[col] => UnresolvedColumnRef::with_column(Identifier::new(col)),
            &[table, col] => {
                UnresolvedColumnRef::with_table(Identifier::new(table), Identifier::new(col))
            }
            &[schema, table, col] => UnresolvedColumnRef::with_schema(
                Identifier::new(schema),
                Identifier::new(table),
                Identifier::new(col),
            ),
            _ => panic!("column ref can not have more than three fields"),
        }
    }

    pub fn resolve_column_ref(
        &self,
        column_ref: &UnresolvedColumnRef,
        involved_tables: &HashMap<TableRef, TableSchema>,
        ctx: Option<&WeaverProcessInfo>,
    ) -> Result<TableCol, Error> {
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
                    &[] => Err(Error::ColumnNotFound(col.to_string())),
                    &[(schema, table)] => {
                        Ok((schema.to_string(), table.to_string(), col.to_string()))
                    }
                    slice => Err(Error::AmbiguousColumn {
                        col: col.to_string(),
                        positives: slice
                            .into_iter()
                            .map(|&(schema, table)| {
                                (schema.to_string(), table.to_string(), col.to_string())
                            })
                            .collect(),
                    }),
                }
            }
            (None, Some(table), col) => {
                debug!("finding column ?.{table}.{col}");

                todo!()
            }
            (Some(schema), Some(table), col) => {
                Ok((schema.to_string(), table.to_string(), col.to_string()))
            }
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
