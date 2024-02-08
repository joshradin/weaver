//! Creates an unoptimized [query plan](QueryPlan) from a [query](Query)

use std::collections::{HashMap, HashSet, VecDeque};

use crate::data::row::OwnedRow;
use tracing::{debug, error_span, trace};

use crate::db::server::processes::WeaverProcessInfo;
use crate::db::server::socket::DbSocket;
use crate::db::server::WeakWeaverDb;
use crate::dynamic_table::{Table, TableCol};
use crate::error::Error;
use crate::key::KeyData;
use crate::queries::query_plan::{QueryPlan, QueryPlanKind, QueryPlanNode};
use crate::rows::{KeyIndex, KeyIndexKind};
use crate::tables::table_schema::{Key, TableSchema, TableSchemaBuilder};
use crate::tables::TableRef;
use weaver_ast::ast::{BinaryOp, Query, Select, Value, Where};

#[derive(Debug)]
pub struct QueryPlanFactory {
    db: WeakWeaverDb,
}

impl QueryPlanFactory {
    pub fn new(db: WeakWeaverDb) -> Self {
        Self { db }
    }

    /// Converts a given query to a plan
    pub fn to_plan<'a>(
        &self,
        query: &Query,
        plan_context: impl Into<Option<&'a WeaverProcessInfo>>,
    ) -> Result<QueryPlan, Error> {
        error_span!("to_plan").in_scope(|| {
            let db = self.db.upgrade().ok_or(Error::NoCoreAvailable)?;
            debug!("upgraded weaver db from weak");
            let socket = db.connect();
            debug!("connected socket to weaver db");
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
                from: table_ref, ..
            }) => emit.push(self.table_ref(table_ref, plan_context)?),
        }
        Ok(emit)
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
            Query::Select(Select {
                columns,
                from: table_ref,
                condition,
                limit,
                offset,
            }) => error_span!("SELECT").in_scope(|| -> Result<QueryPlanNode, Error> {
                let table_ref = self.table_ref(table_ref, plan_context)?;
                trace!("getting table {:?}", table_ref);
                let table = db.get_table(&table_ref)?;
                trace!("table = {:?}", table);
                let mut applicable_keys =
                    match condition
                        .as_ref()
                        .map(|condition| -> Result<VecDeque<&Key>, Error> {
                            let condition: HashSet<(String, String, String)> = condition
                                .columns()
                                .iter()
                                .map(|c| self.column_ref(c, &tables, plan_context))
                                .collect::<Result<_, _>>()?;
                            debug!("columns used in condition: {condition:?}");
                            Ok(table
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
                                        condition.contains(&column_reference(&table, column))
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
                    applicable_keys.push_front(table.schema().primary_key()?);
                }
                let mut applicable_keys = Vec::from(applicable_keys);
                applicable_keys.sort_by_cached_key(|key| {
                    if key.primary() {
                        -1_isize
                    } else {
                        key.columns().len() as isize
                    }
                });

                let keys = applicable_keys
                    .into_iter()
                    .map(|key| self.to_key_index(key, condition.as_ref(), &tables, plan_context))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .flatten()
                    .collect();

                Ok(QueryPlanNode {
                    cost: f64::MAX,
                    rows: u64::MAX,
                    kind: QueryPlanKind::SelectByKey {
                        table: table_ref.to_owned(),
                        key_index: keys,
                    },
                    schema: TableSchemaBuilder::from(table.schema())
                        .in_memory()
                        .build()?,
                })
            }),
        };
        node
    }

    pub fn to_key_index(
        &self,
        key: &Key,
        where_: Option<&Where>,
        involved_tables: &HashMap<TableRef, TableSchema>,
        ctx: Option<&WeaverProcessInfo>,
    ) -> Result<Vec<KeyIndex>, Error> {
        match where_ {
            None => Ok(vec![key.all()]),
            Some(cond) => match cond {
                Where::Op(col, BinaryOp::Eq, Value::Literal(value)) => {
                    let col = self.column_ref(col, involved_tables, ctx)?;
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
                Where::All(wheres) | Where::Any(wheres) => Ok(wheres
                    .iter()
                    .map(|where_| self.to_key_index(key, Some(where_), involved_tables, ctx))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .flatten()
                    .collect()),
                _ => Ok(vec![]),
            },
        }
    }

    /// Convert to a table reference
    pub fn table_ref(
        &self,
        tb_ref: impl AsRef<str>,
        ctx: Option<&WeaverProcessInfo>,
    ) -> Result<TableRef, Error> {
        let tb_ref = tb_ref.as_ref().to_string();
        let in_use = ctx.and_then(|info| info.using.as_ref());
        trace!(
            "creating table ref from tb_ref {:?} and in_use {:?}",
            tb_ref,
            in_use
        );
        match tb_ref.split_once('.') {
            None => in_use
                .ok_or(Error::UnQualifedTableWithoutInUseSchema)
                .map(|schema| (schema.to_string(), tb_ref.to_string())),
            Some((schema, table)) => Ok((schema.to_string(), table.to_string())),
        }
    }

    pub fn column_ref(
        &self,
        column_ref: impl AsRef<str>,
        involved_tables: &HashMap<TableRef, TableSchema>,
        ctx: Option<&WeaverProcessInfo>,
    ) -> Result<TableCol, Error> {
        let split: Vec<&str> = column_ref.as_ref().split('.').collect();
        match split.as_slice() {
            &[col] => {
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
            &[table, col] => {
                debug!("finding column ?.{table}.{col}");

                todo!()
            }
            &[schema, table, col] => Ok((schema.to_string(), table.to_string(), col.to_string())),
            _ => Err(Error::ParseError(
                "column refernce can only have at most 3 segments".to_string(),
            )),
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
