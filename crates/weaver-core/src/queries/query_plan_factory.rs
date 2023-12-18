//! Creates an unoptimized [query plan](QueryPlan) from a [query](Query)

use crate::db::server::socket::DbSocket;
use crate::db::server::WeakWeaverDb;
use crate::dynamic_table::{Col, Table};
use crate::error::Error;
use crate::queries::ast::Query;
use crate::queries::query_plan::{QueryPlan, QueryPlanKind, QueryPlanNode};
use crate::rows::KeyIndex;
use std::collections::{HashMap, HashSet, VecDeque};
use tracing::{debug, error_span, trace};
use crate::db::server::processes::WeaverProcessInfo;
use crate::tables::TableRef;

#[derive(Debug)]
pub struct QueryPlanFactory {
    db: WeakWeaverDb,
}

impl QueryPlanFactory {
    pub fn new(db: WeakWeaverDb) -> Self {
        Self { db }
    }

    /// Converts a given query to a plan
    pub fn to_plan<'a>(&self, query: &Query, plan_context: impl Into<Option< &'a WeaverProcessInfo>>) -> Result<QueryPlan, Error> {
        error_span!("to_plan").in_scope(|| {
            let db = self.db.upgrade().ok_or(Error::NoCoreAvailable)?;
            debug!("upgraded weaver db from weak");
            let socket = db.connect();
            debug!("connected socket to weaver db");
            self.to_plan_node(query, &socket, plan_context.into()).map(QueryPlan::new)
        })
    }

    /// Converts a given query to a plan
    fn to_plan_node(&self, query: &Query, db: &DbSocket, plan_context: Option<&WeaverProcessInfo>) -> Result<QueryPlanNode, Error> {
        let node = match query {
            Query::Select {
                columns,
                table_ref,
                condition,
                limit,
                offset,
            } => {
                error_span!("SELECT").in_scope(||  -> Result<QueryPlanNode, Error>{
                    let table_ref = self.table_ref(table_ref, plan_context)?;
                    trace!("getting table {:?}", table_ref);
                    let table = db.get_table(&table_ref)?;
                    trace!("table = {:?}", table);
                    let mut applicable_keys = condition
                        .as_ref()
                        .map(|condition| {
                            let condition: HashSet<(String, String, String)> = condition
                                .columns()
                                .iter()
                                .map(|c| todo!("to qualified column"))
                                .collect();
                            table
                                .schema()
                                .keys()
                                .iter()
                                .filter_map(|key| {
                                    if key.columns().iter().all(|column| {
                                        condition.contains(&column_reference(&table, column))
                                    }) {
                                        Some(key)
                                    } else {
                                        None
                                    }
                                })
                                .collect::<VecDeque<_>>()
                        })
                        .unwrap_or_default();

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
                        .map(|key| KeyIndex::all(key.name()))
                        .collect::<Vec<_>>();

                    Ok(QueryPlanNode {
                        cost: f64::MAX,
                        rows: u64::MAX,
                        kind: QueryPlanKind::SelectByKey {
                            table: table_ref.to_owned(),
                            key_index: keys,
                        },
                    })
                })

            }
        };
        node
    }

    /// Convert to a table reference
    pub fn table_ref(&self, tb_ref: impl AsRef<str>, ctx: Option<&WeaverProcessInfo>) -> Result<TableRef, Error> {
        let tb_ref = tb_ref.as_ref().to_string();
        let in_use = ctx.and_then(|info| info.using.as_ref());
        trace!("creating table ref from tb_ref {:?} and in_use {:?}", tb_ref, in_use);
        match tb_ref.split_once('.') {
            None => {
                in_use.ok_or(Error::UnQualifedTableWithoutInUseSchema)
                    .map(|schema| {
                        (schema.to_string(), tb_ref.to_string())
                    })
            }
            Some((schema, table)) => {
                Ok((schema.to_string(), table.to_string()))
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
