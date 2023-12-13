//! Creates an unoptimized [query plan](QueryPlan) from a [query](Query)

use crate::db::server::socket::DbSocket;
use crate::dynamic_table::Table;
use crate::error::Error;
use crate::queries::ast::Query;
use crate::queries::query_plan::{QueryPlan, QueryPlanKind, QueryPlanNode};
use crate::rows::KeyIndex;
use std::collections::VecDeque;
use crate::db::server::WeakWeaverDb;

#[derive(Debug)]
pub struct QueryPlanFactory {
    db: WeakWeaverDb,
}

impl QueryPlanFactory {
    pub fn new(db: WeakWeaverDb) -> Self {
        Self { db }
    }

    /// Converts a given query to a plan
    pub fn to_plan(&self, query: &Query) -> Result<QueryPlan, Error> {
        let db = self.db.upgrade().ok_or(Error::NoCoreAvailable)?;
        let socket = db.connect();
        self.to_plan_node(query, &socket).map(QueryPlan::new)
    }

    /// Converts a given query to a plan
    fn to_plan_node(&self, query: &Query, db: &DbSocket) -> Result<QueryPlanNode, Error> {
        let node = match query {
            Query::Select {
                columns,
                table_ref,
                condition,
                limit,
                offset,
            } => {
                let table = db.get_table(table_ref)?;
                let mut applicable_keys = condition
                    .as_ref()
                    .map(|condition| {
                        let condition = condition.columns();
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

                QueryPlanNode {
                    cost: f64::MAX,
                    rows: u64::MAX,
                    kind: QueryPlanKind::SelectByKey {
                        table: table_ref.to_owned(),
                        key_index: keys,
                    },
                }
            }
        };
        Ok(node)
    }
}

pub fn column_reference(table: &Table, column: &String) -> (String, String, String) {
    (
        table.schema().schema().to_string(),
        table.schema().name().to_string(),
        column.to_string(),
    )
}
