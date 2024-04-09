//! Query plan optimization

use std::cell::RefCell;
use std::collections::{HashSet};
use std::fmt::{Debug};



use static_assertions::assert_obj_safe;
use tracing::{debug, debug_span, trace};
use uuid::Uuid;

use weaver_ast::ast::{BinaryOp, ColumnRef, Expr, Identifier, ReferencesCols, ResolvedColumnRef};


use crate::db::server::socket::DbSocket;
use crate::db::server::WeakWeaverDb;
use crate::dynamic_table::{DynamicTable, HasSchema};
use crate::error::WeaverError;

use crate::queries::query_cost::CostTable;
use crate::queries::query_plan::{QueryPlan, QueryPlanKind, QueryPlanNode};

use crate::storage::tables::table_schema::TableSchema;
use crate::tx::Tx;

/// An optimizer
pub trait Optimizer {
    fn optimize(
        &self,
        tx: &Tx,
        db_socket: &DbSocket,
        query: &mut QueryPlan,
    ) -> Result<(), WeaverError>;
}
assert_obj_safe!(Optimizer);

#[derive(Debug)]
pub struct QueryPlanOptimizer {
    db: WeakWeaverDb,
    cost_table: RefCell<CostTable>,
}

impl QueryPlanOptimizer {
    pub fn new(db: WeakWeaverDb) -> Self {
        Self {
            db,
            cost_table: Default::default(),
        }
    }

    /// currently based on this article https://www.geeksforgeeks.org/query-optimization-in-relational-algebra/
    pub fn optimize(&self, tx: &Tx, query: &mut QueryPlan) -> Result<(), WeaverError> {
        let span = debug_span!("optimize_plan");
        let _enter = span.enter();

        let initial_cost = query.root().cost();

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

        // sigma cascade to seperate all binops
        sigma_cascade(query.root_mut())?;
        // push down expressions
        push_down_filters(query, &socket)?;


        let new_cost = query.root().cost();
        debug!("optimization changed cost from {initial_cost} to {new_cost}");

        Ok(())
    }
}

/// splits conjunction filters into multiple filters
fn sigma_cascade(query: &mut QueryPlanNode) -> Result<(), WeaverError> {
    loop {
        match &mut query.kind {
            QueryPlanKind::Filter {
                filtered,
                condition:
                    Expr::Binary {
                        left,
                        op: BinaryOp::And,
                        right,
                    },
            } => {
                if left.columns() != right.columns() {
                    let lower = QueryPlanNode::builder()
                        .rows(query.rows)
                        .cost(query.cost)
                        .schema(query.schema.clone())
                        .kind(QueryPlanKind::Filter {
                            filtered: filtered.clone(),
                            condition: *right.clone(),
                        })
                        .build()?;

                    let upper = QueryPlanNode::builder()
                        .rows(query.rows)
                        .cost(query.cost)
                        .schema(query.schema.clone())
                        .kind(QueryPlanKind::Filter {
                            filtered: Box::new(lower),
                            condition: *left.clone(),
                        })
                        .build()?;

                    *query = upper;
                }
            }
            _ => break,
        }
    }

    query
        .children_mut()
        .into_iter()
        .try_for_each(|child| sigma_cascade(child))?;

    Ok(())
}

/// tries to push down filters as far down as possible
fn push_down_filters(query: &mut QueryPlan, socket: &DbSocket) -> Result<(), WeaverError> {
    let mut visited = HashSet::<Uuid>::new();

    while let Some(node_id) = query
        .root()
        .prefix_order()
        .iter()
        .find(|node| {
            matches!(node.kind, QueryPlanKind::Filter { .. }) && !visited.contains(&node.id())
        })
        .map(|node| node.id())
    {
        let plan_node = query.get_mut(&node_id).unwrap();
        push_down_filter(plan_node, socket)?;
        visited.insert(node_id);
    }

    Ok(())
}

fn push_down_filter(parent: &mut QueryPlanNode, socket: &DbSocket) -> Result<(), WeaverError> {
    let QueryPlanKind::Filter {
        filtered: child,
        condition,
    } = &parent.kind
    else {
        panic!("should always be a filter node")
    };

    let created = match &child.kind {
        QueryPlanKind::Filter {
            filtered: grandchild,
            condition: _,
        } => {
            let mut parent = parent.clone();
            let mut child = *child.clone();
            let grandchild = *grandchild.clone();
            *parent.children_mut()[0] = grandchild;
            *child.children_mut()[0] = parent;
            push_down_filter(&mut child.children_mut()[0], socket)?;
            Some(child)
        }
        QueryPlanKind::Project {
            columns,
            projected: grandchild,
        } => {
            if columns
                .iter()
                .all(|expr| expr_exclusively_in_schema(expr, grandchild.schema()))
            {
                let mut parent = parent.clone();
                let mut child = *child.clone();
                let grandchild = *grandchild.clone();
                *parent.children_mut()[0] = grandchild;
                *child.children_mut()[0] = parent;
                push_down_filter(&mut child.children_mut()[0], socket)?;
                Some(child)
            } else {
                None
            }
        }
        QueryPlanKind::HashJoin { left, right, .. } => {
            let left = *left.clone();
            let right = *right.clone();

            if expr_exclusively_in_schema(condition, left.schema()) {
                let mut parent = parent.clone();
                let mut child = *child.clone();
                let grandchild = left.clone();
                parent.rows = grandchild.rows;
                parent.schema = grandchild.schema.clone();
                *parent.children_mut()[0] = grandchild;


                child.rows = parent.rows;
                *child.children_mut()[0] = parent;
                push_down_filter(&mut child.children_mut()[0], socket)?;
                Some(child)
            } else if expr_exclusively_in_schema(condition, right.schema()) {
                let mut parent = parent.clone();
                let mut child = *child.clone();
                let grandchild = right.clone();
                parent.rows = grandchild.rows;
                parent.schema = grandchild.schema.clone();
                *parent.children_mut()[0] = grandchild;
                child.rows = parent.rows;
                *child.children_mut()[1] = parent;

                push_down_filter(&mut child.children_mut()[1], socket)?;
                Some(child)
            } else {
                None
            }
        }
        QueryPlanKind::TableScan {
            schema,
            table,
            keys: Option::None,
        } => {
            let child_schema = child.schema();
            let condition: HashSet<ResolvedColumnRef> = condition
                .columns()
                .iter()
                .map(|c| {
                    c.resolved()
                        .expect("all columns should be resolved at this point")
                        .clone()
                })
                .collect();
            trace!("columns used in condition: {condition:?}");

            let mut applicable_keys = child_schema
                .keys()
                .iter()
                .filter_map(|key| {
                    trace!(
                        "checking if all of {:?} in condition columns {:?}",
                        key.columns(),
                        condition
                    );
                    if key.columns().iter().all(|column| {
                        let contains = condition.contains(&ResolvedColumnRef::new(
                            Identifier::new(child_schema.schema()),
                            Identifier::new(child_schema.name()),
                            Identifier::new(column),
                        ));
                        trace!("does {condition:?} contain {column:?}? -> {contains}");
                        contains
                    }) {
                        Some(key)
                    } else {
                        None
                    }
                })
                .map(|key| {
                    // key to key_index
                    key.all()
                })
                .collect::<Vec<_>>();

            applicable_keys.sort_by_cached_key(|key_index| {
                socket
                    .get_table(&(schema.to_string(), table.to_string()))
                    .and_then(|table| table.size_estimate(key_index))
                    .unwrap_or(u64::MAX)
            });
            trace!("applicable keys: {applicable_keys:?}");
            if applicable_keys.len() > 0 {
                let mut child = *child.clone();
                let QueryPlanKind::TableScan { keys, .. } = &mut child.kind else {
                    unreachable!()
                };

                let min_rows = applicable_keys.iter()
                    .flat_map(|key_index| {
                        socket
                            .get_table(&(schema.to_string(), table.to_string()))
                            .and_then(|table| table.size_estimate(key_index))
                    })
                    .chain([child.rows])
                    .min()
                    .unwrap();

                child.rows = min_rows;
                *keys = Some(applicable_keys);
                trace!("merged into {child:#?}");
                Some(child)
            } else {
                None
            }
        }
        _ => return Ok(()),
    };

    if let Some(created) = created {
        *parent = created;
    }

    Ok(())
}

/// checks if an expression contains attributes exclusively in one schema
fn expr_exclusively_in_schema(expr: &Expr, schema: &TableSchema) -> bool {
    for col in expr.columns() {
        match col {
            ColumnRef::Unresolved(_) => return false,
            ColumnRef::Resolved(resolve) => {
                if schema.column_index_by_source(&resolve).is_none() {
                    return false;
                }
            }
        }
    }

    true
}
