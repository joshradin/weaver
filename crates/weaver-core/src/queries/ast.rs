//! Query asts


use std::collections::HashSet;
use serde::{Deserialize, Serialize};
use crate::data::values::Value;
use crate::dynamic_table::{Col, TableCol};
use crate::tables::TableRef;

/// The query type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Query {
    Select {
        columns: Vec<String>,
        table_ref: TableRef,
        condition: Option<Where>,
        limit: Option<u64>,
        offset: Option<u64>
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Where {
    Op(TableCol, Op, Value),
    All(
        Vec<Where>
    ),
    Any(
        Vec<Where>
    )
}

impl Where {
    pub fn columns(&self) -> HashSet<TableCol> {
        match self {
            Where::Op(col, _, _) => {
                HashSet::from([col.clone()])
            }
            Where::All(all) => {
                all.iter()
                    .flat_map(|i| i.columns())
                    .collect()
            }
            Where::Any(any) => {
                any.iter()
                   .flat_map(|i| i.columns())
                   .collect()
            }

        }
    }
}

/// Operator for where clauses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Op {
    Eq,
    Neq,
    Greater,
    Less,
    GreaterEq,
    LessEq
}