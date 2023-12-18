//! Query asts

use crate::data::values::Value;
use crate::dynamic_table::{Col, TableCol};
use crate::tables::TableRef;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use crate::db::server::layers::packets::{DbReq, DbReqBody, Headers};
use crate::tx::Tx;

/// The query type
#[derive(Debug, Clone, Serialize, Deserialize, From)]
pub enum Query {
    Select {
        columns: Vec<String>,
        table_ref: String,
        condition: Option<Where>,
        limit: Option<u64>,
        offset: Option<u64>,
    },
}

impl Query {
    pub fn select(columns: &[&str], table: &str, where_: impl Into<Option<Where>>) -> Self  {
        Self::Select {
            columns: columns.iter().map(|s| s.to_string()).collect(),
            table_ref: table.to_string(),
            condition: where_.into(),
            limit: None,
            offset: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Where {
    Op(String, Op, Value),
    All(Vec<Where>),
    Any(Vec<Where>),
}

impl Where {
    pub fn columns(&self) -> HashSet<String> {
        match self {
            Where::Op(col, _, _) => HashSet::from([col.clone()]),
            Where::All(all) => all.iter().flat_map(|i| i.columns()).collect(),
            Where::Any(any) => any.iter().flat_map(|i| i.columns()).collect(),
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
    LessEq,
}


impl Into<DbReq> for (Tx, Query) {
    fn into(self) -> DbReq {
        let (tx, query) = self;
        DbReq::new(Headers::default(), DbReqBody::TxQuery(tx, query))
    }
}

pub fn visit_query<V : QueryVisitor>(visitor: &mut V, query: &Query) {

}

pub fn visit_select<V : QueryVisitor>(visitor: &mut V, query: &Query) {

}

pub trait QueryVisitor {
    fn visit_query(&mut self, query: &Query) {
        visit_query(self, query)
    }
}

pub trait QueryVisitorMut {

}