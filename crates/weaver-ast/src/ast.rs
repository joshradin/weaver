//! Query asts


use derive_more::{Display, From};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::str::FromStr;
pub use literal::Literal;
pub use identifier::Identifier;
use crate::error::QueryParseError;
use crate::QueryParser;

mod literal;
mod identifier;


/// A value, can either be a [Literal] or an [Identifier]
#[derive(Debug, PartialOrd, PartialEq, Clone, Serialize, Deserialize, From, Display)]
#[serde(untagged)]
pub enum Value {
    Literal(Literal),
    Identifier(Identifier)
}

/// The query type
#[derive(Debug, Clone, Serialize, Deserialize, From)]
pub enum Query {
    Select(Select),
}

impl FromStr for Query {
    type Err = QueryParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parser = QueryParser::new();
        parser.parse(s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Select {
    pub columns: Vec<String>,
    pub table_ref: String,
    pub condition: Option<Where>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

impl Query {
    pub fn select(columns: &[&str], table: &str, where_: impl Into<Option<Where>>) -> Self {
        Self::Select(Select {
            columns: columns.iter().map(|s| s.to_string()).collect(),
            table_ref: table.to_string(),
            condition: where_.into(),
            limit: None,
            offset: None,
        })
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
