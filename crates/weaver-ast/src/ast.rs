//! Query asts

use std::collections::HashSet;
use std::str::FromStr;

use derive_more::{Display, From as FromDerive};
use serde::{Deserialize, Serialize};

use crate::error::ParseQueryError;
use crate::QueryParser;
pub use expr::*;
pub use from::*;
pub use identifier::{Identifier, UnresolvedColumnRef};
pub use literal::Literal;

mod expr;
mod identifier;
mod literal;

mod from;

/// The query type
#[derive(Debug, Clone, Serialize, Deserialize, FromDerive)]
#[serde(rename_all = "camelCase")]
pub enum Query {
    Explain(Box<Query>),
    Select(Select),
    #[serde(untagged)]
    QueryList(Vec<Query>),
}

impl Query {
    /// Parse strings
    pub fn parse(query: &str) -> Result<Query, ParseQueryError> {
        let mut query_parser = QueryParser::new();
        query_parser.parse(query)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Select {
    pub columns: Vec<ResultColumn>,
    pub from: Option<FromClause>,
    pub condition: Option<Expr>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ResultColumn {
    #[serde(rename = "*")]
    Wildcard,
    TableWildcard(Identifier),
    #[serde(untagged)]
    Expr {
        expr: Expr,
        alias: Option<Identifier>,
    },
}

/// Some type that references columns
pub trait ReferencesCols {
    fn columns(&self) -> HashSet<UnresolvedColumnRef>;
}
