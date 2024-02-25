//! Query asts

use std::collections::HashSet;
use std::fmt::Formatter;
use std::str::FromStr;

use derive_more::{Display, From as FromDerive};
use nom::combinator::cond;
use serde::{Deserialize, Serialize};

use crate::error::ParseQueryError;
use crate::QueryParser;
pub use expr::*;
pub use from::*;
pub use identifier::{Identifier, ResolvedColumnRef, UnresolvedColumnRef};
pub use literal::Literal;

mod expr;
mod identifier;
mod literal;
pub mod visitor;

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

impl Display for Query {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Query::Explain(q) => {
                write!(f, "explain {q}")
            }
            Query::Select(s) => {
                write!(f, "{s}")
            }
            Query::QueryList(q) => {
                write!(
                    f,
                    "{}",
                    q.iter()
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>()
                        .join("; ")
                )
            }
        }
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

impl Display for Select {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "select {}",
            self.columns
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )?;
        if let Some(from) = &self.from {
            write!(f, " {from}")?;
        }
        if let Some(condition) = &self.condition {
            write!(f, " {condition}")?;
        }
        if let Some(l) = &self.limit {
            write!(f, " limit {l}")?;
        }
        if let Some(l) = &self.offset {
            write!(f, " offset {l}")?;
        }
        Ok(())
    }
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

impl Display for ResultColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ResultColumn::Wildcard => {
                write!(f, "*")
            }
            ResultColumn::TableWildcard(t) => {
                write!(f, "{t}.*")
            }
            ResultColumn::Expr { expr, alias } => {
                write!(f, "{expr}")?;
                if let Some(alias) = alias.as_ref() {
                    write!(f, " as {alias}")?;
                }
                Ok(())
            }
        }
    }
}

/// Some type that references columns
pub trait ReferencesCols {
    fn columns(&self) -> HashSet<ColumnRef>;
}
