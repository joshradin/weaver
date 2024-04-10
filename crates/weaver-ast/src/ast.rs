//! Query asts

use std::collections::HashSet;
use std::fmt::Formatter;
use std::str::FromStr;

use derive_more::{Display, From as FromDerive};
use serde::{Deserialize, Serialize};

pub use create::*;
pub use data_type::*;
pub use expr::*;
pub use from::*;
pub use identifier::{Identifier, ResolvedColumnRef, UnresolvedColumnRef};

pub use literal::Literal;
pub use load::*;
pub use select::*;

use crate::error::ParseQueryError;
use crate::QueryParser;

mod create;
mod data_type;
mod expr;
mod from;
mod identifier;
mod insert;
mod literal;
mod load;
mod select;
pub mod visitor;

/// The query type
#[derive(Debug, Clone, Serialize, Deserialize, FromDerive)]
#[serde(rename_all = "camelCase")]
pub enum Query {
    Explain(Box<Query>),
    Select(Select),
    Create(Create),
    LoadData(LoadData),
    KillProcess(i64),
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

impl FromStr for Query {
    type Err = ParseQueryError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
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
            Query::Create(create) => {
                write!(f, "{create}")
            }
            Query::LoadData(load) => {
                write!(f, "{load}")
            }
            Query::KillProcess(pid) => {
                write!(f, "kill {pid}")
            }
        }
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
