//! The expression block

use std::collections::HashSet;
use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::ast::{Identifier, Literal, BinaryOp, ReferencesCols};

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all="camelCase")]
pub enum Expr {
    #[serde(rename_all="camelCase")]
    Column {
        schema_name: Option<Identifier>,
        table_name: Option<Identifier>,
        column_name: Identifier,
    },
    Literal(Literal),
    BindParameter(Option<i64>),
    #[serde(untagged)]
    Unary(
        BinaryOp, Box<Expr>,
    ),
    #[serde(untagged)]
    Binary(Box<Expr>, BinaryOp, Box<Expr>),
}


impl ReferencesCols for Expr {
    fn columns(&self) -> HashSet<String> {
        match self {
            Expr::Column { schema_name, table_name, column_name, } => {
                let name = format!("{}{}{column_name}",
                                   schema_name.as_ref().map(|s| format!("{s}.")).unwrap_or_default(),
                                   table_name.as_ref().map(|s| format!("{s}.")).unwrap_or_default(),
                );
                HashSet::from([name])
            }
            Expr::Unary(_, expr) => expr.columns(),
            Expr::Binary(l, _, r) => l.columns()
                                      .into_iter()
                                      .chain(r.columns())
                                      .collect(),
            _ => {
                HashSet::new()
            }
        }
    }
}