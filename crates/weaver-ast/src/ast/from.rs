use crate::ast::{Expr, Identifier, Select};
use serde::{Deserialize, Serialize};

/// The from clause
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct From(pub TableOrSubQuery);

/// A table or a subquery
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TableOrSubQuery {
    #[serde(rename_all = "camelCase")]
    Table {
        schema: Option<Identifier>,
        table_name: Identifier,
        alias: Option<Identifier>,
    },
    #[serde(rename_all = "camelCase")]
    Select {
        select: Box<Select>,
        alias: Option<Identifier>,
    },
    Multiple(Vec<TableOrSubQuery>),
    JoinClause(JoinClause),
}

/// The join clause is all joins
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinClause {
    pub left: Box<TableOrSubQuery>,
    pub op: JoinOperator,
    pub right: Box<TableOrSubQuery>,
    pub constraint: JoinConstraint,
}

/// Join Constraint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinConstraint {
    pub on: Expr,
}

/// The join operator
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum JoinOperator {
    Left,
    Right,
    Full,
    Inner,
    Cross,
    Outer,
}
