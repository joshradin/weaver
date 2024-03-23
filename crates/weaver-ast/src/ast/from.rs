use std::fmt::Formatter;

use derive_more::{AsRef, Deref, Display};
use serde::{Deserialize, Serialize};

use crate::ast::{Expr, Identifier};
use crate::ast::select::Select;

/// The from clause
#[derive(Debug, Clone, Serialize, Deserialize, Deref, AsRef, Display)]
#[display("from {_0}")]
pub struct FromClause(pub TableOrSubQuery);

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

impl Display for TableOrSubQuery {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TableOrSubQuery::Table {
                schema,
                table_name,
                alias,
            } => {
                if let Some(schema) = schema {
                    write!(f, "{schema}.")?;
                }
                write!(f, "{table_name}")?;
                if let Some(alias) = alias {
                    write!(f, " as {alias}")?;
                }
            }
            TableOrSubQuery::Select { select, alias } => {
                write!(f, "({select})")?;
                if let Some(alias) = alias {
                    write!(f, " as {alias}")?;
                }
            }
            TableOrSubQuery::Multiple(m) => {
                write!(
                    f,
                    "{}",
                    m.iter()
                        .map(|t| t.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )?;
            }
            TableOrSubQuery::JoinClause(join) => {
                write!(f, "{join}")?;
            }
        }
        Ok(())
    }
}

/// The join clause is all joins
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinClause {
    pub left: Box<TableOrSubQuery>,
    pub op: JoinOperator,
    pub right: Box<TableOrSubQuery>,
    pub constraint: JoinConstraint,
}

impl Display for JoinClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} {} {}",
            self.left, self.op, self.right, self.constraint
        )
    }
}

/// Join Constraint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinConstraint {
    pub on: Expr,
}

impl Display for JoinConstraint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "on {}", self.on)
    }
}

/// The join operator
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum JoinOperator {
    Left,
    Right,
    Full,
    Inner,
    Cross,
    Outer,
}

impl Display for JoinOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            JoinOperator::Left => "left join",
            JoinOperator::Right => "right join",
            JoinOperator::Full => "full join",
            JoinOperator::Inner => "inner join",
            JoinOperator::Cross => "cross join",
            JoinOperator::Outer => "outer join",
        };
        write!(f, "{s}")
    }
}
