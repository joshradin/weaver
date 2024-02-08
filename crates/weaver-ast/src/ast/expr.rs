//! The expression block

use std::collections::HashSet;
use std::fmt::{Debug, Formatter};

use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::ast::literal::Binary;
use crate::ast::{Identifier, Literal, ReferencesCols};

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Expr {
    #[serde(rename_all = "camelCase")]
    Column {
        schema_name: Option<Identifier>,
        table_name: Option<Identifier>,
        column_name: Identifier,
    },
    Literal {
        literal: Literal,
    },
    BindParameter {
        parameter: Option<i64>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
}

impl Display for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Column {
                schema_name,
                table_name,
                column_name,
            } => {
                write!(
                    f,
                    "{}{}{column_name}",
                    schema_name
                        .as_ref()
                        .map(|s| format!("{s}."))
                        .unwrap_or_default(),
                    table_name
                        .as_ref()
                        .map(|s| format!("{s}."))
                        .unwrap_or_default()
                )
            }
            Expr::Literal { literal } => {
                write!(f, "{}", literal)
            }
            Expr::BindParameter { parameter } => {
                write!(
                    f,
                    "{}",
                    parameter.map(|i| i.to_string()).unwrap_or(":?".to_string())
                )
            }
            Expr::Unary { op, expr } => {
                write!(f, "{op}{expr}")
            }
            Expr::Binary { left, op, right } => {
                write!(f, "{left} {op} {right}")
            }
        }
    }
}

impl Expr {
    /// Checks if this expression is constant
    pub fn is_const(&self) -> bool {
        match self {
            Self::Literal { literal: _ } => true,
            Self::Unary { op: _, expr: i } => i.is_const(),
            Self::Binary {
                left: l,
                op: _,
                right: r,
            } => l.is_const() && r.is_const(),
            _ => false,
        }
    }

    /// Get this value as a literal, if possible
    pub fn literal(&self) -> Option<&Literal> {
        if let Self::Literal { literal: lit } = self {
            Some(lit)
        } else {
            None
        }
    }

    /// Reduces this expression, does nothing if not constant
    pub fn reduce(&mut self) {
        if !self.is_const() {
            return;
        }

        match self {
            Expr::Unary { op: op, expr: expr } => {
                expr.reduce();
                let expr = expr.literal().expect("is literal");
                match op {
                    UnaryOp::Not => match expr {
                        Literal::Binary(binary) => {
                            *self = Expr::Literal {
                                literal: Literal::from(Binary::from(
                                    binary.as_ref().iter().map(|b| !*b).collect::<Vec<_>>(),
                                )),
                            }
                        }
                        Literal::Integer(i) => {
                            *self = Expr::Literal {
                                literal: Literal::from(!*i),
                            }
                        }
                        _other => panic!("can not bitwise negate {_other}"),
                    },
                    UnaryOp::Negate => match expr {
                        Literal::Integer(i) => {
                            *self = Expr::Literal {
                                literal: Literal::from(-*i),
                            }
                        }
                        Literal::Float(f) => {
                            *self = Expr::Literal {
                                literal: Literal::from(-*f),
                            }
                        }
                        _other => panic!("can not negate {_other}"),
                    },
                }
            }
            Expr::Binary {
                left: l,
                op: op,
                right: r,
            } => {
                l.reduce();
                r.reduce();
                let l = l.literal().expect("is literal");
                let r = r.literal().expect("is literal");
                let lit: Literal = match op {
                    BinaryOp::Eq => (l == r).into(),
                    BinaryOp::Neq => (l != r).into(),
                    BinaryOp::Greater => (l > r).into(),
                    BinaryOp::Less => (l < r).into(),
                    BinaryOp::GreaterEq => (l >= r).into(),
                    BinaryOp::LessEq => (l <= r).into(),
                    BinaryOp::Plus => match (l, r) {
                        (Literal::Integer(l), Literal::Integer(r)) => (l + r).into(),
                        (Literal::Float(l), Literal::Float(r)) => (l + r).into(),
                        (Literal::String(l), Literal::String(r)) => format!("{l}{r}").into(),
                        (Literal::Binary(l), Literal::Binary(r)) => {
                            Binary::from(l.iter().chain(r.iter()).copied().collect::<Vec<u8>>())
                                .into()
                        }
                        _ => panic!("can not apply `+` to {l} and {r}"),
                    },
                    BinaryOp::Minus => match (l, r) {
                        (Literal::Integer(l), Literal::Integer(r)) => (l - r).into(),
                        (Literal::Float(l), Literal::Float(r)) => (l - r).into(),
                        _ => panic!("can not apply `-` to {l} and {r}"),
                    },
                    BinaryOp::Multiply => match (l, r) {
                        (Literal::Integer(l), Literal::Integer(r)) => (l * r).into(),
                        (Literal::Float(l), Literal::Float(r)) => (l + r).into(),
                        _ => panic!("can not apply `*` to {l} and {r}"),
                    },
                    BinaryOp::Divide => match (l, r) {
                        (Literal::Integer(l), Literal::Integer(r)) => (l / r).into(),
                        (Literal::Float(l), Literal::Float(r)) => (l / r).into(),
                        _ => panic!("can not apply `/` to {l} and {r}"),
                    },
                    BinaryOp::And => {
                        if let (Literal::Boolean(left), Literal::Boolean(right)) = (l, r) {
                            (*left && *right).into()
                        } else {
                            panic!("can not apply `and` to {l} and {r}");
                        }
                    }
                    BinaryOp::Or => {
                        if let (Literal::Boolean(left), Literal::Boolean(right)) = (l, r) {
                            (*left || *right).into()
                        } else {
                            panic!("can not apply `or` to {l} and {r}");
                        }
                    }
                };
                *self = Expr::Literal { literal: lit };
            }
            _ => {}
        }
    }
}

impl ReferencesCols for Expr {
    fn columns(&self) -> HashSet<(Option<String>, Option<String>, String)> {
        match self {
            Expr::Column {
                schema_name,
                table_name,
                column_name,
            } => HashSet::from([(
                schema_name.as_ref().map(ToString::to_string),
                table_name.as_ref().map(ToString::to_string),
                column_name.to_string(),
            )]),
            Expr::Unary { op: _, expr: expr } => expr.columns(),
            Expr::Binary {
                left: l,
                op: _,
                right: r,
            } => l.columns().into_iter().chain(r.columns()).collect(),
            _ => HashSet::new(),
        }
    }
}

/// Operator for where clauses
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Display)]
#[serde(rename_all = "camelCase")]
pub enum BinaryOp {
    #[display("=")]
    Eq,
    #[display("!=")]
    Neq,
    #[display(">")]
    Greater,
    #[display("<")]
    Less,
    #[display(">=")]
    GreaterEq,
    #[display("<=")]
    LessEq,
    #[display("+")]
    Plus,
    #[display("-")]
    Minus,
    #[display("*")]
    Multiply,
    #[display("/")]
    Divide,
    #[display("and")]
    And,
    #[display("or")]
    Or,
}

/// Operator for where clauses
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Display)]
#[serde(rename_all = "camelCase")]
pub enum UnaryOp {
    #[display("!")]
    Not,
    #[display("-")]
    Negate,
}
