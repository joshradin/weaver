//! The expression block

use std::collections::HashSet;
use std::fmt::{Debug, Formatter};

use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use crate::ast::identifier::{ResolvedColumnRef, UnresolvedColumnRef};
use crate::ast::literal::Binary;
use crate::ast::{Identifier, Literal, ReferencesCols};

#[derive(Debug, Eq, PartialEq, Hash, Clone, Serialize, Deserialize, Display, From)]
pub enum ColumnRef {
    Unresolved(UnresolvedColumnRef),
    Resolved(ResolvedColumnRef),
}

impl ColumnRef {
    pub fn resolved(&self) -> Option<&ResolvedColumnRef> {
        if let Self::Resolved(resolved) = self {
            Some(resolved)
        } else {
            None
        }
    }

    pub fn unresolved(&self) -> Option<&UnresolvedColumnRef> {
        if let Self::Unresolved(unresolved) = self {
            Some(unresolved)
        } else {
            None
        }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Hash)]
pub enum Expr {
    Column {
        column: ColumnRef,
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
    FunctionCall {
        function: Identifier,
        args: FunctionArgs,
    },
}

impl Display for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Column { column } => {
                write!(f, "{}", column)
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
            Expr::FunctionCall { function, args } => {
                write!(f, "{}({})", function, args)
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

    /// Get this expression this in a series of post fix expressions
    pub fn postfix(&self) -> Vec<&Expr> {
        let mut ret = vec![];
        match self {
            Expr::Unary { expr, .. } => {
                ret.extend(expr.postfix());
            }
            Expr::Binary { left, right, .. } => {
                ret.extend(left.postfix());
                ret.extend(right.postfix());
            }
            Expr::FunctionCall { args: FunctionArgs::Params { exprs, ..}, .. } => {
                for arg in exprs.iter().rev() {
                    ret.extend(arg.postfix());
                }
            }
            _ => {}
        }
        ret.push(self);
        ret
    }
}

impl ReferencesCols for Expr {
    fn columns(&self) -> HashSet<ColumnRef> {
        match self {
            Expr::Column { column } => HashSet::from([column.clone()]),
            Expr::Unary { op: _, expr } => expr.columns(),
            Expr::Binary {
                left: l,
                op: _,
                right: r,
            } => l.columns().into_iter().chain(r.columns()).collect(),
            Expr::FunctionCall {
                function: _, args: FunctionArgs::Params { distinct: _, exprs, ordered_by }
            } => {
                HashSet::from_iter(
                    exprs.iter()
                        .chain(ordered_by.iter().flatten())
                        .map(|expr| expr.columns())
                        .flatten()
                )
            }
            _ => HashSet::new(),
        }
    }
}

impl<I : Into<Literal>> From<I> for Expr {
    fn from(value: I) -> Self {
        let literal = value.into();
        Expr::Literal { literal }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Hash)]
pub enum FunctionArgs {
    Params {
        distinct: bool,
        exprs: Vec<Expr>,
        ordered_by: Option<Vec<Expr>>,
    },
    Wildcard,
}

impl Display for FunctionArgs {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionArgs::Params {
                distinct,
                exprs,
                ordered_by,
            } => {
                write!(
                    f,
                    "{distinct}{exprs}{ordered_by}",
                    distinct = if *distinct { "distinct " } else { "" },
                    exprs = exprs
                        .iter()
                        .map(|i| i.to_string())
                        .collect::<Vec<_>>()
                        .join(", "),
                    ordered_by = if let Some(ordered_by) = ordered_by {
                        format!(
                            " ordered by {}",
                            ordered_by
                                .iter()
                                .map(|i| i.to_string())
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    } else {
                        "".to_string()
                    }
                )
            }
            FunctionArgs::Wildcard => {
                write!(f, "*")
            }
        }
    }
}

/// Operator for where clauses
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Display, Hash)]
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
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Display, Hash)]
#[serde(rename_all = "camelCase")]
pub enum UnaryOp {
    #[display("!")]
    Not,
    #[display("-")]
    Negate,
}
