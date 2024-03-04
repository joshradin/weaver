use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};

use tracing::{debug, instrument, trace};
use uuid::Uuid;

use weaver_ast::ast::{BinaryOp, Expr, Literal, UnaryOp};

use crate::data::row::Row;
use crate::data::values::DbVal;
use crate::error::WeaverError;
use crate::queries::query_plan::QueryPlan;
use crate::storage::tables::table_schema::TableSchema;

#[derive(Debug)]
pub struct ExpressionEvaluator {
    compiled_evaluators: BTreeMap<Uuid, Vec<(Expr, ())>>,
}

impl ExpressionEvaluator {
    /// Compiles an expression evaluator from a query plan
    pub fn compile(plan: &QueryPlan) -> Result<Self, WeaverError> {
        let mut evaluator = Self {
            compiled_evaluators: Default::default(),
        };
        Ok(evaluator)
    }

    /// Evaluates an expression, with an optional id. Ids can be from any source, and is optional but
    /// required for using compiled evaluators.
    ///
    pub fn evaluate<'a>(
        &self,
        expr: &Expr,
        row: &'a Row,
        schema: &TableSchema,
        id: impl Into<Option<Uuid>>,
    ) -> Result<Cow<'a, DbVal>, WeaverError> {
        if let Some(compiled) = id
            .into()
            .and_then(|id| self.compiled_evaluators.get(&id))
            .and_then(|compiled| {
                compiled.iter().find_map(
                    |(c_expr, compiled)| {
                        if c_expr == expr {
                            Some(compiled)
                        } else {
                            None
                        }
                    },
                )
            })
        {
            trace!("using compiled {compiled:?}");
        }

        runtime_eval(expr, row, schema)
    }
}

/// an evaluation that's always performed
fn runtime_eval<'a>(
    expr: &Expr,
    row: &'a Row,
    schema: &TableSchema,
) -> Result<Cow<'a, DbVal>, WeaverError> {
    let mut stack: Vec<Cow<'a, DbVal>> = vec![];
    let ops = expr.postfix();
    debug!("evaluating using schema {schema:?}");
    for op in ops {
        match op {
            Expr::Column { column } => {
                let idx = schema
                    .column_index_by_source(
                        column
                            .resolved()
                            .expect("all columns should be resolved by now"),
                    )
                    .ok_or_else(|| {
                        WeaverError::EvaluationFailed(
                            op.clone(),
                            format!("could not get index of column {column} in row"),
                        )
                    })?;
                debug!("got index {idx} for column {column}");
                let val = row[idx].clone();
                stack.push(val);
            }
            Expr::Literal { literal } => stack.push(Cow::Owned(DbVal::from(literal.clone()))),
            Expr::BindParameter { .. } => {
                panic!("bind parameter at this point seems bad")
            }
            Expr::Unary { op: unary, expr: _ } => {
                let expr = stack.pop().ok_or_else(|| {
                    WeaverError::EvaluationFailed(
                        op.clone(),
                        "missing value on stack for uniop".to_string(),
                    )
                })?;
                let next = match unary {
                    UnaryOp::Not => match expr.as_ref() {
                        DbVal::Binary(binary, i) => {
                            DbVal::Binary(binary.iter().map(|b| !*b).collect::<Vec<_>>(), *i)
                        }
                        DbVal::Integer(i) => DbVal::Integer(!i),
                        _other => panic!("can not bitwise negate {_other}"),
                    },
                    UnaryOp::Negate => match expr.as_ref() {
                        DbVal::Integer(i) => DbVal::Integer(-i),
                        DbVal::Float(f) => DbVal::Float(-f),
                        _other => panic!("can not negate {_other}"),
                    },
                };
                stack.push(Cow::Owned(next));
            }
            Expr::Binary {
                left: l,
                op: bin_op,
                right: r,
            } => {
                let l = stack.pop().ok_or_else(|| {
                    WeaverError::EvaluationFailed(
                        op.clone(),
                        "missing left value on stack for binop".to_string(),
                    )
                })?;
                let r = stack.pop().ok_or_else(|| {
                    WeaverError::EvaluationFailed(
                        op.clone(),
                        "missing right value on stack for binop".to_string(),
                    )
                })?;
                let evaluated: DbVal = match bin_op {
                    BinaryOp::Eq => (l == r).into(),
                    BinaryOp::Neq => (l != r).into(),
                    BinaryOp::Greater => (l > r).into(),
                    BinaryOp::Less => (l < r).into(),
                    BinaryOp::GreaterEq => (l >= r).into(),
                    BinaryOp::LessEq => (l <= r).into(),
                    BinaryOp::Plus => match (l.as_ref(), r.as_ref()) {
                        (DbVal::Integer(l), DbVal::Integer(r)) => (l + r).into(),
                        (DbVal::Float(l), DbVal::Float(r)) => (l + r).into(),
                        (DbVal::String(l, _), DbVal::String(r, _)) => format!("{l}{r}").into(),
                        (DbVal::Binary(l, l_len), DbVal::Binary(r, r_len)) => DbVal::Binary(
                            l.iter().chain(r.iter()).copied().collect::<Vec<u8>>(),
                            l_len.saturating_add(*r_len),
                        ),
                        _ => panic!("can not apply `+` to {l} and {r}"),
                    },
                    BinaryOp::Minus => match (l.as_ref(), r.as_ref()) {
                        (DbVal::Integer(l), DbVal::Integer(r)) => (l - r).into(),
                        (DbVal::Float(l), DbVal::Float(r)) => (l - r).into(),
                        _ => panic!("can not apply `-` to {l} and {r}"),
                    },
                    BinaryOp::Multiply => match (l.as_ref(), r.as_ref()) {
                        (DbVal::Integer(l), DbVal::Integer(r)) => (l * r).into(),
                        (DbVal::Float(l), DbVal::Float(r)) => (l + r).into(),
                        _ => panic!("can not apply `*` to {l} and {r}"),
                    },
                    BinaryOp::Divide => match (l.as_ref(), r.as_ref()) {
                        (DbVal::Integer(l), DbVal::Integer(r)) => (l / r).into(),
                        (DbVal::Float(l), DbVal::Float(r)) => (l / r).into(),
                        _ => panic!("can not apply `/` to {l} and {r}"),
                    },
                    BinaryOp::And => {
                        if let (DbVal::Boolean(left), DbVal::Boolean(right)) =
                            (l.as_ref(), r.as_ref())
                        {
                            (*left && *right).into()
                        } else {
                            panic!("can not apply `and` to {l} and {r}");
                        }
                    }
                    BinaryOp::Or => {
                        if let (DbVal::Boolean(left), DbVal::Boolean(right)) =
                            (l.as_ref(), r.as_ref())
                        {
                            (*left || *right).into()
                        } else {
                            panic!("can not apply `or` to {l} and {r}");
                        }
                    }
                };
                stack.push(Cow::Owned(evaluated));
            }
        }
    }

    return stack.pop().ok_or_else(|| {
        WeaverError::EvaluationFailed(expr.clone(), "missing value on stack".to_string())
    });
}
