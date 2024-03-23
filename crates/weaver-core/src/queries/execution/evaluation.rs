use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::sync::OnceLock;

use once_cell::sync::Lazy;
use tracing::trace;
use uuid::Uuid;

use builtins::BUILTIN_FUNCTIONS_REGISTRY;
use weaver_ast::ast::{BinaryOp, Expr, FunctionArgs, Identifier, UnaryOp};

use crate::data::row::Row;
use crate::data::types::{DbTypeOf, Type};
use crate::data::values::DbVal;
use crate::error::WeaverError;
use crate::queries::execution::evaluation::functions::{
    ArgType, ArgValue, DbFunction, FunctionRegistry,
};
use crate::queries::query_plan::QueryPlan;
use crate::storage::tables::table_schema::TableSchema;

pub mod builtins;
pub mod functions;

#[derive(Debug)]
pub struct ExpressionEvaluator {
    compiled_evaluators: BTreeMap<Uuid, Vec<(Expr, ())>>,
    functions: FunctionRegistry,
}

impl ExpressionEvaluator {

    pub fn new<T: Into<Option<FunctionRegistry>>>(functions: T) -> Self {
        let registry = match functions.into() {
            None => BUILTIN_FUNCTIONS_REGISTRY.clone(),
            Some(mut registry) => {
                registry.extend(BUILTIN_FUNCTIONS_REGISTRY.clone());
                registry
            }
        };
        let evaluator = Self {
            compiled_evaluators: Default::default(),
            functions: registry,
        };
        evaluator
    }

    /// Compiles an expression evaluator from a query plan
    pub fn compile<T: Into<Option<FunctionRegistry>>>(
        plan: &QueryPlan,
        functions: T,
    ) -> Result<Self, WeaverError> {
        let mut evaluator = Self::new(functions);
        Ok(evaluator)
    }

    /// Evaluates an expression, with an optional id. Ids can be from any source, and is optional but
    /// required for using compiled evaluators.
    pub fn evaluate_one_row<'a>(
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

        runtime_eval(expr, row, schema, &self.functions)
    }
}

/// an evaluation that's always performed
fn runtime_eval<'a>(
    expr: &Expr,
    row: &'a Row,
    schema: &TableSchema,
    functions: &FunctionRegistry,
) -> Result<Cow<'a, DbVal>, WeaverError> {
    let mut stack: Vec<Cow<'a, DbVal>> = vec![];
    let ops = expr.postfix();
    trace!("evaluating using schema {schema:?}");
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
                trace!("got index {idx} for column {column}");
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
            Expr::FunctionCall { function, args } => {
                let arg_types = match args {
                    FunctionArgs::Params { exprs, .. } => exprs
                        .iter()
                        .map(|i| i.type_of(functions, Some(schema)))
                        .flat_map(|i| i.ok())
                        .map(ArgType::One)
                        .collect(),
                    FunctionArgs::Wildcard => {
                        vec![ArgType::Rows]
                    }
                };

                let function = functions
                    .get(function, &arg_types)
                    .ok_or_else(|| WeaverError::UnknownFunction(function.as_ref().to_string(), arg_types))?;

                let args = match args {
                    FunctionArgs::Params { exprs, .. } => {
                        let mut args = vec![];
                        for _ in exprs {
                            let cow = stack.pop().expect("arg not on stack");
                            args.push(ArgValue::One(cow));
                        }

                        args
                    }
                    FunctionArgs::Wildcard => {
                        todo!("wildcard")
                    }
                };

                let result = function.execute(args)?;
                stack.push(Cow::Owned(result));
            }
            _expr => {
                todo!("evaluate {_expr}")
            }
        }
    }

    stack.pop().ok_or_else(|| {
        WeaverError::EvaluationFailed(expr.clone(), "missing value on stack".to_string())
    })
}

#[cfg(test)]
mod tests {
    use weaver_ast::ast::{BinaryOp, Expr, FunctionArgs, Identifier, Literal};

    use crate::data::row::Row;
    use crate::error::WeaverError;
    use crate::queries::execution::evaluation::builtins::BUILTIN_FUNCTIONS_REGISTRY;
    use crate::queries::execution::evaluation::runtime_eval;
    use crate::storage::tables::table_schema::TableSchema;

    #[test]
    fn basic_evaluation() {
        let stored = &Row::new(0);
        let result = runtime_eval(
            &Expr::Binary {
                left: Box::new(Expr::Literal { literal: Literal::from(21) }),
                op: BinaryOp::Multiply,
                right: Box::new(Expr::Literal { literal: Literal::from(3) }),
            },
            stored,
            &TableSchema::empty(),
            &BUILTIN_FUNCTIONS_REGISTRY,
        ).expect("could not build");
        assert_eq!(result.int_value(), Some(63));
    }

    #[test]
    fn single_arg_function() {
        let stored = &Row::new(0);
        let result = runtime_eval(
            &Expr::FunctionCall {
                function: Identifier::new("pow"),
                args: FunctionArgs::Params {
                    distinct: false,
                    exprs: vec![
                        Expr::from(2),
                        Expr::from(8),
                    ],
                    ordered_by: None,
                },
            },
            stored,
            &TableSchema::empty(),
            &BUILTIN_FUNCTIONS_REGISTRY,
        ).expect("could not build");
        assert_eq!(result.int_value(), Some(1<<8));
    }

    #[test]
    fn single_arg_function_overload() {
        let stored = &Row::new(0);
        let result = runtime_eval(
            &Expr::FunctionCall {
                function: Identifier::new("pow"),
                args: FunctionArgs::Params {
                    distinct: false,
                    exprs: vec![
                        Expr::from(2.),
                        Expr::from(8.),
                    ],
                    ordered_by: None,
                },
            },
            stored,
            &TableSchema::empty(),
            &BUILTIN_FUNCTIONS_REGISTRY,
        ).expect("could not build");
        assert!((result.float_value().unwrap() - 256.).abs() < 0.0025);
    }

    #[test]
    fn single_arg_function_wrong_types() {
        let stored = &Row::new(0);
        let result = runtime_eval(
            &Expr::FunctionCall {
                function: Identifier::new("pow"),
                args: FunctionArgs::Params {
                    distinct: false,
                    exprs: vec![
                        Expr::from(2),
                        Expr::from(8.),
                    ],
                    ordered_by: None,
                },
            },
            stored,
            &TableSchema::empty(),
            &BUILTIN_FUNCTIONS_REGISTRY,
        ).expect_err("no (int, float) power function");
        assert!(matches!(result, WeaverError::UnknownFunction(_, _)));
    }
}
