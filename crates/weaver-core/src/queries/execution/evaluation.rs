use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::sync::OnceLock;

use itertools::Itertools;
use once_cell::sync::Lazy;
use rayon::Scope;
use tracing::{debug, trace};
use uuid::Uuid;

use builtins::BUILTIN_FUNCTIONS_REGISTRY;
use weaver_ast::ast::{BinaryOp, ColumnRef, Expr, FunctionArgs, Identifier, UnaryOp};

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

        runtime_eval_single_row(expr, row, schema, &self.functions)
    }

    /// Evaluates an expression, with an optional id. Ids can be from any source, and is optional but
    /// required for using compiled evaluators.
    pub fn evaluate_many_rows<'a, I: IntoIterator<Item = &'a Row<'a>>>(
        &self,
        expr: &Expr,
        rows: I,
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

        let rows = rows.into_iter().collect::<Vec<_>>();

        runtime_eval_many_rows(expr, &rows[..], schema, &self.functions)
    }
}

fn runtime_eval_many_rows<'a>(
    expr: &Expr,
    rows: &[&Row<'a>],
    scope: &TableSchema,
    function_registry: &FunctionRegistry,
) -> Result<Cow<'a, DbVal>, WeaverError> {
    match expr {
        Expr::Column { column } => {
            get_from_column(rows[0], scope, expr, column)
        }
        Expr::Literal { literal } => Ok(Cow::Owned(DbVal::from(literal.clone()))),
        Expr::BindParameter { .. } => {
            panic!("bind parameter at this point is probably bad")
        }
        Expr::Unary { op, expr } => {
            let child = runtime_eval_many_rows(expr, rows, scope, function_registry)?;
            Ok(Cow::Owned(evaluate_unary(op, child)))
        }
        Expr::Binary { left, op, right } => {
            let left = runtime_eval_many_rows(left, rows, scope, function_registry)?;
            let right = runtime_eval_many_rows(right, rows, scope, function_registry)?;
            Ok(Cow::Owned(evaluate_binary(op, left, right)))
        }
        Expr::FunctionCall {
            function: function_name,
            args,
        } => {
            let FunctionKind { aggregate: Some(function), .. } = find_function(function_registry, function_name, args, scope)? else {
                return Err(WeaverError::UnknownFunction(function_name.to_string(), arg_types(function_registry, args, true, scope)))
            };

            let args = match args {
                FunctionArgs::Params {
                    distinct,
                    exprs,
                    ordered_by,
                } => {
                    let mut args = exprs
                        .iter()
                        .map(|expr| -> Result<_, WeaverError> {
                            let mut evals = vec![];
                            for &row in rows {
                                let evaluated =
                                    runtime_eval_single_row(expr, row, scope, function_registry)?;
                                evals.push(evaluated);
                            }
                            Ok(evals)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    if *distinct {
                        args = args.into_iter().unique().collect();
                    }
                    if let Some(ordered_by) = ordered_by {
                        todo!("ordered by {:?}", ordered_by)
                    }

                    args.into_iter().map(|v| ArgValue::Many(v)).collect()
                }
                FunctionArgs::Wildcard { distinct } => {
                    let mut rows = rows.to_vec();
                    if *distinct {
                        rows = rows.into_iter().unique().collect();
                    }
                    vec![ArgValue::Rows(rows.to_vec())]
                }
            };

            let result = function.execute(args)?;
            Ok(Cow::Owned(result))
        }
    }
}

/// an evaluation that's always performed
fn runtime_eval_single_row<'a>(
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
                let val = get_from_column(row, schema, op, column)?;
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
                let next = evaluate_unary(unary, expr);
                stack.push(Cow::Owned(next));
            }
            Expr::Binary {
                left: l,
                op: bin_op,
                right: r,
            } => {
                let r = stack.pop().ok_or_else(|| {
                    WeaverError::EvaluationFailed(
                        op.clone(),
                        "missing right value on stack for binop".to_string(),
                    )
                })?;
                let l = stack.pop().ok_or_else(|| {
                    WeaverError::EvaluationFailed(
                        op.clone(),
                        "missing left value on stack for binop".to_string(),
                    )
                })?;

                let evaluated: DbVal = evaluate_binary(bin_op, l, r);
                stack.push(Cow::Owned(evaluated));
            }
            Expr::FunctionCall {
                function: function_name,
                args,
            } => {
                let FunctionKind { normal: Some(function), .. } = find_function(functions, function_name, args, schema)? else {
                    return Err(WeaverError::UnknownFunction(function_name.to_string(), arg_types(functions, args, false, schema)))
                };

                let args = match args {
                    FunctionArgs::Params { exprs, .. } => {
                        let mut args = vec![];
                        for _ in exprs {
                            let cow = stack.pop().expect("arg not on stack");
                            args.push(ArgValue::One(cow));
                        }

                        args
                    }
                    FunctionArgs::Wildcard { .. } => {
                        return Err(WeaverError::AggregateInSingleRowContext(
                            function_name.to_string(),
                        ))
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

#[derive(Debug)]
pub struct FunctionKind<'a> {
    pub normal: Option<&'a DbFunction>,
    pub aggregate: Option<&'a DbFunction>
}

pub fn find_function<'a, 't>(
    functions: &'a FunctionRegistry,
    function_name: &Identifier,
    args: &FunctionArgs,
    schema: impl Into<Option<&'t TableSchema>>,
) -> Result<FunctionKind<'a>, WeaverError> {
    let schema= schema.into();
    let normal = {
        let arg_types = arg_types(functions, args, false, schema);

        functions.get(function_name, &arg_types).ok_or_else(|| {
            WeaverError::UnknownFunction(function_name.as_ref().to_string(), arg_types)
        }).ok()
    };

    let aggregate = {
        let arg_types = arg_types(functions, args, true, schema);
        functions.get(function_name, &arg_types).ok_or_else(|| {
            WeaverError::UnknownFunction(function_name.as_ref().to_string(), arg_types)
        }).ok()
    };

    if aggregate.as_ref().or(normal.as_ref()).is_none() {
        let arg_types = arg_types(functions, args, false, schema);
        return Err(WeaverError::UnknownFunction(function_name.as_ref().to_string(), arg_types))
    }

    trace!("normal: {normal:?}, aggregate: {aggregate:?}");

    Ok(FunctionKind {
        normal,
        aggregate,
    })
}

fn arg_types<'t>(functions: &FunctionRegistry, args: &FunctionArgs, is_agg: bool, schema: impl Into<Option<&'t TableSchema>>) -> Vec<ArgType> {
    let schema = schema.into();
    let arg_types = match args {
        FunctionArgs::Params { exprs, .. } => exprs
            .iter()
            .map(|i| i.type_of(functions, schema))
            .flat_map(|i| i.ok())
            .map(if is_agg { ArgType::Many } else { ArgType::One })
            .collect(),
        FunctionArgs::Wildcard { .. } => {
            vec![ArgType::Rows]
        }
    };
    arg_types
}

fn evaluate_binary(bin_op: &BinaryOp, l: Cow<DbVal>, r: Cow<DbVal>) -> DbVal {
    match bin_op {
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
            if let (DbVal::Boolean(left), DbVal::Boolean(right)) = (l.as_ref(), r.as_ref()) {
                (*left && *right).into()
            } else {
                panic!("can not apply `and` to {l} and {r}");
            }
        }
        BinaryOp::Or => {
            if let (DbVal::Boolean(left), DbVal::Boolean(right)) = (l.as_ref(), r.as_ref()) {
                (*left || *right).into()
            } else {
                panic!("can not apply `or` to {l} and {r}");
            }
        }
    }
}

fn evaluate_unary(unary: &UnaryOp, expr: Cow<DbVal>) -> DbVal {
    match unary {
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
    }
}

fn get_from_column<'a>(
    row: &Row<'a>,
    schema: &TableSchema,
    op: &Expr,
    column: &ColumnRef,
) -> Result<Cow<'a, DbVal>, WeaverError> {
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
    Ok(val)
}

#[cfg(test)]
mod tests {
    use weaver_ast::ast::{BinaryOp, Expr, FunctionArgs, Identifier, Literal, ResolvedColumnRef};

    use crate::data::row::Row;
    use crate::data::types::Type;
    use crate::error::WeaverError;
    use crate::queries::execution::evaluation::{runtime_eval_many_rows, runtime_eval_single_row};
    use crate::queries::execution::evaluation::builtins::BUILTIN_FUNCTIONS_REGISTRY;
    use crate::storage::tables::table_schema::{TableSchema, TableSchemaBuilder};

    #[test]
    fn basic_evaluation() {
        let stored = &Row::new(0);
        let result = runtime_eval_single_row(
            &Expr::Binary {
                left: Box::new(Expr::Literal {
                    literal: Literal::from(21),
                }),
                op: BinaryOp::Multiply,
                right: Box::new(Expr::Literal {
                    literal: Literal::from(3),
                }),
            },
            stored,
            &TableSchema::empty(),
            &BUILTIN_FUNCTIONS_REGISTRY,
        )
        .expect("could not build");
        assert_eq!(result.int_value(), Some(63));
    }

    #[test]
    fn single_arg_function() {
        let stored = &Row::new(0);
        let result = runtime_eval_single_row(
            &Expr::FunctionCall {
                function: Identifier::new("pow"),
                args: FunctionArgs::Params {
                    distinct: false,
                    exprs: vec![Expr::from(2), Expr::from(8)],
                    ordered_by: None,
                },
            },
            stored,
            &TableSchema::empty(),
            &BUILTIN_FUNCTIONS_REGISTRY,
        )
        .expect("could not build");
        assert_eq!(result.int_value(), Some(1 << 8));
    }

    #[test]
    fn single_arg_function_overload() {
        let stored = &Row::new(0);
        let result = runtime_eval_single_row(
            &Expr::FunctionCall {
                function: Identifier::new("pow"),
                args: FunctionArgs::Params {
                    distinct: false,
                    exprs: vec![Expr::from(2.), Expr::from(8.)],
                    ordered_by: None,
                },
            },
            stored,
            &TableSchema::empty(),
            &BUILTIN_FUNCTIONS_REGISTRY,
        )
        .expect("could not build");
        assert!((result.float_value().unwrap() - 256.).abs() < 0.0025);
    }

    #[test]
    fn single_arg_function_wrong_types() {
        let stored = &Row::new(0);
        let result = runtime_eval_single_row(
            &Expr::FunctionCall {
                function: Identifier::new("pow"),
                args: FunctionArgs::Params {
                    distinct: false,
                    exprs: vec![Expr::from(2), Expr::from(8.)],
                    ordered_by: None,
                },
            },
            stored,
            &TableSchema::empty(),
            &BUILTIN_FUNCTIONS_REGISTRY,
        )
        .expect_err("no (int, float) power function");
        assert!(matches!(result, WeaverError::UnknownFunction(_, _)));
    }

    #[test]
    fn many_rows_aggregate() {
        let rows = &[
            &Row::from([1_i64]),
            &Row::from([2_i64]),
            &Row::from([3_i64]),
            &Row::from([4_i64]),
            &Row::from([5_i64]),
        ];
        let result = runtime_eval_many_rows(
            &Expr::FunctionCall {
                function: Identifier::new("min"),
                args: FunctionArgs::Params {
                    distinct: false,
                    exprs: vec![Expr::Column {
                        column: ResolvedColumnRef::new("s", "t", "col").into(),
                    }],
                    ordered_by: None,
                },
            },
            rows,
            &TableSchemaBuilder::new("s", "t")
                .column("col", Type::Integer, true, None, None)
                .unwrap()
                .build()
                .unwrap(),
            &BUILTIN_FUNCTIONS_REGISTRY,
        )
        .expect("couldn't get minimum value");
        assert_eq!(result.int_value(), Some(1), "minimum value should be 1");
    }

    #[test]
    fn count() {
        let rows = &[
            &Row::from([1_i64]),
            &Row::from([2_i64]),
            &Row::from([3_i64]),
            &Row::from([4_i64]),
            &Row::from([5_i64]),
        ];
        let result = runtime_eval_many_rows(
            &Expr::FunctionCall {
                function: Identifier::new("count"),
                args: FunctionArgs::Wildcard { distinct: false },
            },
            rows,
            &TableSchema::empty(),
            &BUILTIN_FUNCTIONS_REGISTRY,
        )
        .expect("couldn't get minimum value");
        assert_eq!(result.int_value(), Some(5), "count should be 5");
    }

    #[test]
    fn distinct_count() {
        let rows = &[
            &Row::from([1_i64]),
            &Row::from([2_i64]),
            &Row::from([1_i64]),
            &Row::from([2_i64]),
            &Row::from([3_i64]),
        ];
        let result = runtime_eval_many_rows(
            &Expr::FunctionCall {
                function: Identifier::new("count"),
                args: FunctionArgs::Wildcard { distinct: true },
            },
            rows,
            &TableSchema::empty(),
            &BUILTIN_FUNCTIONS_REGISTRY,
        )
        .expect("couldn't get minimum value");
        assert_eq!(result.int_value(), Some(3), "distinct count should be 3");
    }
}
