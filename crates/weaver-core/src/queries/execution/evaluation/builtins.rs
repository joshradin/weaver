use std::cmp::Ordering;

use once_cell::sync::Lazy;

use crate::data::types::Type;
use crate::data::values::DbVal;
use crate::queries::execution::evaluation::functions::{
    ArgType, ArgValue, DbFunction, FunctionRegistry,
};

pub static BUILTIN_FUNCTIONS_REGISTRY: Lazy<FunctionRegistry> = Lazy::new(|| {
    FunctionRegistry::from_iter([
        (
            "count",
            DbFunction::builtin(vec![ArgType::Rows], Type::Integer, |args| {
                match &args[0] {
                    ArgValue::Many(many) => {
                        Ok(DbVal::Integer(many.len() as i64))
                    }
                    ArgValue::Rows(rows) => {
                        Ok(DbVal::Integer(rows.len() as i64))
                    }
                    _ => panic!()
                }


            }),
        ),
        (
            "count",
            DbFunction::builtin(vec![ArgType::Many(Type::Integer)], Type::Integer, |args| {
                let ArgValue::Many(rows) = &args[0] else {
                    panic!()
                };

                Ok(DbVal::Integer(rows.len() as i64))
            }),
        ),
        (
            "min",
            DbFunction::builtin(vec![ArgType::Many(Type::Integer)], Type::Integer, |args| {
                let ArgValue::Many(vals) = &args[0] else {
                    panic!()
                };

                Ok(vals
                    .iter()
                    .flat_map(|i| i.int_value())
                    .min()
                    .map(DbVal::Integer)
                    .unwrap_or(DbVal::Null))
            }),
        ),
        (
            "min",
            DbFunction::builtin(vec![ArgType::Many(Type::Float)], Type::Float, |args| {
                let ArgValue::Many(vals) = &args[0] else {
                    panic!()
                };

                Ok(vals
                    .iter()
                    .flat_map(|i| i.float_value())
                    .reduce(|a, b| match a.total_cmp(&b) {
                        Ordering::Less => a,
                        Ordering::Equal => a,
                        Ordering::Greater => b,
                    })
                    .map(|i| DbVal::Float(i))
                    .unwrap_or(DbVal::Null))
            }),
        ),
        (
            "max",
            DbFunction::builtin(vec![ArgType::Many(Type::Integer)], Type::Integer, |args| {
                let ArgValue::Many(vals) = &args[0] else {
                    panic!()
                };

                Ok(vals
                    .iter()
                    .flat_map(|i| i.int_value())
                    .max()
                    .map(DbVal::Integer)
                    .unwrap_or(DbVal::Null))
            }),
        ),
        (
            "max",
            DbFunction::builtin(vec![ArgType::Many(Type::Float)], Type::Float, |args| {
                let ArgValue::Many(vals) = &args[0] else {
                    panic!()
                };

                Ok(vals
                    .iter()
                    .flat_map(|i| i.float_value())
                    .reduce(|a, b| match a.total_cmp(&b) {
                        Ordering::Less => b,
                        Ordering::Equal => a,
                        Ordering::Greater => a,
                    })
                    .map(|i| DbVal::Float(i))
                    .unwrap_or(DbVal::Null))
            }),
        ),
        (
            "avg",
            DbFunction::builtin(vec![ArgType::Many(Type::Integer)], Type::Float, |args| {
                let ArgValue::Many(vals) = &args[0] else {
                    panic!()
                };

                let (sum, count) = vals
                    .iter()
                    .flat_map(|i| i.int_value())
                    .fold((0, 0), |(sum, count), next| {
                        (sum + next, count + 1)
                    });
                if count == 0 {
                    return Ok(DbVal::Float(f64::NAN))
                }

                Ok(DbVal::Float(sum as f64 / count as f64))
            }),
        ),
        (
            "avg",
            DbFunction::builtin(vec![ArgType::Many(Type::Float)], Type::Float, |args| {
                let ArgValue::Many(vals) = &args[0] else {
                    panic!()
                };

                let (sum, count) = vals
                    .iter()
                    .flat_map(|i| i.float_value())
                    .fold((0.0, 0), |(sum, count), next| {
                        (sum + next, count + 1)
                    });
                if count == 0 {
                    return Ok(DbVal::Float(f64::NAN))
                }

                Ok(DbVal::Float(sum / count as f64))
            }),
        ),
        (
            "pow",
            DbFunction::builtin(
                vec![ArgType::One(Type::Integer), ArgType::One(Type::Integer)],
                Type::Integer,
                |args| {
                    let ArgValue::One(int) = &args[0] else {
                        panic!()
                    };
                    let ArgValue::One(power) = &args[1] else {
                        panic!()
                    };

                    let i = int.int_value().unwrap();
                    let power = power.int_value().unwrap();

                    Ok(i.pow(power as u32).into())
                },
            ),
        ),
        (
            "pow",
            DbFunction::builtin(
                vec![ArgType::One(Type::Float), ArgType::One(Type::Float)],
                Type::Float,
                |args| {
                    let ArgValue::One(base) = &args[0] else {
                        panic!()
                    };
                    let ArgValue::One(power) = &args[1] else {
                        panic!()
                    };

                    let i = base.float_value().unwrap();
                    let power = power.float_value().unwrap();

                    Ok(i.powf(power).into())
                },
            ),
        ),

    ])
});
