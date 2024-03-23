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
            "min",
            DbFunction::builtin(vec![ArgType::Many(Type::Integer)], Type::Integer, |args| {
                let ArgValue::Many(vals) = &args[0] else {
                    panic!()
                };

                Ok(vals
                    .iter()
                    .flat_map(|i| i.int_value())
                    .min()
                    .map(|i| DbVal::Integer(i))
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
