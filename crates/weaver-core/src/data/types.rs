use std::fmt::{Display, Formatter};
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use weaver_ast::ast;
use weaver_ast::ast::{BinaryOp, ColumnRef, DataType, Expr, FunctionArgs, VarBinaryType, VarCharType};

use crate::data::values::DbVal;
use crate::error::WeaverError;
use crate::queries::execution::evaluation::{find_function, FunctionKind};
use crate::queries::execution::evaluation::functions::{ArgType, FunctionRegistry};
use crate::storage::tables::table_schema::TableSchema;

#[derive(Debug, Deserialize, Serialize, Hash, Eq, PartialEq, Copy, Clone)]
pub enum Type {
    String(u16),
    Binary(u16),
    Integer,
    Boolean,
    Float,
}

impl Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Type::String(i) => write!(f, "string({i})"),
            Type::Binary(i) => write!(f, "binary({i})"),
            Type::Integer => write!(f, "int"),
            Type::Boolean => write!(f, "boolean"),
            Type::Float => write!(f, "float"),
        }
    }
}

impl Type {
    /// Checks whether the given value is valid for this type
    pub fn validate(&self, val: &DbVal) -> bool {
        use Type::*;
        match (self, val) {
            (String(len), DbVal::String(s, _)) => s.len() <= (*len as usize),
            (Binary(len), DbVal::Binary(b, _)) => b.len() <= (*len as usize),
            (Integer, DbVal::Integer(..)) => true,
            (Boolean, DbVal::Boolean(..)) => true,
            (Float, DbVal::Float(..)) => true,
            (_, DbVal::Null) => true,
            _ => false,
        }
    }

    /// Attempts to parse a string based on the type
    pub fn parse_value<S: AsRef<str>>(&self, s: S) -> Result<DbVal, WeaverError> {
        let db_val: DbVal = match self {
            Type::String(_) => s.as_ref().to_string().into(),
            Type::Binary(_) => s.as_ref().bytes().collect::<Vec<_>>().into(),
            Type::Integer => i64::from_str(s.as_ref())?.into(),
            Type::Boolean => bool::from_str(s.as_ref())?.into(),
            Type::Float => f64::from_str(s.as_ref())?.into(),
        };
        if !self.validate(&db_val) {
            return Err(WeaverError::TypeError {
                expected: self.clone(),
                actual: db_val,
            });
        };
        Ok(db_val)
    }
}

impl From<ast::DataType> for Type {
    fn from(value: DataType) -> Self {
        match value {
            DataType::Int(_) => Type::Integer,
            DataType::Float(_) => Type::Float,
            DataType::VarCharType(VarCharType(len)) => Type::String(len as u16),
            DataType::VarBinaryType(VarBinaryType(len)) => Type::Binary(len as u16),
            DataType::BooleanType(_) => Type::Boolean,
        }
    }
}

/// Gets the [Type] of the given object
pub trait DbTypeOf {
    /// Gets the db type of a given value, if possible
    fn type_of(
        &self,
        functions: &FunctionRegistry,
        context_schema: Option<&TableSchema>,
    ) -> Result<Type, WeaverError>;
}

impl DbTypeOf for DbVal {
    fn type_of(
        &self,
        functions: &FunctionRegistry,
        context_schema: Option<&TableSchema>,
    ) -> Result<Type, WeaverError> {
        self.value_type().ok_or_else(|| unreachable!())
    }
}

impl DbTypeOf for Expr {
    fn type_of(
        &self,
        functions: &FunctionRegistry,
        context_schema: Option<&TableSchema>,
    ) -> Result<Type, WeaverError> {
        match self {
            Expr::Column { column } => match (column, context_schema) {
                (ColumnRef::Resolved(resolved), Some(schema)) => {
                    Ok(schema.column_by_source(resolved)
                        .ok_or_else(|| WeaverError::ColumnNotFound(resolved.to_string()))?
                        .data_type())
                }
                (ColumnRef::Unresolved(unresolved), None) => {
                    Err(WeaverError::ColumnNotResolved(unresolved.clone()))
                }
                _ => Err(WeaverError::NoTableSchema),
            },
            Expr::Literal { literal } => {
                DbVal::from(literal.clone()).type_of(functions, context_schema)
            }
            Expr::BindParameter { .. } => Err(WeaverError::UnboundParameter),
            Expr::Unary { expr, .. } => expr.type_of(functions, context_schema),
            Expr::Binary { left, right, op } => match op {
                BinaryOp::Eq
                | BinaryOp::Neq
                | BinaryOp::Greater
                | BinaryOp::Less
                | BinaryOp::GreaterEq
                | BinaryOp::LessEq
                | BinaryOp::And
                | BinaryOp::Or => Ok(Type::Boolean),
                BinaryOp::Plus | BinaryOp::Minus | BinaryOp::Multiply | BinaryOp::Divide => left
                    .type_of(functions, context_schema)
                    .or(right.type_of(functions, context_schema)),
            },
            Expr::FunctionCall { function, args } => {
                let FunctionKind {
                    normal, aggregate
                } = find_function(functions, function, args, context_schema)?;

                normal.or(aggregate)
                    .map(|func| {
                        func.return_type().clone()
                    })
                    .ok_or_else(|| {
                        let arg_types = match args {
                            FunctionArgs::Params { exprs, .. } => {
                                exprs.iter().map(|i| i.type_of(functions, context_schema))
                                     .flat_map(|i| i.ok())
                                     .map(ArgType::One)
                                     .collect()
                            }
                            FunctionArgs::Wildcard { ..}=> {
                                vec![ArgType::Rows]
                            }
                        };
                        WeaverError::UnknownFunction(function.to_string(), arg_types)
                    })
            }
        }
    }
}
