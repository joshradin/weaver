use crate::data::values::DbVal;
use crate::error::Error;
use crate::storage::ReadDataError;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, Copy, Clone)]
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
}
