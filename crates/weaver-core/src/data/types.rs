use std::fmt::{Display, Formatter};
use crate::data::values::Value;
use crate::error::Error;
use crate::storage::ReadDataError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, Copy, Clone)]
#[repr(u8)]
pub enum Type {
    String = 1,
    Blob,
    Integer,
    Boolean,
    Float,
}

impl Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s: &str = match self {
            Type::String => { "string"}
            Type::Blob => { "blob"}
            Type::Integer => { "int"}
            Type::Boolean => { "bool"}
            Type::Float => { "float"}
        };
        write!(f, "{s}")
    }
}

impl Type {
    /// Checks whether the given value is valid for this type
    pub fn validate(&self, val: &Value) -> bool {
        use Type::*;
        match (self, val) {
            (String, Value::String(..)) => true,
            (Blob, Value::Blob(..)) => true,
            (Integer, Value::Integer(..)) => true,
            (Boolean, Value::Boolean(..)) => true,
            (Float, Value::Float(..)) => true,
            (_, Value::Null) => true,
            _ => false,
        }
    }
}

impl TryFrom<u8> for Type {
    type Error = ReadDataError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        use Type::*;
        match value {
            1 => Ok(String),
            2 => Ok(Blob),
            3 => Ok(Integer),
            4 => Ok(Boolean),
            5 => Ok(Float),
            u => Err(Self::Error::UnknownTypeDiscriminant(u)),
        }
    }
}
