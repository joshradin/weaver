use serde::{Deserialize, Serialize};
use crate::data::values::Value;

#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, Copy, Clone)]
pub enum Type {
    String,
    Blob,
    Integer,
    Boolean,
    Float,
}

impl Type {

    /// Checks whether the given value is valid for this type
    pub fn validate(&self, val: &Value) -> bool {
        use Type::*;
        match (self, val) {
            (String, Value::String(..)) => true,
            (Blob, Value::Blob(..)) => true,
            (Integer, Value::Integer(..)) => true,
            (Boolean, Value::Boolean(..)) =>true,
            (Float, Value::Float(..)) => true,
            (_, Value::Null) => true,
            _ => false
        }
    }
}
