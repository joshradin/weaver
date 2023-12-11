use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use serde::{Deserialize, Serialize};

/// A single value within a row
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Value {
    String(String),
    Blob(Vec<u8>),
    Integer(i64),
    Boolean(bool),
    Float(f64),
    Null,
}

impl Value {
    /// If this is an int value, returns as an int
    pub fn int_value(&self) -> Option<i64> {
        if let Self::Integer(i) = self {
            Some(*i)
        } else {
            None
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        use Value::*;
        use crate::data::Value::Null;
        match (self, other) {
            (String(l), String(r)) => l == r,
            (Blob(l), Blob(r)) => l == r,
            (Integer(l), Integer(r)) => l == r,
            (Boolean(l), Boolean(r)) => l == r,
            (Float(l), Float(r)) => l.total_cmp(r).is_eq(),
            (Null, Null) => true,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use Value::*;
        use crate::data::Value::Null;
        Some(match (self, other) {
            (String(l), String(r)) => l.cmp(r),
            (Blob(l), Blob(r)) => l.cmp(r),
            (Integer(l), Integer(r)) => l.cmp(r),
            (Boolean(l), Boolean(r)) => l.cmp(r),
            (Float(l), Float(r)) => l.total_cmp(r),
            (Null, Null) => Ordering::Equal,
            (_, Null) => Ordering::Greater,
            (Null, _) => Ordering::Less,
            _ => return None,
        })
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::String(s) => s.hash(state),
            Value::Blob(s) => s.hash(state),
            Value::Integer(s) => s.hash(state),
            Value::Boolean(s) => s.hash(state),
            Value::Float(f) => u64::from_be_bytes(f.to_be_bytes()).hash(state),
            Value::Null => ().hash(state),
        }
    }
}
