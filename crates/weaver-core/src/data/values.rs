use crate::data::types::Type;
use derive_more::From;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};

/// A single value within a row
#[derive(Clone, Deserialize, Serialize, From)]
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

    pub fn value_type(&self) -> Option<Type> {
        Some(match self {
            Value::String(_) => Type::String,
            Value::Blob(_) => Type::Blob,
            Value::Integer(_) => Type::Integer,
            Value::Boolean(_) => Type::Boolean,
            Value::Float(_) => Type::Float,
            Value::Null => {
                return None;
            }
        })
    }
}

impl AsRef<Value> for Value {
    fn as_ref(&self) -> &Value {
        self
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::from(value.to_string())
    }
}

impl From<&String> for Value {
    fn from(value: &String) -> Self {
        Self::from(value.to_string())
    }
}
impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::String(s) => {
                write!(f, "{s}")
            }
            Value::Blob(b) => {
                write!(
                    f,
                    "{}",
                    b.iter().map(|s| format!("{:x}", s)).collect::<String>()
                )
            }
            Value::Integer(i) => {
                write!(f, "{i}")
            }
            Value::Boolean(b) => {
                write!(f, "{b}")
            }
            Value::Float(fl) => {
                write!(f, "{fl}")
            }
            Value::Null => {
                write!(f, "null")
            }
        }
    }
}

impl Debug for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::String(s) => {
                write!(f, "{s:?}")
            }
            Value::Blob(b) => {
                write!(
                    f,
                    "b\"{}\"",
                    b.iter().map(|s| format!("{:x}", s)).collect::<String>()
                )
            }
            Value::Integer(i) => {
                write!(f, "{i}_i64")
            }
            Value::Boolean(b) => {
                write!(f, "{b}")
            }
            Value::Float(fl) => {
                write!(f, "{fl}_f64")
            }
            Value::Null => {
                write!(f, "null")
            }
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        use crate::data::Value::Null;
        use Value::*;
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
        use crate::data::Value::Null;
        use Value::*;
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
