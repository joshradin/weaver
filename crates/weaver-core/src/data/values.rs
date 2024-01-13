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
    String(String, u16),
    Binary(Vec<u8>, u16),
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
            &Value::String(_, max_len) => Type::String(max_len),
            &Value::Binary(_, max_len) => Type::Binary(max_len),
            Value::Integer(_) => Type::Integer,
            Value::Boolean(_) => Type::Boolean,
            Value::Float(_) => Type::Float,
            Value::Null => {
                return None;
            }
        })
    }

    pub fn string<S: AsRef<str>>(s: S, max_len: impl Into<Option<u16>>) -> Self {
        Self::String(s.as_ref().to_string(), max_len.into().unwrap_or(u16::MAX))
    }

    pub fn binary<S: for<'a> AsRef<&'a [u8]>>(bytes: S, max_len: impl Into<Option<u16>>) -> Self {
        Self::Binary(bytes.as_ref().to_vec(), max_len.into().unwrap_or(u16::MAX))
    }
}

impl AsRef<Value> for Value {
    fn as_ref(&self) -> &Value {
        self
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::string(value, None)
    }
}

impl From<&String> for Value {
    fn from(value: &String) -> Self {
        Self::string(value, None)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::string(value, None)
    }
}
impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::String(s, _) => {
                write!(f, "{s}")
            }
            Value::Binary(b, _) => {
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
            Value::String(s, _) => {
                write!(f, "{s:?}")
            }
            Value::Binary(b, _) => {
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
            (String(l, _), String(r, _)) => l == r,
            (Binary(l, _), Binary(r, _)) => l == r,
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
        let emit = Some(match (self, other) {
            (String(l, l_max_len), String(r, r_max_len)) => {
                let max = *l_max_len.max(r_max_len) as usize;
                let l = format!("{l}{}", "\u{0}".repeat(max - l.len()));
                let r = format!("{r}{}", "\u{0}".repeat(max - r.len()));
                l.cmp(&r)
            }
            (Binary(l, _), Binary(r, _)) => l.cmp(r),
            (Integer(l), Integer(r)) => l.cmp(r),
            (Boolean(l), Boolean(r)) => l.cmp(r),
            (Float(l), Float(r)) => l.total_cmp(r),
            (Null, Null) => Ordering::Equal,
            (_, Null) => Ordering::Greater,
            (Null, _) => Ordering::Less,
            _ => return None,
        });
        emit
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::String(s, _) => s.hash(state),
            Value::Binary(s, _) => s.hash(state),
            Value::Integer(s) => s.hash(state),
            Value::Boolean(s) => s.hash(state),
            Value::Float(f) => u64::from_be_bytes(f.to_be_bytes()).hash(state),
            Value::Null => ().hash(state),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::data::values::Value;
    use crate::key::KeyData;
    use std::collections::BTreeSet;

    #[test]
    fn order_strings() {
        let mut bset = BTreeSet::new();
        bset.insert(KeyData::from(["hello, world!"]));
        bset.insert(KeyData::from(["world!"]));
        println!("bset: {bset:#?}");
    }
}
