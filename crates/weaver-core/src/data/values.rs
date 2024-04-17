use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt::Write;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use derive_more::From;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use weaver_ast::ast;

use crate::data::types::Type;
use crate::error::WeaverError;

/// A single value within a row
#[derive(Clone, Deserialize, Serialize, From)]
#[serde(untagged)]
pub enum DbVal {
    String(String, u16),
    Binary(Vec<u8>, u16),
    Integer(i64),
    Boolean(bool),
    Float(f64),
    Null,
}

impl DbVal {
    /// If this is an int value, returns as an int
    pub fn int_value(&self) -> Option<i64> {
        if let Self::Integer(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    pub fn float_value(&self) -> Option<f64> {
        if let Self::Float(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    pub fn string_value(&self) -> Option<&str> {
        if let Self::String(s, ..) = self {
            Some(s.as_ref())
        } else {
            None
        }
    }

    pub fn bool_value(&self) -> Option<bool> {
        if let Self::Boolean(b) = self {
            Some(*b)
        } else {
            None
        }
    }

    pub fn value_type(&self) -> Option<Type> {
        Some(match self {
            &DbVal::String(_, max_len) => Type::String(max_len),
            &DbVal::Binary(_, max_len) => Type::Binary(max_len),
            DbVal::Integer(_) => Type::Integer,
            DbVal::Boolean(_) => Type::Boolean,
            DbVal::Float(_) => Type::Float,
            DbVal::Null => {
                return None;
            }
        })
    }

    pub fn string<S: AsRef<str>>(s: S, max_len: impl Into<Option<u16>>) -> Self {
        Self::String(s.as_ref().to_string(), max_len.into().unwrap_or(u16::MAX))
    }

    pub fn binary<S: AsRef<[u8]>>(bytes: S, max_len: impl Into<Option<u16>>) -> Self {
        Self::Binary(bytes.as_ref().to_vec(), max_len.into().unwrap_or(u16::MAX))
    }
}

impl FromStr for DbVal {
    type Err = WeaverError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(i) = i64::from_str(s) {
            Ok(Self::from(i))
        } else if let Ok(f) = f64::from_str(s) {
            Ok(Self::from(f))
        } else if let Ok(f) = bool::from_str(s) {
            Ok(Self::from(f))
        } else {
            Ok(Self::from(s.to_string()))
        }
    }
}

impl AsRef<DbVal> for DbVal {
    fn as_ref(&self) -> &DbVal {
        self
    }
}
impl From<ast::Literal> for DbVal {
    fn from(value: ast::Literal) -> Self {
        match value {
            ast::Literal::String(s) => DbVal::String(s, u16::MAX),
            ast::Literal::Integer(i) => DbVal::Integer(i),
            ast::Literal::Float(f) => DbVal::Float(f),
            ast::Literal::Boolean(b) => DbVal::Boolean(b),
            ast::Literal::Binary(binary) => DbVal::Binary(binary.into(), u16::MAX),
            ast::Literal::Null => DbVal::Null,
        }
    }
}

impl From<&[u8]> for DbVal {
    fn from(value: &[u8]) -> Self {
        DbVal::binary(value, None)
    }
}

impl From<Vec<u8>> for DbVal {
    fn from(value: Vec<u8>) -> Self {
        DbVal::binary(value, None)
    }
}

impl From<&str> for DbVal {
    fn from(value: &str) -> Self {
        Self::string(value, None)
    }
}

impl From<&String> for DbVal {
    fn from(value: &String) -> Self {
        Self::string(value, None)
    }
}

impl From<String> for DbVal {
    fn from(value: String) -> Self {
        Self::string(value, None)
    }
}

impl From<Uuid> for DbVal {
    fn from(value: Uuid) -> Self {
        let as_binary = value.as_bytes();
        let binary = Vec::from(as_binary.as_slice());
        DbVal::Binary(binary, 16)
    }
}

impl<T> From<Option<T>> for DbVal
where
    DbVal: From<T>,
{
    fn from(value: Option<T>) -> Self {
        match value {
            None => DbVal::Null,
            Some(val) => DbVal::from(val),
        }
    }
}

impl From<Cow<'_, DbVal>> for DbVal {
    fn from(value: Cow<DbVal>) -> Self {
        value.into_owned()
    }
}

impl Display for DbVal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DbVal::String(s, _) => {
                write!(f, "{s}")
            }
            DbVal::Binary(b, _) => {
                write!(
                    f,
                    "{}",
                    b.iter().fold(String::new(), |mut accum, s| {
                        let _ = write!(accum, "{:x}", s);
                        accum
                    })
                )
            }
            DbVal::Integer(i) => {
                write!(f, "{i}")
            }
            DbVal::Boolean(b) => {
                write!(f, "{b}")
            }
            DbVal::Float(fl) => {
                write!(f, "{fl}")
            }
            DbVal::Null => {
                write!(f, "null")
            }
        }
    }
}

impl Debug for DbVal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DbVal::String(s, _) => {
                write!(f, "{s:?}")
            }
            DbVal::Binary(b, _) => {
                write!(
                    f,
                    "b\"{}\"",
                    b.iter().fold(String::new(), |mut accum, s| {
                        let _ = write!(accum, "{s:b}");
                        accum
                    })
                )
            }
            DbVal::Integer(i) => {
                write!(f, "{i}_i64")
            }
            DbVal::Boolean(b) => {
                write!(f, "{b}")
            }
            DbVal::Float(fl) => {
                write!(f, "{fl}_f64")
            }
            DbVal::Null => {
                write!(f, "null")
            }
        }
    }
}

impl PartialEq for DbVal {
    fn eq(&self, other: &Self) -> bool {
        use DbVal::*;
        match (self, other) {
            (String(l, _), String(r, _)) => l == r,
            (Binary(l, _), Binary(r, _)) => l == r,
            (Boolean(l), Boolean(r)) => l == r,
            (Integer(l), Integer(r)) => l == r,
            (Integer(l), Float(r)) => *l as f64 == *r,
            (Float(l), Integer(r)) => *l as i64 == *r,
            (Float(l), Float(r)) => l.total_cmp(r).is_eq(),
            (Null, Null) => true,
            _ => false,
        }
    }
}

impl Eq for DbVal {}

impl PartialOrd for DbVal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DbVal {
    fn cmp(&self, other: &Self) -> Ordering {
        use DbVal::*;
        match (self, other) {
            (String(l, _), String(r, _)) => l.cmp(r),
            (Binary(l, _), Binary(r, _)) => l.cmp(r),
            (Integer(l), Integer(r)) => l.cmp(r),
            (Integer(l), Float(r)) => (*l as f64).total_cmp(r),
            (Float(l), Integer(r)) => (*l as i64).cmp(r),
            (Float(l), Float(r)) => l.total_cmp(r),

            (Boolean(l), Boolean(r)) => l.cmp(r),
            (Null, Null) => Ordering::Equal,
            (_, Null) => Ordering::Greater,
            (Null, _) => Ordering::Less,
            _ => Ordering::Less,
        }
    }
}

impl Hash for DbVal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            DbVal::String(s, _) => s.hash(state),
            DbVal::Binary(s, _) => s.hash(state),
            DbVal::Integer(s) => s.hash(state),
            DbVal::Boolean(s) => s.hash(state),
            DbVal::Float(f) => u64::from_be_bytes(f.to_be_bytes()).hash(state),
            DbVal::Null => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use crate::key::KeyData;

    #[test]
    fn order_strings() {
        let mut bset = BTreeSet::new();
        bset.insert(KeyData::from(["hello, world!"]));
        bset.insert(KeyData::from(["world!"]));
        println!("bset: {bset:#?}");
    }
}
