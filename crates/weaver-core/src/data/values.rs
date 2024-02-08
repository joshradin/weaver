use crate::data::types::Type;
use derive_more::From;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use weaver_ast::ast;

/// A single value within a row
#[derive(Clone, Deserialize, Serialize, From)]
#[serde(untagged)]
pub enum Literal {
    String(String, u16),
    Binary(Vec<u8>, u16),
    Integer(i64),
    Boolean(bool),
    Float(f64),
    Null,
}

impl Literal {
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
            &Literal::String(_, max_len) => Type::String(max_len),
            &Literal::Binary(_, max_len) => Type::Binary(max_len),
            Literal::Integer(_) => Type::Integer,
            Literal::Boolean(_) => Type::Boolean,
            Literal::Float(_) => Type::Float,
            Literal::Null => {
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

impl AsRef<Literal> for Literal {
    fn as_ref(&self) -> &Literal {
        self
    }
}
impl From<ast::Literal> for Literal {
    fn from(value: ast::Literal) -> Self {
        match value {
            ast::Literal::String(s) => Literal::String(s.to_string(), u16::MAX),
            ast::Literal::Integer(i) => Literal::Integer(i),
            ast::Literal::Float(f) => Literal::Float(f),
            ast::Literal::Boolean(b) => Literal::Boolean(b),
        }
    }
}
impl From<&str> for Literal {
    fn from(value: &str) -> Self {
        Self::string(value, None)
    }
}

impl From<&String> for Literal {
    fn from(value: &String) -> Self {
        Self::string(value, None)
    }
}

impl From<String> for Literal {
    fn from(value: String) -> Self {
        Self::string(value, None)
    }
}
impl Display for Literal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::String(s, _) => {
                write!(f, "{s}")
            }
            Literal::Binary(b, _) => {
                write!(
                    f,
                    "{}",
                    b.iter().map(|s| format!("{:x}", s)).collect::<String>()
                )
            }
            Literal::Integer(i) => {
                write!(f, "{i}")
            }
            Literal::Boolean(b) => {
                write!(f, "{b}")
            }
            Literal::Float(fl) => {
                write!(f, "{fl}")
            }
            Literal::Null => {
                write!(f, "null")
            }
        }
    }
}

impl Debug for Literal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::String(s, _) => {
                write!(f, "{s:?}")
            }
            Literal::Binary(b, _) => {
                write!(
                    f,
                    "b\"{}\"",
                    b.iter().map(|s| format!("{:x}", s)).collect::<String>()
                )
            }
            Literal::Integer(i) => {
                write!(f, "{i}_i64")
            }
            Literal::Boolean(b) => {
                write!(f, "{b}")
            }
            Literal::Float(fl) => {
                write!(f, "{fl}_f64")
            }
            Literal::Null => {
                write!(f, "null")
            }
        }
    }
}

impl PartialEq for Literal {
    fn eq(&self, other: &Self) -> bool {
        use crate::data::Literal::Null;
        use Literal::*;
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

impl Eq for Literal {}

impl PartialOrd for Literal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use crate::data::Literal::Null;
        use Literal::*;
        let emit = Some(match (self, other) {
            (String(l, l_max_len), String(r, r_max_len)) => l.cmp(&r),
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

impl Hash for Literal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Literal::String(s, _) => s.hash(state),
            Literal::Binary(s, _) => s.hash(state),
            Literal::Integer(s) => s.hash(state),
            Literal::Boolean(s) => s.hash(state),
            Literal::Float(f) => u64::from_be_bytes(f.to_be_bytes()).hash(state),
            Literal::Null => ().hash(state),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::data::values::Literal;
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
