use std::cmp::Ordering;
use std::fmt::Formatter;
use std::hash::{Hash, Hasher};
use std::mem::discriminant;

use derive_more::{AsRef, Deref, Display, From, IntoIterator};
use serde::{Deserialize, Serialize};

use crate::ast::Literal::Null;

/// A literal value
#[derive(Debug, Clone, Serialize, Deserialize, From, Display)]
#[serde(untagged)]
pub enum Literal {
    Binary(Binary),
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
}

impl PartialEq for Literal {
    fn eq(&self, other: &Self) -> bool {
        use Literal::*;
        match (self, other) {
            (Binary(l), Binary(r)) => l == r,
            (String(l), String(r)) => l == r,
            (Integer(l), Integer(r)) => l == r,
            (Float(l), Float(r)) => l == r,
            (Boolean(l), Boolean(r)) => l == r,
            (Null, Null) => false, // null is explicitly never equal
            _ => false,
        }
    }
}

impl Eq for Literal {}

impl Hash for Literal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let disc = discriminant(self);
        disc.hash(state);
        match self {
            Literal::Binary(b) => b.hash(state),
            Literal::String(s) => s.hash(state),
            Literal::Integer(i) => i.hash(state),
            Literal::Float(_f) => {
                unimplemented!("float hashing?")
            }
            Literal::Boolean(bool) => bool.hash(state),
            Null => {}
        }
    }
}

impl PartialOrd for Literal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use Literal::*;
        match (self, other) {
            (Binary(l), Binary(r)) => l.partial_cmp(r),
            (String(l), String(r)) => l.partial_cmp(r),
            (Integer(l), Integer(r)) => l.partial_cmp(r),
            (Float(l), Float(r)) => l.partial_cmp(r),
            (Boolean(l), Boolean(r)) => l.partial_cmp(r),
            (Null, Null) => None, // explicit null has no ordering
            _ => None,
        }
    }
}

#[derive(
    Debug,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
    Hash,
    Clone,
    Serialize,
    Deserialize,
    From,
    AsRef,
    Deref,
    IntoIterator,
)]
#[serde(transparent)]
pub struct Binary(Vec<u8>);

impl From<Binary> for Vec<u8> {
    fn from(value: Binary) -> Self {
        value.0
    }
}

impl Display for Binary {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "x'{}'",
            self.0.iter().map(|s| format!("{s:x}")).collect::<String>()
        )
    }
}
