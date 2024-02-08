use derive_more::{Add, AsRef, Deref, Display, Div, From, IntoIterator, Mul, Sub};
use serde::{Deserialize, Serialize};
use std::fmt::Formatter;

/// A literal value
#[derive(Debug, PartialOrd, PartialEq, Clone, Serialize, Deserialize, From, Display)]
#[serde(untagged)]
pub enum Literal {
    Binary(Binary),
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
}

#[derive(
    Debug, PartialOrd, PartialEq, Clone, Serialize, Deserialize, From, AsRef, Deref, IntoIterator,
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
