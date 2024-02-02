use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

/// A literal value
#[derive(Debug, PartialOrd, PartialEq, Clone, Serialize, Deserialize, From, Display)]
#[serde(untagged)]
pub enum Literal {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool)
}