use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

/// An identifier
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize, From, Display)]
#[serde(transparent)]
pub struct Identifier(pub String);

impl AsRef<str> for Identifier {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}
