use derive_more::{Display, From};
use serde::{Deserialize, Serialize};
use std::fmt::Formatter;
use std::ops::Deref;

/// An identifier
#[derive(Debug, Ord, PartialOrd, Hash, Eq, PartialEq, Clone, Serialize, Deserialize, Display)]
#[serde(transparent)]
pub struct Identifier(pub String);

impl Identifier {
    /// Creates an identifier
    pub fn new<S: AsRef<str>>(id: S) -> Self {
        Self(id.as_ref().to_string())
    }
}

impl AsRef<str> for Identifier {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl Deref for Identifier {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl From<&str> for Identifier {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for Identifier {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl PartialEq<str> for Identifier {
    fn eq(&self, other: &str) -> bool {
        self.as_ref() == other
    }
}

/// Should be used to refer to
#[derive(Debug, Ord, PartialOrd, Hash, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnresolvedColumnRef {
    table: Option<Identifier>,
    column: Identifier,
}

impl Display for UnresolvedColumnRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "?.{}{}",
            self.table()
                .map(|s| format!("{s}."))
                .unwrap_or("?.".to_string()),
            self.column()
        )
    }
}

impl UnresolvedColumnRef {
    pub fn with_column(col: Identifier) -> Self {
        Self {
            table: None,
            column: col,
        }
    }

    pub fn with_table(table: Identifier, col: Identifier) -> Self {
        Self {
            table: Some(table),
            column: col,
        }
    }

    #[deprecated = "uses schema"]
    #[allow(deprecated)]
    pub fn as_tuple(&self) -> (Option<&Identifier>, Option<&Identifier>, &Identifier) {
        (self.schema(), self.table(), self.column())
    }

    #[deprecated]
    pub fn schema(&self) -> Option<&Identifier> {
        None
    }
    pub fn table(&self) -> Option<&Identifier> {
        self.table.as_ref()
    }
    pub fn column(&self) -> &Identifier {
        &self.column
    }
}

/// Should be used to refer to
#[derive(Debug, Ord, PartialOrd, Hash, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedColumnRef {
    schema: Identifier,
    table: Identifier,
    column: Identifier,
}

impl ResolvedColumnRef {
    pub fn new(
        schema: impl Into<Identifier>,
        table: impl Into<Identifier>,
        column: impl Into<Identifier>,
    ) -> Self {
        Self {
            schema: schema.into(),
            table: table.into(),
            column: column.into(),
        }
    }
    pub fn schema(&self) -> &Identifier {
        &self.schema
    }
    pub fn table(&self) -> &Identifier {
        &self.table
    }
    pub fn column(&self) -> &Identifier {
        &self.column
    }
}

impl Display for ResolvedColumnRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.schema(), self.table(), self.column())
    }
}
