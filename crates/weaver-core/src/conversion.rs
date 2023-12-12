//! Type conversion

use crate::data::row::{OwnedRow, Row};

/// Convert a given type to a row
pub trait IntoRow {
    fn into_row(self) -> OwnedRow;
}

/// Convert a row to a value
pub trait FromRow {
    fn from_row<'a, R: AsRef<Row<'a>>>(row: R) -> Self;
}
