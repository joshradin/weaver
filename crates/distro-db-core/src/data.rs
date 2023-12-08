//! The data that is actually stored

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::borrow::{Borrow, Cow};
use std::cmp::Ordering;
use std::fmt::Formatter;
use std::hash::{Hash, Hasher};
use std::ops::{Deref, DerefMut, Index, IndexMut, RangeBounds};
use std::slice::SliceIndex;
use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeSeq;

#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, Copy, Clone)]
pub enum Type {
    String,
    Blob,
    Number,
    Boolean,
    Float,
}

/// A single value within a row
#[derive(Debug, Clone, Deserialize, Serialize,)]
#[serde(untagged)]
pub enum Value {
    String(String),
    Blob(Vec<u8>),
    Number(i64),
    Boolean(bool),
    Float(f64),
    Null,
}
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        use Value::*;
        match (self, other) {
            (String(l), String(r)) => l == r,
            (Blob(l), Blob(r)) => l == r,
            (Number(l), Number(r)) => l == r,
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
        use Value::*;
        Some(match (self, other) {
            (String(l), String(r)) => l.cmp(r),
            (Blob(l), Blob(r)) => l.cmp(r),
            (Number(l), Number(r)) => l.cmp(r),
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
            Value::Number(s) => s.hash(state),
            Value::Boolean(s) => s.hash(state),
            Value::Float(f) => u64::from_be_bytes(f.to_be_bytes()).hash(state),
            Value::Null => ().hash(state),
        }
    }
}

/// A row of data
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash)]
pub struct Row<'a>(Box<[Cow<'a, Value>]>);

impl<'a> Row<'a> {
    /// Creates a new, empty row of a given length.
    ///
    /// All entries are initialized to Null
    pub fn new(len: usize) -> Self {
        Self::from(vec![Value::Null; len])
    }

    /// Gets a slice of the data
    pub fn slice<I>(&'a self, range: I) -> Row<'a>
    where
        I: SliceIndex<[Cow<'a, Value>], Output = [Cow<'a, Value>]>,
    {
        Self::from(&self.0[range])
    }

    /// Joins two rows together
    pub fn join(&self, other: &Row<'a>) -> Row<'a> {
        Self::from_iter(self.iter().chain(other.iter()).cloned())
    }

    /// Iterator over a row
    pub fn iter(&self) -> impl Iterator<Item = &Cow<'a, Value>> {
        self.0.iter()
    }
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Cow<'a, Value>> {
        self.0.iter_mut()
    }




    /// Gets the length of the row
    pub fn len(&self) -> usize {
        self.0.len()
    }
}


impl<'a> Deserialize<'a> for Row<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'a> {
        deserializer.deserialize_any(RowVisitor)
    }
}

impl<'a> Serialize for Row<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut seq = serializer.serialize_seq(None)?;
        for val in self.iter() {
            seq.serialize_element(&*val)?;
        }
        seq.end()
    }
}
struct RowVisitor;
impl<'de> Visitor<'de> for RowVisitor {
    type Value = Row<'de>;

    fn expecting(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "a valid row")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error> where A: SeqAccess<'de> {
        let mut vec = vec![];
        while let Some(ele) = seq.next_element::<Value>()? {
            vec.push(ele)
        }
        Ok(Row::from(vec))
    }
}


impl<'a> From<OwnedRow> for Row<'a> {
    fn from(value: OwnedRow) -> Self {
        Row(value.0 .0)
    }
}

impl<'a> From<Vec<Cow<'a, Value>>> for Row<'a> {
    fn from(value: Vec<Cow<'a, Value>>) -> Self {
        Self(value.into_boxed_slice())
    }
}

impl From<Vec<Value>> for Row<'_> {
    fn from(value: Vec<Value>) -> Self {
        Self(
            value
                .into_iter()
                .map(|v| Cow::Owned(v))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        )
    }
}

impl<const N: usize> From<[Value; N]> for Row<'_> {
    fn from(value: [Value; N]) -> Self {
        Self::from(value.into_iter().collect::<Vec<_>>())
    }
}

impl From<Box<[Value]>> for Row<'_> {
    fn from(value: Box<[Value]>) -> Self {
        Self::from(value.into_vec())
    }
}

impl<'a> From<Box<[Cow<'a, Value>]>> for Row<'a> {
    fn from(value: Box<[Cow<'a, Value>]>) -> Self {
        Self(value)
    }
}

impl<'a> From<&'a [Value]> for Row<'a> {
    fn from(value: &'a [Value]) -> Self {
        Self(
            value
                .iter()
                .map(|v| Cow::Borrowed(v))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        )
    }
}

impl<'a> From<&'a [Cow<'a, Value>]> for Row<'a> {
    fn from(value: &'a [Cow<'a, Value>]) -> Self {
        Self(
            value
                .iter()
                .map(|v| v.clone())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        )
    }
}

impl<'a, T> FromIterator<T> for Row<'a>
where
    Cow<'a, Value>: From<T>,
{
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self(
            iter.into_iter()
                .map(|v| Cow::from(v))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        )
    }
}

impl<'a> Index<usize> for Row<'a> {
    type Output = Cow<'a, Value>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl<'a> IndexMut<usize> for Row<'a> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.0[index]
    }
}

impl<'a> ToOwned for Row<'a> {
    type Owned = OwnedRow;

    fn to_owned(&self) -> Self::Owned {
        OwnedRow::from(self)
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Hash, Serialize)]
pub struct OwnedRow(Row<'static>);

impl<'a> From<Row<'a>> for OwnedRow {
    fn from(value: Row<'a>) -> Self {
        Self(Row(value
            .iter()
            .map(|c| Cow::Owned(c.to_owned().into_owned()))
            .collect::<Vec<_>>()
            .into_boxed_slice()))
    }
}

impl Clone for OwnedRow {
    fn clone(&self) -> Self {
        self.0.to_owned()
    }
}

impl<'a> Borrow<Row<'a>> for OwnedRow {
    fn borrow(&self) -> &Row<'a> {
        &self.0
    }
}

impl<'a> AsRef<Row<'a>> for OwnedRow {
    fn as_ref(&self) -> &Row<'a> {
        &self.0
    }
}

impl<'a> From<&Row<'a>> for OwnedRow {
    fn from(value: &Row<'a>) -> Self {
        Self(Row(value
            .iter()
            .map(|c| Cow::Owned(c.to_owned().into_owned()))
            .collect::<Vec<_>>()
            .into_boxed_slice()))
    }
}

impl Deref for OwnedRow {
    type Target = Row<'static>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for OwnedRow {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use crate::data::{Row, Value};

    #[test]
    fn slice_row() {
        let row = Row::new(5);
        let slice = row.slice(1..=3);
        assert_eq!(slice, Row::new(3));
    }

    #[test]
    fn deserialize_row() {
        let json = r#"[1, 2, 3, 4, null]"#;
        let as_row: Row = serde_json::from_str(json).expect("could not deserialize");
        assert_eq!(as_row, Row::from([Value::Number(1), Value::Number(2), Value::Number(3),Value::Number(4), Value::Null]))
    }
}
