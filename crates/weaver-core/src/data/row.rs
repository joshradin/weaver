//! A row of data

use std::borrow::{Borrow, Cow};
use std::collections::VecDeque;
use std::fmt;
use std::fmt::{Debug, Formatter, Write};
use std::ops::{
    Deref, DerefMut, Index, IndexMut, RangeBounds, RangeFrom, RangeFull, RangeInclusive, RangeTo,
    RangeToInclusive,
};
use std::slice::SliceIndex;

use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::data::types::Type;
use crate::data::values::DbVal;

/// A row of data
#[derive(PartialEq, Eq, PartialOrd, Hash)]
pub struct Row<'a>(Box<[Cow<'a, DbVal>]>);

impl<'a> Row<'a> {
    /// Creates a new, empty row of a given length.
    ///
    /// All entries are initialized to Null
    pub fn new(len: usize) -> Self {
        Self::from(vec![DbVal::Null; len])
    }

    pub fn get(&self, index: usize) -> Option<&Cow<'a, DbVal>> {
        self.0.get(index)
    }

    /// Gets a slice of the data
    pub fn slice<I>(&self, range: I) -> Row<'a>
    where
        I: SliceIndex<[Cow<'a, DbVal>], Output = [Cow<'a, DbVal>]>,
    {
        Self::from(self.0[range].to_vec())
    }

    /// Gets a slice of the data if all values are within range
    pub fn try_slice<I>(&self, range: I) -> Option<Row<'a>>
    where
        I: SliceIndex<[Cow<'a, DbVal>], Output = [Cow<'a, DbVal>]>,
    {
        self.0.get(range).map(|values| Self::from(values.to_vec()))
    }

    /// Joins two rows together
    pub fn join(&self, other: &Row<'a>) -> Row<'a> {
        Self::from_iter(self.iter().chain(other.iter()).cloned())
    }

    /// Iterator over a row
    pub fn iter(&self) -> RowRefIter<'a, '_> {
        self.0.iter()
    }
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Cow<'a, DbVal>> {
        self.0.iter_mut()
    }

    /// Gets the length of the row
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Gets the types of the values
    pub fn types(&self) -> Vec<Option<Type>> {
        self.iter().map(|val| val.value_type()).collect()
    }
}

impl<'a> Deserialize<'a> for Row<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'a>,
    {
        deserializer.deserialize_any(RowVisitor)
    }
}

impl<'a> Serialize for Row<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(None)?;
        for val in self.iter() {
            seq.serialize_element(&*val)?;
        }
        seq.end()
    }
}

/// Writes a row to a formatter
pub fn write_row(writer: &mut Formatter, row: &Row) -> fmt::Result {
    let mut list = writer.debug_list();
    for value in row {
        list.entry(value);
    }
    list.finish()
}

impl Debug for Row<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_char('&')?;
        write_row(f, self)
    }
}

struct RowVisitor;

impl<'de> Visitor<'de> for RowVisitor {
    type Value = Row<'de>;

    fn expecting(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "a valid row")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut vec = vec![];
        while let Some(ele) = seq.next_element::<DbVal>()? {
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

impl<'a> From<Vec<Cow<'a, DbVal>>> for Row<'a> {
    fn from(value: Vec<Cow<'a, DbVal>>) -> Self {
        Self(value.into_boxed_slice())
    }
}

impl From<Vec<DbVal>> for Row<'_> {
    fn from(value: Vec<DbVal>) -> Self {
        Self(
            value
                .into_iter()
                .map(|v| Cow::Owned(v))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        )
    }
}

impl<V: Into<DbVal>, const N: usize> From<[V; N]> for Row<'_> {
    fn from(value: [V; N]) -> Self {
        Self::from(
            value
                .into_iter()
                .map(|value| value.into())
                .collect::<Vec<_>>(),
        )
    }
}

impl From<Box<[DbVal]>> for Row<'_> {
    fn from(value: Box<[DbVal]>) -> Self {
        Self::from(value.into_vec())
    }
}

impl<'a> From<Box<[Cow<'a, DbVal>]>> for Row<'a> {
    fn from(value: Box<[Cow<'a, DbVal>]>) -> Self {
        Self(value)
    }
}

impl<'a> From<&'a [DbVal]> for Row<'a> {
    fn from(value: &'a [DbVal]) -> Self {
        Self(
            value
                .iter()
                .map(|v| Cow::Borrowed(v))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        )
    }
}

impl<'a> From<&'a [Cow<'a, DbVal>]> for Row<'a> {
    fn from(value: &'a [Cow<'a, DbVal>]) -> Self {
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
    Cow<'a, DbVal>: From<T>,
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
    type Output = Cow<'a, DbVal>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl<'a> Index<RangeTo<usize>> for Row<'a> {
    type Output = [Cow<'a, DbVal>];

    fn index(&self, index: RangeTo<usize>) -> &Self::Output {
        &self.0[index]
    }
}

impl<'a> Index<RangeToInclusive<usize>> for Row<'a> {
    type Output = [Cow<'a, DbVal>];

    fn index(&self, index: RangeToInclusive<usize>) -> &Self::Output {
        &self.0[index]
    }
}

impl<'a> Index<RangeFull> for Row<'a> {
    type Output = [Cow<'a, DbVal>];

    fn index(&self, index: RangeFull) -> &Self::Output {
        &self.0[index]
    }
}
impl<'a> Index<RangeFrom<usize>> for Row<'a> {
    type Output = [Cow<'a, DbVal>];

    fn index(&self, index: RangeFrom<usize>) -> &Self::Output {
        &self.0[index]
    }
}

impl<'a> Index<RangeInclusive<usize>> for Row<'a> {
    type Output = [Cow<'a, DbVal>];

    fn index(&self, index: RangeInclusive<usize>) -> &Self::Output {
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

impl<'a> PartialEq<OwnedRow> for Row<'a> {
    fn eq(&self, other: &OwnedRow) -> bool {
        self == other.as_ref()
    }
}

#[derive(Debug)]
pub struct RowIter {
    values: VecDeque<DbVal>,
}

impl Iterator for RowIter {
    type Item = DbVal;

    fn next(&mut self) -> Option<Self::Item> {
        self.values.pop_front()
    }
}

impl<'a> IntoIterator for Row<'a> {
    type Item = DbVal;
    type IntoIter = RowIter;

    fn into_iter(self) -> Self::IntoIter {
        RowIter {
            values: self.0.iter().map(|t| (**t).clone()).collect(),
        }
    }
}

pub type RowRefIter<'a, 'b> = <&'b [Cow<'a, DbVal>] as IntoIterator>::IntoIter;

impl<'a, 'b: 'a> IntoIterator for &'b Row<'a> {
    type Item = &'b Cow<'a, DbVal>;
    type IntoIter = RowRefIter<'a, 'b>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[derive(PartialEq, Eq, PartialOrd, Hash, Serialize)]
pub struct OwnedRow(Row<'static>);

impl<'a> PartialEq<Row<'a>> for OwnedRow {
    fn eq(&self, other: &Row<'a>) -> bool {
        self.as_ref() == other
    }
}

impl<'de> Deserialize<'de> for OwnedRow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer
            .deserialize_any(RowVisitor)
            .map(|v| v.to_owned())
    }
}

impl<'a> From<Row<'a>> for OwnedRow {
    fn from(value: Row<'a>) -> Self {
        Self(Row(value
            .iter()
            .map(|c| Cow::Owned(c.to_owned().into_owned()))
            .collect::<Vec<_>>()
            .into_boxed_slice()))
    }
}
impl<V: Into<DbVal>, const N: usize> From<[V; N]> for OwnedRow {
    fn from(value: [V; N]) -> Self {
        Row::from(
            value
                .into_iter()
                .map(|value| value.into())
                .collect::<Vec<_>>(),
        )
        .into()
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

impl Debug for OwnedRow {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write_row(f, self.as_ref())
    }
}

pub struct OwnedRowRefIter<'a> {
    values: VecDeque<&'a DbVal>,
}

impl<'a> Iterator for OwnedRowRefIter<'a> {
    type Item = &'a DbVal;

    fn next(&mut self) -> Option<Self::Item> {
        self.values.pop_front()
    }
}

impl<'a> IntoIterator for &'a OwnedRow {
    type Item = &'a DbVal;
    type IntoIter = OwnedRowRefIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        OwnedRowRefIter {
            values: self.iter().map(|s| s.as_ref()).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::data::row::Row;
    use crate::data::values::DbVal;

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
        assert_eq!(
            as_row,
            Row::from([
                DbVal::Integer(1),
                DbVal::Integer(2),
                DbVal::Integer(3),
                DbVal::Integer(4),
                DbVal::Null
            ])
        )
    }
}
