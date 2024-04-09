use crate::data::row::{OwnedRow, Row};

use crate::data::values::DbVal;

use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::ops::{Bound, Deref, RangeBounds};
use tracing::trace;

/// Keys are always order-able
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyData(OwnedRow);

impl KeyData {}

impl Debug for KeyData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.0.len() {
            0 => panic!("key data can not be 0 length"),
            1 => self.0[0].fmt(f),
            2..=3 => self.0.fmt(f),
            _ => write!(f, "{:?}", self.0),
        }
    }
}

impl Deref for KeyData {
    type Target = OwnedRow;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a, T> From<T> for KeyData
where
    Row<'a>: From<T>,
{
    fn from(value: T) -> Self {
        KeyData(Row::from(value).to_owned())
    }
}

impl<'a> AsRef<Row<'a>> for KeyData {
    fn as_ref(&self) -> &Row<'a> {
        self.0.as_ref()
    }
}

impl IntoIterator for KeyData {
    type Item = DbVal;
    type IntoIter = <Row<'static> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        Row::from(self.0).into_iter()
    }
}

impl<'a> IntoIterator for &'a KeyData {
    type Item = &'a DbVal;
    type IntoIter = <&'a OwnedRow as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Eq, PartialEq, Clone)]
pub struct KeyDataRange(pub Bound<KeyData>, pub Bound<KeyData>);

impl<R: RangeBounds<KeyData>> From<R> for KeyDataRange {
    fn from(value: R) -> Self {
        Self(value.start_bound().cloned(), value.end_bound().cloned())
    }
}

impl KeyDataRange {
    pub fn contains(&self, key_data: &KeyData) -> bool {
        let gt = match &self.0 {
            Bound::Included(included) => key_data >= included,
            Bound::Excluded(excluded) => key_data > excluded,
            Bound::Unbounded => true,
        };

        let lt = match &self.1 {
            Bound::Included(included) => key_data <= included,
            Bound::Excluded(excluded) => key_data < excluded,
            Bound::Unbounded => true,
        };

        gt && lt
    }

    /// Checks if the given key is greater than the range given
    pub fn is_greater(&self, key_data: &KeyData) -> bool {
        match &self.0 {
            Bound::Included(included) if key_data > included => true,
            Bound::Excluded(excluded) if key_data >= excluded => true,
            _ => false,
        }
    }

    /// Checks if the given key is less than the range given
    pub fn is_less(&self, key_data: &KeyData) -> bool {
        match &self.1 {
            Bound::Included(included) if key_data < included => true,
            Bound::Excluded(excluded) if key_data <= excluded => true,
            _ => false,
        }
    }

    /// Checks if two ranges overlap
    pub fn overlaps(&self, other: &Self) -> bool {
        partial_compare_bounds(&self.0, &other.1)
            .map(|ordering| (ordering.is_le()))
            .unwrap_or(false)
            || partial_compare_bounds(&other.0, &self.1)
                .map(|ordering| (ordering.is_le()))
                .unwrap_or(false)
    }

    /// if two ranges overlap, creates a union
    pub fn union(&self, other: &Self) -> Option<Self> {
        if !self.overlaps(other) {
            return None;
        }

        let min = [&self.0, &other.0]
            .into_iter()
            .min_by(|&a, &b| compare_bounds(a, b))
            .unwrap();
        let max = [&self.1, &other.1]
            .into_iter()
            .max_by(|&a, &b| compare_bounds(a, b))
            .unwrap();

        let range = Self(min.clone(), max.clone());
        trace!(
            "combining {:15?} with {:15?} into {:15?}",
            self,
            other,
            range
        );
        Some(range)
    }

    /// If two ranges overlap, creates the intersection of the two
    pub fn intersection(&self, other: &Self) -> Option<Self> {
        if !self.overlaps(other) {
            return None;
        }

        let lower = [&self.0, &other.0]
            .into_iter()
            .max_by(|&a, &b| compare_bounds(a, b))
            .unwrap();
        let upper = [&self.1, &other.1]
            .into_iter()
            .min_by(|&a, &b| compare_bounds(a, b))
            .unwrap();

        let range = Self(lower.clone(), upper.clone());
        trace!(
            "combining {:15?} with {:15?} into {:15?}",
            self,
            other,
            range
        );
        Some(range)
    }

    pub fn start_bound(&self) -> Bound<&KeyData> {
        self.0.as_ref()
    }

    pub fn end_bound(&self) -> Bound<&KeyData> {
        self.1.as_ref()
    }
}

impl Debug for KeyDataRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Bound::Included(i) => write!(f, "[{i:?}")?,
            Bound::Excluded(i) => write!(f, "({i:?}")?,
            Bound::Unbounded => write!(f, "(")?,
        }
        write!(f, ",")?;
        match &self.1 {
            Bound::Included(i) => write!(f, "{i:?}]")?,
            Bound::Excluded(i) => write!(f, "{i:?})")?,
            Bound::Unbounded => write!(f, ")")?,
        }
        Ok(())
    }
}

fn partial_compare_bounds<T: PartialOrd>(b1: &Bound<T>, b2: &Bound<T>) -> Option<Ordering> {
    match (b1, b2) {
        (Bound::Unbounded, Bound::Unbounded) => Some(Ordering::Equal),
        (Bound::Excluded(x), Bound::Excluded(y)) => x.partial_cmp(y),
        (Bound::Included(x), Bound::Included(y)) => x.partial_cmp(y),
        (Bound::Included(x), Bound::Excluded(y)) => {
            x.partial_cmp(y)
                .map(|ord| if ord.is_eq() { Ordering::Less } else { ord })
        }
        (Bound::Excluded(x), Bound::Included(y)) => {
            x.partial_cmp(y)
                .map(|ord| if ord.is_eq() { Ordering::Greater } else { ord })
        }
        (Bound::Unbounded, _) => Some(Ordering::Less),
        (_, Bound::Unbounded) => Some(Ordering::Greater),
    }
}

fn compare_bounds<T: Ord>(b1: &Bound<T>, b2: &Bound<T>) -> Ordering {
    match (b1, b2) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Excluded(x), Bound::Excluded(y)) => x.cmp(y),
        (Bound::Included(x), Bound::Included(y)) => x.cmp(y),
        (Bound::Included(_), Bound::Excluded(_)) => todo!(),
        (Bound::Excluded(_), Bound::Included(_)) => todo!(),
        (Bound::Unbounded, _) => Ordering::Less,
        (_, Bound::Unbounded) => Ordering::Greater,
    }
}

#[cfg(test)]
mod tests {
    use crate::data::values::DbVal;
    use crate::key::{KeyData, KeyDataRange};
    use std::collections::{BTreeSet, HashSet};
    

    #[test]
    fn order_keys() {
        let mut btree = BTreeSet::<KeyData>::new();
        btree.insert(KeyData::from([DbVal::Float(4.0)]));
        btree.insert(KeyData::from([DbVal::Float(1.0)]));

        let b = btree.iter().collect::<Vec<_>>();
        assert_eq!(&*b[0][0], &DbVal::Float(1.0));
        assert_eq!(&*b[1][0], &DbVal::Float(4.0));
    }

    #[test]
    fn key_in_range() {
        let range = KeyDataRange::from(
            KeyData::from([DbVal::Float(1.0)])..=KeyData::from([DbVal::Float(4.0)]),
        );
        assert!(!range.contains(&KeyData::from([DbVal::Float(f64::MIN)])));
        assert!(range.contains(&KeyData::from([DbVal::Float(1.0)])));
        assert!(range.contains(&KeyData::from([DbVal::Float(2.0)])));
        assert!(range.contains(&KeyData::from([DbVal::Float(4.0)])));
        assert!(!range.contains(&KeyData::from([DbVal::Float(f64::MAX)])));
    }

    #[test]
    fn hash_keys() {
        let mut hash_set = HashSet::<KeyData>::new();
        hash_set.insert(KeyData::from([DbVal::Float(4.0)]));
        hash_set.insert(KeyData::from([DbVal::Float(1.0)]));

        assert!(hash_set.contains(&KeyData::from([DbVal::Float(4.0)])));
        assert!(hash_set.contains(&KeyData::from([DbVal::Float(1.0)])));
    }
}
