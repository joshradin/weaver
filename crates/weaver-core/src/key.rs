use crate::data::row::{OwnedRow, Row};
use crate::data::values::Value;
use derive_more::From;
use std::cmp::Ordering;
use std::ops::{Bound, Deref, RangeBounds};

/// Keys are always order-able
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Hash)]
pub struct KeyData(OwnedRow);
impl Ord for KeyData {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).expect("could not compare keys")
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
    type Item = Value;
    type IntoIter = <Row<'static> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        Row::from(self.0).into_iter()
    }
}

impl<'a> IntoIterator for &'a KeyData {
    type Item = &'a Value;
    type IntoIter = <&'a OwnedRow as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct KeyDataRange(pub Bound<KeyData>, pub Bound<KeyData>);

impl<R: RangeBounds<KeyData>> From<R> for KeyDataRange {
    fn from(value: R) -> Self {
        Self(value.start_bound().cloned(), value.end_bound().cloned())
    }
}

impl KeyDataRange {
    pub fn contains(&self, key_data: &KeyData) -> bool {
        match &self.0 {
            Bound::Included(included) => {
                if key_data < included {
                    return false;
                }
            }
            Bound::Excluded(excluded) => {
                if key_data <= excluded {
                    return false;
                }
            }
            Bound::Unbounded => {}
        }

        match &self.1 {
            Bound::Included(included) => {
                if key_data > included {
                    return false;
                }
            }
            Bound::Excluded(excluded) => {
                if key_data >= excluded {
                    return false;
                }
            }
            Bound::Unbounded => {}
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use crate::data::values::Value;
    use crate::key::{KeyData, KeyDataRange};
    use std::collections::{BTreeSet, HashSet};

    #[test]
    fn order_keys() {
        let mut btree = BTreeSet::<KeyData>::new();
        btree.insert(KeyData::from([Value::Float(4.0)]));
        btree.insert(KeyData::from([Value::Float(1.0)]));

        let b = btree.iter().collect::<Vec<_>>();
        assert_eq!(&*b[0][0], &Value::Float(1.0));
        assert_eq!(&*b[1][0], &Value::Float(4.0));
    }

    #[test]
    fn key_in_range() {
        let range = KeyDataRange::from(
            KeyData::from([Value::Float(1.0)])..=KeyData::from([Value::Float(4.0)]),
        );
        assert!(!range.contains(&KeyData::from([Value::Float(f64::MIN)])));
        assert!(range.contains(&KeyData::from([Value::Float(1.0)])));
        assert!(range.contains(&KeyData::from([Value::Float(2.0)])));
        assert!(range.contains(&KeyData::from([Value::Float(4.0)])));
        assert!(!range.contains(&KeyData::from([Value::Float(f64::MAX)])));
    }

    #[test]
    fn hash_keys() {
        let mut hash_set = HashSet::<KeyData>::new();
        hash_set.insert(KeyData::from([Value::Float(4.0)]));
        hash_set.insert(KeyData::from([Value::Float(1.0)]));

        assert!(hash_set.contains(&KeyData::from([Value::Float(4.0)])));
        assert!(hash_set.contains(&KeyData::from([Value::Float(1.0)])));
    }
}
