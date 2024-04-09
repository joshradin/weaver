//! A charset must provide a mechanism for creating a total order over strings of a given max length
//! The default charset shall be ASCII

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

/// A collation defines some of characters and gives it a lexicographical order
#[derive(Debug)]
pub struct Collation {
    /// the name of the collation
    name: String,
    /// The characters in the collation mapped to their lexicographical value
    chars: BTreeMap<char, u32>,
}

impl Collation {

    /// Creates a new collation
    pub fn new<S : AsRef<str>, I : IntoIterator<Item=(char, u32)>>(name: S, mapping: I) -> Self {
        Self {
            name: name.as_ref().to_string(),
            chars: mapping.into_iter().collect(),
        }
    }

    /// Gets the lexicographical value of a char, if present within the collation
    pub fn to_lexicographical_value(&self, c: char) -> Option<u32> {
        self.chars.get(&c).map(|&s| s)
    }

    /// Converts a string into an iterator of the given lexicographical values.
    ///
    /// # Panic
    /// Panics if the given string is not within this given collation
    pub fn to_lexicographical_iter<'s>(&'s self, s: &'s str) -> impl Iterator<Item = u32> + 's {
        if !self.in_charset(s) {
            panic!("{} not in collation {}", s, self)
        }

        s.chars().flat_map(|c| self.to_lexicographical_value(c))
    }

    /// Checks if a given string is within this collation, meaning all chars are assigned a lexicographical value
    pub fn in_charset(&self, string: &str) -> bool {
        string.chars().all(|ref c| self.chars.contains_key(c))
    }

    /// Checks if two strings are equal in a given collation
    pub fn partial_eq(&self, l: &str, r: &str) -> bool {
        if l.len() != r.len() || !self.in_charset(l) || !self.in_charset(r) {
            return false;
        }

        self.to_lexicographical_iter(l)
            .zip(self.to_lexicographical_iter(r))
            .all(|(l, r)| l == r)
    }

    /// Partial compare of string. Can only succeed if all characters in l and r are in the collation.
    pub fn partial_cmp(&self, l: &str, r: &str) -> Option<Ordering> {
        if !self.in_charset(l) || !self.in_charset(r) {
            return None;
        }

        let mut l = self.to_lexicographical_iter(l);
        let mut r = self.to_lexicographical_iter(r);

        loop {
            match (l.next(), r.next()) {
                (Some(l), Some(r)) => {
                    match l.cmp(&r) {
                        Ordering::Less => return Some(Ordering::Less),
                        Ordering::Greater => return Some(Ordering::Greater),
                        _ => {}
                    }
                }
                (Some(_), None) => {
                    return Some(Ordering::Greater)
                }
                (None, Some(_)) => {
                    return Some(Ordering::Less)
                }
                (None, None) => {
                    return Some(Ordering::Equal)
                }
            }

        }
    }
}

/// Default charsets
impl Collation {
    pub fn utf8() -> Self {
        Collation::new(
            "utf8",
            []
        )
    }
}

impl Display for Collation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}
