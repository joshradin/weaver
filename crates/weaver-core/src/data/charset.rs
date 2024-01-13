//! A charset must provide a mechanism for creating a total order over strings of a given max length
//! The default charset shall be ASCII

use std::cmp::Ordering;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct Charset {
    /// the name of the charset
    name: String,
    /// The characters in the charset mapped to their lexicographical value
    chars: BTreeMap<char, usize>,
}

impl Charset {
    /// Partial compare of string. Succeeds if all characters in l and r are in the charset.
    pub fn partial_cmp(&self, l: &str, r: &str) -> Option<Ordering> {
        todo!()
    }
}
