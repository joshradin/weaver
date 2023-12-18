//! Pretty bytes

use std::fmt::{Display, Formatter};

/// Pretty byte output
#[derive(Debug)]
pub struct PrettyBytes<'a>(pub &'a [u8]);

impl Display for PrettyBytes<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        let min = self.0.len().min(128);
        for &byte in &self.0[..min] {
            if byte.is_ascii_alphanumeric() || byte.is_ascii_punctuation() {
                let as_char: char = byte.into();
                write!(f, "{}", as_char)?;
            } else {
                write!(f, ".")?;
            }
        }
        write!(f, "]")
    }
}
