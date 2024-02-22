//! Pretty bytes

use std::fmt::{Binary, Debug, Display, Formatter, LowerHex, UpperHex};

/// Pretty byte output

pub struct PrettyBytes<'a>(pub &'a [u8]);

impl<'a> Display for PrettyBytes<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Debug for PrettyBytes<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "'")?;
        let min = self.0.len().min(128);
        for &byte in &self.0[..min] {
            if byte.is_ascii_alphanumeric() || byte.is_ascii_punctuation() {
                let as_char: char = byte.into();
                write!(f, "{}", as_char)?;
            } else {
                write!(f, ".")?;
            }
        }
        write!(f, "'")
    }
}

impl Binary for PrettyBytes<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "b'")?;
        let min = self.0.len().min(128);
        for &byte in &self.0[..min] {
            write!(f, "{byte:b}",)?;
        }
        write!(f, "'")
    }
}

impl LowerHex for PrettyBytes<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "x'")?;
        let min = self.0.len().min(128);
        for &byte in &self.0[..min] {
            write!(f, "{byte:x}",)?;
        }
        write!(f, "'")
    }
}

impl UpperHex for PrettyBytes<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "X'")?;
        let min = self.0.len().min(128);
        for &byte in &self.0[..min] {
            write!(f, "{byte:X}",)?;
        }
        write!(f, "'")
    }
}
