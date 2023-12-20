//! Spans

use std::ops::{Index, Range, RangeInclusive, RangeToInclusive};

/// A span
#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Span(pub usize, pub usize);

impl Span {
    pub fn from_len(len: usize) -> Span {
        Self(0, len)
    }

    /// Slices a string at the span, if possible
    pub fn slice<'a>(&self, s: &'a str) -> Option<&'a str> {
        s.get(self.to_range())
    }
    pub fn join(self, other: Self) -> Self {
        Self(self.0.min(other.0), self.1.max(other.1))
    }

    /// Converts this span to a range
    pub fn to_range(self) -> Range<usize> {
        (self.0)..(self.1)
    }

    /// Offset the span, with a saturating add/subtraction
    pub fn offset(&mut self, alter: isize) {
        match alter {
            0 => {}
            isize::MIN..=-1 => {
                self.0 = self.0.saturating_sub(alter as usize);
                self.1 = self.1.saturating_sub(alter as usize);
            }
            1..=isize::MAX => {
                self.0 = self.0.saturating_add(alter as usize);
                self.1 = self.1.saturating_add(alter as usize);
            }
            _ => unreachable!("all patterns should be covered"),
        }
    }
}
