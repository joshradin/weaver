//! Iter extension trait

use crate::common::batched::{to_batches, Batches};

/// Custom iterator extension trait
pub trait IteratorExt: Iterator + Sized {
    /// Split into batches of at most this size
    fn batches(self, batch_size: usize) -> Batches<Self> {
        to_batches(batch_size, self)
    }
}

impl<I: Iterator> IteratorExt for I {}
