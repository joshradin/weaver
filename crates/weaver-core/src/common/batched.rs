//! Helpful batching functions

use std::collections::VecDeque;

/// Creates an iterator where each batch is of a given sioze
pub fn to_batches<T, I: IntoIterator<Item = T>>(
    batch_size: usize,
    iter: I,
) -> Batches<T, I::IntoIter> {
    Batches {
        iterator: iter.into_iter(),
        batch_size,
    }
}

/// Creates an iterator where each batch is of a given sioze
pub fn to_n_batches<T, I: IntoIterator<Item = T>>(
    batches: usize,
    iter: I,
) -> Batches<T, <Vec<T> as IntoIterator>::IntoIter>
{

    let iter = iter.into_iter().fuse();
    let mut all = match iter.size_hint() {
        (_, Some(max)) => {
            Vec::with_capacity(max)
        }
        (min, None) => {
            Vec::with_capacity(min)
        }
    };
    all.extend(iter);
    let batch_len = all.len() / batches;
    let extra = all.len() % batches;
    let batch_len = if extra > 0 {  batch_len + 1 } else { batch_len };
    to_batches(batch_len, all)
}

#[derive(Debug)]
pub struct Batches<T, I: Iterator<Item = T>> {
    iterator: I,
    batch_size: usize,
}

impl<T, I: Iterator<Item = T>> Iterator for Batches<T, I> {
    type Item = Batch<T>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut vector = Vec::with_capacity(self.batch_size);
        loop {
            if vector.len() >= self.batch_size {
                break;
            }
            if let Some(item) = self.iterator.next() {
                vector.push(item);
            } else {
                break;
            }
        }
        if vector.is_empty() {
            None
        } else {
            Some(Batch {
                iterator: VecDeque::from(vector),
            })
        }
    }
}

#[derive(Debug)]
pub struct Batch<T> {
    iterator: VecDeque<T>,
}

impl<T> Batch<T> {
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.iterator.iter()
    }
}

impl<T> Iterator for Batch<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.pop_front()
    }
}

impl<'a, T> IntoIterator for &'a Batch<T> {
    type Item = &'a T;
    type IntoIter = std::collections::vec_deque::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iterator.iter()
    }
}

#[cfg(test)]
mod tests {
    use crate::common::batched::to_batches;

    #[test]
    fn batched() {
        let r1000 = (0..1000).into_iter();
        let batches = to_batches(125, r1000).collect::<Vec<_>>();
        assert_eq!(batches.len(), 8);
        assert_eq!(batches[3].iter().max(), Some(&499));
    }

    #[test]
    fn to_n_batched() {
        let r1100 = (0..1100).into_iter();
        let batches = super::to_n_batches(8, r1100).collect::<Vec<_>>();
        assert_eq!(batches.len(), 8);
        let mut sum = 0;
        for batch in batches {
            let count = batch.count();
            println!("batch len: {}", count);
            sum += count;
        }
        assert_eq!(sum, 1100, "sum must be 1100, but got {sum} instead");
    }
}
