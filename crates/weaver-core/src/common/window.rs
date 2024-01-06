//! Provides a window iterator

use std::ops::RangeBounds;

#[derive(Debug)]
pub struct ConstWindow<I : Iterator, const N: usize>
    where I::Item : Clone
{
    iter: I,
    window: Option<[I::Item; N]>
}

impl<I: Iterator, const N: usize> Iterator for ConstWindow<I, N> where I::Item: Clone {
    type Item = [I::Item; N];

    fn next(&mut self) -> Option<Self::Item> {
        if N == 1 {
            return self.iter.next().and_then(|item| [item].to_vec().try_into().ok())
        }

        match &mut self.window {
            None => {
                let mut vec = Vec::with_capacity(N);
                for _ in 0..N {
                    vec.push(self.iter.next()?);
                }
                let emit: [I::Item; N] = vec.try_into().ok()?;
                self.window = Some(emit.clone());
                Some(emit)
            },
            Some(prev_window) => {
                let next = self.iter.next()?;
                clone_within(prev_window, 1.., 0);
                *prev_window.last_mut().unwrap() = next;
                Some(prev_window.clone())
            }
        }
    }
}

/// Creates a windowed iterator over iter
pub fn windowed<I: IntoIterator, const N : usize>(iter: I) -> ConstWindow<I::IntoIter, N>
    where I::Item : Clone
{
    ConstWindow::new(iter.into_iter())
}

/// Works identically as a normal slices [copy_within](primitive.slice.copy_within) but using clone.
pub fn clone_within<T : Clone, R : RangeBounds<usize>>(slice: &mut [T], range: R, dest: usize) {
    let cloned = slice[(range.start_bound().cloned(), range.end_bound().cloned())].to_vec();
    slice[dest..][..cloned.len()].clone_from_slice(&cloned[..]);
}

impl<I: Iterator, const N: usize> ConstWindow<I, N>
    where I::Item: Clone{

    /// Creates a new const window iterator.
    ///
    /// # Panic
    /// Panics if N == 0
    pub fn new(iter: I) -> Self {
        if N == 0 {
            panic!("can not have windows of 0-length")
        }
        Self {
            iter,
            window: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn windows() {
        let vec = vec![1, 2, 3, 4];
        let windows = windowed::<_, 2>(&vec);
        let windows = windows.collect::<Vec<_>>();
        assert_eq!(windows.len(), 3);
        assert_eq!(windows[0], [&1, &2]);
        assert_eq!(windows[1], [&2, &3]);
        assert_eq!(windows[2], [&3, &4]);

        let windows = windowed::<_, 3>(&vec);
        let windows = windows.collect::<Vec<_>>();
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0], [&1, &2, &3]);
        assert_eq!(windows[1], [&2, &3, &4]);
    }
}
