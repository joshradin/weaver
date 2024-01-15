//! Utilities for working with linked lists


use std::cmp::Ordering;
use std::collections::LinkedList;
use crate::common::window::windowed;

/// Checks if this list is sorted
pub fn is_sorted<T : Ord>(list: &LinkedList<T>) -> bool {
    windowed::<_, 2>(list)
        .all(|[a, b]| {
            a <= b
        })
}
pub fn sort<T : Ord>(list: &mut LinkedList<T>) {
    sort_by(list, T::cmp)
}

pub fn sort_by<T, F : Fn(&T, &T) -> Ordering>(list: &mut LinkedList<T>, comparator: F) {
    let mut vector = Vec::from_iter(list.split_off(0));
    vector.sort_by(
        |l, r| {
            comparator(l, r)
        }
    );
    list.extend(vector)
}
pub fn sort_by_cached_key<T, K: Ord, F : Fn(&T) -> K>(list: &mut LinkedList<T>, key_gen: F) {
    let mut vector = Vec::from_iter(list.split_off(0));
    vector.sort_by_cached_key(
        |v| {
            key_gen(v)
        }
    );
    list.extend(vector)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_is_sorted() {
        assert!(!is_sorted(&LinkedList::from_iter([5,3,4,1,2])));
        assert!(is_sorted(&LinkedList::from_iter([1, 2, 3, 3, 4, 5, 5])));
    }
    #[test]
    fn sort_linked_list() {
        let mut list = LinkedList::from_iter([5,3,4,1,2]);
        sort(&mut list);
        assert!(is_sorted(&list));
    }
}