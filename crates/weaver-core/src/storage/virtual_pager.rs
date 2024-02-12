//! Allow for a "virtual" address space, where many different paged objects are actually
//! stored on a single paged system

use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap};
use std::collections::hash_map::Entry;
use std::hash::Hash;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::dynamic_table::StorageError;
use crate::storage::Pager;

/// A virtual paged table provides a level of indirection
#[derive(Debug)]
pub struct VirtualPagerTable<K: Eq + Hash, P: Pager> {
    shared: Arc<VirtualPagerShared<K, P>>,
}

impl<K: Eq + Hash, P: Pager> VirtualPagerTable<K, P> {
    pub fn new(paged: P) -> Result<Self, P::Err> {
        Ok(Self {
            shared: Arc::new(VirtualPagerShared::open(paged)?),
        })
    }

    /// Initializes a new member of the virtual pager table
    pub fn init(&self, key: K) {
        let mut paged_id_to_offsets = self.shared.paged_id_to_offsets.write();
        if let Entry::Vacant(vacant) = paged_id_to_offsets.entry(key) {
            vacant.insert(Default::default());
        }
    }
    pub fn get<'a, Q: ?Sized>(&self, key: &'a Q) -> Option<VirtualPager<'a, Q, K, P>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        if !self.shared.paged_id_to_offsets.read().contains_key(key) {
            None
        } else {
            Some(
                VirtualPager {
                    paged_id: key,
                    parent: self.shared.clone()
                }
            )
        }
    }
}

#[derive(Debug)]
struct VirtualPagerShared<K: Eq + Hash, P: Pager> {
    paged: P,
    page_len: usize,
    paged_id_to_offsets: RwLock<HashMap<K, BTreeMap<usize, usize>>>,
}

impl<K: Eq + Hash, P: Pager> VirtualPagerShared<K, P> {
    fn open(paged: P) -> Result<Self, P::Err> {
        if paged.len() == 0 {
            // init
            let (mut page0, _) = paged.new()?;

        } else {
            // read from existing
            let root = paged.get(0)?;
        }

        todo!()
    }



    fn get_page<Q: ?Sized>(&self, key: &Q, page: usize) -> Result<P::Page<'_>, P::Err>
    where
        Q: Eq + Hash,
        K: Borrow<Q>,
    {
        let table = self.paged_id_to_offsets.read();
        let actual = table;

        todo!()
    }

    fn get_page_mut<Q: ?Sized>(&self, key: &Q, page: usize) -> Result<P::PageMut<'_>, P::Err>
        where
            Q: Eq + Hash,
            K: Borrow<Q>,
    {
        let table = self.paged_id_to_offsets.read();
        let actual = table;

        todo!()
    }
}

#[derive(Debug)]
pub struct VirtualPager<'a, Q, K, P>
where
    Q: Eq + Hash + ?Sized,
    K: Borrow<Q> + Eq + Hash,
    P: Pager,
{
    paged_id: &'a Q,
    parent: Arc<VirtualPagerShared<K, P>>,
}

impl<Q, K, P> Pager for VirtualPager<'_, Q, K, P>
where
    Q: Eq + Hash + ?Sized,
    K: Borrow<Q> + Eq + Hash,
    P: Pager,
{
    type Page<'a> = P::Page<'a> where P: 'a, K: 'a, Self : 'a;
    type PageMut<'a> = P::PageMut<'a> where P: 'a, K: 'a,  Self : 'a;
    type Err = StorageError;

    fn page_size(&self) -> usize {
        self.parent.page_len
    }

    fn get(&self, index: usize) -> Result<Self::Page<'_>, Self::Err> {
        self.parent
            .get_page(&self.paged_id, index)
            .map_err(|e| StorageError::custom(e))
    }

    fn get_mut(&self, index: usize) -> Result<Self::PageMut<'_>, Self::Err> {
        self.parent
            .get_page_mut(self.paged_id, index)
            .map_err(|e| StorageError::custom(e))
    }

    fn new(&self) -> Result<(Self::PageMut<'_>, usize), Self::Err> {
        todo!()
    }

    fn free(&self, index: usize) -> Result<(), Self::Err> {
        todo!()
    }

    fn len(&self) -> usize {
        todo!()
    }

    fn reserved(&self) -> usize {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::VecPager;
    use crate::storage::virtual_pager::VirtualPagerTable;

    #[test]
    fn virtual_pager_table() {
        let vp_table = VirtualPagerTable::<String, _>::new(VecPager::default()).unwrap();
        let one = vp_table.get("one");
        assert!(one.is_none());
        vp_table.init("one".to_string());
        let one = vp_table.get("one").expect("should exist now");
    }
}
