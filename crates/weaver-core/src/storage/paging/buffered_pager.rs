//! The buffered pager has buffers writes to disk, calling the `flush()` method on drop

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::error::WeaverError;
use crate::monitoring::{Monitor, Monitorable};
use crate::storage::paging::traits::{Page, PageMut};
use crate::storage::Pager;

/// A pager which buffers writes and caches reads
#[derive(Debug)]
pub struct BufferedPager<P: Pager> {
    buffered: P,
    buffers: Arc<RwLock<HashMap<usize, Box<[u8]>>>>,
    usage: RwLock<HashMap<usize, Arc<AtomicIsize>>>,
}

impl<P: Pager> BufferedPager<P> {
    pub fn new(buffered: P) -> Self {
        Self {
            buffered,
            buffers: Default::default(),
            usage: Default::default(),
        }
    }
}

impl<P: Pager> Drop for BufferedPager<P> {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

impl<P: Pager> Monitorable for BufferedPager<P> {
    fn monitor(&self) -> Box<dyn Monitor> {
        self.buffered.monitor()
    }
}

impl<P: Pager> Pager for BufferedPager<P> {
    type Page<'a> = BufferedPage where P: 'a;
    type PageMut<'a> = BufferedPageMut where P: 'a;
    type Err = WeaverError;

    fn page_size(&self) -> usize {
        self.buffered.page_size()
    }

    fn get(&self, index: usize) -> Result<Self::Page<'_>, Self::Err> {
        let token = self.usage.write().entry(index).or_default().clone();
        token
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |old| {
                if old >= 0 {
                    Some(old + 1)
                } else {
                    None
                }
            })
            .map_err(|used| {
                WeaverError::caused_by(
                    "Failed to get page",
                    io::Error::new(
                        ErrorKind::WouldBlock,
                        format!(
                            "Would block, page at offset {} already in use (used: {used})",
                            index
                        ),
                    ),
                )
            })?;

        match self.buffers.write().entry(index) {
            Entry::Occupied(occ) => Ok(BufferedPage {
                usage: token,
                slice: occ.get().clone(),
            }),
            Entry::Vacant(vacant) => {
                let page = self
                    .buffered
                    .get(index)
                    .map_err(|e| WeaverError::caused_by("backing pager failed", e))?;
                let slice: Box<[u8]> = Box::from(page.as_slice());
                vacant.insert(slice.clone());
                Ok(BufferedPage {
                    usage: token,
                    slice,
                })
            }
        }
    }

    fn get_mut(&self, index: usize) -> Result<Self::PageMut<'_>, Self::Err> {
        let token = self.usage.write().entry(index).or_default().clone();
        token
            .compare_exchange(0, -1, Ordering::SeqCst, Ordering::Relaxed)
            .map_err(|val| {
                WeaverError::caused_by(
                    "Failed to get page",
                    io::Error::new(
                        ErrorKind::WouldBlock,
                        format!(
                            "Would block, page at offset {} already in use (used: {val})",
                            index
                        ),
                    ),
                )
            })?;

        match self.buffers.write().entry(index) {
            Entry::Occupied(occ) => Ok(BufferedPageMut {
                buffers: self.buffers.clone(),
                usage: token,
                index,
                slice: occ.get().clone(),
            }),
            Entry::Vacant(vacant) => {
                let page = self
                    .buffered
                    .get(index)
                    .map_err(|e| WeaverError::caused_by("backing pager failed", e))?;
                let slice: Box<[u8]> = Box::from(page.as_slice());
                vacant.insert(slice.clone());
                Ok(BufferedPageMut {
                    buffers: self.buffers.clone(),
                    usage: token,
                    index,
                    slice,
                })
            }
        }
    }

    fn new_page(&self) -> Result<(Self::PageMut<'_>, usize), Self::Err> {
        let (new_page, index) = self
            .buffered
            .new_page()
            .map_err(|e| WeaverError::caused_by("backing pager failed", e))?;
        let slice = Box::from(new_page.as_slice());

        let buffers = self.buffers.clone();
        let usage = self
            .usage
            .write()
            .entry(index)
            .or_insert_with(|| Arc::new(AtomicIsize::new(-1)))
            .clone();

        Ok((
            BufferedPageMut {
                buffers,
                usage,
                index,
                slice,
            },
            index,
        ))
    }

    fn free(&self, index: usize) -> Result<(), Self::Err> {
        self.buffers.write().remove(&index);
        self.buffered
            .free(index)
            .map_err(|e| WeaverError::caused_by("backing pager failed", e))?;
        Ok(())
    }

    fn allocated(&self) -> usize {
        self.buffered.allocated()
    }

    fn reserved(&self) -> usize {
        self.buffered.reserved()
    }

    fn flush(&self) -> Result<(), Self::Err> {
        let mut buffers = self.buffers.write();
        for (page_offset, bytes) in buffers.drain() {
            let mut page = self
                .buffered
                .get_mut(page_offset)
                .map_err(|e| WeaverError::caused_by("backing pager failed", e))?;
            page.as_mut_slice().copy_from_slice(&bytes);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct BufferedPage {
    usage: Arc<AtomicIsize>,
    slice: Box<[u8]>,
}

impl Drop for BufferedPage {
    fn drop(&mut self) {
        self.usage.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Page<'_> for BufferedPage {
    fn len(&self) -> usize {
        self.slice.len()
    }

    fn as_slice(&self) -> &[u8] {
        &self.slice
    }
}

#[derive(Debug)]
pub struct BufferedPageMut {
    buffers: Arc<RwLock<HashMap<usize, Box<[u8]>>>>,
    usage: Arc<AtomicIsize>,
    index: usize,
    slice: Box<[u8]>,
}

impl Drop for BufferedPageMut {
    fn drop(&mut self) {
        self.buffers.write().insert(self.index, self.slice.clone());
        if self
            .usage
            .compare_exchange(-1, 0, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            panic!("atomic usage token should be -1")
        }
    }
}

impl Page<'_> for BufferedPageMut {
    fn len(&self) -> usize {
        self.slice.len()
    }

    fn as_slice(&self) -> &[u8] {
        &self.slice
    }
}

impl PageMut<'_> for BufferedPageMut {
    fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.slice
    }
}
