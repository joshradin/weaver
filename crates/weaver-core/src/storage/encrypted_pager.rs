//! Allows for encrypted paging. Encrypted paging stores data in pages encrypted, requiring
//! some private key to read/write to the page


use crate::storage::Pager;

#[derive(Debug)]
pub struct EncryptedPager<P : Pager> {
}