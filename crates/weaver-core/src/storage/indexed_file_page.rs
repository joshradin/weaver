use crate::storage::abstraction::Page;
use crate::storage::ram_file::FilePage;
use crate::storage::StorageBackedData;
use std::marker::PhantomData;

/// An indexed file page allows for
#[derive(Debug)]
pub struct KeyIndexedPage<P: Page, K: StorageBackedData, V: StorageBackedData> {
    file_page: P,
    _stored: PhantomData<(K, V)>,
}
