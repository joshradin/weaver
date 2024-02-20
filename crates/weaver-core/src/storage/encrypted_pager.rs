//! Allows for encrypted paging. Encrypted paging stores data in pages encrypted, requiring
//! some private key to read/write to the page

use std::marker::PhantomData;

use weaver_cryptography::{PrivateKey, PublicKey};

use crate::error::Error;
use crate::storage::abstraction::{Page, PageMut};
use crate::storage::Pager;

/// An encrypted pager requires a key to read pages.
///
/// Encryption shall be generalized via an encryption key and a decryption key, which can
/// either be symmetric or asymmetric.
///
/// If using asymmetric, the data should be encrypted using the public key and decrypted with
/// the private key.
pub struct EncryptedPager<P: Pager> {
    backing: P,
    public_key: Box<dyn PublicKey>,
}

impl<P: Pager> EncryptedPager<P> {
    pub fn new(pager: P, public_key: Box<dyn PublicKey>) -> Self {
        Self {
            backing: pager,
            public_key,
        }
    }
}

impl<P: Pager> Pager for EncryptedPager<P> {
    type Page<'a> = EncryptedPage<'a, P::Page<'a>> where P: 'a;
    type PageMut<'a> = EncryptedPageMut<'a, P::PageMut<'a>> where P: 'a;
    type Err = Error;

    fn page_size(&self) -> usize {
        let block_size = self.public_key.block_size();
        (self.backing.page_size() / block_size) * block_size
    }

    fn get(&self, index: usize) -> Result<Self::Page<'_>, Self::Err> {
        todo!()
    }

    fn get_mut(&self, index: usize) -> Result<Self::PageMut<'_>, Self::Err> {
        todo!()
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

#[derive(Debug)]
pub struct EncryptedPage<'a, P: Page<'a>> {
    page: P,
    _lf: PhantomData<&'a ()>,
}

impl<'a, P: Page<'a>> EncryptedPage<'a, P> {
    pub fn unlock(&self, key: &'a dyn PrivateKey) -> Result<DecryptedPage<'a, P>, Error> {

    }
}

impl<'a, P: Page<'a>> Page<'a> for EncryptedPage<'a, P> {
    fn len(&self) -> usize {
        panic!("must provide key")
    }

    fn as_slice(&self) -> &[u8] {
        panic!("must provide key")
    }
}

#[derive(Debug)]
pub struct EncryptedPageMut<'a, P: PageMut<'a>> {
    page: P,
    _lf: PhantomData<&'a ()>,
}

impl<'a, P: PageMut<'a>> Page<'a> for EncryptedPageMut<'a, P> {
    fn len(&self) -> usize {
        todo!()
    }

    fn as_slice(&self) -> &[u8] {
        todo!()
    }
}

impl<'a, P: PageMut<'a>> PageMut<'a> for EncryptedPageMut<'a, P> {

    fn as_mut_slice(&mut self) -> &mut [u8] {
        todo!()
    }
}

#[derive(Debug)]
pub struct DecryptedPage<'a, P : Page<'a>> {
    backing: &'a P,
    private_key: &'a dyn PrivateKey
}

#[cfg(test)]
mod tests {
    use weaver_cryptography::cryptography_provider;
    use crate::storage::encrypted_pager::{EncryptedPage, EncryptedPageMut, EncryptedPager};
    use crate::storage::{Pager, VecPager};

    #[test]
    fn encrypted_pager() {
        let pkey = cryptography_provider().generate().expect("could not generate");
        let pager = EncryptedPager::new(VecPager::default(), pkey.to_public_key());
        let (page, _): (EncryptedPageMut<_>, _) = pager.new().unwrap();

        let mut page = page.unlock(&pkey);


    }
}
