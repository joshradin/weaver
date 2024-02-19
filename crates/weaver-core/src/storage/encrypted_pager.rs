//! Allows for encrypted paging. Encrypted paging stores data in pages encrypted, requiring
//! some private key to read/write to the page


use crate::storage::Pager;

/// An encrypted pager requires a key to read pages.
///
/// Encryption shall be generalized via an encryption key and a decryption key, which can
/// either be symmetric or asymmetric.
///
/// If using asymmetric, the data should be encrypted using the public key and decrypted with
/// the private key.
#[derive(Debug)]
pub struct EncryptedPager<P : Pager> {
    backing: P
}

/// A public key can only encrypt data.
pub trait PublicKey {
    fn encrypt(&self, data: &[u8], buffer: &mut [u8]);
}

/// A private key can decrypt and encrypt (through the [PublicKey] trait) data
pub trait PrivateKey : PublicKey {
    fn decrypt(&self, );
}