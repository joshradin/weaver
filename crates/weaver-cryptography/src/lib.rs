//! Provides cryptography stuff for this

use ::rsa::pkcs1::der::zeroize::Zeroizing;
use once_cell::sync::Lazy;
use pkcs8::SecretDocument;

#[cfg(feature = "chacha20poly1305")]
mod chacha20poly1305;
#[cfg(feature = "rsa")]
mod rsa;

pub type Result<T> = std::result::Result<T, Error>;

/// Provides encryption and decryption
pub trait Provider: Sync + Send {
    /// The name of the provider
    fn name(&self) -> &'static str;

    /// Generates a private key
    fn generate(&self, block_size: usize) -> Result<Box<dyn PrivateKey>>;

    /// read a public key encoded in DER format
    fn read_public(&self, buffer: &[u8]) -> Result<Box<dyn PublicKey>>;

    /// read a private key encoded in DER format, with a password if required
    fn read_private(&self, buffer: &[u8], password: Option<&[u8]>) -> Result<Box<dyn PrivateKey>>;
}

/// Public key
pub trait PublicKey {
    fn encrypt(&self, buffer: &[u8]) -> Result<Vec<u8>>;

    /// To PEM format
    fn to_pem(&self, password: Option<&[u8]>) ->  Result<Zeroizing<String>> ;

    /// To der format
    fn to_der(&self, password: Option<&[u8]>) ->  Result<Zeroizing<Vec<u8>>> ;
}

/// Private key
pub trait PrivateKey : PublicKey {

    /// Decode data
    fn decrypt(&self, buffer: &[u8]) -> Result<Vec<u8>>;
}

/// A cryptographic error occurred
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Unexpected EOF")]
    UnexpectedEof,
    #[error(transparent)]
    Pkcs8Error(#[from] pkcs8::Error),
    #[error(transparent)]
    Pkcs8SpkiError(#[from] pkcs8::spki::Error),
    #[cfg(feature = "rsa")]
    #[error(transparent)]
    RsaError(#[from] ::rsa::Error)
}

/// All cryptography providers
pub static CRYPTOGRAPHY_PROVIDERS: Lazy<Box<[Box<dyn Provider>]>> = Lazy::new(|| {
    Box::new([
        #[cfg(feature = "chacha20poly1305")]
        Box::new(chacha20poly1305::ChaCha20Poly1305Provider::new()),
        #[cfg(feature = "rsa")]
        Box::new(rsa::RsaProvider::new()),
    ])
});

/// Gets a cryptography provider by name if it exists
pub fn crypto_provider<S: AsRef<str>>(provider: S) -> Option<&'static dyn Provider> {
    CRYPTOGRAPHY_PROVIDERS
        .iter()
        .find(|prov| prov.name() == provider.as_ref())
        .map(|p| &**p)
}

#[cfg(test)]
mod tests {
    use crate::crypto_provider;

    #[test]
    #[cfg(feature = "chacha20poly1305")]
    fn test_get_crypto_provider() {
        let _provider = crypto_provider("chacha20poly1305").expect("could not get chacha");
    }
}
