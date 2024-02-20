//! Provides cryptography stuff for this

use ::rsa::pkcs1::der::zeroize::Zeroizing;
use cfg_if::cfg_if;
use once_cell::sync::Lazy;

#[cfg(feature = "chacha20poly1305")]
mod chacha20poly1305;
#[cfg(feature = "rsa")]
mod rsa;

pub type Result<T> = std::result::Result<T, Error>;

/// Provides encryption and decryption
pub trait Provider: Sync + Send {
    /// The name of the provider
    fn name(&self) -> String;

    /// Generates a private key
    fn generate(&self) -> Result<Box<dyn PrivateKey>>;

    /// read a public key encoded in DER format
    fn read_public(&self, buffer: &[u8]) -> Result<Box<dyn PublicKey>>;

    /// read a private key encoded in DER format, with a password if required
    fn read_private(&self, buffer: &[u8], password: Option<&[u8]>) -> Result<Box<dyn PrivateKey>>;
}

/// Public key
pub trait PublicKey {

    /// The size of blocks, which is the maximum number of bits that
    /// can be encrypted at once
    fn block_size(&self) -> usize;

    fn encrypt(&self, msg: &[u8]) -> Result<Vec<u8>>;

    fn to_public_pem(&self) -> Result<String>;

    fn to_public_der(&self) -> Result<Vec<u8>>;
}

/// Private key
pub trait PrivateKey: PublicKey {

    fn to_public_key(&self) -> Box<dyn PublicKey>;

    /// Decode data
    fn decrypt(&self, buffer: &[u8]) -> Result<Vec<u8>>;

    /// To PEM format
    fn to_private_pem(&self, password: Option<&[u8]>) -> Result<Zeroizing<String>>;

    /// To der format
    fn to_private_der(&self, password: Option<&[u8]>) -> Result<Zeroizing<Vec<u8>>>;
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
    RsaError(#[from] ::rsa::Error),
}

/// All cryptography providers
pub static CRYPTOGRAPHY_PROVIDERS: Lazy<Box<[Box<dyn Provider>]>> = Lazy::new(|| {
    Box::new([
        #[cfg(feature = "chacha20poly1305")]
        Box::new(chacha20poly1305::ChaCha20Poly1305Provider::new()),
        #[cfg(feature = "rsa")]
        Box::new(rsa::RsaProvider::new(2048)),
        #[cfg(feature = "rsa")]
        Box::new(rsa::RsaProvider::new(4096)),
    ])
});

cfg_if! {
    if #[cfg(feature="rsa")] {
        pub static DEFAULT_CRYPTOGRAPHY_PROVIDER: &str = "rsa-2048";
    } else if #[cfg(feature="chacha20poly1305")] {
        pub static DEFAULT_CRYPTOGRAPHY_PROVIDER: &str = "chacha20poly1305";
    } else {
        compile_error!("Must enable at least one cryptographic algorithm")
    }
}

/// Gets a cryptography provider by name if it exists
pub fn get_crypto_provider<S: AsRef<str>>(provider: S) -> Option<&'static dyn Provider> {
    CRYPTOGRAPHY_PROVIDERS
        .iter()
        .find(|prov| prov.name() == provider.as_ref())
        .map(|p| &**p)
}

/// Gets the [DEFAULT_CRYPTOGRAPHY_PROVIDER]
pub fn cryptography_provider() -> &'static dyn Provider {
    get_crypto_provider(DEFAULT_CRYPTOGRAPHY_PROVIDER)
        .expect("default cryptography provider should always exist")
}

#[cfg(test)]
mod tests {
    use crate::{cryptography_provider, get_crypto_provider};

    #[test]
    #[cfg(feature = "chacha20poly1305")]
    fn test_get_chacha_crypto_provider() {
        let _provider = get_crypto_provider("chacha20poly1305").expect("could not get chacha");
    }

    #[test]
    #[cfg(feature = "rsa")]
    fn test_get_rsa_crypto_provider() {
        let _provider = get_crypto_provider("rsa-2048").expect("could not get rsa-2048");
        let _provider = get_crypto_provider("rsa-4096").expect("could not get rsa-4096");
    }

    #[test]
    fn test_default_crypto_provider() {
        let _provider = cryptography_provider();
    }
}
