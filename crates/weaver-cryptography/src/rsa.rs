//! Provides RSA


use pkcs8::der::zeroize::Zeroizing;
use pkcs8::LineEnding;
use rand::thread_rng;
use rsa::{BigUint, Pkcs1v15Encrypt, pkcs8::{DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey}, RsaPrivateKey, RsaPublicKey};
use rsa::traits::PublicKeyParts;

use crate::{PrivateKey, Provider, PublicKey, Result};

/// Rsa provider for cryptography
pub struct RsaProvider { bit_size: usize }

impl RsaProvider {
    pub fn new(bit_size: usize) -> Self {
        Self { bit_size }
    }
}

impl Provider for RsaProvider {
    fn name(&self) -> String {
        format!("rsa-{}", self.bit_size)
    }

    fn generate(&self) -> Result<Box<dyn PrivateKey>> {
        Ok(Box::new(RsaPrivateKey::new(
            &mut thread_rng(),
            self.bit_size,
        )?))
    }

    fn read_public(&self, buffer: &[u8]) -> Result<Box<dyn PublicKey>> {
        Ok(Box::new(RsaPublicKey::from_public_key_der(buffer)?))
    }

    fn read_private(&self, buffer: &[u8], password: Option<&[u8]>) -> Result<Box<dyn PrivateKey>> {
        match password {
            None => Ok(Box::new(RsaPrivateKey::from_pkcs8_der(buffer)?)),
            Some(password) => Ok(Box::new(RsaPrivateKey::from_pkcs8_encrypted_der(
                buffer, password,
            )?)),
        }
    }
}

impl PublicKey for RsaPublicKey {
    fn block_size(&self) -> usize {
        self.n().bits()
    }


    fn encrypt(&self, msg: &[u8]) -> Result<Vec<u8>> {
        Ok(self.encrypt(&mut thread_rng(), Pkcs1v15Encrypt, msg)?)
    }

    fn to_public_pem(&self) -> Result<String> {
        Ok(self.to_public_key_pem(LineEnding::LF)?)
    }

    fn to_public_der(&self) -> Result<Vec<u8>> {
        Ok(self.to_public_key_der()?.as_bytes().to_vec())
    }
}

impl PublicKey for RsaPrivateKey {
    fn block_size(&self) -> usize {
        self.to_public_key().block_size()
    }


    fn encrypt(&self, msg: &[u8]) -> Result<Vec<u8>> {
        Ok(self
            .to_public_key()
            .encrypt(&mut thread_rng(), Pkcs1v15Encrypt, msg)?)
    }

    fn to_public_pem(&self) -> Result<String> {
        PublicKey::to_public_pem(&self.to_public_key())
    }

    fn to_public_der(&self) -> Result<Vec<u8>> {
        PublicKey::to_public_der(&self.to_public_key())
    }
}

impl PrivateKey for RsaPrivateKey {
    fn to_public_key(&self) -> Box<dyn PublicKey> {
        Box::new(self.to_public_key())
    }


    fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        Ok(RsaPrivateKey::decrypt(self, Pkcs1v15Encrypt, ciphertext)?)
    }

    fn to_private_pem(&self, password: Option<&[u8]>) -> Result<Zeroizing<String>> {
        Ok(match password {
            None => { self.to_pkcs8_pem(LineEnding::LF)? }
            Some(password) => { self.to_pkcs8_encrypted_pem(&mut thread_rng(), password, LineEnding::LF)? }
        })
    }

    fn to_private_der(&self, password: Option<&[u8]>) -> Result<Zeroizing<Vec<u8>>> {
        Ok(match password {
            None => { self.to_pkcs8_der()?.to_bytes() }
            Some(password) => { self.to_pkcs8_encrypted_der(&mut thread_rng(), password)?.to_bytes() }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::Provider;
    use crate::rsa::RsaProvider;

    #[test]
    fn encrypt_decrypt() {
        let provider = RsaProvider::new(256);
        let private_key = provider.generate().unwrap();

        let message = private_key
            .decrypt(
                &private_key
                    .encrypt(b"hello, world")
                    .expect("could not encrypt"),
            )
            .expect("could not decrypt");
        assert_eq!(message, b"hello, world");
    }

    #[test]
    fn password_protected() {
        let provider = RsaProvider::new(256);
        let password = Some(b"hunter2" as &[u8]);
        let private_key_der = {
            let private_key = provider.generate().unwrap();
            private_key.to_private_der(password).expect("could not get private key")
        };

        let _private_key = provider.read_private(&*private_key_der, password)
            .expect("could not read from encrypted der");
    }
}
