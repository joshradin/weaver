//! Provides RSA

use pkcs8::der::zeroize::Zeroizing;
use pkcs8::LineEnding;
use rand::thread_rng;
use rsa::{
    Pkcs1v15Encrypt,
    pkcs8::{DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey}, RsaPrivateKey, RsaPublicKey,
};

use crate::{PrivateKey, Provider, PublicKey, Result};

/// Rsa provider for cryptography
pub struct RsaProvider;

impl RsaProvider {
    pub fn new() -> Self {
        Self {}
    }
}

impl Provider for RsaProvider {
    fn name(&self) -> &'static str {
        todo!()
    }

    fn generate(&self, block_size: usize) -> Result<Box<dyn PrivateKey>> {
        Ok(Box::new(RsaPrivateKey::new(
            &mut rand::thread_rng(),
            block_size,
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
    fn encrypt(&self, buffer: &[u8]) -> Result<Vec<u8>> {
        todo!()
    }

    fn to_pem(&self, password: Option<&[u8]>) -> Result<Zeroizing<String>> {
        todo!()
    }

    fn to_der(&self, password: Option<&[u8]>) -> Result<Zeroizing<Vec<u8>>> {
        todo!()
    }
}

impl PublicKey for RsaPrivateKey {
    fn encrypt(&self, buffer: &[u8]) -> Result<Vec<u8>> {
        Ok(self
            .to_public_key()
            .encrypt(&mut thread_rng(), Pkcs1v15Encrypt, buffer)?)
    }

    fn to_pem(&self, password: Option<&[u8]>) -> Result<Zeroizing<String>> {
        Ok(match password {
            None => { self.to_pkcs8_pem(LineEnding::LF)? }
            Some(password) => { self.to_pkcs8_encrypted_pem(&mut thread_rng(), password, LineEnding::LF)? }
        })
    }

    fn to_der(&self, password: Option<&[u8]>) -> Result<Zeroizing<Vec<u8>>> {
        Ok(match password {
            None => { self.to_pkcs8_der()?.to_bytes() }
            Some(password) => { self.to_pkcs8_encrypted_der(&mut thread_rng(), password)?.to_bytes() }
        })
    }
}

impl PrivateKey for RsaPrivateKey {
    fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        Ok(RsaPrivateKey::decrypt(self, Pkcs1v15Encrypt, ciphertext)?)
    }
}

#[cfg(test)]
mod tests {
    use crate::Provider;
    use crate::rsa::RsaProvider;

    #[test]
    fn encrypt_decrypt() {
        let provider = RsaProvider::new();
        let private_key = provider.generate(256).unwrap();

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
        let provider = RsaProvider::new();
        let password = Some(b"hunter2" as &[u8]);
        let private_key_der = {
            let private_key = provider.generate(2048).unwrap();
            private_key.to_der(password).expect("could not get private key")
        };

        let _private_key = provider.read_private(&*private_key_der, password)
            .expect("could not read from encrypted der");
    }
}
