use crate::{PrivateKey, Provider, PublicKey, Result};

/// `chacha20poly1305` encryption provider
pub struct ChaCha20Poly1305Provider;

impl ChaCha20Poly1305Provider {
    pub fn new() -> Self {
        Self {}
    }
}


impl Provider for ChaCha20Poly1305Provider {
    fn name(&self) -> String {
        "chacha20poly1305".to_string()
    }

    fn generate(&self) -> Result<Box<dyn PrivateKey>> {
        todo!()
    }


    fn read_public(&self, buffer: &[u8]) -> Result<Box<dyn PublicKey>> {
        todo!()
    }

    fn read_private(&self, buffer: &[u8], password: Option<&[u8]>) -> Result<Box<dyn PrivateKey>> {
        todo!()
    }
}