use std::io;

/// An error that can only be throws within the authentication init method
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct AuthInitError {
    kind: AuthInitErrorKind,
}

impl<E> From<E> for AuthInitError
where
    AuthInitErrorKind: From<E>,
{
    fn from(value: E) -> Self {
        Self {
            kind: AuthInitErrorKind::from(value),
        }
    }
}
impl AuthInitError {
    /// Ges the error kind
    pub fn kind(&self) -> &AuthInitErrorKind {
        &self.kind
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AuthInitErrorKind {
    #[error(transparent)]
    IoError(#[from] io::Error),
    #[error(transparent)]
    OpensslError(#[from] openssl::error::Error),
    #[error(transparent)]
    OpenSslErrorStack(#[from] openssl::error::ErrorStack),
    #[error("No private key")]
    NoPrivateKey,
    #[error("No certificate")]
    NoCertificate,
}
