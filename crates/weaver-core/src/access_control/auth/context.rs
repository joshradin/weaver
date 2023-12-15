//! The auth context for creating streams

use std::fmt::{Debug, Formatter};
use openssl::pkey::{PKey, Private};
use openssl::rsa::Rsa;
use openssl::ssl::{SslAcceptor, SslMethod};
use openssl::x509::X509;
use crate::access_control::auth::error::{AuthInitError, AuthInitErrorKind};
use crate::access_control::auth::secured::Secured;
use crate::cnxn::stream::Transport;
use crate::common::stream_support::Stream;
use crate::error::Error;


/// The auth context
#[derive(Clone)]
pub struct AuthContext {
    acceptor: SslAcceptor,
}

impl AuthContext {

    /// Creates a ssl protected stream
    pub fn stream<S : Stream>(&self, stream: S) -> Result<Secured<S>, Error> {
        self.acceptor.accept(stream).map_err(|_| Error::SslHandshakeError)
            .map(Secured::wrap)
    }

    pub fn secure_transport<S : Stream>(&self, transport: &mut Transport<S>) -> Result<(), Error> {
        if let Transport::Insecure(_) = transport {
            let mut taken = std::mem::replace(transport, Transport::None);
            let Transport::Insecure(secure) = taken else {
                unreachable!();
            };
            let accept = self.stream(secure)?;
            *transport = Transport::Secure(accept);
        }
        Ok(())
    }
}

impl Debug for AuthContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthContext").finish_non_exhaustive()
    }
}

impl AuthContext {
    pub fn builder() -> AuthContextBuilder {
        AuthContextBuilder {
            pkey: None,
            cert: None,
        }
    }
}


pub struct AuthContextBuilder {
    pkey: Option<PKey<Private>>,
    cert: Option<X509>
}

impl AuthContextBuilder {
    pub fn private_key(mut self, key: &Rsa<Private>) -> Result<Self, AuthInitError> {
        self.pkey = Some(PKey::from_rsa(key.clone())?);
        Ok(self)
    }

    pub fn cert(mut self, cert: &X509) -> Self {
        self.cert = Some(cert.clone());
        self
    }
}

impl AuthContextBuilder {

    /// Builds the auth context
    pub fn build(self) -> Result<AuthContext, AuthInitError> {
        let mut acceptor = SslAcceptor::mozilla_intermediate(
            SslMethod::tls()
        )?;
        let pkey = self.pkey.ok_or_else(|| AuthInitErrorKind::NoPrivateKey)?;
        acceptor.set_private_key(pkey.as_ref())?;
        let cert = self.cert.ok_or_else(|| AuthInitErrorKind::NoCertificate)?;
        acceptor.set_certificate(&cert)?;
        acceptor.check_private_key()?;
        Ok(AuthContext {
            acceptor: acceptor.build(),
        })
    }
}

