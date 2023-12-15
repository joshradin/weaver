//! Initializes the auth context
//!
//! The auth context should be idempotent.

use std::path::{Path, PathBuf};
use openssl::hash::MessageDigest;
use openssl::pkey::{PKey, Private, Public};
use openssl::rsa::Rsa;
use openssl::x509::{X509, X509Builder, X509NameBuilder};
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};
use crate::access_control::auth::context::AuthContext;
use crate::access_control::auth::error::{AuthInitError, AuthInitErrorKind};

/// The configuration of the auth
#[derive(Debug, Deserialize, Serialize)]
pub struct AuthConfig {
    key_store: PathBuf
}

impl Default for AuthConfig {
    fn default() -> Self {
        AuthConfig {
            key_store: PathBuf::from("."),
        }
    }
}


/// Initializes the auth context
#[instrument]
pub(crate) fn init_auth_context(config: &AuthConfig) -> Result<AuthContext, AuthInitError> {
    let (private_key, new_key) = private_key(&config.key_store)?;
    let public_key = public_key(&config.key_store, &private_key)?;
    let x509 = x509(&config.key_store, new_key.then_some((private_key.clone(), public_key)))?;

    let context = AuthContext::builder()
        .private_key(&private_key)?
        .cert(&x509)
        .build()?;



    Ok(context)
}

/// create the private key if it doesn't exist
fn private_key(path: &Path) -> Result<(Rsa<Private>, bool), AuthInitError> {
    let key_path = path.join("key.pem");
    let key = if key_path.exists() {
        let buffer = std::fs::read(&key_path)?;
        let rsa = Rsa::private_key_from_pem(&buffer)?;
        (rsa, false)
    } else {
        debug!("generating private key at {:?} using 2048 bits", key_path);
        let rsa = Rsa::generate(2048)?;
        let buffer = rsa.private_key_to_pem()?;
        std::fs::write(&key_path, buffer)?;
        (rsa, true)
    };
    return Ok(key)
}

fn public_key(path: &Path, key: &Rsa<Private>) -> Result<Rsa<Public>, AuthInitError> {
    let key_path = path.join("key.pub");
    let buffer = key.public_key_to_pem()?;
    if !key_path.exists() || std::fs::read(&key_path)? != buffer {
        std::fs::write(&key_path, &buffer)?;
    }
    Ok(Rsa::<Public>::public_key_from_pem(&buffer)?)
}



fn x509(path: &Path, keys: Option<(Rsa<Private>, Rsa<Public>)>) -> Result<X509, AuthInitError> {
    let cert_path = path.join("cert.pem");
    if let Some((private, public)) = keys {
        let mut builder = X509Builder::new()?;
        let public = PKey::from_rsa(public)?;
        let private = PKey::from_rsa(private)?;

        let mut x509_name = X509NameBuilder::new()?;
        x509_name.append_entry_by_text("CN", &whoami::hostname())?;
        builder.set_subject_name(&x509_name.build())?;
        builder.set_pubkey(&public)?;
        builder.sign(&private, MessageDigest::sha256())?;
        builder.set_version(2)?;
        let _v3context = builder.x509v3_context(None, None);

        let x509 = builder.build();
        let pem = x509.to_pem()?;
        std::fs::write(cert_path, pem)?;
        Ok(x509)
    } else {
        let buf = std::fs::read(cert_path)?;
        Ok(X509::from_pem(&buf)?)
    }




}




#[cfg(test)]
mod tests {
    use std::fs::File;
    use tempfile::tempdir;
    use crate::access_control::auth::init::{AuthConfig, init_auth_context};

    #[test]
    #[cfg(not(feature = "vendor-openssl"))]
    fn init_with_dynamic_openssl() {
        init_server_auth()
    }

    #[test]
    #[cfg(feature = "vendor-openssl")]
    fn init_with_vendored_openssl() {
        init_server_auth()
    }

    fn init_server_auth() {
        let temp = tempdir().expect("couldn't create a temp dir");
        let ctx = init_auth_context(&AuthConfig {
            key_store: temp.path().to_path_buf(),
        }).expect("couldn't create context");

    }


}

