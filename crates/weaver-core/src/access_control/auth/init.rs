//! Initializes the auth context
//!
//! The auth context should be idempotent.

use crate::access_control::auth::context::AuthContext;
use crate::access_control::auth::error::AuthInitError;
use openssl::asn1::{Asn1Integer, Asn1Time};
use openssl::bn;
use openssl::hash::MessageDigest;
use openssl::pkey::{PKey, Private, Public};
use openssl::rsa::Rsa;
use openssl::x509::{X509Builder, X509NameBuilder, X509};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tracing::{debug, instrument};

/// The configuration of the auth
#[derive(Debug, Deserialize, Serialize)]
pub struct AuthConfig {
    pub key_store: PathBuf,
    pub force_recreate: bool,
}

impl AuthConfig {
    pub fn in_path<P: AsRef<Path>>(path: P) -> Self {
        Self {
            key_store: path.as_ref().join("../../../../../weaver/keys"),
            force_recreate: false,
        }
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        AuthConfig {
            key_store: PathBuf::from("../../../../../weaver/keys"),
            force_recreate: false,
        }
    }
}

/// Initializes the auth context
#[instrument(level = "trace")]
pub(crate) fn init_auth_context(config: &AuthConfig) -> Result<AuthContext, AuthInitError> {
    std::fs::create_dir_all(&config.key_store)?;
    let (private_key, new_key) = private_key(&config.key_store)?;
    let public_key = public_key(&config.key_store, &private_key)?;
    let x509 = x509(
        &config.key_store,
        new_key.then_some((private_key.clone(), public_key)),
    )?;

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
    Ok(key)
}

fn public_key(path: &Path, key: &Rsa<Private>) -> Result<Rsa<Public>, AuthInitError> {
    let key_path = path.join("key.pub");
    let buffer = key.public_key_to_pem()?;
    debug!("getting public key at {key_path:?} from private key");
    if !key_path.exists() || std::fs::read(&key_path)? != buffer {
        std::fs::write(&key_path, &buffer)?;
    }
    Ok(Rsa::<Public>::public_key_from_pem(&buffer)?)
}

fn x509(path: &Path, keys: Option<(Rsa<Private>, Rsa<Public>)>) -> Result<X509, AuthInitError> {
    let cert_path = path.join("cert.pem");
    if let Some((private, public)) = keys {
        debug!(
            "generating x509 self-signed certificate from public and private keys at {cert_path:?}"
        );
        let mut builder = X509Builder::new()?;
        let serial = Asn1Integer::from_bn(bn::BigNum::from_u32(1)?.as_ref())?;

        let start = Asn1Time::days_from_now(0)?;
        let end = Asn1Time::days_from_now(365)?;

        builder.set_not_before(start.as_ref())?;
        builder.set_not_after(end.as_ref())?;
        builder.set_serial_number(serial.as_ref())?;
        let public = PKey::from_rsa(public)?;
        let private = PKey::from_rsa(private)?;

        let mut x509_name = X509NameBuilder::new()?;
        x509_name.append_entry_by_text(
            "CN",
            &whoami::fallible::hostname().expect("no hostname found"),
        )?;
        let name = x509_name.build();
        builder.set_subject_name(name.as_ref())?;
        builder.set_issuer_name(name.as_ref())?;
        builder.set_pubkey(&public)?;
        builder.set_version(2)?;
        builder.sign(&private, MessageDigest::sha256())?;
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
    use crate::access_control::auth::init::{init_auth_context, AuthConfig};
    use openssl::x509::X509;

    use tempfile::tempdir;

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
        let _ctx = init_auth_context(&AuthConfig {
            key_store: temp.path().to_path_buf(),
            force_recreate: true,
        })
        .expect("couldn't create context");

        let cert_file = temp.path().join("cert.pem");
        assert!(cert_file.exists());
        let _x509 =
            X509::from_pem(&std::fs::read(&cert_file).unwrap()).expect("invalid X509 certificate");
    }
}
