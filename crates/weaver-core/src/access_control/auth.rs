//! Provides authentication for incoming connections.
//!
//! Auth flow:
//!  - client: Provide username
//!  - server: Accept or reject username
//!  - server: if accepted and no password, return user.
//!  - server: if accepted and password exists, request password from client
//!  - client: send password
//!  - server: check password, and return user if correct

use argon2::password_hash::PasswordHashString;
use argon2::PasswordHash;
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, Zeroizing};

pub mod init;
pub mod context;
pub mod error;
pub mod secured;


/// The handshake to connect a client with
pub mod handshake {
    use tracing::{debug, error_span};
    use crate::access_control::auth::context::AuthContext;
    use crate::access_control::auth::LoginContext;
    use crate::access_control::users::User;
    use crate::cnxn::stream::WeaverStream;
    use crate::common::stream_support::{packet_read, packet_write, Stream};
    use crate::db::server::socket::DbSocket;
    use crate::error::Error;
    /// Server side authentication. On success, provides a user struct.
    pub fn server_auth<T : Stream>(mut stream: WeaverStream<T>, auth_context: &AuthContext, db_socket: &DbSocket) -> Result<WeaverStream<T>, Error> {
        error_span!("server-auth").in_scope(|| {
            debug!("performing server-side authentication of peer {}", stream.peer_addr().unwrap());
            auth_context.secure_transport(stream.transport())?;
            let login_ctx: LoginContext = packet_read(stream.transport())?;

            todo!()
        })
    }

    pub fn client_auth<T : Stream>(stream: WeaverStream<T>, login_context: LoginContext) -> Result<WeaverStream<T>, Error> {
        error_span!("client-auth").in_scope(|| {
            debug!("performing client side authentication");
            debug!("securing stream...");
            let remote_host = stream.peer_addr().ok_or(Error::NoHostName).map(|addr| addr.ip().to_string())?;
            let mut stream = stream.to_secure(&remote_host)?;
            debug!("stream now secured on the client side");
            debug!("sending login-context to server about self");
            packet_write(stream.transport(), &login_context)?;

            Ok(todo!())
        })
    }
}

/// The login context
#[derive(Debug, Serialize, Deserialize)]
pub struct LoginContext {
    user: String,
    host: String,
    password_hash: Option<Zeroizing<Vec<u8>>>
}

impl LoginContext {
    pub fn new() -> Self {
        let user = whoami::username();
        let host = whoami::hostname();
        Self {
            user,
            host,
            password_hash: None,
        }
    }

    /// Sets the user for this login context

    pub fn set_user<S : AsRef<str>>(&mut self, user: S) {
        self.user = user.as_ref().to_string();
    }

    pub fn set_password<S : AsRef<[u8]>>(&mut self, password: Zeroizing<Vec<u8>>) {
        self.password_hash = Some(password);
    }
}




