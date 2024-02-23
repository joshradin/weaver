//! Provides authentication for incoming connections.
//!
//! Auth flow:
//!  - client: Provide username
//!  - server: Accept or reject username
//!  - server: if accepted and no password, return user.
//!  - server: if accepted and password exists, request password from client
//!  - client: send password
//!  - server: check password, and return user if correct

use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, Zeroizing};

pub mod context;
pub mod error;
pub mod init;
pub mod secured;

/// The handshake to connect a client with
pub mod handshake {
    use std::fmt::Debug;

    use tracing::{debug, error_span, warn};
    use weaver_ast::ast;

    use crate::access_control::auth::context::AuthContext;
    use crate::access_control::auth::LoginContext;
    use crate::access_control::users::User;
    use crate::common::stream_support::{packet_read, packet_write, Stream};
    use crate::data::values::DbVal;
    use crate::db::server::cnxn::stream::WeaverStream;
    use crate::db::server::cnxn::RemoteDbResp;
    use crate::db::server::layers::packets::{DbReqBody, DbResp};
    use crate::db::server::socket::DbSocket;
    use crate::error::Error;
    use crate::rows::Rows;
    use weaver_ast::ast::{BinaryOp, Query};

    /// Server side authentication. On success, provides a user struct.
    pub fn server_auth<T: Stream + Debug>(
        mut stream: WeaverStream<T>,
        auth_context: &AuthContext,
        db_socket: &DbSocket,
    ) -> Result<WeaverStream<T>, Error> {
        error_span!("server-auth").in_scope(|| {
            debug!(
                "performing server-side authentication of peer {:?}",
                stream.peer_addr()
            );
            auth_context.secure_transport(stream.transport())?;
            let mut login_ctx: LoginContext = packet_read(stream.transport().as_mut().unwrap())?;
            debug!("received login context: {:?}", login_ctx);
            let tx = db_socket.start_tx()?;
            let query = Query::parse(&format!(
                r#"select user, host from weaver.users where user = '{}'"#,
                login_ctx.user
            ))
            .expect("failed to parse");

            let resp = db_socket
                .send((tx, query))
                .join()
                .map_err(|e| Error::ThreadPanicked)??
                .to_result();
            debug!("resp={resp:?}");
            let resp = resp?;
            let DbResp::TxRows(tx, mut rows) = resp else {
                unreachable!();
            };
            let Some(row) = rows.next() else {
                warn!(
                    "user query was empty, no user found with name {:?}",
                    login_ctx.user
                );
                return Err(Error::custom("no user found"));
            };
            debug!("row = {row:?}");
            let auth_string = &row[2];
            match auth_string.as_ref() {
                DbVal::Null => {}
                DbVal::String(str, _) => {
                    todo!("password authentication")
                }
                _ => {
                    unreachable!()
                }
            }

            let user = User::new(row[0].to_string(), row[1].to_string());
            packet_write(stream.transport().as_mut().unwrap(), &user)?;
            stream.set_user(user);
            Ok(stream)
        })
    }

    pub fn client_auth<T: Stream>(
        stream: WeaverStream<T>,
        login_context: LoginContext,
    ) -> Result<WeaverStream<T>, Error> {
        error_span!("client-auth").in_scope(|| {
            debug!("performing client side authentication");
            debug!("securing client-side stream...");
            let remote_host = stream
                .peer_addr()
                .ok_or(Error::NoHostName)
                .map(|addr| addr.ip().to_string())?;
            debug!("using remote host: {:?}", remote_host);

            let mut stream = stream.to_secure(&remote_host)?;
            debug!("stream now secured on the client side");
            debug!("sending login-context to server about self");
            let transport = stream.transport().as_mut().unwrap();
            packet_write(transport, &login_context)?;
            let user = packet_read::<User, _>(transport)?;
            stream.set_user(user);
            Ok(stream)
        })
    }
}

/// The login context
#[derive(Debug, Serialize, Deserialize)]
pub struct LoginContext {
    user: String,
    host: String,
    password_hash: Option<Zeroizing<Vec<u8>>>,
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

    pub fn set_user<S: AsRef<str>>(&mut self, user: S) {
        self.user = user.as_ref().to_string();
    }

    pub fn set_password<S: AsRef<[u8]>>(&mut self, password: Zeroizing<Vec<u8>>) {
        self.password_hash = Some(password);
    }
}
