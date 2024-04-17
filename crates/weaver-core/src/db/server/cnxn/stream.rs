use crate::access_control::auth::context::AuthContext;
use crate::access_control::auth::handshake::{client_auth, server_auth};
use crate::access_control::auth::secured::Secured;
use crate::access_control::auth::LoginContext;
use crate::access_control::users::User;
use crate::cnxn::handshake::handshake_client;
use crate::cnxn::transport::Transport;
use crate::cnxn::{read_msg, write_msg, Message, MessageStream};
use crate::common::stream_support::Stream;
use crate::db::server::socket::DbSocket;
use crate::error::WeaverError;

use std::fmt::Debug;
use std::io::{Read, Write};
use std::mem::size_of;
use std::net::SocketAddr;
use std::sync::OnceLock;
use tracing::{debug, debug_span, error, instrument, trace};

/// A tcp stream that connects to a
#[derive(Debug)]
pub struct WeaverStream<T: Stream> {
    pub(crate) peer_addr: Option<SocketAddr>,
    pub(crate) local_addr: Option<SocketAddr>,
    pub(crate) socket: Option<Transport<T>>,
    localhost: bool,
    user: OnceLock<User>,
}

impl<T: Stream> WeaverStream<T> {
    pub(crate) fn new(
        peer_addr: Option<SocketAddr>,
        local_addr: Option<SocketAddr>,
        localhost: bool,
        socket: Transport<T>,
    ) -> Self {
        debug!("created new stream using transport: {socket:?}");
        Self {
            peer_addr,
            local_addr,
            socket: Some(socket),
            localhost,
            user: OnceLock::new(),
        }
    }

    /// Login using a given login context
    pub(crate) fn login(mut self, context: LoginContext) -> Result<WeaverStream<T>, WeaverError> {
        handshake_client(&mut self)?;
        client_auth(self, context)
    }

    pub fn user(&self) -> &User {
        self.user.get().expect("user must always be set")
    }

    pub(crate) fn set_user(&mut self, user: User) {
        self.user.set(user).expect("user already set");
    }

    /// Makes this connection secure, if it's not already
    pub fn to_secure(mut self, host: &str) -> Result<Self, WeaverError> {
        debug_span!("ssl accept stream").in_scope(|| {
            if let Some(Transport::Insecure(socket)) = self.socket {
                self.socket = Some(Transport::Secure(Secured::new(host, socket)?));
            }
            Ok(self)
        })
    }

    /// Gets the local socket address of the stream
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.local_addr
    }

    /// Gets the peer socket address of the stream
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer_addr
    }

    pub fn localhost(&self) -> bool {
        self.localhost
    }

    /// Gets the transports
    pub(crate) fn transport(&mut self) -> &mut Option<Transport<T>> {
        &mut self.socket
    }
}

impl<T: Stream> MessageStream for WeaverStream<T> {
    #[instrument(skip(self), fields(T=std::any::type_name::<T>()), ret, err)]
    fn read(&mut self) -> Result<Message, WeaverError> {
        trace!("waiting for message");
        let mut len = [0_u8; size_of::<u32>()];
        self.socket.as_mut().unwrap().read_exact(&mut len)?;
        let len = u32::from_be_bytes(len);
        let mut message_buffer = vec![0u8; len as usize];
        match self
            .socket
            .as_mut()
            .unwrap()
            .read_exact(&mut message_buffer)
        {
            Ok(()) => {}
            Err(e) => {
                error!("got error {e} when trying to read {} bytes", len);
                return Err(e.into());
            }
        }
        trace!("got message of length {}", len);
        read_msg(&message_buffer[..])
    }

    #[instrument(skip(self, message), fields(T=std::any::type_name::<T>()), ret, err)]
    fn write(&mut self, message: &Message) -> Result<(), WeaverError> {
        trace!("sending {message:?}");
        let mut msg_buffer = vec![];
        write_msg(&mut msg_buffer, message)?;
        let len = msg_buffer.len() as u32;
        self.socket
            .as_mut()
            .unwrap()
            .write_all(&len.to_be_bytes())?;
        self.socket.as_mut().unwrap().write_all(&msg_buffer[..])?;
        Ok(())
    }
}

/// Server side tcp handshake
pub fn tcp_server_handshake<T: Stream + Debug>(
    tcp: WeaverStream<T>,
    auth_context: &AuthContext,
    core: &DbSocket,
) -> Result<WeaverStream<T>, WeaverError> {
    server_auth(tcp, auth_context, core)
}

#[cfg(test)]
mod tests {
    use crate::cnxn::tcp::*;
    use crate::db::server::WeaverDb;

    #[test]
    fn open_listener() {
        let (dir, server) = WeaverDb::in_temp_dir().expect("could not create");
        let _listener =
            WeaverTcpListener::bind("localhost:0", server.weak()).expect("couldnt create listener");
    }
}
