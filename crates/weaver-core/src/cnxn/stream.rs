use std::io::{ErrorKind, Read, Write};
use std::io;
use serde::{Deserialize, Serialize};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use tracing::trace;
use std::mem::size_of;
use std::sync::OnceLock;
use std::time::Duration;
use crate::access_control::auth::context::AuthContext;
use crate::access_control::auth::handshake::{client_auth, server_auth};
use crate::access_control::auth::LoginContext;
use crate::access_control::auth::secured::Secured;
use crate::access_control::users::User;
use crate::cnxn::{Message, MessageStream, read_msg, write_msg};
use crate::cnxn::handshake::handshake_client;
use crate::db::server::socket::DbSocket;
use crate::error::Error;
use crate::common::stream_support::{packet_read, packet_write, Stream};

/// A tcp stream that connects to a
#[derive(Debug)]
pub struct WeaverStream<T : Stream> {
    pub(super) peer_addr: Option<SocketAddr>,
    pub(super) local_addr: Option<SocketAddr>,
    pub(super) socket: Transport<T>,
    user: OnceLock<User>
}

impl<T : Stream> WeaverStream<T> {
    pub(super) fn new(peer_addr: Option<SocketAddr>, local_addr: Option<SocketAddr>, socket: Transport<T>) -> Self {
        Self {
            peer_addr,
            local_addr,
            socket,
            user: OnceLock::new()
        }
    }

    /// Login using a given login context
    pub(super) fn login(mut self, context: LoginContext) -> Result<WeaverStream<T>, Error> {
        handshake_client(&mut self)?;
        Ok(client_auth(self, context)?)
    }

    pub fn user(&self) -> &User {
        self.user.get().expect("user must always be set")
    }

    pub(crate) fn set_user(&mut self, user: User) {
        self.user.set(user).expect("user already set");
    }

    /// Makes this connection secure, if it's not already
    pub fn to_secure(mut self, host: &str) -> Result<Self, Error> {
        if let Transport::Insecure(socket) = self.socket {
            self.socket = Transport::Secure(Secured::new(host, socket)?);
        }
        Ok(self)
    }


    /// Gets the local socket address of the stream
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.local_addr.clone()
    }

    /// Gets the peer socket address of the stream
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer_addr.clone()
    }

    /// Gets the transports
    pub fn transport(&mut self) -> &mut Transport<T> {
        &mut self.socket
    }
}

impl<T : Stream> MessageStream for WeaverStream<T> {
    fn read(&mut self) -> Result<Message, Error> {
        trace!("waiting for message");
        let mut len = [0_u8; size_of::<u32>()];
        self.socket.read_exact(&mut len)?;
        let len = u32::from_be_bytes(len);
        let mut message_buffer = vec![0u8; len as usize];
        self.socket.read_exact(&mut message_buffer)?;
        trace!("got message of length {}", len);
        read_msg(&message_buffer[..])
    }

    fn write(&mut self, message: &Message) -> Result<(), Error> {
        trace!("sending {message:?}");
        let mut msg_buffer = vec![];
        write_msg(&mut msg_buffer, message)?;
        let len = msg_buffer.len() as u32;
        self.socket.write_all(&len.to_be_bytes())?;
        self.socket.write_all(&msg_buffer[..])?;
        Ok(())
    }
}

/// Server side tcp handshake
pub fn tcp_server_handshake<T : Stream>(tcp: WeaverStream<T>, auth_context: &AuthContext, core: &DbSocket) -> Result<WeaverStream<T>, Error> {
    server_auth(tcp, auth_context, core)
}

#[derive(Debug)]
pub enum Transport<T : Stream> {
    None,
    Insecure(T),
    Secure(Secured<T>)
}

impl<T : Stream> AsRef<T> for Transport<T> {
    fn as_ref(&self) -> &T {
        match self {
            Transport::Insecure(i) => {i}
            Transport::Secure(i) => {i.as_ref()}
            Transport::None => {
                panic!("transport was lost")
            }
        }
    }
}

impl<T: Stream> Write for Transport<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Transport::Insecure(i) => { i.write(buf)}
            Transport::Secure(i) => { i.write(buf)}
            Transport::None => { Ok(0)}
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Transport::Insecure(i) => { i.flush()}
            Transport::Secure(s ) => { s.flush()}
            Transport::None => { Ok(())}
        }
    }
}

impl<T : Stream> Read for Transport<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Transport::Insecure(i) => {i.read(buf)}
            Transport::Secure(i) => { i.read(buf)}
            Transport::None => { Ok(0)}
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cnxn::tcp::*;
    use crate::db::server::WeaverDb;

    #[test]
    fn open_listener() {
        let server = WeaverDb::default();

        let listener =
            WeaverTcpListener::bind("localhost:0", server.weak()).expect("couldnt create listener");
    }
}
