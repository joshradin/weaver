use std::io::ErrorKind;
use crate::access_control::auth::LoginContext;
use crate::cnxn::handshake::handshake_listener;
use crate::cnxn::stream::{tcp_server_handshake, WeaverStream};
use crate::cnxn::transport::{StreamSniffer, Transport};
use crate::cnxn::WeaverStreamListener;
use crate::db::server::WeakWeaverDb;
use crate::error::WeaverError;
use interprocess::local_socket::LocalSocketListener;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::Path;
use std::time::Duration;
use tracing::debug;

pub use interprocess::local_socket::LocalSocketStream;
impl WeaverStream<LocalSocketStream> {
    /// Connect via local socket
    pub fn local_socket<P: AsRef<Path>>(
        path: P,
        login_context: LoginContext,
    ) -> Result<Self, WeaverError> {
        let stream = LocalSocketStream::connect(path.as_ref())?;
        let socket = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 0));
        let socket = Self::new(
            Some(socket.clone()),
            Some(socket),
            true,
            Transport::Insecure(StreamSniffer::from(stream)),
        );
        Ok(socket.login(login_context)?)
    }
}

#[derive(Debug)]
pub struct WeaverLocalSocketListener {
    listener: LocalSocketListener,
    weak: WeakWeaverDb,
}

impl WeaverLocalSocketListener {
    /// Bind a listener to a [`WeakWeaverDb`](WeakWeaverDb)
    pub fn bind<P: AsRef<Path>>(path: P, weak: WeakWeaverDb) -> Result<Self, WeaverError> {
        let path = path.as_ref();
        let listener = LocalSocketListener::bind(path)?;
        debug!("bound local socket listener to {:?}", path);
        Ok(Self { listener, weak })
    }
}

impl WeaverStreamListener for WeaverLocalSocketListener {
    type Stream = LocalSocketStream;

    fn accept(&self) -> Result<WeaverStream<Self::Stream>, WeaverError> {
        let stream = loop {
            match self.listener.accept() {
                Ok(stream) => { break stream }
                Err(error) => {
                    if error.kind() != ErrorKind::WouldBlock {
                        return Err(error.into())
                    }
                }
            }
        };
        let db = self.weak.upgrade().ok_or(WeaverError::NoCoreAvailable)?;

        let mut socket = WeaverStream::new(None, None, true, Transport::Insecure(stream.into()));

        handshake_listener(&mut socket, Duration::from_secs(10))?; // ensures correct connection type first
        let socket = tcp_server_handshake(socket, db.auth_context(), &db.connect())?;

        Ok(socket)
    }

    fn try_accept(&self) -> Result<Option<WeaverStream<Self::Stream>>, WeaverError> {
        self.listener.set_nonblocking(true)?;
        let stream = match self.listener.accept() {
            Ok(stream) => { stream }
            Err(error) => {
                return if error.kind() == ErrorKind::WouldBlock {
                    Ok(None)
                } else {
                    Err(error.into())
                }
            }
        };
        self.listener.set_nonblocking(false)?;
        let db = self.weak.upgrade().ok_or(WeaverError::NoCoreAvailable)?;

        let mut socket = WeaverStream::new(None, None, true, Transport::Insecure(stream.into()));

        handshake_listener(&mut socket, Duration::from_secs(10))?; // ensures correct connection type first
        let socket = tcp_server_handshake(socket, db.auth_context(), &db.connect())?;

        Ok(Some(socket))
    }
}

impl Drop for WeaverLocalSocketListener {
    fn drop(&mut self) {
        debug!("dropping local socket listener");
    }
}