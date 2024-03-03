use crate::access_control::auth::LoginContext;
use crate::cnxn::handshake::handshake_listener;
use crate::cnxn::stream::{tcp_server_handshake, WeaverStream};
use crate::cnxn::transport::{StreamSniffer, Transport};
use crate::cnxn::WeaverStreamListener;
use crate::db::server::WeakWeaverDb;
use crate::error::WeaverError;
use interprocess::local_socket::LocalSocketListener;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream, ToSocketAddrs};
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
        let mut socket = Self::new(
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
        let mut stream = self.listener.accept()?;
        let mut db = self.weak.upgrade().ok_or(WeaverError::NoCoreAvailable)?;

        let mut socket = WeaverStream::new(None, None, true, Transport::Insecure(stream.into()));

        handshake_listener(&mut socket, Duration::from_secs(10))?; // ensures correct connection type first
        let socket = tcp_server_handshake(socket, db.auth_context(), &db.connect())?;

        Ok(socket)
    }
}
