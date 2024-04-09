//! TCP based connections

use std::io;
use std::io::ErrorKind;
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::time::Duration;

use stream::tcp_server_handshake;
use tracing::debug;

use crate::access_control::auth::LoginContext;
use crate::cnxn::handshake::handshake_listener;
use crate::cnxn::stream::WeaverStream;
use crate::cnxn::transport::Transport;
use crate::cnxn::{stream, WeaverStreamListener};
use crate::db::server::WeakWeaverDb;
use crate::error::WeaverError;

impl WeaverStream<TcpStream> {
    /// Connect to a tcp stream
    pub fn connect<A: ToSocketAddrs>(
        socket_addr: A,
        login_context: LoginContext,
    ) -> Result<Self, WeaverError> {
        Self::connect_timeout(socket_addr, Duration::MAX, login_context)
    }

    /// Connect to a tcp stream with a timeout
    pub fn connect_timeout<A: ToSocketAddrs>(
        socket_addr: A,
        timeout: Duration,
        login_context: LoginContext,
    ) -> Result<Self, WeaverError> {
        let connected = {
            let iter = socket_addr.to_socket_addrs()?;
            let mut found_stream = None;
            for ref socket_addr in iter {
                let socket_addr: &SocketAddr = socket_addr;
                if let Ok(stream) = TcpStream::connect_timeout(socket_addr, timeout) {
                    found_stream = Some(stream);
                    break;
                }
            }
            found_stream.ok_or(WeaverError::IoError(io::Error::new(
                ErrorKind::NotConnected,
                "could not connect to socket addr",
            )))?
        };

        let socket_addr = connected.peer_addr().ok();
        let socket = Self::new(
            socket_addr,
            connected.peer_addr().ok(),
            false,
            Transport::Insecure(connected.into()),
        );
        Ok(socket.login(login_context)?)
    }
}

/// A tcp stream listener that accepts tcp connections
#[derive(Debug)]
pub struct WeaverTcpListener {
    tcp_listener: TcpListener,
    weak: WeakWeaverDb,
}

impl WeaverTcpListener {
    /// Bind a listener to a [`WeakWeaverDb`](WeakWeaverDb)
    pub fn bind<A: ToSocketAddrs>(addr: A, weak: WeakWeaverDb) -> Result<Self, WeaverError> {
        let tcp_listener = TcpListener::bind(addr)?;
        debug!("bound tcp listener to {:?}", tcp_listener.local_addr());
        Ok(Self { tcp_listener, weak })
    }

    /// Gets the local address of this listener
    pub fn local_addr(&self) -> Result<SocketAddr, WeaverError> {
        Ok(self.tcp_listener.local_addr()?)
    }
}

impl WeaverStreamListener for WeaverTcpListener {
    type Stream = TcpStream;

    /// Accepts an incoming connection
    fn accept(&self) -> Result<WeaverStream<TcpStream>, WeaverError> {
        let (stream, socket_addr) = loop {
            match self.tcp_listener.accept() {
                Ok(stream) => {break stream }
                Err(error) => {
                    if error.kind() != ErrorKind::WouldBlock {
                        return Err(error.into());
                    }
                }
            }
        };

        let db = self.weak.upgrade().ok_or(WeaverError::NoCoreAvailable)?;

        let mut socket = WeaverStream::new(
            Some(socket_addr),
            stream.local_addr().ok(),
            false,
            Transport::Insecure(stream.into()),
        );

        handshake_listener(&mut socket, Duration::from_secs(10))?; // ensures correct connection type first
        let socket = tcp_server_handshake(socket, db.auth_context(), &db.connect())?;

        Ok(socket)
    }

    fn try_accept(&self) -> Result<Option<WeaverStream<Self::Stream>>, WeaverError> {
        self.tcp_listener.set_nonblocking(true)?;
        let (stream, socket_addr) = match self.tcp_listener.accept() {
            Ok(stream ) => { stream }
            Err(e ) => {
                return match e.kind() {
                    ErrorKind::WouldBlock => { Ok(None)}
                    _ => Err(e.into())
                }
            }
        };
        self.tcp_listener.set_nonblocking(false)?;
        let db = self.weak.upgrade().ok_or(WeaverError::NoCoreAvailable)?;

        let mut socket = WeaverStream::new(
            Some(socket_addr),
            stream.local_addr().ok(),
            false,
            Transport::Insecure(stream.into()),
        );

        handshake_listener(&mut socket, Duration::from_secs(10))?; // ensures correct connection type first
        let socket = tcp_server_handshake(socket, db.auth_context(), &db.connect())?;

        Ok(Some(socket))
    }
}
