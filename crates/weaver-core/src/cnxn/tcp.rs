//! TCP based connections

use std::io;
use std::io::ErrorKind;
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::OnceLock;
use std::time::Duration;

use tracing::debug;
use stream::tcp_server_handshake;

use crate::access_control::auth::LoginContext;
use crate::cnxn::handshake::handshake_listener;
use crate::cnxn::stream;
use crate::cnxn::stream::{Transport, WeaverStream};
use crate::db::server::WeakWeaverDb;
use crate::error::Error;

impl WeaverStream<TcpStream> {
    /// Connect to a tcp stream
    pub fn connect<A: ToSocketAddrs>(socket_addr: A, login_context: LoginContext) -> Result<Self, Error> {
        Self::connect_timeout(socket_addr, Duration::MAX, login_context)
    }

    /// Connect to a tcp stream with a timeout
    pub fn connect_timeout<A: ToSocketAddrs>(
        socket_addr: A,
        timeout: Duration,
        login_context: LoginContext
    ) -> Result<Self, Error> {
        let connected = {
            let mut iter = socket_addr.to_socket_addrs()?;
            let mut found_stream = None;
            for ref socket_addr in iter {
                if let Ok(stream) = TcpStream::connect_timeout(socket_addr, timeout) {
                    found_stream = Some(stream);
                    break;
                }
            }
            found_stream.ok_or(Error::IoError(io::Error::new(
                ErrorKind::NotConnected,
                "could not connect to socket addr",
            )))?
        };

        let socket_addr = connected.peer_addr().ok();
        let mut socket = Self::new(
            socket_addr,
            connected.peer_addr().ok(),
            Transport::Insecure(connected));
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
    pub fn bind<A: ToSocketAddrs>(addr: A, weak: WeakWeaverDb) -> Result<Self, Error> {
        let tcp_listener = TcpListener::bind(addr)?;
        debug!("bound tcp listener to {:?}", tcp_listener.local_addr());
        Ok(Self { tcp_listener, weak })
    }

    /// Gets the local address of this listener
    pub fn local_addr(&self) -> Result<SocketAddr, Error> {
        Ok(self.tcp_listener.local_addr()?)
    }

    /// Accepts an incoming connection
    pub fn accept(&self) -> Result<WeaverStream<TcpStream>, Error> {
        let (mut stream, socket_addr) = self.tcp_listener.accept()?;

        let mut db = self.weak.upgrade().ok_or(Error::NoCoreAvailable)?;

        let mut socket = WeaverStream::new(
            Some(socket_addr),
            stream.local_addr().ok(),
            Transport::Insecure(stream),
        );

        handshake_listener(&mut socket, Duration::from_secs(10))?; // ensures correct connection type first
        let socket = tcp_server_handshake(
            socket,
            db.auth_context(),
            &db.connect()
        )?;

        Ok(socket)
    }
}
