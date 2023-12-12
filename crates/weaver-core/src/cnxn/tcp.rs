//! TCP based connections

use crate::cnxn::handshake::{handshake_client, handshake_listener};
use crate::cnxn::{read_msg, write_msg, Message, MessageStream, RemoteDbReq, RemoteDbResp};
use crate::db::concurrency::WeakWeaverDb;
use crate::error::Error;
use std::io;
use std::io::{ErrorKind, Read, Write};
use std::mem::size_of;
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::time::Duration;
use tracing::debug;

/// A tcp stream that connects to a
#[derive(Debug)]
pub struct WeaverTcpStream {
    socket_addr: Option<SocketAddr>,
    socket: TcpStream,
}

impl WeaverTcpStream {
    /// Connect to a tcp stream
    pub fn connect<A: ToSocketAddrs>(socket_addr: A) -> Result<Self, Error> {
        let connected = TcpStream::connect(socket_addr)?;
        let socket_addr = connected.peer_addr().ok();
        let mut socket = Self {
            socket_addr,
            socket: connected,
        };
        handshake_client(&mut socket, Duration::MAX)?;
        Ok(socket)
    }

    /// Connect to a tcp stream with a timeout
    pub fn connect_timeout<A: ToSocketAddrs>(
        socket_addr: A,
        timeout: Duration,
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
        let mut socket = Self {
            socket_addr,
            socket: connected,
        };
        handshake_client(&mut socket, timeout)?;
        Ok(socket)
    }
}
impl MessageStream for WeaverTcpStream {
    fn read(&mut self) -> Result<Message, Error> {
        debug!("waiting for message");
        let mut len = [0_u8; size_of::<u32>()];
        self.socket.read_exact(&mut len)?;
        let len = u32::from_be_bytes(len);
        let mut message_buffer = vec![0u8; len as usize];
        self.socket.read_exact(&mut message_buffer)?;
        debug!("got message of length {}", len);
        read_msg(&message_buffer[..])
    }

    fn read_timeout(&mut self, duration: Duration) -> Result<Message, Error> {
        debug!("reading message with timeout {:?}", duration.as_millis());
        self.socket.set_read_timeout(Some(duration))?;
        let output = self.read();
        self.socket.set_read_timeout(None)?;
        output
    }

    fn write(&mut self, message: &Message) -> Result<(), Error> {
        debug!("sending {message:?}");
        let mut msg_buffer = vec![];
        write_msg(&mut msg_buffer, message)?;
        let len = msg_buffer.len() as u32;
        self.socket.write_all(&len.to_be_bytes())?;
        self.socket.write_all(&msg_buffer[..])?;
        Ok(())
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
    pub fn accept(&self) -> Result<WeaverTcpStream, Error> {
        let (mut stream, socket_addr) = self.tcp_listener.accept()?;
        let mut socket = WeaverTcpStream {
            socket_addr: Some(socket_addr),
            socket: stream,
        };

        handshake_listener(&mut socket, Duration::from_secs(10))?;
        Ok(socket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::concurrency::WeaverDb;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn open_listener() {
        let server = WeaverDb::default();

        let listener =
            WeaverTcpListener::bind("localhost:0", server.weak()).expect("couldnt create listener");
    }
}
