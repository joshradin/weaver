//! Handshake between two connections

use crate::cnxn::{Message, MessageStream};
use crate::error::WeaverError;
use rand::Rng;
use std::time::Duration;
use tracing::{debug, error, info_span, trace};

/// The client connecting to a listener should be the handshake driver
pub fn handshake_client<T: MessageStream>(server: &mut T) -> Result<(), WeaverError> {
    let span = info_span!("client handshake");
    let _enter = span.enter();

    let mut nonce = [0_u8; 8];
    rand::thread_rng().fill(&mut nonce);
    trace!("Created nonce: {:x?}", nonce);

    debug!("Sending handshake init to server");
    server.write(&Message::Handshake {
        ack: false,
        nonce: Vec::from(nonce),
    })?;
    debug!("Handshake sent, waiting for acknowledgement from server...");
    let Message::Handshake {
        ack: true,
        nonce: nonce_resp,
    } = (match server.read() {
        Ok(msg) => msg,
        Err(e) => {
            error!("No message received from server received because of error: {e}");
            return Err(e);
        }
    })
    else {
        error!("Response from server didn't match expected handshake form");
        return Err(WeaverError::HandshakeFailed);
    };

    if &nonce != &nonce_resp[..] {
        error!("Handshake response nonce was not equal");
        return Err(WeaverError::HandshakeFailed);
    }

    debug!("Client handshake completed");
    Ok(())
}

/// The listening end of the handshake should respond to client requests
pub fn handshake_listener<T: MessageStream>(
    client: &mut T,
    _timeout: Duration,
) -> Result<(), WeaverError> {
    let span = info_span!("server handshake");
    let _enter = span.enter();
    debug!("Starting handshake from listener, waiting for client handshake request...");
    let Message::Handshake { ack: false, nonce } = (match client.read() {
        Ok(msg) => msg,
        Err(e) => {
            error!("No message received from client received because of error: {e}");
            return Err(e);
        }
    }) else {
        error!("Response from client didn't match expected handshake form");
        return Err(WeaverError::HandshakeFailed);
    };

    let resp = &Message::Handshake { ack: true, nonce };

    debug!("Sending ack back to client...");
    client.write(resp)?;
    debug!("Ack sent.");
    debug!("Server handshake completed");
    Ok(())
}
