//! The connect loop provides the "main" method for newly created connections

use tracing::{debug, error, info_span, warn};

use crate::cnxn::{Message, MessageStream};
use crate::db::concurrency::{DbReq, DbResp, WeaverDb, WeakWeaverDb};
use crate::error::Error;

/// The main method to use when connecting to a client
pub fn cnxn_main<S: MessageStream>(mut stream: S, process_id: usize, distro_db: &WeakWeaverDb) -> Result<(), Error> {
    let span = info_span!("external-connection", pid=process_id);
    let _enter = span.enter();
    let socket = distro_db.upgrade().unwrap().connect();
    loop {
        let Ok(message) = stream.read() else {
            warn!("Connection closed");
            break;
        };

        match message {
            Message::Req(req) => {
                debug!("Received req {:?}", req);
                let resp = match req {
                    DbReq::Full(_) => {
                        panic!("can not receive full requests over MessageStreams")
                    }
                    req => {
                        socket.send(req)
                    }
                }.expect("no response");
                debug!("Sending response: {:?}", resp);
                stream.write(&Message::Resp(resp))?;
            }
            _ => {
                error!("only requests allowed at this point");
                break;
            }
        }
    }

    Ok(())
}