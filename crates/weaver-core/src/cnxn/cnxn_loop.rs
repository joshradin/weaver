//! The connect loop provides the "main" method for newly created connections

use std::thread::sleep;
use std::time::Duration;
use tracing::{debug, error, info, info_span, warn};

use crate::cnxn::{Message, MessageStream, RemoteDbReq, RemoteDbResp};
use crate::db::concurrency::{DbReq, DbResp, WeaverDb, WeakWeaverDb};
use crate::db::concurrency::processes::{ProcessState, WeaverProcessChild};
use crate::error::Error;
use crate::tx::Tx;

/// The main method to use when connecting to a client
pub fn cnxn_main<S: MessageStream>(mut stream: S, mut child: WeaverProcessChild) -> Result<(), Error> {
    let socket = child.db().upgrade().unwrap().connect();
    let mut tx = Option::<Tx>::None;
    loop {
        let Ok(message) = stream.read() else {
            warn!("Connection closed");
            break;
        };

        match message {
            Message::Req(req) => {
                debug!("Received req {:?}",  req);
                child.set_state(ProcessState::Active);
                let resp = match req {
                    RemoteDbReq::ConnectionInfo => {
                        child.set_info("Getting connection info");
                        Ok(DbResp::ConnectionInfo(child.info()))
                    }
                    RemoteDbReq::Sleep(time) => {
                        sleep(Duration::from_millis(time));
                        Ok(DbResp::Ok)
                    }
                    RemoteDbReq::Query(query) => {
                        match tx.take() {
                            None => {
                                socket.send(DbReq::Query(query))
                            }
                            Some(existing_tx) => {
                                socket.send(DbReq::TxQuery(existing_tx, query))
                            }
                        }

                    }
                    RemoteDbReq::Ping => {
                        socket.send(DbReq::Ping)
                    }
                }.expect("no response");

                let resp = match resp {
                    DbResp::Pong => {
                        RemoteDbResp::Pong
                    }
                    DbResp::Ok => {
                        RemoteDbResp::Ok
                    }
                    DbResp::TxTable(ret_tx, _) => {
                        tx = Some(ret_tx);
                        RemoteDbResp::Ok
                    }
                    DbResp::Table(_) => {
                        RemoteDbResp::Ok
                    }
                    DbResp::ConnectionInfo(info) => {
                        RemoteDbResp::ConnectionInfo(info)
                    }
                    DbResp::Err(err) => {
                        RemoteDbResp::Err(err)
                    }
                };


                child.make_idle();
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