//! The connect loop provides the "main" method for newly created connections

use either::Either;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use tracing::{debug, error, info, info_span, warn};

use crate::cnxn::{Message, MessageStream, RemoteDbReq, RemoteDbResp};
use crate::db::concurrency::processes::{ProcessState, WeaverProcessChild};
use crate::db::concurrency::{DbReq, DbResp, WeakWeaverDb, WeaverDb};
use crate::dynamic_table::Table;
use crate::error::Error;
use crate::rows::{OwnedRows, Rows};
use crate::tx::Tx;

/// The main method to use when connecting to a client
pub fn cnxn_main<S: MessageStream>(
    mut stream: S,
    mut child: WeaverProcessChild,
) -> Result<(), Error> {
    let socket = child.db().upgrade().unwrap().connect();
    let mut tx = Option::<Tx>::None;
    let mut rows = Option::<Box<dyn OwnedRows>>::None;
    loop {
        let Ok(message) = stream.read() else {
            warn!("Connection closed");
            break;
        };

        match message {
            Message::Req(req) => {
                debug!("Received req {:?}", req);
                child.set_state(ProcessState::Active);
                let resp: Either<Result<RemoteDbResp, Error>, Result<DbResp, Error>> = match req {
                    RemoteDbReq::ConnectionInfo => {
                        child.set_info("Getting connection info");
                        Either::Left(Ok(RemoteDbResp::ConnectionInfo(child.info())))
                    }
                    RemoteDbReq::Sleep(time) => {
                        sleep(Duration::from_millis(time));
                        Either::Left(Ok(RemoteDbResp::Ok))
                    }
                    RemoteDbReq::Query(query) => Either::Right(match tx.take() {
                        None => socket.send(DbReq::TxQuery(Tx::default(), query)),
                        Some(existing_tx) => socket.send(DbReq::TxQuery(existing_tx, query)),
                    }),
                    RemoteDbReq::Ping => Either::Right(socket.send(DbReq::Ping)),
                    RemoteDbReq::StartTransaction => {
                        Either::Right(socket.send(DbReq::StartTransaction))
                    }
                    RemoteDbReq::Commit => {
                        let tx = tx.take().expect("no active tx");
                        Either::Right(socket.send(DbReq::Commit(tx)))
                    }
                    RemoteDbReq::Rollback => {
                        let tx = tx.take().expect("no active tx");
                        Either::Right(socket.send(DbReq::Commit(tx)))
                    }
                    RemoteDbReq::GetRow => {
                        debug!("attempting to get next row");
                        match &mut rows {
                            None => Either::Left(Ok(RemoteDbResp::Err("no table".to_string()))),
                            Some(table) => {
                                Either::Left(Ok(RemoteDbResp::Row(table.next().map(|t| {
                                    t.slice(..table.schema().columns().len()).to_owned()
                                }))))
                            }
                        }
                    }
                    RemoteDbReq::GetSchema => match rows {
                        None => Either::Left(Ok(RemoteDbResp::Err("no table set".to_string()))),
                        Some(ref s) => Either::Left(Ok(RemoteDbResp::Schema(s.schema().clone()))),
                    },
                };

                let resp = match resp {
                    Either::Left(left) => left?,
                    Either::Right(resp) => {
                        debug!("using response: {:?}", resp);
                        match resp? {
                            DbResp::Pong => RemoteDbResp::Pong,
                            DbResp::Ok => RemoteDbResp::Ok,
                            DbResp::Err(err) => RemoteDbResp::Err(err),
                            DbResp::Tx(received_tx) => {
                                tx = Some(received_tx);
                                RemoteDbResp::Ok
                            }
                            DbResp::TxRows(ret_tx, ret_rows) => {
                                tx = Some(ret_tx);
                                rows = Some(ret_rows);
                                RemoteDbResp::Ok
                            }
                            DbResp::TxTable(ret_tx, ret_table) => {
                                // rows = Some(ret_table.all(&ret_tx)?);
                                tx = Some(ret_tx);
                                RemoteDbResp::Ok
                            }
                            DbResp::Rows(ret_rows) => {
                                rows = Some(ret_rows);
                                debug!("received rows from remote");
                                RemoteDbResp::Ok
                            }
                        }
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
