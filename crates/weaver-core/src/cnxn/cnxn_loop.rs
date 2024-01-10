//! The connect loop provides the "main" method for newly created connections

use crate::cancellable_task::{Cancel, CancellableTaskHandle, Cancelled, Canceller};
use crossbeam::channel::{unbounded, Receiver, RecvError, TryRecvError};
use either::Either;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use tracing::{debug, error, trace, warn};

use crate::cnxn::{Message, MessageStream, RemoteDbReq, RemoteDbResp};
use crate::db::server::layers::packets::{DbReqBody, DbResp};
use crate::db::server::processes::{ProcessState, WeaverProcessChild};
use crate::error::Error;
use crate::rows::Rows;
use crate::tx::Tx;

/// The main method to use when connecting to a client
pub fn remote_stream_loop<S: MessageStream + Send>(
    mut stream: S,
    mut child: WeaverProcessChild,
    cancel: &Receiver<Cancel>,
) -> Result<(), Error> {
    let socket = child.db().upgrade().unwrap().connect();
    let mut tx = Option::<Tx>::None;
    let mut rows = Option::<Box<dyn Rows>>::None;

    loop {
        let message = stream.read()?;
        match message {
            Message::Req(req) => {
                trace!("Received req {:?}", req);
                child.set_state(ProcessState::Active);
                let resp: Either<
                    Result<RemoteDbResp, Error>,
                    CancellableTaskHandle<Result<DbResp, Error>>,
                > = match req {
                    RemoteDbReq::ConnectionInfo => {
                        child.set_info("Getting connection info");
                        Either::Left(Ok(RemoteDbResp::ConnectionInfo(child.info())))
                    }
                    RemoteDbReq::Sleep(time) => {
                        sleep(Duration::from_millis(time));
                        Either::Left(Ok(RemoteDbResp::Ok))
                    }
                    RemoteDbReq::Query(query) => Either::Right(match tx.take() {
                        None => socket.send(DbReqBody::TxQuery(Tx::default(), query)),
                        Some(existing_tx) => socket.send(DbReqBody::TxQuery(existing_tx, query)),
                    }),
                    RemoteDbReq::Ping => Either::Right(socket.send(DbReqBody::Ping)),
                    RemoteDbReq::StartTransaction => {
                        Either::Right(socket.send(DbReqBody::StartTransaction))
                    }
                    RemoteDbReq::Commit => {
                        let tx = tx.take().expect("no active tx");
                        Either::Right(socket.send(DbReqBody::Commit(tx)))
                    }
                    RemoteDbReq::Rollback => {
                        let tx = tx.take().expect("no active tx");
                        Either::Right(socket.send(DbReqBody::Commit(tx)))
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
                    Either::Right(mut resp) => {
                        resp.on_cancel(cancel.clone()); // cancelling loop also cancels task
                        let resp = resp.join()?;
                        trace!("using response: {:?}", resp);
                        match resp? {
                            DbResp::Pong => RemoteDbResp::Pong,
                            DbResp::Ok => RemoteDbResp::Ok,
                            DbResp::Err(err) => RemoteDbResp::Err(err.to_string()),
                            DbResp::Tx(received_tx) => {
                                tx = Some(received_tx);
                                RemoteDbResp::Ok
                            }
                            DbResp::TxRows(ret_tx, ret_rows) => {
                                tx = Some(ret_tx);
                                rows = Some(Box::new(ret_rows));
                                RemoteDbResp::Ok
                            }
                            DbResp::TxTable(ret_tx, ret_table) => {
                                // rows = Some(ret_table.all(&ret_tx)?);
                                tx = Some(ret_tx);
                                RemoteDbResp::Ok
                            }
                            DbResp::Rows(ret_rows) => {
                                rows = Some(Box::new(ret_rows));
                                debug!("received rows from remote");
                                RemoteDbResp::Ok
                            }
                        }
                    }
                };

                child.make_idle();
                trace!("Sending response: {:?}", resp);
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
