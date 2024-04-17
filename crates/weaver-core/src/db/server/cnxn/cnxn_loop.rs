//! The connect loop provides the "main" method for newly created connections

use std::io;
use std::io::ErrorKind;
use std::thread::sleep;
use std::time::Duration;

use crossbeam::channel::Receiver;
use tracing::{debug, error, info, Span, trace, warn};

use weaver_ast::ast::Query;

use crate::cancellable_task::Cancel;
use crate::cnxn::{Message, MessageStream, RemoteDbReq, RemoteDbResp};
use crate::db::server::layers::packets::{DbReqBody, DbResp};
use crate::db::server::processes::{ProcessState, RemoteWeaverProcess};
use crate::db::server::socket::DbSocket;
use crate::error::WeaverError;
use crate::rows::Rows;
use crate::tx::Tx;

/// The main method to use when connecting to a client
pub fn remote_stream_loop<S: MessageStream + Send>(
    mut stream: S,
    mut child: RemoteWeaverProcess,
    cancel: &Receiver<Cancel>,
    span: &Span,
) -> Result<(), WeaverError> {
    let socket = child.db().upgrade().unwrap().connect();
    let mut tx = Option::<Tx>::None;
    let mut rows = Option::<Box<dyn Rows>>::None;

    loop {
        let message = stream
            .read()
            .inspect_err(|_err| warn!("failed to receive message from stream"))?;
        match handle_message(
            message,
            &mut stream,
            (&mut child, cancel, &socket),
            &mut tx,
            &mut rows,
            span,
        ) {
            Ok(cont) => {
                if !cont {
                    break;
                }
            }
            Err(err) => {
                error!("sending error to client: {}", err);
                stream.write(&Message::Resp(RemoteDbResp::Err(err.to_string())))?;
                break;
            }
        }
    }
    // optional disconnect
    let _ = stream.write(&Message::Resp(RemoteDbResp::Disconnect));
    info!("ending connection loop for pid {}", child.pid());
    Ok(())
}

pub type Control<'a> = (
    &'a mut RemoteWeaverProcess,
    &'a Receiver<Cancel>,
    &'a DbSocket,
);

fn handle_message<S: MessageStream + Send>(
    message: Message,
    stream: &mut S,
    (child, cancel, socket): Control,
    tx: &mut Option<Tx>,
    mut rows: &mut Option<Box<dyn Rows>>,
    span: &Span,
) -> Result<bool, WeaverError> {
    match message {
        Message::Req(req) => {
            trace!("Received req {:?}", req);
            child.set_state(ProcessState::Active);

            let mut send_request =
                |body: DbReqBody, tx: &mut Option<Tx>| -> Result<RemoteDbResp, WeaverError> {
                    let mut resp = socket.send((body, span.clone()));
                    resp.on_cancel(cancel.clone());
                    let resp = resp.join()?;
                    trace!("using response: {:?}", resp);
                    Ok(match resp? {
                        DbResp::Pong => RemoteDbResp::Pong,
                        DbResp::Ok => RemoteDbResp::Ok,
                        DbResp::Err(err) => RemoteDbResp::Err(err.to_string()),
                        DbResp::Tx(received_tx) => {
                            *tx = Some(received_tx);
                            RemoteDbResp::Ok
                        }
                        DbResp::TxRows(ret_tx, ret_rows) => {
                            *tx = Some(ret_tx);
                            *rows = Some(Box::new(ret_rows));
                            RemoteDbResp::Ok
                        }
                        DbResp::TxTable(ret_tx, _ret_table) => {
                            // rows = Some(ret_table.all(&ret_tx)?);
                            *tx = Some(ret_tx);
                            RemoteDbResp::Ok
                        }
                        DbResp::Rows(ret_rows) => {
                            *rows = Some(Box::new(ret_rows));
                            debug!("received rows from remote");
                            RemoteDbResp::Ok
                        }
                    })
                };

            let resp: Result<RemoteDbResp, WeaverError> = match req {
                RemoteDbReq::ConnectionInfo => {
                    child.set_info("Getting connection info");
                    Ok(RemoteDbResp::ConnectionInfo(child.info()))
                }
                RemoteDbReq::Sleep(time) => {
                    sleep(Duration::from_millis(time));
                    Ok(RemoteDbResp::Ok)
                }
                RemoteDbReq::Query(query) => {
                    trace!("received query = {query:#?}");
                    child.set_info(&query);
                    match tx.take() {
                        None => send_request(DbReqBody::TxQuery(Tx::default(), query), tx),
                        Some(existing_tx) => {
                            send_request(DbReqBody::TxQuery(existing_tx, query), tx)
                        }
                    }
                }
                RemoteDbReq::DelegatedQuery(ref query) => {
                    let query: Query = Query::parse(query)?;
                    match tx.take() {
                        None => send_request(DbReqBody::TxQuery(Tx::default(), query), tx),
                        Some(existing_tx) => {
                            send_request(DbReqBody::TxQuery(existing_tx, query), tx)
                        }
                    }
                }
                RemoteDbReq::Ping => send_request(DbReqBody::Ping, tx),
                RemoteDbReq::StartTransaction => send_request(DbReqBody::StartTransaction, tx),
                RemoteDbReq::Commit => {
                    let this_tx = tx.take().expect("no active tx");
                    send_request(DbReqBody::Commit(this_tx), tx)
                }
                RemoteDbReq::Rollback => {
                    let this_tx = tx.take().expect("no active tx");
                    send_request(DbReqBody::Commit(this_tx), tx)
                }
                RemoteDbReq::GetRow => {
                    trace!("attempting to get next row");
                    match &mut rows {
                        None => Ok(RemoteDbResp::Err("no table".to_string())),
                        Some(table) => {
                            Ok(RemoteDbResp::Row(table.next().map(|t| {
                                t.slice(..table.schema().columns().len()).to_owned()
                            })))
                        }
                    }
                }
                RemoteDbReq::GetSchema => match rows {
                    None => Ok(RemoteDbResp::Err("no table set".to_string())),
                    Some(ref s) => Ok(RemoteDbResp::Schema(s.schema().clone())),
                },
                RemoteDbReq::Disconnect => {
                    child.make_idle();
                    trace!("child wants to disconnect");
                    return Ok(false);
                }
            };

            let resp = resp?;
            child.make_idle();
            trace!("Sending response: {:?}", resp);
            stream.write(&Message::Resp(resp))?;
        }
        _other => {
            error!("only requests allowed at this point");
            return Err(WeaverError::IoError(io::Error::new(
                ErrorKind::Unsupported,
                "unexpected message kind",
            )));
        }
    }
    Ok(true)
}
