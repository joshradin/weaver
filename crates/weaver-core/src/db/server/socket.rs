use crate::cancellable_task::{CancellableTask, CancellableTaskHandle, Cancelled};
use crate::db::server::layers::packets::{DbReq, DbReqBody, DbResp, Packet, PacketId};
use crate::db::server::processes::WeaverPid;
use crate::error::WeaverError;
use crate::storage::tables::shared_table::SharedTable;
use crate::storage::tables::TableRef;
use crate::tx::Tx;
use crossbeam::channel::{unbounded, Receiver, RecvError, Sender, TryRecvError};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::hint;
use std::sync::Arc;
use tracing::{debug, trace, trace_span, warn, Span};

pub type MainQueueItem = (Packet<DbReq>, Sender<Packet<DbResp>>);

#[derive(Debug)]
pub struct DbSocketShared {
    pid: Option<WeaverPid>,
    main_queue: Sender<MainQueueItem>,
    resp_sender: Sender<Packet<DbResp>>,
    receiver: Receiver<Packet<DbResp>>,
    buffer: Mutex<HashMap<PacketId, Packet<DbResp>>>,
}

#[derive(Debug)]
pub struct DbSocket {
    shared: Arc<DbSocketShared>,
}

impl DbSocket {
    /// Creates a new socket associated with an optional [pid](WeaverPid)
    pub(super) fn new(
        main_queue: Sender<MainQueueItem>,
        pid: impl Into<Option<WeaverPid>>,
    ) -> Self {
        let (resp_sender, receiver) = unbounded::<Packet<DbResp>>();
        Self {
            shared: Arc::new(DbSocketShared {
                pid: pid.into(),
                main_queue,
                resp_sender,
                receiver,
                buffer: Default::default(),
            }),
        }
    }

    /// Creates a clone
    fn clone(&self) -> DbSocket {
        DbSocket {
            shared: self.shared.clone(),
        }
    }

    /// Communicate with the db
    pub fn send(&self, req: impl Into<DbReq>) -> CancellableTaskHandle<Result<DbResp, WeaverError>> {
        let clone = self.clone();
        let span = Span::current();

        CancellableTask::with_cancel(
            move |req: DbReq, _canceler| -> Result<Result<DbResp, WeaverError>, Cancelled> {
                trace_span!("req-resp", pid = clone.shared.pid).in_scope(
                    || -> Result<Result<DbResp, WeaverError>, Cancelled> {
                        let mut req: DbReq = req;
                        req.span_mut().get_or_insert(span);
                        let packet = Packet::new(req);
                        trace!("packet={:#?}", packet);
                        let &id = packet.id();
                        match clone
                            .shared
                            .main_queue
                            .send((packet, clone.shared.resp_sender.clone()))
                        {
                            Ok(_) => {}
                            Err(err) => {
                                return Ok(Err(err.into()));
                            }
                        }
                        trace!("sent packet to main queue");
                        trace!("waiting for packet response...");
                        let packet = match clone.get_resp(id) {
                            Ok(ok) => ok,
                            Err(err) => {
                                return Ok(Err(err));
                            }
                        };
                        trace!("got response packet");
                        Ok(Ok(packet.unwrap()))
                    },
                )
            },
        )
        .start(req.into())
    }

    fn get_resp(&self, id: PacketId) -> Result<Packet<DbResp>, WeaverError> {
        loop {
            match self.shared.receiver.try_recv() {
                Ok(recv) => {
                    trace!("received response packet with id {}", recv.id());
                    if recv.id() == &id {
                        trace!("ids matched, sending to owner");
                        break Ok(recv);
                    } else {
                        trace!("id mismatch, saving in buffer");
                        self.shared.buffer.lock().insert(*recv.id(), recv);
                    }
                }
                Err(TryRecvError::Empty) => {
                    // trace!("no responses in channel, looking for packet with id {id}");
                    if let Some(packet) = self.shared.buffer.lock().remove(&id) {
                        trace!("found matching packet in buffer");
                        break Ok(packet);
                    }
                    hint::spin_loop();
                }
                Err(err) => {
                    warn!("channel returned error: {}", err);
                    break Err(WeaverError::RecvError(RecvError));
                }
            }
        }
    }

    pub fn get_table(&self, (schema, table): &TableRef) -> Result<SharedTable, WeaverError> {
        let schema = schema.clone();
        let table = table.clone();
        let tx = Tx::default();
        let DbResp::TxTable(_, table) = self
            .send(DbReqBody::on_core({
                let schema = schema.clone();
                let table = table.clone();
                move |core, _cancel| {
                    debug!("getting table {:?}", (&schema, &table));
                    let table = match core.get_open_table(&schema, &table) {
                        Ok(table) => table,
                        Err(err) => return Ok(DbResp::Err(err)),
                    };
                    Ok(DbResp::TxTable(tx, table))
                }
            }))
            .join()??
        else {
            return Err(WeaverError::NoTableFound {
                table: table.to_string(),
                schema: schema.to_string(),
            });
        };

        Ok(table)
    }

    pub fn start_tx(&self) -> Result<Tx, WeaverError> {
        let DbResp::Tx(tx) = self.send(DbReqBody::StartTransaction).join()?? else {
            return Err(WeaverError::NoTransaction);
        };

        Ok(tx)
    }

    pub fn commit_tx(&self, tx: Tx) -> Result<(), WeaverError> {
        let DbResp::Ok = self.send(DbReqBody::Commit(tx)).join()?? else {
            return Err(WeaverError::NoTransaction);
        };

        Ok(())
    }

    pub fn rollback_tx(&self, tx: Tx) -> Result<(), WeaverError> {
        let DbResp::Ok = self.send(DbReqBody::Rollback(tx)).join()?? else {
            return Err(WeaverError::NoTransaction);
        };

        Ok(())
    }
}
