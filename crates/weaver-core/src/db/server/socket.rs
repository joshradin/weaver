use crate::cancellable_task::{CancellableTask, CancellableTaskHandle, Cancelled};
use crate::db::server::layers::packets::{DbReq, DbReqBody, DbResp, Packet, PacketId};
use crate::db::server::processes::WeaverPid;
use crate::error::Error;
use crate::tables::shared_table::SharedTable;
use crate::tables::TableRef;
use crate::tx::Tx;
use crossbeam::channel::{unbounded, Receiver, RecvError, Sender, TryRecvError};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::hint;
use std::sync::Arc;
use tracing::{debug, error_span, trace, warn};

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
    pub fn send(&self, req: impl Into<DbReq>) -> CancellableTaskHandle<Result<DbResp, Error>> {
        let clone = self.clone();
        CancellableTask::with_cancel(
            move |req: DbReq, canceler| -> Result<Result<DbResp, Error>, Cancelled> {
                error_span!("req-resp", pid = clone.shared.pid).in_scope(
                    || -> Result<Result<DbResp, Error>, Cancelled> {
                        let packet = Packet::new(req.into());
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

    fn get_resp(&self, id: PacketId) -> Result<Packet<DbResp>, Error> {
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
                    break Err(Error::RecvError(RecvError));
                }
            }
        }
    }

    pub fn get_table(&self, (schema, table): &TableRef) -> Result<SharedTable, Error> {
        let schema = schema.clone();
        let table = table.clone();
        let tx = Tx::default();
        let DbResp::TxTable(_, table) = self
            .send(DbReqBody::on_core({
                let schema = schema.clone();
                let table = table.clone();
                move |core, cancel| {
                    debug!("getting table {:?}", (&schema, &table));
                    let table = match core.get_table(&schema, &table).ok_or(Error::NoTableFound {
                        table: schema.to_string(),
                        schema: table.to_string(),
                    }) {
                        Ok(table) => table,
                        Err(err) => return Ok(DbResp::Err(err)),
                    };
                    Ok(DbResp::TxTable(tx, table))
                }
            }))
            .join()??
        else {
            return Err(Error::NoTableFound {
                table: schema.to_string(),
                schema: table.to_string(),
            });
        };

        Ok(table)
    }

    pub fn start_tx(&self) -> Result<Tx, Error> {
        let DbResp::Tx(tx) = self.send(DbReqBody::StartTransaction).join()?? else {
            return Err(Error::NoTransaction);
        };

        Ok(tx)
    }

    pub fn commit_tx(&self, tx: Tx) -> Result<(), Error> {
        let DbResp::Ok = self.send(DbReqBody::Commit(tx)).join()?? else {
            return Err(Error::NoTransaction);
        };

        Ok(())
    }

    pub fn rollback_tx(&self, tx: Tx) -> Result<(), Error> {
        let DbResp::Ok = self.send(DbReqBody::Rollback(tx)).join()?? else {
            return Err(Error::NoTransaction);
        };

        Ok(())
    }
}
