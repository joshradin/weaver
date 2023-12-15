use std::cell::RefCell;
use std::collections::HashMap;
use crate::db::server::layers::packets::{DbReq, DbReqBody, DbResp, Packet, PacketId};
use crate::dynamic_table::Table;
use crate::error::Error;
use crate::tables::TableRef;
use crate::tx::Tx;
use crossbeam::channel::{Receiver, RecvError, Sender, TryRecvError, unbounded};
use std::sync::Arc;
use std::thread;
use parking_lot::{Mutex, RwLock};
use tracing::error_span;
use crate::db::server::processes::WeaverPid;

pub type MainQueueItem = (Packet<DbReq>, Sender<Packet<DbResp>>);

#[derive(Debug)]
pub struct DbSocket {
    pid: Option<WeaverPid>,
    main_queue: Sender<MainQueueItem>,
    resp_sender: Sender<Packet<DbResp>>,
    receiver: Receiver<Packet<DbResp>>,
    buffer: Mutex<HashMap<PacketId, Packet<DbResp>>>
}

impl DbSocket {

    /// Creates a new socket associated with an optional [pid](WeaverPid)
    pub(super) fn new(
        main_queue: Sender<MainQueueItem>,
        pid: impl Into<Option<WeaverPid>>,
    ) -> Self {
        let (resp_sender, receiver) = unbounded::<Packet<DbResp>>();
        Self {
            pid: pid.into(),
            main_queue,
            resp_sender,
            receiver,
            buffer: Default::default(),
        }
    }

    /// Communicate with the db
    pub fn send(&self, req: impl Into<DbReq>) -> Result<DbResp, Error> {
        error_span!("req-resp", pid=self.pid).in_scope(|| {
            let packet = Packet::new(req.into());
            let &id = packet.id();


            self.main_queue
                .send((packet, self.resp_sender.clone()))?;

            let packet = self.get_resp(id)?;
            Ok(packet.unwrap())
        })
    }

    fn get_resp(&self, id: PacketId) -> Result<Packet<DbResp>, Error> {
        loop {
            match self.receiver.try_recv() {
                Ok(recv) => {
                    if recv.id() == &id {
                        break Ok(recv);
                    } else {
                        self.buffer.lock().insert(*recv.id(), recv);
                    }
                }
                Err(TryRecvError::Empty) => {
                    if let Some(packet) = self.buffer.lock().remove(&id) {
                        break Ok(packet)
                    }
                },
                Err(_) => {
                    break Err(Error::RecvError(RecvError));
                }
            }
        }
    }

    pub fn get_table(&self, (schema, table): &TableRef) -> Result<Arc<Table>, Error> {
        let schema = schema.clone();
        let table = table.clone();
        let tx = Tx::default();
        let DbResp::TxTable(_, table) = self.send(DbReqBody::on_core({
            let schema = schema.clone();
            let table = table.clone();
            move |core| {
                let table = core
                    .get_table(&schema, &table)
                    .ok_or(Error::NoTableFound(schema.to_string(), table.to_string()))?;
                Ok(DbResp::TxTable(tx, table))
            }
        }))?
        else {
            return Err(Error::NoTableFound(schema.to_string(), table.to_string()));
        };

        Ok(table)
    }
}
