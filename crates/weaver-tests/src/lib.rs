use std::thread;
use std::thread::JoinHandle;
use crossbeam::channel::{bounded, Sender};
use weaver_core::db::server::WeaverDb;
use weaver_core::error::Error;

pub fn start_server(port: u16) -> eyre::Result<WeaverDbInstance> {
    let (send, recv) = bounded(0);
    let mut weaver = WeaverDb::default();
    weaver.bind_tcp(("localhost", port))?;
    let addr = weaver.local_addr().expect("should exist");
    let join = thread::spawn(move ||-> eyre::Result<()>  {
        let recv = recv;
        let _weaver = weaver;
        let _ = recv.recv();
        println!("shutting down weaver");
        Ok(())
    });
    Ok(WeaverDbInstance {
        killer: send,
        port: addr.port(),
        join_handle: Some(join),
    })
}

#[derive(Debug)]
pub struct WeaverDbInstance {
    killer: Sender<()>,
    port: u16,
    join_handle: Option<JoinHandle<eyre::Result<()>>>
}

impl WeaverDbInstance {

    pub fn port(&self) -> u16 {
        self.port
    }
    pub fn join(self) {}
}

impl Drop for WeaverDbInstance {
    fn drop(&mut self) {
        let _  = self.killer.send(());
        let _ = self.join_handle.take().map(|t| t.join());
    }
}