use std::path::Path;
use std::thread;
use std::thread::JoinHandle;

use crossbeam::channel::{bounded, Sender};
use weaver_core::access_control::auth::init::AuthConfig;
use weaver_core::db::core::WeaverDbCore;

use weaver_core::db::server::WeaverDb;

pub fn start_server(
    port: u16,
    in_path: &Path,
    num_workers: impl Into<Option<usize>>,
) -> eyre::Result<WeaverDbInstance> {
    let (send, recv) = bounded(0);
    let mut weaver = WeaverDb::new(
        num_workers.into().unwrap_or(1),
        WeaverDbCore::new()?,
        AuthConfig {
            key_store: in_path.join("keys"),
            force_recreate: false,
        },
    )?;
    weaver.bind_tcp(("localhost", port))?;
    let addr = weaver.local_addr().expect("should exist");
    let join = thread::spawn(move || -> eyre::Result<()> {
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
    join_handle: Option<JoinHandle<eyre::Result<()>>>,
}

impl WeaverDbInstance {
    pub fn port(&self) -> u16 {
        self.port
    }
    pub fn join(self) {}
}

impl Drop for WeaverDbInstance {
    fn drop(&mut self) {
        let _ = self.killer.send(());
        let _ = self.join_handle.take().map(|t| t.join());
    }
}
