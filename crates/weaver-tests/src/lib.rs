use std::net::TcpStream;
use std::path::Path;
use std::thread;
use std::thread::JoinHandle;

use crossbeam::channel::{bounded, Sender};
use eyre::{eyre, Report};
use log::{debug, error, warn};
use weaver_client::WeaverClient;
use weaver_core::access_control::auth::init::AuthConfig;
use weaver_core::access_control::auth::LoginContext;
use weaver_core::cnxn::interprocess::LocalSocketStream;
use weaver_core::common::dual_result::DualResult;
use weaver_core::db::core::WeaverDbCore;

use weaver_core::db::server::WeaverDb;
use weaver_core::error::Error;

pub fn start_server(
    port: u16,
    in_path: &Path,
    num_workers: impl Into<Option<usize>>,
) -> eyre::Result<WeaverDbInstance> {
    let (send, recv) = bounded(0);
    let socket_path = in_path.join("weaverdb.socket");
    let mut weaver = WeaverDb::new(
        num_workers.into().unwrap_or(1),
        WeaverDbCore::with_path(in_path)?,
        AuthConfig {
            key_store: in_path.join("keys"),
            force_recreate: false,
        },
    )?;
    weaver.bind_tcp(("localhost", port))?;
    weaver.bind_local_socket(socket_path)?;
    let addr = weaver.local_addr().expect("should exist");
    let join = thread::spawn(move || -> eyre::Result<()> {
        let recv = recv;
        let _weaver = weaver;
        let _ = recv.recv();
        warn!("shutting down weaver");
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
    pub fn join(mut self) -> Option<eyre::Result<()>> {
        self.join_handle.take().map(|handle| match handle.join() {
            Ok(result) => result,
            Err(_err) => Err(eyre!("thread panicked")),
        })
    }
}

impl Drop for WeaverDbInstance {
    fn drop(&mut self) {
        let _ = self.killer.send(());
        let _ = self.join_handle.take().map(|t| t.join());
    }
}

pub fn run_full_stack<F>(path: &Path, cb: F) -> Result<(), eyre::Error>
where
    F: FnOnce(
        &mut WeaverDbInstance,
        &mut WeaverClient<LocalSocketStream>,
    ) -> Result<(), eyre::Error>,
{
    let server = start_server(0, path, None);
    DualResult::zip_with(server, |server| {
        let mut context = LoginContext::new();
        context.set_user("root");
        match server {
            Ok(_) => WeaverClient::connect_localhost(path.join("weaverdb.socket"), context),
            Err(err) => Err(eyre!("can not start client without server: {}", err)),
        }
    })
    .then(
        |(mut server, mut client)| {
            debug!("running full stack");
            let output = cb(&mut server, &mut client);
            drop(client);
            drop(server);
            output
        },
        |(e1, e2)| match (e1, e2) {
            (Some(e1), Some(e2)) => {
                error!("both server and client failed");
                Err(eyre::Error::from(e1).wrap_err(e2))
            }
            (Some(e1), None) => {
                error!("only server failed");
                Err(e1)
            }
            (None, Some(e2)) => {
                error!("only client failed");
                Err(e2)
            }
            _ => unreachable!(),
        },
    )
}
