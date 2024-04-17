use std::error::Error;
use std::net::TcpStream;

use std::path::Path;
use std::thread;
use std::thread::JoinHandle;

use crossbeam::channel::{bounded, Sender};
use eyre::eyre;
use tracing::level_filters::LevelFilter;
use tracing::{debug, error, error_span, warn};

use weaver_client::WeaverClient;
use weaver_core::access_control::auth::init::AuthConfig;
use weaver_core::access_control::auth::LoginContext;
use weaver_core::cnxn::interprocess::LocalSocketStream;
use weaver_core::common::dual_result::DualResult;
use weaver_core::db::core::WeaverDbCore;
use weaver_core::db::server::WeaverDb;

use weaver_core::monitoring::{Monitor, Monitorable};

pub fn init_tracing(
    level_filter: impl Into<Option<LevelFilter>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_max_level(level_filter.into().unwrap_or(LevelFilter::DEBUG))
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .event_format(tracing_subscriber::fmt::format())
        .try_init()
}

pub fn start_server(
    port: u16,
    in_path: &Path,
    _num_workers: impl Into<Option<usize>>,
) -> eyre::Result<WeaverDbInstance> {
    let (send, recv) = bounded(0);
    let socket_path = in_path.join("weaverdb.socket");
    let mut weaver = WeaverDb::new(
        WeaverDbCore::with_path(in_path)?,
        AuthConfig {
            key_store: in_path.join("keys"),
            force_recreate: false,
        },
    )?;

    weaver.lifecycle_service().startup()?;

    let monitor = weaver.monitor();
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
        monitor: Some(monitor),
        port: addr.port(),
        join_handle: Some(join),
    })
}

#[derive(Debug)]
pub struct WeaverDbInstance {
    killer: Sender<()>,
    monitor: Option<Box<dyn Monitor>>,
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

    pub fn new_port_client(&self, context: LoginContext) -> eyre::Result<WeaverClient<TcpStream>> {
        WeaverClient::connect(("localhost", self.port()), context)
    }
}

impl Drop for WeaverDbInstance {
    fn drop(&mut self) {
        let _ = self.killer.send(());
        let _ = self.join_handle.take().map(|t| t.join());
    }
}

pub fn run_full_stack_local_socket<F>(path: &Path, cb: F) -> Result<(), eyre::Error>
where
    F: FnOnce(
        &mut WeaverDbInstance,
        &mut WeaverClient<LocalSocketStream>,
    ) -> Result<(), eyre::Error>,
{
    let server = start_server(0, path, None)?;
    DualResult::zip_with(Ok(server), |server: Result<_, &eyre::Error>| {
        let mut context = LoginContext::new();
        context.set_user("root");
        match server {
            Ok(_) => Ok(WeaverClient::connect_localhost(
                path.join("weaverdb.socket"),
                context,
            )?),
            Err(err) => Err(eyre!("can not start client without server: {}", err)),
        }
    })
    .then(
        |(mut server, mut client)| {
            debug!("running full stack");
            let output = error_span!("client").in_scope(|| cb(&mut server, &mut client));
            drop(client);
            let monitor = server.monitor.take().unwrap();
            drop(server);
            output.map(|output| (output, monitor))
        },
        |(e1, e2)| match (e1, e2) {
            (Some(e1), Some(e2)) => {
                error!("both server and client failed");
                Err(e1.wrap_err(e2))
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
    .map(|(server, mut monitor)| {
        debug!("monitor: {:#?}", monitor.stats());
        server
    })
}

pub fn run_full_stack_port<F>(path: &Path, cb: F) -> Result<(), eyre::Error>
where
    F: FnOnce(&mut WeaverDbInstance, &mut WeaverClient<TcpStream>) -> Result<(), eyre::Error>,
{
    let server = start_server(0, path, None)?;
    DualResult::zip_with(Ok(server), |server: Result<_, &eyre::Error>| {
        let mut context = LoginContext::new();
        context.set_user("root");

        match server {
            Ok(server) => {
                let port = server.port();
                Ok(WeaverClient::connect(("localhost", port), context)?)
            }
            Err(err) => Err(eyre!("can not start client without server: {}", err)),
        }
    })
    .then(
        |(mut server, mut client)| {
            debug!("running full stack");
            assert!(client.connected());
            let output = error_span!("client").in_scope(|| cb(&mut server, &mut client));
            drop(client);
            let monitor = server.monitor.take().unwrap();
            drop(server);
            output.map(|output| (output, monitor))
        },
        |(e1, e2)| match (e1, e2) {
            (Some(e1), Some(e2)) => {
                error!("both server and client failed");
                Err(e1.wrap_err(e2))
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
    .map(|(server, mut monitor)| {
        debug!("monitor: {:#?}", monitor.stats());
        server
    })
}
