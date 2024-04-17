use std::process::exit;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::sleep;
use std::time::{Duration, Instant};

use color_eyre::eyre;
use tracing::{debug, Event, info, info_span, Subscriber, trace, warn};
use tracing::instrument::WithSubscriber;
use tracing::metadata::LevelFilter;
use tracing_error::ErrorLayer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;

use weaver_core::access_control::auth::init::AuthConfig;
use weaver_core::db::core::WeaverDbCore;
use weaver_core::db::server::layers::packets::DbReqBody;
use weaver_core::db::server::layers::packets::DbResp;
use weaver_core::db::server::WeaverDb;
use weaver_core::error::WeaverError;

use crate::cli::App;

pub mod cli;

/// Starts the application
pub fn run(app: App) -> eyre::Result<()> {
    warn!("starting weaverd with args: {app:#?}");
    trace!("start time: {:?}", Instant::now());
    let span = info_span!("main");
    let _enter = span.enter();

    info!("Starting weaver db...");
    std::fs::create_dir_all(app.work_dir())?;
    let core = WeaverDbCore::with_path(app.work_dir())?;
    trace!("weaver core created");

    let auth_config = AuthConfig {
        key_store: app.key_store(),
        force_recreate: false,
    };

    let mut weaver = WeaverDb::new(core, auth_config)?;

    let pair = Arc::new((Mutex::new(false), Condvar::new()));
    let pair2 = pair.clone();

    weaver.lifecycle_service().startup()?;
    weaver.lifecycle_service().on_teardown(move |_| {
        let (lock, condvar) = &*pair;
        *lock.lock().unwrap() = true;
        condvar.notify_one();
        Ok(())
    });

    {
        let mut svc = weaver.lifecycle_service().clone();
        ctrlc::set_handler(move || {
            let res = svc.teardown();
            if let Err(e) = res {
                warn!("teardown ended in {}", e);
            }
        })
        .expect("failed to set INTERRUPT handler");
    }

    let socket_path = app.work_dir().join("weaverdb.socket");
    weaver.bind_tcp((&*app.host, app.port))?;
    weaver.bind_local_socket(socket_path)?;

    if let Some(kill) = app.kill {
        let cnxn = weaver.connect();
        thread::spawn(move || {
            sleep(Duration::from_secs(kill));
            cnxn.send(DbReqBody::on_server(|s, _| {
                s.lifecycle_service().teardown().unwrap();
                Ok(DbResp::Ok)
            }));
        });
    }

    let cnxn = weaver.connect();
    let probe = thread::spawn(move || -> eyre::Result<()> {
        loop {
            trace!("Checking if weaver db is alive...");
            let resp = cnxn.send(DbReqBody::Ping).join()??;
            if !matches!(resp, DbResp::Pong) {
                warn!("no pong response, assuming server is dead");
                break;
            }
            trace!("weaver db still alive");
            sleep(Duration::from_secs(30));
        }
        Ok(())
    });
    let cond_wait = thread::spawn(move || {
        let (lock, condvar) = &*pair2;
        let mut clean_up = lock.lock().unwrap();
        while !*clean_up {
            clean_up = condvar.wait(clean_up).unwrap();
        }
        debug!("condvar reclaimed");
    });

    loop {
        if probe.is_finished() || cond_wait.is_finished() {
            break;
        }
    }

    drop(probe);

    Ok(())
}
