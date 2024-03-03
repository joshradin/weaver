//! Weaver process handling
use crossbeam::channel::{unbounded, Receiver, Sender};
use std::cell::OnceCell;
use std::collections::BTreeMap;
use std::panic::{catch_unwind, panic_any, UnwindSafe};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::thread::JoinHandle;
use std::time::Instant;

use crate::access_control::users::User;
use crate::cancellable_task::{CancellableTask, CancellableTaskHandle, Cancelled, Canceller};
use crate::db::server::WeakWeaverDb;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, error_span, info, span, Level};

use crate::error::WeaverError;

pub type WeaverPid = u32;

#[derive(Debug)]
struct WeaverProcessShared {
    pid: WeaverPid,
    started: Instant,
    user: String,
    host: String,
    using: Option<String>,
}

/// A weaver process
#[derive(Debug)]
pub struct WeaverProcess {
    shared: Arc<WeaverProcessShared>,
    state: Arc<RwLock<ProcessState>>,
    info: Arc<RwLock<String>>,
    kill_channel: Sender<Kill>,
    handle: OnceLock<CancellableTaskHandle<Result<(), WeaverError>>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct WeaverProcessInfo {
    pub pid: WeaverPid,
    /// Age in seconds
    pub age: u32,
    pub state: ProcessState,
    pub info: String,
    pub user: String,
    pub host: String,
    pub using: Option<String>,
}

impl WeaverProcess {
    /// Creates the process and it's child struct
    fn new(pid: WeaverPid, user: &User, weak: WeakWeaverDb) -> (Self, WeaverProcessChild) {
        let (rx, tx) = unbounded::<Kill>();

        let state = Arc::<RwLock<ProcessState>>::new(Default::default());
        let info = Arc::<RwLock<String>>::new(Default::default());
        let started = Instant::now();
        let shared = Arc::new(WeaverProcessShared {
            pid,
            started,
            user: user.name().to_string(),
            host: user.host().to_string(),
            using: None,
        });
        (
            WeaverProcess {
                shared: shared.clone(),
                state: state.clone(),
                info: info.clone(),
                kill_channel: rx,
                handle: OnceLock::new(),
            },
            WeaverProcessChild {
                shared: shared.clone(),
                kill_channel: tx,
                state,
                info,
                db: weak.clone(),
            },
        )
    }

    fn set_handle(&mut self, join_handle: CancellableTaskHandle<Result<(), WeaverError>>) {
        let _ = self.handle.set(join_handle);
    }

    /// Gets the info struct
    pub fn info(&self) -> WeaverProcessInfo {
        WeaverProcessInfo {
            pid: self.shared.pid,
            age: self.shared.started.elapsed().as_secs() as u32,
            state: self.state.read().clone(),
            info: self.info.read().clone(),
            user: self.shared.user.clone(),
            host: self.shared.host.clone(),
            using: self.shared.using.clone(),
        }
    }

    pub fn kill(mut self) -> Result<(), WeaverError> {
        self.handle
            .take()
            .ok_or(WeaverError::ProcessFailed(self.shared.pid))
            .and_then(|t| match t.cancel() {
                Ok(ok) => Ok(ok),
                Err(e) => Err(WeaverError::ProcessFailed(self.shared.pid)),
            })
    }

    pub fn join(mut self) -> Result<(), WeaverError> {
        self.handle
            .take()
            .ok_or(WeaverError::ProcessFailed(self.shared.pid))
            .and_then(|t| match t.join() {
                Ok(ok) => match ok {
                    Ok(ok) => Ok(ok),
                    Err(err) => Err(WeaverError::TaskCancelled),
                },
                Err(_) => Err(WeaverError::ProcessFailed(self.shared.pid)),
            })
    }
}

impl Drop for WeaverProcess {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            match handle.join() {
                Ok(_) => {}
                Err(err) => {
                    panic_any(err);
                }
            }
        }
    }
}

/// The child of a weaver process must have access to this information
#[derive(Debug)]
pub struct WeaverProcessChild {
    shared: Arc<WeaverProcessShared>,
    kill_channel: Receiver<Kill>,
    state: Arc<RwLock<ProcessState>>,
    info: Arc<RwLock<String>>,
    db: WeakWeaverDb,
}

#[derive(Debug)]
struct Kill();

impl WeaverProcessChild {
    /// Provides access to the pid of the process
    pub fn pid(&self) -> WeaverPid {
        self.shared.pid
    }

    /// Provides access to a weak connection to the weaver db
    pub fn db(&self) -> &WeakWeaverDb {
        &self.db
    }

    pub fn set_state(&mut self, state: ProcessState) {
        *self.state.write() = state;
    }

    pub fn set_info<S: ToString>(&mut self, info: S) {
        *self.info.write() = info.to_string();
    }

    pub fn make_idle(&mut self) {
        self.set_state(ProcessState::Idle);
        self.set_info("");
    }

    /// Gets the info struct
    pub fn info(&self) -> WeaverProcessInfo {
        WeaverProcessInfo {
            pid: self.shared.pid,
            age: self.shared.started.elapsed().as_secs() as u32,
            state: self.state.read().clone(),
            info: self.info.read().clone(),
            user: self.shared.user.clone(),
            host: self.shared.host.clone(),
            using: self.shared.using.clone(),
        }
    }
}

/// The state of a process
#[derive(Debug, Eq, PartialEq, Default, Serialize, Deserialize, Clone)]
#[serde(rename_all = "UPPERCASE")]
pub enum ProcessState {
    #[default]
    Idle,
    Active,
    Killing,
    Finished,
}

/// The process manager should allow for starting, querying, pausing, and killing [`WeaverProcess`](WeaverProcess)
/// instances
#[derive(Debug)]
pub struct ProcessManager {
    weak: WeakWeaverDb,
    next_pid: AtomicU32,
    processes: Arc<RwLock<BTreeMap<WeaverPid, WeaverProcess>>>,
    process_killed_channel: Sender<WeaverPid>,
    process_killed_handle: JoinHandle<()>,
}

impl ProcessManager {
    /// Creates a new process manager
    pub fn new(weak: WeakWeaverDb) -> Self {
        let (send, read) = unbounded::<WeaverPid>();
        let processes: Arc<RwLock<BTreeMap<WeaverPid, WeaverProcess>>> = Default::default();
        let handle = {
            let processes = processes.clone();
            thread::spawn(move || loop {
                let Ok(pid) = read.recv() else {
                    break;
                };

                let _ = processes.write().remove(&pid);
            })
        };

        Self {
            weak,
            next_pid: AtomicU32::new(1),
            processes,
            process_killed_channel: send,
            process_killed_handle: handle,
        }
    }

    pub fn processes(&self) -> Vec<WeaverProcessInfo> {
        self.processes
            .read()
            .iter()
            .map(|(_, process)| process.info())
            .collect()
    }

    /// Starts a process
    pub fn start(
        &mut self,
        user: &User,
        func: CancellableTask<WeaverProcessChild, Result<(), WeaverError>>,
    ) -> Result<WeaverPid, WeaverError> {
        let pid = self.next_pid.fetch_add(1, Ordering::SeqCst);

        let (mut parent, child) = WeaverProcess::new(pid, user, self.weak.clone());

        let handle = {
            let channel = self.process_killed_channel.clone();
            error_span!("process", pid).in_scope(|| {
                func.wrap(move |child: WeaverProcessChild, mut inner, canceller| {
                    let pid = child.shared.pid;
                    debug!("starting process...");
                    let result = inner(child);
                    debug!("process ended with result {:?}", result);

                    if let Ok(()) = channel.send(pid) {
                    } else {
                        error!("couldn't remove process {} from process list", pid);
                    }

                    result
                })
                .start_with_name(child, format!("weaver-db-process-{}", pid))
            })?
        };
        parent.set_handle(handle);
        let pid = parent.shared.pid;
        self.processes.write().insert(pid, parent);

        Ok(pid)
    }
}

impl Drop for ProcessManager {
    fn drop(&mut self) {
        self.processes.write().clear()
    }
}
