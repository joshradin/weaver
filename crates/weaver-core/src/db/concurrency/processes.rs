//! Weaver process handling
use crossbeam::channel::{unbounded, Receiver, Sender};
use std::cell::OnceCell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::thread::JoinHandle;
use std::time::Instant;

use crate::db::concurrency::WeakWeaverDb;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::error::Error;

pub type WeaverPid = u32;

/// A weaver process
#[derive(Debug)]
pub struct WeaverProcess {
    pid: WeaverPid,
    started: Instant,
    state: Arc<RwLock<ProcessState>>,
    info: Arc<RwLock<String>>,
    kill_channel: Sender<Kill>,
    handle: OnceLock<JoinHandle<Result<(), Error>>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct WeaverProcessInfo {
    pub pid: WeaverPid,
    /// Age in seconds
    pub age: u32,
    pub state: ProcessState,
    pub info: String,
}

impl WeaverProcess {
    /// Creates the process and it's child struct
    fn new(pid: WeaverPid, weak: WeakWeaverDb) -> (Self, WeaverProcessChild) {
        let (rx, tx) = unbounded::<Kill>();

        let state = Arc::<RwLock<ProcessState>>::new(Default::default());
        let info = Arc::<RwLock<String>>::new(Default::default());
        let started = Instant::now();
        (
            WeaverProcess {
                pid,
                started,
                state: state.clone(),
                info: info.clone(),
                kill_channel: rx,
                handle: OnceLock::new(),
            },
            WeaverProcessChild {
                pid,
                started,
                kill_channel: tx,
                state,
                info,
                db: weak.clone(),
            },
        )
    }

    fn set_handle(&mut self, join_handle: JoinHandle<Result<(), Error>>) {
        let _ = self.handle.set(join_handle);
    }
}

/// The child of a weaver process must have access to this information
#[derive(Debug)]
pub struct WeaverProcessChild {
    pid: WeaverPid,
    started: Instant,
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
        self.pid
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
            pid: self.pid,
            age: self.started.elapsed().as_secs() as u32,
            state: self.state.read().clone(),
            info: self.info.read().clone(),
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
}

impl ProcessManager {
    /// Creates a new process manager
    pub fn new(weak: WeakWeaverDb) -> Self {
        Self {
            weak,
            next_pid: AtomicU32::new(1),
            processes: Default::default(),
        }
    }

    /// Starts a process
    pub fn start<F>(&mut self, func: F) -> Result<WeaverPid, Error>
    where
        F: FnOnce(WeaverProcessChild) -> Result<(), Error>,
        F: Send + 'static,
    {
        let pid = self.next_pid.fetch_add(1, Ordering::SeqCst);

        let (mut parent, child) = WeaverProcess::new(pid, self.weak.clone());

        let handle = {
            let processes = Arc::downgrade(&self.processes);

            thread::Builder::new()
                .name(format!("weaver-db-process-{}", pid))
                .spawn(move || {
                    let processes = processes;
                    let child = child;
                    let result = func(child);

                    if let Some(processes) = processes.upgrade() {
                        let _ = processes.write().remove(&pid);
                    }

                    result
                })?
        };
        parent.set_handle(handle);
        let pid = parent.pid;
        self.processes.write().insert(pid, parent);

        Ok(pid)
    }
}
