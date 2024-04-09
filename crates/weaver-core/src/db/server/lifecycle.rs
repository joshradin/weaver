use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};

use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use tracing::{debug, error, error_span, info, instrument, Span, warn};

use crate::db::server::{WeakWeaverDb, WeaverDb};
use crate::error::WeaverError;

/// The phase the
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum LifecyclePhase {
    Failed = -1,
    Uninitialized,
    Initializing,
    Initialized,
    Bootstrapping,
    Ready,
    ShuttingDown,
    Dead,
}

struct WeaverDbLifecycleServiceInternal {
    weak: WeakWeaverDb,
    initialization_functions:
        Vec<Box<dyn FnOnce(&mut WeaverDb) -> Result<(), WeaverError> + Send + Sync>>,
    bootstrapping_functions:
        Vec<Box<dyn FnOnce(&mut WeaverDb) -> Result<(), WeaverError> + Send + Sync>>,
    teardown_functions:
        Vec<Box<dyn FnOnce(&mut WeaverDb) -> Result<(), WeaverError> + Send + Sync>>,
}

#[derive(Clone)]
pub struct WeaverDbLifecycleService {
    helper: Arc<Mutex<WeaverDbLifecycleServiceInternal>>,
    phase: Arc<RwLock<LifecyclePhase>>,
}

impl Debug for WeaverDbLifecycleService {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WeaverDbLifecycleService")
            .field("phase", &self.phase)
            .finish()
    }
}

impl WeaverDbLifecycleService {
    pub(crate) fn new(db: WeakWeaverDb) -> Self {
        


        Self {
            helper: Arc::new(Mutex::new(WeaverDbLifecycleServiceInternal {
                weak: db,

                initialization_functions: vec![],
                bootstrapping_functions: vec![],
                teardown_functions: vec![],
            })),
            phase: Arc::new(RwLock::new(LifecyclePhase::Uninitialized)),
        }
    }

    /// Runs on init
    pub fn on_init<F>(&mut self, callback: F)
    where
        F: FnOnce(&mut WeaverDb) -> Result<(), WeaverError>,
        F: Send + Sync + 'static,
    {
        self.helper
            .lock()
            .initialization_functions
            .push(Box::new(callback))
    }

    /// Runs on bootstrap
    pub fn on_bootstrap<F>(&mut self, callback: F)
    where
        F: FnOnce(&mut WeaverDb) -> Result<(), WeaverError>,
        F: Send + Sync + 'static,
    {
        self.helper
            .lock()
            .bootstrapping_functions
            .push(Box::new(callback))
    }

    /// Runs on teardown
    pub fn on_teardown<F>(&mut self, callback: F)
    where
        F: FnOnce(&mut WeaverDb) -> Result<(), WeaverError>,
        F: Send + Sync + 'static,
    {
        self.helper
            .lock()
            .teardown_functions
            .push(Box::new(callback))
    }

    /// Gets the current lifecycle phase of the weaver db instance
    pub fn phase(&self) -> LifecyclePhase {
        *self.phase.read()
    }

    /// Makes sure the WeaverDb instance is ready.
    pub fn startup(&mut self) -> Result<(), WeaverError> {
        let mut phase_lock = self.phase.write();
        match &*phase_lock {
            LifecyclePhase::Uninitialized => {
                *phase_lock = LifecyclePhase::Initializing;
            }
            LifecyclePhase::Ready => return Ok(()),
            _other => panic!("can not startup server in phase {_other:?}"),
        }
        info!(
            "Starting weaver db server version {}",
            env!("CARGO_PKG_VERSION")
        );
        Span::current().record("phase", format!("{:?}", phase_lock));
        drop(phase_lock);

        let res = self.startup_();
        if let Err(e) = res {
            error!("startup failed: {}", e);
            *self.phase.write() = LifecyclePhase::Failed;
            return Err(e);
        }

        info!("Weaver ready!");

        Ok(())
    }

    fn startup_(&mut self) -> Result<(), WeaverError> {
        let helper = &mut *self.helper.lock();
        let mut weaver = helper
            .weak
            .upgrade()
            .ok_or_else(|| WeaverError::NoCoreAvailable)?;
        // init
        let init_functions = VecDeque::from_iter(helper.initialization_functions.drain(..));

        info!("Initializing weaver...");
        error_span!("startup", phase = "initialization").in_scope(
            || -> Result<(), WeaverError> {
                for init_function in init_functions {
                    init_function(&mut weaver)?;
                }

                info!("initialization completed");
                *self.phase.write() = LifecyclePhase::Initialized;
                Ok(())
            },
        )?;
        if !weaver.is_bootstrapped() {
            *self.phase.write() = LifecyclePhase::Bootstrapping;
            error_span!("startup", phase = "bootstrap").in_scope(
                || -> Result<(), WeaverError> {
                    info!("Bootstrapping weaver...");
                    // bootstrap
                    let bootstrap_functions =
                        VecDeque::from_iter(helper.bootstrapping_functions.drain(..));

                    for bootstrap_function in bootstrap_functions {
                        bootstrap_function(&mut weaver)?;
                    }
                    Ok(())
                },
            )?;
        }

        *self.phase.write() = LifecyclePhase::Ready;
        Ok(())
    }

    #[instrument(skip_all, fields(phase="teardown"))]
    pub fn teardown(&mut self) -> Result<(), WeaverError> {
        let mut phase_lock = self.phase.write();
        match &*phase_lock {
            LifecyclePhase::Ready => {
                *phase_lock = LifecyclePhase::ShuttingDown;
            }
            LifecyclePhase::Dead | LifecyclePhase::Uninitialized | LifecyclePhase::Failed => {
                return Ok(())
            }
            _other => panic!("can not teardown server in phase {_other:?}"),
        }
        warn!("Shutting down weaver...");
        drop(phase_lock);
        let helper = &mut *self.helper.lock();
        let mut weaver = helper
            .weak
            .upgrade()
            .ok_or_else(|| WeaverError::NoCoreAvailable)?;
        // bootstrap
        let teardown_functions =
            VecDeque::from_iter(helper.teardown_functions.drain(..));

        for teardown in teardown_functions {
            teardown(&mut weaver)?;
        }

        debug!("weaverdb strong: {}, weak: {}", Arc::strong_count(&weaver.shared), Arc::weak_count(&weaver.shared));

        *self.phase.write() = LifecyclePhase::Dead;
        warn!("Weaver dead");
        Ok(())
    }
}
