//! A task that can be cancelled at any point.
//!
//! Ideally this would just be split into many parts that

use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::thread::JoinHandle;

use crossbeam::channel::{bounded, Receiver, SendError, Sender};
use tracing::Span;

use crate::error::WeaverError;

/// A task that can be cancelled
///
/// Useful for having potentially long running tasks
pub struct CancellableTask<I, O> {
    cancel_send: Sender<Cancel>,
    cancel_receiver: Receiver<Cancel>,
    func: Box<dyn FnOnce(I) -> Result<O, Cancelled> + Send + Sync>,
}

impl<I, O> Debug for CancellableTask<I, O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancellableTask").finish_non_exhaustive()
    }
}

impl CancellableTask<(), ()> {
    /// Creates a no-op cancellable task
    pub fn new() -> Self {
        let (sender, receiver) = bounded(0);
        Self {
            cancel_send: sender,
            cancel_receiver: receiver,
            func: Box::new(|_| Ok(())),
        }
    }
}

impl<O: Send + 'static> CancellableTask<(), O> {
    /// Start specialization for unit input
    pub fn run(self) -> CancellableTaskHandle<O> {
        self.start(())
    }

    pub fn spawn<F>(func: F) -> CancellableTaskHandle<O>
    where
        F: FnOnce(&Receiver<Cancel>) -> Result<O, Cancelled> + 'static + Send + Sync,
    {
        CancellableTask::with_cancel(|(), cancel| func(cancel)).run()
    }
}

impl<I: Send + 'static, O: Send + 'static> CancellableTask<I, O> {
    pub fn start(self, input: I) -> CancellableTaskHandle<O> {
        let CancellableTask {
            cancel_send,
            cancel_receiver: _,
            func,
        } = self;
        let span = Span::current();
        CancellableTaskHandle {
            handle: OnceLock::from(thread::spawn(move || span.in_scope(|| func(input)))),
            canceller: Arc::new(cancel_send),
            cancelled: AtomicBool::default(),
        }
    }

    pub fn start_with_name(
        self,
        input: I,
        name: String,
    ) -> Result<CancellableTaskHandle<O>, WeaverError> {
        let CancellableTask {
            cancel_send,
            cancel_receiver: _,
            func,
        } = self;
        let span = Span::current();
        Ok(CancellableTaskHandle {
            handle: OnceLock::from(
                thread::Builder::new()
                    .name(name)
                    .spawn(move || span.in_scope(|| func(input)))?,
            ),
            canceller: Arc::new(cancel_send),
            cancelled: AtomicBool::default(),
        })
    }
    pub fn with_step<F>(step: F) -> Self
    where
        F: FnOnce(I) -> O + 'static + Send + Sync,
    {
        let (sender, receiver) = bounded(0);
        Self {
            cancel_send: sender,
            cancel_receiver: receiver,
            func: Box::new(move |i| Ok(step(i))),
        }
    }
    pub fn with_cancel<F>(step: F) -> Self
    where
        F: FnOnce(I, &Receiver<Cancel>) -> Result<O, Cancelled> + 'static + Send + Sync,
    {
        let (sender, receiver) = bounded(0);
        Self {
            cancel_send: sender,
            cancel_receiver: receiver.clone(),
            func: Box::new(move |i| step(i, &receiver)),
        }
    }

    pub fn next<F, U>(self, step: F) -> CancellableTask<I, U>
    where
        F: FnOnce(O) -> U + 'static + Send + Sync,
    {
        let CancellableTask {
            cancel_send,
            cancel_receiver,
            func,
        } = self;
        let new_func = {
            let cancel_receiver = cancel_receiver.clone();
            move |input: I| -> Result<U, Cancelled> {
                let inter = func(input)?;
                if let Ok(Cancel) = cancel_receiver.try_recv() {
                    Err(Cancelled)
                } else {
                    Ok(step(inter))
                }
            }
        };
        CancellableTask {
            cancel_send,
            cancel_receiver,
            func: Box::new(new_func),
        }
    }

    pub fn next_with_cancel<F, U>(self, step: F) -> CancellableTask<I, U>
    where
        F: FnOnce(O, &Receiver<Cancel>) -> Result<U, Cancelled> + 'static + Send + Sync,
    {
        let CancellableTask {
            cancel_send,
            cancel_receiver,
            func,
        } = self;
        let new_func = {
            let cancel_receiver = cancel_receiver.clone();
            move |input: I| -> Result<U, Cancelled> {
                let inter = func(input)?;
                if let Ok(Cancel) = cancel_receiver.try_recv() {
                    Err(Cancelled)
                } else {
                    step(inter, &cancel_receiver)
                }
            }
        };
        CancellableTask {
            cancel_send,
            cancel_receiver,
            func: Box::new(new_func),
        }
    }

    pub fn wrap<T, R, F>(self, wrapper: F) -> CancellableTask<T, R>
    where
        F: FnOnce(
            T,
            Box<(dyn FnOnce(I) -> Result<O, Cancelled> + Send + Sync)>,
            &Receiver<Cancel>,
        ) -> Result<R, Cancelled>,
        F: 'static + Send + Sync,
        T: 'static + Send + Sync,
        R: 'static + Send + Sync,
    {
        let CancellableTask {
            cancel_send,
            cancel_receiver,
            func,
        } = self;
        let new_func = {
            let cancel_receiver = cancel_receiver.clone();
            move |input: T| -> Result<R, Cancelled> { wrapper(input, func, &cancel_receiver) }
        };
        CancellableTask {
            cancel_send,
            cancel_receiver,
            func: Box::new(new_func),
        }
    }

    pub fn for_each<Item, ToIter, Iter, U, Func>(
        self,
        to_iter: ToIter,
        mut for_each: Func,
    ) -> CancellableTask<I, Vec<U>>
    where
        ToIter: FnOnce(&O) -> Iter + 'static + Send + Sync,
        Iter: IntoIterator<Item = Item> + 'static + Send + Sync,
        Func: FnMut(Item, &O) -> U + 'static + Send + Sync,
        Item: 'static + Send + Sync,
        U: 'static + Send + Sync,
    {
        self.next_with_cancel(move |ref input, cancel| {
            let iter = to_iter(input);
            if let Ok(_) = cancel.try_recv() {
                return Err(Cancelled);
            }
            let mut vec = vec![];
            for item in iter {
                let next = (for_each)(item, input);
                vec.push(next);
                if let Ok(_) = cancel.try_recv() {
                    return Err(Cancelled);
                }
            }
            Ok(vec)
        })
    }
    pub fn for_each_with_cancel<Item, ToIter, Iter, U, Func>(
        self,
        to_iter: ToIter,
        mut for_each: Func,
    ) -> CancellableTask<I, Vec<U>>
    where
        ToIter: FnOnce(&O, &Receiver<Cancel>) -> Result<Iter, Cancelled> + 'static + Send + Sync,
        Iter: IntoIterator<Item = Result<Item, Cancelled>> + 'static + Send + Sync,
        Func: FnMut(Item, &O, &Receiver<Cancel>) -> Result<U, Cancelled> + 'static + Send + Sync,
        Item: 'static + Send + Sync,
        U: 'static + Send + Sync,
    {
        self.next_with_cancel(move |ref input, cancel| {
            let iter = to_iter(input, cancel)?;
            if let Ok(_) = cancel.try_recv() {
                return Err(Cancelled);
            }
            let mut vec = vec![];
            for item in iter {
                let item = item?;
                let next = for_each(item, input, cancel)?;
                vec.push(next);
                if let Ok(_) = cancel.try_recv() {
                    return Err(Cancelled);
                }
            }
            Ok(vec)
        })
    }
}

impl<T, R, F> From<F> for CancellableTask<T, R>
where
    T: 'static + Send + Sync,
    R: 'static + Send + Sync,
    F: FnOnce(T, &Receiver<Cancel>) -> Result<R, Cancelled>,
    F: 'static + Send + Sync,
{
    fn from(value: F) -> Self {
        CancellableTask::with_cancel(value)
    }
}

/// A handle to a cancellable task
#[derive(Debug)]
pub struct CancellableTaskHandle<O> {
    handle: OnceLock<JoinHandle<Result<O, Cancelled>>>,
    canceller: Arc<Sender<Cancel>>,
    cancelled: AtomicBool,
}

impl<O> CancellableTaskHandle<O> {
    pub fn cancel(&self) -> Result<(), SendError<Cancel>> {
        if self
            .cancelled
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            == Ok(false)
        {
            return self.canceller.send(Cancel);
        }
        Ok(())
    }

    pub fn canceller(&self) -> Canceller {
        Canceller {
            canceller: self.canceller.clone(),
        }
    }

    pub fn on_cancel(&mut self, cancel: Receiver<Cancel>) {
        let canceller = self.canceller.clone();
        thread::spawn(move || {
            let Ok(cancel) = cancel.recv() else {
                return;
            };
            let _ = canceller.send(cancel);
        });
    }

    pub fn join(self) -> Result<O, WeaverError> {
        match { self }
            .handle
            .take()
            .unwrap()
            .join()
            .map_err(|_e| WeaverError::ThreadPanicked)
        {
            Ok(ok) => Ok(ok?),
            Err(err) => Err(err),
        }
    }
}
impl<O> Drop for CancellableTaskHandle<O> {
    fn drop(&mut self) {
        let _ = self.cancel();
    }
}

/// Used to cancel a task handle
#[derive(Debug)]
pub struct Canceller {
    canceller: Arc<Sender<Cancel>>,
}

impl Canceller {
    /// Cancels a remote task, remotely
    pub fn cancel(&self) -> Result<(), SendError<Cancel>> {
        self.canceller.send(Cancel)
    }
}

pub type CancelRecv = Receiver<Cancel>;

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct Cancelled;

#[derive(Debug)]
pub struct Cancel;

#[cfg(test)]
mod tests {
    use std::thread;
    use std::thread::sleep;
    use std::time::Duration;

    use crate::cancellable_task::{Cancel, CancellableTask, Cancelled};
    use crate::error::WeaverError;

    /// Join a non-cancelled task
    #[test]
    fn join_task() {
        let cancellable = CancellableTask::with_step(|i: i32| -> i32 { i * i }).start(5);
        assert!(matches!(cancellable.join(), Ok(25)));
    }

    /// Cancel a running task
    #[test]
    fn cancel_task() {
        let cancellable = CancellableTask::with_cancel(|_i: i32, cancel| -> Result<i32, _> {
            if let Ok(_cancel) = cancel.recv() {
                return Err(Cancelled);
            }
            unreachable!();
        })
        .start(0);
        cancellable.cancel().expect("could not cancel");
        assert!(matches!(cancellable.join(), Err(WeaverError::TaskCancelled)));
    }

    /// Cancel a task using a spawned canceller from a separate thread
    #[test]
    fn remote_cancel_task() {
        let cancellable = CancellableTask::with_cancel(|_i: i32, cancel| -> Result<i32, _> {
            if let Ok(_cancel) = cancel.recv() {
                return Err(Cancelled);
            }
            unreachable!();
        })
        .start(0);
        let canceller = cancellable.canceller();
        thread::spawn(move || {
            canceller.cancel().expect("could not cancel");
        })
        .join()
        .unwrap();
        assert!(matches!(cancellable.join(), Err(WeaverError::TaskCancelled)));
    }

    /// Cancel long loop
    #[test]
    fn join_task_for_loop() {
        fn fib(n: usize) -> usize {
            match n {
                0 | 1 => n,
                n => fib(n - 1) + fib(n - 2),
            }
        }

        let cancellable = CancellableTask::with_step(|i: usize| i).for_each(
            |&i| 0..i,
            |i, _| {
                fib(i);
            },
        );

        let handle = cancellable.start(100);
        sleep(Duration::from_secs(5));
        handle.cancel().expect("could not cancel");
        assert!(matches!(handle.join(), Err(WeaverError::TaskCancelled)));
    }

    /// Cancel long loop
    #[test]
    fn cancellable_iter_task_for_loop() {
        fn fib(n: usize) -> usize {
            match n {
                0 | 1 => n,
                n => fib(n - 1) + fib(n - 2),
            }
        }

        let cancellable = CancellableTask::with_step(|i: usize| i).for_each_with_cancel(
            |&i, cancel| {
                let cancel = cancel.clone();
                Ok((0..i).into_iter().map(move |i| match cancel.try_recv() {
                    Ok(Cancel) => Err(Cancelled),
                    Err(_) => Ok(fib(i)),
                }))
            },
            |fib, i, _| {
                println!("{i} => {fib}");
                Ok(())
            },
        );

        let handle = cancellable.start(100);
        sleep(Duration::from_secs(5));
        handle.cancel().expect("could not cancel");
        assert!(matches!(handle.join(), Err(WeaverError::TaskCancelled)));
    }

    #[test]
    fn cancel_after_task_finishes_errors() {
        let cancellable = CancellableTask::with_step(|i: i32| -> i32 { i * i }).start(5);
        let canceller = cancellable.canceller();
        assert!(matches!(cancellable.join(), Ok(25)));
        assert!(matches!(canceller.cancel(), Err(_)));
    }
}
