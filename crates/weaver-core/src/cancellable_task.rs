//! A task that can be cancelled at any point.
//!
//! Ideally this would just be split into many parts that

use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

use crossbeam::channel::{bounded, Receiver, Sender, SendError};

use crate::error::Error;

/// A task that can be cancelled
///
/// Useful for having potentially long running tasks
pub struct CancellableTask<I, O> {
    cancel_send: Sender<Cancel>,
    cancel_receiver: Receiver<Cancel>,
    func: Box<dyn FnMut(I) -> Result<O, Cancelled> + Send + Sync>,
}

impl<I, O> Debug for CancellableTask<I, O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancellableTask")
         .finish_non_exhaustive()
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
}

impl<I: Send + 'static, O: Send + 'static> CancellableTask<I, O> {
    pub fn start(self, input: I) -> CancellableTaskHandle<O>
    {
        let CancellableTask { cancel_send, cancel_receiver: _, mut func } = self;
        CancellableTaskHandle {
            handle: thread::spawn(move || {
                func(input)
            }),
            canceller: Arc::new(cancel_send),
        }
    }

    pub fn start_with_name(self, input: I, name: String) -> Result<CancellableTaskHandle<O>, Error>
    {
        let CancellableTask { cancel_send, cancel_receiver: _, mut func } = self;
        Ok(CancellableTaskHandle {
            handle: thread::Builder::new().name(name).spawn(move || {
                func(input)
            })?,
            canceller: Arc::new(cancel_send),
        })
    }
    pub fn with_step<F>(mut step: F) -> Self
        where F: FnMut(I) -> O + 'static + Send + Sync
    {
        let (sender, receiver) = bounded(0);
        Self {
            cancel_send: sender,
            cancel_receiver: receiver,
            func: Box::new(move |i| Ok(step(i))),
        }
    }
    pub fn with_cancel<F>(mut step: F) -> Self
        where F: FnMut(I, &Receiver<Cancel>) -> Result<O, Cancelled> + 'static + Send + Sync
    {
        let (sender, receiver) = bounded(0);
        Self {
            cancel_send: sender,
            cancel_receiver: receiver.clone(),
            func: Box::new(move |i| {
                step(i, &receiver)
            }),
        }
    }

    pub fn next<F, U>(mut self, mut step: F) -> CancellableTask<I, U>
        where F: FnMut(O) -> U + 'static + Send + Sync
    {
        let CancellableTask { cancel_send, cancel_receiver, mut func } = self;
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

    pub fn next_with_cancel<F, U>(mut self, mut step: F) -> CancellableTask<I, U>
        where F: FnMut(O, &Receiver<Cancel>) -> Result<U, Cancelled> + 'static + Send + Sync
    {
        let CancellableTask { cancel_send, cancel_receiver, mut func } = self;
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

    pub fn wrap<T, R, F>(self, mut wrapper: F) -> CancellableTask<T, R>
        where F: FnMut(T, &mut (dyn FnMut(I) -> Result<O, Cancelled> + Send + Sync), &Receiver<Cancel>) -> Result<R, Cancelled>,
              F: 'static + Send + Sync,
              T: 'static + Send + Sync,
              R: 'static + Send + Sync
    {
        let CancellableTask { cancel_send, cancel_receiver, mut func } = self;
        let new_func = {
            let cancel_receiver = cancel_receiver.clone();
            move |input: T| -> Result<R, Cancelled> {
                wrapper(input, &mut *func, &cancel_receiver)
            }
        };
        CancellableTask {
            cancel_send,
            cancel_receiver,
            func: Box::new(new_func),
        }
    }

    pub fn for_each<Item, ToIter, Iter, U, Func>(self, mut to_iter: ToIter, mut for_each: Func) -> CancellableTask<I, Vec<U>>
        where
            ToIter: FnMut(&O) -> Iter + 'static + Send + Sync,
            Iter: IntoIterator<Item=Item> + 'static + Send + Sync,
            Func: FnMut(Item, &O) -> U + 'static + Send + Sync,
            Item: 'static + Send + Sync,
            U: 'static + Send + Sync
    {
        self.next_with_cancel(move |ref input, cancel| {
            let iter = to_iter(input);
            if let Ok(_) = cancel.try_recv() {
                return Err(Cancelled);
            }
            let mut vec = vec![];
            for item in iter {
                let next = for_each(item, input);
                vec.push(next);
                if let Ok(_) = cancel.try_recv() {
                    return Err(Cancelled);
                }
            }
            Ok(vec)
        })
    }
    pub fn for_each_with_cancel<Item, ToIter, Iter, U, Func>(self, mut to_iter: ToIter, mut for_each: Func) -> CancellableTask<I, Vec<U>>
        where
            ToIter: FnMut(&O, &Receiver<Cancel>) -> Result<Iter, Cancelled> + 'static + Send + Sync,
            Iter: IntoIterator<Item=Result<Item, Cancelled>> + 'static + Send + Sync,
            Func: FnMut(Item, &O, &Receiver<Cancel>) -> Result<U, Cancelled> + 'static + Send + Sync,
            Item: 'static + Send + Sync,
            U: 'static + Send + Sync
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
    where T: 'static + Send + Sync,
          R: 'static + Send + Sync,
          F: FnMut(T, &Receiver<Cancel>) -> Result<R, Cancelled>,
          F: 'static + Send + Sync {
    fn from(value: F) -> Self {
        CancellableTask::with_cancel(value)
    }
}


/// A handle to a cancellable task
#[derive(Debug)]
pub struct CancellableTaskHandle<O> {
    handle: JoinHandle<Result<O, Cancelled>>,
    canceller: Arc<Sender<Cancel>>,
}

impl<O> CancellableTaskHandle<O> {
    pub fn cancel(&self) -> Result<(), SendError<Cancel>> {
        self.canceller.send(Cancel)
    }

    pub fn canceller(&self) -> Canceller {
        Canceller { canceller: self.canceller.clone() }
    }

    pub fn join(self) -> thread::Result<Result<O, Cancelled>> {
        self.handle.join()
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

    /// Join a non-cancelled task
    #[test]
    fn join_task() {
        let cancellable = CancellableTask::with_step(|i: i32| -> i32 {
            i * i
        }).start(5);
        assert!(matches!(cancellable.join(), Ok(Ok(25))));
    }

    /// Cancel a running task
    #[test]
    fn cancel_task() {
        let cancellable = CancellableTask::with_cancel(|i: i32, cancel| -> Result<i32, _> {
            if let Ok(cancel) = cancel.recv() {
                return Err(Cancelled);
            }
            unreachable!();
        }).start(0);
        cancellable.cancel().expect("could not cancel");
        assert!(matches!(cancellable.join(), Ok(Err(Cancelled))));
    }

    /// Cancel a task using a spawned canceller from a separate thread
    #[test]
    fn remote_cancel_task() {
        let cancellable = CancellableTask::with_cancel(|i: i32, cancel| -> Result<i32, _> {
            if let Ok(cancel) = cancel.recv() {
                return Err(Cancelled);
            }
            unreachable!();
        }).start(0);
        let canceller = cancellable.canceller();
        thread::spawn(move || {
            canceller.cancel().expect("could not cancel");
        }).join().unwrap();
        assert!(matches!(cancellable.join(), Ok(Err(Cancelled))));
    }

    /// Cancel long loop
    #[test]
    fn join_task_for_loop() {
        fn fib(n: usize) -> usize {
            match n {
                0 | 1 => n,
                n => fib(n - 1) + fib(n - 2)
            }
        }

        let cancellable = CancellableTask::with_step(|i: usize| { i })
            .for_each(|&i| 0..i, |i, _| {
                ;
                fib(i);
            });

        let handle = cancellable.start(100);
        sleep(Duration::from_secs(5));
        handle.cancel().expect("could not cancel");
        assert!(matches!(handle.join(), Ok(Err(Cancelled))));
    }

    /// Cancel long loop
    #[test]
    fn cancellable_iter_task_for_loop() {
        fn fib(n: usize) -> usize {
            match n {
                0 | 1 => n,
                n => fib(n - 1) + fib(n - 2)
            }
        }

        let cancellable = CancellableTask::with_step(|i: usize| { i })
            .for_each_with_cancel(|&i, cancel| {
                let cancel = cancel.clone();
                Ok((0..i).into_iter().map(move |i| {
                    match cancel.try_recv() {
                        Ok(Cancel) => { Err(Cancelled) }
                        Err(_) => { Ok(fib(i)) }
                    }
                })
                )
            }, |fib, i, _| {
                println!("{i} => {fib}");
                Ok(())
            });

        let handle = cancellable.start(100);
        sleep(Duration::from_secs(5));
        handle.cancel().expect("could not cancel");
        assert!(matches!(handle.join(), Ok(Err(Cancelled))));
    }

    #[test]
    fn cancel_after_task_finishes_errors() {
        let cancellable = CancellableTask::with_step(|i: i32| -> i32 {
            i * i
        }).start(5);
        let canceller = cancellable.canceller();
        assert!(matches!(cancellable.join(), Ok(Ok(25))));
        assert!(matches!(canceller.cancel(), Err(_)));
    }
}