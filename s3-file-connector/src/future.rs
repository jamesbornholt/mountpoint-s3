use std::future::Future;

use futures::task::SpawnExt;
use futures::FutureExt;

use crate::sync::Arc;

pub trait Spawn {
    type JoinHandle<T>;

    fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;

    fn block_on<F: Future>(&self, future: F) -> F::Output;
}

impl Spawn for futures::executor::ThreadPool {
    type JoinHandle<T> = ();

    fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let future = future.map(|_| ());
        SpawnExt::spawn(&self, future).unwrap()
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        futures::executor::block_on(future)
    }
}

impl Spawn for tokio::runtime::Runtime {
    type JoinHandle<T> = tokio::task::JoinHandle<T>;

    fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.spawn(future)
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.block_on(future)
    }
}

impl<S: Spawn> Spawn for Arc<S> {
    type JoinHandle<T> = S::JoinHandle<T>;

    fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.as_ref().spawn(future)
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.as_ref().block_on(future)
    }
}
