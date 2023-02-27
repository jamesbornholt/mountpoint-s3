#[cfg(not(all(feature = "shuttle", test)))]
mod std {
    pub use std::sync::*;
    pub use std::thread;

    pub use async_lock::Mutex as AsyncMutex;
    pub use async_lock::RwLock as AsyncRwLock;

    pub use async_channel;
}

#[cfg(not(all(feature = "shuttle", test)))]
pub use self::std::*;

#[cfg(all(feature = "shuttle", test))]
mod shuttle {
    pub use ::shuttle::sync::*;
    pub use ::shuttle::thread;

    // TODO these might need a richer Shuttle mock
    pub use async_channel;
    pub use async_lock::Mutex as AsyncMutex;
    pub use async_lock::RwLock as AsyncRwLock;

    /// Shuttle async runtime
    pub struct ShuttleRuntime;

    impl crate::future::Spawn for ShuttleRuntime {
        type JoinHandle<T> = ::shuttle::future::JoinHandle<T>;

        fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
        where
            F: futures::Future + Send + 'static,
            F::Output: Send + 'static,
        {
            ::shuttle::future::spawn(future)
        }

        fn block_on<F: futures::Future>(&self, future: F) -> F::Output {
            ::shuttle::future::block_on(future)
        }
    }
}

#[cfg(all(feature = "shuttle", test))]
pub use self::shuttle::*;
