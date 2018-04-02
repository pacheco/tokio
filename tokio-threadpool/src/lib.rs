//! A work-stealing based thread pool for executing futures.

// # Implementation thoughts
//
// Gotta go back and document it all.
//
// ## Blocking
//
// Blockign requirements are:
//
// * A backup pool of worker threds.
//      * Threads shutdown when idle.
// * At most `n` active async workers
// * Task nodes can form a queue of "pending blocking"
//      * When assigned capacity, the capacity must be used or it is lost.
//
// * Create a set of blocking workers
// * Inner has the pending blocking queue.

#![doc(html_root_url = "https://docs.rs/tokio-threadpool/0.1.2")]
#![deny(warnings, missing_docs, missing_debug_implementations)]

extern crate tokio_executor;

extern crate crossbeam_deque as deque;
#[macro_use]
extern crate futures;
extern crate num_cpus;
extern crate rand;

#[macro_use]
extern crate log;

#[cfg(feature = "unstable-futures")]
extern crate futures2;

pub mod park;

mod backup;
mod blocking;
mod builder;
mod callback;
mod config;
mod inner;
#[cfg(feature = "unstable-futures")]
mod futures2_wake;
mod notifier;
mod sender;
mod shutdown;
mod shutdown_task;
mod sleep_stack;
mod state;
mod task;
mod thread_pool;
mod worker;
mod worker_entry;
mod worker_state;

pub use blocking::{BlockingError, blocking};
pub use builder::Builder;
pub use sender::Sender;
pub use shutdown::Shutdown;
pub use thread_pool::ThreadPool;
pub use worker::Worker;
