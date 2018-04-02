use worker::Worker;

use futures::Poll;

/// Error raised by `blocking`.
#[derive(Debug)]
pub struct BlockingError {
    _p: (),
}

/// Enter a blocking section of code.
///
/// # Panics
///
/// This function panics if not called from the context of a thread pool worker.
pub fn blocking<T>() -> Poll<T, BlockingError> {
    Worker::with_current(|worker| {
        let worker = worker.expect("not called from a runtime thread");

        try_ready!(worker.transition_to_blocking());

        unimplemented!();
    })
}
