use inner::Inner;
use notifier::Notifier;
use sender::Sender;
use state::State;
use task::Task;
use worker_entry::WorkerEntry;
use worker_state::{
    WorkerState,
    WORKER_SHUTDOWN,
    WORKER_RUNNING,
    WORKER_SLEEPING,
    WORKER_NOTIFIED,
    WORKER_SIGNALED,
};

use tokio_executor;

use futures::Poll;

use std::cell::Cell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::Ordering::{AcqRel, Acquire};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Thread worker
///
/// This is passed to the `around_worker` callback set on `Builder`. This
/// callback is only expected to call `run` on it.
#[derive(Debug)]
pub struct Worker {
    // Shared scheduler data
    pub(crate) inner: Arc<Inner>,

    /// The current worker state.
    ///
    /// It can either be a primary worker or a secondary worker.
    state: ThreadState,

    // Keep the value on the current thread.
    _p: PhantomData<Rc<()>>,
}

#[derive(Debug)]
struct Primary {
    // Worker ID
    id: WorkerId,

    // Set when the worker should finalize on drop
    should_finalize: Cell<bool>,
}

/// TODO: This should probably be named `WorkerState`, but that is already
/// taken.
#[derive(Debug)]
enum ThreadState {
    Primary(Primary),
    // Backup,
}

/// Identifiers a thread pool worker.
///
/// This identifier is unique scoped by the thread pool. It is possible that
/// different thread pool instances share worker identifier values.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct WorkerId {
    pub(crate) idx: usize,
}

// Pointer to the current worker info
thread_local!(static CURRENT_WORKER: Cell<*const Worker> = Cell::new(0 as *const _));

impl Worker {
    pub(crate) fn spawn(id: WorkerId, inner: &Arc<Inner>) {
        trace!("spawning new worker thread; id={}", id.idx);

        let mut th = thread::Builder::new();

        if let Some(ref prefix) = inner.config.name_prefix {
            th = th.name(format!("{}{}", prefix, id.idx));
        }

        if let Some(stack) = inner.config.stack_size {
            th = th.stack_size(stack);
        }

        let inner = inner.clone();
        let primary = Primary::new(id);

        th.spawn(move || {
            let worker = Worker {
                inner,
                state: ThreadState::Primary(primary),
                _p: PhantomData,
            };

            // Make sure the ref to the worker does not move
            let wref = &worker;

            // Create another worker... It's ok, this is just a new type around
            // `Inner` that is expected to stay on the current thread.
            CURRENT_WORKER.with(|c| {
                c.set(wref as *const _);

                let inner = wref.inner.clone();
                let mut sender = Sender { inner };

                // Enter an execution context
                let mut enter = tokio_executor::enter().unwrap();

                tokio_executor::with_default(&mut sender, &mut enter, |enter| {
                    if let Some(ref callback) = wref.inner.config.around_worker {
                        callback.call(wref, enter);
                    } else {
                        wref.run();
                    }
                });
            });
        }).unwrap();
    }

    pub(crate) fn with_current<F: FnOnce(Option<&Worker>) -> R, R>(f: F) -> R {
        CURRENT_WORKER.with(move |c| {
            let ptr = c.get();

            if ptr.is_null() {
                f(None)
            } else {
                f(Some(unsafe { &*ptr }))
            }
        })
    }

    /// Returns a reference to the worker's identifier.
    ///
    /// This identifier is unique scoped by the thread pool. It is possible that
    /// different thread pool instances share worker identifier values.
    pub fn id(&self) -> &WorkerId {
        use self::ThreadState::Primary;

        match self.state {
            Primary(ref primary) => &primary.id,
            // _ => panic!("not currently a primary worker"),
        }
    }

    /// Returns a reference to the primary entry
    pub(crate) fn primary(&self) -> &WorkerEntry {
        &self.inner.workers[self.id().idx]
    }

    /// Transition the current worker to a blocking worker
    pub(crate) fn transition_to_blocking(&self) -> Poll<(), ::BlockingError> {
        unimplemented!();
    }

    /// Run the worker
    ///
    /// This function blocks until the worker is shutting down.
    pub fn run(&self) {
        use self::ThreadState::*;

        match self.state {
            Primary(ref primary) => {
                primary.run(&self.inner);
            }
            // _ => panic!("worker not in primary state"),
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        use self::ThreadState::Primary;

        match self.state {
            Primary(ref mut p) => p.maybe_finalize(&self.inner),
            // _ => {}
        }
    }
}

// ===== impl Primary =====

impl Primary {
    fn new(id: WorkerId) -> Primary {
        Primary {
            id,
            should_finalize: Cell::new(false),
        }
    }

    /// Run the primary worker routine
    fn run(&self, inner: &Arc<Inner>) {
        const LIGHT_SLEEP_INTERVAL: usize = 32;

        // Get the notifier.
        let notify = Arc::new(Notifier {
            inner: Arc::downgrade(inner),
        });
        let mut sender = Sender { inner: inner.clone() };

        let mut first = true;
        let mut spin_cnt = 0;
        let mut tick = 0;

        while self.check_run_state(first, inner) {
            first = false;

            // Poll inbound until empty, transfering all tasks to the internal
            // queue.
            let consistent = self.drain_inbound(inner);

            // Run the next available task
            if self.try_run_task(&notify, &mut sender, inner) {
                if tick % LIGHT_SLEEP_INTERVAL == 0 {
                    self.sleep_light(inner);
                }

                tick = tick.wrapping_add(1);
                spin_cnt = 0;

                // As long as there is work, keep looping.
                continue;
            }

            // No work in this worker's queue, it is time to try stealing.
            if self.try_steal_task(&notify, &mut sender, inner) {
                if tick % LIGHT_SLEEP_INTERVAL == 0 {
                    self.sleep_light(inner);
                }

                tick = tick.wrapping_add(1);
                spin_cnt = 0;
                continue;
            }

            if !consistent {
                spin_cnt = 0;
                continue;
            }

            // Starting to get sleeeeepy
            if spin_cnt < 61 {
                spin_cnt += 1;
            } else {
                tick = 0;

                if !self.sleep(inner) {
                    return;
                }
            }

            // If there still isn't any work to do, shutdown the worker?
        }

        self.should_finalize.set(true);
    }


    /// Checks the worker's current state, updating it as needed.
    ///
    /// Returns `true` if the worker should run.
    #[inline]
    fn check_run_state(&self, first: bool, inner: &Arc<Inner>) -> bool {
        let mut state: WorkerState = self.entry(inner).state.load(Acquire).into();

        loop {
            let pool_state: State = inner.state.load(Acquire).into();

            if pool_state.is_terminated() {
                return false;
            }

            let mut next = state;

            match state.lifecycle() {
                WORKER_RUNNING => break,
                WORKER_NOTIFIED | WORKER_SIGNALED => {
                    // transition back to running
                    next.set_lifecycle(WORKER_RUNNING);
                }
                lifecycle => panic!("unexpected worker state; lifecycle={}", lifecycle),
            }

            let actual = self.entry(inner).state.compare_and_swap(
                state.into(), next.into(), AcqRel).into();

            if actual == state {
                break;
            }

            state = actual;
        }

        // If this is the first iteration of the worker loop, then the state can
        // be signaled.
        if !first && state.is_signaled() {
            trace!("Worker::check_run_state; delegate signal");
            // This worker is not ready to be signaled, so delegate the signal
            // to another worker.
            inner.signal_work(inner);
        }

        true
    }

    /// Runs the next task on this worker's queue.
    ///
    /// Returns `true` if work was found.
    #[inline]
    fn try_run_task(&self,
                    notify: &Arc<Notifier>,
                    sender: &mut Sender,
                    inner: &Arc<Inner>) -> bool
    {
        use deque::Steal::*;

        // Poll the internal queue for a task to run
        match self.entry(inner).deque.steal() {
            Data(task) => {
                self.run_task(task, notify, sender, inner);
                true
            }
            Empty => false,
            Retry => true,
        }
    }

    /// Tries to steal a task from another worker.
    ///
    /// Returns `true` if work was found
    #[inline]
    fn try_steal_task(&self,
                      notify: &Arc<Notifier>,
                      sender: &mut Sender,
                      inner: &Arc<Inner>)
        -> bool
    {
        use deque::Steal::*;

        let len = inner.workers.len();
        let mut idx = inner.rand_usize() % len;
        let mut found_work = false;
        let start = idx;

        loop {
            if idx < len {
                match inner.workers[idx].steal.steal() {
                    Data(task) => {
                        trace!("stole task");

                        self.run_task(task, notify, sender, inner);

                        trace!("try_steal_task -- signal_work; self={:?}; from={}",
                               self, idx);

                        // Signal other workers that work is available
                        inner.signal_work(inner);

                        return true;
                    }
                    Empty => {}
                    Retry => found_work = true,
                }

                idx += 1;
            } else {
                idx = 0;
            }

            if idx == start {
                break;
            }
        }

        found_work
    }

    fn run_task(&self,
                task: Task,
                notify: &Arc<Notifier>,
                sender: &mut Sender,
                inner: &Arc<Inner>)
    {
        use task::Run::*;

        match task.run(notify, sender) {
            Idle => {}
            Schedule => {
                self.entry(inner).push_internal(task);
            }
            Complete => {
                let mut state: State = inner.state.load(Acquire).into();

                loop {
                    let mut next = state;
                    next.dec_num_futures();

                    let actual = inner.state.compare_and_swap(
                        state.into(), next.into(), AcqRel).into();

                    if actual == state {
                        trace!("task complete; state={:?}", next);

                        if state.num_futures() == 1 {
                            // If the thread pool has been flagged as shutdown,
                            // start terminating workers. This involves waking
                            // up any sleeping worker so that they can notice
                            // the shutdown state.
                            if next.is_terminated() {
                                inner.terminate_sleeping_workers();
                            }
                        }

                        // The worker's run loop will detect the shutdown state
                        // next iteration.
                        return;
                    }

                    state = actual;
                }
            }
        }
    }

    /// Drains all tasks on the extern queue and pushes them onto the internal
    /// queue.
    ///
    /// Returns `true` if the operation was able to complete in a consistent
    /// state.
    #[inline]
    fn drain_inbound(&self, inner: &Arc<Inner>) -> bool {
        use task::Poll::*;

        let mut found_work = false;

        loop {
            let task = unsafe { self.entry(inner).inbound.poll() };

            match task {
                Empty => {
                    if found_work {
                        trace!("found work while draining; signal_work");
                        inner.signal_work(inner);
                    }

                    return true;
                }
                Inconsistent => {
                    if found_work {
                        trace!("found work while draining; signal_work");
                        inner.signal_work(inner);
                    }

                    return false;
                }
                Data(task) => {
                    found_work = true;
                    self.entry(inner).push_internal(task);
                }
            }
        }
    }

    /// Put the worker to sleep
    ///
    /// Returns `true` if woken up due to new work arriving.
    fn sleep(&self, inner: &Arc<Inner>) -> bool {
        trace!("Worker::sleep; worker={:?}", self);

        let mut state: WorkerState = self.entry(inner).state.load(Acquire).into();

        // The first part of the sleep process is to transition the worker state
        // to "pushed". Now, it may be that the worker is already pushed on the
        // sleeper stack, in which case, we don't push again.

        loop {
            let mut next = state;

            match state.lifecycle() {
                WORKER_RUNNING => {
                    // Try setting the pushed state
                    next.set_pushed();

                    // Transition the worker state to sleeping
                    next.set_lifecycle(WORKER_SLEEPING);
                }
                WORKER_NOTIFIED | WORKER_SIGNALED => {
                    // No need to sleep, transition back to running and move on.
                    next.set_lifecycle(WORKER_RUNNING);
                }
                actual => panic!("unexpected worker state; {}", actual),
            }

            let actual = self.entry(inner).state.compare_and_swap(
                state.into(), next.into(), AcqRel).into();

            if actual == state {
                if state.is_notified() {
                    // The previous state was notified, so we don't need to
                    // sleep.
                    return true;
                }

                if !state.is_pushed() {
                    debug_assert!(next.is_pushed());

                    trace!("  sleeping -- push to stack; worker={:?}", self);

                    // We obtained permission to push the worker into the
                    // sleeper queue.
                    if let Err(_) = inner.push_sleeper(self.id.idx) {
                        trace!("  sleeping -- push to stack failed; idx={}", self.id.idx);
                        // The push failed due to the pool being terminated.
                        //
                        // This is true because the "work" being woken up for is
                        // shutting down.
                        return true;
                    }
                }

                break;
            }

            state = actual;
        }

        trace!("    -> starting to sleep; idx={}", self.id.idx);

        let sleep_until = inner.config.keep_alive
            .map(|dur| Instant::now() + dur);

        // The state has been transitioned to sleeping, we can now wait by
        // calling the parker. This is done in a loop as condvars can wakeup
        // spuriously.
        loop {
            let mut drop_thread = false;

            match sleep_until {
                Some(when) => {
                    let now = Instant::now();

                    if when >= now {
                        drop_thread = true;
                    }

                    let dur = when - now;

                    unsafe {
                        (*self.entry(inner).park.get())
                            .park_timeout(dur)
                            .unwrap();
                    }
                }
                None => {
                    unsafe {
                        (*self.entry(inner).park.get())
                            .park()
                            .unwrap();
                    }
                }
            }

            trace!("    -> wakeup; idx={}", self.id.idx);

            // Reload the state
            state = self.entry(inner).state.load(Acquire).into();

            loop {
                match state.lifecycle() {
                    WORKER_SLEEPING => {}
                    WORKER_NOTIFIED | WORKER_SIGNALED => {
                        // Transition back to running
                        loop {
                            let mut next = state;
                            next.set_lifecycle(WORKER_RUNNING);

                            let actual = self.entry(inner).state.compare_and_swap(
                                state.into(), next.into(), AcqRel).into();

                            if actual == state {
                                return true;
                            }

                            state = actual;
                        }
                    }
                    _ => unreachable!(),
                }

                if !drop_thread {
                    // This goees back to the outer loop.
                    break;
                }

                let mut next = state;
                next.set_lifecycle(WORKER_SHUTDOWN);

                let actual = self.entry(inner).state.compare_and_swap(
                    state.into(), next.into(), AcqRel).into();

                if actual == state {
                    // Transitioned to a shutdown state
                    return false;
                }

                state = actual;
            }

            // The worker hasn't been notified, go back to sleep
        }
    }

    /// This doesn't actually put the thread to sleep. It calls
    /// `park.park_timeout` with a duration of 0. This allows the park
    /// implementation to perform any work that might be done on an interval.
    fn sleep_light(&self, inner: &Arc<Inner>) {
        unsafe {
            (*self.entry(inner).park.get())
                .park_timeout(Duration::from_millis(0))
                .unwrap();
        }
    }

    fn entry<'a>(&self, inner: &'a Arc<Inner>) -> &'a WorkerEntry {
        &inner.workers[self.id.idx]
    }

    fn maybe_finalize(&mut self, inner: &Arc<Inner>) {
        if self.should_finalize.get() {
            // Drain all work
            self.drain_inbound(inner);

            while let Some(_) = self.entry(inner).deque.pop() {
            }

            // TODO: Drain the work queue...
            inner.worker_terminated();
        }
    }
}

// ===== impl WorkerId =====

impl WorkerId {
    pub(crate) fn new(idx: usize) -> WorkerId {
        WorkerId { idx }
    }
}
