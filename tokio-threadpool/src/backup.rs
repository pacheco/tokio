use sleep_stack::{
    SleepStack,
    // EMPTY,
    TERMINATED,
};

use std::cell::UnsafeCell;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, AcqRel, Relaxed};

/* Backup workers
 *
 * When a task on an primary worker wishes to perform some "blocking" work, it
 * must transition to a backup worker.
 *
 * This transition does not happen by moving any state around. Instead, the
 * current thread hands off its "primary" role to a thread waiting in the backup
 * pool. At this point, the backup thread switches to a primary worker and
 * resumes processing the task queue.
 */

/// Stack of available backup workers.
#[derive(Debug)]
pub(crate) struct Backup {
    entries: Box<[Entry]>,

    sleep_stack: AtomicUsize,
}

#[derive(Debug)]
pub(crate) struct Entry {
    /// Backup worker state
    state: AtomicUsize,

    /// Next entry in the parked Trieber stack
    next_sleeper: UnsafeCell<usize>,
}

#[derive(Debug)]
struct State(usize);

/* The least significant bit is a flag indicating if the entry is pushed onto
 * the sleep stack.
 */

/// Set when the worker is pushed onto the scheduler's stack of sleeping
/// threads.
pub(crate) const PUSHED_MASK: usize = 0b001;

/// Manages the worker lifecycle part of the state
const LIFECYCLE_MASK: usize = 0b110;
const LIFECYCLE_SHIFT: usize = 1;

// const RUNNING: usize = 1 << LIFECYCLE_SHIFT;

const SHUTDOWN: usize = 2 << LIFECYCLE_SHIFT;

// ===== impl Backup =====

impl Backup {
    pub fn new(size: usize) -> Backup {
        let mut entries = vec![];

        for _ in 0..size {
            let mut state = State::new();
            state.set_pushed();
            state.set_lifecycle(SHUTDOWN);

            entries.push(Entry::new(state));
        }

        let ret = Backup {
            entries: entries.into_boxed_slice(),
            sleep_stack: AtomicUsize::new(0),
        };

        for i in 0..size {
            ret.push_sleeper(i).unwrap();
        }

        ret
    }

    /// Push a backup entry onto the sleep stack.
    ///
    /// Returns `Err` if the pool has been terminated
    fn push_sleeper(&self, idx: usize) -> Result<(), ()> {
        let mut state: SleepStack = self.sleep_stack.load(Acquire).into();

        debug_assert!(State::from(self.entries[idx].state.load(Relaxed)).is_pushed());

        loop {
            let mut next = state;

            let head = state.head();

            if head == TERMINATED {
                // The pool is terminated, cannot push the sleeper.
                return Err(());
            }

            self.entries[idx].set_next_sleeper(head);
            next.set_head(idx);

            let actual = self.sleep_stack.compare_and_swap(
                state.into(), next.into(), AcqRel).into();

            if state == actual {
                return Ok(());
            }

            state = actual;
        }
    }
}

// ===== impl Entry =====

impl Entry {
    /// Create a new `Entry`.
    fn new(state: State) -> Entry {
        Entry {
            state: AtomicUsize::new(state.into()),
            next_sleeper: UnsafeCell::new(0),
        }
    }

    #[inline]
    fn set_next_sleeper(&self, val: usize) {
        unsafe { *self.next_sleeper.get() = val; }
    }
}

// ===== impl State =====

impl State {
    pub fn new() -> Self {
        State(0)
    }

    /// Returns true if the worker entry is pushed in the sleeper stack
    pub fn is_pushed(&self) -> bool {
        self.0 & PUSHED_MASK == PUSHED_MASK
    }

    pub fn set_pushed(&mut self) {
        self.0 |= PUSHED_MASK
    }

    pub fn lifecycle(&self) -> usize {
        self.0 & LIFECYCLE_MASK
    }

    pub fn set_lifecycle(&mut self, val: usize) {
        self.0 = (self.0 & !LIFECYCLE_MASK) | val
    }
}

impl From<usize> for State {
    fn from(val: usize) -> Self {
        State(val)
    }
}

impl From<State> for usize {
    fn from(val: State) -> Self {
        val.0
    }
}
