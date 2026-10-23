//! Injectable monotonic time for leases, retries, and deterministic protocol tests.

use std::time::Instant;

#[cfg(any(test, feature = "test-support"))]
use std::sync::atomic::{AtomicU64, Ordering};

/// Monotonic nanoseconds from an unspecified local origin.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct MonotonicTime(u64);

impl MonotonicTime {
    /// Creates a monotonic time from nanoseconds since the clock's origin.
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    /// Returns nanoseconds since the clock's origin.
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// Adds a duration, saturating at the representable maximum.
    pub const fn saturating_add_nanos(self, nanos: u64) -> Self {
        Self(self.0.saturating_add(nanos))
    }
}

/// Clock interface injected into lease and retry protocols.
pub trait Clock: Send + Sync {
    /// Returns the current monotonic time.
    fn now(&self) -> MonotonicTime;
}

/// Production monotonic clock backed by `std::time::Instant`.
#[derive(Debug)]
pub struct SystemMonotonicClock {
    origin: Instant,
}

impl Default for SystemMonotonicClock {
    fn default() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Clock for SystemMonotonicClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime(self.origin.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64)
    }
}

/// Deterministic clock available to tests and fixture builders.
#[cfg(any(test, feature = "test-support"))]
#[derive(Debug, Default)]
pub struct ManualClock(AtomicU64);

#[cfg(any(test, feature = "test-support"))]
impl ManualClock {
    /// Creates a manual clock at a specific monotonic nanosecond value.
    pub const fn at(nanos: u64) -> Self {
        Self(AtomicU64::new(nanos))
    }

    /// Sets the clock to an exact monotonic nanosecond value.
    pub fn set(&self, nanos: u64) {
        self.0.store(nanos, Ordering::SeqCst);
    }

    /// Advances time by nanoseconds and returns the new value.
    pub fn advance(&self, nanos: u64) -> MonotonicTime {
        let mut current = self.0.load(Ordering::SeqCst);
        loop {
            let next = current.saturating_add(nanos);
            match self
                .0
                .compare_exchange_weak(current, next, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return MonotonicTime(next),
                Err(observed) => current = observed,
            }
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
impl Clock for ManualClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime(self.0.load(Ordering::SeqCst))
    }
}
