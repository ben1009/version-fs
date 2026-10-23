//! Named failure points shared by storage adapters and protocol tests.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Mutex;

/// Named fault-injection points. Values describe protocol boundaries, not implementation APIs.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum FailurePoint {
    /// Before an immutable object upload is issued.
    ObjectUploadBeforeRequest,
    /// After the store commits an upload but before the caller receives the reply.
    ObjectUploadAfterCommitBeforeReply,
    /// Before a Metadata Authority transaction commits.
    AuthorityBeforeCommit,
    /// After an authority commit but before the caller receives the reply.
    AuthorityAfterCommitBeforeReply,
    /// Before a block request is executed by the device adapter.
    DeviceBeforeExecute,
    /// After device state changes but before request completion.
    DeviceAfterExecuteBeforeCompletion,
    /// Before filesystem freeze begins.
    FilesystemFreezeBefore,
    /// After filesystem freeze succeeds but before capture proceeds.
    FilesystemFreezeAfter,
    /// Before a protocol response is sent to its caller.
    ReplyBeforeSend,
}

/// Error returned when a named failure point is armed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InjectedFailure(pub FailurePoint);

impl fmt::Display for InjectedFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "injected failure at {:?}", self.0)
    }
}

impl std::error::Error for InjectedFailure {}

/// Interface used to inject deterministic faults at protocol boundaries.
pub trait FailureInjector: Send + Sync {
    /// Checks a named point and returns an error when that point is armed.
    fn check(&self, point: FailurePoint) -> Result<(), InjectedFailure>;
}

/// Failure injector that never fails.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoFailures;

impl FailureInjector for NoFailures {
    fn check(&self, _point: FailurePoint) -> Result<(), InjectedFailure> {
        Ok(())
    }
}

/// One-shot, thread-safe failure injector for deterministic tests.
#[derive(Debug, Default)]
pub struct OneShotFailures {
    armed: Mutex<BTreeSet<FailurePoint>>,
}

impl OneShotFailures {
    /// Arms one failure point. Re-arming an existing point has no additional effect.
    pub fn arm(&self, point: FailurePoint) {
        self.armed
            .lock()
            .expect("failure injector mutex poisoned")
            .insert(point);
    }

    /// Clears all armed failure points.
    pub fn clear(&self) {
        self.armed
            .lock()
            .expect("failure injector mutex poisoned")
            .clear();
    }
}

impl FailureInjector for OneShotFailures {
    fn check(&self, point: FailurePoint) -> Result<(), InjectedFailure> {
        if self
            .armed
            .lock()
            .expect("failure injector mutex poisoned")
            .remove(&point)
        {
            Err(InjectedFailure(point))
        } else {
            Ok(())
        }
    }
}
