//! `toy-kv-engine` adapter for the Version FS Metadata Authority contract.
//!
//! This adapter assumes one process owns the engine and every mutation enters
//! through this type. The authority commit gate serializes each complete
//! point-transaction body through commit or unknown-outcome reconciliation.
//! Object-store I/O and other preparation must happen before calling `transact`.

use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use kv_engine::{
    iterators::StorageIterator,
    lsm_storage::{KvEngine, LsmStorageOptions},
    mvcc::{snapshot::Snapshot, txn::Transaction},
};
use versionfs_core::{
    fault::{FailureInjector, FailurePoint, NoFailures},
    metadata::{
        MetadataAuthority, MetadataCursor, MetadataError, MetadataPage, MetadataSnapshot,
        MetadataTxn, MutationIdentity, ReconciledOutcome,
    },
    time::{Clock, SystemMonotonicClock},
};

const ENCODED_VALUE_PREFIX: u8 = 0;
static NEXT_SNAPSHOT_SCOPE: AtomicU64 = AtomicU64::new(1);

/// Single-owner Metadata Authority backed by a WAL-enabled toy-kv engine.
pub struct ToyKvMetadataAuthority {
    path: PathBuf,
    options: LsmStorageOptions,
    engine: RwLock<Arc<KvEngine>>,
    authority_gate: RwLock<MutationGate>,
    active_snapshots: Arc<AtomicUsize>,
    clock: Arc<dyn Clock>,
    failures: Arc<dyn FailureInjector>,
}

#[derive(Default)]
struct MutationGate {
    unresolved: Option<UnresolvedMutation>,
}

struct UnresolvedMutation {
    operation_id: Vec<u8>,
    outcome_key: Vec<u8>,
    requires_reopen: bool,
}

/// A pinned toy-kv snapshot used for all pages in one scan.
pub struct ToyKvSnapshot {
    snapshot: Snapshot,
    scope: u64,
    active_snapshots: Arc<AtomicUsize>,
}

impl MetadataSnapshot for ToyKvSnapshot {
    fn cursor_scope(&self) -> u64 {
        self.scope
    }
}

impl Drop for ToyKvSnapshot {
    fn drop(&mut self) {
        self.active_snapshots.fetch_sub(1, Ordering::AcqRel);
    }
}

impl ToyKvMetadataAuthority {
    /// Open or create the metadata engine at `path` with production adapter
    /// defaults: WAL enabled, serializable point transactions, and no compaction
    /// assumptions beyond toy-kv's own defaults.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MetadataError> {
        Self::open_with(
            path,
            Arc::new(SystemMonotonicClock::default()),
            Arc::new(NoFailures),
        )
    }

    /// Open the engine with injected clock and failure boundaries.
    ///
    /// The failure injector and clock are public protocol dependencies so tests
    /// can verify lease ordering and unknown commit recovery deterministically.
    pub fn open_with(
        path: impl AsRef<Path>,
        clock: Arc<dyn Clock>,
        failures: Arc<dyn FailureInjector>,
    ) -> Result<Self, MetadataError> {
        let path = path.as_ref().to_path_buf();
        let options = LsmStorageOptions {
            enable_wal: true,
            serializable: true,
            ..LsmStorageOptions::default()
        };
        let engine = KvEngine::open(&path, options.clone()).map_err(backend_error)?;
        Ok(Self {
            path,
            options,
            engine: RwLock::new(engine),
            authority_gate: RwLock::new(MutationGate::default()),
            active_snapshots: Arc::new(AtomicUsize::new(0)),
            clock,
            failures,
        })
    }

    fn write_gate(&self) -> Result<std::sync::RwLockWriteGuard<'_, MutationGate>, MetadataError> {
        self.authority_gate
            .write()
            .map_err(|_| MetadataError::Backend("authority gate poisoned".into()))
    }

    fn read_gate(&self) -> Result<std::sync::RwLockReadGuard<'_, MutationGate>, MetadataError> {
        self.authority_gate
            .read()
            .map_err(|_| MetadataError::Backend("authority gate poisoned".into()))
    }

    fn engine_read(&self) -> Result<std::sync::RwLockReadGuard<'_, Arc<KvEngine>>, MetadataError> {
        self.engine
            .read()
            .map_err(|_| MetadataError::Backend("engine handle lock poisoned".into()))
    }

    fn engine_write(
        &self,
    ) -> Result<std::sync::RwLockWriteGuard<'_, Arc<KvEngine>>, MetadataError> {
        self.engine
            .write()
            .map_err(|_| MetadataError::Backend("engine handle lock poisoned".into()))
    }

    fn validate_operation(operation: MutationIdentity<'_>) -> Result<(), MetadataError> {
        if operation.operation_id.is_empty() {
            return Err(MetadataError::InvalidRequest("operation ID is empty"));
        }
        if operation.outcome_key.is_empty() {
            return Err(MetadataError::InvalidRequest("outcome key is empty"));
        }
        Ok(())
    }

    fn check_unresolved(gate: &MutationGate) -> Result<(), MetadataError> {
        if let Some(unresolved) = &gate.unresolved {
            return Err(MetadataError::ReconciliationRequired {
                operation_id: unresolved.operation_id.clone(),
            });
        }
        Ok(())
    }

    fn recover_engine(&self) -> Result<(), MetadataError> {
        let mut engine = self.engine_write()?;
        if self.active_snapshots.load(Ordering::Acquire) != 0 {
            return Err(MetadataError::Backend(
                "drop authority snapshots before recovering the engine".into(),
            ));
        }
        engine.close().map_err(backend_error)?;
        *engine = KvEngine::open(&self.path, self.options.clone()).map_err(backend_error)?;
        Ok(())
    }
}

impl MetadataAuthority for ToyKvMetadataAuthority {
    type Snapshot = ToyKvSnapshot;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetadataError> {
        let gate = self.read_gate()?;
        Self::check_unresolved(&gate)?;
        let engine = self.engine_read()?;
        engine
            .get(key)
            .map_err(backend_error)?
            .map(|value| decode_value(value.as_ref()))
            .transpose()
    }

    fn transact<R, F>(
        &self,
        operation: MutationIdentity<'_>,
        mut body: F,
    ) -> Result<R, MetadataError>
    where
        F: FnMut(&mut dyn MetadataTxn) -> Result<R, MetadataError>,
    {
        Self::validate_operation(operation)?;
        let mut gate = self.write_gate()?;
        Self::check_unresolved(&gate)?;

        let engine_guard = self.engine_read()?;
        let engine = Arc::clone(&engine_guard);
        drop(engine_guard);
        let transaction = engine.new_txn().map_err(backend_error)?;
        let mut metadata_txn = ToyKvTransaction {
            transaction: &transaction,
            clock: self.clock.as_ref(),
            outcome_key: operation.outcome_key,
            outcome_written: false,
        };
        if metadata_txn.get(operation.outcome_key)?.is_some() {
            return Err(MetadataError::Conflict);
        }
        let result = body(&mut metadata_txn)?;
        if !metadata_txn.outcome_written {
            return Err(MetadataError::InvalidRequest(
                "mutation did not write its terminal outcome record",
            ));
        }

        self.failures
            .check(FailurePoint::AuthorityBeforeCommit)
            .map_err(|error| MetadataError::Rejected(error.to_string()))?;

        if let Err(error) = self
            .failures
            .check(FailurePoint::AuthorityCommitOutcomeUnknownWithoutCommit)
        {
            gate.unresolved = Some(UnresolvedMutation {
                operation_id: operation.operation_id.to_vec(),
                outcome_key: operation.outcome_key.to_vec(),
                requires_reopen: true,
            });
            return Err(MetadataError::UnknownOutcome {
                operation_id: operation.operation_id.to_vec(),
                cause: error.to_string(),
            });
        }

        if let Err(error) = transaction.commit() {
            gate.unresolved = Some(UnresolvedMutation {
                operation_id: operation.operation_id.to_vec(),
                outcome_key: operation.outcome_key.to_vec(),
                requires_reopen: true,
            });
            return Err(MetadataError::UnknownOutcome {
                operation_id: operation.operation_id.to_vec(),
                cause: error.to_string(),
            });
        }

        if let Err(error) = self
            .failures
            .check(FailurePoint::AuthorityAfterCommitBeforeReply)
        {
            gate.unresolved = Some(UnresolvedMutation {
                operation_id: operation.operation_id.to_vec(),
                outcome_key: operation.outcome_key.to_vec(),
                requires_reopen: false,
            });
            return Err(MetadataError::UnknownOutcome {
                operation_id: operation.operation_id.to_vec(),
                cause: error.to_string(),
            });
        }

        Ok(result)
    }

    fn reconcile_unknown(
        &self,
        operation: MutationIdentity<'_>,
    ) -> Result<ReconciledOutcome, MetadataError> {
        Self::validate_operation(operation)?;
        let mut gate = self.write_gate()?;
        let Some(unresolved) = gate.unresolved.as_ref() else {
            return Err(MetadataError::NoUnknownOutcome);
        };
        if unresolved.operation_id != operation.operation_id
            || unresolved.outcome_key != operation.outcome_key
        {
            return Err(MetadataError::ReconciliationRequired {
                operation_id: unresolved.operation_id.clone(),
            });
        }

        if unresolved.requires_reopen {
            self.recover_engine()?;
        }

        let engine = self.engine_read()?;
        let outcome = engine
            .get(operation.outcome_key)
            .map_err(backend_error)?
            .map(|value| decode_value(value.as_ref()))
            .transpose()?;
        gate.unresolved = None;
        Ok(match outcome {
            Some(value) => ReconciledOutcome::Committed(value),
            None => ReconciledOutcome::NotCommitted,
        })
    }

    fn snapshot(&self) -> Result<Self::Snapshot, MetadataError> {
        let gate = self.read_gate()?;
        Self::check_unresolved(&gate)?;
        let engine = self.engine_read()?;
        let snapshot = engine.snapshot().map_err(backend_error)?;
        self.active_snapshots.fetch_add(1, Ordering::AcqRel);
        Ok(ToyKvSnapshot {
            snapshot,
            scope: NEXT_SNAPSHOT_SCOPE.fetch_add(1, Ordering::Relaxed),
            active_snapshots: Arc::clone(&self.active_snapshots),
        })
    }

    fn scan_prefix(
        &self,
        snapshot: &Self::Snapshot,
        prefix: &[u8],
        cursor: Option<&MetadataCursor>,
        limit: NonZeroUsize,
    ) -> Result<MetadataPage, MetadataError> {
        if cursor.is_some_and(|cursor| cursor.snapshot_scope() != snapshot.cursor_scope()) {
            return Err(MetadataError::InvalidRequest(
                "scan cursor belongs to another snapshot",
            ));
        }
        if cursor.is_some_and(|cursor| cursor.prefix() != prefix) {
            return Err(MetadataError::InvalidRequest(
                "scan cursor belongs to another prefix",
            ));
        }
        if cursor.is_some_and(|cursor| !cursor.last_key().starts_with(prefix)) {
            return Err(MetadataError::InvalidRequest(
                "scan cursor is outside the requested prefix",
            ));
        }

        let upper = prefix_successor(prefix);
        let lower_bound = match cursor {
            Some(cursor) => std::ops::Bound::Excluded(cursor.last_key()),
            None if prefix.is_empty() => std::ops::Bound::Unbounded,
            None => std::ops::Bound::Included(prefix),
        };
        let upper_bound = upper
            .as_deref()
            .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        let mut scan = snapshot
            .snapshot
            .scan(lower_bound, upper_bound)
            .map_err(backend_error)?;

        let mut entries = Vec::with_capacity(limit.get());
        let mut has_more = false;
        while scan.is_valid() {
            if entries.len() == limit.get() {
                has_more = true;
                break;
            }
            let key = scan.key().to_vec();
            let value = decode_value(scan.value())?;
            entries.push((key, value));
            scan.next().map_err(backend_error)?;
        }

        let next_cursor = if has_more {
            entries
                .last()
                .map(|(key, _)| MetadataCursor::new(snapshot, prefix, key.clone()))
        } else {
            None
        };
        Ok(MetadataPage {
            entries,
            next_cursor,
        })
    }
}

struct ToyKvTransaction<'a> {
    transaction: &'a Transaction,
    clock: &'a dyn Clock,
    outcome_key: &'a [u8],
    outcome_written: bool,
}

impl MetadataTxn for ToyKvTransaction<'_> {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, MetadataError> {
        self.transaction
            .get(key)
            .map_err(backend_error)?
            .map(|value| decode_value(value.as_ref()))
            .transpose()
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), MetadataError> {
        if key == self.outcome_key {
            if self.outcome_written {
                return Err(MetadataError::InvalidRequest(
                    "terminal outcome record can only be written once",
                ));
            }
            self.outcome_written = true;
        }
        let encoded = encode_value(value);
        self.transaction.put(key, &encoded).map_err(backend_error)
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), MetadataError> {
        if key == self.outcome_key {
            return Err(MetadataError::InvalidRequest(
                "terminal outcome records are immutable",
            ));
        }
        self.transaction.delete(key).map_err(backend_error)
    }

    fn now(&self) -> versionfs_core::time::MonotonicTime {
        self.clock.now()
    }
}

fn encode_value(value: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(value.len() + 1);
    encoded.push(ENCODED_VALUE_PREFIX);
    encoded.extend_from_slice(value);
    encoded
}

fn decode_value(value: &[u8]) -> Result<Vec<u8>, MetadataError> {
    match value.split_first() {
        Some((&ENCODED_VALUE_PREFIX, payload)) => Ok(payload.to_vec()),
        _ => Err(MetadataError::Backend(
            "stored metadata value has an unknown encoding".into(),
        )),
    }
}

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut successor = prefix.to_vec();
    for index in (0..successor.len()).rev() {
        if successor[index] != u8::MAX {
            successor[index] += 1;
            successor.truncate(index + 1);
            return Some(successor);
        }
    }
    None
}

fn backend_error(error: impl std::fmt::Display) -> MetadataError {
    MetadataError::Backend(error.to_string())
}
