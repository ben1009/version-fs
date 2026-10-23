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
const DOMAIN_KEYSPACE_PREFIX: u8 = 1;
const PENDING_OPERATION_KEY: &[u8] = b"\0versionfs/metadata/pending-operation";
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingMutation {
    operation_id: Vec<u8>,
    outcome_key: Vec<u8>,
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
        resolve_pending_on_open(&engine)?;
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

    fn mark_unresolved(
        gate: &mut MutationGate,
        operation: MutationIdentity<'_>,
        cause: impl Into<String>,
        requires_reopen: bool,
    ) -> MetadataError {
        gate.unresolved = Some(UnresolvedMutation {
            operation_id: operation.operation_id.to_vec(),
            outcome_key: operation.outcome_key.to_vec(),
            requires_reopen,
        });
        MetadataError::UnknownOutcome {
            operation_id: operation.operation_id.to_vec(),
            cause: cause.into(),
        }
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

fn encode_domain_key(key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(key.len() + 1);
    encoded.push(DOMAIN_KEYSPACE_PREFIX);
    encoded.extend_from_slice(key);
    encoded
}

fn read_domain_value(engine: &KvEngine, key: &[u8]) -> Result<Option<Vec<u8>>, MetadataError> {
    engine
        .get(&encode_domain_key(key))
        .map_err(backend_error)?
        .map(|value| decode_value(value.as_ref()))
        .transpose()
}

fn read_pending_marker(engine: &KvEngine) -> Result<Option<PendingMutation>, MetadataError> {
    engine
        .get(PENDING_OPERATION_KEY)
        .map_err(backend_error)?
        .map(|value| decode_value(value.as_ref()).and_then(|bytes| decode_pending(&bytes)))
        .transpose()
}

fn write_pending_marker(engine: &KvEngine, pending: &PendingMutation) -> Result<(), MetadataError> {
    let transaction = engine.new_txn().map_err(backend_error)?;
    let encoded = encode_value(&encode_pending(pending)?);
    transaction
        .put(PENDING_OPERATION_KEY, &encoded)
        .map_err(backend_error)?;
    transaction.commit().map_err(backend_error)
}

fn clear_pending_marker(
    engine: &KvEngine,
    expected: &PendingMutation,
) -> Result<(), MetadataError> {
    let transaction = engine.new_txn().map_err(backend_error)?;
    let current = transaction
        .get(PENDING_OPERATION_KEY)
        .map_err(backend_error)?
        .map(|value| decode_value(value.as_ref()).and_then(|bytes| decode_pending(&bytes)))
        .transpose()?;
    match current {
        None => return Ok(()),
        Some(current) if current == *expected => {}
        Some(_) => {
            return Err(MetadataError::Backend(
                "pending operation changed under the authority gate".into(),
            ));
        }
    }
    transaction
        .delete(PENDING_OPERATION_KEY)
        .map_err(backend_error)?;
    transaction.commit().map_err(backend_error)
}

/// WAL recovery makes the prior mutation's outcome authoritative before this
/// authority admits traffic after a process restart. The reservation is kept
/// until this check has read the operation result and durably removed it.
fn resolve_pending_on_open(engine: &KvEngine) -> Result<(), MetadataError> {
    let Some(pending) = read_pending_marker(engine)? else {
        return Ok(());
    };
    // WAL recovery has completed. Presence means the mutation committed;
    // absence means it did not. Keep any result record for caller reconciliation.
    let _outcome = read_domain_value(engine, &pending.outcome_key)?;
    clear_pending_marker(engine, &pending)
}

fn encode_pending(pending: &PendingMutation) -> Result<Vec<u8>, MetadataError> {
    let operation_id_len = u32::try_from(pending.operation_id.len())
        .map_err(|_| MetadataError::InvalidRequest("operation ID is too large"))?;
    let outcome_key_len = u32::try_from(pending.outcome_key.len())
        .map_err(|_| MetadataError::InvalidRequest("outcome key is too large"))?;
    let mut encoded =
        Vec::with_capacity(1 + 4 + pending.operation_id.len() + 4 + pending.outcome_key.len());
    encoded.push(1);
    encoded.extend_from_slice(&operation_id_len.to_be_bytes());
    encoded.extend_from_slice(&pending.operation_id);
    encoded.extend_from_slice(&outcome_key_len.to_be_bytes());
    encoded.extend_from_slice(&pending.outcome_key);
    Ok(encoded)
}

fn decode_pending(encoded: &[u8]) -> Result<PendingMutation, MetadataError> {
    if encoded.first() != Some(&1) {
        return Err(MetadataError::Backend(
            "pending operation has an unknown encoding".into(),
        ));
    }
    let mut offset = 1;
    let operation_id = take_pending_field(encoded, &mut offset)?;
    let outcome_key = take_pending_field(encoded, &mut offset)?;
    if offset != encoded.len() || operation_id.is_empty() || outcome_key.is_empty() {
        return Err(MetadataError::Backend(
            "pending operation record is malformed".into(),
        ));
    }
    Ok(PendingMutation {
        operation_id: operation_id.to_vec(),
        outcome_key: outcome_key.to_vec(),
    })
}

fn take_pending_field<'a>(
    encoded: &'a [u8],
    offset: &mut usize,
) -> Result<&'a [u8], MetadataError> {
    let length_end = offset
        .checked_add(4)
        .ok_or_else(|| MetadataError::Backend("pending operation length overflow".into()))?;
    let length_bytes = encoded
        .get(*offset..length_end)
        .ok_or_else(|| MetadataError::Backend("pending operation record is truncated".into()))?;
    let length = u32::from_be_bytes(
        length_bytes
            .try_into()
            .map_err(|_| MetadataError::Backend("pending operation length is malformed".into()))?,
    ) as usize;
    *offset = length_end;
    let end = offset
        .checked_add(length)
        .ok_or_else(|| MetadataError::Backend("pending operation length overflow".into()))?;
    let field = encoded
        .get(*offset..end)
        .ok_or_else(|| MetadataError::Backend("pending operation record is truncated".into()))?;
    *offset = end;
    Ok(field)
}

enum TransactionAttemptError {
    Definite(MetadataError),
    Unknown {
        cause: String,
        requires_reopen: bool,
    },
}

impl From<MetadataError> for TransactionAttemptError {
    fn from(error: MetadataError) -> Self {
        Self::Definite(error)
    }
}

fn execute_transaction<R, F>(
    engine: &KvEngine,
    clock: &dyn Clock,
    failures: &dyn FailureInjector,
    operation: MutationIdentity<'_>,
    body: &mut F,
) -> Result<R, TransactionAttemptError>
where
    F: FnMut(&mut dyn MetadataTxn) -> Result<R, MetadataError>,
{
    let transaction = engine
        .new_txn()
        .map_err(backend_error)
        .map_err(TransactionAttemptError::Definite)?;
    let mut metadata_txn = ToyKvTransaction {
        transaction: &transaction,
        clock,
        outcome_key: operation.outcome_key,
        outcome_written: false,
    };
    if metadata_txn.get(operation.outcome_key)?.is_some() {
        return Err(TransactionAttemptError::Definite(MetadataError::Conflict));
    }
    let result = body(&mut metadata_txn)?;
    if !metadata_txn.outcome_written {
        return Err(TransactionAttemptError::Definite(
            MetadataError::InvalidRequest("mutation did not write its terminal outcome record"),
        ));
    }

    failures
        .check(FailurePoint::AuthorityBeforeCommit)
        .map_err(|error| {
            TransactionAttemptError::Definite(MetadataError::Rejected(error.to_string()))
        })?;

    if let Err(error) = failures.check(FailurePoint::AuthorityCommitOutcomeUnknownWithoutCommit) {
        return Err(TransactionAttemptError::Unknown {
            cause: error.to_string(),
            requires_reopen: true,
        });
    }

    if let Err(error) = transaction.commit() {
        return Err(TransactionAttemptError::Unknown {
            cause: error.to_string(),
            requires_reopen: true,
        });
    }

    if let Err(error) = failures.check(FailurePoint::AuthorityAfterCommitBeforeReply) {
        return Err(TransactionAttemptError::Unknown {
            cause: error.to_string(),
            requires_reopen: false,
        });
    }

    Ok(result)
}

impl MetadataAuthority for ToyKvMetadataAuthority {
    type Snapshot = ToyKvSnapshot;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetadataError> {
        let gate = self.read_gate()?;
        Self::check_unresolved(&gate)?;
        let engine = self.engine_read()?;
        read_domain_value(&engine, key)
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
        let pending = PendingMutation {
            operation_id: operation.operation_id.to_vec(),
            outcome_key: operation.outcome_key.to_vec(),
        };
        if let Err(error) = write_pending_marker(&engine, &pending) {
            return Err(Self::mark_unresolved(
                &mut gate,
                operation,
                error.to_string(),
                true,
            ));
        }

        match execute_transaction(
            &engine,
            self.clock.as_ref(),
            self.failures.as_ref(),
            operation,
            &mut body,
        ) {
            Ok(result) => Ok(result),
            Err(TransactionAttemptError::Unknown {
                cause,
                requires_reopen,
            }) => Err(Self::mark_unresolved(
                &mut gate,
                operation,
                cause,
                requires_reopen,
            )),
            Err(TransactionAttemptError::Definite(error)) => {
                if let Err(clear_error) = clear_pending_marker(&engine, &pending) {
                    return Err(Self::mark_unresolved(
                        &mut gate,
                        operation,
                        format!("{error}; pending-operation cleanup failed: {clear_error}"),
                        true,
                    ));
                }
                Err(error)
            }
        }
    }

    fn reconcile_unknown(
        &self,
        operation: MutationIdentity<'_>,
    ) -> Result<ReconciledOutcome, MetadataError> {
        Self::validate_operation(operation)?;
        let mut gate = self.write_gate()?;
        let unresolved = gate.unresolved.as_ref().map(|pending| {
            (
                pending.operation_id.clone(),
                pending.outcome_key.clone(),
                pending.requires_reopen,
            )
        });
        if let Some((operation_id, outcome_key, _)) = &unresolved
            && (operation_id != operation.operation_id || outcome_key != operation.outcome_key)
        {
            return Err(MetadataError::ReconciliationRequired {
                operation_id: operation_id.clone(),
            });
        }
        if unresolved
            .as_ref()
            .is_some_and(|(_, _, requires_reopen)| *requires_reopen)
        {
            self.recover_engine()?;
        }

        let engine = self.engine_read()?;
        let outcome = read_domain_value(&engine, operation.outcome_key)?;
        if let Some((operation_id, outcome_key, _)) = unresolved {
            let queried = PendingMutation {
                operation_id,
                outcome_key,
            };
            if let Some(pending) = read_pending_marker(&engine)? {
                let pending_outcome_is_resolved = pending == queried
                    || (outcome.is_none()
                        && read_domain_value(&engine, &pending.outcome_key)?.is_some());
                if pending_outcome_is_resolved {
                    if let Err(error) = clear_pending_marker(&engine, &pending) {
                        gate.unresolved = Some(UnresolvedMutation {
                            operation_id: queried.operation_id.clone(),
                            outcome_key: queried.outcome_key.clone(),
                            requires_reopen: true,
                        });
                        return Err(MetadataError::UnknownOutcome {
                            operation_id: queried.operation_id,
                            cause: error.to_string(),
                        });
                    }
                } else {
                    return Err(MetadataError::Backend(
                        "pending operation does not match its durable outcome".into(),
                    ));
                }
            }
            gate.unresolved = None;
        }
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

        let encoded_prefix = encode_domain_key(prefix);
        let encoded_cursor = cursor.map(|cursor| encode_domain_key(cursor.last_key()));
        let upper = prefix_successor(&encoded_prefix);
        let lower_bound = match encoded_cursor.as_deref() {
            Some(key) => std::ops::Bound::Excluded(key),
            None => std::ops::Bound::Included(encoded_prefix.as_slice()),
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
            let encoded_key = scan.key();
            let key = encoded_key
                .strip_prefix(&[DOMAIN_KEYSPACE_PREFIX])
                .ok_or_else(|| {
                    MetadataError::Backend("snapshot scan escaped the domain keyspace".into())
                })?
                .to_vec();
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
        let encoded_key = encode_domain_key(key);
        self.transaction
            .get(&encoded_key)
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
        let encoded_key = encode_domain_key(key);
        let encoded = encode_value(value);
        self.transaction
            .put(&encoded_key, &encoded)
            .map_err(backend_error)
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), MetadataError> {
        if key == self.outcome_key {
            return Err(MetadataError::InvalidRequest(
                "terminal outcome records are immutable",
            ));
        }
        self.transaction
            .delete(&encode_domain_key(key))
            .map_err(backend_error)
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
