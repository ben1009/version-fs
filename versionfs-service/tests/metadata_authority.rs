//! Conformance tests for the pinned toy-kv Metadata Authority adapter.

use std::{
    env,
    num::NonZeroUsize,
    path::PathBuf,
    process::{Command, ExitStatus},
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
    },
    thread,
};

use kv_engine::lsm_storage::{KvEngine, LsmStorageOptions};
use versionfs_core::{
    fault::{FailureInjector, FailurePoint, InjectedFailure, OneShotFailures},
    metadata::{MetadataAuthority, MetadataError, MutationIdentity, ReconciledOutcome},
    time::{Clock, MonotonicTime},
};
use versionfs_service::toy_kv_metadata::ToyKvMetadataAuthority;

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);
const PENDING_OPERATION_KEY: &[u8] = b"\0versionfs/metadata/pending-operation";

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!("versionfs-metadata-{}-{id}", std::process::id()));
        std::fs::create_dir_all(&path).expect("create metadata test directory");
        Self(path)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Default)]
struct TestClock(AtomicU64);

impl TestClock {
    fn set(&self, nanos: u64) {
        self.0.store(nanos, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_nanos(self.0.load(Ordering::SeqCst))
    }
}

struct AdvanceClockAfterCommit {
    clock: Arc<TestClock>,
    deadline: u64,
}

impl FailureInjector for AdvanceClockAfterCommit {
    fn check(&self, point: FailurePoint) -> Result<(), InjectedFailure> {
        if point == FailurePoint::AuthorityAfterCommitBeforeReply {
            self.clock.set(self.deadline);
            return Err(InjectedFailure(point));
        }
        Ok(())
    }
}

fn operation<'a>(id: &'a [u8], outcome_key: &'a [u8]) -> MutationIdentity<'a> {
    MutationIdentity {
        operation_id: id,
        outcome_key,
    }
}

fn authority(path: &std::path::Path) -> ToyKvMetadataAuthority {
    ToyKvMetadataAuthority::open(path).expect("open toy-kv metadata authority")
}

fn pending_marker(path: &std::path::Path) -> Option<Vec<u8>> {
    let engine = KvEngine::open(
        path,
        LsmStorageOptions {
            enable_wal: true,
            serializable: true,
            ..LsmStorageOptions::default()
        },
    )
    .expect("open raw toy-kv engine to inspect pending marker");
    let value = engine
        .get(PENDING_OPERATION_KEY)
        .expect("read raw pending marker")
        .map(|value| value.as_ref().to_vec());
    engine.close().expect("close raw toy-kv engine");
    value
}

fn expected_pending_marker(operation_id: &[u8], outcome_key: &[u8]) -> Vec<u8> {
    let mut encoded = vec![0, 1];
    encoded.extend_from_slice(&(operation_id.len() as u32).to_be_bytes());
    encoded.extend_from_slice(operation_id);
    encoded.extend_from_slice(&(outcome_key.len() as u32).to_be_bytes());
    encoded.extend_from_slice(outcome_key);
    encoded
}

#[test]
fn atomic_point_transactions_support_read_your_writes_and_known_conflicts() {
    let dir = TestDir::new();
    let authority = authority(dir.path());

    authority
        .transact(operation(b"write-1", b"outcome/write-1"), |txn| {
            txn.put(b"branch/revision", b"2")?;
            txn.put(b"generation/2", b"descriptor")?;
            assert_eq!(txn.get(b"branch/revision")?, Some(b"2".to_vec()));
            txn.put(b"outcome/write-1", b"published")?;
            Ok(())
        })
        .expect("commit complete point transaction");

    assert_eq!(
        authority.get(b"branch/revision").expect("fresh read"),
        Some(b"2".to_vec())
    );
    assert_eq!(
        authority.get(b"generation/2").expect("fresh read"),
        Some(b"descriptor".to_vec())
    );

    let rejected = authority.transact(
        operation(b"stale-write", b"outcome/stale-write"),
        |txn| -> Result<(), MetadataError> {
            assert_eq!(txn.get(b"branch/revision")?, Some(b"2".to_vec()));
            txn.put(b"branch/revision", b"3")?;
            Err(MetadataError::Conflict)
        },
    );
    assert_eq!(rejected, Err(MetadataError::Conflict));
    assert_eq!(
        authority
            .get(b"branch/revision")
            .expect("read after conflict"),
        Some(b"2".to_vec())
    );
    assert_eq!(
        authority
            .get(b"outcome/stale-write")
            .expect("absent outcome"),
        None
    );
}

#[test]
fn absent_point_claims_are_serialized_without_range_predicates() {
    let dir = TestDir::new();
    let authority = Arc::new(authority(dir.path()));
    let start = Arc::new(Barrier::new(3));

    let workers = (0..2)
        .map(|index| {
            let authority = Arc::clone(&authority);
            let start = Arc::clone(&start);
            thread::spawn(move || {
                let id = format!("claim-{index}").into_bytes();
                let outcome_key = format!("outcome/claim-{index}").into_bytes();
                start.wait();
                authority.transact(operation(&id, &outcome_key), |txn| {
                    if txn.get(b"name/main")?.is_some() {
                        return Err(MetadataError::Conflict);
                    }
                    txn.put(b"name/main", &id)?;
                    txn.put(&outcome_key, b"created")?;
                    Ok(())
                })
            })
        })
        .collect::<Vec<_>>();

    start.wait();
    let results = workers
        .into_iter()
        .map(|worker| worker.join().expect("claim worker did not panic"))
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(MetadataError::Conflict)))
            .count(),
        1
    );
    assert!(authority.get(b"name/main").expect("name read").is_some());
}

#[test]
fn snapshot_pages_keep_one_sorted_boundary_across_mutations() {
    let dir = TestDir::new();
    let authority = authority(dir.path());
    authority
        .transact(operation(b"seed", b"outcome/seed"), |txn| {
            for (key, value) in [
                (b"logs/a".as_slice(), b"1".as_slice()),
                (b"logs/b", b"2"),
                (b"logs/c", b"3"),
                (b"logs/d", b"4"),
            ] {
                txn.put(key, value)?;
            }
            txn.put(b"outcome/seed", b"seeded")?;
            Ok(())
        })
        .expect("seed authority");

    let snapshot = authority.snapshot().expect("capture read snapshot");
    let page1 = authority
        .scan_prefix(&snapshot, b"logs/", None, NonZeroUsize::new(2).unwrap())
        .expect("read first page");
    assert_eq!(
        page1.entries,
        vec![
            (b"logs/a".to_vec(), b"1".to_vec()),
            (b"logs/b".to_vec(), b"2".to_vec())
        ]
    );
    let cursor = page1.next_cursor.expect("more entries");
    let other_snapshot = authority.snapshot().expect("capture another snapshot");
    assert!(matches!(
        authority.scan_prefix(
            &other_snapshot,
            b"logs/",
            Some(&cursor),
            NonZeroUsize::new(2).unwrap(),
        ),
        Err(MetadataError::InvalidRequest(_))
    ));
    assert!(matches!(
        authority.scan_prefix(
            &snapshot,
            b"logs/a",
            Some(&cursor),
            NonZeroUsize::new(2).unwrap(),
        ),
        Err(MetadataError::InvalidRequest(_))
    ));
    drop(other_snapshot);

    authority
        .transact(operation(b"mutate", b"outcome/mutate"), |txn| {
            txn.put(b"logs/aa", b"new")?;
            txn.put(b"logs/c", b"changed")?;
            txn.delete(b"logs/d")?;
            txn.put(b"outcome/mutate", b"mutated")?;
            Ok(())
        })
        .expect("mutate latest state");

    let page2 = authority
        .scan_prefix(
            &snapshot,
            b"logs/",
            Some(&cursor),
            NonZeroUsize::new(2).unwrap(),
        )
        .expect("continue original snapshot");
    assert_eq!(
        page2.entries,
        vec![
            (b"logs/c".to_vec(), b"3".to_vec()),
            (b"logs/d".to_vec(), b"4".to_vec())
        ]
    );
    assert_eq!(page2.next_cursor, None);
    drop(snapshot);

    let latest = authority.snapshot().expect("capture latest snapshot");
    let latest_page = authority
        .scan_prefix(&latest, b"logs/", None, NonZeroUsize::new(10).unwrap())
        .expect("scan latest state");
    assert_eq!(
        latest_page.entries,
        vec![
            (b"logs/a".to_vec(), b"1".to_vec()),
            (b"logs/aa".to_vec(), b"new".to_vec()),
            (b"logs/b".to_vec(), b"2".to_vec()),
            (b"logs/c".to_vec(), b"changed".to_vec()),
        ]
    );
}

#[test]
fn unknown_commit_blocks_mutations_until_operation_result_reconciliation() {
    let dir = TestDir::new();
    let failures = Arc::new(OneShotFailures::default());
    failures.arm(FailurePoint::AuthorityAfterCommitBeforeReply);
    let authority =
        ToyKvMetadataAuthority::open_with(dir.path(), Arc::new(TestClock::default()), failures)
            .expect("open authority with failure injection");

    let result = authority.transact(operation(b"op-unknown", b"outcome/op-unknown"), |txn| {
        txn.put(b"branch/head", b"g1")?;
        txn.put(b"outcome/op-unknown", b"committed-g1")?;
        Ok(())
    });
    assert!(matches!(
        result,
        Err(MetadataError::UnknownOutcome { operation_id, .. })
            if operation_id == b"op-unknown"
    ));

    let blocked = authority.transact(operation(b"op-next", b"outcome/op-next"), |txn| {
        txn.put(b"branch/head", b"g2")?;
        txn.put(b"outcome/op-next", b"committed-g2")?;
        Ok(())
    });
    assert!(matches!(
        blocked,
        Err(MetadataError::ReconciliationRequired { .. })
    ));

    assert_eq!(
        authority
            .reconcile_unknown(operation(b"op-unknown", b"outcome/op-unknown"))
            .expect("resolve operation outcome"),
        ReconciledOutcome::Committed(b"committed-g1".to_vec())
    );
    authority
        .transact(operation(b"op-next", b"outcome/op-next"), |txn| {
            txn.put(b"branch/head", b"g2")?;
            txn.put(b"outcome/op-next", b"committed-g2")?;
            Ok(())
        })
        .expect("mutations resume after reconciliation");
}

#[test]
fn lease_expiry_is_checked_at_the_gated_decision_and_late_ack_stays_committed() {
    let dir = TestDir::new();
    let clock = Arc::new(TestClock::default());
    let deadline = 100;
    clock.set(deadline - 1);
    let failures = Arc::new(AdvanceClockAfterCommit {
        clock: Arc::clone(&clock),
        deadline,
    });
    let authority = ToyKvMetadataAuthority::open_with(dir.path(), clock.clone(), failures)
        .expect("open authority with injected clock");

    let result = authority.transact(operation(b"expired", b"outcome/expired"), |txn| {
        txn.put(b"branch/head", b"must-not-commit")?;
        if txn.now().as_nanos() >= deadline {
            return Err(MetadataError::Conflict);
        }
        txn.put(b"outcome/expired", b"unexpected")?;
        Ok(())
    });
    assert!(matches!(result, Err(MetadataError::UnknownOutcome { .. })));
    assert_eq!(clock.now().as_nanos(), deadline);
    assert_eq!(
        authority
            .reconcile_unknown(operation(b"expired", b"outcome/expired"))
            .expect("reconcile late reply"),
        ReconciledOutcome::Committed(b"unexpected".to_vec())
    );
    let expired_result = authority.transact(
        operation(b"at-deadline", b"outcome/at-deadline"),
        |txn| -> Result<(), MetadataError> {
            txn.put(b"branch/head", b"expired-write")?;
            if txn.now().as_nanos() >= deadline {
                return Err(MetadataError::Conflict);
            }
            txn.put(b"outcome/at-deadline", b"unexpected")?;
            Ok(())
        },
    );
    assert_eq!(expired_result, Err(MetadataError::Conflict));
    assert_eq!(
        authority.get(b"branch/head").expect("read branch head"),
        Some(b"must-not-commit".to_vec())
    );
    assert_eq!(authority.get(b"outcome/at-deadline").unwrap(), None);
}

#[test]
fn metadata_wal_recovers_after_the_writer_process_exits() {
    const CHILD_PATH: &str = "VERSIONFS_METADATA_CRASH_CHILD_PATH";
    if let Ok(path) = env::var(CHILD_PATH) {
        let authority = authority(PathBuf::from(path).as_path());
        authority
            .transact(operation(b"crash-op", b"outcome/crash-op"), |txn| {
                txn.put(b"branch/head", b"g-after-crash")?;
                txn.put(b"outcome/crash-op", b"committed")?;
                Ok(())
            })
            .expect("acknowledge durable child commit");
        std::process::exit(86);
    }

    let dir = TestDir::new();
    let status: ExitStatus = Command::new(env::current_exe().expect("test executable path"))
        .arg("--exact")
        .arg("metadata_wal_recovers_after_the_writer_process_exits")
        .arg("--nocapture")
        .env(CHILD_PATH, dir.path())
        .status()
        .expect("launch metadata writer child");
    assert_eq!(status.code(), Some(86));

    let recovered = authority(dir.path());
    assert_eq!(
        recovered.get(b"branch/head").expect("recovered head"),
        Some(b"g-after-crash".to_vec())
    );
    assert_eq!(
        recovered
            .get(b"outcome/crash-op")
            .expect("recovered outcome"),
        Some(b"committed".to_vec())
    );
}

#[test]
fn metadata_unknown_outcome_child_exits_after_uncertain_result() {
    const CHILD_PATH: &str = "VERSIONFS_METADATA_UNKNOWN_CHILD_PATH";
    const CHILD_MODE: &str = "VERSIONFS_METADATA_UNKNOWN_CHILD_MODE";
    let (Ok(path), Ok(mode)) = (env::var(CHILD_PATH), env::var(CHILD_MODE)) else {
        return;
    };
    let failures = Arc::new(OneShotFailures::default());
    let operation_id = match mode.as_str() {
        "committed" => {
            failures.arm(FailurePoint::AuthorityAfterCommitBeforeReply);
            b"restart-committed".as_slice()
        }
        "not-committed" => {
            failures.arm(FailurePoint::AuthorityCommitOutcomeUnknownWithoutCommit);
            b"restart-not-committed".as_slice()
        }
        _ => panic!("unknown child test mode"),
    };
    let outcome_key = [b"outcome/".as_slice(), operation_id].concat();
    let authority = ToyKvMetadataAuthority::open_with(
        PathBuf::from(path),
        Arc::new(TestClock::default()),
        failures,
    )
    .expect("open child authority");
    let result = authority.transact(operation(operation_id, &outcome_key), |txn| {
        txn.put(b"restart/state", operation_id)?;
        txn.put(&outcome_key, b"terminal-result")?;
        Ok(())
    });
    assert!(matches!(result, Err(MetadataError::UnknownOutcome { .. })));
    std::process::exit(87);
}

#[test]
fn restart_reconciles_persisted_unknown_outcomes_before_serving_mutations() {
    const CHILD_PATH: &str = "VERSIONFS_METADATA_UNKNOWN_CHILD_PATH";
    const CHILD_MODE: &str = "VERSIONFS_METADATA_UNKNOWN_CHILD_MODE";

    fn crash_child(path: &std::path::Path, mode: &str) {
        let status = Command::new(env::current_exe().expect("test executable path"))
            .arg("--exact")
            .arg("metadata_unknown_outcome_child_exits_after_uncertain_result")
            .arg("--nocapture")
            .env(CHILD_PATH, path)
            .env(CHILD_MODE, mode)
            .status()
            .expect("launch unknown-outcome child");
        assert_eq!(status.code(), Some(87));
    }

    let committed_dir = TestDir::new();
    crash_child(committed_dir.path(), "committed");
    assert_eq!(
        pending_marker(committed_dir.path()),
        Some(expected_pending_marker(
            b"restart-committed",
            b"outcome/restart-committed",
        )),
        "child exit must leave the committed operation's marker durable"
    );
    let committed = authority(committed_dir.path());
    drop(committed);
    assert_eq!(
        pending_marker(committed_dir.path()),
        None,
        "opening the authority must resolve and clear the committed marker"
    );
    let committed = authority(committed_dir.path());
    assert_eq!(
        committed
            .reconcile_unknown(operation(
                b"restart-committed",
                b"outcome/restart-committed",
            ))
            .expect("query recovered committed result"),
        ReconciledOutcome::Committed(b"terminal-result".to_vec())
    );
    assert_eq!(
        committed.get(b"restart/state").expect("committed state"),
        Some(b"restart-committed".to_vec())
    );
    committed
        .transact(
            operation(b"after-restart-commit", b"outcome/after-restart-commit"),
            |txn| {
                txn.put(b"restart/next", b"allowed-after-recovery")?;
                txn.put(b"outcome/after-restart-commit", b"next-result")?;
                Ok(())
            },
        )
        .expect("startup reconciliation completes before writes are admitted");

    let aborted_dir = TestDir::new();
    crash_child(aborted_dir.path(), "not-committed");
    assert_eq!(
        pending_marker(aborted_dir.path()),
        Some(expected_pending_marker(
            b"restart-not-committed",
            b"outcome/restart-not-committed",
        )),
        "child exit must leave the uncommitted operation's marker durable"
    );
    let aborted = authority(aborted_dir.path());
    drop(aborted);
    assert_eq!(
        pending_marker(aborted_dir.path()),
        None,
        "opening the authority must resolve and clear the uncommitted marker"
    );
    let aborted = authority(aborted_dir.path());
    assert_eq!(
        aborted
            .reconcile_unknown(operation(
                b"restart-not-committed",
                b"outcome/restart-not-committed",
            ))
            .expect("query recovered absent result"),
        ReconciledOutcome::NotCommitted
    );
    assert_eq!(aborted.get(b"restart/state").expect("aborted state"), None);
    aborted
        .transact(
            operation(b"restart-not-committed", b"outcome/restart-not-committed"),
            |txn| {
                txn.put(b"restart/state", b"retry-after-recovery")?;
                txn.put(b"outcome/restart-not-committed", b"retry-result")?;
                Ok(())
            },
        )
        .expect("retry after startup established the prior operation did not commit");
}

#[test]
fn before_commit_failure_is_definitely_not_committed() {
    let dir = TestDir::new();
    let failures = Arc::new(OneShotFailures::default());
    failures.arm(FailurePoint::AuthorityBeforeCommit);
    let authority =
        ToyKvMetadataAuthority::open_with(dir.path(), Arc::new(TestClock::default()), failures)
            .expect("open authority with failure injection");

    let result = authority.transact(operation(b"before", b"outcome/before"), |txn| {
        txn.put(b"branch/head", b"must-not-commit")?;
        txn.put(b"outcome/before", b"unexpected")?;
        Ok(())
    });
    assert!(matches!(result, Err(MetadataError::Rejected(_))));
    assert_eq!(authority.get(b"branch/head").unwrap(), None);
    assert_eq!(authority.get(b"outcome/before").unwrap(), None);

    authority
        .transact(operation(b"after", b"outcome/after"), |txn| {
            txn.put(b"branch/head", b"g1")?;
            txn.put(b"outcome/after", b"committed")?;
            Ok(())
        })
        .expect("definite precommit failure leaves gate available");
}

#[test]
fn mutation_terminal_outcome_is_required_and_written_once() {
    let dir = TestDir::new();
    let authority = authority(dir.path());

    let missing = authority.transact(
        operation(b"missing-outcome", b"outcome/missing-outcome"),
        |txn| {
            txn.put(b"state/missing", b"must-not-commit")?;
            Ok(())
        },
    );
    assert!(matches!(missing, Err(MetadataError::InvalidRequest(_))));
    assert_eq!(authority.get(b"state/missing").unwrap(), None);

    let duplicate = authority.transact(
        operation(b"duplicate-outcome", b"outcome/duplicate-outcome"),
        |txn| {
            txn.put(b"state/duplicate", b"must-not-commit")?;
            txn.put(b"outcome/duplicate-outcome", b"first")?;
            txn.put(b"outcome/duplicate-outcome", b"second")?;
            Ok(())
        },
    );
    assert!(matches!(duplicate, Err(MetadataError::InvalidRequest(_))));
    assert_eq!(authority.get(b"state/duplicate").unwrap(), None);
    assert_eq!(authority.get(b"outcome/duplicate-outcome").unwrap(), None);
}

#[test]
fn snapshot_cursor_must_remain_inside_prefix() {
    let dir = TestDir::new();
    let authority = authority(dir.path());
    let snapshot = authority.snapshot().expect("empty snapshot");
    let cursor =
        versionfs_core::metadata::MetadataCursor::new(&snapshot, b"other/", b"other/key".to_vec());
    let error = authority.scan_prefix(
        &snapshot,
        b"logs/",
        Some(&cursor),
        NonZeroUsize::new(1).unwrap(),
    );
    assert!(matches!(error, Err(MetadataError::InvalidRequest(_))));
}

#[test]
fn missing_outcome_reconciles_as_not_committed() {
    let dir = TestDir::new();
    let failures = Arc::new(OneShotFailures::default());
    failures.arm(FailurePoint::AuthorityCommitOutcomeUnknownWithoutCommit);
    let authority =
        ToyKvMetadataAuthority::open_with(dir.path(), Arc::new(TestClock::default()), failures)
            .expect("open authority");
    let pinned_snapshot = authority.snapshot().expect("hold an authority snapshot");

    let result = authority.transact(
        operation(b"missing-result", b"outcome/missing-result"),
        |txn| {
            txn.put(b"other/state", b"value")?;
            txn.put(b"outcome/missing-result", b"committed")?;
            Ok(())
        },
    );
    assert!(matches!(result, Err(MetadataError::UnknownOutcome { .. })));

    assert!(matches!(
        authority.reconcile_unknown(operation(b"missing-result", b"outcome/missing-result")),
        Err(MetadataError::Backend(_))
    ));
    let blocked = authority.transact(operation(b"blocked", b"outcome/blocked"), |txn| {
        txn.put(b"other/state", b"blocked")?;
        txn.put(b"outcome/blocked", b"blocked")?;
        Ok(())
    });
    assert!(matches!(
        blocked,
        Err(MetadataError::ReconciliationRequired { .. })
    ));
    drop(pinned_snapshot);

    assert_eq!(
        authority
            .reconcile_unknown(operation(b"missing-result", b"outcome/missing-result"))
            .expect("resolve absent outcome"),
        ReconciledOutcome::NotCommitted
    );
    authority
        .transact(
            operation(b"missing-result", b"outcome/missing-result"),
            |txn| {
                txn.put(b"other/state", b"retry-after-recovery")?;
                txn.put(b"outcome/missing-result", b"committed-on-retry")?;
                Ok(())
            },
        )
        .expect("retry same logical mutation after absent result is established");
    assert_eq!(
        authority.get(b"other/state").unwrap(),
        Some(b"retry-after-recovery".to_vec())
    );
}
