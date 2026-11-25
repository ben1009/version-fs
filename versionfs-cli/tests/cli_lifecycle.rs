//! End-to-end multi-process CLI + `versionfsd` + two-worker lazy clean-cache test.

use serde_json::Value;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let id = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "versionfs-cli-lifecycle-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct ChildGuard(Option<Child>);

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self(Some(child))
    }

    fn wait_exit(&mut self, timeout: Duration) {
        if let Some(child) = self.0.as_mut() {
            let start = Instant::now();
            while start.elapsed() < timeout {
                if let Ok(Some(_)) = child.try_wait() {
                    self.0 = None;
                    return;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            let _ = child.kill();
            let _ = child.wait();
            self.0 = None;
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn cli_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_versionfs-cli"))
}

fn ensure_versionfsd_bin() -> PathBuf {
    let cli = cli_bin();
    let candidate = cli.parent().unwrap().join("versionfsd");
    if candidate.exists() {
        return candidate;
    }
    let status = Command::new(env!("CARGO"))
        .args([
            "build",
            "--offline",
            "--locked",
            "-p",
            "versionfs-service",
            "--bin",
            "versionfsd",
        ])
        .status()
        .expect("failed to build versionfsd");
    assert!(status.success(), "cargo build versionfsd failed");
    assert!(candidate.exists(), "versionfsd binary missing after build");
    candidate
}

fn run_cli(endpoint: &str, workspace_id: &str, principal_id: &str, args: &[&str]) -> Value {
    let out = Command::new(cli_bin())
        .args([
            "--endpoint",
            endpoint,
            "--workspace-id",
            workspace_id,
            "--principal-id",
            principal_id,
            "--auth-token",
            "cli-test-token",
            "--json",
        ])
        .args(args)
        .output()
        .expect("failed to execute versionfs-cli");
    assert!(
        out.status.success(),
        "cli {:?} failed:\nstdout: {}\nstderr: {}",
        args,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("cli stdout must be valid JSON")
}

fn run_cli_expect_err(
    endpoint: &str,
    workspace_id: &str,
    principal_id: &str,
    args: &[&str],
) -> String {
    let out = Command::new(cli_bin())
        .args([
            "--endpoint",
            endpoint,
            "--workspace-id",
            workspace_id,
            "--principal-id",
            principal_id,
            "--auth-token",
            "cli-test-token",
            "--json",
        ])
        .args(args)
        .output()
        .expect("failed to execute versionfs-cli");
    assert!(!out.status.success(), "expected cli {:?} to fail", args);
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn wait_for_socket(path: &Path) {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        if path.exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("worker control socket {} did not appear", path.display());
}

#[test]
fn two_worker_cli_lifecycle_with_lazy_clean_cache_and_provenance() {
    let dir = TempDir::new();
    let meta_dir = dir.0.join("svc-meta");
    let obj_dir = dir.0.join("svc-objects");

    let versionfsd = ensure_versionfsd_bin();
    let mut svc_child = Command::new(versionfsd)
        .args([
            "--metadata-dir",
            meta_dir.to_str().unwrap(),
            "--object-dir",
            obj_dir.to_str().unwrap(),
            "--listen",
            "tcp://127.0.0.1:0",
            "--auth-token",
            "cli-test-token",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn versionfsd");

    let stdout = svc_child.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout);
    let mut ready_line = String::new();
    reader
        .read_line(&mut ready_line)
        .expect("failed to read READY line from versionfsd");
    let _svc_guard = ChildGuard::new(svc_child);
    let endpoint = ready_line
        .trim()
        .strip_prefix("READY ")
        .expect("versionfsd must print READY <endpoint>")
        .to_owned();

    let ws_id = "11111111111111111111111111111111";
    let principal_id = "22222222222222222222222222222222";

    // 1. Create workspace.
    let created = run_cli(
        &endpoint,
        ws_id,
        principal_id,
        &["workspace", "create", "--capacity-bytes", "131072"],
    );
    let main_branch_id = created["branch"]["branch_id"].as_str().unwrap().to_owned();
    let main_stream_epoch = created["branch"]["stream_epoch"].as_u64().unwrap();

    // 2. Start Worker A as a separate OS process.
    let worker_a_dir = dir.0.join("worker-a");
    let cache_a = worker_a_dir.join("cache");
    let journal_a = worker_a_dir.join("journal");
    let capture_a = worker_a_dir.join("capture");
    let sock_a = worker_a_dir.join("worker-a.sock");
    std::fs::create_dir_all(&worker_a_dir).unwrap();

    let worker_a_child = Command::new(cli_bin())
        .args([
            "--endpoint",
            &endpoint,
            "--workspace-id",
            ws_id,
            "--principal-id",
            principal_id,
            "--auth-token",
            "cli-test-token",
            "volume",
            "serve",
            "--branch-id",
            &main_branch_id,
            "--cache-dir",
            cache_a.to_str().unwrap(),
            "--journal-dir",
            journal_a.to_str().unwrap(),
            "--capture-dir",
            capture_a.to_str().unwrap(),
            "--control-socket",
            sock_a.to_str().unwrap(),
        ])
        .spawn()
        .expect("failed to start Worker A");
    let mut worker_a_guard = ChildGuard::new(worker_a_child);
    wait_for_socket(&sock_a);

    // 3. Write bytes (WriteAck), flush (FlushAck), stage artifact, create/append log (AppendAck).
    let pattern_hex = "cafebabe".repeat(1024); // 4096 bytes
    let write_ack = run_cli(
        &endpoint,
        ws_id,
        principal_id,
        &[
            "volume",
            "write",
            "--control-socket",
            sock_a.to_str().unwrap(),
            "--offset",
            "0",
            "--data-hex",
            &pattern_hex,
        ],
    );
    assert_eq!(write_ack["local_volatile_only"], Value::Bool(true));

    let flush_ack = run_cli(
        &endpoint,
        ws_id,
        principal_id,
        &[
            "volume",
            "flush",
            "--control-socket",
            sock_a.to_str().unwrap(),
        ],
    );
    assert_eq!(flush_ack["durable_live_volume"], Value::Bool(true));

    run_cli(
        &endpoint,
        ws_id,
        principal_id,
        &[
            "artifact",
            "stage-put",
            "--control-socket",
            sock_a.to_str().unwrap(),
            "--name",
            "plan.txt",
            "--payload-hex",
            "706c616e2d7631", // "plan-v1"
        ],
    );

    let log_created = run_cli(
        &endpoint,
        ws_id,
        principal_id,
        &[
            "log",
            "create",
            "--branch-id",
            &main_branch_id,
            "--name",
            "events",
        ],
    );
    let log_id = log_created["stream_id"]["log_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let append_ack = run_cli(
        &endpoint,
        ws_id,
        principal_id,
        &[
            "log",
            "append",
            "--branch-id",
            &main_branch_id,
            "--name",
            "events",
            "--stream-epoch",
            &main_stream_epoch.to_string(),
            "--log-id",
            &log_id,
            "--match-seq",
            "0",
            "--record",
            "step-1-ok",
        ],
    );
    assert_eq!(append_ack["durable_log_range"], Value::Bool(true));

    // 4. Checkpoint G1 with provenance (including secret token to verify sanitization).
    let g1_ack = run_cli(
        &endpoint,
        ws_id,
        principal_id,
        &[
            "checkpoint",
            "create",
            "--control-socket",
            sock_a.to_str().unwrap(),
            "--mode",
            "block_cut",
            "--actor",
            "agent-a",
            "--reason",
            "checkpoint with Bearer sk-secret999999",
        ],
    );
    assert_eq!(g1_ack["published_generation"], Value::Bool(true));
    assert_eq!(
        g1_ack["provenance_status"],
        Value::String("recorded".to_owned())
    );
    let g1_id = g1_ack["generation_id"].as_str().unwrap().to_owned();

    // 5. Write uncheckpointed live state to main, fork candidate branch from G1, promote candidate
    //    -> main discarding live state.
    run_cli(
        &endpoint,
        ws_id,
        principal_id,
        &[
            "volume",
            "write",
            "--control-socket",
            sock_a.to_str().unwrap(),
            "--offset",
            "4096",
            "--data-hex",
            &"deadbeef".repeat(1024),
        ],
    );
    run_cli(
        &endpoint,
        ws_id,
        principal_id,
        &[
            "volume",
            "flush",
            "--control-socket",
            sock_a.to_str().unwrap(),
        ],
    );

    let cand = run_cli(
        &endpoint,
        ws_id,
        principal_id,
        &[
            "branch",
            "create",
            "--name",
            "candidate",
            "--base-generation",
            &g1_id,
        ],
    );
    let cand_id = cand["branch_id"].as_str().unwrap().to_owned();
    let cand_rev = cand["revision"].as_u64().unwrap();

    // Detach Worker A, wait for exit, and delete Worker A's entire local state directory.
    run_cli(
        &endpoint,
        ws_id,
        principal_id,
        &[
            "volume",
            "detach",
            "--control-socket",
            sock_a.to_str().unwrap(),
            "--branch-id",
            &main_branch_id,
        ],
    );
    worker_a_guard.wait_exit(Duration::from_secs(5));
    std::fs::remove_dir_all(&worker_a_dir).unwrap();

    let main_after_detach = run_cli(
        &endpoint,
        ws_id,
        principal_id,
        &["branch", "get", "--branch-id", &main_branch_id],
    );
    let main_rev = main_after_detach["revision"].as_u64().unwrap();
    let main_writer_epoch = main_after_detach["writer_epoch"].as_u64().unwrap();
    run_cli(
        &endpoint,
        ws_id,
        principal_id,
        &[
            "writer",
            "acquire",
            "--branch-id",
            &main_branch_id,
            "--expected-revision",
            &main_rev.to_string(),
            "--expected-writer-epoch",
            &main_writer_epoch.to_string(),
        ],
    );

    run_cli(
        &endpoint,
        ws_id,
        principal_id,
        &[
            "branch",
            "promote",
            "--source-branch-id",
            &cand_id,
            "--expected-source-revision",
            &cand_rev.to_string(),
            "--destination-branch-id",
            &main_branch_id,
            "--expected-destination-revision",
            &main_rev.to_string(),
            "--discard-live-state",
        ],
    );

    // 6. Attach Worker B and start it with a completely fresh empty cache on `main` (now at G1).
    let main_after_promote = run_cli(
        &endpoint,
        ws_id,
        principal_id,
        &["branch", "get", "--branch-id", &main_branch_id],
    );
    let rev_for_b = main_after_promote["revision"].as_u64().unwrap();
    let writer_epoch_for_b = main_after_promote["writer_epoch"].as_u64().unwrap();
    run_cli(
        &endpoint,
        ws_id,
        principal_id,
        &[
            "writer",
            "acquire",
            "--branch-id",
            &main_branch_id,
            "--expected-revision",
            &rev_for_b.to_string(),
            "--expected-writer-epoch",
            &writer_epoch_for_b.to_string(),
        ],
    );
    run_cli(
        &endpoint,
        ws_id,
        principal_id,
        &[
            "volume",
            "attach",
            "--branch-id",
            &main_branch_id,
            "--expected-revision",
            &rev_for_b.to_string(),
        ],
    );

    let worker_b_dir = dir.0.join("worker-b");
    let cache_b = worker_b_dir.join("cache");
    let journal_b = worker_b_dir.join("journal");
    let capture_b = worker_b_dir.join("capture");
    let sock_b = worker_b_dir.join("worker-b.sock");
    std::fs::create_dir_all(&worker_b_dir).unwrap();

    let worker_b_child = Command::new(cli_bin())
        .args([
            "--endpoint",
            &endpoint,
            "--workspace-id",
            ws_id,
            "--principal-id",
            principal_id,
            "--auth-token",
            "cli-test-token",
            "volume",
            "serve",
            "--branch-id",
            &main_branch_id,
            "--cache-dir",
            cache_b.to_str().unwrap(),
            "--journal-dir",
            journal_b.to_str().unwrap(),
            "--capture-dir",
            capture_b.to_str().unwrap(),
            "--control-socket",
            sock_b.to_str().unwrap(),
        ])
        .spawn()
        .expect("failed to start Worker B");
    let mut worker_b_guard = ChildGuard::new(worker_b_child);
    wait_for_socket(&sock_b);

    // Lazy read from Worker B's empty cache must return Worker A's checkpointed pattern!
    let read_b = run_cli(
        &endpoint,
        ws_id,
        principal_id,
        &[
            "volume",
            "read",
            "--control-socket",
            sock_b.to_str().unwrap(),
            "--offset",
            "0",
            "--length",
            "4096",
        ],
    );
    assert_eq!(read_b["data_hex"].as_str().unwrap(), pattern_hex);

    // Inspect provenance on G1 -> recorded, actor == "agent-a", secret redacted.
    let prov = run_cli(
        &endpoint,
        ws_id,
        principal_id,
        &[
            "generation",
            "inspect-provenance",
            "--generation-id",
            &g1_id,
        ],
    );
    assert_eq!(prov["status"], Value::String("recorded".to_owned()));
    assert_eq!(
        prov["context"]["actor"],
        Value::String("agent-a".to_owned())
    );
    let reason = prov["context"]["reason"].as_str().unwrap();
    assert!(
        !reason.contains("sk-secret999999"),
        "secret must be redacted: {reason}"
    );

    // Read native log and artifact from G1.
    let log_read = run_cli(
        &endpoint,
        ws_id,
        principal_id,
        &["log", "read", "--generation-id", &g1_id, "--name", "events"],
    );
    assert_eq!(log_read["batches"].as_array().unwrap().len(), 1);

    let art_get = run_cli(
        &endpoint,
        ws_id,
        principal_id,
        &[
            "artifact",
            "get",
            "--generation-id",
            &g1_id,
            "--name",
            "plan.txt",
        ],
    );
    assert_eq!(
        art_get["data_hex"],
        Value::String("706c616e2d7631".to_owned())
    );

    // P10 report command returns UnsupportedOperation cleanly.
    let report_err = run_cli_expect_err(&endpoint, ws_id, principal_id, &["report", "collect"]);
    assert!(
        report_err.contains("unsupported") || report_err.contains("Unsupported"),
        "unexpected report error: {report_err}"
    );

    // Explicit destructive format refuses without --confirm-destructive.
    let fmt_err = run_cli_expect_err(
        &endpoint,
        ws_id,
        principal_id,
        &["fs", "format", "--device", "/tmp/versionfs-no-dev"],
    );
    assert!(
        fmt_err.contains("confirmation") || fmt_err.contains("confirm"),
        "unexpected fs format error: {fmt_err}"
    );

    run_cli(
        &endpoint,
        ws_id,
        principal_id,
        &[
            "volume",
            "detach",
            "--control-socket",
            sock_b.to_str().unwrap(),
            "--branch-id",
            &main_branch_id,
        ],
    );
    worker_b_guard.wait_exit(Duration::from_secs(5));
}
