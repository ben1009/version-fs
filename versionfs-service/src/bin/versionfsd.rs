//! Standalone `versionfsd` service daemon backed by `ToyKvMetadataAuthority`
//! and `DirectoryObjectStore`.

use std::collections::HashSet;
use std::net::TcpListener;
#[cfg(unix)]
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use versionfs_core::control::start_authority_owner;
use versionfs_core::identity::{OperationId, PrincipalId};
use versionfs_service::object_store::DirectoryObjectStore;
#[cfg(unix)]
use versionfs_service::service::serve_unix;
use versionfs_service::service::{ServiceConfig, WorkspaceService, serve_tcp};
use versionfs_service::toy_kv_metadata::ToyKvMetadataAuthority;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let mut metadata_dir: Option<PathBuf> = None;
    let mut object_dir: Option<PathBuf> = None;
    let mut listen: Option<String> = None;
    let mut auth_token = std::env::var("VERSIONFS_AUTH_TOKEN").ok();
    let mut owner_op_id: Option<OperationId> = None;
    let mut deny_provenance = HashSet::new();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--metadata-dir" => {
                i += 1;
                metadata_dir = Some(PathBuf::from(
                    args.get(i).ok_or("missing value for --metadata-dir")?,
                ));
            }
            "--object-dir" => {
                i += 1;
                object_dir = Some(PathBuf::from(
                    args.get(i).ok_or("missing value for --object-dir")?,
                ));
            }
            "--listen" => {
                i += 1;
                listen = Some(args.get(i).ok_or("missing value for --listen")?.clone());
            }
            "--auth-token" => {
                i += 1;
                auth_token = Some(args.get(i).ok_or("missing value for --auth-token")?.clone());
            }
            "--owner-op-id" => {
                i += 1;
                let hex = args.get(i).ok_or("missing value for --owner-op-id")?;
                owner_op_id = Some(parse_operation_id(hex)?);
            }
            "--deny-provenance-principal" => {
                i += 1;
                let hex = args
                    .get(i)
                    .ok_or("missing value for --deny-provenance-principal")?;
                deny_provenance.insert(parse_principal_id(hex)?);
            }
            other => {
                return Err(format!("unknown argument: {other}").into());
            }
        }
        i += 1;
    }

    let metadata_dir = metadata_dir.ok_or("--metadata-dir is required")?;
    let object_dir = object_dir.ok_or("--object-dir is required")?;
    let listen = listen.ok_or("--listen is required (unix:///path or tcp://addr:port)")?;
    let auth_token = auth_token
        .filter(|t| !t.is_empty())
        .ok_or("--auth-token or VERSIONFS_AUTH_TOKEN is required")?;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let authority = Arc::new(ToyKvMetadataAuthority::open(&metadata_dir)?);
    let op_id = owner_op_id.unwrap_or_else(random_operation_id);
    let incarnation = start_authority_owner(authority.as_ref(), op_id)?;
    let object_store = Arc::new(DirectoryObjectStore::open(&object_dir)?);

    let mut config = ServiceConfig::new(auth_token.into_bytes());
    config.provenance_deny_principals = deny_provenance;
    let service = Arc::new(WorkspaceService::new(
        authority,
        incarnation,
        object_store,
        rt.handle().clone(),
        config,
    ));

    let shutdown = Arc::new(AtomicBool::new(false));
    if let Some(addr) = listen.strip_prefix("tcp://") {
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener.local_addr()?;
        println!("READY tcp://{local_addr}");
        serve_tcp(service, listener, Arc::clone(&shutdown))?;
    } else if let Some(path) = listen.strip_prefix("unix://") {
        #[cfg(unix)]
        {
            use std::os::unix::fs::{FileTypeExt, PermissionsExt};
            let sock_path = PathBuf::from(path);
            if let Ok(meta) = std::fs::symlink_metadata(&sock_path) {
                if !meta.file_type().is_socket() {
                    return Err(format!(
                        "refusing to remove non-socket path {}",
                        sock_path.display()
                    )
                    .into());
                }
                match std::os::unix::net::UnixStream::connect(&sock_path) {
                    Ok(_) => {
                        return Err(format!(
                            "socket {} is already in use by an active listener",
                            sock_path.display()
                        )
                        .into());
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                        std::fs::remove_file(&sock_path)?;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            let listener = UnixListener::bind(&sock_path)?;
            let _ = std::fs::set_permissions(&sock_path, std::fs::Permissions::from_mode(0o600));
            println!("READY unix://{path}");
            serve_unix(service, listener, Arc::clone(&shutdown))?;
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            return Err("unix:// listeners are only supported on Unix".into());
        }
    } else {
        return Err("--listen must start with tcp:// or unix://".into());
    }

    let _ = shutdown.load(Ordering::Relaxed);
    Ok(())
}

fn random_operation_id() -> OperationId {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(1, |d| d.as_nanos());
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&(nanos as u64).to_le_bytes());
    bytes[8..12].copy_from_slice(&std::process::id().to_le_bytes());
    bytes[12] = 0xD9;
    OperationId::from_bytes(bytes)
}

fn parse_operation_id(hex: &str) -> Result<OperationId, Box<dyn std::error::Error>> {
    Ok(OperationId::from_bytes(parse_hex_16(hex)?))
}

fn parse_principal_id(hex: &str) -> Result<PrincipalId, Box<dyn std::error::Error>> {
    Ok(PrincipalId::from_bytes(parse_hex_16(hex)?))
}

fn parse_hex_16(hex: &str) -> Result<[u8; 16], Box<dyn std::error::Error>> {
    let hex = hex.strip_prefix("0x").unwrap_or(hex);
    if hex.len() != 32 {
        return Err(format!("expected 32 hex chars, got {}", hex.len()).into());
    }
    let mut out = [0_u8; 16];
    for i in 0..16 {
        out[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)?;
    }
    Ok(out)
}
