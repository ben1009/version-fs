//! Workspace-scoped immutable object storage.
//!
//! The public API accepts typed Version FS objects and deliberately provides
//! no overwrite or delete operation. S3 keys are scoped by deployment and
//! workspace; the content digest is never an authorization capability.

use async_trait::async_trait;
use futures_util::TryStreamExt;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::list::{PaginatedListOptions, PaginatedListStore};
use object_store::path::Path;
use object_store::{Error as S3Error, GetOptions, ObjectStore as S3Store, PutMode, PutOptions};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, Weak};
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use versionfs_core::format::{
    ENVELOPE_HEADER_BYTES, MAX_ENCODED_OBJECT_BYTES, ObjectRef, ObjectType,
};
use versionfs_core::identity::{ObjectId, ObjectStoreScopeId, WorkspaceId};
use versionfs_core::persistent_map::{DurableObjectStore, MapObjectStore};

/// Maximum number of objects returned by one inventory request.
pub const MAX_INVENTORY_PAGE_SIZE: usize = 1_000;
/// Default concurrent object transfers admitted for one workspace.
pub const DEFAULT_WORKSPACE_TRANSFER_LIMIT: usize = 8;
/// Default concurrent object transfers admitted by one service instance.
pub const DEFAULT_SERVICE_TRANSFER_LIMIT: usize = 32;
const MAX_UPLOAD_ATTEMPTS: usize = 2;

/// Errors returned by the trusted object layer.
#[derive(Debug, Error)]
pub enum ObjectStoreError {
    /// The supplied bytes are not a supported canonical Version FS object.
    #[error("invalid encoded object: {0}")]
    InvalidObject(#[from] versionfs_core::format::FormatError),
    /// A validly addressed object is missing from the requested workspace.
    #[error("object {object_id} is missing from workspace {workspace_id}")]
    NotFound {
        /// Workspace whose object namespace was queried.
        workspace_id: WorkspaceId,
        /// Expected object identity.
        object_id: ObjectId,
    },
    /// Stored bytes failed the expected hash, length, or type check.
    #[error("stored object {object_id} in workspace {workspace_id} failed integrity verification")]
    IntegrityMismatch {
        /// Workspace whose object namespace was queried.
        workspace_id: WorkspaceId,
        /// Expected object identity.
        object_id: ObjectId,
    },
    /// An upload may have committed, but a verified read could not establish its outcome.
    #[error(
        "upload outcome for object {object_id} in workspace {workspace_id} is unknown: {reason}"
    )]
    OutcomeUnknown {
        /// Workspace whose object namespace was written.
        workspace_id: WorkspaceId,
        /// Expected object identity.
        object_id: ObjectId,
        /// Upload and reconciliation details.
        reason: String,
    },
    /// The requested object or inventory page exceeds configured bounds.
    #[error("object-store request exceeds its bound: {0}")]
    LimitExceeded(&'static str),
    /// Inventory pagination used a cursor from another workspace or deployment prefix.
    #[error("inventory cursor does not belong to this object namespace")]
    InvalidCursor,
    /// The object listing contains a key that does not match the canonical object layout.
    #[error("object inventory contains an invalid entry: {0}")]
    InvalidInventoryEntry(String),
    /// Object-store configuration is invalid.
    #[error("invalid object-store configuration: {0}")]
    InvalidConfiguration(String),
    /// The backend could not complete the requested operation.
    #[error("object-store backend error: {0}")]
    Backend(String),
    /// A transfer semaphore was closed while a request was waiting for admission.
    #[error("object-store transfer admission is closed")]
    AdmissionClosed,
}

/// The result category for a successful immutable object upload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PutDisposition {
    /// This request conditionally created the object.
    Created,
    /// The key already existed and its complete bytes were verified as identical.
    Reused,
}

/// An object reference and the outcome of its idempotent upload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoredObject {
    /// Verified reference to the complete encoded object.
    pub object_ref: ObjectRef,
    /// Whether the object was created or verified for reuse.
    pub disposition: PutDisposition,
}

/// An unverified object observation returned by inventory.
///
/// Inventory reads names and sizes only. Call [`ObjectStore::get`] before using
/// an entry as trusted state; a listed key is not proof that its bytes match.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InventoryEntry {
    /// Type encoded in the object's canonical key.
    pub object_type: ObjectType,
    /// Digest encoded in the object's canonical key.
    pub object_id: ObjectId,
    /// Size reported by the backend, not yet verified against the object bytes.
    pub observed_encoded_len: u64,
}

/// Opaque continuation cursor bound to one deployment prefix and workspace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InventoryCursor {
    workspace_id: WorkspaceId,
    deployment_prefix: String,
    backend_namespace: String,
    backend_token: String,
}

/// One bounded page of unverified inventory observations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InventoryPage {
    /// Object names and observed sizes in this page.
    pub entries: Vec<InventoryEntry>,
    /// Cursor for the next backend page, if one exists.
    pub next_cursor: Option<InventoryCursor>,
}

/// Typed, workspace-scoped immutable object API used by Version FS protocols.
///
/// This interface has no delete or overwrite operation. `put` validates the
/// canonical object encoding and computes its own type, digest, and length;
/// callers cannot choose a key or claim a digest.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Conditionally creates an encoded object or verifies identical reuse.
    async fn put(
        &self,
        workspace_id: WorkspaceId,
        encoded_object: &[u8],
    ) -> Result<StoredObject, ObjectStoreError>;

    /// Fetches and verifies every byte against the expected reference.
    async fn get(
        &self,
        workspace_id: WorkspaceId,
        expected: ObjectRef,
    ) -> Result<Vec<u8>, ObjectStoreError>;

    /// Lists a bounded page of names and observed sizes for one workspace.
    async fn inventory_page(
        &self,
        workspace_id: WorkspaceId,
        cursor: Option<&InventoryCursor>,
        limit: usize,
    ) -> Result<InventoryPage, ObjectStoreError>;
}

/// S3 server-side encryption required for immutable object uploads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum S3Encryption {
    /// Request S3-managed AES-256 encryption for every new object.
    SseS3,
    /// Request SSE-KMS using the configured key identifier.
    SseKms(String),
}

/// Configuration for the Amazon S3 Standard object backend.
#[derive(Clone, Debug)]
pub struct S3ObjectStoreConfig {
    bucket: String,
    deployment_prefix: String,
    encryption: S3Encryption,
    workspace_transfer_limit: usize,
    service_transfer_limit: usize,
    #[cfg(feature = "s3-emulator")]
    local_emulator_endpoint: Option<String>,
}

impl S3ObjectStoreConfig {
    /// Creates configuration for a bucket and isolated deployment prefix.
    pub fn new(bucket: impl Into<String>, deployment_prefix: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            deployment_prefix: deployment_prefix.into(),
            encryption: S3Encryption::SseS3,
            workspace_transfer_limit: DEFAULT_WORKSPACE_TRANSFER_LIMIT,
            service_transfer_limit: DEFAULT_SERVICE_TRANSFER_LIMIT,
            #[cfg(feature = "s3-emulator")]
            local_emulator_endpoint: None,
        }
    }

    /// Selects SSE-S3 or SSE-KMS for new uploads.
    pub fn with_encryption(mut self, encryption: S3Encryption) -> Self {
        self.encryption = encryption;
        self
    }

    /// Sets the bounded transfer concurrency for each workspace and service.
    pub fn with_transfer_limits(mut self, workspace_limit: usize, service_limit: usize) -> Self {
        self.workspace_transfer_limit = workspace_limit;
        self.service_transfer_limit = service_limit;
        self
    }

    /// Enables the test-only S3 emulator endpoint.
    ///
    /// Available only with the `s3-emulator` feature. The endpoint is checked
    /// by [`S3ObjectStore::connect`] and must be plain HTTP on `127.0.0.1`.
    /// This local test path does not request server-side encryption; use the
    /// Amazon S3 conformance path to validate encryption behavior.
    #[cfg(feature = "s3-emulator")]
    pub fn with_local_emulator_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.local_emulator_endpoint = Some(endpoint.into());
        self
    }
}

/// S3-backed implementation of the immutable object API.
pub struct S3ObjectStore {
    backend: Arc<dyn Backend>,
    deployment_prefix: String,
    backend_namespace: String,
    transfer_budget: TransferBudget,
}

/// Synchronous typed map-store bridge for block workers using an S3 client.
///
/// Block algorithms run on the ublk request thread and require synchronous
/// object reads. This adapter drives the async S3 client through the supplied
/// Tokio handle. Call its map methods only from a thread outside a Tokio
/// runtime; a runtime worker is rejected instead of being blocked or nested.
pub struct BlockingS3MapObjectStore {
    store: Arc<S3ObjectStore>,
    workspace_id: WorkspaceId,
    scope: ObjectStoreScopeId,
    runtime: Handle,
}

impl BlockingS3MapObjectStore {
    /// Creates a workspace-scoped bridge for one S3 store and runtime.
    pub fn new(store: Arc<S3ObjectStore>, workspace_id: WorkspaceId, runtime: Handle) -> Self {
        let mut scope_input =
            Vec::with_capacity(store.backend_namespace.len() + workspace_id.as_bytes().len());
        scope_input.extend_from_slice(store.backend_namespace.as_bytes());
        scope_input.extend_from_slice(workspace_id.as_bytes());
        let digest = Sha256::digest(&scope_input);
        let scope = ObjectStoreScopeId::from_bytes(
            digest[..16]
                .try_into()
                .expect("SHA-256 digest has at least 16 bytes"),
        );
        Self {
            store,
            workspace_id,
            scope,
            runtime,
        }
    }

    fn block_on<T>(
        &self,
        future: impl Future<Output = Result<T, ObjectStoreError>>,
    ) -> Result<T, String> {
        if Handle::try_current().is_ok() {
            return Err("blocking S3 map access cannot run on a Tokio runtime thread".to_owned());
        }
        self.runtime
            .block_on(future)
            .map_err(|error| error.to_string())
    }
}

impl MapObjectStore for BlockingS3MapObjectStore {
    fn proof_scope(&self) -> ObjectStoreScopeId {
        self.scope
    }

    fn get(&self, _id: ObjectId) -> Result<Option<Vec<u8>>, String> {
        Err("typed ObjectRef is required for workspace-scoped S3 reads".to_owned())
    }

    fn get_ref(&self, reference: ObjectRef) -> Result<Option<Vec<u8>>, String> {
        let store = Arc::clone(&self.store);
        let workspace_id = self.workspace_id;
        self.block_on(async move {
            match store.get(workspace_id, reference).await {
                Ok(encoded) => Ok(Some(encoded)),
                Err(ObjectStoreError::NotFound { .. }) => Ok(None),
                Err(error) => Err(error),
            }
        })
    }

    fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
        let reference = ObjectRef::from_encoded(&bytes).map_err(|error| error.to_string())?;
        if reference.object_id != id {
            return Err("object ID does not match encoded bytes".to_owned());
        }
        self.put_durable(reference, &bytes)
    }
}

impl DurableObjectStore for BlockingS3MapObjectStore {
    fn put_durable(&mut self, reference: ObjectRef, encoded: &[u8]) -> Result<(), String> {
        reference
            .verify(encoded)
            .map_err(|error| error.to_string())?;
        let store = Arc::clone(&self.store);
        let workspace_id = self.workspace_id;
        let encoded = encoded.to_vec();
        self.block_on(async move {
            let stored = store.put(workspace_id, &encoded).await?;
            if stored.object_ref != reference {
                return Err(ObjectStoreError::IntegrityMismatch {
                    workspace_id,
                    object_id: reference.object_id,
                });
            }
            let verified = store.get(workspace_id, reference).await?;
            if verified != encoded {
                return Err(ObjectStoreError::IntegrityMismatch {
                    workspace_id,
                    object_id: reference.object_id,
                });
            }
            Ok(())
        })
    }
}

impl S3ObjectStore {
    /// Connects using the standard AWS credential chain and HTTPS.
    ///
    /// The adapter enables conditional create, configures encryption, and never
    /// exposes backend overwrite or delete methods through [`ObjectStore`].
    /// With the `s3-emulator` feature, a test-only endpoint may instead use
    /// plain HTTP if it is bound to `127.0.0.1`.
    pub fn connect(config: S3ObjectStoreConfig) -> Result<Self, ObjectStoreError> {
        let deployment_prefix = normalize_prefix(&config.deployment_prefix)?;
        if config.bucket.is_empty() {
            return Err(ObjectStoreError::InvalidConfiguration(
                "bucket name must not be empty".to_owned(),
            ));
        }
        let transfer_budget = TransferBudget::new(
            config.workspace_transfer_limit,
            config.service_transfer_limit,
        )?;

        #[cfg(feature = "s3-emulator")]
        if let Some(endpoint) = config.local_emulator_endpoint.as_deref() {
            validate_local_emulator_endpoint(endpoint)?;
        }

        #[cfg(feature = "s3-emulator")]
        let backend_namespace = match config.local_emulator_endpoint.as_deref() {
            Some(endpoint) => format!(
                "s3-emulator://{}/{}/{}",
                endpoint, config.bucket, deployment_prefix
            ),
            None => format!("s3://{}/{}", config.bucket, deployment_prefix),
        };
        #[cfg(not(feature = "s3-emulator"))]
        let backend_namespace = format!("s3://{}/{}", config.bucket, deployment_prefix);

        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(config.bucket)
            .with_allow_http(false)
            .with_s3_express(false)
            .with_conditional_put(S3ConditionalPut::ETagMatch);
        #[cfg(feature = "s3-emulator")]
        if let Some(endpoint) = config.local_emulator_endpoint.as_deref() {
            builder = builder.with_endpoint(endpoint).with_allow_http(true);
        }
        #[cfg(feature = "s3-emulator")]
        let request_server_side_encryption = config.local_emulator_endpoint.is_none();
        #[cfg(not(feature = "s3-emulator"))]
        let request_server_side_encryption = true;

        builder = match config.encryption {
            S3Encryption::SseS3 if request_server_side_encryption => builder.with_config(
                "aws_server_side_encryption"
                    .parse()
                    .map_err(|error: S3Error| {
                        ObjectStoreError::InvalidConfiguration(error.to_string())
                    })?,
                "AES256",
            ),
            S3Encryption::SseS3 => builder,
            S3Encryption::SseKms(key_id)
                if !key_id.trim().is_empty() && request_server_side_encryption =>
            {
                builder.with_sse_kms_encryption(key_id)
            }
            S3Encryption::SseKms(key_id) if !key_id.trim().is_empty() => builder,
            S3Encryption::SseKms(_) => {
                return Err(ObjectStoreError::InvalidConfiguration(
                    "SSE-KMS key identifier must not be empty".to_owned(),
                ));
            }
        };
        let backend = builder
            .build()
            .map_err(|error| ObjectStoreError::InvalidConfiguration(error.to_string()))?;

        Ok(Self {
            backend: Arc::new(S3Backend {
                store: Arc::new(backend),
            }),
            deployment_prefix,
            backend_namespace,
            transfer_budget,
        })
    }

    #[cfg(test)]
    fn with_backend(
        backend: Arc<dyn Backend>,
        deployment_prefix: &str,
        workspace_limit: usize,
        service_limit: usize,
    ) -> Result<Self, ObjectStoreError> {
        Ok(Self {
            backend,
            deployment_prefix: normalize_prefix(deployment_prefix)?,
            backend_namespace: format!("test://memory/{deployment_prefix}"),
            transfer_budget: TransferBudget::new(workspace_limit, service_limit)?,
        })
    }

    fn workspace_prefix(&self, workspace_id: WorkspaceId) -> String {
        format!(
            "{}/workspaces/{workspace_id}/objects",
            self.deployment_prefix
        )
    }

    fn object_key(&self, workspace_id: WorkspaceId, object_ref: ObjectRef) -> String {
        format!(
            "{}/{:04x}/{}",
            self.workspace_prefix(workspace_id),
            object_ref.object_type as u16,
            object_ref.object_id
        )
    }

    async fn verified_get(
        &self,
        workspace_id: WorkspaceId,
        key: &str,
        expected: ObjectRef,
    ) -> Result<Vec<u8>, ObjectStoreError> {
        let encoded = self.backend.get(key).await.map_err(|error| match error {
            BackendError::NotFound => ObjectStoreError::NotFound {
                workspace_id,
                object_id: expected.object_id,
            },
            BackendError::TooLarge | BackendError::InvalidBytes(_) => {
                ObjectStoreError::IntegrityMismatch {
                    workspace_id,
                    object_id: expected.object_id,
                }
            }
            BackendError::AlreadyExists => ObjectStoreError::Backend(
                "backend returned an invalid already-exists result for GET".to_owned(),
            ),
            BackendError::Failed(message) | BackendError::Unknown(message) => {
                ObjectStoreError::Backend(message)
            }
        })?;
        if expected.verify(&encoded).is_err() {
            return Err(ObjectStoreError::IntegrityMismatch {
                workspace_id,
                object_id: expected.object_id,
            });
        }
        Ok(encoded)
    }

    async fn reconcile_upload(
        &self,
        workspace_id: WorkspaceId,
        key: &str,
        object_ref: ObjectRef,
        encoded_object: &[u8],
        upload_error: &str,
    ) -> Result<Option<StoredObject>, ObjectStoreError> {
        match self.verified_get(workspace_id, key, object_ref).await {
            Ok(existing) if existing == encoded_object => Ok(Some(StoredObject {
                object_ref,
                disposition: PutDisposition::Reused,
            })),
            Ok(_) => Err(ObjectStoreError::IntegrityMismatch {
                workspace_id,
                object_id: object_ref.object_id,
            }),
            Err(ObjectStoreError::NotFound { .. }) => Ok(None),
            Err(error @ ObjectStoreError::IntegrityMismatch { .. }) => Err(error),
            Err(read_error) => Err(ObjectStoreError::OutcomeUnknown {
                workspace_id,
                object_id: object_ref.object_id,
                reason: format!("upload: {upload_error}; reconciliation: {read_error}"),
            }),
        }
    }
}

#[async_trait]
impl ObjectStore for S3ObjectStore {
    async fn put(
        &self,
        workspace_id: WorkspaceId,
        encoded_object: &[u8],
    ) -> Result<StoredObject, ObjectStoreError> {
        if encoded_object.len() > MAX_ENCODED_OBJECT_BYTES {
            return Err(ObjectStoreError::LimitExceeded(
                "encoded object exceeds the canonical format maximum",
            ));
        }
        let object_ref = ObjectRef::from_encoded(encoded_object)?;
        let key = self.object_key(workspace_id, object_ref);
        let _permit = self.transfer_budget.acquire(workspace_id).await?;
        let mut previous_unknown = None;

        for attempt in 0..MAX_UPLOAD_ATTEMPTS {
            match self.backend.put_if_absent(&key, encoded_object).await {
                Ok(()) => {
                    return Ok(StoredObject {
                        object_ref,
                        disposition: PutDisposition::Created,
                    });
                }
                Err(BackendError::AlreadyExists) => {
                    return self
                        .reconcile_upload(
                            workspace_id,
                            &key,
                            object_ref,
                            encoded_object,
                            "conditional create reported an existing key",
                        )
                        .await?
                        .ok_or_else(|| ObjectStoreError::OutcomeUnknown {
                            workspace_id,
                            object_id: object_ref.object_id,
                            reason: "conditional create reported an existing key, but a verified GET found no object".to_owned(),
                        });
                }
                Err(BackendError::Failed(message)) => {
                    if let Some(previous_unknown) = previous_unknown {
                        return self
                            .reconcile_upload(
                                workspace_id,
                                &key,
                                object_ref,
                                encoded_object,
                                &format!("{previous_unknown}; retry failed: {message}"),
                            )
                            .await?
                            .ok_or_else(|| ObjectStoreError::OutcomeUnknown {
                                workspace_id,
                                object_id: object_ref.object_id,
                                reason: format!("{previous_unknown}; retry failed: {message}; object is still missing"),
                            });
                    }
                    return Err(ObjectStoreError::Backend(message));
                }
                Err(BackendError::TooLarge | BackendError::InvalidBytes(_)) => {
                    return Err(ObjectStoreError::Backend(
                        "backend rejected an encoded object before upload".to_owned(),
                    ));
                }
                Err(BackendError::Unknown(put_error)) => {
                    previous_unknown = Some(match previous_unknown {
                        Some(previous) => format!("{previous}; retry outcome: {put_error}"),
                        None => put_error,
                    });
                    match self
                        .reconcile_upload(
                            workspace_id,
                            &key,
                            object_ref,
                            encoded_object,
                            previous_unknown
                                .as_deref()
                                .expect("set before reconciliation"),
                        )
                        .await?
                    {
                        Some(stored) => return Ok(stored),
                        None if attempt + 1 < MAX_UPLOAD_ATTEMPTS => {
                            // The retry is byte-identical and still conditional; it cannot
                            // overwrite an object that may have committed on the first try.
                            continue;
                        }
                        None => {
                            return Err(ObjectStoreError::OutcomeUnknown {
                                workspace_id,
                                object_id: object_ref.object_id,
                                reason: previous_unknown.expect("set before reconciliation"),
                            });
                        }
                    }
                }
                Err(BackendError::NotFound) => {
                    return Err(ObjectStoreError::Backend(
                        "backend returned not-found for conditional object creation".to_owned(),
                    ));
                }
            }
        }

        // `MAX_UPLOAD_ATTEMPTS` is nonzero, and every loop branch returns or continues.
        Err(ObjectStoreError::OutcomeUnknown {
            workspace_id,
            object_id: object_ref.object_id,
            reason: "upload attempts were exhausted".to_owned(),
        })
    }

    async fn get(
        &self,
        workspace_id: WorkspaceId,
        expected: ObjectRef,
    ) -> Result<Vec<u8>, ObjectStoreError> {
        let _permit = self.transfer_budget.acquire(workspace_id).await?;
        let key = self.object_key(workspace_id, expected);
        self.verified_get(workspace_id, &key, expected).await
    }

    async fn inventory_page(
        &self,
        workspace_id: WorkspaceId,
        cursor: Option<&InventoryCursor>,
        limit: usize,
    ) -> Result<InventoryPage, ObjectStoreError> {
        if limit == 0 || limit > MAX_INVENTORY_PAGE_SIZE {
            return Err(ObjectStoreError::LimitExceeded(
                "inventory page size must be between 1 and 1000",
            ));
        }
        if let Some(cursor) = cursor
            && (cursor.workspace_id != workspace_id
                || cursor.deployment_prefix != self.deployment_prefix
                || cursor.backend_namespace != self.backend_namespace)
        {
            return Err(ObjectStoreError::InvalidCursor);
        }
        let prefix = self.workspace_prefix(workspace_id);
        let list_prefix = format!("{prefix}/");
        let backend_token = cursor.map(|cursor| cursor.backend_token.as_str());
        let _permit = self.transfer_budget.acquire(workspace_id).await?;
        let page = self
            .backend
            .list_page(&list_prefix, backend_token, limit)
            .await
            .map_err(|error| ObjectStoreError::Backend(error.message()))?;
        if page.entries.len() > limit {
            return Err(ObjectStoreError::Backend(
                "backend exceeded the requested inventory page size".to_owned(),
            ));
        }
        if backend_token.is_some() && page.next_token.as_deref() == backend_token {
            return Err(ObjectStoreError::Backend(
                "backend returned a non-advancing inventory cursor".to_owned(),
            ));
        }
        let entries = page
            .entries
            .into_iter()
            .map(|entry| parse_inventory_entry(&prefix, entry))
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = page.next_token.map(|backend_token| InventoryCursor {
            workspace_id,
            deployment_prefix: self.deployment_prefix.clone(),
            backend_namespace: self.backend_namespace.clone(),
            backend_token,
        });
        Ok(InventoryPage {
            entries,
            next_cursor,
        })
    }
}

#[cfg(feature = "s3-emulator")]
fn validate_local_emulator_endpoint(endpoint: &str) -> Result<(), ObjectStoreError> {
    let valid_port = endpoint
        .strip_prefix("http://127.0.0.1:")
        .and_then(|port| port.parse::<u16>().ok())
        .is_some_and(|port| port != 0);
    if !valid_port {
        return Err(ObjectStoreError::InvalidConfiguration(
            "S3 emulator endpoint must be exactly http://127.0.0.1:<port> with a nonzero port"
                .to_owned(),
        ));
    }
    Ok(())
}

fn normalize_prefix(prefix: &str) -> Result<String, ObjectStoreError> {
    let trimmed = prefix.trim_matches('/');
    if trimmed.is_empty() {
        return Err(ObjectStoreError::InvalidConfiguration(
            "deployment prefix must not be empty".to_owned(),
        ));
    }
    if trimmed.split('/').any(|segment| {
        segment.is_empty()
            || segment == "."
            || segment == ".."
            || !segment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    }) {
        return Err(ObjectStoreError::InvalidConfiguration(
            "deployment prefix may contain only non-empty alphanumeric, dash, underscore, or dot segments".to_owned(),
        ));
    }
    Ok(trimmed.to_owned())
}

fn parse_inventory_entry(
    prefix: &str,
    entry: BackendEntry,
) -> Result<InventoryEntry, ObjectStoreError> {
    let namespace_prefix = format!("{prefix}/");
    let relative = entry
        .key
        .strip_prefix(&namespace_prefix)
        .ok_or_else(|| ObjectStoreError::InvalidInventoryEntry(entry.key.clone()))?;
    let mut components = relative.split('/');
    let type_component = components.next();
    let digest_component = components.next();
    if components.next().is_some() {
        return Err(ObjectStoreError::InvalidInventoryEntry(entry.key));
    }
    let (Some(type_component), Some(digest_component)) = (type_component, digest_component) else {
        return Err(ObjectStoreError::InvalidInventoryEntry(entry.key));
    };
    if type_component.len() != 4
        || !type_component
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ObjectStoreError::InvalidInventoryEntry(entry.key));
    }
    let type_code = u16::from_str_radix(type_component, 16)
        .map_err(|_| ObjectStoreError::InvalidInventoryEntry(entry.key.clone()))?;
    let object_type = ObjectType::try_from(type_code)
        .map_err(|_| ObjectStoreError::InvalidInventoryEntry(entry.key.clone()))?;
    let object_id = parse_object_id(digest_component)
        .ok_or_else(|| ObjectStoreError::InvalidInventoryEntry(entry.key.clone()))?;
    if object_id.to_string() != digest_component
        || entry.encoded_len < ENVELOPE_HEADER_BYTES as u64
        || entry.encoded_len > MAX_ENCODED_OBJECT_BYTES as u64
    {
        return Err(ObjectStoreError::InvalidInventoryEntry(entry.key));
    }
    Ok(InventoryEntry {
        object_type,
        object_id,
        observed_encoded_len: entry.encoded_len,
    })
}

fn parse_object_id(digest: &str) -> Option<ObjectId> {
    if digest.len() != 64 {
        return None;
    }
    let mut bytes = [0; 32];
    let pairs = digest.as_bytes().as_chunks::<2>().0;
    for (index, pair) in pairs.iter().enumerate() {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        bytes[index] = (high << 4) | low;
    }
    Some(ObjectId::from_bytes(bytes))
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug)]
struct TransferLimits {
    workspace: usize,
}

#[derive(Clone)]
struct TransferBudget {
    limits: TransferLimits,
    service: Arc<Semaphore>,
    workspaces: Arc<Mutex<HashMap<WorkspaceId, Weak<Semaphore>>>>,
}

struct TransferPermit {
    _workspace: OwnedSemaphorePermit,
    _service: OwnedSemaphorePermit,
}

impl TransferBudget {
    fn new(workspace: usize, service: usize) -> Result<Self, ObjectStoreError> {
        if workspace == 0
            || service == 0
            || workspace > Semaphore::MAX_PERMITS
            || service > Semaphore::MAX_PERMITS
        {
            return Err(ObjectStoreError::InvalidConfiguration(
                "transfer limits must be positive and supported by the runtime".to_owned(),
            ));
        }
        Ok(Self {
            limits: TransferLimits { workspace },
            service: Arc::new(Semaphore::new(service)),
            workspaces: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    async fn acquire(&self, workspace_id: WorkspaceId) -> Result<TransferPermit, ObjectStoreError> {
        let workspace_semaphore = {
            let mut workspaces = self
                .workspaces
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            workspaces.retain(|_, semaphore| semaphore.strong_count() > 0);
            if let Some(existing) = workspaces.get(&workspace_id).and_then(Weak::upgrade) {
                existing
            } else {
                let semaphore = Arc::new(Semaphore::new(self.limits.workspace));
                workspaces.insert(workspace_id, Arc::downgrade(&semaphore));
                semaphore
            }
        };
        let workspace = workspace_semaphore
            .acquire_owned()
            .await
            .map_err(|_| ObjectStoreError::AdmissionClosed)?;
        let service = Arc::clone(&self.service)
            .acquire_owned()
            .await
            .map_err(|_| ObjectStoreError::AdmissionClosed)?;
        Ok(TransferPermit {
            _workspace: workspace,
            _service: service,
        })
    }
}

#[derive(Debug)]
enum BackendError {
    NotFound,
    AlreadyExists,
    TooLarge,
    InvalidBytes(String),
    Failed(String),
    Unknown(String),
}

impl BackendError {
    fn message(self) -> String {
        match self {
            Self::NotFound => "object was not found".to_owned(),
            Self::AlreadyExists => "object already exists".to_owned(),
            Self::TooLarge => "object exceeds the configured byte limit".to_owned(),
            Self::InvalidBytes(message) | Self::Failed(message) | Self::Unknown(message) => message,
        }
    }
}

#[derive(Debug)]
struct BackendEntry {
    key: String,
    encoded_len: u64,
}

#[derive(Debug)]
struct BackendPage {
    entries: Vec<BackendEntry>,
    next_token: Option<String>,
}

#[async_trait]
trait Backend: Send + Sync {
    async fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<(), BackendError>;
    async fn get(&self, key: &str) -> Result<Vec<u8>, BackendError>;
    async fn list_page(
        &self,
        prefix: &str,
        token: Option<&str>,
        limit: usize,
    ) -> Result<BackendPage, BackendError>;
}

struct S3Backend {
    store: Arc<object_store::aws::AmazonS3>,
}

#[async_trait]
impl Backend for S3Backend {
    async fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<(), BackendError> {
        let path = Path::parse(key).map_err(|error| BackendError::Failed(error.to_string()))?;
        self.store
            .put_opts(
                &path,
                bytes.to_vec().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
            .map(|_| ())
            .map_err(map_put_error)
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, BackendError> {
        let path = Path::parse(key).map_err(|error| BackendError::Failed(error.to_string()))?;
        let result = self
            .store
            .get_opts(&path, GetOptions::default())
            .await
            .map_err(map_get_error)?;
        if result.meta.size > MAX_ENCODED_OBJECT_BYTES as u64 {
            return Err(BackendError::TooLarge);
        }
        let expected_len = usize::try_from(result.meta.size).map_err(|_| BackendError::TooLarge)?;
        let mut bytes = Vec::with_capacity(expected_len);
        let mut stream = result.into_stream();
        while let Some(chunk) = stream.try_next().await.map_err(map_get_error)? {
            let next_len = bytes
                .len()
                .checked_add(chunk.len())
                .ok_or(BackendError::TooLarge)?;
            if next_len > MAX_ENCODED_OBJECT_BYTES {
                return Err(BackendError::TooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() != expected_len {
            return Err(BackendError::InvalidBytes(
                "GET body length differs from the backend's declared size".to_owned(),
            ));
        }
        Ok(bytes)
    }

    async fn list_page(
        &self,
        prefix: &str,
        token: Option<&str>,
        limit: usize,
    ) -> Result<BackendPage, BackendError> {
        let result = self
            .store
            .list_paginated(
                Some(prefix),
                PaginatedListOptions {
                    page_token: token.map(str::to_owned),
                    max_keys: Some(limit),
                    ..PaginatedListOptions::default()
                },
            )
            .await
            .map_err(|error| BackendError::Failed(error.to_string()))?;
        let entries = result
            .result
            .objects
            .into_iter()
            .map(|metadata| BackendEntry {
                key: metadata.location.to_string(),
                encoded_len: metadata.size,
            })
            .collect();
        Ok(BackendPage {
            entries,
            next_token: result.page_token,
        })
    }
}

fn map_put_error(error: S3Error) -> BackendError {
    match error {
        S3Error::AlreadyExists { .. }
        | S3Error::Precondition { .. }
        | S3Error::NotModified { .. } => BackendError::AlreadyExists,
        S3Error::NotFound { .. } => BackendError::Failed(error.to_string()),
        S3Error::InvalidPath { .. }
        | S3Error::NotSupported { .. }
        | S3Error::NotImplemented { .. }
        | S3Error::PermissionDenied { .. }
        | S3Error::Unauthenticated { .. }
        | S3Error::UnknownConfigurationKey { .. } => BackendError::Failed(error.to_string()),
        _ => BackendError::Unknown(error.to_string()),
    }
}

fn map_get_error(error: S3Error) -> BackendError {
    match error {
        S3Error::NotFound { .. } => BackendError::NotFound,
        S3Error::InvalidPath { .. } => BackendError::Failed(error.to_string()),
        _ => BackendError::Unknown(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
    use std::time::Duration;
    use versionfs_core::format::{ObjectType, encode_object};

    const FAIL_PUT_BEFORE_COMMIT: u8 = 1;
    const LOSE_PUT_REPLY_AFTER_COMMIT: u8 = 2;
    const FAIL_GET: u8 = 1;

    #[derive(Default)]
    struct MemoryBackend {
        objects: Mutex<BTreeMap<String, Vec<u8>>>,
        next_put_fault: AtomicU8,
        next_get_fault: AtomicU8,
        put_calls: AtomicUsize,
    }

    impl MemoryBackend {
        fn set_put_fault(&self, fault: u8) {
            self.next_put_fault.store(fault, Ordering::SeqCst);
        }

        fn fail_next_get(&self) {
            self.next_get_fault.store(FAIL_GET, Ordering::SeqCst);
        }

        fn overwrite_raw(&self, key: String, value: Vec<u8>) {
            self.objects
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(key, value);
        }

        fn object_count(&self) -> usize {
            self.objects
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len()
        }
    }

    #[async_trait]
    impl Backend for MemoryBackend {
        async fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<(), BackendError> {
            self.put_calls.fetch_add(1, Ordering::SeqCst);
            let fault = self.next_put_fault.swap(0, Ordering::SeqCst);
            if fault == FAIL_PUT_BEFORE_COMMIT {
                return Err(BackendError::Unknown("simulated lost request".to_owned()));
            }
            let mut objects = self
                .objects
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if objects.contains_key(key) {
                return Err(BackendError::AlreadyExists);
            }
            objects.insert(key.to_owned(), bytes.to_vec());
            drop(objects);
            if fault == LOSE_PUT_REPLY_AFTER_COMMIT {
                return Err(BackendError::Unknown("simulated lost reply".to_owned()));
            }
            Ok(())
        }

        async fn get(&self, key: &str) -> Result<Vec<u8>, BackendError> {
            if self.next_get_fault.swap(0, Ordering::SeqCst) == FAIL_GET {
                return Err(BackendError::Unknown("simulated GET outage".to_owned()));
            }
            self.objects
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(key)
                .cloned()
                .ok_or(BackendError::NotFound)
        }

        async fn list_page(
            &self,
            prefix: &str,
            token: Option<&str>,
            limit: usize,
        ) -> Result<BackendPage, BackendError> {
            let objects = self
                .objects
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut matching =
                objects
                    .range((token.unwrap_or("").to_owned())..)
                    .filter(|(key, _)| {
                        key.starts_with(prefix) && token.is_none_or(|last| key.as_str() > last)
                    });
            let entries = matching
                .by_ref()
                .take(limit + 1)
                .map(|(key, bytes)| BackendEntry {
                    key: key.clone(),
                    encoded_len: bytes.len() as u64,
                })
                .collect::<Vec<_>>();
            let has_more = entries.len() > limit;
            let mut entries = entries;
            if has_more {
                entries.pop();
            }
            let next_token = has_more
                .then(|| entries.last().map(|entry| entry.key.clone()))
                .flatten();
            Ok(BackendPage {
                entries,
                next_token,
            })
        }
    }

    fn workspace(value: u8) -> WorkspaceId {
        WorkspaceId::from_bytes([value; 16])
    }

    fn make_store(backend: Arc<MemoryBackend>) -> S3ObjectStore {
        S3ObjectStore::with_backend(backend, "test/deployment", 8, 32)
            .expect("test configuration is valid")
    }

    fn encoded(kind: ObjectType, payload: &[u8]) -> Vec<u8> {
        encode_object(kind, payload).expect("test object fits the canonical format")
    }

    #[tokio::test]
    async fn put_reuses_identical_objects_and_scopes_keys_by_workspace() {
        let backend = Arc::new(MemoryBackend::default());
        let store = make_store(Arc::clone(&backend));
        let bytes = encoded(ObjectType::ArtifactBlob, b"same content");

        let first = store.put(workspace(1), &bytes).await.expect("first put");
        let duplicate = store
            .put(workspace(1), &bytes)
            .await
            .expect("duplicate put");
        let other_workspace = store.put(workspace(2), &bytes).await.expect("scoped put");

        assert_eq!(first.disposition, PutDisposition::Created);
        assert_eq!(duplicate.disposition, PutDisposition::Reused);
        assert_eq!(other_workspace.disposition, PutDisposition::Created);
        assert_eq!(first.object_ref, duplicate.object_ref);
        assert_eq!(first.object_ref, other_workspace.object_ref);
        assert_eq!(backend.object_count(), 2);
        assert_eq!(
            store.get(workspace(1), first.object_ref).await.unwrap(),
            bytes
        );
        assert!(matches!(
            store
                .get(
                    workspace(1),
                    ObjectRef {
                        encoded_len: first.object_ref.encoded_len + 1,
                        ..first.object_ref
                    }
                )
                .await,
            Err(ObjectStoreError::IntegrityMismatch { .. })
        ));
        assert!(matches!(
            store.get(workspace(3), first.object_ref).await,
            Err(ObjectStoreError::NotFound { .. })
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn blocking_map_bridge_uses_typed_refs_and_verifies_durable_puts() {
        let backend = Arc::new(MemoryBackend::default());
        let store = Arc::new(make_store(backend));
        let runtime = Handle::current();
        let first_workspace =
            BlockingS3MapObjectStore::new(Arc::clone(&store), workspace(1), runtime.clone());
        let second_workspace =
            BlockingS3MapObjectStore::new(Arc::clone(&store), workspace(2), runtime);
        assert_ne!(
            first_workspace.proof_scope(),
            second_workspace.proof_scope()
        );

        std::thread::spawn(move || {
            let mut first_workspace = first_workspace;
            let bytes = encoded(ObjectType::ArtifactBlob, b"durable bridge");
            let reference = ObjectRef::from_encoded(&bytes).expect("reference encoded object");
            first_workspace
                .put_durable(reference, &bytes)
                .expect("conditionally store and verify object");
            assert_eq!(
                first_workspace.get_ref(reference).unwrap(),
                Some(bytes.clone())
            );
            assert!(first_workspace.get(reference.object_id).is_err());
        })
        .join()
        .expect("blocking S3 bridge thread");
    }

    #[tokio::test]
    async fn ambiguous_put_is_reconciled_by_verified_get_or_identical_retry() {
        let backend = Arc::new(MemoryBackend::default());
        let store = make_store(Arc::clone(&backend));
        let bytes = encoded(ObjectType::Extent, &[7; 64]);

        backend.set_put_fault(LOSE_PUT_REPLY_AFTER_COMMIT);
        let committed = store
            .put(workspace(1), &bytes)
            .await
            .expect("lost reply reconciles");
        assert_eq!(committed.disposition, PutDisposition::Reused);
        assert_eq!(backend.put_calls.load(Ordering::SeqCst), 1);

        backend.set_put_fault(FAIL_PUT_BEFORE_COMMIT);
        let retried = store
            .put(workspace(2), &bytes)
            .await
            .expect("identical retry commits");
        assert_eq!(retried.disposition, PutDisposition::Created);
        assert_eq!(backend.put_calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn unresolved_upload_reply_never_returns_success() {
        let backend = Arc::new(MemoryBackend::default());
        let store = make_store(Arc::clone(&backend));
        let bytes = encoded(ObjectType::ArtifactBlob, b"outage");
        backend.set_put_fault(FAIL_PUT_BEFORE_COMMIT);
        backend.fail_next_get();

        let result = store.put(workspace(1), &bytes).await;
        assert!(matches!(
            result,
            Err(ObjectStoreError::OutcomeUnknown { .. })
        ));
        assert_eq!(backend.object_count(), 0);
    }

    #[tokio::test]
    async fn corrupt_existing_bytes_never_pass_as_reuse_or_verified_read() {
        let backend = Arc::new(MemoryBackend::default());
        let store = make_store(Arc::clone(&backend));
        let bytes = encoded(ObjectType::VolumeRoot, b"root");
        let reference = ObjectRef::from_encoded(&bytes).unwrap();
        store.put(workspace(1), &bytes).await.unwrap();
        let key = store.object_key(workspace(1), reference);
        backend.overwrite_raw(key, b"corrupt bytes".to_vec());

        assert!(matches!(
            store.get(workspace(1), reference).await,
            Err(ObjectStoreError::IntegrityMismatch { .. })
        ));
        assert!(matches!(
            store.put(workspace(1), &bytes).await,
            Err(ObjectStoreError::IntegrityMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn backend_read_outage_is_not_reported_as_a_missing_object() {
        let backend = Arc::new(MemoryBackend::default());
        let store = make_store(Arc::clone(&backend));
        let bytes = encoded(ObjectType::ArtifactBlob, b"available before outage");
        let reference = store.put(workspace(1), &bytes).await.unwrap().object_ref;
        backend.fail_next_get();

        assert!(matches!(
            store.get(workspace(1), reference).await,
            Err(ObjectStoreError::Backend(_))
        ));
    }

    #[tokio::test]
    async fn oversized_and_malformed_uploads_fail_before_backend_io() {
        let backend = Arc::new(MemoryBackend::default());
        let store = make_store(Arc::clone(&backend));
        let oversized = vec![0; MAX_ENCODED_OBJECT_BYTES + 1];
        assert!(matches!(
            store.put(workspace(1), &oversized).await,
            Err(ObjectStoreError::LimitExceeded(_))
        ));
        assert!(matches!(
            store.put(workspace(1), b"not an object").await,
            Err(ObjectStoreError::InvalidObject(_))
        ));
        assert_eq!(backend.put_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn inventory_is_bounded_paged_and_scope_bound() {
        let backend = Arc::new(MemoryBackend::default());
        let store = make_store(Arc::clone(&backend));
        let first = encoded(ObjectType::ArtifactBlob, b"first");
        let second = encoded(ObjectType::LogSegment, b"second");
        let first_ref = store.put(workspace(1), &first).await.unwrap().object_ref;
        let second_ref = store.put(workspace(1), &second).await.unwrap().object_ref;
        store.put(workspace(2), &first).await.unwrap();

        let page_one = store.inventory_page(workspace(1), None, 1).await.unwrap();
        assert_eq!(page_one.entries.len(), 1);
        let cursor = page_one.next_cursor.expect("second object has a cursor");
        assert!(matches!(
            store.inventory_page(workspace(2), Some(&cursor), 1).await,
            Err(ObjectStoreError::InvalidCursor)
        ));
        let page_two = store
            .inventory_page(workspace(1), Some(&cursor), 1)
            .await
            .unwrap();
        assert_eq!(page_two.entries.len(), 1);
        assert!(page_two.next_cursor.is_none());
        let found = [page_one.entries[0].object_id, page_two.entries[0].object_id];
        assert!(found.contains(&first_ref.object_id));
        assert!(found.contains(&second_ref.object_id));
        assert!(matches!(
            store
                .inventory_page(workspace(1), None, MAX_INVENTORY_PAGE_SIZE + 1)
                .await,
            Err(ObjectStoreError::LimitExceeded(_))
        ));
    }

    #[tokio::test]
    async fn inventory_returns_empty_and_single_page_results_without_a_cursor() {
        let backend = Arc::new(MemoryBackend::default());
        let store = make_store(Arc::clone(&backend));

        let empty_page = store
            .inventory_page(workspace(1), None, 10)
            .await
            .expect("empty workspace has a valid final page");
        assert!(empty_page.entries.is_empty());
        assert!(empty_page.next_cursor.is_none());

        store
            .put(
                workspace(1),
                &encoded(ObjectType::ArtifactBlob, b"one object"),
            )
            .await
            .expect("test object is stored");
        let single_page = store
            .inventory_page(workspace(1), None, 10)
            .await
            .expect("single-page inventory has a valid final page");
        assert_eq!(single_page.entries.len(), 1);
        assert!(single_page.next_cursor.is_none());
    }

    #[tokio::test]
    async fn transfer_budget_limits_workspace_and_service_concurrency() {
        let budget = TransferBudget::new(1, 2).unwrap();
        let first_workspace = budget.acquire(workspace(1)).await.unwrap();
        let second_workspace = budget.acquire(workspace(2)).await.unwrap();
        let blocked_budget = budget.clone();
        let blocked = tokio::spawn(async move { blocked_budget.acquire(workspace(1)).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!blocked.is_finished());

        drop(first_workspace);
        let third = tokio::time::timeout(Duration::from_secs(1), blocked)
            .await
            .expect("workspace permit is released")
            .expect("admission task does not panic")
            .expect("admission remains open");
        let global_budget = budget.clone();
        let blocked_globally =
            tokio::spawn(async move { global_budget.acquire(workspace(3)).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!blocked_globally.is_finished());

        drop(second_workspace);
        let fourth = tokio::time::timeout(Duration::from_secs(1), blocked_globally)
            .await
            .expect("service permit is released")
            .expect("admission task does not panic")
            .expect("admission remains open");
        drop((third, fourth));
    }

    #[test]
    fn deployment_prefix_rejects_path_traversal_and_empty_scopes() {
        assert!(normalize_prefix(" ").is_err());
        assert!(normalize_prefix("/deployment/../other/").is_err());
        assert!(normalize_prefix("deployment//nested").is_err());
        assert_eq!(
            normalize_prefix("/deployment/nested/").unwrap(),
            "deployment/nested"
        );
    }

    #[cfg(feature = "s3-emulator")]
    #[test]
    fn emulator_endpoint_is_restricted_to_loopback_http() {
        assert!(validate_local_emulator_endpoint("http://127.0.0.1:9000").is_ok());
        for endpoint in [
            "http://localhost:9000",
            "http://127.0.0.2:9000",
            "https://127.0.0.1:9000",
            "http://example.com:9000",
            "http://127.0.0.1:0",
            "http://127.0.0.1:9000/path",
            "http://127.0.0.1:9000/",
        ] {
            assert!(
                validate_local_emulator_endpoint(endpoint).is_err(),
                "endpoint should be rejected: {endpoint}"
            );
        }
    }

    #[test]
    fn inventory_rejects_noncanonical_type_components() {
        for type_component in ["1", "000A", "000g"] {
            let entry = BackendEntry {
                key: format!(
                    "deployment/workspaces/{}/objects/{type_component}/{}",
                    workspace(1),
                    "00".repeat(32)
                ),
                encoded_len: ENVELOPE_HEADER_BYTES as u64,
            };
            assert!(matches!(
                parse_inventory_entry(
                    &format!("deployment/workspaces/{}/objects", workspace(1)),
                    entry
                ),
                Err(ObjectStoreError::InvalidInventoryEntry(_))
            ));
        }
    }
}
