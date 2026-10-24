//! Verified disk cache scoped to a workspace and immutable-object namespace.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use thiserror::Error;
use versionfs_core::format::{FormatError, MAX_ENCODED_OBJECT_BYTES, ObjectRef};
use versionfs_core::identity::{ObjectId, ObjectStoreScopeId, WorkspaceId};
use versionfs_core::persistent_map::MapObjectStore;

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);
const CACHE_LOCK_FILE: &str = ".lock";

/// Errors returned by the verified local object cache.
#[derive(Debug, Error)]
pub enum CacheError {
    /// An encoded object did not match its requested immutable identity.
    #[error(transparent)]
    Format(#[from] FormatError),
    /// The remote object fetch failed.
    #[error("object fetch failed: {0}")]
    Fetch(String),
    /// A local cache operation failed.
    #[error("cache I/O at {path}: {source}")]
    Io {
        /// Path involved in the failed operation.
        path: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// The cache's local coordination lock was poisoned.
    #[error("object cache lock is poisoned")]
    Poisoned,
    /// The cache and backing store use different immutable-object namespaces.
    #[error("object cache scope {cache_scope} does not match backing store scope {backing_scope}")]
    ScopeMismatch {
        /// Namespace selected when the cache was opened.
        cache_scope: ObjectStoreScopeId,
        /// Namespace declared by the backing store.
        backing_scope: ObjectStoreScopeId,
    },
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct CacheKey(ObjectId);

#[derive(Clone, Copy, Debug)]
struct CacheEntry {
    encoded_len: u64,
    last_access: u64,
}

#[derive(Clone)]
struct CacheVictim {
    key: CacheKey,
    entry: CacheEntry,
    path: PathBuf,
}

type SharedResult = Result<Option<Arc<[u8]>>, SharedCacheError>;

#[derive(Default)]
struct CacheState {
    entries: HashMap<CacheKey, CacheEntry>,
    encoded_bytes: u64,
    pending_delete_bytes: u64,
    reserved_bytes: u64,
    access_clock: u64,
    in_flight: HashMap<CacheKey, Arc<InFlight>>,
    evicting_keys: HashSet<CacheKey>,
}

#[derive(Default)]
struct InFlight {
    result: Mutex<Option<SharedResult>>,
    ready: Condvar,
}

#[derive(Clone)]
enum SharedCacheError {
    Format(FormatError),
    Fetch(Arc<str>),
    Io {
        path: PathBuf,
        kind: std::io::ErrorKind,
        message: Arc<str>,
    },
    Poisoned,
    ScopeMismatch {
        cache_scope: ObjectStoreScopeId,
        backing_scope: ObjectStoreScopeId,
    },
}

impl SharedCacheError {
    fn from_cache_error(error: CacheError) -> Self {
        match error {
            CacheError::Format(error) => Self::Format(error),
            CacheError::Fetch(message) => Self::Fetch(Arc::from(message)),
            CacheError::Io { path, source } => Self::Io {
                path,
                kind: source.kind(),
                message: Arc::from(source.to_string()),
            },
            CacheError::Poisoned => Self::Poisoned,
            CacheError::ScopeMismatch {
                cache_scope,
                backing_scope,
            } => Self::ScopeMismatch {
                cache_scope,
                backing_scope,
            },
        }
    }

    fn into_cache_error(self) -> CacheError {
        match self {
            Self::Format(error) => CacheError::Format(error),
            Self::Fetch(message) => CacheError::Fetch(message.to_string()),
            Self::Io {
                path,
                kind,
                message,
            } => CacheError::Io {
                path,
                source: std::io::Error::new(kind, message.to_string()),
            },
            Self::Poisoned => CacheError::Poisoned,
            Self::ScopeMismatch {
                cache_scope,
                backing_scope,
            } => CacheError::ScopeMismatch {
                cache_scope,
                backing_scope,
            },
        }
    }
}

impl InFlight {
    fn wait(&self) -> SharedResult {
        let mut result = self
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while result.is_none() {
            result = self
                .ready
                .wait(result)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        result.as_ref().expect("result was checked above").clone()
    }

    fn complete(&self, result: SharedResult) {
        let mut slot = self
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *slot = Some(result);
        self.ready.notify_all();
    }
}

struct FillLeader<'a> {
    cache: &'a WorkspaceObjectCache,
    key: CacheKey,
    flight: Arc<InFlight>,
    completed: bool,
}

impl FillLeader<'_> {
    fn complete(mut self, result: SharedResult) {
        self.cache.release_flight(self.key, &self.flight);
        self.flight.complete(result);
        self.completed = true;
    }
}

impl Drop for FillLeader<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.cache.release_flight(self.key, &self.flight);
            self.flight.complete(Err(SharedCacheError::Fetch(Arc::from(
                "object cache fill was interrupted",
            ))));
        }
    }
}

struct CapacityReservation<'a> {
    cache: &'a WorkspaceObjectCache,
    bytes: u64,
    victims: Vec<Option<CacheVictim>>,
    owned_path: Option<PathBuf>,
    active: bool,
}

impl CapacityReservation<'_> {
    fn delete_victim(&mut self, index: usize) -> Result<(), CacheError> {
        let Some(victim) = self.victims[index].as_ref() else {
            return Ok(());
        };
        let victim = victim.clone();
        match fs::remove_file(&victim.path) {
            Ok(()) => self.finish_victim_deletion(index, &victim),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.finish_victim_deletion(index, &victim);
            }
            Err(source) => {
                return Err(CacheError::Io {
                    path: victim.path.clone(),
                    source,
                });
            }
        }
        Ok(())
    }

    fn finish_victim_deletion(&mut self, index: usize, victim: &CacheVictim) {
        let mut state = self
            .cache
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.pending_delete_bytes = state
            .pending_delete_bytes
            .saturating_sub(victim.entry.encoded_len);
        state.evicting_keys.remove(&victim.key);
        self.victims[index] = None;
    }

    fn commit(mut self, object_id: ObjectId, encoded_len: u64) -> Result<(), CacheError> {
        let mut state = self.cache.lock_state()?;
        state.reserved_bytes = state.reserved_bytes.saturating_sub(self.bytes);
        state.encoded_bytes = state.encoded_bytes.saturating_add(encoded_len);
        let access = next_access(&mut state);
        state.entries.insert(
            CacheKey(object_id),
            CacheEntry {
                encoded_len,
                last_access: access,
            },
        );
        self.active = false;
        Ok(())
    }
}

impl Drop for CapacityReservation<'_> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let released = self
            .owned_path
            .as_ref()
            .is_none_or(|path| match fs::remove_file(path) {
                Ok(()) => true,
                Err(error) => error.kind() == std::io::ErrorKind::NotFound,
            });
        let mut state = self
            .cache
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for victim in self.victims.iter().flatten() {
            state.pending_delete_bytes = state
                .pending_delete_bytes
                .saturating_sub(victim.entry.encoded_len);
            state.evicting_keys.remove(&victim.key);
            if !state.entries.contains_key(&victim.key) {
                state.encoded_bytes = state.encoded_bytes.saturating_add(victim.entry.encoded_len);
                state.entries.insert(victim.key, victim.entry);
            }
        }
        // Keep undeleted bytes charged until the exclusively owned cache is
        // reopened and rebuilds its byte budget.
        if released {
            state.reserved_bytes = state.reserved_bytes.saturating_sub(self.bytes);
        }
    }
}

/// Bounded clean-object cache for one workspace and immutable-object namespace.
///
/// Every fill is verified before atomic installation. Per-object in-flight
/// coordination deduplicates concurrent misses while unrelated object reads
/// proceed independently. Cache I/O never holds the shared index lock. The
/// cache stores immutable objects only; dirty block and control state use
/// separate caller-owned storage. Incomplete fills whose files cannot be removed
/// retain their capacity reservation until the cache is reopened.
pub struct WorkspaceObjectCache {
    workspace_id: WorkspaceId,
    store_scope: ObjectStoreScopeId,
    directory: PathBuf,
    capacity_bytes: u64,
    state: Mutex<CacheState>,
    _directory_lock: File,
}

impl WorkspaceObjectCache {
    /// Opens a cache for one workspace and immutable-object namespace.
    ///
    /// The directory is exclusively locked until this cache is dropped. Another
    /// handle or process opening the same directory receives an I/O error.
    /// Reopening under another namespace removes the previous namespace's files.
    pub fn open(
        root: impl AsRef<Path>,
        workspace_id: WorkspaceId,
        store_scope: ObjectStoreScopeId,
        capacity_bytes: u64,
    ) -> Result<Self, CacheError> {
        let directory = root.as_ref().join(workspace_id.to_string());
        fs::create_dir_all(&directory).map_err(|source| CacheError::Io {
            path: directory.clone(),
            source,
        })?;
        let metadata = fs::symlink_metadata(&directory).map_err(|source| CacheError::Io {
            path: directory.clone(),
            source,
        })?;
        if !metadata.file_type().is_dir() {
            return Err(CacheError::Io {
                path: directory,
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "workspace cache path must be a directory",
                ),
            });
        }

        let lock_path = directory.join(CACHE_LOCK_FILE);
        let directory_lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|source| CacheError::Io {
                path: lock_path.clone(),
                source,
            })?;
        directory_lock.try_lock().map_err(|source| CacheError::Io {
            path: lock_path,
            source: source.into(),
        })?;

        let mut cache = Self {
            workspace_id,
            store_scope,
            directory,
            capacity_bytes,
            state: Mutex::new(CacheState::default()),
            _directory_lock: directory_lock,
        };
        cache.rebuild_index()?;
        Ok(cache)
    }

    /// Returns the workspace that scopes this cache instance.
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    /// Returns the immutable-object namespace that scopes this cache instance.
    pub const fn store_scope(&self) -> ObjectStoreScopeId {
        self.store_scope
    }

    /// Returns a verified object, filling it from `fetch` on a cache miss.
    ///
    /// The fetcher must read from this cache's workspace and [`Self::store_scope`].
    pub fn get_or_fetch(
        &self,
        expected: ObjectRef,
        fetch: impl FnOnce(WorkspaceId, ObjectRef) -> Result<Vec<u8>, String>,
    ) -> Result<Vec<u8>, CacheError> {
        self.get_or_fetch_id(expected.object_id, |_| {
            fetch(self.workspace_id, expected).map(Some)
        })
        .and_then(|bytes| {
            let bytes = bytes.ok_or_else(|| {
                CacheError::Fetch("immutable object fetch returned no object".to_owned())
            })?;
            expected.verify(&bytes)?;
            Ok(bytes)
        })
    }

    fn get_or_fetch_id(
        &self,
        object_id: ObjectId,
        fetch: impl FnOnce(ObjectId) -> Result<Option<Vec<u8>>, String>,
    ) -> Result<Option<Vec<u8>>, CacheError> {
        let key = CacheKey(object_id);
        let (flight, is_leader) = match self.acquire_flight(key) {
            Ok(flight) => flight,
            Err(CacheError::Poisoned) => return fetch_verified(object_id, fetch),
            Err(error) => return Err(error),
        };
        if !is_leader {
            return flight
                .wait()
                .map(|result| result.map(|bytes| bytes.to_vec()))
                .map_err(SharedCacheError::into_cache_error);
        }

        let leader = FillLeader {
            cache: self,
            key,
            flight,
            completed: false,
        };
        let result = self.read_cached_or_fetch(key, fetch);
        let shared_result = result
            .map(|result| result.map(Arc::<[u8]>::from))
            .map_err(SharedCacheError::from_cache_error);
        leader.complete(shared_result.clone());
        shared_result
            .map(|result| result.map(|bytes| bytes.to_vec()))
            .map_err(SharedCacheError::into_cache_error)
    }

    fn acquire_flight(&self, key: CacheKey) -> Result<(Arc<InFlight>, bool), CacheError> {
        let mut state = self.lock_state()?;
        if let Some(flight) = state.in_flight.get(&key) {
            return Ok((Arc::clone(flight), false));
        }
        let flight = Arc::new(InFlight::default());
        state.in_flight.insert(key, Arc::clone(&flight));
        Ok((flight, true))
    }

    fn release_flight(&self, key: CacheKey, flight: &Arc<InFlight>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .in_flight
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, flight))
        {
            state.in_flight.remove(&key);
        }
    }

    fn read_cached_or_fetch(
        &self,
        key: CacheKey,
        fetch: impl FnOnce(ObjectId) -> Result<Option<Vec<u8>>, String>,
    ) -> Result<Option<Vec<u8>>, CacheError> {
        if let Some(bytes) = self.read_cached(key) {
            return Ok(Some(bytes));
        }

        let Some(bytes) = fetch_verified(key.0, fetch)? else {
            return Ok(None);
        };
        let reference = ObjectRef::from_encoded(&bytes)?;
        if reference.encoded_len <= self.capacity_bytes {
            // A local cache fill is optional; the verified backing bytes remain usable.
            let _ = self.install(reference.object_id, &bytes);
        }
        Ok(Some(bytes))
    }

    fn read_cached(&self, key: CacheKey) -> Option<Vec<u8>> {
        let is_indexed = self
            .lock_state()
            .is_ok_and(|state| state.entries.contains_key(&key));
        if !is_indexed {
            return None;
        }

        let path = self.object_path(key.0);
        let bytes = read_file(&path).ok().filter(|bytes| {
            ObjectRef::from_encoded(bytes).is_ok_and(|reference| reference.object_id == key.0)
        });
        if let Some(bytes) = bytes {
            if let Ok(mut state) = self.lock_state() {
                touch(&mut state, key);
            }
            return Some(bytes);
        }

        match fs::remove_file(path) {
            Ok(()) => self.remove_entry_index(key),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.remove_entry_index(key);
            }
            // Keep undeleted files charged even when a verified backend read
            // can still succeed. A later cleanup or eviction may remove them.
            Err(_) => {}
        }
        None
    }

    fn install(&self, object_id: ObjectId, bytes: &[u8]) -> Result<(), CacheError> {
        let encoded_len = bytes.len() as u64;
        let Some(reservation) = self.reserve_install(object_id, encoded_len)? else {
            return Ok(());
        };
        let mut reservation = reservation;
        for index in 0..reservation.victims.len() {
            reservation.delete_victim(index)?;
        }

        let path = self.object_path(object_id);
        let temp_id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let temp_path = self
            .directory
            .join(format!(".tmp-{}-{temp_id}", std::process::id()));
        let mut temp = create_new_file(&temp_path)?;
        reservation.owned_path = Some(temp_path.clone());
        temp.write_all(bytes).map_err(|source| CacheError::Io {
            path: temp_path.clone(),
            source,
        })?;
        temp.sync_all().map_err(|source| CacheError::Io {
            path: temp_path.clone(),
            source,
        })?;
        drop(temp);
        fs::rename(&temp_path, &path).map_err(|source| CacheError::Io {
            path: path.clone(),
            source,
        })?;
        reservation.owned_path = Some(path);
        reservation.commit(object_id, encoded_len)
    }

    fn reserve_install(
        &self,
        protected_id: ObjectId,
        incoming_bytes: u64,
    ) -> Result<Option<CapacityReservation<'_>>, CacheError> {
        let mut state = self.lock_state()?;
        let protected_key = CacheKey(protected_id);
        // A failed cleanup can leave this object indexed. Avoid replacing its
        // file and charging it again if local access recovered during the fetch.
        if state.entries.contains_key(&protected_key)
            || state.evicting_keys.contains(&protected_key)
        {
            return Ok(None);
        }
        if incoming_bytes.saturating_add(state.reserved_bytes) > self.capacity_bytes {
            return Ok(None);
        }
        let required_bytes = state
            .encoded_bytes
            .saturating_add(state.pending_delete_bytes)
            .saturating_add(state.reserved_bytes)
            .saturating_add(incoming_bytes)
            .saturating_sub(self.capacity_bytes);
        let reclaimable_bytes = state
            .entries
            .iter()
            .filter(|(key, _)| key.0 != protected_id)
            .map(|(_, entry)| entry.encoded_len)
            .fold(0_u64, u64::saturating_add);
        if reclaimable_bytes < required_bytes {
            return Ok(None);
        }

        let mut victims: Vec<Option<CacheVictim>> = Vec::new();
        let mut planned_reclaim_bytes = 0_u64;
        while state
            .encoded_bytes
            .saturating_add(state.pending_delete_bytes)
            .saturating_add(state.reserved_bytes)
            .saturating_add(incoming_bytes)
            .saturating_sub(planned_reclaim_bytes)
            > self.capacity_bytes
        {
            let Some(key) = state
                .entries
                .iter()
                .filter(|(key, _)| key.0 != protected_id)
                .min_by_key(|(_, entry)| entry.last_access)
                .map(|(key, _)| *key)
            else {
                for victim in victims.into_iter().flatten() {
                    state.pending_delete_bytes = state
                        .pending_delete_bytes
                        .saturating_sub(victim.entry.encoded_len);
                    state.evicting_keys.remove(&victim.key);
                    state.encoded_bytes =
                        state.encoded_bytes.saturating_add(victim.entry.encoded_len);
                    state.entries.insert(victim.key, victim.entry);
                }
                return Ok(None);
            };
            if let Some(entry) = state.entries.remove(&key) {
                state.encoded_bytes = state.encoded_bytes.saturating_sub(entry.encoded_len);
                state.pending_delete_bytes =
                    state.pending_delete_bytes.saturating_add(entry.encoded_len);
                state.evicting_keys.insert(key);
                planned_reclaim_bytes = planned_reclaim_bytes.saturating_add(entry.encoded_len);
                victims.push(Some(CacheVictim {
                    key,
                    entry,
                    path: self.object_path(key.0),
                }));
            }
        }
        state.reserved_bytes = state.reserved_bytes.saturating_add(incoming_bytes);
        Ok(Some(CapacityReservation {
            cache: self,
            bytes: incoming_bytes,
            victims,
            owned_path: None,
            active: true,
        }))
    }

    fn remove_entry_index(&self, key: CacheKey) {
        if let Ok(mut state) = self.lock_state()
            && let Some(entry) = state.entries.remove(&key)
        {
            state.encoded_bytes = state.encoded_bytes.saturating_sub(entry.encoded_len);
        }
    }

    fn rebuild_index(&mut self) -> Result<(), CacheError> {
        let mut state = CacheState::default();
        for item in fs::read_dir(&self.directory).map_err(|source| CacheError::Io {
            path: self.directory.clone(),
            source,
        })? {
            let item = item.map_err(|source| CacheError::Io {
                path: self.directory.clone(),
                source,
            })?;
            let path = item.path();
            if item.file_name() == CACHE_LOCK_FILE {
                continue;
            }
            let kind = item.file_type().map_err(|source| CacheError::Io {
                path: path.clone(),
                source,
            })?;
            if !kind.is_file() {
                if kind.is_symlink() {
                    fs::remove_file(&path).map_err(|source| CacheError::Io { path, source })?;
                }
                continue;
            }
            let Some(object_id) = object_id_from_file_name(&path, self.store_scope) else {
                fs::remove_file(&path).map_err(|source| CacheError::Io { path, source })?;
                continue;
            };
            let bytes = match read_file(&path) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == std::io::ErrorKind::InvalidData => {
                    fs::remove_file(&path).map_err(|source| CacheError::Io { path, source })?;
                    continue;
                }
                Err(source) => {
                    return Err(CacheError::Io {
                        path: path.clone(),
                        source,
                    });
                }
            };
            let Ok(reference) = ObjectRef::from_encoded(&bytes) else {
                fs::remove_file(&path).map_err(|source| CacheError::Io { path, source })?;
                continue;
            };
            if reference.object_id != object_id || reference.encoded_len > self.capacity_bytes {
                fs::remove_file(&path).map_err(|source| CacheError::Io { path, source })?;
                continue;
            }
            state.access_clock = state.access_clock.saturating_add(1);
            let access = state.access_clock;
            state.encoded_bytes = state.encoded_bytes.saturating_add(reference.encoded_len);
            state.entries.insert(
                CacheKey(object_id),
                CacheEntry {
                    encoded_len: reference.encoded_len,
                    last_access: access,
                },
            );
        }
        while state.encoded_bytes > self.capacity_bytes {
            let Some(key) = state
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_access)
                .map(|(key, _)| *key)
            else {
                break;
            };
            let path = self.object_path(key.0);
            if let Some(entry) = state.entries.remove(&key) {
                state.encoded_bytes = state.encoded_bytes.saturating_sub(entry.encoded_len);
            }
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(source) => return Err(CacheError::Io { path, source }),
            }
        }
        *self.state.get_mut().map_err(|_| CacheError::Poisoned)? = state;
        Ok(())
    }

    fn object_path(&self, object_id: ObjectId) -> PathBuf {
        self.directory
            .join(format!("{}-{object_id}.obj", self.store_scope))
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, CacheState>, CacheError> {
        self.state.lock().map_err(|_| CacheError::Poisoned)
    }
}

fn fetch_verified(
    object_id: ObjectId,
    fetch: impl FnOnce(ObjectId) -> Result<Option<Vec<u8>>, String>,
) -> Result<Option<Vec<u8>>, CacheError> {
    let Some(bytes) = fetch(object_id).map_err(CacheError::Fetch)? else {
        return Ok(None);
    };
    let reference = ObjectRef::from_encoded(&bytes)?;
    if reference.object_id != object_id {
        return Err(FormatError::ObjectReferenceMismatch.into());
    }
    Ok(Some(bytes))
}

/// A cache-backed synchronous map store used by the persistent-map reader.
///
/// Writes go to the backing store before entering the clean cache; dirty volume
/// sectors are managed separately by [`crate::volume::DirtyVolume`].
pub struct CachedMapStore<S> {
    backing: S,
    cache: WorkspaceObjectCache,
}

impl<S: MapObjectStore> CachedMapStore<S> {
    /// Creates a map store combining a synchronous backend and workspace cache.
    ///
    /// # Errors
    ///
    /// Returns an error when the cache and backend declare different namespaces.
    pub fn new(backing: S, cache: WorkspaceObjectCache) -> Result<Self, CacheError> {
        let cache_scope = cache.store_scope();
        let backing_scope = backing.proof_scope();
        if cache_scope != backing_scope {
            return Err(CacheError::ScopeMismatch {
                cache_scope,
                backing_scope,
            });
        }
        Ok(Self { backing, cache })
    }

    /// Returns a reference to the immutable-object backend.
    pub const fn backing(&self) -> &S {
        &self.backing
    }
}

impl<S: MapObjectStore> MapObjectStore for CachedMapStore<S> {
    fn proof_scope(&self) -> versionfs_core::identity::ObjectStoreScopeId {
        self.backing.proof_scope()
    }

    fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
        self.cache
            .get_or_fetch_id(id, |object_id| self.backing.get(object_id))
            .map_err(|error| error.to_string())
    }

    fn get_ref(&self, reference: ObjectRef) -> Result<Option<Vec<u8>>, String> {
        let bytes = self
            .cache
            .get_or_fetch_id(reference.object_id, |_| self.backing.get_ref(reference))
            .map_err(|error| error.to_string())?;
        if let Some(bytes) = &bytes {
            reference.verify(bytes).map_err(|error| error.to_string())?;
        }
        Ok(bytes)
    }

    fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
        let reference = ObjectRef::from_encoded(&bytes).map_err(|error| error.to_string())?;
        if reference.object_id != id {
            return Err(FormatError::ObjectReferenceMismatch.to_string());
        }
        self.backing.put(id, bytes.clone())?;
        match self.cache.get_or_fetch_id(id, |_| Ok(Some(bytes))) {
            Ok(_) | Err(CacheError::Io { .. } | CacheError::Poisoned) => Ok(()),
            Err(error) => Err(error.to_string()),
        }
    }
}

impl<S: versionfs_core::persistent_map::DurableObjectStore>
    versionfs_core::persistent_map::DurableObjectStore for CachedMapStore<S>
{
    fn put_durable(&mut self, reference: ObjectRef, encoded: &[u8]) -> Result<(), String> {
        reference
            .verify(encoded)
            .map_err(|error| error.to_string())?;
        self.backing.put_durable(reference, encoded)
    }
}

fn touch(state: &mut CacheState, key: CacheKey) {
    let access = next_access(state);
    if let Some(entry) = state.entries.get_mut(&key) {
        entry.last_access = access;
    }
}

fn next_access(state: &mut CacheState) -> u64 {
    state.access_clock = state.access_clock.saturating_add(1);
    state.access_clock
}

fn create_new_file(path: &Path) -> Result<File, CacheError> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| CacheError::Io {
            path: path.to_owned(),
            source,
        })
}

fn read_file(path: &Path) -> Result<Vec<u8>, std::io::Error> {
    let file = File::open(path)?;
    let capacity = file.metadata()?.len().min(MAX_ENCODED_OBJECT_BYTES as u64) as usize;
    let mut bytes = Vec::with_capacity(capacity);
    let mut limited = file.take(MAX_ENCODED_OBJECT_BYTES as u64 + 1);
    limited.read_to_end(&mut bytes)?;
    if bytes.len() > MAX_ENCODED_OBJECT_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "cached object exceeds the maximum encoded object size",
        ));
    }
    Ok(bytes)
}

fn object_id_from_file_name(path: &Path, store_scope: ObjectStoreScopeId) -> Option<ObjectId> {
    let name = path.file_name()?.to_str()?;
    let (scope, digest) = name.strip_suffix(".obj")?.split_once('-')?;
    if scope != store_scope.to_string() {
        return None;
    }
    if digest.len() != 64 {
        return None;
    }
    let mut bytes = [0_u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let start = index.checked_mul(2)?;
        *byte = u8::from_str_radix(digest.get(start..start + 2)?, 16).ok()?;
    }
    Some(ObjectId::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::{
        CacheError, CacheKey, CachedMapStore, FillLeader, SharedCacheError, WorkspaceObjectCache,
    };
    use std::collections::HashMap;
    use std::fs::{self, OpenOptions};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{Duration, Instant};
    use versionfs_core::format::{MAX_ENCODED_OBJECT_BYTES, ObjectRef, ObjectType, encode_object};
    use versionfs_core::identity::{ObjectId, ObjectStoreScopeId, WorkspaceId};
    use versionfs_core::persistent_map::MapObjectStore;

    static NEXT_TEST_DIR: AtomicUsize = AtomicUsize::new(1);
    const STORE_SCOPE: ObjectStoreScopeId = ObjectStoreScopeId::from_bytes([0x38; 16]);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let id = NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "versionfs-object-cache-{}-{id}",
                std::process::id()
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn encoded_object(fill: u8) -> (ObjectRef, Vec<u8>) {
        let bytes = encode_object(ObjectType::ArtifactBlob, &[fill; 256]).unwrap();
        let reference = ObjectRef::from_encoded(&bytes).unwrap();
        (reference, bytes)
    }

    fn fetch(cache: &WorkspaceObjectCache, reference: ObjectRef, bytes: &[u8]) -> Vec<u8> {
        cache
            .get_or_fetch(reference, |workspace, requested| {
                assert_eq!(workspace, cache.workspace_id());
                assert_eq!(requested, reference);
                Ok(bytes.to_vec())
            })
            .unwrap()
    }

    #[test]
    fn workspace_cache_directory_has_one_owner_until_drop() {
        let directory = TestDirectory::new();
        let workspace = WorkspaceId::from_bytes([0x3F; 16]);
        let cache = WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, 1024).unwrap();

        assert!(matches!(
            WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, 1024),
            Err(CacheError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::WouldBlock
        ));

        drop(cache);
        let _reopened =
            WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, 1024).unwrap();
        assert!(matches!(
            WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, 1024),
            Err(CacheError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::WouldBlock
        ));
    }

    #[test]
    fn workspace_cache_directory_lock_rejects_another_process() {
        const CHILD_DIRECTORY: &str = "VERSIONFS_CACHE_LOCK_TEST_DIRECTORY";
        let workspace = WorkspaceId::from_bytes([0x40; 16]);
        if let Some(directory) = std::env::var_os(CHILD_DIRECTORY) {
            assert!(matches!(
                WorkspaceObjectCache::open(directory, workspace, STORE_SCOPE, 1024),
                Err(CacheError::Io { source, .. })
                    if source.kind() == std::io::ErrorKind::WouldBlock
            ));
            return;
        }

        let directory = TestDirectory::new();
        let _cache =
            WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, 1024).unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("object_cache::tests::workspace_cache_directory_lock_rejects_another_process")
            .env(CHILD_DIRECTORY, &directory.0)
            .status()
            .unwrap();
        assert!(status.success());
    }

    #[test]
    fn verified_cache_fill_is_reused_after_reopen() {
        let directory = TestDirectory::new();
        let workspace = WorkspaceId::from_bytes([0x31; 16]);
        let (reference, bytes) = encoded_object(0xA1);
        let cache = WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, 1024).unwrap();
        assert_eq!(cache.store_scope(), STORE_SCOPE);
        let fetches = AtomicUsize::new(0);

        let first = cache
            .get_or_fetch(reference, |scope, requested| {
                assert_eq!(scope, workspace);
                assert_eq!(requested, reference);
                fetches.fetch_add(1, Ordering::SeqCst);
                Ok(bytes.clone())
            })
            .unwrap();
        assert_eq!(first, bytes);
        assert_eq!(fetches.load(Ordering::SeqCst), 1);

        drop(cache);
        let reopened =
            WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, 1024).unwrap();
        assert_eq!(fetch(&reopened, reference, &[]), bytes);
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
        let files: Vec<_> = fs::read_dir(directory.0.join(workspace.to_string()))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            files.len(),
            2,
            "successful fills leave only the lock and object file"
        );
    }

    #[test]
    fn reopening_with_another_scope_removes_other_scope_and_legacy_objects() {
        let directory = TestDirectory::new();
        let workspace = WorkspaceId::from_bytes([0x42; 16]);
        let (reference, bytes) = encoded_object(0xB4);
        let cache = WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, 1024).unwrap();
        fetch(&cache, reference, &bytes);
        let scoped_path = cache.object_path(reference.object_id);
        let legacy_path = cache.directory.join(format!("{}.obj", reference.object_id));
        fs::copy(&scoped_path, &legacy_path).unwrap();
        drop(cache);

        let other_scope = ObjectStoreScopeId::from_bytes([0x43; 16]);
        let reopened =
            WorkspaceObjectCache::open(&directory.0, workspace, other_scope, 1024).unwrap();
        assert!(!scoped_path.exists());
        assert!(!legacy_path.exists());
        assert!(reopened.lock_state().unwrap().entries.is_empty());
        assert_eq!(fetch(&reopened, reference, &bytes), bytes);
    }

    #[test]
    fn corrupt_and_oversized_cache_entries_are_removed_before_refill() {
        let directory = TestDirectory::new();
        let workspace = WorkspaceId::from_bytes([0x32; 16]);
        let (reference, bytes) = encoded_object(0xB2);
        let workspace_dir = directory.0.join(workspace.to_string());
        let cache = WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, 1024).unwrap();
        fetch(&cache, reference, &bytes);
        drop(cache);

        let object_path = workspace_dir.join(format!("{STORE_SCOPE}-{}.obj", reference.object_id));
        fs::write(&object_path, [0xFF, 0x00]).unwrap();
        let reopened =
            WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, 1024).unwrap();
        assert_eq!(fetch(&reopened, reference, &bytes), bytes);

        drop(reopened);
        let file = OpenOptions::new().write(true).open(&object_path).unwrap();
        file.set_len(MAX_ENCODED_OBJECT_BYTES as u64 + 1).unwrap();
        let reopened =
            WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, 1024).unwrap();
        assert_eq!(fetch(&reopened, reference, &bytes), bytes);
        assert_eq!(
            fs::metadata(&object_path).unwrap().len(),
            bytes.len() as u64
        );
    }

    #[test]
    fn cache_capacity_evicts_the_least_recently_used_clean_object() {
        let directory = TestDirectory::new();
        let workspace = WorkspaceId::from_bytes([0x33; 16]);
        let (a_ref, a) = encoded_object(1);
        let (b_ref, b) = encoded_object(2);
        let (c_ref, c) = encoded_object(3);
        let capacity = (a.len() * 2) as u64;
        let cache =
            WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, capacity).unwrap();
        let fetches = AtomicUsize::new(0);
        let get = |reference, bytes: &[u8]| {
            cache
                .get_or_fetch(reference, |_, _| {
                    fetches.fetch_add(1, Ordering::SeqCst);
                    Ok(bytes.to_vec())
                })
                .unwrap()
        };

        assert_eq!(get(a_ref, &a), a);
        assert_eq!(get(b_ref, &b), b);
        assert_eq!(get(a_ref, &[]), a); // Refresh A's LRU position.
        assert_eq!(get(c_ref, &c), c); // Evicts B.
        assert_eq!(get(a_ref, &[]), a);
        assert_eq!(get(b_ref, &b), b);
        assert_eq!(fetches.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn pending_eviction_cannot_be_reused_if_deletion_fails() {
        let directory = TestDirectory::new();
        let workspace = WorkspaceId::from_bytes([0x3E; 16]);
        let large_bytes = encode_object(ObjectType::ArtifactBlob, &[0xA1; 1024]).unwrap();
        let large_ref = ObjectRef::from_encoded(&large_bytes).unwrap();
        let (small_ref, small_bytes) = encoded_object(0xA2);
        let (first_new_ref, _) = encoded_object(0xA3);
        let (second_new_ref, second_new_bytes) = encoded_object(0xA4);
        let capacity = (large_bytes.len() + small_bytes.len()) as u64;
        let cache =
            WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, capacity).unwrap();
        fetch(&cache, large_ref, &large_bytes);
        fetch(&cache, small_ref, &small_bytes);

        // The first install plans to evict the larger object, freeing more than
        // its own reservation needs. A concurrent install must not consume that
        // provisional space until the deletion has actually completed.
        let mut first_reservation = cache
            .reserve_install(first_new_ref.object_id, small_bytes.len() as u64)
            .unwrap()
            .unwrap();
        assert_eq!(first_reservation.victims.len(), 1);
        assert_eq!(
            first_reservation.victims[0].as_ref().unwrap().key,
            super::CacheKey(large_ref.object_id)
        );
        assert!(
            cache
                .reserve_install(second_new_ref.object_id, second_new_bytes.len() as u64)
                .unwrap()
                .is_none()
        );

        let victim_path = cache.object_path(large_ref.object_id);
        let saved_victim_path = directory.0.join("saved-victim.obj");
        fs::rename(&victim_path, &saved_victim_path).unwrap();
        fs::create_dir(&victim_path).unwrap();
        assert!(first_reservation.delete_victim(0).is_err());
        drop(first_reservation);
        fs::remove_dir(&victim_path).unwrap();
        fs::rename(saved_victim_path, &victim_path).unwrap();

        {
            let state = cache.lock_state().unwrap();
            assert_eq!(state.encoded_bytes, capacity);
            assert_eq!(state.pending_delete_bytes, 0);
            assert_eq!(state.reserved_bytes, 0);
            assert!(state.evicting_keys.is_empty());
        }

        cache
            .install(second_new_ref.object_id, &second_new_bytes)
            .unwrap();
        let state = cache.lock_state().unwrap();
        assert!(state.encoded_bytes <= capacity);
        assert_eq!(state.pending_delete_bytes, 0);
        assert_eq!(state.reserved_bytes, 0);
    }

    #[cfg(unix)]
    #[test]
    fn transient_cache_io_failure_keeps_undeleted_files_within_capacity() {
        use std::os::unix::fs::PermissionsExt;

        struct RestorePermissions(PathBuf, fs::Permissions);

        impl Drop for RestorePermissions {
            fn drop(&mut self) {
                let _ = fs::set_permissions(&self.0, self.1.clone());
            }
        }

        let directory = TestDirectory::new();
        let workspace = WorkspaceId::from_bytes([0x4B; 16]);
        let (first_ref, first_bytes) = encoded_object(0xB1);
        let (second_ref, second_bytes) = encoded_object(0xB2);
        let capacity = first_bytes.len() as u64;
        let cache =
            WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, capacity).unwrap();
        fetch(&cache, first_ref, &first_bytes);

        let restore = RestorePermissions(
            cache.directory.clone(),
            fs::metadata(&cache.directory).unwrap().permissions(),
        );
        fs::set_permissions(&cache.directory, fs::Permissions::from_mode(0o0)).unwrap();
        if fs::read(cache.object_path(first_ref.object_id)).is_ok() {
            eprintln!("skipping permission denial: this process bypasses directory permissions");
            return;
        }
        let fallback = cache.get_or_fetch(first_ref, |_, _| Ok(first_bytes.clone()));
        drop(restore);
        assert_eq!(fallback.unwrap(), first_bytes);
        assert_eq!(fetch(&cache, second_ref, &second_bytes), second_bytes);

        let object_bytes: u64 = fs::read_dir(&cache.directory)
            .unwrap()
            .map(Result::unwrap)
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "obj"))
            .map(|entry| entry.metadata().unwrap().len())
            .sum();
        assert_eq!(object_bytes, capacity);
        assert!(!cache.object_path(first_ref.object_id).exists());
        assert_eq!(cache.lock_state().unwrap().encoded_bytes, capacity);
    }

    #[test]
    fn failed_temporary_cleanup_keeps_capacity_reserved_until_reopen() {
        let directory = TestDirectory::new();
        let workspace = WorkspaceId::from_bytes([0x4D; 16]);
        let (reference, bytes) = encoded_object(0xB5);
        let capacity = bytes.len() as u64;
        let cache =
            WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, capacity).unwrap();
        let mut reservation = cache
            .reserve_install(reference.object_id, capacity)
            .unwrap()
            .unwrap();

        // A directory at the cleanup path makes unlink fail even as root. Its
        // payload stands in for the temporary bytes that could not be removed.
        let blocked_path = cache.directory.join(".tmp-blocked");
        fs::create_dir(&blocked_path).unwrap();
        let payload_path = blocked_path.join("payload");
        fs::write(&payload_path, &bytes).unwrap();
        reservation.owned_path = Some(blocked_path.clone());
        drop(reservation);

        assert_eq!(cache.lock_state().unwrap().reserved_bytes, capacity);
        assert_eq!(fetch(&cache, reference, &bytes), bytes);
        assert!(!cache.object_path(reference.object_id).exists());
        assert_eq!(fs::read(&payload_path).unwrap(), bytes);

        // Recover access to the residual regular file before reopening, as
        // would happen when a transient directory permission failure clears.
        let orphan_path = cache.directory.join(".tmp-orphan");
        fs::rename(&payload_path, &orphan_path).unwrap();
        fs::remove_dir(&blocked_path).unwrap();
        drop(cache);
        let reopened =
            WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, capacity).unwrap();
        assert!(!orphan_path.exists());
        assert_eq!(fetch(&reopened, reference, &bytes), bytes);
        let state = reopened.lock_state().unwrap();
        assert_eq!(state.encoded_bytes, capacity);
        assert_eq!(state.reserved_bytes, 0);
    }

    #[test]
    fn failed_unlink_retains_charge_through_eviction_rollback_and_same_key_refill() {
        let directory = TestDirectory::new();
        let workspace = WorkspaceId::from_bytes([0x4C; 16]);
        let (reference, bytes) = encoded_object(0xB3);
        let capacity = (bytes.len() * 2) as u64;
        let replacement = encode_object(ObjectType::ArtifactBlob, &[0xB4; 528]).unwrap();
        let replacement_ref = ObjectRef::from_encoded(&replacement).unwrap();
        assert_eq!(replacement.len() as u64, capacity);
        let cache =
            WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, capacity).unwrap();
        fetch(&cache, reference, &bytes);
        let object_path = cache.object_path(reference.object_id);
        fs::remove_file(&object_path).unwrap();
        fs::create_dir(&object_path).unwrap();

        let result = cache
            .get_or_fetch(reference, |_, _| {
                // A directory obstructs both reading and unlinking, even as root.
                assert_eq!(
                    cache.lock_state().unwrap().encoded_bytes,
                    bytes.len() as u64
                );
                let mut eviction = cache
                    .reserve_install(replacement_ref.object_id, capacity)
                    .unwrap()
                    .unwrap();
                {
                    let state = cache.lock_state().unwrap();
                    assert_eq!(state.encoded_bytes, 0);
                    assert_eq!(state.pending_delete_bytes, bytes.len() as u64);
                    assert_eq!(state.reserved_bytes, capacity);
                }
                assert!(
                    cache
                        .reserve_install(reference.object_id, bytes.len() as u64)
                        .unwrap()
                        .is_none()
                );
                assert!(eviction.delete_victim(0).is_err());
                drop(eviction);
                {
                    let state = cache.lock_state().unwrap();
                    assert_eq!(state.encoded_bytes, bytes.len() as u64);
                    assert_eq!(state.pending_delete_bytes, 0);
                    assert_eq!(state.reserved_bytes, 0);
                    assert!(state.evicting_keys.is_empty());
                }

                // Recover local access before the fill installs its result. The
                // retained indexed object must not receive a second byte charge.
                fs::remove_dir(&object_path).unwrap();
                fs::write(&object_path, &bytes).unwrap();
                Ok(bytes.clone())
            })
            .unwrap();
        assert_eq!(result, bytes);
        let state = cache.lock_state().unwrap();
        assert_eq!(state.entries.len(), 1);
        assert_eq!(state.encoded_bytes, bytes.len() as u64);
        assert_eq!(state.pending_delete_bytes, 0);
        assert_eq!(state.reserved_bytes, 0);
    }

    #[test]
    fn cache_fill_coordination_fetches_an_object_once_for_concurrent_readers() {
        let directory = TestDirectory::new();
        let workspace = WorkspaceId::from_bytes([0x34; 16]);
        let (reference, bytes) = encoded_object(0xD4);
        let cache = Arc::new(
            WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, 1024).unwrap(),
        );
        let fetches = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(Barrier::new(8));
        let workers: Vec<_> = (0..8)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let fetches = Arc::clone(&fetches);
                let start = Arc::clone(&start);
                let bytes = bytes.clone();
                thread::spawn(move || {
                    start.wait();
                    cache
                        .get_or_fetch(reference, |_, _| {
                            fetches.fetch_add(1, Ordering::SeqCst);
                            thread::sleep(Duration::from_millis(10));
                            Ok(bytes)
                        })
                        .unwrap()
                })
            })
            .collect();

        for worker in workers {
            assert_eq!(worker.join().unwrap(), bytes);
        }
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn panicking_fill_wakes_its_follower_and_allows_retry() {
        let directory = TestDirectory::new();
        let workspace = WorkspaceId::from_bytes([0x41; 16]);
        let (reference, bytes) = encoded_object(0xA4);
        let cache = Arc::new(
            WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, 1024).unwrap(),
        );
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (panic_tx, panic_rx) = std::sync::mpsc::channel();
        let leader_cache = Arc::clone(&cache);
        let leader = thread::spawn(move || {
            leader_cache.get_or_fetch(reference, |_, _| {
                entered_tx.send(()).unwrap();
                panic_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                panic!("injected object fetch panic");
            })
        });
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        // Join the existing flight before allowing its leader to unwind.
        let (flight, is_leader) = cache.acquire_flight(CacheKey(reference.object_id)).unwrap();
        assert!(!is_leader);
        let (completed_tx, completed_rx) = std::sync::mpsc::channel();
        let follower = thread::spawn(move || assert!(completed_tx.send(flight.wait()).is_ok()));
        panic_tx.send(()).unwrap();

        assert!(leader.join().is_err());
        assert!(matches!(
            completed_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Err(SharedCacheError::Fetch(_))
        ));
        follower.join().unwrap();
        assert_eq!(fetch(&cache, reference, &bytes), bytes);
    }

    #[test]
    fn completed_and_interrupted_fills_retire_before_publishing_results() {
        for interrupted in [false, true] {
            let directory = TestDirectory::new();
            let workspace = WorkspaceId::from_bytes([0x4A; 16]);
            let cache =
                WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, 1024).unwrap();
            let (reference, _) = encoded_object(0xA5);
            let key = CacheKey(reference.object_id);
            let (flight, is_leader) = cache.acquire_flight(key).unwrap();
            assert!(is_leader);
            let (follower_flight, is_leader) = cache.acquire_flight(key).unwrap();
            assert!(!is_leader);

            // Block publication so retirement and replacement can be observed
            // independently of which thread runs immediately after notification.
            let publication = flight.result.lock().unwrap();
            thread::scope(|scope| {
                let follower = scope.spawn(move || follower_flight.wait());
                let leader = scope.spawn(|| {
                    let leader = FillLeader {
                        cache: &cache,
                        key,
                        flight: Arc::clone(&flight),
                        completed: false,
                    };
                    if interrupted {
                        drop(leader);
                    } else {
                        leader.complete(Err(SharedCacheError::Fetch(Arc::from(
                            "injected fetch failure",
                        ))));
                    }
                });
                let deadline = Instant::now() + Duration::from_secs(2);
                let replacement = loop {
                    match cache.acquire_flight(key) {
                        Ok((flight, true)) => break Some(flight),
                        Ok(_) if Instant::now() < deadline => thread::yield_now(),
                        Ok(_) | Err(_) => break None,
                    }
                };

                // Release publication and join both threads before asserting,
                // including when the old ordering prevents a replacement.
                drop(publication);
                let leader_result = leader.join();
                let follower_result = follower.join();
                leader_result.unwrap();
                let expected_error = if interrupted {
                    "object cache fill was interrupted"
                } else {
                    "injected fetch failure"
                };
                assert!(matches!(
                    follower_result.unwrap(),
                    Err(SharedCacheError::Fetch(message)) if message.as_ref() == expected_error
                ));
                let replacement = replacement.expect("finished fills must permit a new attempt");
                let (current, is_leader) = cache.acquire_flight(key).unwrap();
                assert!(!is_leader);
                assert!(Arc::ptr_eq(&current, &replacement));
                assert!(!Arc::ptr_eq(&current, &flight));
                cache.release_flight(key, &replacement);
            });
        }
    }

    #[test]
    fn different_object_misses_fetch_concurrently() {
        let directory = TestDirectory::new();
        let workspace = WorkspaceId::from_bytes([0x3A; 16]);
        let (first_ref, first_bytes) = encoded_object(0xA3);
        let (second_ref, second_bytes) = encoded_object(0xB3);
        let cache = Arc::new(
            WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, 1024).unwrap(),
        );
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_first_tx, release_first_rx) = std::sync::mpsc::channel();
        let (release_second_tx, release_second_rx) = std::sync::mpsc::channel();

        let first_cache = Arc::clone(&cache);
        let first_entered = entered_tx.clone();
        let first = thread::spawn(move || {
            first_cache
                .get_or_fetch(first_ref, |_, _| {
                    first_entered.send(()).unwrap();
                    release_first_rx
                        .recv_timeout(Duration::from_secs(2))
                        .unwrap();
                    Ok(first_bytes)
                })
                .unwrap()
        });

        let second_cache = Arc::clone(&cache);
        let second = thread::spawn(move || {
            second_cache
                .get_or_fetch(second_ref, |_, _| {
                    entered_tx.send(()).unwrap();
                    release_second_rx
                        .recv_timeout(Duration::from_secs(2))
                        .unwrap();
                    Ok(second_bytes)
                })
                .unwrap()
        });

        let first_entered = entered_rx.recv_timeout(Duration::from_secs(1)).is_ok();
        let second_entered =
            first_entered && entered_rx.recv_timeout(Duration::from_millis(100)).is_ok();
        release_first_tx.send(()).unwrap();
        release_second_tx.send(()).unwrap();

        assert_eq!(first.join().unwrap().len(), 272);
        assert_eq!(second.join().unwrap().len(), 272);
        assert!(
            first_entered && second_entered,
            "unrelated object fetches must not serialize behind one cache lock"
        );
    }

    #[test]
    fn local_cache_io_failures_do_not_fail_verified_reads_or_backing_writes() {
        let directory = TestDirectory::new();
        let workspace = WorkspaceId::from_bytes([0x3B; 16]);
        let cache = WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, 1024).unwrap();
        let (read_ref, read_bytes) = encoded_object(0xC3);
        fs::create_dir(cache.object_path(read_ref.object_id)).unwrap();

        let read_result = cache
            .get_or_fetch(read_ref, |_, _| Ok(read_bytes.clone()))
            .unwrap();
        assert_eq!(read_result, read_bytes);

        let workspace = WorkspaceId::from_bytes([0x3C; 16]);
        let cache = WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, 1024).unwrap();
        let (write_ref, write_bytes) = encoded_object(0xD3);
        fs::create_dir(cache.object_path(write_ref.object_id)).unwrap();
        let mut store = CachedMapStore::new(MemoryStore::default(), cache).unwrap();

        assert!(
            store
                .put(ObjectId::from_bytes([0; 32]), write_bytes.clone())
                .is_err()
        );
        assert!(store.backing().objects.is_empty());
        store.put(write_ref.object_id, write_bytes.clone()).unwrap();
        assert_eq!(store.get(write_ref.object_id).unwrap(), Some(write_bytes));
        assert_eq!(store.backing().gets.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn unreadable_cached_file_falls_back_to_the_verified_backend() {
        let directory = TestDirectory::new();
        let workspace = WorkspaceId::from_bytes([0x3D; 16]);
        let (reference, bytes) = encoded_object(0xE3);
        let cache = WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, 1024).unwrap();
        fetch(&cache, reference, &bytes);

        let object_path = cache.object_path(reference.object_id);
        fs::remove_file(&object_path).unwrap();
        fs::create_dir(&object_path).unwrap();
        let fetches = AtomicUsize::new(0);
        let result = cache
            .get_or_fetch(reference, |_, _| {
                fetches.fetch_add(1, Ordering::SeqCst);
                Ok(bytes.clone())
            })
            .unwrap();

        assert_eq!(result, bytes);
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn workspace_caches_do_not_share_local_objects_or_authorization_scope() {
        let directory = TestDirectory::new();
        let first_workspace = WorkspaceId::from_bytes([0x35; 16]);
        let second_workspace = WorkspaceId::from_bytes([0x36; 16]);
        let (reference, bytes) = encoded_object(0xE5);
        let first =
            WorkspaceObjectCache::open(&directory.0, first_workspace, STORE_SCOPE, 1024).unwrap();
        let second =
            WorkspaceObjectCache::open(&directory.0, second_workspace, STORE_SCOPE, 1024).unwrap();
        fetch(&first, reference, &bytes);

        let second_fetches = AtomicUsize::new(0);
        second
            .get_or_fetch(reference, |workspace, _| {
                assert_eq!(workspace, second_workspace);
                second_fetches.fetch_add(1, Ordering::SeqCst);
                Ok(bytes.clone())
            })
            .unwrap();
        assert_eq!(second_fetches.load(Ordering::SeqCst), 1);
        assert_ne!(first_workspace.to_string(), second_workspace.to_string());
    }

    #[test]
    fn fetched_bytes_must_match_the_requested_immutable_identity() {
        let directory = TestDirectory::new();
        let workspace = WorkspaceId::from_bytes([0x37; 16]);
        let (reference, _) = encoded_object(0xF7);
        let (_, wrong_bytes) = encoded_object(0xF8);
        let cache = WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, 1024).unwrap();

        assert!(matches!(
            cache.get_or_fetch(reference, |_, _| Ok(wrong_bytes)),
            Err(CacheError::Format(_))
        ));
        assert_eq!(
            fs::read_dir(directory.0.join(workspace.to_string()))
                .unwrap()
                .count(),
            1
        );
    }

    struct MemoryStore {
        scope: ObjectStoreScopeId,
        objects: HashMap<ObjectId, Vec<u8>>,
        gets: AtomicUsize,
    }

    impl Default for MemoryStore {
        fn default() -> Self {
            Self {
                scope: STORE_SCOPE,
                objects: HashMap::new(),
                gets: AtomicUsize::new(0),
            }
        }
    }

    impl MapObjectStore for MemoryStore {
        fn proof_scope(&self) -> ObjectStoreScopeId {
            self.scope
        }

        fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            Ok(self.objects.get(&id).cloned())
        }

        fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
            self.objects.insert(id, bytes);
            Ok(())
        }
    }

    #[test]
    fn cached_map_store_rejects_a_mismatched_object_namespace() {
        let directory = TestDirectory::new();
        let workspace = WorkspaceId::from_bytes([0x44; 16]);
        let other_scope = ObjectStoreScopeId::from_bytes([0x45; 16]);
        let cache = WorkspaceObjectCache::open(&directory.0, workspace, other_scope, 1024).unwrap();
        assert!(matches!(
            CachedMapStore::new(MemoryStore::default(), cache),
            Err(CacheError::ScopeMismatch { cache_scope, backing_scope })
                if cache_scope == other_scope && backing_scope == STORE_SCOPE
        ));
    }

    #[test]
    fn another_scope_cannot_verify_a_generation_from_the_previous_scopes_cache() {
        use versionfs_core::format::{CaptureMode, GenerationDescriptor, VolumeGeometry};
        use versionfs_core::graph::{GraphError, verify_generation, verify_generation_with_reuse};
        use versionfs_core::identity::{GenerationId, VolumeBoundaryId};
        use versionfs_core::views::{ArtifactMap, LogMap, VolumeExtentMap};

        let directory = TestDirectory::new();
        let workspace = WorkspaceId::from_bytes([0x46; 16]);
        let cache =
            WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, 16 * 1024).unwrap();
        let mut original = CachedMapStore::new(MemoryStore::default(), cache).unwrap();
        let volume = VolumeExtentMap::empty(VolumeGeometry::new(64 * 1024).unwrap(), &mut original)
            .unwrap()
            .seal(&mut original)
            .unwrap();
        let logs = LogMap::empty(&mut original)
            .unwrap()
            .seal(&mut original)
            .unwrap();
        let artifacts = ArtifactMap::empty(&mut original)
            .unwrap()
            .seal(&mut original)
            .unwrap();
        let descriptor = GenerationDescriptor {
            generation_id: GenerationId::from_bytes([0x47; 16]),
            parent_generation_id: None,
            volume_root: volume,
            logs_root: logs,
            artifacts_root: artifacts,
            capture_mode: CaptureMode::BlockCut,
            volume_boundary_id: VolumeBoundaryId::from_bytes([0x48; 16]),
            created_at_unix_ms: 0,
        }
        .encode()
        .unwrap();
        let reference = ObjectRef::from_encoded(&descriptor).unwrap();
        original.put(reference.object_id, descriptor).unwrap();
        let original_proof = verify_generation(&original, reference).unwrap();
        assert_eq!(original_proof.objects().len(), 7);
        drop(original);

        let other_scope = ObjectStoreScopeId::from_bytes([0x49; 16]);
        let cache =
            WorkspaceObjectCache::open(&directory.0, workspace, other_scope, 16 * 1024).unwrap();
        let empty = CachedMapStore::new(
            MemoryStore {
                scope: other_scope,
                ..MemoryStore::default()
            },
            cache,
        )
        .unwrap();
        assert!(matches!(
            verify_generation(&empty, reference),
            Err(GraphError::MissingObject(id)) if id == reference.object_id
        ));
        assert!(verify_generation_with_reuse(&empty, reference, &[&original_proof]).is_err());
        assert!(empty.backing().objects.is_empty());
    }

    #[test]
    fn cached_map_store_seeds_verified_puts_and_preserves_missing_object_semantics() {
        let directory = TestDirectory::new();
        let workspace = WorkspaceId::from_bytes([0x39; 16]);
        let cache = WorkspaceObjectCache::open(&directory.0, workspace, STORE_SCOPE, 1024).unwrap();
        let mut store = CachedMapStore::new(MemoryStore::default(), cache).unwrap();
        let (reference, bytes) = encoded_object(0xA9);

        store.put(reference.object_id, bytes.clone()).unwrap();
        assert_eq!(store.get(reference.object_id).unwrap(), Some(bytes));
        assert_eq!(store.backing().gets.load(Ordering::SeqCst), 0);
        assert_eq!(store.get(ObjectId::from_bytes([0; 32])).unwrap(), None);
        assert_eq!(store.backing().gets.load(Ordering::SeqCst), 1);

        let (uncached_reference, uncached_bytes) = encoded_object(0xB9);
        store
            .backing
            .objects
            .insert(uncached_reference.object_id, uncached_bytes.clone());
        assert_eq!(
            store.get_ref(uncached_reference).unwrap(),
            Some(uncached_bytes)
        );

        let (missing_reference, _) = encoded_object(0xC9);
        assert_eq!(store.get_ref(missing_reference).unwrap(), None);
    }
}
