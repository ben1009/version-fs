//! Low-overhead counters for device and immutable-object I/O.

use std::sync::atomic::{AtomicU64, Ordering};

/// Snapshot of I/O and object-store counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IoCounterSnapshot {
    /// Number of block read requests.
    pub block_reads: u64,
    /// Number of block write requests.
    pub block_writes: u64,
    /// Number of block flush requests.
    pub block_flushes: u64,
    /// Number of FUA-tagged block writes observed by an adapter.
    pub fua_writes: u64,
    /// Total block bytes read.
    pub block_bytes_read: u64,
    /// Total block bytes written.
    pub block_bytes_written: u64,
    /// Number of immutable object uploads acknowledged.
    pub object_uploads: u64,
    /// Number of immutable object reads completed and verified.
    pub object_downloads: u64,
    /// Total encoded bytes uploaded.
    pub object_bytes_uploaded: u64,
    /// Total encoded bytes downloaded and verified.
    pub object_bytes_downloaded: u64,
}

/// Thread-safe counters for one worker or service instance.
#[derive(Debug, Default)]
pub struct IoCounters {
    block_reads: AtomicU64,
    block_writes: AtomicU64,
    block_flushes: AtomicU64,
    fua_writes: AtomicU64,
    block_bytes_read: AtomicU64,
    block_bytes_written: AtomicU64,
    object_uploads: AtomicU64,
    object_downloads: AtomicU64,
    object_bytes_uploaded: AtomicU64,
    object_bytes_downloaded: AtomicU64,
}

impl IoCounters {
    /// Records a completed block read.
    pub fn block_read(&self, bytes: u64) {
        self.block_reads.fetch_add(1, Ordering::Relaxed);
        self.block_bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records a completed block write, optionally tagged FUA by the adapter.
    pub fn block_write(&self, bytes: u64, fua: bool) {
        self.block_writes.fetch_add(1, Ordering::Relaxed);
        self.block_bytes_written.fetch_add(bytes, Ordering::Relaxed);
        if fua {
            self.fua_writes.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records a completed block flush.
    pub fn block_flush(&self) {
        self.block_flushes.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one acknowledged immutable object upload.
    pub fn object_upload(&self, encoded_bytes: u64) {
        self.object_uploads.fetch_add(1, Ordering::Relaxed);
        self.object_bytes_uploaded
            .fetch_add(encoded_bytes, Ordering::Relaxed);
    }

    /// Records one completed and verified immutable object read.
    pub fn object_download(&self, encoded_bytes: u64) {
        self.object_downloads.fetch_add(1, Ordering::Relaxed);
        self.object_bytes_downloaded
            .fetch_add(encoded_bytes, Ordering::Relaxed);
    }

    /// Returns a consistent-enough relaxed snapshot for metrics and diagnostics.
    pub fn snapshot(&self) -> IoCounterSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        IoCounterSnapshot {
            block_reads: load(&self.block_reads),
            block_writes: load(&self.block_writes),
            block_flushes: load(&self.block_flushes),
            fua_writes: load(&self.fua_writes),
            block_bytes_read: load(&self.block_bytes_read),
            block_bytes_written: load(&self.block_bytes_written),
            object_uploads: load(&self.object_uploads),
            object_downloads: load(&self.object_downloads),
            object_bytes_uploaded: load(&self.object_bytes_uploaded),
            object_bytes_downloaded: load(&self.object_bytes_downloaded),
        }
    }
}
