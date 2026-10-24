//! Version FS client transport, Linux worker integration, and capture coordination.

pub mod object_cache;
pub mod volume;

#[cfg(all(target_os = "linux", feature = "ublk-adapter"))]
pub mod ublk;
