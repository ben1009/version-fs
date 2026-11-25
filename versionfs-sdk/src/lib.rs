//! Version FS client transport, Linux worker integration, and capture coordination.

pub mod artifacts;
pub mod barrier;
pub mod capture;
pub mod client;
pub mod linux_fs;
pub mod logs;
pub mod object_cache;
pub mod volume;

#[cfg(all(target_os = "linux", feature = "ublk-adapter"))]
pub mod ublk;
