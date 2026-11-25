//! Explicit Linux ext4 formatting, mount/unmount, and `fsfreeze` boundary helpers.
//!
//! Formatting is explicitly destructive (`confirm_destructive == true` required)
//! and is never performed implicitly during attach, fork, rollback, or restore.
//! Mount and unmount never pass lazy (`-l`) or forced (`-f`) flags.

use std::path::Path;
use std::process::Command;
use thiserror::Error;

use crate::capture::FilesystemBoundary;
use crate::volume::BlockDevice;

/// Errors returned by explicit Linux filesystem helpers.
#[derive(Debug, Error)]
pub enum LinuxFsError {
    /// Destructive formatting requires explicit confirmation.
    #[error("refusing to format {0} without explicit destructive confirmation")]
    ConfirmationRequired(String),
    /// The target device or mount point is in an invalid state (for example, mounted during
    /// format).
    #[error("invalid filesystem state: {0}")]
    InvalidState(String),
    /// External Linux helper command failed or is unavailable.
    #[error("filesystem helper command failed: {0}")]
    CommandFailed(String),
    /// Helper was invoked on a non-Linux platform.
    #[error("Linux ext4/fsfreeze helpers require Linux")]
    UnsupportedPlatform,
}

/// Explicitly formats `device` with `ext4` (`mkfs.ext4 -F -b 4096 -E nodiscard`).
///
/// Never called implicitly during attach or restore. Fails if
/// `confirm_destructive` is `false` or if `device` is currently mounted.
pub fn format_ext4(device: &Path, confirm_destructive: bool) -> Result<(), LinuxFsError> {
    if !confirm_destructive {
        return Err(LinuxFsError::ConfirmationRequired(
            device.display().to_string(),
        ));
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = device;
        Err(LinuxFsError::UnsupportedPlatform)
    }
    #[cfg(target_os = "linux")]
    {
        if is_device_mounted(device)? {
            return Err(LinuxFsError::InvalidState(format!(
                "device {} is currently mounted; unmount before formatting",
                device.display()
            )));
        }
        run_checked(
            "mkfs.ext4",
            &[
                "-F",
                "-b",
                "4096",
                "-E",
                "nodiscard",
                &device.display().to_string(),
            ],
        )
    }
}

/// Mounts an existing ext4 filesystem at `mount_point`. Never formats implicitly.
pub fn mount_ext4(device: &Path, mount_point: &Path) -> Result<(), LinuxFsError> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (device, mount_point);
        Err(LinuxFsError::UnsupportedPlatform)
    }
    #[cfg(target_os = "linux")]
    {
        run_checked(
            "mount",
            &[
                "-t",
                "ext4",
                &device.display().to_string(),
                &mount_point.display().to_string(),
            ],
        )
    }
}

/// Cleanly unmounts `mount_point` without lazy (`-l`) or forced (`-f`) flags.
pub fn unmount_ext4(mount_point: &Path) -> Result<(), LinuxFsError> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = mount_point;
        Err(LinuxFsError::UnsupportedPlatform)
    }
    #[cfg(target_os = "linux")]
    {
        run_checked("umount", &[&mount_point.display().to_string()])
    }
}

/// Real Linux [`FilesystemBoundary`] using `fsfreeze` and `umount`.
#[derive(Clone, Debug)]
pub struct LinuxFilesystemBoundary {
    mount_point: std::path::PathBuf,
}

impl LinuxFilesystemBoundary {
    /// Creates a boundary controller targeting `mount_point`.
    pub fn new(mount_point: impl AsRef<Path>) -> Self {
        Self {
            mount_point: mount_point.as_ref().to_path_buf(),
        }
    }
}

impl FilesystemBoundary for LinuxFilesystemBoundary {
    fn freeze(&mut self, _device: &mut dyn BlockDevice) -> Result<(), String> {
        #[cfg(not(target_os = "linux"))]
        {
            Err(LinuxFsError::UnsupportedPlatform.to_string())
        }
        #[cfg(target_os = "linux")]
        {
            run_checked(
                "fsfreeze",
                &["--freeze", &self.mount_point.display().to_string()],
            )
            .map_err(|e| e.to_string())
        }
    }

    fn thaw(&mut self) -> Result<(), String> {
        #[cfg(not(target_os = "linux"))]
        {
            Err(LinuxFsError::UnsupportedPlatform.to_string())
        }
        #[cfg(target_os = "linux")]
        {
            if !is_mountpoint(&self.mount_point).map_err(|e| e.to_string())? {
                return Ok(());
            }
            let output = Command::new("fsfreeze")
                .args(["--unfreeze", &self.mount_point.display().to_string()])
                .output()
                .map_err(|e| format!("failed to run fsfreeze --unfreeze: {e}"))?;
            if output.status.success() {
                return Ok(());
            }
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Idempotent thaw when already unfrozen returns EINVAL on some kernels.
            if stderr.contains("Invalid argument") {
                return Ok(());
            }
            Err(format!("fsfreeze --unfreeze failed: {stderr}"))
        }
    }

    fn clean_unmount(&mut self, _device: &mut dyn BlockDevice) -> Result<(), String> {
        unmount_ext4(&self.mount_point).map_err(|e| e.to_string())
    }
}

#[cfg(target_os = "linux")]
fn run_checked(program: &str, args: &[&str]) -> Result<(), LinuxFsError> {
    let output = Command::new(program)
        .args(args)
        .output()
        .map_err(|e| LinuxFsError::CommandFailed(format!("failed to invoke {program}: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(LinuxFsError::CommandFailed(format!(
            "{program} exited with {}: {}",
            output.status,
            stderr.trim()
        )));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn is_device_mounted(device: &Path) -> Result<bool, LinuxFsError> {
    let mounts = std::fs::read_to_string("/proc/mounts")
        .map_err(|e| LinuxFsError::CommandFailed(format!("read /proc/mounts: {e}")))?;
    let target = std::fs::canonicalize(device).unwrap_or_else(|_| device.to_path_buf());
    let target_str = target.to_string_lossy();
    let raw_str = device.to_string_lossy();
    for line in mounts.lines() {
        if let Some(src) = line.split_whitespace().next()
            && (src == target_str.as_ref() || src == raw_str.as_ref())
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(target_os = "linux")]
fn is_mountpoint(mount_point: &Path) -> Result<bool, LinuxFsError> {
    let status = Command::new("mountpoint")
        .args(["--quiet", "--", &mount_point.display().to_string()])
        .status()
        .map_err(|e| LinuxFsError::CommandFailed(format!("invoke mountpoint: {e}")))?;
    Ok(status.success())
}
