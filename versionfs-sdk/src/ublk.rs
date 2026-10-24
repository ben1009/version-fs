//! Linux ublk adapter for the fixed-capacity volatile block data path.

use crate::volume::{BlockError, BlockRequest, BlockResponse, DirtyVolume};
use libublk::ctrl::UblkCtrlBuilder;
use libublk::helpers::IoBuf;
use libublk::io::{BufDesc, BufDescList, UblkDev, UblkQueue};
use libublk::{UblkError, UblkFlags, UblkIORes};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
use versionfs_core::format::LOGICAL_SECTOR_BYTES;
use versionfs_core::persistent_map::MapObjectStore;

const KERNEL_SECTOR_BYTES: u64 = 512;
const UBLK_IO_F_FUA: u32 = 1 << 13;
type QueueInitializer =
    for<'device> fn(u16, &'device UblkDev) -> Result<UblkQueue<'device>, UblkError>;

/// Serves one fixed-size, single-queue ublk device.
///
/// The adapter advertises a volatile write cache and does not advertise native
/// FUA. FLUSH and FUA requests fail until P6 supplies the remote durability path.
/// The shutdown receiver is consumed by the device-control thread; sending a
/// value asks ublk to stop the queue and remove the device.
pub fn run_device<S: MapObjectStore + Send + 'static>(
    name: &str,
    volume: Arc<Mutex<DirtyVolume<S>>>,
    shutdown: mpsc::Receiver<()>,
) -> Result<i32, UblkError> {
    run_device_with_queue_initializer(name, volume, shutdown, UblkQueue::new)
}

fn run_device_with_queue_initializer<S: MapObjectStore + Send + 'static>(
    name: &str,
    volume: Arc<Mutex<DirtyVolume<S>>>,
    shutdown: mpsc::Receiver<()>,
    initialize_queue: QueueInitializer,
) -> Result<i32, UblkError> {
    let capacity_bytes = match volume.lock() {
        Ok(volume) => volume.geometry().capacity_bytes,
        Err(_) => return Err(UblkError::InvalidVal),
    };
    let ctrl = UblkCtrlBuilder::default()
        .name(name)
        .nr_queues(1)
        .depth(32)
        .dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV)
        .build()?;
    let queue_volume = Arc::clone(&volume);
    let shutdown = Mutex::new(shutdown);
    let (queue_error_sender, queue_error_receiver) = mpsc::channel();
    let queue_error_receiver = Mutex::new(queue_error_receiver);
    let control_failed = Arc::new(AtomicBool::new(false));
    let control_failed_flag = Arc::clone(&control_failed);
    let queue_start_error = Arc::new(Mutex::new(None));
    let queue_start_error_for_thread = Arc::clone(&queue_start_error);

    let run_result = ctrl.run_target(
        move |device| {
            device.set_default_params(capacity_bytes);
            device.tgt.params.basic.logical_bs_shift = 12;
            device.tgt.params.basic.physical_bs_shift = 12;
            device.tgt.params.basic.io_min_shift = 12;
            device.tgt.params.basic.io_opt_shift = 12;
            // `set_default_params` sets VOLATILE_CACHE only. Do not set
            // UBLK_ATTR_FUA before the remote barrier path exists.
            Ok::<(), UblkError>(())
        },
        move |queue_id, device| {
            serve_queue(
                queue_id,
                device,
                Arc::clone(&queue_volume),
                queue_error_sender.clone(),
                Arc::clone(&queue_start_error_for_thread),
                initialize_queue,
            );
        },
        move |device_control| {
            let mut failed = false;
            loop {
                match queue_error_receiver
                    .lock()
                    .map(|receiver| receiver.try_recv())
                {
                    Ok(Ok(())) => {
                        eprintln!("ublk queue failed; stopping the device");
                        failed = true;
                        break;
                    }
                    Ok(Err(mpsc::TryRecvError::Empty)) => {}
                    Ok(Err(mpsc::TryRecvError::Disconnected)) | Err(_) => {
                        eprintln!("ublk queue stopped unexpectedly");
                        failed = true;
                        break;
                    }
                }

                let shutdown_result = shutdown
                    .lock()
                    .map(|receiver| receiver.recv_timeout(Duration::from_millis(100)));
                match shutdown_result {
                    Ok(Ok(())) => break,
                    Ok(Err(mpsc::RecvTimeoutError::Timeout)) => {}
                    Ok(Err(mpsc::RecvTimeoutError::Disconnected)) | Err(_) => {
                        eprintln!("ublk shutdown channel closed unexpectedly");
                        failed = true;
                        break;
                    }
                }
            }

            if let Err(error) = device_control.kill_dev() {
                eprintln!("failed to stop ublk device: {error}");
                failed = true;
            }
            if failed {
                control_failed_flag.store(true, Ordering::Release);
            }
        },
    );

    if let Some(error) = queue_start_error
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
    {
        ctrl.del_dev()?;
        return Err(error);
    }
    run_result?;

    ctrl.del_dev()?;
    if control_failed.load(Ordering::Acquire) {
        return Err(UblkError::InvalidVal);
    }
    Ok(0)
}

fn serve_queue<S: MapObjectStore + Send + 'static>(
    queue_id: u16,
    device: &UblkDev,
    volume: Arc<Mutex<DirtyVolume<S>>>,
    queue_errors: mpsc::Sender<()>,
    queue_start_error: Arc<Mutex<Option<UblkError>>>,
    initialize_queue: QueueInitializer,
) {
    let setup = check_queue_buffer_allocations(device.alloc_queue_io_bufs()).and_then(|buffers| {
        let buffers = Rc::new(RefCell::new(buffers));
        let queue = initialize_queue(queue_id, device)?
            .submit_fetch_commands_unified(BufDescList::Slices(Some(&buffers.borrow())))?;
        Ok((queue, buffers))
    });
    let (queue, buffers) = match setup {
        Ok(queue) => queue,
        Err(error) => {
            eprintln!("failed to start ublk queue: {error}");
            *queue_start_error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error);
            // libublk 0.4.8 waits for buffer registration before running the
            // control callback. Its only public registration-failure signal is
            // the mlock flag; use it to abort that wait, then return the actual
            // setup error saved above instead of the internal EPERM sentinel.
            device.notify_buffer_registration_complete(true);
            let _ = queue_errors.send(());
            return;
        }
    };

    let callback_queue_errors = queue_errors.clone();
    queue.wait_and_handle_io(move |queue, tag, _context| {
        let mut queue_buffers = buffers.borrow_mut();
        let Some(io_buffer) = queue_buffers.get_mut(tag as usize) else {
            eprintln!("ublk returned out-of-range request tag {tag}");
            let _ = callback_queue_errors.send(());
            return;
        };
        let buffer = io_buffer.as_mut_slice();
        let request = queue.get_iod(tag);
        let operation = request.op_flags & 0xff;
        let has_fua = request.op_flags & UBLK_IO_F_FUA != 0;
        let result = dispatch_request(
            &volume,
            operation,
            has_fua,
            request.start_sector,
            request.nr_sectors,
            buffer,
        );
        complete(queue, tag, buffer, result, &callback_queue_errors);
    });

    // The ublk library returns from this loop when its io_uring wait fails or
    // the device queue stops. Notify the control loop so an unexpected exit
    // cannot leave `run_device` waiting indefinitely for explicit shutdown.
    let _ = queue_errors.send(());
}

fn check_queue_buffer_allocations(buffers: Vec<IoBuf<u8>>) -> Result<Vec<IoBuf<u8>>, UblkError> {
    if buffers.iter().any(|buffer| buffer.as_ptr().is_null()) {
        // libublk 0.4.8 silently skips null buffers during registration and its
        // IoBuf destructor unconditionally deallocates the pointer. Discard
        // those failed allocations without running that destructor.
        for buffer in buffers {
            if buffer.as_ptr().is_null() {
                std::mem::forget(buffer);
            }
        }
        return Err(UblkError::OtherError(-libc::ENOMEM));
    }

    Ok(buffers)
}

fn dispatch_request<S: MapObjectStore + Send>(
    volume: &Mutex<DirtyVolume<S>>,
    operation: u32,
    has_fua: bool,
    start_sector: u64,
    nr_sectors: u32,
    buffer: &mut [u8],
) -> i32 {
    if operation == libublk::sys::UBLK_IO_OP_FLUSH {
        return -libc::EOPNOTSUPP;
    }
    if !matches!(
        operation,
        libublk::sys::UBLK_IO_OP_READ | libublk::sys::UBLK_IO_OP_WRITE
    ) {
        return -libc::EOPNOTSUPP;
    }

    let Some(offset) = start_sector.checked_mul(KERNEL_SECTOR_BYTES) else {
        return -libc::EINVAL;
    };
    let Some(length) = usize::try_from(nr_sectors)
        .ok()
        .and_then(|sectors| sectors.checked_mul(KERNEL_SECTOR_BYTES as usize))
    else {
        return -libc::EINVAL;
    };
    if length == 0
        || length > buffer.len()
        || !offset.is_multiple_of(u64::from(LOGICAL_SECTOR_BYTES))
        || !length.is_multiple_of(LOGICAL_SECTOR_BYTES as usize)
    {
        return -libc::EINVAL;
    }
    let Ok(mut volume) = volume.lock() else {
        return -libc::EIO;
    };

    let request = match operation {
        libublk::sys::UBLK_IO_OP_READ => BlockRequest::Read {
            offset,
            length: length as u64,
        },
        libublk::sys::UBLK_IO_OP_WRITE => BlockRequest::Write {
            offset,
            data: &buffer[..length],
            fua: has_fua,
        },
        _ => unreachable!("operation was checked above"),
    };

    match volume.dispatch(request) {
        Ok(BlockResponse::Read(bytes)) => {
            buffer[..length].copy_from_slice(&bytes);
            i32::try_from(length).unwrap_or(-libc::EOVERFLOW)
        }
        Ok(BlockResponse::Written(bytes)) => i32::try_from(bytes).unwrap_or(-libc::EOVERFLOW),
        Err(error) => error_to_errno(error),
    }
}

fn error_to_errno(error: BlockError) -> i32 {
    match error {
        BlockError::DirtyCapacityExceeded => -libc::ENOSPC,
        BlockError::DurableBarrierUnavailable | BlockError::UnsupportedOperation(_) => {
            -libc::EOPNOTSUPP
        }
        BlockError::Format(_) => -libc::EINVAL,
        BlockError::View(_) | BlockError::InvalidDirtyCapacity => -libc::EIO,
    }
}

fn complete(
    queue: &UblkQueue<'_>,
    tag: u16,
    buffer: &mut [u8],
    result: i32,
    queue_errors: &mpsc::Sender<()>,
) {
    if let Err(error) =
        queue.complete_io_cmd_unified(tag, BufDesc::Slice(buffer), Ok(UblkIORes::Result(result)))
    {
        eprintln!("failed to complete ublk request: {error}");
        let _ = queue_errors.send(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use versionfs_core::format::VolumeGeometry;
    use versionfs_core::identity::{ObjectId, ObjectStoreScopeId};
    use versionfs_core::persistent_map::MapObjectStore;
    use versionfs_core::views::VolumeExtentMap;

    #[derive(Default)]
    struct Store(HashMap<ObjectId, Vec<u8>>);

    impl MapObjectStore for Store {
        fn proof_scope(&self) -> ObjectStoreScopeId {
            ObjectStoreScopeId::from_bytes([7; 16])
        }

        fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
            Ok(self.0.get(&id).cloned())
        }

        fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
            self.0.insert(id, bytes);
            Ok(())
        }
    }

    #[test]
    fn queue_buffer_allocation_check_preserves_buffers() {
        let buffers = vec![IoBuf::<u8>::new(4096), IoBuf::<u8>::new(8192)];
        let pointers: Vec<_> = buffers.iter().map(IoBuf::as_ptr).collect();

        let checked = check_queue_buffer_allocations(buffers).unwrap();

        assert_eq!(checked.len(), 2);
        assert_eq!(checked[0].len(), 4096);
        assert_eq!(checked[1].len(), 8192);
        assert_eq!(
            checked.iter().map(IoBuf::as_ptr).collect::<Vec<_>>(),
            pointers
        );
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn failpoint_queue_buffer_allocation_failure_returns_enomem() {
        // A 4 EiB layout is valid but exceeds even five-level x86-64 userspace
        // addressing. Sanitizer runs skip failpoint_ tests because their
        // allocators abort on this oversized request instead of returning null.
        let failed_size = 1_usize << 62;
        assert!(std::alloc::Layout::from_size_align(failed_size, 4096).is_ok());
        let failed = IoBuf::<u8>::new(failed_size);
        assert!(failed.as_ptr().is_null());
        let buffers = vec![IoBuf::<u8>::new(4096), failed, IoBuf::<u8>::new(8192)];

        assert!(matches!(
            check_queue_buffer_allocations(buffers),
            Err(UblkError::OtherError(error)) if error == -libc::ENOMEM
        ));
    }

    #[test]
    #[ignore = "requires privileged Linux with /dev/ublk-control"]
    fn ublk_queue_setup_failure_returns_without_waiting_for_registration() {
        fn fail_queue_setup(_queue_id: u16, _device: &UblkDev) -> Result<UblkQueue<'_>, UblkError> {
            Err(UblkError::OtherError(-libc::ENOMEM))
        }

        let mut store = Store::default();
        let root =
            VolumeExtentMap::empty(VolumeGeometry::new(65_536).unwrap(), &mut store).unwrap();
        let volume = Arc::new(Mutex::new(DirtyVolume::new(store, root, 65_536).unwrap()));
        let (_shutdown_sender, shutdown) = mpsc::channel();

        assert!(matches!(
            run_device_with_queue_initializer(
                "versionfs-queue-failure-test",
                volume,
                shutdown,
                fail_queue_setup,
            ),
            Err(UblkError::OtherError(error)) if error == -libc::ENOMEM
        ));
    }

    #[test]
    fn ublk_request_validation_rejects_barriers_and_unaligned_ranges() {
        let mut store = Store::default();
        let root =
            VolumeExtentMap::empty(VolumeGeometry::new(65_536).unwrap(), &mut store).unwrap();
        let volume = DirtyVolume::new(store, root, 65_536).unwrap();
        let mutex = Mutex::new(volume);
        let mut buffer = vec![0; 4096];

        assert_eq!(
            dispatch_request(
                &mutex,
                libublk::sys::UBLK_IO_OP_READ,
                false,
                1,
                8,
                &mut buffer,
            ),
            -libc::EINVAL
        );
        assert_eq!(
            dispatch_request(
                &mutex,
                libublk::sys::UBLK_IO_OP_FLUSH,
                false,
                0,
                0,
                &mut buffer,
            ),
            -libc::EOPNOTSUPP
        );
        assert_eq!(
            dispatch_request(
                &mutex,
                libublk::sys::UBLK_IO_OP_WRITE,
                true,
                0,
                8,
                &mut buffer,
            ),
            -libc::EOPNOTSUPP
        );
    }
}
