//! Disposable RAM-backed ublk target used only by the P0 ext4 feasibility harness.

#[cfg(target_os = "linux")]
mod linux {
    use libublk::ctrl::{UblkCtrl, UblkCtrlBuilder};
    use libublk::io::{BufDescList, UblkDev, UblkQueue};
    use libublk::{BufDesc, UblkError, UblkFlags, UblkIORes};
    use std::cell::RefCell;
    use std::io::Write;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};

    const DEVICE_BYTES: usize = 128 * 1024 * 1024;

    fn serve_queue(qid: u16, device: &UblkDev, storage: Arc<Mutex<Vec<u8>>>) {
        let buffers = Rc::new(RefCell::new(device.alloc_queue_io_bufs()));
        let queue = match UblkQueue::new(qid, device)
            .unwrap()
            .submit_fetch_commands_unified(BufDescList::Slices(Some(&buffers.borrow())))
        {
            Ok(queue) => queue,
            Err(error) => {
                eprintln!("failed to submit ublk fetch commands: {error}");
                return;
            }
        };

        queue.wait_and_handle_io(move |queue, tag, _context| {
            let mut queue_buffers = buffers.borrow_mut();
            let Some(io_buffer) = queue_buffers.get_mut(tag as usize) else {
                eprintln!("driver supplied out-of-range ublk tag {tag}");
                return;
            };
            let buffer = io_buffer.as_mut_slice();
            let request = queue.get_iod(tag);
            let operation = request.op_flags & 0xff;
            let byte_offset = request.start_sector.checked_mul(512);
            let byte_length = usize::try_from(request.nr_sectors)
                .ok()
                .and_then(|sectors| sectors.checked_mul(512));

            let result = if operation == libublk::sys::UBLK_IO_OP_FLUSH {
                eprintln!("event=flush");
                0
            } else {
                match (byte_offset, byte_length) {
                    (Some(offset), Some(length)) if length <= buffer.len() => {
                    let mut ram = match storage.lock() {
                        Ok(ram) => ram,
                        Err(_) => {
                            complete(queue, tag, buffer, -libc::EIO);
                            return;
                        }
                    };
                    let start = usize::try_from(offset).ok();
                    let end = start.and_then(|start| start.checked_add(length));
                    match (start, end) {
                        (Some(start), Some(end)) if end <= ram.len() => match operation {
                            libublk::sys::UBLK_IO_OP_READ => {
                                buffer[..length].copy_from_slice(&ram[start..end]);
                                length as i32
                            }
                            libublk::sys::UBLK_IO_OP_WRITE => {
                                ram[start..end].copy_from_slice(&buffer[..length]);
                                eprintln!(
                                    "event=write offset={offset} bytes={length} op_flags={:#x}",
                                    request.op_flags >> 8
                                );
                                length as i32
                            }
                            _ => -libc::EOPNOTSUPP,
                        },
                        _ => -libc::EINVAL,
                    }
                    }
                    _ => -libc::EINVAL,
                }
            };

            if result < 0 {
                eprintln!(
                    "event=io_error op={operation} start_sector={} nr_sectors={} offset={byte_offset:?} length={byte_length:?} buffer_bytes={} op_flags={:#x} result={result}",
                    request.start_sector,
                    request.nr_sectors,
                    buffer.len(),
                    request.op_flags >> 8,
                );
            }
            complete(queue, tag, buffer, result);
        });
    }

    fn complete(queue: &UblkQueue<'_>, tag: u16, buffer: &mut [u8], result: i32) {
        if let Err(error) = queue.complete_io_cmd_unified(
            tag,
            BufDesc::Slice(buffer),
            Ok(UblkIORes::Result(result)),
        ) {
            eprintln!("failed to complete ublk request: {error}");
        }
    }

    fn delete_device(device_id: i32) -> Result<(), Box<dyn std::error::Error>> {
        UblkCtrl::new_simple(device_id)?.del_dev()?;
        Ok(())
    }

    pub fn run() -> Result<(), Box<dyn std::error::Error>> {
        let args: Vec<String> = std::env::args().skip(1).collect();
        if args.len() == 2 && args[0] == "--delete" {
            delete_device(args[1].parse()?)?;
            return Ok(());
        }
        if !args.is_empty() {
            return Err("usage: versionfs-ublk-smoke [--delete DEVICE_ID]".into());
        }

        let storage = Arc::new(Mutex::new(vec![0; DEVICE_BYTES]));
        let ctrl = Arc::new(
            UblkCtrlBuilder::default()
                .name("versionfs-p0-smoke")
                .nr_queues(1)
                .depth(32)
                .dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV)
                .build()?,
        );

        let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
        let shutdown_rx = Mutex::new(shutdown_rx);
        ctrlc::set_handler(move || {
            let _ = shutdown_tx.send(());
        })?;

        let queue_storage = Arc::clone(&storage);
        ctrl.run_target(
            |device| {
                device.set_default_params(DEVICE_BYTES as u64);
                device.tgt.params.basic.logical_bs_shift = 12;
                device.tgt.params.basic.physical_bs_shift = 12;
                Ok::<(), UblkError>(())
            },
            move |qid, device| serve_queue(qid, device, Arc::clone(&queue_storage)),
            move |device_ctrl| {
                println!("READY /dev/ublkb{}", device_ctrl.dev_info().dev_id);
                let _ = std::io::stdout().flush();
                let shutdown_received = shutdown_rx
                    .lock()
                    .ok()
                    .is_some_and(|receiver| receiver.recv().is_ok());
                if shutdown_received {
                    if let Err(error) = device_ctrl.kill_dev() {
                        eprintln!("failed to stop ublk device after signal: {error}");
                    }
                }
            },
        )?;

        ctrl.del_dev()?;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    linux::run()
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("versionfs-ublk-smoke is only available on Linux");
    std::process::exit(2);
}
