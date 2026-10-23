#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
server="${VERSIONFS_UBLK_SERVER:-$repo_root/target/debug/versionfs-ublk-smoke}"

fail() {
    printf 'ublk smoke prerequisite failed: %s\n' "$*" >&2
    exit 2
}

[[ "$(uname -s)" == Linux ]] || fail "Linux is required"
[[ -x "$server" ]] || fail "build the server with: cargo build -p versionfs-sdk --features ublk-smoke --bin versionfs-ublk-smoke"
(( EUID == 0 )) || fail "run as root or with equivalent CAP_SYS_ADMIN and ublk-control access"
[[ -c /dev/ublk-control ]] || fail "/dev/ublk-control is missing; load CONFIG_BLK_DEV_UBLK"
[[ -x "$(command -v mkfs.ext4 || true)" ]] || fail "mkfs.ext4 is required"
[[ -x "$(command -v fsfreeze || true)" ]] || fail "fsfreeze is required"
[[ -x "$(command -v mount || true)" ]] || fail "mount is required"
[[ -x "$(command -v umount || true)" ]] || fail "umount is required"
[[ -x "$(command -v mountpoint || true)" ]] || fail "mountpoint is required"
[[ -x "$(command -v unshare || true)" ]] || fail "unshare is required"
[[ -x "$(command -v timeout || true)" ]] || fail "timeout is required"
[[ -x "$(command -v blkdiscard || true)" ]] || fail "blkdiscard is required"
unshare --mount --propagation private true 2>/dev/null || fail "mount namespaces are required"

work_dir="$(mktemp -d)"
mount_dir="$work_dir/mnt"
server_log="$work_dir/server.log"
crash_log="$work_dir/server-crash.log"
mkdir -p "$mount_dir"
server_pid=""
crash_pid=""
device=""
device_id=""
keep="${VERSIONFS_SMOKE_KEEP:-0}"

mount_state() {
    local result=0
    mountpoint --quiet -- "$mount_dir" || result=$?
    case "$result" in
        0) printf 'mounted' ;;
        32) printf 'unmounted' ;;
        *) printf 'unknown' ;;
    esac
}

cleanup() {
    local status=$?
    local state
    trap - EXIT INT TERM
    state="$(mount_state)"
    if [[ "$state" == mounted ]]; then
        timeout 30s fsfreeze --unfreeze "$mount_dir" >/dev/null 2>&1 || true
        if ! timeout 30s umount "$mount_dir" >/dev/null 2>&1; then
            printf 'smoke cleanup: unmount failed for %s\n' "$mount_dir" >&2
            keep=1
            (( status != 0 )) || status=1
        fi
    fi
    state="$(mount_state)"
    if [[ "$state" != unmounted ]]; then
        printf 'smoke cleanup incomplete: mount state=%s at %s; preserving server PID %s and device %s\n' \
            "$state" "$mount_dir" "${server_pid:-unknown}" "${device:-unknown}" >&2
        keep=1
        (( status != 0 )) || status=1
    else
        if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
            kill -INT "$server_pid" 2>/dev/null || true
            timeout 15s tail --pid="$server_pid" -f /dev/null >/dev/null 2>&1 || kill -KILL "$server_pid" 2>/dev/null || true
            wait "$server_pid" 2>/dev/null || true
        fi
        if [[ -n "$crash_pid" ]] && kill -0 "$crash_pid" 2>/dev/null; then
            kill -KILL "$crash_pid" 2>/dev/null || true
            wait "$crash_pid" 2>/dev/null || true
        fi
        if [[ -n "$device_id" && -e "/sys/block/ublkb$device_id" ]]; then
            timeout 15s "$server" --delete "$device_id" >/dev/null 2>&1 || true
        fi
    fi
    if [[ "$keep" == 1 ]]; then
        printf 'smoke logs kept at %s\n' "$work_dir"
        cat "$server_log" 2>/dev/null || true
        cat "$crash_log" 2>/dev/null || true
    else
        rm -rf "$work_dir"
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# The server must not share the consumer's mount namespace: teardown could otherwise
# unmount ext4 while the exiting server is still needed to service its flush.
unshare --mount --propagation private "$server" >"$server_log" 2>&1 &
server_pid=$!
for _ in $(seq 1 200); do
    if grep -q '^READY /dev/ublkb[0-9][0-9]*$' "$server_log"; then
        device="$(sed -n 's/^READY //p' "$server_log" | head -n 1)"
        break
    fi
    if ! kill -0 "$server_pid" 2>/dev/null; then
        cat "$server_log" >&2
        fail "ublk server exited before publishing its device"
    fi
    sleep 0.05
done
[[ -n "$device" ]] || { cat "$server_log" >&2; fail "timed out waiting for ublk device"; }
device_id="${device##*/ublkb}"

printf 'Formatting %s as ext4...\n' "$device"
timeout 30s mkfs.ext4 -F -b 4096 -E nodiscard "$device" >/dev/null
timeout 30s mount "$device" "$mount_dir"
printf 'versionfs ublk P0 smoke\n' >"$mount_dir/probe.txt"
flushes_before_sync="$(grep -c '^event=flush$' "$server_log" || true)"
timeout 30s sync -f "$mount_dir/probe.txt"
flushes_after_sync="$(grep -c '^event=flush$' "$server_log" || true)"
(( flushes_after_sync > flushes_before_sync )) || fail "ext4 sync did not reach the device FLUSH handler"

printf 'Freezing and thawing ext4...\n'
timeout 30s fsfreeze --freeze "$mount_dir"
timeout 30s fsfreeze --unfreeze "$mount_dir"

# Discard the filesystem's page cache before checking the target's data readback.
timeout 30s umount "$mount_dir"
timeout 30s mount "$device" "$mount_dir"
grep -q '^versionfs ublk P0 smoke$' "$mount_dir/probe.txt"

device_name="${device##*/}"
discard_limit="$(cat "/sys/block/$device_name/queue/discard_max_bytes" 2>/dev/null || printf '0')"
[[ "$discard_limit" == 0 ]] || fail "unsupported discard was advertised ($discard_limit bytes)"

# Exercise orderly teardown with the filesystem unmounted while its server is alive.
timeout 30s umount "$mount_dir"
discard_status=0
timeout 5s blkdiscard "$device" >/dev/null 2>&1 || discard_status=$?
[[ "$discard_status" -ne 0 ]] || fail "unsupported discard unexpectedly succeeded"
[[ "$discard_status" -ne 124 ]] || fail "unsupported discard probe timed out"
kill -INT "$server_pid"
timeout 15s tail --pid="$server_pid" -f /dev/null >/dev/null || fail "server did not stop after SIGINT"
wait "$server_pid"
server_pid=""
for _ in $(seq 1 100); do
    [[ ! -e "/sys/block/$device_name" ]] && break
    sleep 0.05
done
[[ ! -e "/sys/block/$device_name" ]] || fail "ublk device remained after orderly server teardown"
server_pid=""
device=""
device_id=""

# A second device verifies that abrupt server death also releases the kernel device.
unshare --mount --propagation private "$server" >"$crash_log" 2>&1 &
crash_pid=$!
crash_device=""
for _ in $(seq 1 200); do
    if grep -q '^READY /dev/ublkb[0-9][0-9]*$' "$crash_log"; then
        crash_device="$(sed -n 's/^READY //p' "$crash_log" | head -n 1)"
        break
    fi
    if ! kill -0 "$crash_pid" 2>/dev/null; then
        cat "$crash_log" >&2
        fail "second ublk server exited before publishing its device"
    fi
    sleep 0.05
done
[[ -n "$crash_device" ]] || { cat "$crash_log" >&2; fail "timed out waiting for second ublk device"; }
crash_device_id="${crash_device##*/ublkb}"
crash_device_name="${crash_device##*/}"
device_id="$crash_device_id"
kill -KILL "$crash_pid"
wait "$crash_pid" 2>/dev/null || true
crash_pid=""
for _ in $(seq 1 100); do
    [[ ! -e "/sys/block/$crash_device_name" ]] && break
    sleep 0.05
done
if [[ -e "/sys/block/$crash_device_name" ]]; then
    "$server" --delete "$crash_device_id" >/dev/null 2>&1 || true
    fail "ublk device remained after abrupt server death"
fi
device_id=""

printf 'ublk/ext4 mount, read/write, freeze/thaw, unsupported discard, orderly teardown, and server-death teardown passed.\n'
printf 'This RAM-backed prototype does not qualify remote durability or Generation publication.\n'
