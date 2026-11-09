// SPDX-License-Identifier: Apache-2.0 OR MIT
//! Native macOS kqueue bindings. Keep ABI types supplied by libc.

pub use libc::{EV_ADD, EV_DELETE, EV_ENABLE, EV_ERROR, EVFILT_READ, F_SETFD, FD_CLOEXEC};
pub type KqueueEvent = libc::kevent;

/// Creates a native kernel event queue.
///
/// # Safety
/// The caller must close the returned descriptor after a successful call.
pub unsafe fn kqueue() -> i32 {
    unsafe { libc::kqueue() }
}

/// Applies changes and/or waits for kernel events.
///
/// # Safety
/// The descriptor must be valid and all pointers must reference the stated
/// number of initialized elements (or be null when their count is zero).
pub unsafe fn kevent(
    queue: i32,
    changes: *const KqueueEvent,
    change_count: i32,
    events: *mut KqueueEvent,
    event_count: i32,
    timeout: *const super::types::timespec,
) -> i32 {
    unsafe { libc::kevent(queue, changes, change_count, events, event_count, timeout) }
}
