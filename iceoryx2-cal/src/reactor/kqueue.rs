// SPDX-License-Identifier: Apache-2.0 OR MIT
//! macOS level-triggered readiness with nanosecond timeout arguments.

use crate::reactor::{ReactorAttachError, ReactorCreateError, ReactorWaitError};
use alloc::vec::Vec;
use core::{fmt::Debug, ptr, time::Duration};
use iceoryx2_bb_posix::{
    file_descriptor::FileDescriptor, file_descriptor_set::SynchronousMultiplexing,
};
use iceoryx2_log::warn;
use iceoryx2_pal_concurrency_sync::cell::{Cell, RefCell};
use iceoryx2_pal_posix::posix::{self, errno::Errno};

const CAPACITY: usize = 1024;

#[derive(Debug)]
pub struct Reactor {
    queue: FileDescriptor,
    // Like posix_select this reactor is Send, not Sync. Never hold a borrow
    // while invoking user callbacks, which may remove other attachments.
    attachments: RefCell<Vec<(i32, usize)>>,
    next_token: Cell<usize>,
}

pub struct Guard<'reactor, 'attachment> {
    reactor: &'reactor Reactor,
    fd: &'attachment FileDescriptor,
    token: usize,
}

impl crate::reactor::ReactorGuard<'_, '_> for Guard<'_, '_> {
    fn file_descriptor(&self) -> &FileDescriptor {
        self.fd
    }
}

impl Drop for Guard<'_, '_> {
    fn drop(&mut self) {
        let raw = unsafe { self.fd.native_handle() };
        self.reactor
            .attachments
            .borrow_mut()
            .retain(|v| *v != (raw, self.token));
        if let Err(error) = self.reactor.remove(raw, self.token) {
            // Closing an fd automatically removes its kernel registration.
            // A remaining stale event cannot pass the live-token check.
            warn!(from self.reactor, "Unable to remove kqueue attachment {raw}: {error:?}");
        }
    }
}

fn timeout_spec(timeout: Duration) -> posix::timespec {
    let seconds = timeout.as_secs().min(i64::MAX as u64);
    posix::timespec {
        tv_sec: seconds as _,
        tv_nsec: timeout.subsec_nanos() as _,
    }
}

fn event(raw: i32, flags: u16, token: usize) -> posix::KqueueEvent {
    posix::KqueueEvent {
        ident: raw as _,
        filter: posix::EVFILT_READ,
        flags,
        fflags: 0,
        data: 0,
        udata: token as *mut _,
    }
}

impl Reactor {
    fn remove(&self, raw: i32, token: usize) -> Result<(), Errno> {
        loop {
            match self.change(raw, posix::EV_DELETE, token) {
                // Even if the interrupted syscall applied the deletion,
                // repeating it is safe: an absent registration is success.
                Err(Errno::EINTR) => continue,
                Ok(()) | Err(Errno::ENOENT | Errno::EBADF) => return Ok(()),
                error => return error,
            }
        }
    }

    fn change(&self, raw: i32, flags: u16, token: usize) -> Result<(), Errno> {
        let change = event(raw, flags, token);
        if unsafe {
            posix::kevent(
                self.queue.native_handle(),
                &change,
                1,
                ptr::null_mut(),
                0,
                ptr::null(),
            )
        } < 0
        {
            return Err(Errno::get());
        }
        Ok(())
    }

    fn wait<F: FnMut(&FileDescriptor)>(
        &self,
        mut callback: F,
        timeout: Option<Duration>,
    ) -> Result<usize, ReactorWaitError> {
        let timeout = timeout.map(timeout_spec);
        let mut events = [event(0, 0, 0); CAPACITY];
        let count = unsafe {
            posix::kevent(
                self.queue.native_handle(),
                ptr::null(),
                0,
                events.as_mut_ptr(),
                CAPACITY as _,
                timeout.as_ref().map_or(ptr::null(), |t| t),
            )
        };
        if count < 0 {
            return Err(match Errno::get() {
                // Preserve WaitSet's interrupt contract. Its caller recomputes
                // remaining monotonic deadlines; do not restart the full wait.
                Errno::EINTR => ReactorWaitError::Interrupt,
                Errno::EACCES | Errno::EPERM => ReactorWaitError::InsufficientPermissions,
                _ => ReactorWaitError::InternalError,
            });
        }
        let mut delivered = 0;
        for event in &events[..count as usize] {
            let raw = event.ident as i32;
            let token = event.udata as usize;
            if !self.attachments.borrow().contains(&(raw, token)) {
                continue;
            }
            if event.flags & posix::EV_ERROR != 0 {
                return Err(ReactorWaitError::InternalError);
            }
            // The guard borrows the original descriptor. Copy only the handle;
            // never duplicate ownership or retain it past this callback.
            let fd = unsafe { FileDescriptor::non_owning_new_unchecked(raw) };
            callback(&fd);
            delivered += 1;
        }
        Ok(delivered)
    }
}

impl crate::reactor::Reactor for Reactor {
    type Guard<'reactor, 'attachment> = Guard<'reactor, 'attachment>;
    type Builder = ReactorBuilder;

    fn capacity(&self) -> usize {
        CAPACITY
    }
    fn len(&self) -> usize {
        self.attachments.borrow().len()
    }
    fn is_empty(&self) -> bool {
        self.attachments.borrow().is_empty()
    }

    fn attach<'reactor, 'attachment, F: SynchronousMultiplexing + Debug + ?Sized>(
        &'reactor self,
        value: &'attachment F,
    ) -> Result<Self::Guard<'reactor, 'attachment>, ReactorAttachError> {
        let fd = value.file_descriptor();
        let raw = unsafe { fd.native_handle() };
        let mut attachments = self.attachments.borrow_mut();
        if attachments.len() == CAPACITY {
            return Err(ReactorAttachError::CapacityExceeded);
        }
        if attachments.iter().any(|v| v.0 == raw) {
            return Err(ReactorAttachError::AlreadyAttached);
        }
        let token = self.next_token.get();
        let next = token
            .checked_add(1)
            .ok_or(ReactorAttachError::InsufficientResources)?;
        // Failed attempts also consume an identity, never an attachment slot.
        self.next_token.set(next);
        if let Err(error) = self.change(raw, posix::EV_ADD | posix::EV_ENABLE, token) {
            // A failed syscall must not leave an untracked kernel registration.
            if let Err(cleanup) = self.remove(raw, token) {
                warn!(from self, "Unable to clean failed kqueue attachment {raw}: {cleanup:?}");
            }
            return Err(match error {
                Errno::ENOMEM | Errno::ENOSPC => ReactorAttachError::InsufficientResources,
                _ => ReactorAttachError::InternalError,
            });
        }
        attachments.push((raw, token));
        Ok(Guard {
            reactor: self,
            fd,
            token,
        })
    }

    fn try_wait<F: FnMut(&FileDescriptor)>(&self, callback: F) -> Result<usize, ReactorWaitError> {
        self.wait(callback, Some(Duration::ZERO))
    }
    fn timed_wait<F: FnMut(&FileDescriptor)>(
        &self,
        callback: F,
        timeout: Duration,
    ) -> Result<usize, ReactorWaitError> {
        self.wait(callback, Some(timeout))
    }
    fn blocking_wait<F: FnMut(&FileDescriptor)>(
        &self,
        callback: F,
    ) -> Result<usize, ReactorWaitError> {
        self.wait(callback, None)
    }
}

pub struct ReactorBuilder;

impl crate::reactor::ReactorBuilder<Reactor> for ReactorBuilder {
    fn new() -> Self {
        Self
    }
    fn create(self) -> Result<Reactor, ReactorCreateError> {
        let raw = unsafe { posix::kqueue() };
        if raw < 0 {
            return Err(match Errno::get() {
                Errno::EMFILE | Errno::ENFILE | Errno::ENOMEM => {
                    ReactorCreateError::InsufficientResources
                }
                _ => ReactorCreateError::InternalError,
            });
        }
        let queue = unsafe { FileDescriptor::new_unchecked(raw) };
        if unsafe { posix::fcntl_int(queue.native_handle(), posix::F_SETFD, posix::FD_CLOEXEC) } < 0
        {
            return Err(ReactorCreateError::InternalError);
        }
        Ok(Reactor {
            queue,
            attachments: RefCell::new(Vec::new()),
            next_token: Cell::new(1),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn timeout_arguments_preserve_submillisecond_precision() {
        for duration in [
            Duration::ZERO,
            Duration::from_nanos(1),
            Duration::from_nanos(999_999),
            Duration::new(3, 123_456_789),
        ] {
            let spec = timeout_spec(duration);
            assert_eq!(spec.tv_sec as u64, duration.as_secs());
            assert_eq!(spec.tv_nsec as u32, duration.subsec_nanos());
        }
        assert_eq!(timeout_spec(Duration::MAX).tv_sec, i64::MAX);
    }

    #[test]
    fn token_exhaustion_cannot_wrap_or_consume_a_slot() {
        use crate::reactor::{Reactor as _, ReactorBuilder as _};
        use iceoryx2_bb_posix::file_descriptor::FileDescriptorBased;
        #[derive(Debug)]
        struct Watched(FileDescriptor);
        impl FileDescriptorBased for Watched {
            fn file_descriptor(&self) -> &FileDescriptor {
                &self.0
            }
        }
        impl SynchronousMultiplexing for Watched {}
        let reactor = ReactorBuilder::new().create().unwrap();
        let fd = Watched(FileDescriptor::non_owning_new(0).unwrap());
        reactor.next_token.set(usize::MAX);
        assert!(matches!(
            reactor.attach(&fd),
            Err(ReactorAttachError::InsufficientResources)
        ));
        assert_eq!(reactor.next_token.get(), usize::MAX);
        assert!(reactor.is_empty());
    }
}
