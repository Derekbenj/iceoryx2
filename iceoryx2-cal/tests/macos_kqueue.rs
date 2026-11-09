// Focused macOS reactor regressions; SPDX-License-Identifier: Apache-2.0 OR MIT
#![cfg(target_os = "macos")]
extern crate alloc;
extern crate iceoryx2_bb_loggers;

pub use iceoryx2_cal::reactor;
// Compile the actual implementation so private timeout/token invariants are
// tested without adding test-only production API or missing tests-common crates.
#[path = "../src/reactor/kqueue.rs"]
mod implementation;

use iceoryx2_bb_posix::{
    file_descriptor::{FileDescriptor, FileDescriptorBased},
    file_descriptor_set::SynchronousMultiplexing,
};
use iceoryx2_cal::reactor::{Reactor, ReactorAttachError, ReactorBuilder, ReactorWaitError};
use iceoryx2_pal_concurrency_sync::cell::RefCell;
use std::{
    fs::File,
    os::{fd::AsRawFd, unix::net::UnixDatagram},
    process::Command,
    time::{Duration, Instant},
};

#[derive(Debug)]
struct Watched(FileDescriptor);
impl FileDescriptorBased for Watched {
    fn file_descriptor(&self) -> &FileDescriptor {
        &self.0
    }
}
impl SynchronousMultiplexing for Watched {}

fn duplicate(fd: i32, target: i32) -> Watched {
    let raw = unsafe { libc::fcntl(fd, libc::F_DUPFD, target) };
    assert_eq!(raw, target, "exact descriptor unavailable");
    Watched(FileDescriptor::new(raw).unwrap())
}

#[test]
fn native_lifecycle_capacity_and_interrupt() {
    if std::env::var_os("IOX_KQUEUE_CHILD").is_none() {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "native_lifecycle_capacity_and_interrupt",
                "--nocapture",
            ])
            .env("IOX_KQUEUE_CHILD", "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{:?}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        print!("{}", String::from_utf8_lossy(&output.stdout));
        return;
    }
    // A stalled native wait must terminate this disposable child, not the runner.
    unsafe { libc::alarm(15) };
    // Change only this disposable child's soft descriptor limit.
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    assert_eq!(
        unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) },
        0
    );
    assert!(limit.rlim_max >= 4096);
    limit.rlim_cur = 4096;
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) }, 0);
    let reactor = implementation::ReactorBuilder::new().create().unwrap();
    let (receiver, sender) = UnixDatagram::pair().unwrap();
    let watched = duplicate(receiver.as_raw_fd(), 2048);
    let guard = reactor.attach(&watched).unwrap();
    assert!(matches!(
        reactor.attach(&watched),
        Err(ReactorAttachError::AlreadyAttached)
    ));
    assert_eq!(reactor.len(), 1);

    for _ in 0..3 {
        sender.send(b"queued").unwrap();
    }
    for _ in 0..2 {
        let mut ready = Vec::new();
        assert_eq!(
            reactor
                .try_wait(|fd| ready.push(unsafe { fd.native_handle() }))
                .unwrap(),
            1
        );
        assert_eq!(ready, [2048]); // Multiple pending datagrams coalesce as readiness.
    }
    for _ in 0..3 {
        receiver.recv(&mut [0; 16]).unwrap();
    }
    let start = Instant::now();
    assert_eq!(
        reactor
            .timed_wait(|_| panic!("consumed fd ready"), Duration::from_millis(20))
            .unwrap(),
        0
    );
    assert!(start.elapsed() >= Duration::from_millis(10));
    let send = sender.try_clone().unwrap();
    let wake = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(10));
        send.send(b"wake").unwrap();
    });
    assert_eq!(reactor.blocking_wait(|_| {}).unwrap(), 1);
    wake.join().unwrap();
    receiver.recv(&mut [0; 16]).unwrap();
    drop(guard);
    drop(watched);
    assert!(reactor.is_empty());

    // Removal and same-number reuse must not inherit the old socket's readiness.
    let (replacement, replacement_sender) = UnixDatagram::pair().unwrap();
    let reused = duplicate(replacement.as_raw_fd(), 2048);
    let reused_guard = reactor.attach(&reused).unwrap();
    sender.send(b"old").unwrap();
    assert_eq!(reactor.try_wait(|_| panic!("stale reused fd")), Ok(0));
    replacement_sender.send(b"new").unwrap();
    assert_eq!(reactor.try_wait(|_| {}), Ok(1));
    replacement.recv(&mut [0; 16]).unwrap();
    drop(reused_guard);
    drop(reused);
    println!("actual_fd=2048 level_ready=2 coalesced=3 consumed_block=ok wake=ok removal_reuse=ok");

    // Both are ready in one returned batch. Removing the other guard from the
    // first callback must suppress its already-returned event without a borrow panic.
    let a = duplicate(receiver.as_raw_fd(), 2048);
    let b = duplicate(replacement.as_raw_fd(), 2049);
    let guards = RefCell::new(vec![
        reactor.attach(&a).unwrap(),
        reactor.attach(&b).unwrap(),
    ]);
    replacement_sender.send(b"batch").unwrap();
    let mut callbacks = 0;
    assert_eq!(
        reactor.try_wait(|_| {
            callbacks += 1;
            guards.borrow_mut().clear();
            guards
                .borrow_mut()
                .extend([reactor.attach(&a).unwrap(), reactor.attach(&b).unwrap()]);
        }),
        Ok(1)
    );
    assert_eq!(callbacks, 1);
    assert_eq!(reactor.len(), 2);
    assert_eq!(reactor.try_wait(|_| {}), Ok(2));
    guards.borrow_mut().clear();
    assert!(reactor.is_empty());
    receiver.recv(&mut [0; 16]).unwrap();
    replacement.recv(&mut [0; 16]).unwrap();
    drop(guards);
    drop(a);
    drop(b);

    // /dev/null has no kqueue read filter; it is still a valid owned fd.
    let unsupported = File::open("/dev/null").unwrap();
    let invalid_filter = duplicate(unsupported.as_raw_fd(), 2048);
    assert!(reactor.attach(&invalid_filter).is_err());
    assert!(reactor.is_empty());
    drop(invalid_filter);
    println!("failed_registration_slot_count=0 callback_removal=ok");

    let descriptors = (1100..2125)
        .map(|target| duplicate(receiver.as_raw_fd(), target))
        .collect::<Vec<_>>();
    let mut guards = descriptors[..1024]
        .iter()
        .map(|fd| reactor.attach(fd).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(reactor.capacity(), 1024);
    assert_eq!(reactor.len(), 1024);
    assert!(matches!(
        reactor.attach(&descriptors[1024]),
        Err(ReactorAttachError::CapacityExceeded)
    ));
    drop(guards.pop());
    let restored = reactor.attach(&descriptors[1024]).unwrap();
    assert_eq!(reactor.len(), 1024);
    drop(restored);
    drop(guards);
    drop(descriptors);
    println!(
        "attachment_capacity=1024 fd_range=1100..2123 overflow=CapacityExceeded restoration=ok"
    );

    // Preserve EINTR rather than silently restarting an entire relative wait.
    // The caller uses one monotonic origin to recalculate remaining duration.
    extern "C" fn signal_handler(_: i32) {}
    let mut action: libc::sigaction = unsafe { core::mem::zeroed() };
    action.sa_sigaction = signal_handler as *const () as usize;
    action.sa_flags = 0;
    assert_eq!(unsafe { libc::sigemptyset(&mut action.sa_mask) }, 0);
    assert_eq!(
        unsafe { libc::sigaction(libc::SIGUSR1, &action, core::ptr::null_mut()) },
        0
    );
    let target = unsafe { libc::pthread_self() } as usize;
    let signals = std::thread::spawn(move || {
        for _ in 0..3 {
            std::thread::sleep(Duration::from_millis(15));
            assert_eq!(unsafe { libc::pthread_kill(target as _, libc::SIGUSR1) }, 0);
        }
    });
    let timeout = Duration::from_millis(120);
    let origin = Instant::now();
    let mut remaining = Vec::new();
    let mut interrupts = 0;
    loop {
        let left = timeout.saturating_sub(origin.elapsed());
        remaining.push(left);
        match reactor.timed_wait(|_| panic!("idle readiness"), left) {
            Err(ReactorWaitError::Interrupt) => interrupts += 1,
            Ok(0) => break,
            outcome => panic!("unexpected wait {outcome:?}"),
        }
    }
    signals.join().unwrap();
    assert!(interrupts > 0);
    assert!(remaining.windows(2).all(|w| w[1] < w[0]));
    assert!(origin.elapsed() >= Duration::from_millis(100));
    assert_eq!(reactor.try_wait(|_| panic!("idle callback")), Ok(0));
    println!(
        "eintr={interrupts} monotonic_remaining_ns={:?} elapsed_ns={} idle=ok shutdown=drop",
        remaining.iter().map(Duration::as_nanos).collect::<Vec<_>>(),
        origin.elapsed().as_nanos()
    );
}
