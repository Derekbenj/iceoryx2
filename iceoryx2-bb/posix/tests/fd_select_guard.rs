// Focused select-range regression tests; SPDX-License-Identifier: Apache-2.0 OR MIT
extern crate alloc;
extern crate iceoryx2_bb_loggers;
#[macro_use]
extern crate iceoryx2_bb_posix;

// Also exercise private numeric/removal checks without manufacturing an invalid
// FileDescriptor (the safe constructor rejects negative values). The published
// dependency's original test target needs unavailable tests-common packages.
pub use iceoryx2_bb_posix::{clock, file_descriptor};
#[path = "../src/file_descriptor_set.rs"]
pub mod implementation;

#[cfg(target_os = "macos")]
mod macos {
    use iceoryx2_bb_posix::{
        file_descriptor::{FileDescriptor, FileDescriptorBased},
        file_descriptor_set::{
            FileDescriptorSet, FileDescriptorSetAddError, FileEvent, SynchronousMultiplexing,
        },
        udp_socket::{UdpClientBuilder, UdpReceiveError, UdpServerBuilder},
    };
    use iceoryx2_bb_system_types::ipv4_address::LOCALHOST;
    use iceoryx2_cal::reactor::{Reactor, ReactorAttachError, ReactorBuilder, posix_select};
    use iceoryx2_pal_posix::posix::{self, MemZeroedStruct};
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
        // F_DUPFD allocates a distinct owned descriptor without replacing a live one.
        let raw = unsafe { posix::fcntl_int(fd, libc::F_DUPFD, target) };
        assert_eq!(raw, target, "exact descriptor unavailable");
        Watched(FileDescriptor::new(raw).unwrap())
    }

    #[test]
    fn native_boundaries_capacity_reuse_and_udp() {
        if std::env::var_os("IOX_SELECT_GUARD_CHILD").is_none() {
            let output = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "macos::native_boundaries_capacity_reuse_and_udp",
                    "--nocapture",
                ])
                .env("IOX_SELECT_GUARD_CHILD", "1")
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
        // Only this disposable child changes its soft limit, within the hard limit.
        let mut limit = posix::rlimit::new_zeroed();
        assert_eq!(
            unsafe { posix::getrlimit(libc::RLIMIT_NOFILE as _, &mut limit) },
            0
        );
        assert!(limit.rlim_max >= 4096);
        limit.rlim_cur = 4096;
        assert_eq!(
            unsafe { posix::setrlimit(libc::RLIMIT_NOFILE as _, &limit) },
            0
        );
        assert!(FileDescriptor::non_owning_new(-1).is_none());

        let (receiver, sender) = UnixDatagram::pair().unwrap();
        let set = FileDescriptorSet::new();
        let reactor = posix_select::ReactorBuilder::new().create().unwrap();
        for target in [1024, 1100, 2048] {
            let fd = duplicate(receiver.as_raw_fd(), target);
            assert!(!set.contains(&fd));
            assert!(matches!(
                set.add(&fd),
                Err(FileDescriptorSetAddError::FileDescriptorOutOfRange)
            ));
            assert!(set.is_empty());
            assert!(matches!(
                reactor.attach(&fd),
                Err(ReactorAttachError::InsufficientResources)
            ));
            assert!(reactor.is_empty());
            println!(
                "actual_fd={target} add=FileDescriptorOutOfRange reactor=InsufficientResources count=0"
            );
        }

        let fd = duplicate(receiver.as_raw_fd(), 1023);
        let guard = set.add(&fd).unwrap();
        assert!(set.contains(&fd));
        assert!(matches!(
            set.add(&fd),
            Err(FileDescriptorSetAddError::AlreadyAttached)
        ));
        sender.send(b"a").unwrap();
        for _ in 0..2 {
            let mut ready = Vec::new();
            assert_eq!(
                set.timed_wait(Duration::ZERO, FileEvent::Read, |fd| ready
                    .push(unsafe { fd.native_handle() }))
                    .unwrap(),
                1
            );
            assert_eq!(ready, [1023]); // Unread data stays ready.
        }
        receiver.recv(&mut [0]).unwrap();
        let start = Instant::now();
        assert_eq!(
            set.timed_wait(Duration::from_millis(20), FileEvent::Read, |_| panic!(
                "consumed descriptor was ready"
            ))
            .unwrap(),
            0
        );
        assert!(start.elapsed() >= Duration::from_millis(10));
        let send = sender.try_clone().unwrap();
        let worker = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(5));
            send.send(b"b").unwrap();
        });
        assert_eq!(set.blocking_wait(FileEvent::Read, |_| {}).unwrap(), 1);
        worker.join().unwrap();
        receiver.recv(&mut [0]).unwrap();
        drop(guard);
        assert!(!set.contains(&fd));
        assert!(set.is_empty());
        drop(fd);

        let (replacement, replacement_sender) = UnixDatagram::pair().unwrap();
        let reused = duplicate(replacement.as_raw_fd(), 1023);
        let guard = set.add(&reused).unwrap();
        sender.send(b"old").unwrap();
        assert_eq!(
            set.timed_wait(Duration::ZERO, FileEvent::Read, |_| panic!(
                "stale readiness after descriptor reuse"
            ))
            .unwrap(),
            0
        );
        replacement_sender.send(b"new").unwrap();
        assert_eq!(
            set.timed_wait(Duration::ZERO, FileEvent::Read, |_| {})
                .unwrap(),
            1
        );
        drop(guard);
        drop(reused);
        println!("actual_fd=1023 unread_ready=2 consumed_timeout=0 blocking_ready=1 reuse=clean");

        let mut held = Vec::new();
        loop {
            let file = File::open("/dev/null").unwrap();
            let fd = file.as_raw_fd();
            held.push(file);
            if fd >= 2048 {
                break;
            }
        }
        let descriptors = (0..1024)
            .map(|raw| Watched(FileDescriptor::non_owning_new(raw).unwrap()))
            .collect::<Vec<_>>();
        let mut guards = descriptors
            .iter()
            .map(|fd| set.add(fd).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(set.len(), FileDescriptorSet::capacity());
        assert!(matches!(
            set.add(&descriptors[0]),
            Err(FileDescriptorSetAddError::CapacityExceeded)
        ));
        drop(guards.pop());
        assert_eq!(set.len(), 1023);
        let restored = set.add(&descriptors[1023]).unwrap();
        assert_eq!(set.len(), 1024);
        drop(restored);
        drop(guards);
        assert!(set.is_empty());
        println!("attachment_capacity=1024 full=CapacityExceeded remove_and_readd=clean");

        let server = UdpServerBuilder::new().address(LOCALHOST).listen().unwrap();
        let client = UdpClientBuilder::new(LOCALHOST)
            .connect_to(server.port())
            .unwrap();
        assert!(matches!(
            server.timed_receive_from(&mut [0], Duration::ZERO),
            Err(UdpReceiveError::InsufficientResources)
        ));
        assert!(matches!(
            client.timed_receive(&mut [0], Duration::ZERO),
            Err(UdpReceiveError::InsufficientResources)
        ));
        println!("udp_server={server:?} udp_client={client:?} timed_receive=InsufficientResources");
    }
}
