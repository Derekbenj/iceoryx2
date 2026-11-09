// Focused 0.10.0 backport regression tests; SPDX-License-Identifier: Apache-2.0 OR MIT
extern crate iceoryx2_bb_loggers;

use iceoryx2_bb_concurrency::atomic::{AtomicU64, Ordering};
use iceoryx2_bb_container::semantic_string::SemanticString;
use iceoryx2_bb_posix::access_mode::AccessMode;
use iceoryx2_bb_system_types::file_name::FileName;
use iceoryx2_cal::{
    dynamic_storage::{
        DynamicStorage, DynamicStorageBuilder, DynamicStorageCreateError, DynamicStorageOpenError,
        posix_shared_memory,
    },
    named_concept::{NamedConceptBuilder, NamedConceptMgmt},
    static_storage::{
        StaticStorage, StaticStorageBuilder, StaticStorageLocked, StaticStorageOpenError,
        StaticStorageReadError, StaticStorageView, file,
    },
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn name() -> FileName {
    static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    FileName::new(
        format!(
            "fd-backport-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        )
        .as_bytes(),
    )
    .unwrap()
}

#[test]
fn static_create_open_read_remove_and_recreate() {
    let name = name();
    for contents in [&b"storage bytes"[..], &b""[..]] {
        let owner = file::Builder::new(&name).create(contents).unwrap();
        let reader = file::Builder::new(&name).open(Duration::ZERO).unwrap();
        assert_eq!(owner.len(), contents.len() as u64);
        assert_eq!(reader.is_empty(), contents.is_empty());
        let mut bytes = vec![0xA5; contents.len() + 3];
        reader.read(&mut bytes).unwrap();
        assert_eq!(&bytes[..contents.len()], contents);
        assert_eq!(&bytes[contents.len()..], &[0xA5; 3]);
        if !contents.is_empty() {
            assert_eq!(
                reader.read(&mut []),
                Err(StaticStorageReadError::BufferTooSmall)
            );
        }
        drop(owner);
        assert!(!file::Storage::does_exist(&name).unwrap());
        assert!(matches!(
            file::Builder::new(&name).open(Duration::ZERO),
            Err(StaticStorageOpenError::DoesNotExist)
        ));
        let mut retained = vec![0; contents.len()];
        reader.read(&mut retained).unwrap();
        assert_eq!(retained, contents);
        drop(reader);
    }
}

#[test]
fn static_locked_and_transferred_ownership() {
    let name = name();
    let locked = file::Builder::new(&name).create_locked().unwrap();
    assert!(matches!(
        file::Builder::new(&name).open(Duration::ZERO),
        Err(StaticStorageOpenError::InitializationNotYetFinalized)
    ));
    let owner = locked.unlock(b"ready").unwrap();
    owner.release_ownership();
    drop(owner);
    let reopened = file::Builder::new(&name).open(Duration::ZERO).unwrap();
    reopened.acquire_ownership();
    let mut bytes = [0; 5];
    reopened.read(&mut bytes).unwrap();
    assert_eq!(&bytes, b"ready");
    drop(reopened);
    assert!(!file::Storage::does_exist(&name).unwrap());
}

#[test]
fn dynamic_mapping_remains_after_owner_removal_and_name_can_be_reused() {
    let name = name();
    let owner = posix_shared_memory::Builder::new(&name)
        .initializer(|value, _| {
            value.write(AtomicU64::new(17));
            true
        })
        .create()
        .unwrap();
    let reader = posix_shared_memory::Builder::<AtomicU64>::new(&name)
        .open(AccessMode::ReadWrite)
        .unwrap();
    assert_eq!(reader.get().swap(42, Ordering::SeqCst), 17);
    assert_eq!(owner.get().load(Ordering::SeqCst), 42);
    drop(owner);
    assert!(!posix_shared_memory::Storage::<AtomicU64>::does_exist(&name).unwrap());
    assert!(matches!(
        posix_shared_memory::Builder::<AtomicU64>::new(&name).open(AccessMode::Read),
        Err(DynamicStorageOpenError::DoesNotExist)
    ));
    assert_eq!(reader.get().load(Ordering::SeqCst), 42);
    let replacement = posix_shared_memory::Builder::new(&name)
        .initializer(|value, _| {
            value.write(AtomicU64::new(99));
            true
        })
        .create()
        .unwrap();
    assert_eq!(replacement.get().load(Ordering::SeqCst), 99);
    assert_eq!(reader.get().load(Ordering::SeqCst), 42);
    drop(reader);
    drop(replacement);
    assert!(!posix_shared_memory::Storage::<AtomicU64>::does_exist(&name).unwrap());
}

#[test]
fn dynamic_ownership_transfer_and_failed_initialization_cleanup() {
    let name = name();
    assert!(matches!(
        posix_shared_memory::Builder::<AtomicU64>::new(&name).create(),
        Err(DynamicStorageCreateError::InitializationFailed)
    ));
    assert!(!posix_shared_memory::Storage::<AtomicU64>::does_exist(&name).unwrap());
    let owner = posix_shared_memory::Builder::new(&name)
        .initializer(|value, _| {
            value.write(AtomicU64::new(73));
            true
        })
        .create()
        .unwrap();
    owner.release_ownership();
    drop(owner);
    let reopened = posix_shared_memory::Builder::<AtomicU64>::new(&name)
        .open(AccessMode::ReadWrite)
        .unwrap();
    assert!(!reopened.has_ownership());
    reopened.acquire_ownership();
    assert!(reopened.has_ownership());
    assert_eq!(reopened.get().load(Ordering::SeqCst), 73);
    drop(reopened);
    assert!(!posix_shared_memory::Storage::<AtomicU64>::does_exist(&name).unwrap());
}
