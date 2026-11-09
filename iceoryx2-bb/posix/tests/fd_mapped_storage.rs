// Focused adaptations of PR 2102 tests; SPDX-License-Identifier: Apache-2.0 OR MIT
extern crate iceoryx2_bb_loggers;

use iceoryx2_bb_concurrency::atomic::{AtomicU64, Ordering};
use iceoryx2_bb_container::semantic_string::SemanticString;
use iceoryx2_bb_elementary_traits::testing::abandonable::Abandonable;
use iceoryx2_bb_posix::shared_memory::{
    AccessMode, CreationMode, MappedSharedMemory, Permission, SharedMemory, SharedMemoryBuilder,
};
use iceoryx2_bb_system_types::file_name::FileName;
use std::time::{SystemTime, UNIX_EPOCH};

fn name() -> FileName {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    FileName::new(
        format!(
            "fd-mapped-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        )
        .as_bytes(),
    )
    .unwrap()
}

fn create(name: &FileName, permission: Permission) -> SharedMemory {
    SharedMemoryBuilder::new(name)
        .creation_mode(CreationMode::CreateExclusive)
        .size(1024)
        .permission(permission)
        .zero_memory(true)
        .create()
        .unwrap()
}

#[test]
fn mapping_retains_bytes_after_owner_removes_name() {
    let name = name();
    let mut owner = create(&name, Permission::OWNER_READ_WRITE);
    owner.as_mut_slice().fill(42);
    let reader = SharedMemoryBuilder::new(&name)
        .open_existing(AccessMode::Read)
        .unwrap()
        .into_mapped();
    assert_eq!(reader.name(), &name);
    assert!(reader.size() >= 1024);
    assert!(SharedMemory::does_exist(&name));
    drop(owner);
    assert!(!SharedMemory::does_exist(&name));
    // The reader owns this mapping, and the original owner has stopped writing.
    let bytes = unsafe { core::slice::from_raw_parts(reader.base_address().as_ptr(), 1024) };
    assert!(bytes.iter().all(|&byte| byte == 42));
}

#[test]
fn owning_mapping_removes_read_only_name_on_drop() {
    let name = name();
    let owner = create(&name, Permission::OWNER_READ).into_mapped();
    assert!(owner.has_ownership());
    assert!(SharedMemory::does_exist(&name));
    drop(owner);
    assert!(!SharedMemory::does_exist(&name));
}

#[test]
fn mapped_ownership_can_be_transferred() {
    let name = name();
    let owner = create(&name, Permission::OWNER_READ_WRITE).into_mapped();
    owner.release_ownership();
    assert!(!owner.has_ownership());
    let reader = SharedMemoryBuilder::new(&name)
        .open_existing(AccessMode::Read)
        .unwrap()
        .into_mapped();
    reader.acquire_ownership();
    assert!(reader.has_ownership());
    drop(owner);
    assert!(SharedMemory::does_exist(&name));
    drop(reader);
    assert!(!SharedMemory::does_exist(&name));
}

#[test]
fn mapped_memory_can_be_reopened_by_name_without_original_descriptor() {
    let name = name();
    let owner = create(&name, Permission::OWNER_READ_WRITE).into_mapped();
    // This mapping is exclusively owned until the following open.
    unsafe { *owner.base_address().as_ptr() = 73 };
    let reader = SharedMemoryBuilder::new(&name)
        .open_existing(AccessMode::Read)
        .unwrap();
    assert_eq!(reader.as_slice()[0], 73);
    drop(reader);
    drop(owner);
    assert!(!SharedMemory::does_exist(&name));
}

#[test]
fn abandoning_mapping_preserves_name_and_allows_new_owner_cleanup() {
    if !SharedMemory::does_support_persistency() {
        return; // Same persistence requirement as the upstream abandonment test.
    }
    let name = name();
    let owner = create(&name, Permission::OWNER_READ_WRITE).into_mapped();
    MappedSharedMemory::abandon(owner);
    let reader = SharedMemoryBuilder::new(&name)
        .open_existing(AccessMode::Read)
        .unwrap();
    assert!(reader.size() >= 1024);
    reader.acquire_ownership();
    drop(reader);
    assert!(!SharedMemory::does_exist(&name));
}

#[test]
fn released_mapping_leaves_persistent_name() {
    if !SharedMemory::does_support_persistency() {
        return; // Platforms without persistent POSIX shared memory differ here.
    }
    let name = name();
    let owner = create(&name, Permission::OWNER_READ_WRITE).into_mapped();
    owner.release_ownership();
    drop(owner);
    assert!(SharedMemory::does_exist(&name));
    assert!(SharedMemory::remove(&name).unwrap());
    assert!(!SharedMemory::does_exist(&name));
}
