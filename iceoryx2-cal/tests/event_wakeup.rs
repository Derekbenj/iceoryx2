// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache Software License 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0, or the MIT license
// which is available at https://opensource.org/licenses/MIT.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT

extern crate iceoryx2_bb_loggers;

use iceoryx2_bb_concurrency::{
    atomic::{AtomicU64, Ordering},
    cell::RefCell,
};
use iceoryx2_bb_container::semantic_string::SemanticString;
use iceoryx2_bb_elementary_traits::testing::abandonable::Abandonable;
use iceoryx2_bb_posix::{
    file_descriptor::{FileDescriptor, FileDescriptorBased},
    file_descriptor_set::{FileDescriptorSet, FileEvent, SynchronousMultiplexing},
};
use iceoryx2_bb_system_types::{file_name::FileName, path::Path};
use iceoryx2_cal::{
    dynamic_storage::posix_shared_memory,
    event::{
        Event, EventId, Listener, ListenerBuilder, ListenerCreateError, ListenerWaitError,
        NamedConceptBuilder, Notifier, NotifierBuilder, NotifierNotifyError, NotifierOpenError,
        common::EventImpl,
        event_state::{EventActivation, counting_bit_set::RelocatableCountingBitSet},
        trigger::{
            Configuration, HandlerInterface, State, WaiterInterface,
            unix_datagram_socket::{UnixDatagramHandle, UnixDatagramWaiter},
        },
    },
    named_concept::{NamedConceptPathHintRemoveError, NamedConceptRemoveError},
};
use std::{mem::MaybeUninit, ptr::NonNull, sync::mpsc, time::Duration};

type EventState = RelocatableCountingBitSet;
type Storage = posix_shared_memory::Storage<State<EventState, ()>>;
type NativeWaiter = UnixDatagramWaiter<EventState, Storage>;
type NativeHandle = UnixDatagramHandle<EventState, Storage>;
type Sut = EventImpl<EventState, (), Storage, InterleavedHandle, InterleavedWaiter>;
type TestListener = <Sut as Event<EventState>>::Listener;
type TestNotifier = <Sut as Event<EventState>>::Notifier;

type Hook = Option<Box<dyn FnOnce()>>;
thread_local! {
    // Only these wrappers intercept the native trigger. Production code has no
    // hook, and parallel tests cannot consume another test thread's schedule.
    static BEFORE_EMPTY: RefCell<Hook> = const { RefCell::new(None) };
    static AFTER_SEND: RefCell<Hook> = const { RefCell::new(None) };
}

fn run_hook(slot: &RefCell<Hook>) {
    let callback = slot.borrow_mut().take();
    if let Some(callback) = callback {
        callback();
    }
}

#[derive(Debug)]
struct InterleavedWaiter(NativeWaiter);

impl FileDescriptorBased for InterleavedWaiter {
    fn file_descriptor(&self) -> &FileDescriptor {
        self.0.file_descriptor()
    }
}
impl SynchronousMultiplexing for InterleavedWaiter {}

impl Abandonable for InterleavedWaiter {
    unsafe fn abandon_in_place(mut this: NonNull<Self>) {
        let this = unsafe { this.as_mut() };
        unsafe { NativeWaiter::abandon_in_place(NonNull::from_mut(&mut this.0)) };
    }
}

impl WaiterInterface<EventState, (), Storage> for InterleavedWaiter {
    const IS_FILE_DESCRIPTOR_BASED: bool = true;

    unsafe fn remove(
        name: &FileName,
        config: &Configuration,
    ) -> Result<bool, NamedConceptRemoveError> {
        unsafe { NativeWaiter::remove(name, config) }
    }

    fn remove_path_hint(value: &Path) -> Result<(), NamedConceptPathHintRemoveError> {
        NativeWaiter::remove_path_hint(value)
    }

    fn create(
        name: &FileName,
        config: &Configuration,
        mgmt: &mut MaybeUninit<()>,
    ) -> Result<Self, ListenerCreateError> {
        NativeWaiter::create(name, config, mgmt).map(Self)
    }

    fn try_wait(&self) -> Result<(), ListenerWaitError> {
        self.0.try_wait()
    }

    fn timed_wait(&self, timeout: Duration) -> Result<(), ListenerWaitError> {
        self.0.timed_wait(timeout)
    }

    fn blocking_wait(&self) -> Result<(), ListenerWaitError> {
        self.0.blocking_wait()
    }

    fn empty_buffer(&self) -> Result<(), ListenerWaitError> {
        BEFORE_EMPTY.with(run_hook);
        self.0.empty_buffer()
    }
}

#[derive(Debug)]
struct InterleavedHandle(NativeHandle);

impl Abandonable for InterleavedHandle {
    unsafe fn abandon_in_place(mut this: NonNull<Self>) {
        let this = unsafe { this.as_mut() };
        unsafe { NativeHandle::abandon_in_place(NonNull::from_mut(&mut this.0)) };
    }
}

impl HandlerInterface<EventState, (), Storage> for InterleavedHandle {
    fn open(name: &FileName, config: &Configuration, mgmt: &()) -> Result<Self, NotifierOpenError> {
        NativeHandle::open(name, config, mgmt).map(Self)
    }

    fn notify(&self) -> Result<(), NotifierNotifyError> {
        self.0.notify()?;
        AFTER_SEND.with(run_hook);
        Ok(())
    }
}

fn fixture() -> (TestListener, TestNotifier) {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let name = FileName::new(
        format!(
            "event-wakeup-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        )
        .as_bytes(),
    )
    .unwrap();
    let listener = <Sut as Event<_>>::ListenerBuilder::new(&name)
        .event_id_max(EventId::new(8))
        .create()
        .unwrap();
    let notifier = <Sut as Event<_>>::NotifierBuilder::new(&name)
        .open()
        .unwrap();
    (listener, notifier)
}

fn notifier(listener: &TestListener) -> TestNotifier {
    use iceoryx2_cal::event::NamedConcept;
    <Sut as Event<_>>::NotifierBuilder::new(listener.name())
        .open()
        .unwrap()
}

fn ready(listener: &TestListener) -> usize {
    let descriptors = FileDescriptorSet::new();
    let _attachment = descriptors.add(listener).unwrap();
    descriptors
        .timed_wait(Duration::from_millis(5), FileEvent::Read, |_| {})
        .unwrap()
}

fn expect_event(listener: &TestListener, id: usize, count: u64) {
    let mut activations = Vec::new();
    assert_eq!(listener.try_wait(|a| activations.push(a)).unwrap(), count);
    assert_eq!(
        activations,
        [EventActivation {
            id: EventId::new(id),
            count
        }]
    );
}

fn notification_before_empty(seed: bool, timed: bool) {
    let (listener, sender) = fixture();
    let interleaved = notifier(&listener);
    if seed {
        sender.notify(EventId::new(0)).unwrap();
    }
    BEFORE_EMPTY.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move || {
            interleaved.notify(EventId::new(1)).unwrap();
        }));
    });
    let mut activations = Vec::new();
    let drained = if timed {
        listener.timed_wait(|a| activations.push(a), Duration::from_millis(1))
    } else {
        listener.try_wait(|a| activations.push(a))
    }
    .unwrap();
    let expected = if seed { vec![0, 1] } else { vec![1] };
    assert_eq!(drained, expected.len() as u64);
    assert_eq!(
        activations,
        expected
            .into_iter()
            .map(|id| EventActivation {
                id: EventId::new(id),
                count: 1
            })
            .collect::<Vec<_>>()
    );

    sender.notify(EventId::new(2)).unwrap();
    // A WaitSet observes the real FD, not the shared event counters. The old
    // ordering leaves NOTIFIED with an empty FD in the unseeded cases.
    assert_eq!(ready(&listener), 1, "future notification lost its wakeup");
    expect_event(&listener, 2, 1);
    assert_eq!(ready(&listener), 0);
}

#[test]
fn notification_between_try_wait_and_buffer_drain_preserves_future_wakeup() {
    notification_before_empty(false, false);
}

#[test]
fn notification_between_timeout_and_buffer_drain_preserves_future_wakeup() {
    notification_before_empty(false, true);
}

#[test]
fn notified_fast_path_preserves_coalesced_events_and_future_wakeup() {
    notification_before_empty(true, false);
}

#[test]
fn pending_notifiers_completing_after_rearm_preserve_future_wakeup() {
    let (listener, sender) = fixture();
    let mut pending = Vec::new();
    for id in 0..2 {
        let interleaved = notifier(&listener);
        let (sent, received) = mpsc::channel();
        let (resume, resumed) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            AFTER_SEND.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move || {
                    sent.send(()).unwrap();
                    resumed.recv_timeout(Duration::from_secs(3)).unwrap();
                }));
            });
            interleaved.notify(EventId::new(id)).unwrap();
        });
        received.recv_timeout(Duration::from_secs(3)).unwrap();
        pending.push((resume, worker));
    }
    // Both senders have enqueued native datagrams but have not yet completed
    // PENDING -> NOTIFIED. Drain them, then let those stale completions observe
    // the receiver's already re-armed IDLE state.
    let mut activations = Vec::new();
    assert_eq!(listener.try_wait(|a| activations.push(a)).unwrap(), 2);
    assert_eq!(
        activations,
        [
            EventActivation {
                id: EventId::new(0),
                count: 1
            },
            EventActivation {
                id: EventId::new(1),
                count: 1
            }
        ]
    );
    assert_eq!(ready(&listener), 0);
    for (resume, worker) in pending {
        resume.send(()).unwrap();
        worker.join().unwrap();
    }
    sender.notify(EventId::new(2)).unwrap();
    assert_eq!(ready(&listener), 1);
    expect_event(&listener, 2, 1);
    assert_eq!(ready(&listener), 0);
}

#[test]
fn notification_from_drain_callback_keeps_next_wakeup() {
    let (listener, sender) = fixture();
    sender.notify(EventId::new(0)).unwrap();
    assert_eq!(
        listener
            .try_wait(|activation| {
                assert_eq!(activation.id, EventId::new(0));
                assert_eq!(activation.count, 1);
                sender.notify(EventId::new(0)).unwrap();
            })
            .unwrap(),
        1
    );
    assert_eq!(ready(&listener), 1);
    expect_event(&listener, 0, 1);
    assert_eq!(ready(&listener), 0);
}

#[test]
fn coalesced_counts_and_idle_timeout_use_native_trigger() {
    let (listener, sender) = fixture();
    assert_eq!(
        listener.timed_wait(|_| panic!("idle event"), Duration::from_millis(1)),
        Ok(0)
    );
    for _ in 0..50 {
        sender.notify(EventId::new(3)).unwrap();
    }
    assert_eq!(ready(&listener), 1);
    expect_event(&listener, 3, 50);
    assert_eq!(ready(&listener), 0);
    assert_eq!(listener.try_wait(|_| panic!("stale event")), Ok(0));
}
