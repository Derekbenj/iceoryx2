# Zegra iceoryx2 0.10.0 fixes

This branch starts at upstream `v0.10.0`, commit
`135d09dd8b29f321f1725920d434864c4e512378`. It carries the existing
Zegra descriptor-lifetime backports, select-range guards, and macOS kqueue
reactor with their regression sources. Production Rust changes are identical
to `Derekbenj/zegra` commit `2c716a083aade351f9a66cbf99e3de01d0a8abe3`,
`vendor/patches/iceoryx2-0.10.0/series`. Workspace path dependencies connect all
iceoryx2 crates; consuming applications need no `[patch]` entries.

The first two changes backport upstream PRs 2083 and 2102. The select guards
and macOS reactor are local fixes. Zegra records the original archive checksums,
ordered patch series, provenance, and platform evidence. The additional manifest
changes declare CAL's macOS PAL dependency and the regression tests' dependencies.

Run the focused regressions from this repository with native local IPC access:

```sh
cargo test --locked -p iceoryx2-cal --features std --test fd_storage --test macos_kqueue -- --test-threads=1
cargo test --locked -p iceoryx2-bb-posix --features std --test fd_mapped_storage --test fd_select_guard -- --test-threads=1
```

Native storage regressions cover macOS and Linux; kqueue and select-boundary
checks are macOS-only. Zegra separately verifies its actual cross-process runtime.
These focused checks do not certify Windows, the C ABI, or the full upstream
workspace. Zegra's original patch set deliberately excludes the C FFI fixed
storage-size changes. Other upstream packages remain present and unmodified.

Consumers should pin a full commit. Once a released upstream version contains
all the needed fixes, Zegra can switch its direct dependency back to that release
and run its compatibility and runtime checks.
