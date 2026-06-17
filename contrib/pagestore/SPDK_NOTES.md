# SPDK storage backend — research & bring-up plan

This is the production-storage track for the page-store daemon: replace its
single-threaded, blocking-POSIX I/O with **SPDK** (userspace NVMe + per-core
reactors + asynchronous I/O), on a dedicated NVMe "control disk".  It directly
realizes the two big directions in `DESIGN_NOTES.md`:

- **#1 LSM-like layered store** — SPDK gives us raw block access to build
  image/delta layers, compaction and GC on, instead of flat `seg_*` files.
- **#2 multi-core daemon** — SPDK's reactor model *is* the per-core, lock-free,
  async-I/O architecture that doc asks for (and asks to be co-designed, not
  retrofitted).

This track lives on its own branch (`feat/pagestore-spdk`) because it is a large,
self-contained rework of the daemon's storage and threading model.

## Environment on this box (surveyed 2026-06-17)

Disks are identified by **PCI address** (stable); the kernel `nvmeN` names are
NOT stable across reboot — after the first reboot the system disk came back as
`nvme2n1`, so never key on the kernel name.  `scripts/setup.sh` also refuses to
bind any disk with active mounts, a second safety net.

| PCI      | model / size                | role                         |
|----------|-----------------------------|------------------------------|
| 01:00.0  | WD SN770 500G               | **system** (`/`, ESP, swap) — do not touch |
| 02:00.0  | WD_BLACK SN850X 1000G       | **`/home`** (xfs, repo lives here) — do not touch |
| 06:00.0  | WD SN850 (WDS500G1X0E) 500G | **control disk** → SPDK target (bound to vfio-pci) |

- CPU: i9-13900K, 32 logical CPUs, **single NUMA node** → clean per-core reactors.
- Build deps present: gcc 15.2, meson 1.8, ninja 1.13, nasm 2.16, `libnuma`,
  `libuuid`, `libaio` header, OpenSSL 3.5.  isa-l comes from SPDK's submodule.
- Network OK (can clone github.com/spdk/spdk).
- **Prerequisites still missing** (S0 work):
  - SPDK not installed.
  - Hugepages = 0 (SPDK needs hugepage-backed DMA memory).
  - **IOMMU is not active** — `/sys/kernel/iommu_groups/` is empty and the
    kernel cmdline has no `intel_iommu=on`.  For safe `vfio-pci` DMA we must add
    `intel_iommu=on iommu=pt` to the cmdline and reboot.  Without it SPDK falls
    back to `uio_pci_generic` (or vfio no-IOMMU mode) — works, but no DMA
    protection (a daemon bug can scribble host memory).  **Decision needed:**
    reboot-for-vfio vs unsafe-noiommu for the prototype.

## SPDK primer (only the parts that matter here)

Target version: **v26.01** (released 2026-01-30; bundles DPDK 25.11).  Pin a tag.

- **Reactor / thread model.** One thread pinned per core runs a polled event
  loop; there are *no blocking syscalls* in the data path.  Work is dispatched
  to `spdk_thread`s; each gets its own `io_channel` to a device.  Data owned by a
  single thread needs no locks — exactly the property the daemon relies on today
  (single accessor) but extended to N cores.
- **Memory.** DMA buffers come from hugepages (`spdk_dma_malloc` / `spdk_malloc`).
  I/O buffers must be DMA-able; cannot just `pwrite` an arbitrary heap pointer.
- **Device access.** The NVMe namespace is driven from userspace; the kernel
  `nvme` driver is detached and the device bound to `vfio-pci`/`uio`. Hot path
  has zero kernel involvement.
- **Storage abstractions** (pick per stage below):
  - **bdev** — raw async block layer: `spdk_bdev_read_blocks` /
    `spdk_bdev_write_blocks` over LBAs.  *You* own allocation. Max control.
  - **blobstore** — a lock-free allocator over a bdev: "blobs" are resizable
    extents (thin-provisioned, growable).  Metadata ops are isolated to a single
    metadata thread (its lock-free guarantee).  Can bind the NVMe driver
    directly and skip bdev overhead.  A `seg_*` file maps cleanly to a blob.
  - higher layers (lvol, blobfs, FTL) — not needed.

## The daemon I/O surface to abstract (it is narrow)

Everything the store persists goes through ~4 operations plus a startup scan —
this narrowness is what makes the swap tractable. In `pagestore_daemon.c`:

1. **append page-version** to the current 8 MB segment, returning `(seg, off)` —
   `pwrite(hdr)` + `pwrite(page)` (~L725-746), segment roll-over at `segment_size`.
2. **random read page** at `(seg, off)` — `pread` (~L759-763).
3. **append WAL** to per-timeline `wal_<tl>` — `open(O_APPEND)` + write (~L510).
4. **ranged WAL read** by byte range — `pread` loop (~L549-566).
5. **startup recovery** — sequentially scan every `seg_*` to rebuild the
   in-memory indexes (`recover()`, ~L771-815).  SPDK + persistent metadata
   should eventually remove this scan.

`seg_fd()`/`seg_path()` (~L92-121) are the fd cache to be replaced by
blob handles / LBA ranges.

## Architecture decisions

**A. Packaging: two binaries, one shared brain (DECIDED).**  SPDK must be an
*optional* higher-performance choice with the IPC ABI unchanged, because some
machines cannot run SPDK.  So:
- `pagestore_daemon` (POSIX) keeps its current synchronous poll loop, libc-only,
  no SPDK dependency — the portable default, unchanged in behaviour.
- `pagestore_daemon_spdk` (opt-in build) is a separate binary: its own loop that
  also polls SPDK, async request handling, the SPDK storage backend.
- The **brain is single-sourced** in `pagestore_core.{c,h}` and compiled into
  both: the in-memory indexes, COW/`read_through`, timelines, per-page WAL index,
  shipped-WAL metadata, recovery, and all the non-I/O request handling
  (`ps_handle_meta`).  Only the request *loop* and the page **byte** I/O differ
  per binary (sync vs async), and the byte I/O already goes through `PsStorage`.
- Seam: the brain exposes `ps_locate` (read-through lookup → PsPageVer),
  `ps_reserve` (advance the append cursor) and `ps_record` (index a written
  page); a frontend does the actual `seg_read`/`seg_write` between them (inline
  for POSIX, callback-driven for SPDK).  `ps_handle_meta(ch)` handles every op
  except the four byte-I/O ops (EXTEND/WRITEV/READV/READ_AT), which each frontend
  does itself.  Adding an op touches the shared switch once.
- SPDK runs in **library mode** (`spdk_env_init` + `spdk_thread_poll` inside our
  loop), so even the SPDK frontend keeps its own loop rather than surrendering it
  to `spdk_app_start`.

**B. Storage layout.**
- Start: **blobstore, one blob per segment** (and per `wal_<tl>`). Least code,
  lock-free, thin-provisioned, persistent metadata for free.
- Target: **raw bdev + our own LSM allocator** (image/delta layers), co-designed
  with `DESIGN_NOTES.md` #1.  The blobstore stage validates the async plumbing;
  the LSM stage is where the real layered store lives.

**C. Synchronous→asynchronous request handling (the real work).**  Today a
backend request is served synchronously on the poll loop (`pread` returns, reply
goes back).  With async NVMe a read *completes in a callback*.  The daemon must
track **in-flight requests** and send the reply from the I/O completion callback.
This async refactor of request handling — not the device plumbing — is the bulk
of the effort.

**D. Sharding (multi-core).**  Partition the key space `(timeline, key)` across
reactors; route a backend channel to the core that owns its relations; keep
per-core indexes single-owner (still lock-free).  This is #2's design.

## Staged plan

- **S0 — bring up & validate the control disk.** Install SPDK (clone +
  submodules + `pkgdep.sh` + `configure` + `make`), reserve hugepages, bind
  `06:00.0` to vfio/uio (`scripts/setup.sh`), and benchmark raw nvme2 with
  `examples/identify` and `examples/perf` (randread/randwrite 4 KiB) to get a
  baseline ceiling.  Deliverable: a repeatable setup script + recorded baseline.
  *Blocked on the IOMMU decision above.*
- **S1 — minimal async bdev backend, single reactor.** Daemon becomes an
  `spdk_app`; channel poll becomes a poller; replace segment `pread`/`pwrite`
  with async `spdk_bdev_*`; make request handling in-flight/callback-driven.
  Gate: the existing `integration_test.sh` passes against an SPDK-backed store.
  - **S1.1 — pluggable storage seam (DONE).** All raw byte movement and
    enumeration now go through a `PsStorage` vtable (`pagestore_storage.h`); the
    daemon's indexes, append cursor, timeline metadata and request handling stay
    storage-agnostic.  `storage_posix.c` is the libc-only default backend
    (the prior file layout, moved verbatim), selectable via `--storage posix`
    (default).  A `--storage spdk` slot and `extern PsStorageSpdk` are stubbed
    behind `#ifdef PAGESTORE_SPDK`.  Zero behaviour change: standalone suite
    110/110, `integration_test.sh` PASS.  IPC ABI and compute side untouched —
    SPDK will be a *second* `PsStorage` implementation, never an ABI change.
  - **S1.2a — shared brain extracted (DONE).** The store's logic moved to
    `pagestore_core.{c,h}` (indexes, COW/`read_through`, timelines, per-page WAL
    index, shipped-WAL metadata, recovery, and `ps_handle_meta` for all non-I/O
    ops).  `pagestore_daemon.c` is now a thin POSIX frontend: the synchronous
    channel loop + the four byte-I/O ops via `append_page`/`read_through`/
    `read_version`.  Both compiled into `pagestore_daemon`.  Zero behaviour
    change: standalone 110/110, `integration_test.sh` PASS.  This sets up the
    two-binary split — the SPDK daemon will reuse `pagestore_core.c` verbatim.
  - **S1.2b — SPDK build wiring (DONE).** `storage_spdk.c` (the SPDK PsStorage
    backend) + `pagestore_daemon_spdk.c` (frontend skeleton) + `spdk_build.sh`.
    Library-mode bring-up works: `ps_storage->open()` does `spdk_env_init` +
    `spdk_nvme_probe` on the control disk's PCI address and grabs ns1 + an I/O
    qpair; the frontend reuses `ps_core_open` (the shared brain) and reports
    ready.  Verified: `sudo pagestore_daemon_spdk --pci 0000:06:00.0` attaches
    the disk (sector=512, 976773168 sectors, ~466 GiB).  Byte I/O is stubbed.
    - Build recipe (in `spdk_build.sh`): SPDK static libs need
      `-Wl,--whole-archive ... pkg-config --libs spdk_nvme spdk_env_dpdk ...
      --no-whole-archive` plus `pkg-config --libs --static spdk_syslibs`;
      `PKG_CONFIG_PATH` must include both `$SPDK/build/lib/pkgconfig` and
      `$SPDK/dpdk/build/lib/pkgconfig`; DPDK is shared, so rpath
      `$SPDK/{build,dpdk/build}/lib` to avoid `LD_LIBRARY_PATH`.  The SPDK build
      also links `storage_posix.c` (core's default `ps_storage` references
      `PsStoragePosix`).  Not wired into PG's meson (SPDK is a local-only,
      bound-disk artifact); the script is the compile switch.
  - **S1.2c-1 — SPDK I/O on real NVMe (DONE).** `storage_spdk.c` now does real
    I/O via the async `spdk_nvme_ns_cmd_read/write` API, polled to completion per
    op (correct; cross-channel pipelining is S1.2c-2).  Layout: page **segments**
    live on the raw namespace (segment S byte O -> device byte S*segsize+O), each
    written as one sector-aligned zero-padded extent so recover() stops at the
    zero tail (no per-segment length tracking); the current append segment is a
    DMA buffer flushed on roll-over/sync, older segments read with an aligned
    read + slice.  The shipped **WAL and timeline metadata** stay on the
    filesystem under `--store` (delegated to the POSIX backend) -- small, not hot,
    and awkward to lay out per-timeline on raw blocks; on-device is a later LSM
    refinement.  Segment count persists in `<store>/spdk_super` so a fresh
    `--store` dir is a fresh store and a restart continues.
    - **Validated on the control disk**: the SPDK daemon is argument-compatible
      with the standalone harness (PCI via `$PS_SPDK_PCI`), so
      `sudo PS_SPDK_PCI=0000:06:00.0 ./pagestore_test ./pagestore_daemon_spdk`
      runs the full suite -- **110/110**, exercising page I/O, COW, branches,
      shipped WAL, vectored I/O, 8-client concurrency, and daemon-restart
      recovery, all with pages on actual NVMe.
    - Gotcha: SPDK v26.01 needs `opts.opts_size = sizeof(opts)` set *before*
      `spdk_env_opts_init` (else "Invalid opts->opts_size 40 too small").
  - **S1.2c-2 (next) — pipeline it**: loop issues I/O for many ready channels and
    serves replies from completion callbacks (per-channel in-flight tracking),
    instead of polling each op to completion, to approach the S0 ceiling
    (432 K IOPS).  Then benchmark POSIX vs SPDK.
- **S2 — blobstore for segments + WAL; persistent metadata.** Blob per segment /
  per timeline log; index persisted in blob xattrs or a metadata blob → drop the
  scan-on-restart.
- **S3 — per-core sharding.** N reactors, key-space partition, channel routing,
  per-core lock-free indexes.
- **S4 — LSM layers on raw bdev** (`DESIGN_NOTES.md` #1): image/delta layers,
  per-shard compaction + GC, replacing flat segments.

## S0 progress (2026-06-17)

- **SPDK v26.01 cloned + built at `~/spdk`** (`make` exits 0; `build/bin/spdk_tgt`
  present).  The bench tools are `build/bin/spdk_nvme_perf` and
  `build/bin/spdk_nvme_identify` (the `examples/nvme/{perf,identify}` got renamed
  to `spdk_nvme_*` in `build/bin/` — not `build/examples/`).
- **`scripts/pkgdep.sh` fails on a pip `grpcio` step** under Python 3.14
  ("No matching distribution found for grpcio").  Harmless: that installs the
  Python RPC/test tooling, not C build deps (all the C deps were already on the
  box).  The C build succeeds regardless.
- **IOMMU cmdline applied**: `grubby --update-kernel=ALL --args="intel_iommu=on
  iommu=pt"` — takes effect after a reboot (pending).
- **Bind + benchmark script written**: `contrib/pagestore/spdk_setup.sh`
  (`up`/`status`/`reset`; pins `PCI_ALLOWED=0000:06:00.0` so it only ever touches
  the control disk).  Run it after the reboot to finish S0.

- **S0 COMPLETE** (after the reboot).  IOMMU active (18 groups); `06:00.0` bound
  to vfio-pci; 2048×2 MiB hugepages reserved; system/`/home` disks left on the
  kernel driver.  Control disk = WD SN850 (WDS500G1X0E 500G, 64 I/O queues,
  MDTS 512 KiB, 4 KiB page).
  - **Raw baseline (the ceiling to chase):** 4 KiB randread, qd=128, single core
    → **432 K IOPS, 1688 MiB/s, ~296 µs avg latency** (`spdk_nvme_perf`).

## Concrete commands (reference)

```sh
# build SPDK (done): tools land in build/bin/spdk_nvme_{perf,identify}
git clone --branch v26.01 https://github.com/spdk/spdk ~/spdk
cd ~/spdk && git submodule update --init --recursive && ./configure && make -j

# enable IOMMU for safe DMA (done), then REBOOT:
sudo grubby --update-kernel=ALL --args="intel_iommu=on iommu=pt"

# after reboot: reserve hugepages + bind ONLY the control disk, then bench
contrib/pagestore/spdk_setup.sh
```

> Safety: `spdk_setup.sh` passes `PCI_ALLOWED="0000:06:00.0"` so `setup.sh` only
> ever rebinds the control disk — never the system or `/home` NVMe.

## Open questions

- IOMMU: reboot-for-vfio vs unsafe-noiommu for the prototype.
- SPDK build integration: vendor a tag vs system install; how `meson.build` finds
  SPDK libs/headers (it currently builds a PG contrib module, not an SPDK app).
- Does the daemon stay one `spdk_app`, or split (control-plane PG contrib +
  data-plane SPDK app)?  Leaning single app for now.
- Hugepage reservation persistence across reboot (sysctl `vm.nr_hugepages`).
</content>
</invoke>
