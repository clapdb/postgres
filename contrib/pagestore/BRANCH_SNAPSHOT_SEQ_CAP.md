# Branch snapshot seq cap: a read-side same-position freeze for Bug B

Status: design proposal, for review before any implementation.
Baseline: `origin/pagestore` @ `0550146156d`. Line references are to that tree.
Supersedes: the admission-time LSN promotion in PR #294 (`d504a7af748`), which was abandoned on 2026-09-25.
Keeps from #294: the per-opcode `ls_op_lsn()` fallback (split into its own PR, phase P0), and the Bug B regression suites (`run_bugb_suite`, `run_bugb_revision_suite`, `run_bugb_ancestry_suite`), which become acceptance tests here (§8.2).

---

## 0. Problem and constraints

**Bug B.** A COW branch reads its ancestors through `tl_walk_next()` (`pagestore_core.c:7362`). That walk caps each ancestor level by `branch_lsn` only, and every ancestor hop passes `seq_cap = 0` (`read_through_checked`, `fork_nblocks_through`, `fork_exists_through`, `fork_block_death_through`, `read_resolve_version`: all use `seq_cap = w.lsn == read_lsn ? read_seq : 0`). So anything the parent admits after the fork at a position ≤ `branch_lsn` is visible to the branch:

- A hint-bit-only page rewrite under an unchanged `pd_lsn = p ≤ L` wins over the version the branch saw, because it has a larger `admission_seq` at the same LSN (`page_visible()`, `:5544`).
- A fork event (TRUNCATE/UNLINK/ZEROEXTEND/CREATE) whose `req_lsn` lands at or below L changes the branch's size or existence.

This matters for branches in particular. A branch reuses XID numbers the parent later commits under a different meaning, so post-fork hint bits are corruption for it, not just staleness.

**Why not promotion.** In #294 the LSN served both as the branch-visibility coordinate and as the parent's version-order coordinate. Moving it to fix the first kept breaking the second (V4–V7, G1, G2). This design never moves an admitted record. Parent newest reads keep today's `(lsn, admission_seq)` order bit for bit.

**Why not a pure 2D cap** (`lsn ≤ L ∧ seq ≤ S`). Honest late admissions are real WAL history below the cap that reaches the store after the fork. Examples: a buffer dirty at fork time with `pd_lsn ≤ L` that is evicted later, and a smgr request in flight at fork time. They must stay visible. `pagestore_test.c:4631` ("branch sees parent as-of branch LSN, not later writes") pins this: block 5 is written at LSN 1200 after the branch at 1500 and must be visible to the branch. The same holds for #294's `rel100` check (a genuine first admission exactly at L is visible).

**Goal.** A read-side rule, parameterized by a per-view `cap_seq`. It must:
1. hide same-position rewrites admitted after the view froze;
2. keep honest first admissions visible;
3. be *stable*: once a view has resolved a position, later admissions never change that resolution;
4. be retainable: pruning can keep, per fence, exactly what the rule can select.

---

## 1. Semantics

### 1.1 Positions

A **position** is the unit at which "rewrite" is defined:

| Object | Position | Rationale |
|---|---|---|
| Relation page version | `(timeline level, key, block, lsn)` where `lsn = pd_lsn` of the bytes | The bytes self-certify their WAL position. Two versions at one position share the same WAL-logged state and differ only in unlogged state (hint bits, VM/FSM unlogged bits). |
| Object version (SLRU / CONTROL / SLRU_LIVE/TOMB/WM / READER_SNAPSHOT / ARTIFACT) | `(level, key, block, version)`, where `version` is the caller's WAL LSN (`append_page_impl` `:16907-16944`) | Same shape. The label is caller-supplied, see §1.5. |
| Fork event, **META class**: SET (CREATE/TRUNCATE), DEAD (UNLINK), GROW from ZEROEXTEND | `(level, key, lsn)`. All META events at one LSN share one position. | These are logical operations. Two of them at one LSN are either retries/redo duplicates or a stale-labelled mutation colliding with history. |
| Fork event, **PAGE class**: GROW derived from a page append (plain `FEV_GROW` from `fork_grow_apply`, `FEV_SEG_GROW*` markers) | `(level, key, lsn, block = nblocks-1)`. It mirrors the page version it came from and is **never seq-filtered** (see 1.4). | It has to agree with page visibility. |
| Inert markers (`FEV_SEG_COMMIT*`, `FEV_SEG_ID`, `FEV_MIGRAT*`, `FEV_SNAPSHOT_BASE`) | n/a | They have no size effect, so visibility is irrelevant to the fold. |

### 1.2 View caps and their composition

A view is identified by a cap triple, carried per ancestry level:

```
ViewCap { uint64 lsn;          /* L: positions above are invisible            */
          uint64 seq;          /* S: the freeze point (PS_SEQ_UNBOUNDED = ∞)  */
          uint64 strict_seq; } /* X: at position == lsn, require seq ≤ X      */
```

- **Branch edge** `(L_e, S_e)`: INCLUSIVE, so `X = ∞`.
- **Retention pin / exact-R reader** `(R, S_r)`: STRICT at R, so `X = S_r`. This keeps today's tuple fence at R (READ_CONSISTENCY_DESIGN §1d).
- **Newest read**: `(∞, ∞, ∞)`.
- **Seq-less as-of read** (`req_seq == 0`): `(R, ∞, ∞)`. This is today's semantics, deliberately unchanged.

**Composition** when the walk crosses edge `(L_e, S_e)` into the parent. This is the single source of truth. The read walk (`tl_walk_next`), retention projection (`retention_project_lsn`), the frontier gates, and all fence builders must call this one function:

```
lsn'        = min(lsn, L_e)
seq'        = min(seq, S_e)
strict_seq' = (lsn' == lsn) ? strict_seq : ∞   /* a lower edge unbinds the pin's R boundary */
```

Taking the min over seqs is exact. The rule of 1.3 admits `{seq ≤ S} ∪ {first}`, and the intersection of two such sets is `{seq ≤ min(S1,S2)} ∪ {first}`.

### 1.3 The rule (one level, no cross-level effects)

For a view level with cap `(L, S, X)` and a position `p`:

- Let `V(p)` be the versions or events at `p`.
- Let `s_min(p) = min seq in V(p)`, where legacy seq 0 counts as 0.
- Let `S'(p) = (p == L) ? min(S, X) : S`.

`v ∈ V(p)` is **admissible** iff

```
p ≤ L  ∧  (p < L ∨ v.seq ≤ X)  ∧  ( v.seq ≤ S'(p)  ∨  v.seq == s_min(p) )
```

Equivalently, the per-position effective seq cap is `c(p) = max(S'(p), s_min(p))`, except that under STRICT at `p == L` the `s_min` escape is disabled.

- **Pages.** Pick the admissible version with the greatest `(lsn, seq)`. In words: take the newest position `p* ≤ L` that has any admissible version. At `p*`, take the newest version with `seq ≤ S` if one exists, otherwise the first arrival.
- **Fork events.** The hop state is the fold (`FORK_HOP_*`, `nblocks`, fence size) over admissible events in `(lsn, seq)` order. PAGE-class GROWs are always admissible when `lsn ≤ L` (and, at `p == L` under STRICT, when `seq ≤ X`).
- **Legacy seq 0** is `≤ S` for every S, as it is today.

**Equivalence at S = ∞.** Every version is `≤ S`, so the rule reduces to the current `page_visible(e, L, 0)` and the current cached prefix fold. §4.2 relies on this: a legacy branch is bit-for-bit today's behaviour.

### 1.4 Validation

| Case | Rule outcome | Correct? |
|---|---|---|
| Honest late admission: no pre-fork version at `p ≤ L`; a genuine version arrives post-fork | It is the first arrival at `p`, so it is visible. The newest position rule picks it over older positions. | Yes. The `:4631` test and `rel100` hold. |
| Leak: pre-fork version at `p`, post-fork same-position rewrite | The pre-S set at `p` is non-empty, so the newest pre-S version is picked. | Yes. `rel103`, revision-1 and the control rewrite are hidden. |
| Stale META event (TRUNCATE/UNLINK/ZEROEXTEND at 1000) where CREATE@1000 is pre-fork | The META position 1000 already has a pre-S event, so the stale one is hidden. | Yes. `rel101`, `rel105` and ancestry-5 hold. |
| Honest multi-block record: blocks 5 and 7 share `pd_lsn p`; block 5 pre-fork, block 7 flushed post-fork | Page 7 is first at `(7,p)`, so visible. Its GROW(8) is PAGE class at `(p, block 7)`, so visible. | Yes, and consistent. Had GROW been keyed per `(key, lsn)`, nblocks would be 6 while block 7 is readable, which is why the PAGE/META split exists. |
| In-flight honest ZEROEXTEND at an occupied META position | Hidden. | Acceptable: see "crash equivalence" below. |

**The ambiguity: no pre-fork version, several post-fork arrivals at one position.**

We choose the **first arrival** (minimum seq). The argument:

1. **The alternatives carry no more information.** For relation pages the label *is* the content: `hdr.lsn = page_lsn(page)` (`:16923`). Every arrival at `(block, p)` therefore has identical WAL-logged state. They differ only in unlogged state that accumulates monotonically in the writer's buffer (hint bits are only ever set, never cleared, between WAL-logged changes). The earliest admission is the earliest capture, so it carries the fewest post-freeze unlogged bits. Any later arrival carries a superset.
2. **Stability.** The first arrival at a position is immutable, because later arrivals always get larger seqs. "Newest post-fork" would drift with every hint-bit write-back. That would break requirement 3, and pruning could not retain a moving target.
3. **Universality.** The first arrival at `p` is admissible to *every* view with `L > p`, or with `L == p` under INCLUSIVE. Pruning and the fork-fold fast path both use this: "min-seq-at-position is universally visible".
4. **Can the first arrival be wrong?**
   - *Relation pages:* only through unlogged state it already carried. That is G3, which is design-independent (§7). It cannot be a stale label, because the bytes certify `p`. The clamped/WAL-less ordered path never lands at or below a fence (§3.6).
   - *Object versions and fork META events:* the label is caller-supplied. If the *first* post-fork arrival at a **fresh** position carries a stale label, it is visible, and any later honest arrival at that position is hidden. No read-side rule can fix this without provenance. Both events are post-S and carry the same label, so they are indistinguishable. Choosing "newest" instead would be wrong in the far more common rewrite shape. This residual class, a *stale label at a fresh position*, is closed at the source instead: P0 (`ls_op_lsn()`) removes the stale fallback for CREATE/TRUNCATE; UNLINK's transaction-end fallback is honest; startup redo stamps the true replay LSN, whose duplicates are hidden or are no-ops. It is listed as explicitly out of scope in §7.

**Crash equivalence (secondary safety net).** A branch at L boots from the control state at or below L and replays WAL from that checkpoint's redo up to L. Hiding an honest in-flight admission at an occupied position (for example a ZEROEXTEND or a same-LSN block flush) is therefore equivalent to that effect being lost in a crash at L:
- unlogged growth and hint bits are legitimately losable;
- WAL-logged effects after the redo point are re-applied by the branch's recovery.

This is why the rule may err toward hiding at an occupied position. It may never err toward revealing a rewrite.

### 1.5 Cross-level positions

A version or META event at an **ancestor level k** can collide with a position that the view already resolves through a *deeper* level `j > k`. The typical sources are a child-local control rewrite at an inherited version, or a child-local META event at `p ≤ branch_lsn` (#294 ancestry-1 and ancestry-2; Codex 4098328771/4098328776).

These arise because:
- the relation-page clamp (`branch_floor = branch_lsn+1`, `:16890`) covers only `PS_KLASS_RELATION`, and
- fork META events on a child are not clamped. A child's boot-time recovery legitimately replays parent WAL at `p ≤ L` onto the child.

**Definition.** "First arrival" is evaluated over the view's **merged position**: every level reachable from level k under the composed caps. A post-S candidate `v` at level k, position `p`, is admissible only if no version or META event at the same position exists at a deeper reachable level `j > k` (with `p ≤ L_j`) that has a smaller seq. Level shadowing is unchanged: if level k has any admissible candidate, the walk stops there.

- *Pages:* when the level's selection is a post-S first arrival, probe the deeper levels for a version at exactly `p` with a smaller seq. If one exists, drop the candidate and re-select at level k among the remaining admissible versions, then continue the walk as today.
- *Fork META:* a post-S, first-at-level META event collides if a deeper reachable level has a META event at the same LSN with a smaller seq. It is then hidden (§3.2).
- *Cost:* probes run only when a level's candidate is post-S. For forks, a per-fork `max_meta_seq ≤ S` gate skips this entirely.

Duplicates from a child's boot-time replay are pre-S for every view created after them, so they are never hidden by this.

### 1.6 Unstamped and ordered records (Q5 in detail in §5)

Records without a WAL position have no meaningful "first arrival at p". They are visible to a capped view iff `seq ≤ S` (and `lsn ≤ L`, which only matters for parent ordering). For a view with `S = ∞`, this is today's behaviour.

---

## 2. Which views carry a cap_seq

**Invariant I-ALLOC.** Every `admission_seq` that ends up on an indexed page version or fork event is allocated and published, meaning inserted into the in-memory index, within a single hold of that key's shard write lock, while the admission read lock is held.

Today's daemon satisfies this: `run_request_admitted()` takes `ps_lock_shard_wr(shard)` for write ops, and `append_page_impl` allocates at `:16876`. The meta ops at `:20403/20488/20553/20583` and `fork_grow` at `:6984` run inside `handle_request` under the same lock.

- P1 adds a debug assertion, a thread-local "holds shard-wr for key's shard" check, at the two allocation helpers used for indexed records.
- The non-indexed allocations (the pin barrier `:1834`, pin SET `:20950`, artifact attempt seqs) are exempt.

**Stable seq.** A seq S is *stable* if every seq ≤ S that will ever be indexed already is. Under I-ALLOC, `next_admission_seq - 1` read while holding **all** shard locks (read or write) is stable, and so is any seq allocated while holding admission-wr.

| View | cap_seq source | When / under which locks | Persistence | Recovery | Lock order |
|---|---|---|---|---|---|
| **Branch** (`branch_seq`) | `admission_seq_alloc()` inside CREATE_BRANCH. The allocated S is never used by a record, so the pre-S set is `seq < S`. | The daemon already holds admission-rd + **all shard-wr** + map-wr (`pagestore_daemon.c:766-777`). No shard-bound admission can be between allocation and publication, so S is stable. An exact idempotent retry returns the persisted S and allocates nothing. Reuse of a DELETED id allocates a fresh S. | New timeline record `TimelineRecEventV3.branch_seq` (§4.1). | Replay calls **`admission_seq_observe(branch_seq)`**. This is required: otherwise a store whose newest record predates the branch could restart with `next ≤ S`, the next post-fork record would get a seq ≤ S, and it would be classed pre-fork. | lifecycle → admission(rd) → shard(all, asc, wr) → map(wr) → meta append. Unchanged. |
| **Retention pin** (reader / owner / materializer, any resources) | `pin.admission_seq`, already allocated under `admission_write_lock()` at SET/RESERVE (`:20948-20951`) | Admission-wr drains every admission-rd holder, so it is stable. | Already durable in the retention log; already observed at open (`:22756`). | unchanged | unchanged |
| **Exact-R reader / reader snapshot** | The `(R, S)` fence from `ps_admission_barrier()` (`:1828`), published in control block 2 and sent as `req_seq` (`backend_localsvc.c` `ls_pinned_read_seq`) | Stable, since it is allocated under admission-wr. | Existing (block-2 marker plus the reserved seq in retention). | unchanged | unchanged |
| **Advancing reader** | Each adopted generation's `(R, S)` is an exact-R fence as above | Adoption swaps the whole triple atomically. | Existing | unchanged | unchanged |
| **Artifact fence** (`ArtifactFence`, `:18474`) | S_a = the admission seq of the artifact's **BEGIN** (lifecycle artifacts), or of the data version that noted the fence (legacy SLRU seeds without lifecycle) | Derived. Nothing new is sampled at `artifact_fence_reserve()`. The seq that already exists on the durable record is used, because the control era the artifact resolved was resolved before its BEGIN. | No new persistence. Add an in-memory `seq` field to `ArtifactFence`. `artifact_fence_note()` (`:18524`) takes `min(seq)` over the fence's versions. | Rebuilt from replayed versions, as the registry already is. | `artifact_fence_lock` stays a leaf after map. |
| **WAL-index progress horizon** (`walidx_progress[tl]`) | S_H = stable seq sampled at `walidx_commit()` | Sample `next_admission_seq-1` after taking all shard locks in read mode, *before* `walidx_publish_wrlock`. In the daemon, WAL_INDEX_PROGRESS already holds shard 0 in write mode (`pagestore_daemon.c:829-857`), so it takes shards 1..n-1 in read mode, ascending. Order: shard(asc) → walidx_publish → wal_lock → walidx_meta, which conforms to the global order. The cost is once per indexing batch. | `WalIdxProgressRec` v2 (`horizon_seq`) and walidx snapshot header v4 (§4.3). Legacy progress means ∞. | `admission_seq_observe(horizon_seq)` | as stated |
| **Materializer marker** (control block 3/4, `PsMaterializerMarkerFormat`) | **None.** It is a progress claim, not a frozen view. The materializer's view is its MATERIALIZER retention pin, which already carries a seq. | n/a | unchanged | unchanged | n/a |
| **Forkmeta cutover cutoff** `(cutoff_lsn, cutoff_seq)` | **Stays lexicographic.** It is the compaction boundary of the timeline's own newest history plus the "below cutoff is rejected" rule (`fork_meta_mutation_future`, `:8277`; `fork_meta_event_future` `:8700`). It is not a view. What changes is that the snapshot builder must retain what positional views need across it (§3.7). | n/a | FMS payload version bump only for the event flags (§4.3) | unchanged | unchanged |
| **Page reclamation frontier** `(F_lsn, F_seq)` | **Not a view.** `F_seq = next_admission_seq-1` at compaction publication (`:3638,3692`) is already "the stable seq the prune ran at". §3.5 reuses it as the gate for unregistered capped reads. | | unchanged | | |

---

## 3. Code touch points

### 3.1 Read path, pages

- **`page_visible()` → `page_select(e, const ViewCap *c, ...)`** (`:5544`). Two passes over `e->vers`:
  1. find `p*`, the max LSN among versions admissible by LSN/STRICT;
  2. at `p*`, take the newest with `seq ≤ S'`; otherwise take the min-seq version (ties: last append, matching `ps_page_prune_plan`'s exact-tuple rule).

  The cost is O(nver), as today. Keep a thin `page_visible(e, lsn, seq)` wrapper for the parent-side uncapped callers (`read_through(..., UINT64_MAX, 0)`, object version derivation `:16940`).
- **`TlWalk`** (`:7338-7372`) carries a `ViewCap` instead of a bare `lsn`. `tl_walk_first(tl, cap)` and `tl_walk_next()` apply the composition of §1.2. Every `seq_cap = w.lsn == read_lsn ? read_seq : 0` expression is deleted, in `read_through_checked` `:7651`, `read_resolve_version` `:17631`, `fork_nblocks_through` `:7718`, `fork_exists_through` `:7815`, `fork_block_death_through` `:7787`, `page_frontier_ancestry_allows` `:7383`, and `fork_asof_query_allowed`.
- **`read_through_checked`** / **`read_resolve_version`**: select with `page_select`, then apply the cross-level probe (§1.5) when the selected version is post-S.
- **Storage lookups must fetch by exact identity.** `ps_memtable_lookup(..., rl, seq_cap, ...)` (`:17694`) and `layer_map_lookup(..., rl, seq_cap, pv->lsn, ...)` (`:17703`) currently resolve "newest at or below (rl, seq_cap)" and then compare the result with `pv`. Under the new rule the selected version need not be the lexicographic newest, so both are called with `(pv->lsn, pv->admission_seq)`, the identity tuple, exactly as the artifact path already does (`:17644-17648`). The pgcache is already identity-keyed.
- **LSN-0 relation bytes at ancestor levels** keep failing closed (`:7663`, `:17660`). No change.

### 3.2 Read path, fork events

**In-memory additions** (no change to the persisted `ForkEvent` size: the `_Static_assert(sizeof(ForkEvent) == 40)` at `:4052` stays):
- `snapshot_dropped` becomes a flags byte:
  - bit0 `SNAPSHOT_DROPPED` (existing meaning);
  - bit1 `META` (class);
  - bit2 `META_FIRST` (min-seq META event at its LSN on this fork);
  - bit3 `UNSTAMPED`.
- `ForkEnt` gains `uint64_t max_meta_seq` (a monotone max; conservative after prune) and `uint32_t *late_meta_idx` / `nlate_meta`: indexes of META events that are **not** `META_FIRST`, maintained like `def_idx` (`fork_def_index_insert` `:5985`).
- `fork_event_insert_pos()` (`:6021`) and its callers maintain `META_FIRST`. Inserting a META event with a smaller seq than the current first at the same LSN (possible during recovery ordering) flips the old first's bit.

**`fork_asof_hop(e, const ViewCap *c, const TlWalk *deeper, uint32_t *nb)`** (`:5712`):
- **Fast path:** if `c->seq == ∞`, or `e->max_meta_seq ≤ c->seq`, no META event can be hidden. The visible set is exactly the tuple prefix `upper_bound(c->lsn, c->strict_seq)`, which is today's indexed fast path with `seq_cap := strict_seq`. The upper_bound prefix property holds precisely when the hidden set H is empty. This is the common case: no META mutation on this fork after the view froze.
- **Slow path:**
  - `H = { i : ev[i] is META ∧ ev[i].seq > S'(lsn) ∧ ev[i].lsn ≤ L ∧ (¬META_FIRST ∨ collides_deeper(i)) } ∪ { i : UNSTAMPED ∧ seq > S }`. It is computed from `late_meta_idx` plus the META-first events with `seq > S` (bounded by the number of META events).
  - If H is empty, use the fast path.
  - Otherwise let `h0 = min(H)`. Start from the cached fold at `h0-1`, fold `[h0, pos)` skipping H, and fold the STRICT run at `lsn == L` as today.
  - Cost is O(pos − h0). It runs only for forks with post-view META mutations, which are rare. The F5 FSM/VM rewrite pattern produces inert `SEG_COMMIT*` markers, which are neither META nor in H, so the F5 scaling guard (`test_fork_event_index_scaling`) must stay green.
- **`fork_inheritance_fenced()`** (after `:5921`) uses the same two paths over `cached_fence_nblocks`.
- **`fork_page_invalidated()`** (`:5881`) keeps its newest-first walk over `def_idx`, but skips events in H (same predicate). Its `seq_cap`-at-`cap` clause becomes the STRICT clause.
- **`fork_block_death_through()`** (`:7769`) skips hidden definitive events with the same predicate.
- **`fork_nblocks_through` / `fork_exists_through` / `fork_size_asof_hop`**: callers pass ViewCaps.
- **`fork_nblocks_recovery()`** (`:7738`) stays LSN-only. It is a conservative "reserve every block redo could touch" maximum, and hidden events can only enlarge it.
- **`fork_newest_visible_lsn_through()`** stays LSN-only. It is parent-side operational placement.

**GROW dedup** (`fork_size_asof_hop(fe, lsn, seq) < to_nblocks` in `fork_grow_with_seq` `:6966` and `append_page_impl` `:17078`). A PAGE-class GROW must not be skipped because of a META event that some view may hide; otherwise that view would see the page but not its size. Rule: skip the dedup, and always record the GROW, if the fork has any META event with `lsn ≤` the new GROW's LSN that is not `META_FIRST`, or that is UNSTAMPED. Such forks are rare, so the cost is negligible, and the extra GROW is redundant for newest reads.

### 3.3 Artifacts and control

- **`artifact_visible()`** (`pagestore_artifact_lifecycle.inc:93-136`) selects the COMMIT/DROP record with `page_select` under the view's cap. A replacement generation at the same LSN C, meaning a second COMMIT at C, is a same-position rewrite: views frozen before it keep the first generation. This resolves U4. The data-page interval `begin_seq < v.seq < commit.seq` is unchanged.
- **Control restore**: the image (block 0) and note (block 1) are selected independently by the rule. Same-version retries carry the same redo, so the pairing survives. P5 adds a debug check that the selected note's redo equals the selected image's.
- **Clients.** A WAL owner's or materializer's control restore must send its pin's `(R, seq)` (today it sends `req_seq = 0`, which by design keeps legacy newest-at-position semantics). This is a client change in `pagestore_control_restore.c` / `pagestore_control.c`, phase P5.
- **Deleted**, never introduced: `control_pair_follow_promotion`, the artifact-fence-lock-through-publication dance, and every `fence_promote_lsn` / `*_ancestry` collision walk from #294.

### 3.4 WAL index

- **`walidx_get()`** (`:16703`) stays LSN-capped. WAL records are identified by their LSN, so there are no same-position rewrites (duplicates are byte-identical retries). Its ancestry walk switches to the ViewCap `TlWalk` but uses only `.lsn`.
- **`PS_OP_BLOCK_DEATH`** (`:20458`) goes through `fork_block_death_through()` with the caller's `(req_lsn, req_seq)` mapped to a ViewCap. WAL-index-only consumers send their pin's or horizon's seq.
- **`walidx_plan_bases_build()`** (`:15205-15420`):
  - base selection comes from the new page planner (§3.5);
  - per-horizon death/size uses `fork_asof_hop` with the horizon's ViewCap, `(H, S_H, ∞)` for progress and the pin's cap for WAL_INDEX pins, instead of `fork_asof_hop(f, h, 0, …)` (`:15282,15296,15356`);
  - `walidx_prune_fences()` (`:18873`) returns ViewCaps instead of bare LSNs.

### 3.5 Retention: pages (and the frontier gate)

**Planner.** `ps_page_prune_plan()` (`pagestore_prune.c:7-78`) gains a fence kind. To avoid touching the persisted `PsPruneFence` used by the frontier file, add an in-memory `PsViewFence { lsn, seq, strict_seq }`. For each positional fence (branch caps, pins, walidx horizons, artifact fences), keep:
1. the rule's selection (§1.3), and
2. conservatively for cross-level effects (§1.5): the newest version with `seq ≤ S` at or below the fence (call its position `p_pre`), plus the first arrival at every position in `(p_pre, L]`.

The union covers every choice the view can make at this level, whatever the deeper-level probes decide. The extra set is the honest-late first arrivals between `p_pre` and L, which is small.
- The floor base stays `{floor, UINT64_MAX}` (it serves the timeline's own uncapped reads).
- The exact-tuple duplicate collapse is unchanged.

**Consequence: pre-S versions are no longer superseded under a fence.** Today a branch cap `(L, ∞)` keeps only the newest-seq version at the newest position ≤ L. Under `(L, S)` it keeps the pre-S version and, when the post-S rewrite is also the newest, that one too. After the branch is DELETED, the next prune pass drops it (`timeline_delete_publish_one` `:14572-14636` already re-marks `page_prune_due`).

**Also changed:**
- `prune_version_needed()` (`:18426-18460`) re-implements fence visibility. It switches to `page_select`/ViewCap and to the ViewCap form of `fork_page_invalidated`.
- `page_prune_fences()` (`:18721`) emits ViewCaps. Pins get `(projected lsn, composed seq, strict if unprojected)`. Descendants get `(projected L, composed S, ∞)`. Today's `UINT64_MAX` for branch caps becomes `branch_seq`, or ∞ for legacy branches.
- `retention_project_lsn()` (`:18166`) becomes `retention_project_cap()` and uses the §1.2 composition.

**Ordered-path fence bump** (`append_page_impl` `:17040-17075`). This is critical. The WAL-less/clamped ordered path lifts `hdr.lsn` above each fence and only bumps past a fence at equal LSN when `admission_seq <= fence.admission_seq`. With finite branch S, a fresh ordered write at exactly `L` would no longer be bumped: it would be a first arrival at `(block, L)` and leak. The ordered path must therefore treat **positional (INCLUSIVE) fences as LSN-strict**: always go to `> L`. STRICT pin fences keep today's tuple test. The same applies to any other consumer that treats `PsPruneFence.admission_seq == UINT64_MAX` as meaning "bare LSN". §8 lists the audit greps.

**Frontier gate for capped reads.** `page_frontier_allows()` (`:4650`), `page_frontier_structural_fence_active()` (`:4620`), `page_frontier_projected_fence_active()` (`:4568`):
- The structural fence matches a live descendant's **projected `(L, S)`**, not `branch_lsn` with seq 0.
- **New condition:** a level read whose finite cap seq `S < F.seq` (the prune ran after the view froze) is honoured only if its `(level, cap)` is a registered fence: structural branch, projected pin, walidx horizon, or artifact fence. Otherwise it returns -2 (history reclaimed). Reason: the prune kept the base "newest as of F.seq" at positions below `F_lsn`, which may be a post-S rewrite.
- Views with `S ≥ F.seq`, and seq-less as-of reads (`S = ∞`), behave exactly as today.

### 3.6 Retention: control

- `control_prune_fences()` (`:18783`) emits ViewCaps: pins, descendants, and artifact fences carrying `S_a`.
- `control_chain_plan()` (`:18209`) uses the positional planner. The exact-redo twin lookup (`:18329-18334`) uses the fence-selected twin, not the max-seq one.
- `control_chain_keeps()` (`:18369-18412`) currently keeps "only the newest-seq durable copy per LSN" for paired notes and fence blocks. It must also keep each positional fence's selected copy of the paired note.
- The WAL retain floor (`control_images_covered`, `wal_retain_floor`) is LSN-based and unchanged.

### 3.7 Retention: forkmeta (highest risk)

`ps_forkmeta_prune_plan_required` (`pagestore_forkmeta_prune.c:84-131`) uses a lexicographic `event_visible` (`:7-13`) and `retain_horizon` (`:24-82`: the latest SET/DEAD, the size-minimum envelope, and the largest GROW after the latest definitive event). Changes:

1. **Positional fences.** `event_visible` becomes a per-fence precomputed mask. For each fence, compute the §1.3 admissibility, including the `META_FIRST` and UNSTAMPED flags, over the fork's events at or below `L`, then run `retain_horizon` over that masked view.
2. **Uncertain events** (cross-level, §1.5). A post-S, `META_FIRST` event at `p ≤ L` may or may not be hidden depending on deeper levels. The planner does not decide. Let `r0` be the minimum LSN of such events. Run `retain_horizon` over the masked prefix `< r0` as one horizon, and retain **every** event in `[r0, L]` verbatim. This is exact under either resolution. The union-of-summaries shortcut is **not** exact: a honest-late SET after the latest pre-S definitive event with a later smaller GROW is a counterexample, worked in review notes. `r0` normally does not exist.
3. **Position closure.** If any META event at position `p` is retained, the `META_FIRST` event at `p` is retained too. Otherwise a later-seq event would become "first" after the prune and change every view's resolution at `p`.
4. **Fence filter** (`:9990-9998`): a positional fence exactly at `cutoff_lsn` is currently dropped because its seq is `UINT64_MAX`. It must be kept.
5. **Derived fences across the cutoff.** For every positional view `(L, S)` with `L ≥ cutoff_lsn` and `S < cutoff_seq`, add a derived fence `(cutoff_lsn, S, strict = cutoff_seq)`. Today the collapsed region `≤ cutoff` is summarized only for the cutoff horizon (lexicographic newest), which would fold in post-S META rewrites that the view hides. The future tail (`lsn > cutoff_lsn`, or `lsn == cutoff_lsn ∧ seq > cutoff_seq`) is retained verbatim, and closure (3) guarantees the tail's first-status at `cutoff_lsn`.
6. **WAL-index horizons** (`:10009-10020`) become ViewCaps `(H, S_H, ∞)` instead of `{lsn, 0}` wildcards.
7. **`fork_meta_snapshot_cutoff()`** (`:9482-9600`): a live branch without a frontier contributes `(branch_lsn, 1)` today. That stays valid, since it bounds the cutoff at or below every fork point and derived fences cover the rest.

### 3.8 Persistence of admission_seq (confirmed; no change needed)

- **Global and monotonic:** a single `next_admission_seq` (`:353`), allocated by CAS in `admission_seq_alloc` (`:918`). Recovery raises it by CAS-max through `admission_seq_observe` (`:934`) from segment and layer replay (`:19589`), the forkmeta log and snapshot (`:8930,8940,9275`), the cutover cutoff (`:10574`), the retention high-water mark and pins (`:22738-22756`). This design adds two more observe sources: `branch_seq` and walidx `horizon_seq`.
- **Preserved through flush and compaction:** segment headers `SegRecHdrAdmission` / `SegRecHdrBoundAdmission` (`:3917-3927`), image layer v4 `PsImgIndexEnt.admission_seq`, `compact_order_cmp` (`:2973`) sorting by `(key, block, lsn, seq)`, and `recs[].admission_seq` (`:3599`).
- **Forkmeta:** V2/V3 records carry seq, and the snapshot records carry it.
- **Legacy data:** v3 layers and 48-byte segment headers load seq 0 (legacy, treated as pre-S).
- **Nothing in production rewrites a stored seq** (per the survey). This is the property the whole design rests on. P1 adds a comment and a test assertion in the compaction merge that `admission_seq` is identity-preserving.

---

## 4. Persisted formats

### 4.1 Timeline log: `TimelineRecEventV3`

The `TLM2` log is self-sized: the loader dispatches on `rec_len` (`:10704-10707`). Add a new record shape:

```
TimelineRecEventV3 { magic TLM2, rec_len = 64, kind, id, parent, state,
                     branch_lsn, incarnation, parent_incarnation,
                     branch_seq, crc, reserved }
```

- The new layout is 64 bytes; the existing `TimelineRecEvent` is 56.
- CREATE writes V3 with `branch_seq ≠ 0`.
- STATE events carry the same `branch_seq`, and replay validates equality, as it already does for `branch_lsn` and `parent_incarnation`.
- **Replay:** V3 CREATE requires `branch_seq ≠ 0` and calls `admission_seq_observe(branch_seq)`.
- **Legacy V2/V1/TimelineRec creates** map to `branch_seq = PS_SEQ_UNBOUNDED` in memory.
- **Identity:** `ps_core_format_identities` (`:22934`) changes `{"timelines", TLM2, 2}` → `3`, so `pagestore_fixture.py --check` forces a fixture update.
- **Old binaries** reject `rec_len = 64` (`return -1`), so they fail closed.
- **Wire and inspection:** nothing changes on the wire. `TIMELINE_INFO`, `REQUIRE_BRANCH` and `CHECK_BRANCH` are untouched. Exposing `fork_seq` in the inspector is optional, deferred, and would change the exact-JSON check at `pagestore_test.c:4571`.

**Legacy branch rule: S = ∞.** For such a branch every version is pre-S, so:
- `page_select` equals the current `page_visible(…, 0)`;
- the fork fold's hidden set is empty (the fast path is the current prefix);
- the cross-level probe never triggers (it needs a post-S candidate);
- the positional prune fence `(L, ∞)` keeps "newest at or below L", which is today's keep rule, and has no uncertain events;
- the frontier gate is today's.

A legacy branch is therefore bit-for-bit equivalent to the old behaviour. Its Bug B exposure is unchanged: we cannot reconstruct the seq it was created at.
- **Why not 0 or a guess:** `S = 0` would retroactively switch an existing branch's resolutions to first arrivals, a visible change to a live branch. Any reconstructed S, such as the max seq of records at or below L at upgrade time, could only be a guess.
- **Composition:** a *new* descendant of a legacy branch has finite S on its own edge, and the min-composition freezes the whole ancestry for that descendant as of its creation. That is correct and strictly better.
- **Documentation:** document "recreate the branch to get Bug B protection".

**Fixtures** (D5 rule 2; precedent in MVP_COMPLETION_PLAN.md:1711-1730 and RELEASE_VALIDATION.md:873-890):
- demote `posix-timeline-delete-holes` (current) to `role: legacy`, restored byte-identical from `origin/pagestore`;
- capture a new current fixture containing a V3 branch plus a post-fork same-position rewrite;
- its reopen oracle asserts the frozen view;
- the legacy fixture's reopen oracle asserts the S = ∞ legacy view;
- the old binary must fail closed on the new fixture.

### 4.2 Forkmeta record and snapshot

**Records.** `ForkMetaRecV2`/V3 (`:8122`) carry `kind` in a `uint8`, and V3 uses `pad[3]` for a CRC-24. Add `FORK_META_V4_MAGIC`, with the same layout and CRC as V3, whose `kind` byte carries flags:
- `0x40` `META` (set for the ZEROEXTEND GROW; SET and DEAD imply META);
- `0x80` `UNSTAMPED`.

`fork_meta_persist()` (`:8286`) writes V4 for new records. Old binaries reject the V4 magic, so they fail closed.

**Legacy V2/V3 records:**
- SET/DEAD are META;
- a plain `FEV_GROW` is **PAGE class**, never seq-filtered. This is today's behaviour: legacy ZEROEXTENDs cannot be told apart from page-derived GROWs once they are in a snapshot, and treating them as PAGE is the equivalence-preserving choice;
- no legacy record is UNSTAMPED.

**Snapshot.** The FMS payload (`ForkMetaSnapshotPayloadHeader` `:8136`, FMS1 v1) persists events with their kind. It goes to payload version 2 so the flags survive a cutover. The cutoff and freeze tuples are unchanged.

Identities: `forkmeta` V4 and `forkmeta_snapshot` FMS1 v2. Fixtures: demote `posix-forkmeta-crc` and capture a current one that includes a flagged ZEROEXTEND and an unstamped event.

### 4.3 WAL-index progress

- `WalIdxProgressRec` (`:11966`) gets v2 with `uint64_t horizon_seq`, distinguished by `rec_len` or a new WIPG version.
- The walidx snapshot header (`walidx_snapshot_encode_header` `:14796`, WISD v3 → v4) carries `horizon_seq`, and `PsWalIdxSnapshot` gains the field.
- Legacy progress maps to ∞, which is today's LSN-only behaviour.
- Identity bump and fixture per D5.
- `WalIdxRec` itself is unchanged, since records need no seq (§3.4).

### 4.4 Unchanged

Page segments, image layers, the page and walidx frontier files, the retention log, artifact formats, control blocks and the materializer marker.

---

## 5. Unstamped fork events (`req_lsn == 0`)

**Position.** Unchanged: `fork_op_lsn()` (`:20139`) places the event at `fork_newest_visible_lsn_through()+1`, or at the cutoff after a snapshot cutover. That keeps parent newest ordering, and V5-type ordering, exactly as it is today.

**Visibility.** The event is persisted with `UNSTAMPED`. At a capped level it is admissible iff `lsn ≤ L ∧ seq ≤ S` (and `seq ≤ X` at `lsn == L`). No first-arrival escape applies: it has no WAL position, so its only time coordinate is its admission. With `S = ∞` (newest reads, seq-less as-of reads, legacy branches) this is today's behaviour.

**Why not #294's promotion of unstamped events.** Promotion needs the complete fence set, sampled race-free against every fence class, including walidx progress (V1) and artifact fences. The flag has no race: `seq` is fixed at admission, and each view's S is stable by construction (§2).

**The WAL-less/clamped ordered page path** (`SEG_WALLESS_ADMISSION_MAGIC` / `SEG_CLAMPED_ADMISSION_MAGIC`, `:17027-17075`) stays as it is, pre-existing and bound-marker-backed. Its fence bump must treat positional fences as LSN-strict (§3.5). Its GROW is PAGE class but lands above every fence, so it is invisible to existing views (#294's `rel106` holds).

---

## 6. Compatibility with the parts of #294 that are kept

### 6.1 `ls_op_lsn()` per-opcode fallback: its own PR (P0)

- Take `backend_localsvc.c` from `8e5fea3430f`: UNLINK keeps `Max(XactLastCommitEnd, XactLastAbortEnd)`; CREATE/TRUNCATE use `GetXLogInsertRecPtr()`, or `GetXLogReplayRecPtr()` during recovery; the startup process uses the replay pointer.
- Remove the comment sentence that refers to daemon-side promotion.
- It is independent of the read-side design, and it closes the main *stale label at a fresh position* source, which the read-side rule cannot close (§1.4.4).
- Roughly 60 lines, including the comment. No store-side test exists; add a regression to `integration_test.sh` if feasible, or state it as untested in the PR per the review-conclusion rule.

### 6.2 Acceptance suites

Port the three suites from `d504a7af748` into the redesign PR unchanged, except as noted below.

**`run_bugb_suite`.** Every check holds under the new rule. The rule predicts each outcome:

| Check | Why it holds |
|---|---|
| rel100 visible at L | First arrival, INCLUSIVE at L. Also on the grandchild. |
| rel101 (ZEROEXTEND at stale 1000) | Non-first META at 1000 → hidden. |
| rel102 (ZEROEXTEND with `req_lsn` 0) | UNSTAMPED, post-S → hidden. |
| rel103 (hint-bit rewrite) | The pre-S version is selected, on the branch and on the grandchild. |
| rel104 (WAL-less rewrite) | `p* = 1000 > 0`. |
| rel105 (stale TRUNCATE) | Non-first META → hidden. |
| rel106 (zero EXTEND) | The ordered path lands above the fence. |
| Pinned reader at 1200 sees 0x10 | **Adjust.** The check reads with `req_seq 0`, which is a seq-less as-of read with legacy semantics by design (§1.2). Pass the pin's seq, as the production reader does. |
| Control, branch as-of 1500 | Holds. The branch edge S freezes the control position. |

Also: **add** the parent newest checks the suite lacks. The root newest must see rel103=0x30, control=0x77, rel101/102 at 3 blocks, and rel105 truncated. They prove the "parent unchanged" half of the contract.

**`run_bugb_revision_suite`:**
- **1** (pin at 3000, rewrite at 2500): holds. The pin is positional below R, which is a change from today (U2).
- **2** (WAL owner at 3000, control rewrite at 2000): **adjust** `read_control_at(0, 3000)` to send the MATERIALIZER pin's seq. This is the §3.3 client change. Without a seq it is a legacy as-of read.
- **3** (sibling caps, then the newest read after branch deletion): holds trivially, since parent order is `(lsn, seq)`.
- **4** (CREATE retry idempotence): holds via the pre-existing `fork_has_create_at()` (`:6008`), because no promotion interferes.

**`run_bugb_ancestry_suite`:**
- **1** and **2**: hold via the cross-level rule (§1.5). These are its acceptance tests.
- **3** and **4**: hold, because retries are idempotent at the original LSN.
- **5** (WAL_INDEX pin plus stale ZEROEXTEND): holds (non-first META, pin seq). **Adjust** it to pass the pin's seq if the helper sends 0.

**Tests that are not carried over:**
- `test_promoted_control_collision_follows_its_pair` (`pagestore_control_prune_test.c`) asserts promotion mechanics (`image_ver > 2500`, `note_ver == image_ver`). Replace it with: the note and the image each keep version 2000; the artifact fence at 2500 with `S_a` selects the pre-fence copy; `wal_floor == 1800`.
- The forkmeta cutover round-3 rejection (explicit TRUNCATE/UNLINK below the cutoff are rejected) holds as is: `fork_meta_mutation_future` is untouched.

---

## 7. Audit items

| Item | Resolution |
|---|---|
| **V1** (promotion samples walidx progress without serializing against commit) | **By construction.** Nothing is promoted. `S_H` is a stable seq sampled at `walidx_commit()` under all shard locks (§2). |
| **V2/V3** (ancestry collision walks do not stop at the first definitive hop / shadowing version) | **By construction.** No admission-time ancestry walks exist. The read walk keeps today's "first definitive hop ends the walk" and level shadowing. The cross-level probe (§1.5) only runs *inside* the level whose candidate is post-S and compares exact positions. |
| **V4** (key-level newest floor) | **By construction.** No floor exists. |
| **V5** (a same-position rewrite family loses ordering) | **By construction.** Parent order is the untouched `(lsn, seq)`. Clear B at 1000 with the larger seq wins newest reads. Retention after the branch is deleted is the plain keep rule. |
| **V6** (redo TRUNCATE at its original LSN) | **By construction.** Redo stays at its LSN with a fresh seq. Parent newest behaves as before #294: pages at a greater LSN are not invalidated (`fork_page_invalidated` breaks on `v.lsn < page.lsn`). For branches, a redo duplicate at a pre-S position is a hidden non-first META. |
| **V7** (retry of a promoted event) | **By construction.** Retries land at the original position and are idempotent (`fork_has_create_at`) or hidden duplicates. |
| **U1** (relation pages vs WAL-index-only readers) | **Needs work (P5).** WAL-index horizons and WAL_INDEX pins become positional ViewCaps. The base selection in `walidx_plan_bases_build` uses the same planner. Death/size use `fork_asof_hop` with the horizon cap. For WAL-index-only owners of the parent's own history, post-horizon hint bits are same-history and MVCC-harmless, so no stricter handling is needed. |
| **U2** (exact-R reader window) | **By construction, with a semantic change.** Pins become positional below R and STRICT at R. A post-pin rewrite at `p < R` is hidden (revision-1). A seq-less as-of read is deliberately unchanged. |
| **U3** (fork event between walidx snapshot plan and publish) | **Collisions: by construction**, since `S_H` is fixed and post-`S_H` META collisions are hidden. **Residual:** an honest late fork event at a *fresh* position at or below the horizon, admitted between plan and publish (for example an UNLINK after commit that follows indexing past the commit end). This is inherent to honest late arrivals. **Needs extra work, as a separate small PR:** a per-timeline `fork_event_admit_seq` watermark. The planner records it; publication, under `walidx_publish`, aborts and re-plans if any fork event with `lsn ≤ max horizon` and `seq >` the plan watermark was admitted. |
| **U4** (same-version SLRU rewrite at a fenced C) | **By construction.** A replacement COMMIT at the same C is a same-position rewrite, and artifact fences carry `S_a` (§3.3). |
| **U5** (counter-versioned klasses vs LSN caps) | **Out of scope, guarded.** All 8 current klasses use `pd_lsn` or caller WAL LSNs (`:16921-16929`). The `cur->lsn + 1` counter branch (`:16930-16943`) is reachable only for unknown klasses. P1 adds an assertion that capped reads never resolve a counter-versioned klass: it fails closed. If one is ever added, its natural rule is seq-only (`seq ≤ S`), like UNSTAMPED. |
| **G1** (forkmeta provenance lost after pruning) | **By construction.** No provenance is needed: nothing depends on an original-LSN event persisting except first-status, which the prune preserves by closure (§3.7.3). |
| **G2** (newest floor overrides genuinely newer events) | **By construction.** No floor exists. |
| **G3** (parent buffer dirty at fork with `pd_lsn ≤ F`, written late with post-fork hint bits) | **Out of scope for the store; control-plane mitigation specified.** The first arrival carries whatever unlogged bits the buffer had when written. **Mitigation:** cut branches at a *completed checkpoint's redo pointer* (L = redo(C), S sampled after C completes). The checkpoint flushed every buffer with `pd_lsn < redo` before S. Later hint-only rewrites at those positions are same-position and hidden. WAL-logged changes after redo have `pd_lsn > L`. With `wal_log_hints`/checksums the first post-checkpoint hint emits an FPI (`pd_lsn > L`). Document this as a branch-creation requirement in MVP_COMPLETION_PLAN. Alternative (PG-side, deferred): scrub `HEAP_XMIN/XMAX_COMMITTED/INVALID` hints for XIDs ≥ the branch's `nextXid` at L on read-through. |
| **Residual: stale label at a fresh position** | **Out of scope**, closed at the source by P0 (§1.4.4). Document it as a known limit of any read-side rule without provenance. |

---

## 8. Test plan

### 8.1 Unit tests (core, no daemon)

- **`page_select` table tests:** pre-S present; post-S only (first arrival); multiple post-S (min seq, with the tie rule); STRICT at L (a post-X first arrival is invisible, so fall back to an older position); legacy seq 0; `S = ∞` equals `page_visible(…, 0)` over randomized arrays (differential).
- **ViewCap composition:** a table over 3-level chains with legacy (∞) edges, pin STRICT unbinding when a lower edge caps, and edge `L == R`.
- **Fork fold:** extend `ps_test_fork_event_index_selftest` with random META/PAGE/UNSTAMPED mixes and random caps. Compare the fast and slow paths against a reference fold implementing §1.3 literally. Keep the F5 scaling guard (`test_fork_event_index_scaling`) and add a variant with one post-view META event to bound the slow path.
- **`META_FIRST` maintenance under out-of-order recovery insertion.**
- **Cross-level:** ancestry-1/2 in-process equivalents, plus the negative case where child-local boot-replay duplicates are pre-S and remain visible.
- **GROW dedup:** a hidden-META fork followed by an honest late page GROW means the view's nblocks covers the page.

### 8.2 Planner property tests

These go in `pagestore_prune_test.c` and `pagestore_forkmeta_prune_test.c`.
- For random version/event histories, random positional and STRICT fences, and a random floor: for every fence, `select(kept) == select(all)` and `fold(kept masked) == fold(all masked)`. This is **the** guard against read/prune mismatch.
- Also under both resolutions of every uncertain event (§3.7.2), and with a random cutoff plus derived fences (§3.7.5).
- Named cases (from the prune survey):
  - `(20,1)/(20,5)` under `(20, S=3)` keeps `(20,1)`;
  - only post-S versions at P means the min seq is kept;
  - a fence above P uses P's selection, not P's newest;
  - after the fence is deleted, `(20,1)` becomes collectable;
  - closure: the retained META at p implies the first META at p is retained.

### 8.3 Crash / restart

- **`branch_seq`:** survives a clean and a crash restart. An exact CREATE_BRANCH retry returns the same S. A reused id gets a new S. **`next_admission_seq > branch_seq` after reopening a store whose newest record predates the branch.** This is the observe regression and must fail without it.
- **Legacy timelines log:** reopening yields ∞ and today's reads (the legacy fixture oracle).
- **Forkmeta:** V4 flags survive the log, a crash, a snapshot cutover and reopen. A legacy V3 GROW stays PAGE class.
- **`horizon_seq`:** survives a WIPG replay and a snapshot cutover.
- **Fault points:** `pagestore_fault_points.def` covers the V3 timeline append (torn or failed), which poisons as today.

### 8.4 Compaction / prune integration

`pagestore_test.c`, `pagestore_lifecycle_prune_test.c`, `pagestore_control_prune_test.c`, `pagestore_forkmeta_cutover_test.c`:
- the frozen view survives forced compaction, forkmeta cutover and walidx snapshot (the four-phase `check_bugb_state` pattern, extended with a cutover and a walidx snapshot);
- after the branch is deleted, pre-S versions are reclaimed (bounded-space soak stays green);
- an unregistered capped read with `S < F.seq` gets -2;
- a registered one succeeds.

### 8.5 Op fuzzer (`test/pagestore-op-fuzz`)

**Finding.** With `PAGESTORE_FUZZ_BUGB_WORKAROUND` unset, the fuzzer's next parent op after a fork is stamped with the *pre-append* `wal_end`, which equals `L`. That is a **first** admission at exactly L, which the new rule, like #294 and the Codex review, keeps visible. So `verify_branch_frozen` would still fail. The oracle also reads the **parent** as-of L with `req_seq 0`, which is a legacy seq-less read that *should* still see same-position rewrites. Required changes:

1. **LSN convention:** `ship_wal()` returns the record's **end** LSN, matching PG's `pd_lsn` convention. A record shipped after the fork then lands at a position greater than L. Retire the workaround env var.
2. **The oracle reads the branch:** READV, READ_AT(L), NBLOCKS and EXISTS on the branch timeline, compared against the frozen model. The parent is read with `(L, branch S)` only if a test hook exposes S.
3. **New adversarial actions**, each with a model:
   - (a) hint-bit rewrite: re-write an existing parent block with an unchanged `pd_lsn` after a fork → hidden;
   - (b) delayed honest write: a parent write at a fresh position ≤ L whose WAL was shipped before the fork but admitted after → visible (update the frozen model);
   - (c) stale-label META at an occupied position → hidden;
   - (d) unstamped META → hidden;
   - (e) a grandchild branch (currently only timeline 0 is ever a parent) to cover composition;
   - (f) a same-version control rewrite → hidden for the branch;
   - (g) an explicit compaction/cutover action.
4. With the workaround removed, `pagestore_op_fuzz` in meson is part of acceptance. Seeds: the default plus 20 random seeds in the PR's validation data, with `_SHRINK` on any failure.

### 8.6 System

`integration_test.sh` and `mvp_golden_test.sh` (exact-redo, control pairing), plus the endurance driver in one live run. The endurance live run is still subject to E-6; note the interaction in the PR.

---

## 9. Implementation phases, sizes and risks

Each phase is an independently reviewable PR, or a commit series within one PR. Nothing observable changes before P3.

| Phase | Content | Size (code + tests) |
|---|---|---|
| **P0** | `ls_op_lsn()` per-opcode fallback (from #294) | ~60 + 0 |
| **P1** | Refactor with no behaviour change. `ViewCap` and the composition function; `TlWalk` carries caps; `page_select` and `page_visible` wrapper; identity-exact memtable/layer lookups; `ForkEvent` flag byte, `META_FIRST`, `late_meta_idx`, `max_meta_seq`; `fork_asof_hop`, `fork_inheritance_fenced`, `fork_page_invalidated`, `fork_block_death_through` with the slow path and cross-level probe; I-ALLOC assertions; U5 assertion. Every cap is still `S = ∞`, so behaviour is identical. Includes the differential and unit tests (§8.1). | ~800 + ~500 |
| **P2** | Planner. `PsViewFence`; positional and STRICT modes in `ps_page_prune_plan`; the forkmeta prune mask, uncertain-event handling, closure and derived fences; `prune_version_needed`; `control_chain_keeps` and twin lookup; fence builders emit ViewCaps (still ∞); the ordered-path LSN-strict bump. Includes property tests (§8.2). | ~600 + ~600 |
| **P3** | **Activate branches.** `TimelineRecEventV3`, `branch_seq` allocation in CREATE_BRANCH, retry and reuse, replay plus observe; identity v3 and fixtures; branch edges carry S in reads, fences, projections and the frontier gate (structural `(L,S)` and the unregistered-read rule). Reads, prune and gate must flip in one commit, because reads without matching retention silently select wrong after compaction. Ports `run_bugb_suite` and the ancestry suite. | ~400 + ~500, plus fixtures |
| **P4** | Forkmeta V4 flags: `META` on ZEROEXTEND, `UNSTAMPED`; the FMS v2 snapshot; UNSTAMPED visibility; the GROW-dedup rule; identities and fixtures. Makes rel101, rel102 and ancestry-5 pass. | ~350 + ~350, plus fixtures |
| **P5** | Other views. Pins become positional below R (reader semantics change; release note); the walidx `horizon_seq` (WIPG v2, WISD v4, stable-seq sampling, observe) and walidx planner caps; artifact fence `S_a`; the client sends the pin seq for owner control restore; ports the revision suite. | ~450 + ~400, plus fixtures |
| **P6** | Fuzzer (§8.5); docs: READ_CONSISTENCY_DESIGN §1d, MVP_COMPLETION_PLAN (the fork tuple is now real; G3 branch-at-checkpoint requirement), RELEASE_VALIDATION (the format table), MVP_STATUS. | ~450 + docs |
| **Follow-up** | U3 plan-epoch validation. | ~150 + ~150 |

Total: roughly 3.1k lines of code and 2.9k lines of tests, plus fixtures and docs.

**Highest-risk parts, in order:**

1. **Read/prune divergence.** The rule is implemented four times: `page_select`, the fork fold, `ps_page_prune_plan`, and `ps_forkmeta_prune_plan_required`. Any mismatch is a *silent* wrong selection after compaction, not an error. Mitigation: a single composition function, a single admissibility predicate shared by the fold and the planner masks, and the §8.2 property tests as merge blockers.
2. **Forkmeta positional retention** (§3.7): uncertain events, closure, and derived fences across the cutoff. It is the most novel logic and interacts with the F3 and F5 history, orphan adoption, and the cutover rejection rules.
3. **Missing `admission_seq_observe`** for new cap sources (`branch_seq`, `horizon_seq`). This produces a post-fork record classified as pre-fork after a restart. P3 and P5 each carry a dedicated regression.
4. **The ordered-path fence bump** and any other code that reads `admission_seq == UINT64_MAX` as meaning "bare LSN fence". Audit greps: `UINT64_MAX` next to `fences[`, `PsPruneFence`, `retention_project_lsn`, `admission_seq = projected`.
5. **The walidx `horizon_seq` stable sampling.** It adds a multi-shard lock acquisition to the WAL_INDEX_PROGRESS path. Lock order is shard(asc) → walidx_publish → wal_lock → walidx_meta. Verify with the concurrent stress/soak suites and a TSAN run.
6. **Semantic change for pinned readers below R** (P5). Existing tests may encode the old lexicographic rule. Every changed expectation must be justified in the PR's review-conclusion comment.
7. **Three persisted-format bumps** (timelines, forkmeta plus FMS, WIPG plus WISD), each needing a legacy/current fixture pair under D5.
