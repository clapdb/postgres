# Branch snapshot seq cap: a read-side same-position freeze for Bug B

Status: design proposal, revision 2 (addresses the PR #297 Codex round 1 findings 4100849461/4100849476/4100849489/4100849491 and the P0 finding on PR #296, comment 4100769750). For review before any implementation.
Baseline: `origin/pagestore` @ `0550146156d`. Line references are to that tree.
Supersedes: the admission-time LSN promotion in PR #294 (`d504a7af748`), which was abandoned on 2026-09-25.
Keeps from #294:
- the per-opcode `ls_op_lsn()` fallback (its own PR, phase P0, now with the `+1` of §6.1);
- the child-first, ancestry-aware CREATE-retry idempotence check (`fork_newest_definitive_event_through()`, §6.2). It is independent of promotion and needed by ancestry-3;
- the Bug B regression suites (`run_bugb_suite`, `run_bugb_revision_suite`, `run_bugb_ancestry_suite`), which become acceptance tests here (§6.2).

### Revision 2 changes

| Area | Change | Section |
|---|---|---|
| Cross-level visibility | The deeper-level probe is replaced by an *inherited-range* rule, which is stateless and immune to pruning. | §1.5 |
| GROW dedup | Page GROW dedup is per fork: `max_meta_seq > S_min`. The doc argues why an unrecorded GROW cannot matter to a later-created view, and gives the cost. | §3.2 |
| Artifact fences | `S_a` binds to the committed attempt's `begin_seq`. | §2, §3.3 |
| G3 | Hint-WAL is required, plus a cut at a checkpoint redo. | §7 |
| Prune closure | Position closure applies to page, control and forkmeta prune, including invalidated first arrivals. | §3.5–§3.7 |
| Stale labels | Stale-label handling at a fresh position is re-scoped (P0 is a mitigation), and there is a new analysis of equality at the cut. | §1.4, §5, §7 |

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
3. be *stable*: once a view has resolved a position, no later admission, prune, compaction or restart changes that resolution;
4. be retainable: pruning can keep, per fence, exactly what the rule can select, **including the facts the rule depends on** (the first arrival `s_min(p)` of each reachable position).

---

## 1. Semantics

### 1.1 Positions

A **position** is the unit at which "rewrite" is defined:

| Object | Position | Rationale |
|---|---|---|
| Relation page version | `(timeline level, key, block, lsn)` where `lsn = pd_lsn` of the bytes | The bytes self-certify their WAL position. Two versions at one position share the same WAL-logged state and differ only in unlogged state (hint bits, VM/FSM unlogged bits). |
| Object version (SLRU / CONTROL / SLRU_LIVE/TOMB/WM / READER_SNAPSHOT / ARTIFACT) | `(level, key, block, version)`, where `version` is the caller's WAL LSN (`append_page_impl` `:16907-16944`) | Same shape. The label is caller-supplied; the equality edge is analysed in §5.3. |
| Fork event, **META class**: SET (CREATE/TRUNCATE), DEAD (UNLINK), GROW from ZEROEXTEND | `(level, key, lsn)`. All META events at one LSN share one position. | These are logical operations. Two of them at one LSN are retries or redo duplicates, or a stale-labelled mutation colliding with history. |
| Fork event, **PAGE class**: GROW derived from a page append (plain `FEV_GROW` from `fork_grow_apply`, `FEV_SEG_GROW*` markers) | `(level, key, lsn, block = nblocks-1)`. It mirrors its page version and is **never seq-filtered**. | It has to agree with page visibility (§1.4). |
| Inert markers (`FEV_SEG_COMMIT*`, `FEV_SEG_ID`, `FEV_MIGRAT*`, `FEV_SNAPSHOT_BASE`) | n/a | They have no size effect. |

### 1.2 View caps and their composition

```
ViewCap { uint64 lsn;          /* L: positions above are invisible            */
          uint64 seq;          /* S: the freeze point (PS_SEQ_UNBOUNDED = ∞)  */
          uint64 strict_seq; } /* X: at position == lsn, require seq ≤ X      */
```

- **Branch edge** `(L_e, S_e)`: INCLUSIVE, so `X = ∞`.
- **Retention pin / exact-R reader** `(R, S_r)`: STRICT at R, so `X = S_r`. This keeps today's tuple fence at R (READ_CONSISTENCY_DESIGN §1d).
- **Newest read**: `(∞, ∞, ∞)`.
- **Seq-less as-of read** (`req_seq == 0`): `(R, ∞, ∞)`. This is today's semantics, deliberately unchanged.

**Composition** when crossing edge `(L_e, S_e)` into the parent. This is the single source of truth, used by `tl_walk_next`, `retention_project_*`, the frontier gates and all fence builders:

```
lsn'        = min(lsn, L_e)
seq'        = min(seq, S_e)
strict_seq' = (lsn' == lsn) ? strict_seq : ∞
```

Taking the min over seqs is exact. Each rule admits `{seq ≤ S} ∪ {first}`, and the intersection of two such sets is `{seq ≤ min(S1,S2)} ∪ {first}`.

### 1.3 The rule

For a view level `k` with cap `(L, S, X)` and a position `p`:

- Let `V(p)` be the versions or events at `p` on level `k`.
- Let `s_min(p)` be the minimum seq in `V(p)`, with legacy seq 0 counting as 0.
- Let `S'(p) = (p == L) ? min(S, X) : S`.
- Let `B_k` be `branch_lsn` of timeline `k`, or `−∞` for the root.

Position `p` is **escape-eligible** at level `k` iff `p > B_k` and not (STRICT and `p == L`).

`v ∈ V(p)` is **admissible** iff

```
p ≤ L  ∧  (p < L ∨ v.seq ≤ X)  ∧
( v.seq ≤ S'(p)  ∨  (escape-eligible(k, p) ∧ v.seq == s_min(p)) )
```

In words, the per-position effective seq cap is `c(p) = max(S'(p), s_min(p))` at escape-eligible positions, and `S'(p)` otherwise.

- **Pages.** Pick the admissible version with the greatest `(lsn, seq)`. That is, take the newest position `p* ≤ L` that has any admissible version. At `p*`, take the newest version with `seq ≤ S` if one exists, otherwise the first arrival.
- **Fork events.** The hop state is the fold (`FORK_HOP_*`, `nblocks`, fence size) over admissible events in `(lsn, seq)` order. PAGE-class GROWs are always admissible when `lsn ≤ L` (and when `seq ≤ X` at `lsn == L` under STRICT).
- **Legacy seq 0** is `≤ S` for every S.

**Equivalence at S = ∞.** Every version is `≤ S`, so the rule reduces to the current `page_visible(e, L, 0)` and the current cached prefix fold. Legacy branches (§4.1) are bit-for-bit today's behaviour.

### 1.4 Validation

| Case | Rule outcome | Correct? |
|---|---|---|
| Honest late: no pre-fork version at `p ≤ L` (`p > B_k`); a genuine version arrives post-fork | It is the first arrival, so it is visible. | Yes. The `:4631` test and `rel100` pass. |
| Leak: pre-fork version at `p`, post-fork same-position rewrite | The newest pre-S version is selected. | Yes. `rel103`, revision-1 and the control rewrite are hidden. |
| Stale META at 1000 while CREATE@1000 is pre-fork | The post-S non-first META is hidden. | Yes. `rel101`, `rel105` and ancestry-5 pass. |
| Multi-block record: blocks 5 and 7 share `pd_lsn p`; 5 is pre-fork, 7 is flushed post-fork | Page 7 is first at `(7,p)`. Its GROW(8) is PAGE class, so it is visible. | Yes, and consistent. Keying GROW per `(key,lsn)` would have hidden it. |
| In-flight honest ZEROEXTEND at an occupied META position | Hidden. | Acceptable by crash equivalence (below). |
| Child-local post-S write at `p ≤ B_child` (ancestry-1/2) | Not escape-eligible, so hidden (§1.5). | Yes. |

**The ambiguity: no pre-fork version, several post-fork arrivals at one position.** We choose the **first arrival** (minimum seq):

1. **The later arrivals carry no more information.** For relation pages the label *is* the content (`hdr.lsn = page_lsn(page)`, `:16923`). Every arrival at `(block, p)` has identical WAL-logged state and differs only in unlogged state, which accumulates monotonically in the writer's buffer. The earliest admission therefore carries the fewest post-freeze bits.
2. **Stability.** The first arrival is immutable: later arrivals always have larger seqs. That holds *provided retention never removes it while another version at `p` survives* (§3.5–§3.7 closure).
3. **Universality.** The first arrival at an escape-eligible `p` is admissible to every view with `L > p` (or `L == p` INCLUSIVE).
4. **Can the first arrival be wrong?**
   - *Relation pages:* only through unlogged state it already carried (G3, closed by §7's branch-creation requirement).
   - *Object versions and fork META events:* the label is caller-supplied, so a first arrival at a fresh position with a *synthetic* label that is ≤ an existing cut would be visible. No read-side rule can distinguish it from an honest late arrival. This residual is handled by giving synthetic labels a position strictly after every existing cut: P0's `+1`, with a precise form in P4 (§5.2). P0 alone is a mitigation, not a proof. §5.3 audits every other label source for the equality edge at the cut.

**Crash equivalence (safety net).** A branch at L restores control state at or below L and replays WAL from that checkpoint's redo to L. Hiding an honest in-flight admission at an occupied position is equivalent to losing it in a crash at L. Unlogged growth and hint bits may be lost, and WAL-logged effects are re-applied by the branch's own recovery. So the rule may err toward hiding at an occupied position. It must never err toward revealing a rewrite.

### 1.5 Cross-level positions: the inherited range

Revision 1 hid a post-S candidate at level `k` if a *deeper* level held the same position with a smaller seq. That made visibility depend on another timeline's retained history. The deeper version can legitimately be pruned when the view's fence at the deeper level selects a newer position. After that, the level-`k` candidate would become visible, which breaks requirement 3. This revision replaces the probe with a local, stateless rule.

**Inherited-range rule.** On a branch timeline `k`, positions `p ≤ B_k` are *inherited history*: their WAL belongs to the parent's stream. The first-arrival escape is disabled there (the `p > B_k` term in §1.3), so a local version or META event at `p ≤ B_k` is admissible to a capped view only if `seq ≤ S`.

Why this is sound:
- **No leak.** Relation pages on a branch are already clamped to `> B_k` (`branch_floor`, `:16890`). Other local writes at `p ≤ B_k` fall into three groups:
  - boot-time replay and seeding of parent history (control images from recovery, SLRU seeds at a cutoff `C ≤ B_k`, re-applied fork events). These happen during branch preparation and first boot, before any descendant or pin of `k` exists, so they are pre-S for every view of `k`;
  - later re-applications of the same history (crash recovery, retries), which are duplicates;
  - stale labels.

  Hiding the post-S ones is therefore never a loss of genuinely new state.
- **Honest late arrivals** at `p > B_k` keep the escape.
- **The root** has `B = −∞`, so the rule is unchanged there.
- **It is local:** it depends only on `B_k`, which is immutable, and on `k`'s own versions. Retention at other levels cannot change it.
- **Ancestry-1** (tl1-local control rewrite at 1500 ≤ `B_tl1` = 2000) and **ancestry-2** (tl1 TRUNCATE at 1500) are hidden from tl2. After tl1 closes, tl1's own newest reads still see them (S = ∞).

### 1.6 Unstamped records

Records without a WAL position (§5) have no meaningful first arrival. They are admissible to a capped view iff `lsn ≤ L ∧ seq ≤ S` (and `seq ≤ X` at `lsn == L`). With `S = ∞` this is today's behaviour.

---

## 2. Which views carry a cap_seq

**Invariant I-ALLOC.** Every `admission_seq` that ends up on an indexed page version or fork event is allocated and published, meaning inserted into the in-memory index, within one hold of that key's shard write lock, under admission-rd.
- Daemon paths satisfy this: `run_request_admitted()` takes shard-wr for write ops; allocation happens at `:16876`, `:6984`, `:20403/20488/20553/20583`.
- P1 adds a debug assertion at the indexed-record allocation helpers.
- Non-indexed allocations are exempt: the barrier (`:1834`), pin SET (`:20950`), and artifact attempt tokens.

**Stable seq.** A seq is stable if every smaller seq that will ever be indexed already is. `next_admission_seq - 1` read while holding all shard locks is stable, and so is any seq allocated under admission-wr.

| View | cap_seq source | When / locks | Persistence | Recovery | Lock order |
|---|---|---|---|---|---|
| **Branch** (`branch_seq`) | `admission_seq_alloc()` inside CREATE_BRANCH. The allocated S is used by no record, so the pre-S set is `seq < S`. | The daemon already holds admission-rd + all shard-wr + map-wr (`pagestore_daemon.c:766-777`), so S is stable. An exact retry returns the persisted S. Reuse of a DELETED id allocates a fresh S. | `TimelineRecEventV3.branch_seq` (§4.1) | **`admission_seq_observe(branch_seq)`** on replay. This is required: without it, a store whose newest record predates the branch could restart with `next ≤ S` and misclassify post-fork records as pre-fork. | unchanged |
| **Retention pin** | `pin.admission_seq`, allocated under admission-wr (`:20948-20951`) | stable | existing retention log; observed at open (`:22756`) | unchanged | unchanged |
| **Exact-R / advancing reader** | `(R, S)` from `ps_admission_barrier()` (`:1828`), sent as `req_seq` | stable | existing | unchanged | unchanged |
| **Artifact fence** | **`S_a` = `begin_seq` of the committed attempt** at that cutoff (details below) | bound at COMMIT | already persisted: `PsArtifactLifecycle.begin_seq` in the completion record (`pagestore_artifact_format.h:47`, lifecycle record version 2). **No format change.** | rebuilt from replayed completion records | `artifact_fence_lock` stays a leaf after map |
| **WAL-index progress horizon** | `S_H` = stable seq sampled in `walidx_commit()` | Sampled with all shards held. WAL_INDEX_PROGRESS already holds shard 0 in write mode; it takes shards 1..n-1 in read mode, ascending, before `walidx_publish_wrlock`. | `WalIdxProgressRec` v2 and WISD v4 (§4.3); legacy is ∞ | `admission_seq_observe(S_H)` | shard(asc) → walidx_publish → wal_lock → walidx_meta |
| **Materializer marker** | none (a progress claim; the materializer's view is its MATERIALIZER pin) | – | unchanged | – | – |
| **Forkmeta cutover cutoff** | stays lexicographic. It is a compaction boundary with a "below the cutoff is rejected" rule, not a view (§3.7). | – | FMS v2 only for event flags | – | – |
| **Page frontier** `(F_lsn, F_seq)` | not a view. `F_seq` (`:3638,3692`) gates unregistered capped reads (§3.5). | – | unchanged | – | – |

**Artifact fence cap rule.**
- **Entries.** An `ArtifactFence` entry (`:18474`, keyed `(timeline, lsn=C)`) holds a small set of caps instead of the LSN-only fence:
  - `committed`: the `begin_seq` read from the durable COMMIT record at C (lifecycle `state == PS_ARTIFACT_COMMITTED`);
  - `inflight`: the token of the current unfinished attempt (`fork->artifact_attempt_seq`), added at BEGIN and at the data-append `artifact_fence_reserve()` (`:16971`), both under the artifact key's shard lock.
- **Transitions.**
  - A successful COMMIT moves its token from `inflight` to `committed`.
  - A superseding BEGIN at the same C, a DROP of an unfinished attempt, or a restart removes the old `inflight` token. Unfinished pre-restart attempts cannot commit (ARTIFACT_LIFECYCLE.md "Publication" §3), so recovery rebuilds only `committed` caps, from replayed completion records.
  - **Failed attempts never contribute.** The revision-1 rule `min(seq over versions)` is removed.
- **Several committed caps.** A committed generation is immutable per C. A DROP followed by re-publication at the same C keeps the dropped generation's cap until the existing fence release (`artifact_fence_release` / `artifact_fence_forget`), so an entry can briefly hold two committed caps. Control prune retains the selection for each cap.
- **Producer ordering requirement** (checked in P5). The artifact producer resolves the control era it embeds with a capped read `(C, token)`: `req_seq = token` on its control-image read after BEGIN. The fence cap `begin_seq` then preserves exactly the copy the producer resolved, even if a control rewrite lands between BEGIN and the read. A producer that resolved before BEGIN with a seq-less read is also covered, because any rewrite before BEGIN has `seq < begin_seq`. P5 audits `pagestore_slru.c` / the reader-snapshot producer for this.
- **Legacy SLRU seeds** without lifecycle records use `S_a = ∞` (today's LSN-only fence).

---

## 3. Code touch points

### 3.1 Read path, pages

- **`page_visible()` → `page_select(e, const ViewCap *c, uint64 B_k, ...)`** (`:5544`). Two passes over `e->vers`:
  1. find `p*`;
  2. at `p*`, take the newest with `seq ≤ S'`, else the min-seq version if `p*` is escape-eligible (ties go to the last append, matching `ps_page_prune_plan`'s exact-tuple rule), else step to the next lower position.

  The cost is O(nver). `page_visible(e, lsn, seq)` stays as a thin wrapper for uncapped callers (`read_through(..., UINT64_MAX, 0)`, object-version derivation `:16940`).
- **`TlWalk`** (`:7338-7372`) carries a `ViewCap` plus the level's `B_k`. `tl_walk_next()` applies §1.2. Every `seq_cap = w.lsn == read_lsn ? read_seq : 0` is deleted, in `read_through_checked` `:7651`, `read_resolve_version` `:17631`, `fork_nblocks_through` `:7718`, `fork_exists_through` `:7815`, `fork_block_death_through` `:7787`, `page_frontier_ancestry_allows` `:7383`, and `fork_asof_query_allowed`.
- **Storage lookups fetch by exact identity.** `ps_memtable_lookup` (`:17694`) and `layer_map_lookup` (`:17703`) are called with `(pv->lsn, pv->admission_seq)`, as the artifact path already does (`:17644-17648`). The selected version need not be the lexicographic newest.
- **LSN-0 relation bytes** at ancestor levels keep failing closed (`:7663`, `:17660`).

### 3.2 Read path, fork events

**In-memory additions** (`_Static_assert(sizeof(ForkEvent) == 40)` at `:4052` stays):
- `snapshot_dropped` becomes a flags byte:
  - bit0 `SNAPSHOT_DROPPED` (existing);
  - bit1 `META`;
  - bit2 `META_FIRST` (min-seq META at its LSN on this fork);
  - bit3 `UNSTAMPED`.
- `ForkEnt` gains:
  - `uint64_t max_meta_seq` (monotone max over META and UNSTAMPED events ever inserted, never lowered by prune);
  - `late_meta_idx` / `nlate_meta` (indexes of non-`META_FIRST` META events, maintained like `def_idx`, `:5985`).
- `fork_event_insert_pos()` (`:6021`) and its callers maintain `META_FIRST`. An insert with a smaller seq at the same LSN (recovery ordering) flips the old first's bit.

**`fork_asof_hop(e, const ViewCap *c, uint64 B_k, uint32_t *nb)`** (`:5712`):
- **Fast path.** Used when `c->seq == ∞` or `e->max_meta_seq ≤ c->seq`. No META or UNSTAMPED event can be hidden, so the visible set is the tuple prefix `upper_bound(c->lsn, c->strict_seq)`. This is today's indexed fast path, and the upper_bound prefix property holds exactly when the hidden set H is empty.
- **Slow path.**
  - `H = { i : META(i) ∧ seq_i > S'(lsn_i) ∧ lsn_i ≤ L ∧ (¬META_FIRST(i) ∨ lsn_i ≤ B_k) } ∪ { i : UNSTAMPED(i) ∧ seq_i > S'(lsn_i) }`.
  - If H is empty, use the fast path. Otherwise fold from the cached state at `min(H)−1`, skipping H, plus the STRICT run at `lsn == L` as today.
  - The cost is O(pos − min(H)), and it applies only to forks with post-view META mutations. The F5 FSM/VM pattern produces inert markers only, so `test_fork_event_index_scaling` must stay green.
- The same predicate is used by `fork_inheritance_fenced()`, `fork_page_invalidated()` (skip hidden events; the old `seq_cap`-at-`cap` clause becomes the STRICT clause) and `fork_block_death_through()` (`:7769`).
- `fork_nblocks_recovery()` (`:7738`) and `fork_newest_visible_lsn_through()` stay LSN-only. They are parent-side and conservative.

**GROW dedup** (addresses 4100849461). Dedup happens in `fork_event_add()` (`:6096-6098`, which covers both live `fork_grow_apply` and recovery replay), in the `segment_grows` decision in `append_page_impl` (`:17098`, persisted as SEG_GROW vs SEG_COMMIT for ordered records), and in `fork_grow_with_seq` (`:6966`). Each one skips a PAGE-class GROW(n)@(p, g) when the hop-local fold at `(p, g)` is already ≥ n. For a view that hides a META event which contributed to that fold, the page would be readable beyond nblocks. Revision 1 missed META_FIRST events that are hidden by the inherited-range rule.

- **Which views are at risk.** Only views `V` with `S_V < g` that reach this timeline. Proof that later-created views are safe: a view created after the GROW's admission has a stable `S' ≥ g`. Every event in the dedup prefix has `seq ≤ g ≤ S'`, so every one is admissible to it; hiding applies only to `seq > S'`. Its hop fold therefore includes the whole prefix and covers n. Events admitted *later* at `lsn < p` affect the parent's newest fold in the same way and are a pre-existing, design-independent hazard, unchanged here. **So an unrecorded GROW never matters to a view created after it, and dedup need not be dropped entirely.**
- **Rule.** Let `S_min(tl)` be the minimum finite seq over live views that reach timeline `tl`, or ∞ if there are none. Those views are: descendants with finite `branch_seq`, PAGE_HISTORY/WAL_INDEX pins on `tl` or its descendants (composed), and the WAL-index horizon `S_H` of `tl` and of its descendants (composed). Artifact fences are excluded: they cover control/SLRU keys, which have no META events. A PAGE-class GROW may be deduped **only if `fork.max_meta_seq ≤ S_min(tl)`**. Otherwise it is always recorded.
  - If `max_meta_seq ≤ S_min`, every META event in the prefix has `seq ≤ S_min ≤ S_V` for every live view, so it is admissible to all of them, and the dedup's fold is every view's fold at this level.
  - PAGE-class GROWs in the prefix are always admissible.
  - This is a strict refinement of the review decision ("no dedup on fenced timelines"). A fenced timeline dedups only on forks with no META mutation since its oldest live view froze. If reviewers prefer, the blanket form (`S_min(tl) == ∞`) can be substituted with no other change.
- **Maintenance.** `S_min(tl)` is a per-timeline atomic, recomputed on registration and release of branches, pins and walidx horizons.
  - Each registration excludes concurrent relation appends: CREATE_BRANCH holds all shard-wr; pin SET holds admission-wr; `walidx_commit` samples `S_H` holding all shards. So no append can read a stale `S_min` for a view whose S is below its own g.
  - Releases may lag, which is conservative.
- **Recovery.** Replay evaluates the rule against the registry state at replay time: the timelines log and retention registry are loaded before segment/layer replay, and P3 verifies that order. This is sound in both directions. A view that existed at admission but not at replay no longer needs the GROW. A view registered after admission has `S ≥ g`, per the argument above.
- **Deferred mode** (`fork_restore_later_page_growth`, `:7036-7125`) already bypasses dedup, and stays as is.
- **Cost.**
  - The extra in-memory GROWs (40 B each) come from non-extending page writes on forks with a META mutation (CREATE/TRUNCATE/UNLINK/ZEROEXTEND) after the oldest live view froze. In practice that means every relation that bulk-extends (ZEROEXTEND) while a branch, pin or WAL-index horizon older than that extension is live.
  - Durable cost: plain records add nothing, because the segment is the durability. Ordered records persist SEG_GROW instead of SEG_COMMIT, which is the same size.
  - Plain GROWs are not compacted in memory today (`fork_event_compact_dropped_markers` drops only inert markers), so P4 extends it to drop plain PAGE-class GROWs that the just-published forkmeta snapshot dropped. §3.7 retains every GROW a live fence needs. That bounds memory by the cutover cadence. Recovery may re-derive dropped GROWs from segments or layers. That is harmless: a re-added GROW is either redundant for every live fence or sits below a later definitive event.
  - Estimate: about 40 B × (non-extending writes between cutovers on affected forks). For example, 10k page writes/s with a 60 s cutover cadence is at most about 24 MB.

### 3.3 Artifacts and control

- **`artifact_visible()`** (`pagestore_artifact_lifecycle.inc:93-136`) selects the COMMIT/DROP record with `page_select` under the view's cap. The data interval `begin_seq < v.seq < commit.seq` is unchanged. A same-C re-publication after DROP is a same-position rewrite, which resolves U4.
- **Control restore.** The image (block 0) and note (block 1) are selected independently. Same-version retries carry the same redo. P5 adds a debug check that the selected note's redo matches the selected image.
- **Clients.** A WAL owner's or materializer's control restore sends its pin's `(R, seq)` (`pagestore_control_restore.c` / `pagestore_control.c`, P5). The artifact producer reads control with `(C, token)` (§2).
- **Deleted, never introduced:** `control_pair_follow_promotion`, the artifact-fence lock held through publication, and all `fence_promote_lsn` / `*_ancestry` collision walks from #294.

### 3.4 WAL index

- `walidx_get()` (`:16703`) stays LSN-capped: WAL records have no same-position rewrites.
- `PS_OP_BLOCK_DEATH` (`:20458`) maps `(req_lsn, req_seq)` to a ViewCap.
- `walidx_plan_bases_build()` (`:15205-15420`):
  - base selection comes from the new page planner (§3.5);
  - per-horizon death and size use `fork_asof_hop` with the horizon's ViewCap (`(H, S_H, ∞)`, or the WAL_INDEX pin's composed cap) instead of `fork_asof_hop(f, h, 0, …)` (`:15282,15296,15356`);
  - `walidx_prune_fences()` (`:18873`) returns ViewCaps.

### 3.5 Retention: pages (and the frontier gate)

**Planner.** `ps_page_prune_plan()` (`pagestore_prune.c:7-78`) gains an in-memory `PsViewFence { lsn, seq, strict_seq, inherited_below }`. `inherited_below` is `B` of the level being pruned. The persisted `PsPruneFence` of the frontier file is untouched. The keep set is the union of:

1. **the floor base and future tail**, as today (`{floor, UINT64_MAX}`);
2. **per fence, the rule's selection (§1.3).** It is one version. The revision-1 "(p_pre, L] first arrivals" superset is removed, because the inherited-range rule removed cross-level uncertainty;
3. **position closure (new; addresses 4100849491).** For every position `p` where step 1 or 2 keeps any version, if `p` is escape-eligible for *some* finite-S positional fence (`p ≤ L_f`, `p > inherited_below`, and not the STRICT boundary), also keep the **min-seq version at `p`**. That version defines `s_min(p)`. If it were dropped, the next arrival would become "first" and change the resolution of every view that uses the escape at `p`.

   Closure is idempotent: it adds only the min-seq version, which is itself a first arrival.

   Positions where nothing else is kept may disappear entirely. That cannot change a view: a view that selected `p` is a fence whose selection is kept by step 2, unless step 4 drops it (see below).

4. **`prune_version_needed()`** (`:18426-18460`) may still drop a kept version that is fork-invalidated for every horizon it serves, now evaluated per fence ViewCap with the §3.2 hidden-event predicate. **It must not drop a version that closure (3) requires.** Invalidation hides bytes; it does not remove the position's first-arrival fact.
   - Codex's example: a first page P1@(b,p), a same-p truncate, then a rewrite P2@(b,p). The branch selects P1, which the truncate invalidates, so the result is empty. The floor selects P2, which is valid. Dropping P1 would make P2 the first arrival, so the branch would later read P2's bytes.
   - Under closure, P1 is kept because P2 is kept at `p`.
   - The result is re-checked by the property test "an invalidated first arrival still defines `s_min`" (§8.2).

**Consequence: pre-S versions are no longer superseded under a fence.** They become collectable once their fence is released. `timeline_delete_publish_one` (`:14572-14636`) already re-marks `page_prune_due`, and pin/artifact release already marks due.

**Also changed:**
- `page_prune_fences()` (`:18721`) emits ViewCaps: pins as `(projected lsn, composed seq, strict if unprojected)`, descendants as `(projected L, composed S, ∞)`, with `inherited_below = B` of the level being pruned. Today's `UINT64_MAX` becomes `branch_seq` (∞ for legacy).
- `retention_project_lsn()` (`:18166`) becomes `retention_project_cap()` (§1.2).

**Ordered-path fence bump** (`append_page_impl` `:17040-17075`). Positional (INCLUSIVE) fences must be treated as LSN-strict, so the write always lands at `> L`. With finite S, today's `admission_seq <= fence.admission_seq` test would leave a fresh ordered write at exactly L as a leaking first arrival. STRICT pin fences keep the tuple test. Audit greps are listed in §9.

**Frontier gate.** `page_frontier_allows()` (`:4650`), `…_structural_fence_active()` (`:4620`), `…_projected_fence_active()` (`:4568`):
- the structural fence matches a descendant's projected `(L, S)`;
- a finite-S level read with `S < F.seq` is honoured only for a registered fence (structural, projected pin, walidx horizon, artifact fence), otherwise it gets -2;
- `S ≥ F.seq` and seq-less reads are unchanged.

### 3.6 Retention: control

- `control_prune_fences()` (`:18783`) emits ViewCaps: pins, descendants, and artifact fences with *each* of their caps (§2).
- `control_chain_plan()` (`:18209`) uses the positional planner, including closure (§3.5.3). The exact-redo twin lookup (`:18329-18334`) uses the fence-selected twin, not the max-seq one.
- **`control_chain_keeps()`** (`:18369-18412`) currently keeps only the newest-seq durable copy per LSN for paired notes and fence blocks. That is the same "current state" pruning that destroys `s_min`. It must keep:
  - each positional fence's selected copy;
  - the min-seq copy at every retained LSN that is escape-eligible for some fence (closure).
- The WAL retain floor (`control_images_covered`, `wal_retain_floor`) is unchanged.

### 3.7 Retention: forkmeta (highest risk)

`ps_forkmeta_prune_plan_required` (`pagestore_forkmeta_prune.c:84-131`) uses a lexicographic `event_visible` (`:7-13`) and `retain_horizon` (`:24-82`). Changes:

1. **Positional fences.** Per fence, precompute the §1.3 admissibility mask (META/PAGE class, `META_FIRST`, UNSTAMPED, inherited range `≤ B`) over events at or below `L`, and run `retain_horizon` over the masked view. The inherited-range rule makes this exact: there are no "uncertain" events, and the revision-1 "retain `[r0, L]` verbatim" clause is removed.
2. **Position closure.** If any META event at LSN `p` is retained and `p` is escape-eligible for some fence, the `META_FIRST` event at `p` is retained too. This includes when it is currently superseded, or invalidated for the uncapped horizon. The same holds for UNSTAMPED events: an UNSTAMPED event hidden from some fence (`seq > S`) is retained whenever a later event at the same LSN is retained, so `max_meta_seq` and the fold stay reproducible.
3. **GROW retention.** `retain_horizon` over each fence's mask already keeps the largest PAGE-class GROW after that fence's latest *visible* definitive event. This is what the §3.2 dedup rule and the in-memory GROW compaction rely on. The property tests assert it explicitly.
4. **Fence filter** (`:9990-9998`): a positional fence exactly at `cutoff_lsn` must be kept, not dropped as `seq == UINT64_MAX`.
5. **Derived fences across the cutoff.** For every positional view `(L, S)` with `L ≥ cutoff_lsn` and `S < cutoff_seq`, add a derived fence `(cutoff_lsn, S, strict = cutoff_seq)`. The tail beyond the cutoff is kept verbatim; closure preserves first-status at `cutoff_lsn`.
6. **WAL-index horizons** (`:10009-10020`) become `(H, S_H, ∞)` ViewCaps.
7. **`fork_meta_snapshot_cutoff()`** (`:9482-9600`) is unchanged.
8. **Restart reproducibility.** After a cutover and reopen, `META_FIRST` and `max_meta_seq` are recomputed from the retained events. Closure (2) makes `META_FIRST` identical. `max_meta_seq` may *decrease* if every post-view META at a position was pruned together with its position. That is safe, because the fold of every live fence is preserved by (1). The §3.2 dedup rule then sees the correct remaining set.

### 3.8 Persistence of admission_seq (confirmed; no change needed)

- **Global and monotonic:** a single `next_admission_seq` (`:353`); `admission_seq_alloc` (`:918`) allocates by CAS; `admission_seq_observe` (`:934`) raises it by CAS-max.
- **Observe sources today:** segment/layer replay (`:19589`), the forkmeta log and snapshot (`:8930,8940,9275`), the cutover cutoff (`:10574`), and the retention high-water mark and pins (`:22738-22756`).
- **New observe sources:** `branch_seq` and `S_H`.
- **Preserved** through segment headers (`:3917-3927`), image layer v4, the compaction merge (`:2973`, `:3599`), and forkmeta V2/V3 records and snapshots.
- **Legacy data** loads as 0, which is pre-S.
- **Nothing in production rewrites a stored seq.** P1 adds an identity-preservation assertion in the merge.

---

## 4. Persisted formats

### 4.1 Timeline log: `TimelineRecEventV3`

```
TimelineRecEventV3 { magic TLM2, rec_len = 64, kind, id, parent, state,
                     branch_lsn, incarnation, parent_incarnation,
                     branch_seq, crc, reserved }
```

- The existing `TimelineRecEvent` is 56 bytes; the loader dispatches on `rec_len` (`:10704-10707`).
- CREATE writes V3 with `branch_seq ≠ 0`. STATE events carry it, and replay validates equality.
- Replay calls `admission_seq_observe(branch_seq)`.
- Legacy creates map to `PS_SEQ_UNBOUNDED`.
- The identity `{"timelines", TLM2, 2}` becomes `3` (`:22934`).
- Old binaries reject `rec_len = 64`.
- There is no wire change.

**Legacy branch rule: S = ∞.** A legacy branch is bit-for-bit today's behaviour: `page_select` = `page_visible(…,0)`, an empty hidden set, the positional prune fence `(L, ∞)` equal to today's keep rule, and closure inactive (it needs a finite S). Its Bug B exposure is unchanged, because the seq it was created at cannot be reconstructed. `S = 0` would retroactively change a live branch, and a reconstructed S would only be a guess. A new descendant of a legacy branch has finite S on its own edge, and min-composition freezes the whole ancestry for it. Documentation will say: recreate the branch to get Bug B protection.

**Fixtures** (D5 rule 2; precedent in MVP_COMPLETION_PLAN.md:1711-1730 and RELEASE_VALIDATION.md:873-890):
- demote `posix-timeline-delete-holes` to `role: legacy`, restored byte-identical;
- capture a current fixture with a V3 branch and a post-fork same-position rewrite; its reopen oracle asserts the frozen view;
- the legacy oracle asserts S = ∞;
- the old binary fails closed on the new fixture.

### 4.2 Forkmeta record and snapshot

- Add `FORK_META_V4_MAGIC` (the same layout and CRC-24 as V3). Its `kind` byte carries flags: `0x40` META (set on the ZEROEXTEND GROW; SET/DEAD imply META) and `0x80` UNSTAMPED.
- Legacy V2/V3: SET/DEAD are META; a plain `FEV_GROW` is PAGE class, which is today's behaviour and the equivalence-preserving choice; nothing is UNSTAMPED.
- The FMS payload goes from FMS1 v1 to v2, carrying the flags. The cutoff and freeze tuples are unchanged.
- Identities: forkmeta V4 and FMS1 v2. Fixtures: demote `posix-forkmeta-crc`; the new current fixture includes a flagged ZEROEXTEND and an unstamped event.

### 4.3 WAL-index progress

- `WalIdxProgressRec` v2 gets `horizon_seq`. The WISD snapshot header goes from v3 to v4, and `PsWalIdxSnapshot` gains the field.
- Legacy progress is ∞.
- Identity bump plus fixture.
- `WalIdxRec` is unchanged.

### 4.4 Unchanged

Page segments, image layers, the frontier files, the retention log, **artifact formats**, control blocks and the materializer marker. The artifact `S_a` reuses `PsArtifactLifecycle.begin_seq`, which lifecycle record v2 already persists.

---

## 5. Unstamped and synthetic-label fork events

### 5.1 Daemon side (`req_lsn == 0`)

- **Position:** `fork_op_lsn()` (`:20139`), which is `max(fork_newest_visible_lsn_through()+1, req_floor_lsn)`, or the cutoff after a cutover. `req_floor_lsn` is new (§5.2) and 0 for legacy clients. Parent newest ordering is unchanged.
- **Persisted** with `UNSTAMPED`.
- **Visibility:** seq-only (§1.6).
- **The WAL-less/clamped ordered page path** (`:17027-17075`) is unchanged, except that the fence bump is LSN-strict for positional fences (§3.5).

### 5.2 Client side: WAL-less CREATE/TRUNCATE/ZEROEXTEND (supersedes the revision-1 claim that P0 closes the source)

**The problem.** The P0 fallback stamps a WAL-less CREATE/TRUNCATE with `GetXLogInsertRecPtr()`, and `ls_zeroextend` (`backend_localsvc.c:977-996`) always does so outside recovery. When WAL is idle, that value equals a branch cut taken at the current insert position, so an op issued *after* the cut lands exactly at `L` as a first arrival and leaks (PR #296, comment 4100769750). P0 therefore changes to `GetXLogInsertRecPtr() + 1`.

- **What the +1 buys.** Every live view's L was cut at or below the insert position at its creation. So `I+1` is strictly above every existing cut, and the op is invisible by LSN to all coexisting views. WAL record ends are MAXALIGNed, so `I+1` never collides with a real record end. A later cut `≥ I+8` includes the op; a later cut at exactly `I` (WAL still idle) excludes it.
- **The cost.** A pre-fork WAL-less op while WAL is idle is invisible to a branch cut at the same idle position. That affects only unlogged relations, which branch boot resets from their init fork, so there is no observable effect.
- **Why P0 alone is a mitigation, not the proof.** The label is still synthetic, and correctness rests on the client computing it.

**The precise form, in P4.**
- The client sends WAL-less fork ops as **unstamped**: `req_lsn = 0`, with a new `req_floor_lsn = GetXLogInsertRecPtr() + 1` (`GetXLogReplayRecPtr() + 1` in recovery).
- The daemon places them at `max(fork_op_lsn(), req_floor_lsn)` and persists them `UNSTAMPED`.
- For coexisting views they are seq-only (hidden by seq, independent of the label). For later-created views they are LSN-positioned by the floor.
- **Why the floor is required, and why bare `req_lsn = 0` is not enough.** With bare `req_lsn = 0` the daemon places the op at `newest_visible+1`, which can be far below its real time. A view created *later* at a past L' in `[newest+1, I)` would have `S' > seq` and `L' ≥ lsn`, so it would see an op that happened after L'. The seq-only rule protects only views that already existed; the floor protects later ones.
- `ls_op_lsn()`'s `+1` fallback and `ls_zeroextend`'s insert-pointer stamp are removed in the same P4 commit that adds UNSTAMPED persistence and `req_floor_lsn`. The client and daemon must land together: a new client with an old daemon would get bare `req_lsn = 0` placement. P4 gates this on the protocol version in `PS_SHM_VERSION`.
- Stamped ops are unchanged: ops with their own WAL record (`XactLastRecEnd != 0`) and startup-process replay (`GetCurrentReplayRecPtr`).

### 5.3 Equality at the cut for other label sources

A label that is a real WAL record end `E` is exact under equality. A cut at `L = E` includes that record, so the effect belongs to the branch. A synthetic label (the insert pointer) is exact only if it carries no state newer than the WAL at that pointer.

| Source | Label | Equality edge? | Conclusion |
|---|---|---|---|
| UNLINK at end of transaction (`ls_op_lsn` UNLINK fallback) | `Max(XactLastCommitEnd, XactLastAbortEnd)` = the end of this transaction's own commit or abort record. Pending deletes run in the same (sub)transaction, and subtransaction aborts that dropped files have an xid, so the abort record sets `XactLastRecEnd`. | Equality is the correct inclusion: a cut at E includes the commit or abort, so the drop is part of the branch. | **No edge.** Keep it. |
| Control images, WAL-driven updates | the record end (`UpdateControlFile` callers; `xlog.c:4669-4677`), clamped up to `minRecoveryPoint` | none | **No edge.** |
| Control images, record-less state transitions | `GetXLogInsertRecPtr()`: `DB_SHUTDOWNING` (`xlog.c:7654`) and promotion (`xlog.c:6818-6820`, after the end-of-recovery record) | Yes: a cut at the idle insert position taken *before* the transition can select the transitioned image. | **Benign.** Only `ControlFile->state` differs, and branch restore never trusts it: `archive_bootstrap` sets `minRecoveryPoint` and startup produces its own end-of-recovery state (`pagestore_control_restore.c:688-715`). P5 adds a restore-side assertion that no field other than `state` differs between such an image and its predecessor at the same version. If one ever does, the caller switches to `+1`. |
| SLRU live mirror on the writer | `GetXLogInsertRecPtr()` (`pagestore_slru.c:3514`); on redo, the replayed record end | Yes, formally. | **Benign.** The mirrored bytes are a capture of WAL-driven SLRU state: clog, multixact and commit_ts changes follow their WAL record. So a capture stamped `I` contains no effect of any record after `I`, and with WAL idle there is no newer state. The one record-less SLRU, pg_subtrans, is reset at startup and must not be mirrored; P5 audits this. |
| SLRU seeds / reader snapshots | the proven cutoff `C`; the content is as-of C by construction | none | **No edge.** |
| Relation pages | the real `pd_lsn`; WAL-less pages go through the ordered path above fences | none | **No edge.** |

---

## 6. Compatibility with the parts of #294 that are kept

### 6.1 `ls_op_lsn()` per-opcode fallback: its own PR (P0, PR #296)

- UNLINK keeps `Max(XactLastCommitEnd, XactLastAbortEnd)`.
- CREATE/TRUNCATE without their own WAL use `GetXLogInsertRecPtr() + 1`, or `GetXLogReplayRecPtr() + 1` in recovery (§5.2).
- The startup process uses the replay pointer.
- This is a mitigation. P4 replaces the `+1` with unstamped + `req_floor_lsn`.
- About 60 lines.

### 6.2 Acceptance suites

**Also kept from #294:** `fork_newest_definitive_event_through()` from `d504a7af748` (child-first; the first visible definitive hop ends the walk) as the CREATE-retry idempotence check. Without it, a tl1 retry of an *inherited* CREATE@1500 persists a local zero-block SET on tl1. That hides the inherited page from tl1's own newest reads (a pre-existing parent-side bug that has nothing to do with promotion), and ancestry-3 fails. It goes into P3.

**`run_bugb_suite`:**

| Check | Outcome |
|---|---|
| rel100 | first arrival, INCLUSIVE at L. Also on the grandchild. |
| rel101 (ZEROEXTEND at stale 1000) | non-first META → hidden |
| rel102 (ZEROEXTEND with `req_lsn` 0) | UNSTAMPED, post-S → hidden |
| rel103 | the pre-S version is selected (on the branch and on the grandchild) |
| rel104 | `p* = 1000` |
| rel105 | non-first META → hidden |
| rel106 | the ordered write lands above the fence |
| Pinned reader at 1200 | **Adjust:** pass the pin's seq, since a seq-less as-of read keeps legacy semantics. |
| Control, branch as-of 1500 | holds |

**Add** the parent newest checks: rel103=0x30, control=0x77, rel101/102 at 3 blocks, rel105 truncated.

**Revision suite:**
- **1** holds (the pin is positional below R).
- **2**: **adjust** to send the MATERIALIZER pin's seq.
- **3** holds (parent `(lsn, seq)` order).
- **4** holds via `fork_has_create_at()`.

**Ancestry suite:**
- **1** and **2** hold via the inherited-range rule (§1.5).
- **3** holds via the kept child-first idempotence check.
- **4** holds (a local definitive event takes precedence).
- **5** holds (non-first META). **Adjust** it to pass the pin's seq if the helper sends 0.

**Not carried over:** `test_promoted_control_collision_follows_its_pair`, which asserts promotion mechanics. Replace it with: the note and image stay at 2000; the artifact fence with `S_a` = the committed attempt's `begin_seq` selects the pre-rewrite copy; `wal_floor == 1800`. The cutover round-3 rejection holds unchanged.

---

## 7. Audit items

| Item | Resolution |
|---|---|
| **V1** | **By construction.** Nothing is promoted, and `S_H` is stable-sampled (§2). |
| **V2/V3** | **By construction.** There are no admission-time ancestry walks, and the inherited-range rule (§1.5) is local. |
| **V4** | **By construction.** No floor exists. |
| **V5** | **By construction.** Parent order is `(lsn, seq)`, untouched. |
| **V6** | **By construction.** Redo keeps its LSN. Parent newest behaves as before #294, and branch duplicates are hidden non-first META events. |
| **V7** | **By construction.** Retries are idempotent or hidden duplicates. |
| **U1** | **Needs work (P5).** WAL-index horizons and pins become positional ViewCaps; the planner and fold use them. |
| **U2** | **By construction, with a semantic change.** Pins are positional below R and STRICT at R. |
| **U3** | Collisions are resolved by construction. A fresh-position honest late event between plan and publish remains. It is handled by a follow-up plan-epoch check: a per-timeline `fork_event_admit_seq` watermark, re-planning on change. |
| **U4** | **By construction.** Same-C re-publication is a same-position rewrite, and `S_a` is the committed attempt's `begin_seq`. |
| **U5** | **Out of scope, guarded.** All current klasses use real LSNs. Capped reads assert against counter-versioned klasses. |
| **G1** | **By construction.** No provenance is needed. First-status is preserved by closure (§3.5–§3.7). |
| **G2** | **By construction.** No floor exists. |
| **G3** | **Closed by a branch-creation requirement** (below). |
| **Stale/synthetic label at a fresh position** | Handled by P0 `+1` (mitigation), then by P4's unstamped + `req_floor_lsn` (§5.2). The other label sources are audited in §5.3. |

**G3: parent hint bits written after the cut.** The first arrival of a page with `pd_lsn ≤ L` carries whatever unlogged bits its buffer had. For a branch, hints about XIDs committed after L are corruption. G3 is closed iff both of the following hold:

- **(a) Hint-bit WAL logging on the parent** (`XLogHintBitIsNeeded()`: data checksums enabled or `wal_log_hints = on`) was in effect from `redo(C)` through the completion of checkpoint C.
- **(b) The cut is a checkpoint redo pointer:** `L = redo(C)` for a checkpoint C that completed before CREATE_BRANCH.

Proof sketch:
- After `redo(C)` is established, `RedoRecPtr = L`. With (a), the first hint-only dirtying of any buffer whose `pd_lsn ≤ L` emits an FPI (`XLogSaveBufferForHint`, since `pd_lsn ≤ RedoRecPtr`). The page's `pd_lsn` moves above L.
- So every page version with `pd_lsn ≤ L`, whenever it is admitted, contains only hints set before `redo(C)`, and those are about commits at or before L.
- Pages dirtied before `redo(C)` were flushed by C before it completed, and therefore before S. Later same-position rewrites are hidden by the rule.
- Without (a), a buffer can be hint-dirtied after `redo(C)` with no FPI. Without (b), pages with `pd_lsn ∈ (redo(C_prev), L]` can be hint-dirtied after L with no FPI, because their `pd_lsn` already exceeds `RedoRecPtr`. Either gap reopens G3.

(a) is what review decision 3 required; this analysis adds (b), because (a) alone does not close G3.

**Enforcement.**
- **Where.** A single helper `pagestore_branch_hint_safety_check(parent_tl, parent_incarnation, L)` in `pagestore.c`. Every compute-side path that reaches `pagestore_localsvc_create_branch` calls it before issuing CREATE_BRANCH:
  - `pagestore_create_branch` (`pagestore.c:1425`);
  - `pagestore_create_branch_with_incarnation` (`:1446`);
  - the prepare/idempotent paths (`:13535`, `:13567`).
- **What it checks.** It reads the parent's control images from the store (the existing control-restore read path) and requires a checkpoint-completion image with `checkPointCopy.redo == L`. This reuses the `archive_bootstrap` check at `pagestore_control_restore.c:700-704`, which already requires an exact checkpoint-redo image. The image must show `data_checksum_version == PG_DATA_CHECKSUM_VERSION` (enabled, not in-progress, since this tree has online checksum transitions, `xlog.c:4837-5050`) or `wal_log_hints == true`.
  - Because the image is the completion image of C, it reflects the settings at completion. An online checksum disable during C is recorded by its own control update and makes the completion image fail the check.
  - Enabling `wal_log_hints` requires a restart, which yields a new checkpoint.
- **The daemon is unchanged.** It stays version-independent.

**Error messages.**
```
ERROR:  cannot create pagestore branch %u of timeline %u at %X/%08X
DETAIL: The parent does not WAL-log hint-bit changes (data checksums are disabled and wal_log_hints is off as of checkpoint redo %X/%08X); parent hint bits written after the branch point could be admitted into the branch.
HINT:   Enable data checksums or set wal_log_hints = on, restart the parent, run CHECKPOINT, and create the branch at the new checkpoint's redo LSN.

ERROR:  cannot create pagestore branch %u of timeline %u at %X/%08X
DETAIL: %X/%08X is not the redo pointer of a completed checkpoint stored for timeline %u.
HINT:   Run CHECKPOINT on the parent and branch at pg_control_checkpoint().redo_lsn.
```
Both use `ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE`.

**Test-only override.** `pagestore.branch_allow_unsafe_cut` (PGC_SUSET, default off) downgrades both errors to a WARNING. It exists for unit harnesses that cut at synthetic LSNs.

**Compatibility.**
- **Checksums, this tree.** `initdb` defaults to data checksums on (`src/bin/initdb/initdb.c:167`). No pagestore script or harness passes `--no-data-checksums` or sets `wal_log_hints`: all `initdb` calls in `integration_test.sh`, `mvp_golden_test.sh`, `branch_boot_test.sh`, the redo demos, `harness/pagestore_harness.py` and `harness/pagestore_pgdata_fixture.py` use the defaults. So (a) holds for every existing test and golden cluster.
- **Checksums, other supported majors.** The REL_15–REL_17 `initdb` defaults to checksums off. The branchdb_15/16/17 test scripts must add `--data-checksums` (or `-c wal_log_hints=on`). This is part of the cross-version sync.
- **(b)** changes callers that cut at arbitrary LSNs. The controller's "branch now" path must CHECKPOINT and use its redo. The archive-bootstrap path already does this. Every existing test that cuts elsewhere (the fuzzer, golden tests at chosen targets, core-level daemon tests that do not go through `pagestore.c`) must be moved to redo cuts or set the override. Core/IPC tests are unaffected, because the check is compute-side.
- **Deferred alternative:** a PG-side hint scrub, clearing `HEAP_XMIN/XMAX_COMMITTED/INVALID` hints for XIDs ≥ the branch's `nextXid` at L on read-through. It would allow arbitrary cuts, but it is invasive, per-AM, and version-dependent.

---

## 8. Test plan

### 8.1 Unit tests (core)

- **`page_select` tables:**
  - pre-S present;
  - post-S only (first arrival);
  - multiple post-S (min seq, with the tie rule);
  - STRICT at L;
  - inherited range (`p ≤ B`: no escape);
  - legacy seq 0;
  - a differential against `page_visible(…,0)` at S = ∞.
- **Composition:** 3-level chains, legacy edges, STRICT unbinding.
- **Fork fold:** extend `ps_test_fork_event_index_selftest` with META/PAGE/UNSTAMPED mixes, `B`, and random caps, against a literal §1.3 reference. Keep the F5 scaling guard, plus a variant with one post-view META event.
- **`META_FIRST` maintenance** under out-of-order recovery insertion.
- **GROW dedup (4100849461):**
  - a child-local META_FIRST at `p ≤ B_child` hidden from the grandchild, followed by an honest late page on the child at `p' ∈ (B_child, L_grandchild]`: the grandchild's nblocks covers the page, both live and after reopen;
  - a later-created view with no recorded GROW sees correct nblocks;
  - `S_min` updates on branch create/delete, pin set/drop and walidx commit;
  - in-memory GROW compaction after cutover keeps every fence-needed GROW.
- **Artifact caps (4100849476):** attempt A at C BEGINs and fails; a control rewrite follows; attempt B at C BEGINs, resolves with `(C, token_B)`, and COMMITs; a further rewrite and a prune follow. The fence selects B's copy. A has no cap after supersession or restart.
- **Inherited range:** the tl1-local post-S control/META at `p ≤ B_tl1` stays hidden after root's version at `p` is pruned (the revision-1 probe failed this).

### 8.2 Planner property tests (merge blockers)

- For random version/event histories, positional and STRICT fences with random `B`, a random floor, random invalidating definitive events, and a random cutoff with derived fences, for every fence:
  - `select(kept) == select(all)`;
  - `fold(kept masked) == fold(all masked)`;
  - **the same equalities for every *future* admission sequence**: append a random later arrival at a random retained position and recheck. This is what catches a lost `s_min`.
- Named cases:
  - `(20,1)/(20,5)` under `(20, S=3)` keeps `(20,1)`;
  - with only post-S versions, the min is kept;
  - a fence above P uses P's selection;
  - after release, `(20,1)` becomes collectable;
  - **"an invalidated first arrival still defines `s_min`"** (4100849491): first page P1@(b,p), a same-p truncate, rewrite P2@(b,p), a branch fence `(L ≥ p, S < seq(P1))`, then prune. P1 must be retained; the branch still resolves P1 (empty after invalidation), not P2;
  - control-note closure: the min-seq note copy at a retained LSN survives `control_chain_keeps`;
  - forkmeta closure: a retained META at p implies `META_FIRST` at p, including when it is superseded;
  - GROW retention under a mask that hides a META.

### 8.3 Crash / restart

- **`branch_seq`:** survives restart; a retry returns the same S; a reuse gets a new S; the **observe regression** (`next > branch_seq` after reopening a store whose newest record predates the branch).
- **Legacy timelines** reopen as ∞.
- **Forkmeta V4 flags** survive the log, crash, cutover and reopen; `META_FIRST` is identical after reopen.
- **`horizon_seq`** survives.
- **Artifact caps** are rebuilt from completion records only.
- **Fault points** cover the V3 timeline append.

### 8.4 Compaction / prune integration

- The frozen view survives compaction, forkmeta cutover and walidx snapshot (the four-phase `check_bugb_state` plus cutover and walidx phases).
- Pre-S versions are reclaimed after the branch is deleted, and the bounded-space soak stays green.
- The unregistered-read -2 gate works.

### 8.5 Op fuzzer (`test/pagestore-op-fuzz`)

**Why the current oracle would still fail.** With the workaround off, the next parent op after a fork is stamped with the pre-append `wal_end = L`. That is a first arrival at L, which stays visible. The oracle also reads the parent with `req_seq 0`.

**Changes:**
1. `ship_wal()` returns the record's **end** LSN. The workaround env var is retired.
2. The oracle reads the **branch** (READV / READ_AT(L) / NBLOCKS / EXISTS) against the frozen model.
3. New actions:
   - hint-bit same-`pd_lsn` rewrite → hidden;
   - delayed honest write at a fresh position ≤ L → visible;
   - stale META at an occupied position → hidden;
   - unstamped META (with and without `req_floor_lsn`) → hidden;
   - a grandchild branch, plus child-local writes at `p ≤ B_child`;
   - a same-version control rewrite;
   - artifact attempt fail and retry around a control rewrite;
   - explicit compaction and cutover;
   - after every prune or restart, re-verify every frozen view (the stability requirement).
4. The fuzzer's CREATE_BRANCH is core-level, so the compute-side G3 check does not apply.
5. Acceptance runs: the default seed plus 20 random seeds, with `_SHRINK` on failure.

### 8.6 System

`integration_test.sh`, `mvp_golden_test.sh` (moved to redo cuts or the override, §7), `branch_boot_test.sh`, and one endurance live run (note the interaction with E-6).

---

## 9. Implementation phases, sizes and risks

| Phase | Content | Size (code + tests) |
|---|---|---|
| **P0** | `ls_op_lsn()` per-opcode fallback with `+1` (PR #296) | ~60 + 0 |
| **P1** | Refactor, no behaviour change. `ViewCap` and composition, `TlWalk` with `B_k`; `page_select` and identity lookups; `ForkEvent` flags, `META_FIRST`, `late_meta_idx`, `max_meta_seq`; the fold slow path with the inherited range; I-ALLOC and U5 asserts. Every cap is still ∞. | ~750 + ~500 |
| **P2** | Planner. `PsViewFence`, positional/STRICT modes, **closure** (page, control notes, forkmeta), the invalidation-drop exception, forkmeta masks and derived fences, the ordered-path LSN-strict bump. Includes the property tests. | ~650 + ~700 |
| **P3** | Activate branches. `TimelineRecEventV3`, `branch_seq`, observe, fixtures; branch edges carry S in reads, fences and gates, flipped **in one commit**; the kept child-first CREATE idempotence; `S_min` maintenance for branches; ports `run_bugb_suite` and the ancestry suite. | ~450 + ~500, plus fixtures |
| **P4** | Forkmeta V4 flags and FMS v2; UNSTAMPED visibility; **client unstamped + `req_floor_lsn` together with daemon persistence** (removes P0's `+1` and `ls_zeroextend`'s insert stamp; `PS_SHM_VERSION` gate); the GROW-dedup rule and in-memory PAGE-GROW compaction; identities and fixtures. | ~500 + ~450, plus fixtures |
| **P5** | Pins positional below R; walidx `horizon_seq` (WIPG v2, WISD v4) and walidx planner caps; `S_min` for pins and horizons; artifact caps (`committed`/`inflight`) and producer `(C, token)` reads; control-restore client seqs; §5.3 audits (the control `state`-only assertion, subtrans not mirrored); ports the revision suite. | ~550 + ~450, plus fixtures |
| **P6** | The G3 enforcement helper, errors, GUC override, and moving tests to redo cuts; `--data-checksums` in the REL_15–17 scripts; the fuzzer (§8.5); docs (READ_CONSISTENCY_DESIGN §1d, MVP_COMPLETION_PLAN (fork tuple, G3 requirement), RELEASE_VALIDATION, MVP_STATUS). | ~550 + ~450, plus docs |
| **Follow-up** | U3 plan-epoch validation | ~150 + ~150 |

Total: roughly 3.7k lines of code and 3.2k lines of tests, plus fixtures and docs.

**Highest-risk parts, in order:**

1. **Read/prune divergence**, including lost *facts*. The rule depends on `s_min(p)`, and every retention path that keeps "the current newest", or drops "currently invalidated" versions, can silently change it. Mitigation: one admissibility predicate shared by the fold and the planner masks; closure in all three planners; the §8.2 future-arrival property test as a merge blocker. The places audited for this class are `ps_page_prune_plan`, `prune_version_needed`, `control_chain_keeps` (newest copy per LSN), the control twin lookup, forkmeta `retain_horizon`, the exact-tuple duplicate collapse (safe: same identity), `fork_event_compact_dropped_markers` (inert only; the new PAGE-GROW compaction is §3.7(3)-guarded), and the cross-level probe (removed).
2. **Forkmeta positional retention**: masks, closure, derived fences, and the interaction with F3/F5 and orphan adoption.
3. **Missing `admission_seq_observe`** for `branch_seq` / `S_H`, which would misclassify records after a restart.
4. **The ordered-path fence bump** and every consumer that reads `UINT64_MAX` as meaning "bare-LSN fence". Audit greps: `UINT64_MAX` near `fences[`, `PsPruneFence`, `retention_project_lsn`, `admission_seq = projected`.
5. **The GROW dedup rule.** A wrong `S_min` (a registration racing an append) gives readable pages beyond nblocks. The exclusions are per registration type (§3.2), and recovery ordering is verified in P3.
6. **Client/daemon lockstep for unstamped ops** (P4 protocol gate).
7. **G3 enforcement (b)** changes branch-creation contracts for existing callers.
8. **The multi-shard lock** in the WAL_INDEX_PROGRESS stable sampling (run TSAN and the soak).
9. **Three persisted-format bumps**, each with a legacy/current fixture pair.
