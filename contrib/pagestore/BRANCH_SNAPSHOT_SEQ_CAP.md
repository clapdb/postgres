# Branch snapshot seq cap: a read-side same-position freeze for Bug B

**Status:** design proposal, revision 4, for review before any implementation.
- Rev 2 addressed PR #297 Codex round 1 and PR #296 comment 4100769750.
- Rev 3 addresses round 2: 4100994214/4100994220/4100994223/4100994229/4100994233/4100994236.
- Rev 4 replaces the rev-3 G3 attestation and core hook with compute-side proofs R1 and R2 (§7).

**Baseline:** `origin/pagestore` @ `0550146156d`. Line references are to that tree.

**Supersedes:** the admission-time LSN promotion in PR #294 (`d504a7af748`), abandoned 2026-09-25.

**Keeps from #294:**
- the per-opcode `ls_op_lsn()` fallback (P0, PR #296, with `+1`, §5.2);
- the child-first CREATE-retry idempotence check `fork_newest_definitive_event_through()` (§6.2);
- the Bug B regression suites, as acceptance tests (§6.2).

### Revision history

| Rev | Change | Section |
|---|---|---|
| 2 | Inherited-range rule replaces the cross-level probe | §1.5 |
| 2 | Artifact `S_a` = committed attempt's `begin_seq` | §2 |
| 2 | Position closure in page/control/forkmeta prune | §3.5–§3.7 |
| 2 | G3 branch-creation requirement | §7 |
| 2 | Synthetic-label analysis | §5 |
| 3 | **PAGE-class GROW dedup removed entirely**; cost and dependents re-audited | §3.2 |
| 3 | **Every finite-S read must be registered** (the `S ≥ F.seq` allowance is removed) | §3.5 |
| 3 | STRICT boundary keeps the first-arrival escape; composition re-proved | §1.2, §1.3 |
| 3 | G3 proof serialized with the S sample | §7 |
| 4 | G3 without checksums, hooks or attestation: R1 (`wal_log_hints` + cut at the redo of a checkpoint in the current lifetime) or R2 (controller flow, verified structurally) | §7, §9 |
| 3 | U3 plan-epoch validation moved into P2 | §3.7, §9 |
| 3 | Phases re-sequenced: formats before activation | §9 |
| 3 | Removed order-dependent optimizations are listed | §9.1 |

---

## 0. Problem and constraints

**Bug B.** A COW branch reads its ancestors through `tl_walk_next()` (`pagestore_core.c:7362`). That walk caps each ancestor level by `branch_lsn` only, and every ancestor hop passes `seq_cap = 0` (all `seq_cap = w.lsn == read_lsn ? read_seq : 0` sites). So anything the parent admits after the fork at a position ≤ `branch_lsn` is visible to the branch:
- a hint-bit-only rewrite under an unchanged `pd_lsn = p ≤ L` wins by a larger `admission_seq` at the same LSN (`page_visible()`, `:5544`);
- a fork event whose `req_lsn ≤ L` changes the branch's size or existence.

A branch reuses XIDs the parent later commits under a different meaning, so post-fork hint bits are corruption for it, not staleness.

**Why not promotion.** In #294 the LSN served as both the branch-visibility coordinate and the parent's version order. Moving it to fix the first kept breaking the second (V4–V7, G1, G2). This design never moves an admitted record; parent newest reads keep today's `(lsn, admission_seq)` order bit for bit.

**Why not a pure 2D cap** (`lsn ≤ L ∧ seq ≤ S`). Honest late admissions must stay visible:
- `pagestore_test.c:4631` (block 5 written at 1200 after the branch at 1500 is visible to the branch);
- #294's `rel100` (a genuine first admission exactly at L is visible).

**Goal.** A read-side rule parameterized by a per-view `cap_seq` that:
1. hides same-position rewrites admitted after the view froze;
2. keeps honest first admissions visible;
3. is *stable*: no later admission, prune, compaction or restart changes a resolution;
4. is retainable, including the facts the rule depends on (`s_min(p)`);
5. has **no correctness dependence on the order of view registration, recovery, or maintenance** beyond the locks that already serialize retention (rev 3 principle; §9.1).

---

## 1. Semantics

### 1.1 Positions

| Object | Position | Rationale |
|---|---|---|
| Relation page version | `(level, key, block, lsn = pd_lsn)` | The bytes self-certify their WAL position. Same-position versions differ only in unlogged state. |
| Object version (SLRU / CONTROL / SLRU_LIVE/TOMB/WM / READER_SNAPSHOT / ARTIFACT) | `(level, key, block, version)`, where `version` is the caller's WAL LSN (`:16907-16944`) | The equality edge is audited in §5.3. |
| Fork event, **META** class: SET (CREATE/TRUNCATE), DEAD (UNLINK), GROW from ZEROEXTEND | `(level, key, lsn)` | Logical operations. |
| Fork event, **PAGE** class: GROW derived from a page append (`fork_grow_apply`, `FEV_SEG_GROW*`) | `(level, key, lsn, block = nblocks-1)`, mirroring its page version (§1.3) | Must agree with page visibility. |
| Inert markers | n/a | No size effect. |

### 1.2 View caps and composition

```
ViewCap { uint64 lsn;          /* L */
          uint64 seq;          /* S (PS_SEQ_UNBOUNDED = ∞) */
          uint64 strict_seq; } /* X: extra bound at position == L */
```

- **Branch edge** `(L_e, S_e, X = ∞)`.
- **Retention pin / exact-R reader** `(R, S_r, X = S_r)`.
- **Newest read** `(∞, ∞, ∞)`.
- **Seq-less as-of read** `(R, ∞, ∞)` (today's semantics, deliberately unchanged).

**Composition across edge `(L_e, S_e)`.** This is the single function used by `tl_walk_next`, `retention_project_cap`, the gates and all fence builders:

```
lsn'        = min(lsn, L_e)
seq'        = min(seq, S_e)
strict_seq' = (L_e < lsn) ? ∞ : strict_seq
```

**Composition is exact (the intersection of the constituents' admissible sets).** Write the admissible set of a constraint `i = (L_i, S_i, X_i)` at level `k` as (§1.3):

```
A_i = { v : p ≤ L_i ∧ (p < L_i ∨ v.seq ≤ X_i) ∧ (v.seq ≤ S_i ∨ esc_k(v)) }
```

where `esc_k(v) ≡ p > B_k ∧ v.seq == s_min_k(p)` depends only on the level and the position, **not on i**. Then for `L = min L_i`:

- `p ≤ L_i` for all i ⟺ `p ≤ L`;
- for `p < L` every boundary conjunct is true;
- at `p = L`, constraints with `L_i > L` are satisfied by `p < L_i`, and those with `L_i = L` require `v.seq ≤ X_i`. So the conjunct is `v.seq ≤ min{X_i : L_i = L}`, which is what `strict_seq'` computes (an edge that lowers L carries `X = ∞`);
- `∩_i (v.seq ≤ S_i ∨ esc) = (v.seq ≤ min S_i) ∨ esc`, because `esc` is common to all i.

Hence `A_composed = ∩ A_i` exactly, **also under STRICT**. Rev 2 disabled `esc` at the strict boundary. That made the composition stricter than the intersection, which is Codex 4100994223.

**Worked example.**
- Setup: branch edge `S_e = 10` at `L_e ≥ R`; the root's first event at `p = R` has seq 15; an exact-R pin on the branch has `S_r = 20`.
- The composed cap at the root level is `(R, 10, 20)`.
- The event passes `p ≤ R`; passes `p = R ⇒ 15 ≤ X = 20`; and, since `15 > 10`, needs `esc`: the root has `B = −∞` and seq 15 is `s_min(R)`. So it is **visible**.
- This matches both constituents: the branch admits it as a first arrival, and the pin admits it since `15 ≤ 20`.
- A second arrival at R with seq 25 fails `X`, so it is hidden.

### 1.3 The rule

For level `k` with cap `(L, S, X)`, position `p`, `V(p)` the entries at `p` on level `k`, `s_min(p)` the min seq in `V(p)` (legacy seq 0 counts as 0), and `B_k` = `branch_lsn` of timeline `k` (`−∞` for the root):

```
admissible(v) ⟺ p ≤ L ∧ (p < L ∨ v.seq ≤ X) ∧ ( v.seq ≤ S ∨ (p > B_k ∧ v.seq == s_min(p)) )
```

- **Pages:** the admissible version with the greatest `(lsn, seq)`. That is: the newest position with any admissible version; there, the newest version with `seq ≤ S` if one exists, otherwise the first arrival.
- **META fork events:** the hop fold over admissible events in `(lsn, seq)` order.
- **PAGE-class GROW:** admissible iff `p ≤ L ∧ (p < L ∨ seq ≤ X) ∧ (seq ≤ S ∨ p > B_k)`. It is never filtered by first-arrival status (it mirrors a page version; a hidden same-position page rewrite's GROW duplicates the visible version's `nblocks`). It is filtered in the inherited range exactly like its page.
- **UNSTAMPED:** `p ≤ L ∧ (p < L ∨ seq ≤ X) ∧ seq ≤ S` (§1.6).
- **Legacy seq 0** is `≤ S` for every S.

**At `S = X = ∞`** this is exactly today's `page_visible(e, L, 0)` and the cached prefix fold, which is the legacy-branch equivalence (§4.1).

### 1.4 Validation

| Case | Outcome |
|---|---|
| Honest late, no pre-fork version at `p ≤ L`, `p > B_k` | First arrival, visible. `:4631` and `rel100` pass. |
| Pre-fork version at `p`, then a post-fork rewrite | The newest pre-S version is chosen (`rel103`, revision-1, control). |
| Stale META at 1000 with CREATE@1000 pre-fork | Non-first post-S META is hidden (`rel101`, `rel105`, ancestry-5). |
| Multi-block record, block 7 flushed post-fork at a `p` shared with pre-fork block 5 | Page 7 is first at `(7,p)`; its PAGE GROW is admissible. Consistent. |
| Honest in-flight ZEROEXTEND at an occupied META position | Hidden. Acceptable by crash equivalence. |
| Child-local post-S write at `p ≤ B_child` (ancestry-1/2) | Hidden (§1.5). |
| First root event at the strict boundary under a composed pin | Visible iff `seq ≤ X` (§1.2 example). |

**Ambiguity (several post-fork arrivals, none pre-fork): first arrival.**
- For relation pages the label is the content, so later arrivals only add unlogged bits.
- The first arrival is immutable, provided retention keeps it (closure, §3.5–§3.7).
- It is admissible to every view reaching `p > B_k`.
- It can be wrong only through bits it already carried (G3, closed in §7), or through a synthetic object/META label at a fresh position (§5.2 gives synthetic labels a position strictly after every existing cut).

**Crash equivalence.** A branch restores control at or below L and replays WAL to L. Hiding an honest in-flight admission at an occupied position equals losing it in a crash at L. The rule may err toward hiding at an occupied position, never toward revealing a rewrite.

### 1.5 The inherited range

On a branch timeline `k`, positions `p ≤ B_k` are inherited history, and the first-arrival escape is disabled there.
- Relation pages on a branch are already clamped above `B_k` (`branch_floor`, `:16890`).
- Other local writes at `p ≤ B_k` are one of three kinds:
  - preparation or first-boot replay and seeding, which happen before any descendant or pin of `k` exists, so they are pre-S for all of its views;
  - later re-applications (duplicates);
  - stale labels.
- The rule is local (it depends only on the immutable `B_k` and on `k`'s own entries), so retention elsewhere cannot change it.
- Ancestry-1 and ancestry-2 are hidden from tl2 and remain visible to tl1's newest reads.

### 1.6 Unstamped records

Admissible to a capped view iff `lsn ≤ L ∧ (lsn < L ∨ seq ≤ X) ∧ seq ≤ S`. At `S = ∞` this is today's behaviour. Placement is in §5.

---

## 2. Which views carry a cap_seq

**Invariant I-ALLOC.** Every `admission_seq` that ends up indexed is allocated and published within one hold of that key's shard write lock, under admission-rd.
- Daemon paths comply: `:16876`, `:6984`, `:20403/20488/20553/20583`.
- P1 asserts it.
- Exempt: the barrier (`:1834`), pin SET (`:20950`), and artifact tokens.

**Stable seq.** `next_admission_seq - 1` read under all shard locks, or any seq allocated under admission-wr.

**Registration.** Every finite-S view is registered at the level it reads. A capped read whose composed `(level, L, S)` does not match a registered view is rejected (§3.5).

| View | cap_seq | When / locks | Persistence | Recovery |
|---|---|---|---|---|
| **Branch** `branch_seq` | `admission_seq_alloc()` inside CREATE_BRANCH (unused by any record; pre-S means `seq < S`) | admission-rd + all shard-wr + map-wr (`pagestore_daemon.c:766-777`). Exact retries return the persisted S; a DELETED id reuse allocates fresh. The G3 proof (R1 or R2, §7) is checked on the compute side in the same backend immediately before this request. | `TimelineRecEventV3.branch_seq` | `admission_seq_observe` (required) |
| **Retention pin** | `pin.admission_seq` (under admission-wr, `:20948-20951`) | stable | retention log | existing |
| **Exact-R / advancing reader** | `(R, S)` from `ps_admission_barrier()` (`:1828`) | stable. **Must be covered by a retention pin at `(R, S)` before use** (§3.5, audited in P5). | existing | existing |
| **Artifact fence** | `S_a` = `begin_seq` of the committed attempt, plus a transient `inflight` token cap (§2.1) | bound at COMMIT, under the artifact key's shard lock | `PsArtifactLifecycle.begin_seq` (lifecycle v2, `pagestore_artifact_format.h:47`); **no format change** | rebuilt from completion records |
| **WAL-index horizon** | `S_H`, stable-sampled in `walidx_commit()` (shard 0 already held in write mode; shards 1..n-1 taken in read mode, ascending, before `walidx_publish_wrlock`) | order: shard(asc) → walidx_publish → wal_lock → walidx_meta | WIPG v2 and WISD v4 | `admission_seq_observe` |
| **Materializer marker** | none (its view is its MATERIALIZER pin) | – | – | – |
| **Forkmeta cutoff** | not a view (lexicographic compaction boundary) | – | – | – |
| **Page frontier** `(F_lsn, F_seq)` | not a view | – | – | – |

### 2.1 Artifact fence caps

An `ArtifactFence` entry (`:18474`) holds a small cap set:
- `committed`: `begin_seq` from the durable COMMIT record at C;
- `inflight`: the current unfinished attempt's token, added at BEGIN and at `artifact_fence_reserve()` (`:16971`).

Transitions:
- COMMIT moves the token from `inflight` to `committed`.
- A superseding BEGIN, a DROP of an unfinished attempt, or a restart drops the `inflight` cap. Unfinished attempts cannot commit after a restart.
- Failed attempts never contribute.
- A DROP followed by a same-C re-publication may briefly hold two committed caps, until the existing release.

**Producer requirement (audited in P5).** The producer reads the control era it embeds with `(C, token)`. Legacy lifecycle-less seeds use `S_a = ∞`.

---

## 3. Code touch points

### 3.1 Read path, pages

- `page_visible()` → `page_select(e, const ViewCap *c, uint64 B_k, ...)` (`:5544`): find `p*`; there take the newest with `seq ≤ S` (and `≤ X` if `p* == L`); otherwise the min-seq version if `p* > B_k` and it satisfies `X`; otherwise step down a position. O(nver). `page_visible` stays as a wrapper for uncapped callers.
- `TlWalk` (`:7338-7372`) carries a `ViewCap` and `B_k`. Every `seq_cap = w.lsn == read_lsn ? read_seq : 0` is deleted (`:7651`, `:17631`, `:7718`, `:7815`, `:7787`, `:7383`, `fork_asof_query_allowed`).
- Memtable and layer lookups (`:17694`, `:17703`) are called with the selected identity `(pv->lsn, pv->admission_seq)`.
- LSN-0 relation bytes at ancestor levels keep failing closed (`:7663`, `:17660`).

### 3.2 Read path, fork events; GROW recording

**In-memory state** (`sizeof(ForkEvent) == 40` stays):
- the `snapshot_dropped` byte becomes flags: bit0 `SNAPSHOT_DROPPED`, bit1 `META`, bit2 `META_FIRST`, bit3 `UNSTAMPED`;
- `ForkEnt` gains `max_meta_seq`, defined as the **max seq over the META/UNSTAMPED events currently present**. It is recomputed exactly on load and after in-memory compaction; it is a pure function of the present set, not of history;
- `late_meta_idx` holds the non-first META events;
- `META_FIRST` is maintained on insert, including recovery's out-of-order inserts.

**`fork_asof_hop(e, const ViewCap *c, uint64 B_k, uint32_t *nb)`** (`:5712`):
- **Fast path**, when `c->seq == ∞` or `max_meta_seq ≤ c->seq` and no PAGE GROW sits in the inherited range with `seq > c->seq`. For the latter, a per-fork `max_inherited_page_seq` is kept; it is also a pure function of the present set. The visible set is then the tuple prefix `upper_bound(L, X)`.
- **Slow path.** Compute

  ```
  H = { META ∧ seq > S ∧ (¬META_FIRST ∨ lsn ≤ B_k) }
    ∪ { UNSTAMPED ∧ seq > S }
    ∪ { PAGE-GROW ∧ seq > S ∧ lsn ≤ B_k }
  ```

  (all with `lsn ≤ L`) and fold from `min(H)−1`, skipping H. The `X` bound applies at `lsn == L` as today.
- The same predicate is used by `fork_inheritance_fenced`, `fork_page_invalidated` and `fork_block_death_through`.
- `fork_nblocks_recovery` and `fork_newest_visible_lsn_through` stay LSN-only.

**PAGE-class GROW dedup is removed (rev 3; 4100849461/4100994214/4100994236).**
- Three findings in a row (4100849461, 4100994214, 4100994236) came from skipping a PAGE GROW because the hop fold at `(p, g)` already covered it. That fold includes META events that some view may hide. Whether the skip is safe then depends on which views exist, and on recovery restoring them before page replay.
- Rev 3 records **every** PAGE-class GROW:
  - `fork_event_add()` (`:6096-6098`) no longer drops a `FEV_GROW` whose hop size already covers it, for PAGE-class GROWs. Exact-identity duplicates (same `(lsn, seq)`, as in `fork_restore_later_page_growth` `:7058-7117`) are still collapsed; that is identity, not state.
  - `append_page_impl`: the persisted marker for ordered records is always `FEV_SEG_GROW(_BOUND)`, never `FEV_SEG_COMMIT(_BOUND)` for new records.

**Dependents of the old dedup, re-audited:**

| Dependent | Treatment |
|---|---|
| **Below-cutoff admission check** (`(ordered_record \|\| segment_grows) && !fork_meta_mutation_future(...)`, `:17103`) | Kept as a *view-independent* admission rule under a new name, `extends_uncapped = fork_size_asof_hop(fe, lsn, seq, ∞) < block+1`. A non-ordered page write below the cutoff is still admitted when it does not extend the uncapped size (honest late evictions of old pages keep working), and its GROW is recorded in memory. Its durability is the page record itself: replay re-derives it. The next cutover plans it under every fence mask (§3.7). |
| **Commit-class orphan adoption** (`fork_event_commit_adoptable`, `:6300-6320`; F2/F3 history) | Applies only to legacy `SEG_COMMIT*` markers. New records are growth-class, whose adoption rule is already proven sound unconditionally (MVP_COMPLETION_PLAN rev-3 row of 2026-09-19). Tests `test_orphaned_commit_marker_*` stay as legacy-format tests. |
| **F5 scaling guard** (`test_fork_event_index_scaling`) | The FSM/VM rewrite pattern now produces GROW events instead of inert commit markers. The guard's step bounds stay (lookups are still bisected). Add a memory-bound assertion after a cutover (below). |
| **`fev_bench.c`** | Re-baseline. |
| **ZEROEXTEND persist-skip** (`fork_grow_with_seq`, `:6966`) | This is a **META** GROW, and it is kept. It is view-independent: a skipped ZEROEXTEND equals a hidden one, which is crash-equivalent (unlogged growth). It never makes a page readable beyond `nblocks`, because pages carry their own PAGE GROWs. |
| **`fork_has_growth_at`** (markerless SEG0) | Reads only; unaffected. |

**Cost:**
- **Memory.** One 40 B `ForkEvent` per non-extending page write (formerly deduped) until the next forkmeta cutover.
- **Durable bytes.** Plain records add 0, since the segment record is the durability. Ordered records write `SEG_GROW` instead of `SEG_COMMIT`, which is the same size. Forkmeta snapshots grow only by the GROWs the plan retains.
- **Restart.** Each retained page version re-derives one GROW, bounded by retained versions, until the next cutover.

**Can consecutive GROWs be merged?**
- **Not locally.** Dropping `e1 = GROW(n1)@(p1,s1)` because a later `e2` exists changes the fold for every cap with `p1 ≤ L < p2`, or with `p1 == p2` and `X ∈ [s1, s2)`, or with `S ∈ [s1, s2)` in the inherited range. Future caps are unknown.
- **Only the forkmeta planner** may drop GROWs. It knows the live fence set, the floor, and the cutoff horizon. It keeps exactly what every mask's `retain_horizon` needs (§3.7(3)).
- **In-memory compaction.** After a successful cutover publish, extend `fork_event_compact_dropped_markers` to drop the plain PAGE-class GROWs at or below the cutoff that the published plan dropped. This needs no ordering argument beyond the planner's own:
  - later-created views have `L ≥` the page frontier ≥ the cutoff (CREATE_BRANCH and pin SET reject L below the page frontier, `branch_frontiers_allow`; the cutoff is ≤ every owner's page frontier, `fork_meta_snapshot_cutoff` `:9482`), so they see the collapsed region only through the retained cutoff-horizon fold;
  - GROWs re-derived by a later replay are fold-neutral for every live fence, because the plan proved them droppable.
- **Estimate.** ≤ 40 B × non-extending writes between cutovers. For example, 10k writes/s with a 60 s cadence is ≤ 24 MB.

### 3.3 Artifacts and control

- `artifact_visible()` (`pagestore_artifact_lifecycle.inc:93-136`) selects COMMIT/DROP with `page_select`. A same-C re-publication after DROP is a same-position rewrite (U4).
- Control restore selects the image and note independently. P5 adds a debug check that the redo values match.
- Clients:
  - WAL-owner/materializer control restore sends its pin `(R, seq)`;
  - the artifact producer reads with `(C, token)`.
- Not introduced: `control_pair_follow_promotion`, the lock-through-publication scheme, and the #294 collision walks.

### 3.4 WAL index

- `walidx_get()` stays LSN-capped.
- `PS_OP_BLOCK_DEATH` maps `(req_lsn, req_seq)` to a ViewCap.
- `walidx_plan_bases_build()` uses the new page planner and `fork_asof_hop` with the horizon or pin cap (`:15282,15296,15356`).
- `walidx_prune_fences()` returns ViewCaps.

### 3.5 Retention: pages; the registration gate

**Planner** (`ps_page_prune_plan`, `pagestore_prune.c:7-78`). Add an in-memory `PsViewFence { lsn, seq, strict_seq, inherited_below }`. The keep set is:
1. the floor base and the future tail, as today;
2. each fence's selection under §1.3 (one version);
3. **position closure.** At every position `p` where (1) or (2) keeps a version and `p > inherited_below` and `p ≤ L_f` for some finite-S fence, also keep the min-seq version at `p`;
4. **`prune_version_needed()`** (`:18426-18460`), per fence ViewCap with the §3.2 hidden-event predicate, may drop a fork-invalidated kept version, **but never one required by (3)**. Invalidation hides bytes; it does not erase the first-arrival fact (4100849491 example).

Closure depends on the *current* fence set, which is inherent to fence-driven retention. It is safe because:
- a view registered later has `S' ≥` every existing seq, so it never uses the escape at an existing position;
- every finite-S read is registered (below).

**Also changed:**
- `page_prune_fences()` (`:18721`) and `retention_project_cap()` (`:18166`) use §1.2.
- Descendant caps carry `branch_seq` (∞ for legacy).

**Ordered-path fence bump** (`:17040-17075`): positional fences are LSN-strict, so the write always lands at `> L`. STRICT pins keep the tuple test.

**Registration gate** (rev 3; 4100994214). `page_frontier_allows()` (`:4650`) and `…_structural/projected_fence_active()` (`:4620`, `:4568`):
- A level read with **finite composed S is honoured only if `(level, L, S)` matches a registered view**:
  - a live descendant's projected `(L, S)`;
  - a projected pin `(L, S)`;
  - a WAL-index horizon `(H, S_H)` or WAL_INDEX pin;
  - an artifact fence cap.
- **Otherwise it gets -2**, whatever `F.seq` is. Rev 2 allowed unregistered reads with `S ≥ F.seq`; that is removed (§9.1).
- Seq-less reads (`S = ∞`) are unchanged.
- Consequence: exact-R and advancing readers must hold a retention pin at `(R, S)` before issuing capped reads. P5 audits `ls_pinned_read_seq` and the adoption path, and the tests that pass a raw `pin_seq` already hold that pin.

### 3.6 Retention: control

- `control_prune_fences()` emits ViewCaps, including each artifact cap.
- `control_chain_plan()` uses the positional planner plus closure. The twin lookup (`:18329-18334`) uses the fence-selected twin.
- `control_chain_keeps()` (`:18369-18412`) keeps each fence's selected note copy and the min-seq copy at each retained, escape-eligible LSN (closure), not only the newest copy per LSN.

### 3.7 Retention: forkmeta (highest risk)

1. **Per-fence admissibility masks** (§1.3, including PAGE GROWs in the inherited range) feed `retain_horizon`.
2. **Closure.** A retained META at an escape-eligible `p` implies retaining `META_FIRST` at `p`. A hidden UNSTAMPED event is retained whenever a later event at its LSN is retained.
3. **GROW retention.** `retain_horizon` over each mask keeps the largest admissible PAGE GROW after that mask's latest visible definitive event. This is the only GROW-dropping mechanism (§3.2), and property tests assert it.
4. **Fence filter** (`:9990-9998`): keep positional fences at `cutoff_lsn`.
5. **Derived fences.** Add `(cutoff_lsn, S, strict = cutoff_seq)` for every view with `L ≥ cutoff_lsn` and `S < cutoff_seq`.
6. **WAL-index horizons** become ViewCaps.
7. **Plan-epoch validation (U3), moved into P2 as a prerequisite of any activation.**
   - Keep a per-timeline `fork_event_admit_seq`: the max seq of any fork event or PAGE GROW admitted at `lsn ≤` the plan's max horizon. It is updated under the key's shard lock on admission.
   - The planner records the value at plan time.
   - Publication (under `walidx_publish` for WAL-index snapshots, and under the forkmeta cutover's admission-wr for forkmeta snapshots) compares it again and aborts and re-plans on change.
   - The forkmeta cutover already takes admission-wr for `freeze_seq` (`:10575`), so this is a check, not a new lock.
8. **Restart.** `META_FIRST`, `max_meta_seq` and `max_inherited_page_seq` are recomputed from the retained events. Closure makes `META_FIRST` identical, and the other two are functions of the present set.

### 3.8 Persistence of admission_seq

- Global and monotonic (`:353`, `:918`, `:934`).
- Observed from segments/layers, forkmeta, the cutoff and retention. `branch_seq` and `S_H` are added.
- Preserved through flush, compaction (`:2973`, `:3599`) and forkmeta.
- Nothing rewrites a stored seq; P1 asserts identity preservation in the merge.

---

## 4. Persisted formats (all land in P3a, before any activation; §9)

### 4.1 Timeline log `TimelineRecEventV3` (rec_len 64)

```
{ magic TLM2, rec_len, kind, id, parent, state, branch_lsn, incarnation,
  parent_incarnation, branch_seq, crc, reserved }
```

- CREATE and STATE carry `branch_seq`; replay validates equality and observes it.
- Legacy creates map to ∞.
- Identity `timelines` goes from 2 to 3.
- Old binaries reject `rec_len = 64`.

**Legacy rule: S = ∞ is bit-for-bit today's behaviour**: `page_select` = `page_visible(…,0)`, an empty hidden set, the positional fence `(L, ∞)` equal to today's keep rule, closure inactive, and no registration needed because the read is seq-less at that edge. A new descendant of a legacy branch freezes its whole ancestry through min-composition.

**Fixtures (D5 rule 2):** demote `posix-timeline-delete-holes` to legacy (byte-identical) and add a current V3 fixture (branch + post-fork rewrite) with a frozen-view reopen oracle.

### 4.2 Forkmeta V4 / FMS v2

- `FORK_META_V4_MAGIC`, the V3 layout; `kind` flags `0x40` META (ZEROEXTEND GROW) and `0x80` UNSTAMPED.
- Legacy V2/V3: SET/DEAD are META; a plain `FEV_GROW` is PAGE.
- The FMS payload goes from v1 to v2 with the flags.
- Identities and fixtures: demote `posix-forkmeta-crc`.
- **These must be written from P3a on, before P3b activates finite caps.** Otherwise a META GROW written in the V3 shape reloads as PAGE and becomes unconditionally visible (4100994233).

### 4.3 WAL-index progress

WIPG v2 `horizon_seq`, WISD v4. Legacy is ∞. Identity plus fixture.

### 4.4 Unchanged

Page segments, image layers, frontier files, the retention log, artifact formats, control blocks, and the materializer marker.

---

## 5. Unstamped and synthetic-label fork events

### 5.1 Daemon side

- `req_lsn == 0` is placed at `max(fork_op_lsn(), req_floor_lsn)` (`:20139`; `req_floor_lsn` is a new channel field, 0 for legacy clients).
- It is persisted `UNSTAMPED`, with seq-only visibility (§1.6).
- The ordered page path is unchanged except for the LSN-strict bump.

### 5.2 Client side: WAL-less CREATE/TRUNCATE/ZEROEXTEND

**P0 `+1` (mitigation).**
- `GetXLogInsertRecPtr() + 1` (the replay pointer + 1 in recovery) is strictly above every existing cut. Record ends are MAXALIGNed, so it never collides with a real record end.
- **Cost:** a pre-fork WAL-less op while WAL is idle is invisible to a cut at that idle position. That affects only unlogged relations, which are reset at branch boot.

**P4 (precise).**
- The client sends `req_lsn = 0` together with `req_floor_lsn = insert + 1`.
- Bare `req_lsn = 0` is insufficient: `newest_visible + 1` can lie far below the op's real time, so a *later* view at a past `L'` would see it.
- In the same P4 commit, `ls_op_lsn()`'s `+1` and `ls_zeroextend`'s insert stamp (`backend_localsvc.c:977-996`) are removed, behind a `PS_SHM_VERSION` bump. The format (the UNSTAMPED flag) is already in place from P3a.

### 5.3 Equality at the cut for other label sources

| Source | Conclusion |
|---|---|
| UNLINK at transaction end (this transaction's commit/abort record end) | Equality is the correct inclusion. **No edge.** |
| Control images from WAL-driven updates (record end, clamped to `minRecoveryPoint`) | **No edge.** |
| Control images from record-less transitions (`DB_SHUTDOWNING` `xlog.c:7654`, promotion `:6818-6820`) | The edge exists but is **benign**: only `state` differs, and restore does not trust it (`pagestore_control_restore.c:688-715`). P5 asserts this. |
| SLRU live mirror (`pagestore_slru.c:3514`, the insert pointer) | **Benign.** It captures WAL-driven state. pg_subtrans must not be mirrored (P5 audit). |
| SLRU seeds / reader snapshots at C | **No edge.** |
| Relation pages | **No edge.** |

---

## 6. Compatibility with the parts of #294 that are kept

### 6.1 P0 (PR #296)

- UNLINK keeps the transaction end.
- CREATE/TRUNCATE without their own WAL use insert + 1 (replay + 1 in recovery).
- The startup process uses the replay pointer.
- P4 replaces the `+1`.

### 6.2 Acceptance suites

**Kept from #294:** `fork_newest_definitive_event_through()` as the CREATE-retry idempotence check (lands in P3b). Without it, ancestry-3 fails because of a pre-existing parent-side bug.

**`run_bugb_suite`:** every check holds as analysed in rev 2: rel100 first arrival, rel101/105 non-first META, rel102 UNSTAMPED, rel103 pre-S, rel104, rel106 LSN-strict bump, and the control check. Adjustments:
- the pinned reader at 1200 passes its pin seq; it holds a pin, so it is registered;
- add parent newest checks (rel103=0x30, control=0x77, rel101/102 at 3 blocks, rel105 truncated).

**Revision suite:**
- 1 holds (it reads with `pin_seq` and the pin is registered);
- 2: adjust to send the MATERIALIZER pin seq;
- 3 and 4 hold.

**Ancestry suite:**
- 1 and 2 hold via §1.5;
- 3 holds via the kept idempotence check;
- 4 holds;
- 5: pass the pin seq.

**Replaced:** `test_promoted_control_collision_follows_its_pair`. The note and image stay at 2000; the fence with `S_a` selects the pre-rewrite copy; `wal_floor == 1800`. The cutover round-3 rejection holds.

---

## 7. Audit items

| Item | Resolution |
|---|---|
| V1, V2/V3, V4, V5, V6, V7, G1, G2 | By construction (no promotion; local inherited range; untouched parent order; closure preserves first-status). |
| U1 | Needs work (P5): WAL-index caps and planner. |
| U2 | By construction, with a semantic change: pins are positional below R, and at R `seq ≤ X` with the escape kept. |
| U3 | Plan-epoch validation, **in P2** (§3.7(7)). |
| U4 | By construction (§2.1). |
| U5 | Out of scope, guarded by an assert. |
| Stale/synthetic label at a fresh position | P0 `+1`, then P4 (§5.2); other sources in §5.3. |
| **G3** | Closed by the requirement below. |

### G3 requirement (rev 4: no data-checksum dependence, no core hook, no attestation protocol)

**What must hold.** Every page version with `pd_lsn ≤ L` that is admitted before S, or that is the first arrival at its position after S, carries no hint bit about a transaction that commits after L. After S, the rule already hides every same-position rewrite of a position that has a pre-S version. So G3 reduces to two conditions:

- **(G3-i)** no pre-S version with `pd_lsn ≤ L` carries a post-L hint; and
- **(G3-ii)** every position `≤ L` that can later be rewritten with post-L hints has a pre-S version, or its first post-S arrival carries no post-L hint.

Two proofs, **R1** and **R2**, discharge this. Branch creation must satisfy one of them.

#### R1: live parent, cut at a checkpoint redo, `wal_log_hints = on`

**Requirements:**
1. **`wal_log_hints = on` on the parent**, as its own setting. Rev 4 deliberately does **not** accept data checksums instead: this tree can disable checksums online (`SetDataChecksumsOff`, `xlog.c:4807-5050`), and trusting them would need a core hook. `wal_log_hints` is `PGC_POSTMASTER`, so it cannot change within a postmaster lifetime.
2. **`L = redo(C)`** for a checkpoint C that completed **within the current postmaster lifetime**.

**Proof.**
- C completed, so every buffer dirtied before `redo(C)` was flushed before S. Such a buffer's hints concern commits at or before `redo(C) = L`.
- For the whole lifetime, `XLogHintBitIsNeeded()` is true, so the first hint-only dirtying of a page with `pd_lsn ≤ RedoRecPtr` emits an FPI (`XLogSaveBufferForHint`). After C began, `RedoRecPtr = L`, so any hint set after `redo(C)` on a page with `pd_lsn ≤ L` moves its `pd_lsn` above L.
- Hence every version with `pd_lsn ≤ L`, pre-S or post-S, holds only hints set before `redo(C)`. That gives G3-i and G3-ii.
- A checkpoint from an *earlier* lifetime gives no such guarantee, because that lifetime may have run without `wal_log_hints`.

**How "C within the current lifetime" is checked.**
- pagestore already installs `control_file_write_hook` (`pagestore_control.c:684-685`, part of the existing core series). No new hook is needed.
- The first invocation after shared-memory initialization records `ps_lifetime_floor_lsn = update_lsn` in pagestore shared memory. `StartupXLOG` updates the control file (to production or crash-recovery state) before any backend can write, so this floor is at or below every WAL position this lifetime writes.
- The check requires `ps_lifetime_floor_lsn != 0 ∧ ps_lifetime_floor_lsn ≤ redo(C)`.
- As a cross-check (logged if inconsistent, but not relied on), it also compares `CheckPoint.time ≥ PgStartTime`.
- A crash-restart inside one postmaster re-initializes shared memory and resets the floor, which is conservative. `wal_log_hints` does not change across such a restart.

**Serialization with S.** The check and CREATE_BRANCH run in the same backend of the parent writer, back to back, inside `pagestore_branch_g3_check_and_create()`. No proof has to be carried to the daemon:
- `wal_log_hints` and the lifetime can change only through a new postmaster.
- PostgreSQL refuses to start a new postmaster while any process of the previous one is still attached to its shared memory (`PGSharedMemoryIsInUse`, `src/backend/port/sysv_shmem.c`). So while the checking backend lives, including while its CREATE_BRANCH IPC is in flight, the lifetime cannot change.
- A backend orphaned by a postmaster crash keeps the old segment attached, which blocks the restart until it exits.
- R1 requires the caller to be the parent writer itself: `pagestore_localsvc_timeline() == parent` and `!RecoveryInProgress()`. A recovery server cannot use R1, because on a standby hint-only changes are either not dirtied (hint WAL on) or written without WAL (off), and the lifetime argument does not transfer. Recovery-side branching uses R2.

**Cost of requiring `wal_log_hints` everywhere.** With data checksums on (this tree's `initdb` default, `src/bin/initdb/initdb.c:167`), `XLogHintBitIsNeeded()` is already true, so `wal_log_hints = on` adds **no extra WAL**. Without checksums it adds one FPI per page per checkpoint cycle on the first hint-only dirtying, which is the standard `wal_log_hints` overhead.

#### R2: controller flow (`pagestore_prepare_branch_from_control`)

**Verification of the controller flow** (`pagestore_branch_prepare.py`, `execute()` at `:1572-1650`):

1. `capture_and_pin_base()`.
2. `stop_writer()` (`:1162-1166`): `pg_ctl -m fast stop`, which is a **shutdown checkpoint C**.
3. `start_restricted_writer()` (`:1168-1202`): **the writer is restarted, not kept stopped.** It runs with `listen_addresses=''`, a private socket, `autovacuum=off`, `max_wal_senders=0`, no logical workers, and `auto_reader_artifacts` / `auto_wal_index` off. The checkpointer, bgwriter and walwriter run, and the controller's own SQL sessions run catalog lookups, which can set hint bits.
4. `select_checkpoint()` (`:1204-1224`): `pagestore_branch_checkpoint()` (`pagestore.c:13395`) returns C's redo and end.
5. `archive_checkpoint()` (`:1226-1267`): `pg_switch_wal()` on the restricted writer, which writes WAL after C.
6. `wait_materializer(boundary)` (`:1291-1302`), then `pause_and_capture(keep_paused=True)` (`:1131-1160`). This is a materializer restartpoint that durably covers the paused replay position (`pagestore_capture_slru_snapshot()`; a stale restartpoint is rejected, `STALE_RESTARTPOINT` at `:29`). **fork = L =** that position, a WAL-segment boundary **after** the switch, so `L > redo(C)`.
7. `prepare_branch()` (`:1304-1353`): `pagestore_prepare_branch_from_control(...)` on the **restricted writer**. This issues CREATE_BRANCH, which samples S.
8. `success_restore()`: resume the materializer, stop the restricted writer, and start the normal writer (`restore_writer`, `:1358-1375`).

**Conclusions:**
- **There is a window.** Between C and S the writer runs (restricted), writes WAL (the switch, plus FPIs from hints if hint WAL is on), and its background processes can flush pages.
- **R1 does not apply here**, because `L ≠ redo(C)`. Applying the `wal_log_hints` + lifetime check to this flow would reject every controller branch. The flow is covered by R2.

**R2 requirements, all checked inside `pagestore_prepare_branch_from_control` (`pagestore.c:13664`):**
- **(R2-a) No transaction committed after C up to S:**
  - the parent's `ReadNextFullTransactionId()` equals C's `checkPointCopy.nextXid` (from the control image the function already resolves via `pagestore_branch_horizons_from_control`);
  - `pg_prepared_xacts` is empty.

  Checked **before and after** the CREATE_BRANCH call. If the post-check fails, the function issues BEGIN_DELETE for the new timeline and raises the error below. The prepared manifest is published only after this, so nothing can boot the branch. An unchanged nextXid means no XID was assigned in `[C, post-check]`. With no prepared transactions, no pre-C XID can commit either.
- **(R2-b) Every relation position `≤ L` is materialized before S:** the store-observed materializer marker (`pagestore_materializer_status()`, `pagestore.c:2184`; control block 3) must be `≥ L`. This is read by the writer before CREATE_BRANCH. Today `fork_lsn ≤ materialized` is checked only when the caller is the materializer itself (`pagestore.c:13697-13710`), so for the controller flow this is a **new** check.

**Proof.**
- By R2-a, no transaction commits after L until after S. Hints set before S (by the restricted writer's sessions, or by the materializer replaying up to L) concern commits at or before L. That gives G3-i.
- By R2-b, the materializer restartpoint wrote a version of every page state with `pd_lsn ≤ L` before S, because each such state was dirtied by replay at or before L and flushed by the restartpoint or earlier. So every position `≤ L` has a pre-S version. Post-S hint-only rewrites, by the normal writer after `restore_writer` or by the resumed materializer, are same-position rewrites and are hidden **whatever the hint-WAL setting**. That gives G3-ii.
- **So the controller flow is G3-free without requiring `wal_log_hints`, given R2-a/b.**

**Serialization.** R2-b is monotonic: the marker only advances, so it cannot be undone. R2-a is proven over the whole interval by the before/after checks.

#### Entry points and errors

| Entry point | Proof |
|---|---|
| `pagestore_create_branch` (`pagestore.c:1425`) | R1 |
| `pagestore_create_branch_with_incarnation` (`:1446`) | R1 |
| legacy prepare (`:13535`, `:13567`) | R1 |
| `pagestore_prepare_branch_from_control` (`:13664`) | R2 |

In all cases R1 or R2 must be satisfied. The daemon is unchanged.

Errors use `ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE`:
```
-- R1, setting
ERROR:  cannot create pagestore branch %u of timeline %u at %X/%08X
DETAIL: wal_log_hints is off on the parent; hint-bit changes are not WAL-logged, so parent hint bits written after the branch point could enter the branch.
HINT:   Set wal_log_hints = on, restart the parent, run CHECKPOINT, and create the branch at the new checkpoint's redo LSN.

-- R1, cut / lifetime
ERROR:  cannot create pagestore branch %u of timeline %u at %X/%08X
DETAIL: %X/%08X is not the redo pointer of a checkpoint completed by the parent's current postmaster (lifetime starts at %X/%08X).
HINT:   Run CHECKPOINT on the parent and branch at pg_control_checkpoint().redo_lsn.

-- R1, caller
ERROR:  cannot create pagestore branch of timeline %u from this server
DETAIL: A live-parent branch must be created by the parent's own primary; this server is %s.

-- R2
ERROR:  cannot prepare pagestore branch %u of timeline %u at %X/%08X
DETAIL: %s   -- one of: "a transaction was assigned an XID after checkpoint %X/%08X (next XID %llu, checkpoint next XID %llu)"; "prepared transactions exist"; "the materializer has made only %X/%08X durable, before the fork"
HINT:   Keep the parent writer restricted between the checkpoint and branch preparation, and retry the preparation.
```

**Override.** The compute GUC `pagestore.branch_allow_unsafe_cut` (PGC_SUSET, default off) downgrades R1 and R2 errors to WARNING, for unit harnesses that cut at synthetic LSNs. Core/IPC tests that send CREATE_BRANCH directly are unaffected, because the check is on the compute side.

#### Compatibility

- **Test and golden clusters in this tree** use `initdb` defaults (checksums on), and none sets `wal_log_hints`. Grep: all `initdb` calls in `integration_test.sh`, `mvp_golden_test.sh`, `branch_boot_test.sh`, the redo demos, `harness/pagestore_harness.py` and `harness/pagestore_pgdata_fixture.py`.
- **R1 callers** (direct `pagestore_create_branch*`) must add `wal_log_hints = on` to the writer's config. Because checksums are already on, this costs no WAL. They must also branch at a fresh `CHECKPOINT`'s redo, or set the override.
- **The controller flow (R2)** needs no configuration change. It gains the R2-a/b checks, which the existing flow already satisfies: the restricted writer runs no XID-assigning statements, and the capture precedes prepare.
- **REL_15–17:** `initdb` defaults to checksums off, but because rev 4 requires `wal_log_hints` rather than checksums, the branchdb_15/16/17 scripts only need `wal_log_hints = on` for R1 callers. R2 is unaffected.
- **Deferred alternatives:** accepting data checksums in place of `wal_log_hints` (needs a core hook against online disable); a PG-side hint scrub (clearing hints for XIDs ≥ the branch's `nextXid` at L on read-through).


---

## 8. Test plan

### 8.1 Unit tests (core)

- **`page_select`:** pre-S; post-S first arrival; multiple post-S; the **strict boundary with the escape** (the §1.2 example: first arrival seq 15 visible under `(R,10,20)`, a second arrival seq 25 hidden); the inherited range; legacy; a differential at ∞.
- **Composition:** 3-level chains, legacy edges, and a brute-force check that `admissible(composed) == ∩ admissible(constituents)` over random caps and entries, including STRICT.
- **Fork fold:** extend `ps_test_fork_event_index_selftest` with META/PAGE/UNSTAMPED/inherited mixes against a literal §1.3 reference. Keep the F5 step guard and add a post-view-META variant.
- **GROW recording:**
  - every PAGE GROW is recorded (no size-based skip);
  - the grandchild case from 4100849461;
  - the unregistered-read case from 4100994214 is now rejected with -2;
  - the restart case from 4100994236: a live horizon, a post-horizon META, then a page extension; restart; the horizon read gives nblocks covering the page;
  - the below-cutoff honest late non-extending write is admitted and its GROW recorded;
  - memory is bounded after a cutover (in-memory compaction).
- **Artifact caps** (4100849476 scenario).
- **Inherited range** after an ancestor prune.

### 8.2 Planner property tests (merge blockers)

- For random histories, fences (positional/STRICT, random `B`), floor, invalidating events, cutoff and derived fences: `select(kept) == select(all)` and `fold(kept) == fold(all)` per fence, **and again after appending random future arrivals**.
- Named cases:
  - `(20,1)/(20,5)` under `S=3`;
  - post-S-only keeps the min;
  - a fence above P;
  - collectable after release;
  - **an invalidated first arrival still defines `s_min`**;
  - control-note closure;
  - forkmeta closure;
  - GROW retention under hiding masks;
  - **plan-epoch abort**: an admission between plan and publish forces a re-plan.

### 8.3 Crash / restart

- **`branch_seq`:** persistence, the retry/reuse cases, and the **observe regression**.
- Legacy reopen as ∞.
- V4 flags, and identical `META_FIRST` after reopen.
- `S_H`.
- Artifact caps from completion records.
- **G3 lifetime floor:** `ps_lifetime_floor_lsn` is reset on postmaster restart and crash-reinit; R1 rejects a checkpoint from an earlier lifetime.
- Fault points on the V3 appends.

### 8.4 Compaction / prune integration

- The frozen view survives compaction, cutover and walidx snapshot.
- Pre-S versions are reclaimed after release (soak).
- The registration gate.

### 8.5 Op fuzzer

- `ship_wal()` returns the **end** LSN; the workaround is retired.
- The oracle reads the **branch**.
- New actions:
  - hint rewrite;
  - delayed honest write;
  - stale META;
  - unstamped META with and without floor;
  - grandchild and child-local `p ≤ B`;
  - control same-version rewrite;
  - artifact fail/retry;
  - explicit compaction/cutover;
  - pin-on-branch strict boundary;
  - **re-verify every frozen view after every prune and restart**.
- Default seed plus 20 seeds.

### 8.5a G3 checks

- **R1:**
  - `wal_log_hints = off` is rejected;
  - a cut that is not a redo pointer is rejected;
  - a checkpoint from before a restart is rejected;
  - a caller in recovery is rejected;
  - acceptance at a fresh `CHECKPOINT` redo.
- **R2**, in the `pagestore_branch_prepare.py` harness:
  - an XID-assigning statement injected on the restricted writer between the checkpoint and prepare, or during CREATE_BRANCH (fault hook), is rejected by the pre- or post-check, and the new timeline ends up DELETED;
  - an existing prepared transaction is rejected;
  - a materializer marker below the fork is rejected;
  - an end-to-end check: after `restore_writer`, run post-L commits plus hint-setting scans on the parent. The branch pages keep their pre-S bytes, with and without `wal_log_hints`.

### 8.6 System

`integration_test.sh`, `mvp_golden_test.sh` (redo cuts or overrides), `branch_boot_test.sh`, and an endurance run (note E-6).

---

## 9. Phases, sizes and risks

**Rule:** every persisted-format bump lands before the activation that depends on it, and no activation spans a format change.

| Phase | Content | Code + tests |
|---|---|---|
| **P0** | `ls_op_lsn()` `+1` fallback (PR #296) | ~60 + 0 |
| **P1** | Refactor, no behaviour change. ViewCap and composition (STRICT-exact), `TlWalk` + `B_k`, `page_select`, identity lookups, ForkEvent flags and indexes, the fold slow path, I-ALLOC/U5 asserts. All caps ∞. | ~750 + ~550 |
| **P2** | Planner. `PsViewFence`, positional/STRICT modes, closure (page/control/forkmeta), the invalidation exception, masks, derived fences, the LSN-strict ordered bump, **U3 plan-epoch validation**, property tests. | ~800 + ~850 |
| **P3a** | Formats, no activation. `TimelineRecEventV3` (writes `branch_seq`, observed but read as ∞); forkmeta V4 / FMS v2 (META/UNSTAMPED flags written); WIPG v2 / WISD v4 (`S_H` written and observed, read as ∞); `req_floor_lsn` + UNSTAMPED persistence on the daemon side; **PAGE GROW dedup removal** and in-memory GROW compaction (behaviour-neutral for views, since all caps are ∞); identities and fixtures. | ~700 + ~550, plus fixtures |
| **P3b** | Activate branches. Branch edges use `branch_seq` in reads, fences, gates and projection in **one commit**; the registration gate for branch levels; the kept child-first CREATE idempotence; ports `run_bugb_suite` and the ancestry suite. | ~300 + ~450 |
| **P4** | Activate client unstamped. `req_lsn = 0` + `req_floor_lsn`; removes P0's `+1` and `ls_zeroextend`'s stamp; `PS_SHM_VERSION` bump. | ~150 + ~200 |
| **P5** | Activate the other views. Pins positional below R (reads, fences, gate; pins required for exact-R readers); `S_H` caps in the walidx planner and fold; artifact caps and producer `(C, token)`; control-restore client seqs; §5.3 audits; ports the revision suite. | ~550 + ~450 |
| **P6** | G3 (compute side only, no daemon or core change): `ps_lifetime_floor_lsn` via the existing `control_file_write_hook`; `pagestore_branch_g3_check_and_create()` with R1 at `pagestore_create_branch*` and legacy prepare, and R2 (nextXid / prepared-xact before and after checks, materializer-marker check, BEGIN_DELETE on post-check failure) in `pagestore_prepare_branch_from_control`; errors and override; `wal_log_hints = on` plus redo cuts for R1 test callers (and REL_15–17 scripts). Also the fuzzer and the docs. | ~450 + ~450, plus docs |

Total: about 3.8k lines of code and 3.5k lines of tests, plus fixtures and docs.

**Top risks:**
1. Read/prune divergence, including lost `s_min` facts. Mitigated by one predicate for fold and masks, closure everywhere, and the §8.2 future-arrival property test.
2. Forkmeta masks, closure, derived fences and plan epoch.
3. A missing `admission_seq_observe` for `branch_seq` / `S_H`.
4. The LSN-strict ordered bump and `UINT64_MAX`-as-bare-LSN consumers (grep `fences[`, `PsPruneFence`, `retention_project_lsn`, `admission_seq = projected`).
5. Memory growth from always-recorded GROWs, bounded only by the cutover cadence. A stalled cutover (forkmeta backpressure) must surface through the existing controller.
6. Reader registration (P5): an unpinned exact-R reader now fails closed.
7. G3 R2 relies on the controller keeping the writer restricted; its checks (nextXid, prepared transactions, materializer marker) must fail closed.
8. Client/daemon lockstep (P4).
9. Three format bumps with fixtures.

### 9.1 Order-dependent optimizations removed in rev 3

| Removed | Why | Replacement |
|---|---|---|
| PAGE-class GROW dedup (`fork_event_add` size skip, the `segment_grows`-driven SEG_COMMIT marker for new records), and rev 2's `S_min(tl)` / `max_meta_seq > S_min` rule with its maintenance and recovery-order proof | Correctness depended on which views were registered and on recovery restoring `S_H` before page replay. Three consecutive findings. | Always record; merge only through the planner (§3.2, §3.7(3)). |
| Registration gate allowance "unregistered finite-S read OK if `S ≥ F.seq`" | Correct only if no retention or recording step between the read's S and the read depends on the view. Other planners (forkmeta, in-memory GROW compaction) do not consult `F.seq`. | Every finite-S read must be registered (§3.5). |

**Kept after review, and why they are not order-dependent:**
- `max_meta_seq` / `max_inherited_page_seq` fast-path gates: pure functions of the present event set, recomputed on load.
- Fence-conditional closure: it depends only on the fence set that retention already serializes on (`page_prune_lock` / admission-wr), and later views never use the escape at existing positions.
- ZEROEXTEND persist-skip: META class, view-independent, crash-equivalent.
- The exact-identity GROW collapse: identity, not state.
- Artifact `inflight` caps: correctness, rebuilt deterministically.
- Stable-seq sampling: correctness.
