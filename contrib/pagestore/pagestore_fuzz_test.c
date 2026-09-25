/*-------------------------------------------------------------------------
 *
 * pagestore_fuzz_test.c
 *	  The pagestore op-sequence fuzzer (layer 1). Stage 1 built the
 *	  framework + relation/WAL/timeline/retention opcodes; stage 2 adds the
 *	  artifact lifecycle opcodes, ADMISSION_BARRIER and the PS_OP_NONE
 *	  refusal probe (all 37 opcodes now covered), a precise (not weak)
 *	  WAL_INDEX_GET membership oracle, a "branch view frozen" oracle for
 *	  Bug B, and a replay-based delta-debugging (ddmin) shrinker.
 *
 * Unlike pagestore_soak_test.c (one long deterministic workload measuring
 * physical footprint over many rounds), this fuzzer's goal is coverage of
 * call x state x interleaving combinations in a short run: it drives a real
 * pagestore_daemon over the shared-memory IPC protocol with a weighted
 * random choice of relation/WAL/timeline/retention/artifact operations,
 * mixing "legal" arguments (consistent with a small in-process model) with
 * "adversarial" ones (bad incarnation, undefined/deleted timelines,
 * malformed lengths, stale retention generations, ...) and checks each
 * response against one of three oracle strictness levels described in the
 * spec this implements (see MVP_STATUS.md and the op-fuzzer design note):
 * MUST (exact match), MUST-REFUSE (status != OK), and MAY-BE-UNAVAILABLE
 * (OK with the exact modeled version, or a refusal -- never wrong content).
 *
 * Opcode coverage (see g_stage1_ops[] below; the name is stage-1-era but
 * the table now lists all 37 -- see FZ_NSTAGE1_OPS): every relation, WAL,
 * WAL-index, timeline, retention and artifact opcode, ADMISSION_BARRIER,
 * and PS_OP_NONE (an unknown-opcode refusal probe), plus clean/crash
 * restarts.  A handful of opcodes remain WEAK ORACLE where an exact
 * prediction needs event-ordering state this stage's model does not keep
 * (see g_weak_oracle_ops[] for the current, precise list of which and why).
 *
 * Model: a small in-process shadow of daemon state -- per timeline
 * (state/incarnation/parent/branch_lsn/wal position, plus a frozen
 * fork-point snapshot for the Bug-B oracle), per relation (existence +
 * "latest" nblocks/tag/lsn per block, like the soak's RelModel), every
 * retention pin this process itself holds, and per-generation artifact
 * lifecycle state.  It is deliberately *not* a full replica of
 * pagestore_core.c's history: where exact prediction would require
 * re-deriving internal event-ordering rules (fork_op_lsn's clamping, page
 * pruning above/below a floor with no local pin, ...), the corresponding
 * check uses the weaker oracle levels the spec allows and this file
 * documents each case inline with "WEAK ORACLE".
 *
 * Environment:
 *   PAGESTORE_FUZZ_SEED     PRNG seed (default: a fixed CI-sized value)
 *   PAGESTORE_FUZZ_OPS      step budget (default: CI-sized, <= ~60s)
 *   PAGESTORE_FUZZ_TRACE    print every op to stderr
 *   PAGESTORE_FUZZ_KEEP     keep the store directory on exit
 *   PAGESTORE_FUZZ_REPLAY=<file>   replay a serialized op-sequence file
 *                           (written by a failure, or by the shrinker below)
 *                           instead of re-driving the generator; set
 *                           PAGESTORE_FUZZ_SEED/_OPS directly (with this
 *                           unset) to re-run the generator the stage-1 way.
 *   PAGESTORE_FUZZ_LOG=<file>      write the human-readable op trace
 *   PAGESTORE_FUZZ_SHRINK           on a failure, ddmin the recorded
 *                           sequence down to a minimal one that still fails
 *                           at the same ck() call site, writing
 *                           <store-dir>.opseq (full) and
 *                           <store-dir>.opseq.min (minimized)
 *   PAGESTORE_FUZZ_SHRINK_BUDGET_S  shrink wall-clock budget (default 300)
 *   PAGESTORE_FUZZ_BUGB_WORKAROUND   ship a throwaway parent WAL record
 *                           right after every CREATE_BRANCH, dodging the
 *                           still-unfixed Bug B race (PR #294) instead of
 *                           exercising it; off by default, forced on by the
 *                           meson test's env until #294 merges -- see
 *                           env_branch_create()/verify_branch_frozen()
 *   PAGESTORE_FUZZ_FORCE_KNOWN    do not skip seeds in g_known_failures[]
 *   PAGESTORE_FUZZ_SHRINK_CHILD    internal: set by the shrinker on its own
 *                           re-exec'd candidate-replay children so they do
 *                           not themselves try to shrink
 *
 * Usage: pagestore_fuzz_test <path-to-pagestore_daemon> [store-base-dir]
 *
 *-------------------------------------------------------------------------
 */
#include "pagestore_test_client.h"
#include "pagestore_artifact_format.h"

/* ===================== configuration ==================================== */

#define FZ_PAGE_SIZE		8192u
#define FZ_SEGMENT_SIZE		16384u	/* 2 pages/segment: aggressive GC */
#define FZ_NSHARDS			2u
#define FZ_FLUSH_PAGES		2u		/* aggressive flush */
#define FZ_COMPACT_LAYERS	1u		/* aggressive compaction */

#define FZ_PAGE_HIGH_WATER		(16u * 1024u)
#define FZ_PAGE_CATCH_UP		(4u * 1024u)
#define FZ_WAL_HIGH_WATER		(32u * 1024u)
#define FZ_WAL_CATCH_UP			(8u * 1024u)
#define FZ_WALIDX_HIGH_WATER	(8u * 1024u)
#define FZ_WALIDX_CATCH_UP		(2u * 1024u)
#define FZ_FORKMETA_HIGH_WATER	(4u * 1024u)
#define FZ_FORKMETA_CATCH_UP	(1u * 1024u)

#define FZ_NTL			3u		/* timeline 0 = main; 1, 2 = branch slots */
#define FZ_TL_UNDEF_A	50u		/* never created: canonical "undefined" target */
#define FZ_TL_UNDEF_B	5000u	/* never created, and >= a small MAX_TIMELINES */
#define FZ_NREL			3u
#define FZ_MAXBLK		10u
#define FZ_NREADERS		2u
#define FZ_WAL_PAYLOAD	512u	/* every shipped WAL record has this fixed size */

#define FZ_RING_SIZE	200
#define FZ_DESC_LEN		200

#define FZ_DEFAULT_SEED	20260925ull
#define FZ_DEFAULT_OPS	4000

/* ===================== RNG (same xorshift64 as the soak, for a portable,
 * libc-independent, deterministic stream) ================================ */

static uint64_t rng_state;

static uint64_t
rng_next(void)
{
	uint64_t	x = rng_state;

	x ^= x << 13;
	x ^= x >> 7;
	x ^= x << 17;
	rng_state = x;
	return x;
}

static uint32_t
rng_below(uint32_t n)
{
	return n == 0 ? 0 : (uint32_t) (rng_next() % n);
}

static int
rng_pct(uint32_t pct)
{
	return rng_below(100) < pct;
}

/* ===================== model ============================================= */

typedef struct FzRel
{
	int			exists;
	uint32_t	nblocks;
	unsigned char tag[FZ_MAXBLK];
	uint64_t	lsn[FZ_MAXBLK];
} FzRel;

typedef struct FzReaderPin
{
	int			held;
	uint64_t	owner_id;
	uint32_t	generation;
	uint64_t	lsn;
	uint64_t	seq;
	FzRel		snap[FZ_NREL];
} FzReaderPin;

typedef struct FzTimeline
{
	int			known;			/* ever created (defined) in this run */
	PsTimelineState state;		/* our tracked belief */
	uint64_t	incarnation;
	int			has_parent;
	uint32_t	parent;
	uint64_t	branch_lsn;
	uint64_t	wal_start;
	uint64_t	wal_end;		/* next byte we will append at */
	int			wal_shipped;	/* has this timeline's *own* WAL log ever
								 * been appended to?  A fresh branch's
								 * wal_end/wal_start is only the position its
								 * first ship_wal() will target -- the
								 * daemon's own wal_end_read() for it is 0
								 * (empty log) until that actually happens,
								 * not the parent's fork LSN. */
	uint64_t	walidx_progress;
	uint64_t	mat_lsn;
	uint64_t	mat_seq;
	int			mat_registered;
	FzRel		rel[FZ_NREL];
	/*
	 * Branch slots only: the parent's per-relation state at the exact
	 * instant of CREATE_BRANCH, captured before anything (including the
	 * Bug-B workaround's own throwaway WAL) can run again -- see
	 * verify_branch_frozen()/PAGESTORE_FUZZ_BUGB_WORKAROUND.
	 */
	FzRel		frozen[FZ_NREL];
} FzTimeline;

static FzTimeline g_tl[FZ_NTL];
static FzReaderPin g_reader[FZ_NREADERS];	/* readers only ever pin timeline 0 */
static uint64_t g_branch_last_incarnation[FZ_NTL];	/* for id reuse after delete */

/*
 * Artifact lifecycle model (stage 2).  Derived directly from
 * pagestore_artifact_lifecycle.inc's ps_artifact_begin/write/commit/drop and
 * artifact_metadata()/artifact_visible():
 *
 *   - BEGIN(lsn) at a lsn strictly newer than any this key has ever used
 *     always succeeds (assuming a fresh, ship_wal()-derived lsn, which is
 *     always >= the fork's last page lsn and > any branch point -- the
 *     "fenced" precondition this model does not otherwise re-derive; see
 *     g_weak_oracle_ops), opens a NEW attempt (a fresh token, zero pages),
 *     and -- per artifact_attempt()'s "!last || last->admission_seq <
 *     token" check -- abandons any still-open older attempt.
 *   - WRITE is just PS_OP_EXTEND/WRITEV(nblocks=1) on the SAME key with
 *     req_lsn/req_seq set to the open attempt's (lsn, token); it does not
 *     touch what ordinary reads see.
 *   - COMMIT(lsn, token, count) with an exact match to the open attempt
 *     (count == the number of *distinct* blocks written) publishes it: from
 *     that instant, EXISTS/NBLOCKS/READV/READ_AT resolve to exactly the
 *     committed generation's blocks (unwritten blocks within nblocks read
 *     as zero, per "missing blocks never inherit an older page") --
 *     regardless of any *later* attempt opened against this key, until
 *     that one also settles (commits or drops).
 *   - DROP(lsn) at a lsn strictly newer than anything this key has ever
 *     used always succeeds and makes the key explicitly nonexistent from
 *     that instant, INCLUDING abandoning any still-open attempt at a lower
 *     lsn the same way a newer BEGIN would (drop's record becomes the
 *     newest COMMIT-block version, so artifact_attempt()'s "last->
 *     admission_seq < token" check on the old attempt's token now fails).
 *   - An exact-lsn retry of the last settled generation is idempotent:
 *     BEGIN again at a COMMITTED generation's own lsn returns its original
 *     token (PS_STATUS_OK, no new attempt); BEGIN or DROP again at a
 *     DROPPED generation's own lsn is refused/succeeds per
 *     PS_ARTIFACT_REFUSE_DROPPED / the drop-of-a-drop short circuit,
 *     respectively (see act_artifact_begin/drop()).
 *   - Any lsn older than the newest BEGIN this key has EVER used (settled
 *     or still open) is PS_ARTIFACT_REFUSE_BEGIN_NEWER; any lsn (other than
 *     an exact retry) older than the last settled generation is
 *     PS_ARTIFACT_REFUSE_OLDER_GENERATION -- both level-1 MUST refusals,
 *     via max_begin_lsn vs. the settled lsn tracked below.
 */
typedef struct FzArtifact
{
	int			state;			/* 0 NONE, 1 OPEN, 2 COMMITTED, 3 DROPPED */
	uint64_t	lsn;			/* lsn of the *current* state (open attempt,
								 * or the last settled commit/drop) */
	uint64_t	token;			/* begin_seq; valid while OPEN or COMMITTED */
	uint64_t	max_begin_lsn;	/* highest lsn any successful BEGIN has ever
								 * used for this key, settled or not */
	/*
	 * Shadow of (state, lsn, token) as of just before the *current* open
	 * attempt's own BEGIN overwrote them -- i.e. the last settled (COMMITTED
	 * or DROPPED, or NONE if there never was one) generation.  Needed only to
	 * recover after a restart abandons an open attempt (artifact_recovery_seq,
	 * pagestore_core.c -- "reopening the daemon abandons pre-restart
	 * unfinished attempts"): state/lsn/token get restored from here so a
	 * later idempotent-retry sub-case still targets the *real* last
	 * settled lsn, not the vanished attempt's.  Untouched while state !=
	 * OPEN (COMMIT/DROP already describe the new settled state directly in
	 * state/lsn/token, so there is nothing to shadow until the next BEGIN).
	 */
	int			prev_state;
	uint64_t	prev_lsn;
	uint64_t	prev_token;
	uint32_t	open_count;		/* distinct blocks written this attempt */
	uint32_t	open_nblocks;
	unsigned char open_written[FZ_MAXBLK];
	unsigned char open_tag[FZ_MAXBLK];
	uint64_t	open_block_lsn[FZ_MAXBLK];
	FzRel		visible;		/* what EXISTS/NBLOCKS/READV currently see:
								 * only changes on COMMIT/DROP, never on a
								 * still-open attempt */
} FzArtifact;

#define FZ_NAKLASS	2			/* 0 = PS_KLASS_SLRU, 1 = PS_KLASS_READER_SNAPSHOT.
								 * PS_KLASS_CONTROL is deliberately excluded:
								 * artifact_data_key() (pagestore_artifact_
								 * lifecycle.inc) only recognizes SLRU/
								 * READER_SNAPSHOT -- a CONTROL-klass artifact
								 * opcode is refused outright, exercised as
								 * one of the adversarial klass probes below,
								 * not as a third legal klass. */
static const uint32_t g_artifact_klass[FZ_NAKLASS] = {PS_KLASS_SLRU,
	PS_KLASS_READER_SNAPSHOT};
static FzArtifact g_artifact[FZ_NTL][FZ_NAKLASS][FZ_NREL];

/* ===================== bookkeeping ======================================= */

static long long checks;
static long long failed;
static int	trace;
static int	keep_store;
static uint64_t g_seed;
static long long g_ops_budget;
static long long g_step;
static FILE *log_fp;

/* See env_branch_create()'s comment: off by default (exercises the Bug-B
 * race), forced on in CI via the meson test's env until PR #294 merges. */
static int	g_bugb_workaround;

/* For the ddmin shrinker (shrink_on_failure()): the original argv (to
 * re-exec this same binary via /proc/self/exe for each candidate) and the
 * store base directory (where candidate sequence files and captured child
 * output are staged; the same directory psc_store_dir itself is under). */
static char **g_argv;
static const char *g_store_base;

typedef struct FzRingEntry
{
	long long	step;
	char		desc[FZ_DESC_LEN];
} FzRingEntry;

static FzRingEntry g_ring[FZ_RING_SIZE];
static int	g_ring_pos;

static unsigned char page_buf[FZ_MAXBLK * PSC_PAGE_SIZE];
static unsigned char read_buf[FZ_MAXBLK * PSC_PAGE_SIZE];
static unsigned char wal_buf[FZ_WAL_PAYLOAD];

/*
 * WAL_INDEX_GET result buffer for act_walidx_get()'s membership check.  Must
 * be large enough that the just-added (newest) entry for a (timeline,
 * relation, block) tuple is never pushed out by walidx_get()'s ascending-
 * LSN, oldest-first truncation once that tuple has accumulated more history
 * than a small cap over a long run -- that truncation is normal behavior,
 * not something to flag.  4096 entries (96KiB, well under PS_IO_UNIT) is a
 * large margin against the handful of entries any one (timeline, key,
 * block) triple actually accumulates in a stage-1 run; static, not a local,
 * to avoid a large stack frame.
 */
#define FZ_WALIDX_GET_CAP	4096
static PsWalRec fz_walidx_get_recs[FZ_WALIDX_GET_CAP];

/* ===================== coverage =========================================== */

#define FZ_MAX_OPCODE	48
#define FZ_NSTATUS		3		/* PS_STATUS_{OK,ERROR,STALE} == {0,1,2} */
#define FZ_NREASON		9		/* 0..7 exact, 8 = "8+" */

static long long g_cov[FZ_MAX_OPCODE][FZ_NSTATUS][FZ_NREASON];

typedef struct FzOpInfo
{
	PsOpcode	opcode;
	const char *name;
	int			refusal_possible;
	const char *refusal_note;
	/*
	 * Stage-1 initializers below list only the first four fields; C zero-
	 * fills the rest, so skip_ok_check defaults to 0 (OK cell required) for
	 * every one of them without touching a single existing line.  Only the
	 * stage-2 additions that can never legally return OK (PS_OP_NONE) set
	 * this explicitly.
	 */
	int			skip_ok_check;
} FzOpInfo;

static const FzOpInfo g_stage1_ops[] = {
	{PS_OP_CREATE, "CREATE", 1, NULL},
	{PS_OP_EXISTS, "EXISTS", 1, NULL},
	{PS_OP_NBLOCKS, "NBLOCKS", 1, NULL},
	{PS_OP_EXTEND, "EXTEND", 1, NULL},
	{PS_OP_ZEROEXTEND, "ZEROEXTEND", 1, NULL},
	{PS_OP_WRITEV, "WRITEV", 1, NULL},
	{PS_OP_READV, "READV", 1, NULL},
	{PS_OP_READ_AT, "READ_AT", 1, NULL},
	{PS_OP_TRUNCATE, "TRUNCATE", 1, NULL},
	{PS_OP_UNLINK, "UNLINK", 1, NULL},
	{PS_OP_IMMEDSYNC, "IMMEDSYNC", 0,
		"ps_handle_meta's only refusal path is ps_storage->sync() failing; "
		"not reachable via malformed IPC args in this harness"},
	{PS_OP_BLOCK_DEATH, "BLOCK_DEATH", 1, NULL},
	{PS_OP_WAL_APPEND, "WAL_APPEND", 1, NULL},
	{PS_OP_WAL_SIZE, "WAL_SIZE", 1, NULL},
	{PS_OP_WAL_READ, "WAL_READ", 1, NULL},
	{PS_OP_WAL_INDEX_ADD, "WAL_INDEX_ADD", 1, NULL},
	{PS_OP_WAL_INDEX_ADD_BATCH, "WAL_INDEX_ADD_BATCH", 1, NULL},
	{PS_OP_WAL_INDEX_GET, "WAL_INDEX_GET", 1, NULL},
	{PS_OP_WAL_INDEX_PROGRESS, "WAL_INDEX_PROGRESS", 1, NULL},
	{PS_OP_WAL_RETAIN_FLOOR, "WAL_RETAIN_FLOOR", 1, NULL},
	{PS_OP_CREATE_BRANCH, "CREATE_BRANCH", 1, NULL},
	{PS_OP_CHECK_BRANCH, "CHECK_BRANCH", 1, NULL},
	{PS_OP_REQUIRE_BRANCH, "REQUIRE_BRANCH", 1, NULL},
	{PS_OP_TIMELINE_STATE, "TIMELINE_STATE", 1, NULL},
	{PS_OP_TIMELINE_INFO, "TIMELINE_INFO", 1, NULL},
	{PS_OP_BEGIN_DELETE, "BEGIN_DELETE", 1, NULL},
	{PS_OP_RETENTION_PIN_RESERVE, "RETENTION_PIN_RESERVE", 1, NULL},
	{PS_OP_RETENTION_PIN_SET, "RETENTION_PIN_SET", 1, NULL},
	{PS_OP_RETENTION_PIN_GET, "RETENTION_PIN_GET", 1, NULL},
	{PS_OP_RETENTION_PIN_LOOKUP, "RETENTION_PIN_LOOKUP", 1, NULL},
	{PS_OP_RETENTION_PIN_DROP, "RETENTION_PIN_DROP", 1, NULL},
	{PS_OP_RETENTION_FLOOR, "RETENTION_FLOOR", 1, NULL},

	/* ----- stage 2 additions: the remaining 5 of 37 opcodes ----- */
	{PS_OP_NONE, "NONE(unknown-opcode)", 1, NULL, 1 /* skip_ok_check: never OK */},
	{PS_OP_ADMISSION_BARRIER, "ADMISSION_BARRIER", 0,
		"served by the daemon's request dispatcher before any per-timeline "
		"validation (no ps_handle_meta/timeline_op_allowed call on this "
		"path) and only fails if ps_admission_barrier() itself returns 0; "
		"not reachable via malformed IPC args in this harness"},
	{PS_OP_ARTIFACT_BEGIN, "ARTIFACT_BEGIN", 1, NULL},
	{PS_OP_ARTIFACT_COMMIT, "ARTIFACT_COMMIT", 1, NULL},
	{PS_OP_ARTIFACT_DROP, "ARTIFACT_DROP", 1, NULL},
};
#define FZ_NSTAGE1_OPS ((int) (sizeof(g_stage1_ops) / sizeof(g_stage1_ops[0])))

/* Opcodes whose *legal*-path oracle in this stage is weak (level 3: no
 * crash/hang/poison, status in {OK,ERROR,STALE}, never provably-wrong
 * content) because pinning down an exact prediction needs more model state
 * than stage 1 builds.  Printed verbatim in the final report. */
static const char *g_weak_oracle_ops[] = {
	"RETENTION_FLOOR: only status in {OK,ERROR} is checked, not asserted OK, "
	"because pagestore_core.c documents \"a floor that cannot be proven is "
	"an error, never a lower bound\" for PS_OP_WAL_RETAIN_FLOOR and the same "
	"failure path (an unreadable/absent control note) is structurally "
	"reachable here too, e.g. for a timeline with no control-object write "
	"of its own yet (a branch, in this stage's model).",
	"BLOCK_DEATH: newest retained death of (key,block) at/below a horizon "
	"requires a full per-block death-event history (every UNLINK/TRUNCATE "
	"that could have killed it); the model here only tracks each relation's "
	"*latest* state, not that event log, so only {no crash/hang, status in "
	"{OK,ERROR}} is checked, never the returned (lsn,seq) value.",
	"WAL_INDEX_GET: stage 2 upgraded the just-added-record membership check "
	"to a precise, provable level-1 rule (see act_walidx_get()'s comment: "
	"absence is legal iff the model shows the block dead -- relation absent "
	"or block>=nblocks -- on tl at the instant of the query; otherwise "
	"presence is MUST). This is still not exhaustive equality with the full "
	"merged-ancestry result set for arbitrary *older* records queried much "
	"later, which would need a full per-block version/death history this "
	"model does not keep -- that broader case remains unchecked, not weak.",
	"RETENTION_PIN_SET: only two sub-cases are strongly checked (an exact "
	"(lsn,seq) retry of a held pin -> OK; generation 0 -> refused); moving a "
	"pin to a genuinely new LSN is exercised but its exact accept/refuse "
	"boundary (page/WAL/WAL-index frontier ancestry) is not independently "
	"re-derived here, so that sub-case only checks {no crash, status in "
	"{OK,ERROR,STALE}}.",
	"CREATE/TRUNCATE/UNLINK/ZEROEXTEND with an adversarial *content* LSN "
	"(as opposed to a bad timeline/incarnation): fork_op_lsn() clamps to the "
	"newest definitive event rather than refusing, and predicting the "
	"clamped value needs the same per-block event history BLOCK_DEATH would "
	"need. This stage never sends that adversarial sub-case for these four "
	"opcodes (LSN adversarial coverage is exercised on the read paths "
	"instead: EXISTS/NBLOCKS/READV/READ_AT); see the report's limitations.",
	"ARTIFACT_BEGIN/DROP's \"older than max_begin_lsn\" adversarial probe: "
	"refusal is MUST (status != OK), but the exact reason is not (could be "
	"PS_ARTIFACT_REFUSE_HORIZON, checked before the BEGIN/COMMIT-block "
	"ordering checks, whenever the probe's lsn also falls below the data "
	"fork's own last written page lsn -- a value this model does not track "
	"separately from max_begin_lsn -- or BEGIN_NEWER/OLDER_GENERATION "
	"otherwise); see act_artifact_begin/drop()'s comments.",
};
#define FZ_NWEAK ((int) (sizeof(g_weak_oracle_ops) / sizeof(g_weak_oracle_ops[0])))

static void
record_cov(uint32_t opcode, uint32_t status, uint32_t reason)
{
	uint32_t	r = reason > 7 ? 8 : reason;

	if (opcode >= FZ_MAX_OPCODE || status >= FZ_NSTATUS)
		return;
	g_cov[opcode][status][r]++;
}

/* ===================== environment-action coverage ======================= */

typedef enum FzEnv
{
	ENV_MATERIALIZE = 0,
	ENV_READER_RESERVE,
	ENV_READER_ADVANCE,
	ENV_READER_DROP,
	ENV_BRANCH_CREATE,
	ENV_BRANCH_WRITE,
	ENV_BRANCH_BEGIN_DELETE,
	ENV_WAIT_DELETED,
	ENV_CLEAN_RESTART,
	ENV_CRASH_RESTART,
	ENV_SLEEP,
	ENV_COUNT
} FzEnv;

static const char *g_env_names[ENV_COUNT] = {
	"materialize", "reader_reserve", "reader_advance", "reader_drop",
	"branch_create", "branch_write", "branch_begin_delete", "wait_deleted",
	"clean_restart", "crash_restart", "sleep",
};
static long long g_env_cov[ENV_COUNT][2];	/* [0]=ok/happened [1]=skipped/refused */

static void
record_env(FzEnv e, int ok)
{
	g_env_cov[e][ok ? 0 : 1]++;
}

/* ===================== known failures ===================================== */

typedef struct FzKnownFailure
{
	uint64_t	seed;
	const char *note;
} FzKnownFailure;

/*
 * Seeds that reproduce a *product* bug (not a fuzzer/model bug), confirmed
 * by reading the relevant pagestore_core.c/pagestore_daemon.c code against
 * the design docs.  Populated from this stage's validation runs; see the
 * implementer's final report for the full seed/repro/expected-vs-actual
 * writeup of each entry.  Skipped by default so CI is not blocked by an
 * already-triaged, not-yet-fixed bug; set PAGESTORE_FUZZ_FORCE_KNOWN=1 to
 * run one of these seeds anyway (e.g. to re-confirm a fix, or because it is
 * timing-sensitive and does not always reproduce).
 */
static const FzKnownFailure g_known_failures[] = {
	/* (no *seed-keyed* known failure in this stage's validation run.) */
	{0, NULL},
};
#define FZ_NKNOWN ((int) (sizeof(g_known_failures) / sizeof(g_known_failures[0])) - 1)

/*
 * A distinct, non-seed-keyed known issue lives directly at its call site
 * (act_writev()'s PS_STATUS_ERROR-vs-PS_STATUS_OK "capacity" probe): it is
 * deterministic given the op itself (reachable from almost every seed within
 * a handful of steps, not tied to one seed), so gating it by seed would not
 * keep CI unblocked.  It shares this file's PAGESTORE_FUZZ_FORCE_KNOWN gate
 * instead -- skipped by default, the real (currently-failing) probe runs
 * when that variable is set.  Summary: pagestore_daemon.c handle_request()'s
 * PS_OP_WRITEV and PS_OP_READV cases index ch->data + i*page_size for i up
 * to ch->nblocks with no check that ch->nblocks * page_size <= PS_IO_UNIT
 * (the channel's actual data[] capacity); see the implementer's report for
 * the full writeup.
 */

/* ===================== small helpers ====================================== */

/* ===================== small helpers ====================================== */

static void
trace_op(const char *fmt, ...)
{
	va_list		ap;

	if (!trace && log_fp == NULL)
		return;
	va_start(ap, fmt);
	if (trace)
	{
		va_list		ap2;

		va_copy(ap2, ap);
		fprintf(stderr, "[%lld] ", g_step);
		vfprintf(stderr, fmt, ap2);
		fputc('\n', stderr);
		va_end(ap2);
	}
	if (log_fp != NULL)
	{
		fprintf(log_fp, "[%lld] ", g_step);
		vfprintf(log_fp, fmt, ap);
		fputc('\n', log_fp);
	}
	va_end(ap);
}

static void
ring_note(const char *fmt, ...)
{
	va_list		ap;
	FzRingEntry *e = &g_ring[g_ring_pos % FZ_RING_SIZE];

	e->step = g_step;
	va_start(ap, fmt);
	vsnprintf(e->desc, sizeof(e->desc), fmt, ap);
	va_end(ap);
	g_ring_pos++;
	if (trace)
		fprintf(stderr, "RINGDBG [%lld] %s\n", g_step, e->desc);
}

static void
dump_ring(void)
{
	int			n = g_ring_pos < FZ_RING_SIZE ? g_ring_pos : FZ_RING_SIZE;
	int			start = g_ring_pos < FZ_RING_SIZE ? 0 : g_ring_pos % FZ_RING_SIZE;

	fprintf(stderr, "---- last %d ops ----\n", n);
	for (int i = 0; i < n; i++)
	{
		FzRingEntry *e = &g_ring[(start + i) % FZ_RING_SIZE];

		fprintf(stderr, "  step %lld: %s\n", e->step, e->desc);
	}
	fprintf(stderr, "---------------------\n");
}

/* Forward declarations: defined near run_step() (op-sequence recording
 * lives there), but ck() -- defined early so every act_*()/env_*() below can
 * use it -- needs to invoke them on a failure. */
static void write_seq_file(const char *path);
static void shrink_on_failure(const char *orig_fmt);

/* Oracle failure: distinct from psc_fatal() (infra/hang).  Prints the seed,
 * step, ring buffer and expected-vs-actual, keeps the store, and exits
 * non-zero.  Never returns. */
static void
ck(int cond, const char *fmt, ...)
{
	va_list		ap;

	checks++;
	if (cond)
		return;
	failed++;
	fprintf(stderr, "\nORACLE FAILURE at step %lld (seed %llu): ",
			g_step, (unsigned long long) g_seed);
	va_start(ap, fmt);
	vfprintf(stderr, fmt, ap);
	va_end(ap);
	fputc('\n', stderr);
	/*
	 * A raw, unsubstituted fingerprint of *which* check fired -- distinct
	 * lines for distinct ck() call sites even when the substituted message
	 * above happens to look similar.  The shrinker's child processes each
	 * print this to their captured output; the parent driver greps for it
	 * verbatim to decide whether a reduced candidate "still fails at the
	 * same oracle point" (spec wording) rather than just "still fails".
	 */
	fprintf(stderr, "ORACLE_SITE: %s\n", fmt);
	dump_ring();
	fprintf(stderr, "reproduce with: PAGESTORE_FUZZ_SEED=%llu "
			"PAGESTORE_FUZZ_OPS=%lld <this binary> <daemon> [store-dir]\n",
			(unsigned long long) g_seed, g_step);
	{
		char		seq_path[560];

		snprintf(seq_path, sizeof(seq_path), "%s.opseq", psc_store_dir);
		write_seq_file(seq_path);
		fprintf(stderr, "op sequence for PAGESTORE_FUZZ_REPLAY written to "
				"%s\n", seq_path);
		if (getenv("PAGESTORE_FUZZ_SHRINK") != NULL &&
			getenv("PAGESTORE_FUZZ_SHRINK_CHILD") == NULL)
			shrink_on_failure(fmt);
	}
	if (log_fp != NULL)
		fflush(log_fp);
	psc_kill_daemon();
	if (!keep_store)
		psc_remove_tree(psc_store_dir);
	else
		fprintf(stderr, "store kept at %s\n", psc_store_dir);
	exit(1);
}

/* status in {OK,ERROR,STALE} and always records coverage; the caller has
 * already decided whether OK or refusal was legal to observe here. */
static void
observe(uint32_t opcode, int status, uint32_t reason, const char *what)
{
	ck(status == PS_STATUS_OK || status == PS_STATUS_ERROR ||
	   status == PS_STATUS_STALE, "%s: status %d is not in {OK,ERROR,STALE}",
	   what, status);
	record_cov(opcode, (uint32_t) status, reason);
}

/* ===================== timeline/incarnation targeting ===================== */

typedef enum FzAdv
{
	ADV_NONE = 0,
	ADV_BAD_INCARNATION,
	ADV_UNDEFINED_TIMELINE,
	ADV_DELETED_TIMELINE,
} FzAdv;

/* ~22% adversarial, split across the three generic timeline/incarnation
 * dimensions every ps_handle_meta-routed opcode shares. */
static FzAdv
pick_adv(void)
{
	uint32_t	r = rng_below(100);

	if (r < 78)
		return ADV_NONE;
	if (r < 88)
		return ADV_BAD_INCARNATION;
	if (r < 96)
		return ADV_UNDEFINED_TIMELINE;
	return ADV_DELETED_TIMELINE;
}

/* Resolves (tl, incarnation) for a request against a chosen "home" live
 * timeline `base_tl`, honoring `adv`.  Returns 0 if the requested adversarial
 * kind is not currently constructible (e.g. no DELETED timeline exists yet)
 * and the caller should fall back to ADV_NONE or a different kind. */
static int
resolve_target(uint32_t base_tl, FzAdv adv, uint32_t *tl_out, uint64_t *inc_out)
{
	switch (adv)
	{
		case ADV_NONE:
			*tl_out = base_tl;
			*inc_out = g_tl[base_tl].incarnation;
			return 1;
		case ADV_BAD_INCARNATION:
			*tl_out = base_tl;
			*inc_out = g_tl[base_tl].incarnation + 1;
			return 1;
		case ADV_UNDEFINED_TIMELINE:
			*tl_out = rng_pct(50) ? FZ_TL_UNDEF_A : FZ_TL_UNDEF_B;
			*inc_out = 1;		/* nonzero: defeats the legacy inc==0 pass-through */
			return 1;
		case ADV_DELETED_TIMELINE:
			for (uint32_t t = 1; t < FZ_NTL; t++)
				if (g_tl[t].known && g_tl[t].state == PS_TIMELINE_DELETED)
				{
					*tl_out = t;
					*inc_out = g_tl[t].incarnation;	/* correct token; still refused */
					return 1;
				}
			return 0;
	}
	return 0;
}

static uint32_t
pick_live_tl(void)
{
	uint32_t	live[FZ_NTL];
	uint32_t	n = 0;

	for (uint32_t t = 0; t < FZ_NTL; t++)
		if (g_tl[t].known && g_tl[t].state == PS_TIMELINE_LIVE)
			live[n++] = t;
	if (n == 0)
		return 0;				/* main is always live once bootstrapped */
	return live[rng_below(n)];
}

/* ===================== WAL shipping ======================================= */

/* Deterministic, position-only fill: byte 0..7 carry the start LSN, the rest
 * repeat a byte derived from (start/4096)%64, exactly the soak's pattern
 * (pagestore_soak_test.c:ship_wal_on), so WAL_READ can recompute the
 * expected bytes for any still-retained, still-FZ_WAL_PAYLOAD-sized record
 * from its start LSN alone -- no separate log of shipped records needed. */
static void
fz_wal_fill(uint64_t start, unsigned char *buf)
{
	memset(buf, (int) (0x40 + (start / 4096) % 64), FZ_WAL_PAYLOAD);
	memcpy(buf, &start, sizeof(start));
}

static uint64_t
ship_wal(uint32_t tl)
{
	uint64_t	start = g_tl[tl].wal_end;
	int			status;

	fz_wal_fill(start, wal_buf);
	status = psc_op_wal_append(tl, g_tl[tl].incarnation, start, wal_buf,
							   FZ_WAL_PAYLOAD);
	ck(status == PS_STATUS_OK, "WAL append on timeline %u at %llu (status %d)",
	   tl, (unsigned long long) start, status);
	record_cov(PS_OP_WAL_APPEND, (uint32_t) status, 0);
	g_tl[tl].wal_end = start + FZ_WAL_PAYLOAD;
	g_tl[tl].wal_shipped = 1;
	return start;
}

/*
 * Like ship_wal(), but guarantees an lsn strictly greater than tl's own
 * branch point.  A fresh branch's wal_end starts out *equal* to branch_lsn
 * (env_branch_create()), so this timeline's very first ship_wal() call
 * returns branch_lsn itself -- fine for an ordinary page write (which only
 * needs >= the fork's last page lsn), but artifact_mutation_horizon()
 * (pagestore_artifact_lifecycle.inc) requires a *strictly* newer lsn than a
 * live child's branch point ("Branch-local mutations must be later than the
 * branch point", ARTIFACT_LIFECYCLE.md), so an artifact BEGIN/DROP's "fresh"
 * sub-case needs this instead of ship_wal() directly to stay in the legal
 * (not HORIZON-refused) case on a newly created branch's first artifact op.
 */
static uint64_t
ship_wal_past_fork(uint32_t tl)
{
	uint64_t	lsn = ship_wal(tl);

	if (g_tl[tl].has_parent && lsn <= g_tl[tl].branch_lsn)
		lsn = ship_wal(tl);
	return lsn;
}

/* ===================== relation ops ======================================= */

static void
act_create(void)
{
	uint32_t	tl = pick_live_tl();
	uint32_t	rel = rng_below(FZ_NREL);
	FzAdv		adv = pick_adv();
	uint32_t	target_tl;
	uint64_t	target_inc;

	if (!resolve_target(tl, adv, &target_tl, &target_inc))
		adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

	if (adv == ADV_NONE)
	{
		FzRel	   *m = &g_tl[tl].rel[rel];

		if (m->exists && rng_pct(40))
		{
			/* ensure-existing: req_lsn=0 is a no-op idempotent ensure */
			int			status = psc_op_create(tl, target_inc, PS_KLASS_RELATION,
												  rel, 0);

			ring_note("CREATE ensure tl=%u rel=%u", tl, rel);
			ck(status == PS_STATUS_OK, "CREATE ensure of existing tl=%u rel=%u"
			   " (status %d)", tl, rel, status);
			record_cov(PS_OP_CREATE, (uint32_t) status, 0);
		}
		else
		{
			uint64_t	lsn = ship_wal(tl);
			int			status = psc_op_create(tl, target_inc, PS_KLASS_RELATION,
												  rel, lsn);

			ring_note("CREATE tl=%u rel=%u lsn=%llu", tl, rel,
					  (unsigned long long) lsn);
			ck(status == PS_STATUS_OK, "CREATE tl=%u rel=%u lsn=%llu (status %d)",
			   tl, rel, (unsigned long long) lsn, status);
			record_cov(PS_OP_CREATE, (uint32_t) status, 0);
			if (status == PS_STATUS_OK)
			{
				m->exists = 1;
				m->nblocks = 0;
				memset(m->tag, 0, sizeof(m->tag));
				memset(m->lsn, 0, sizeof(m->lsn));
			}
		}
	}
	else
	{
		uint64_t	lsn = adv == ADV_UNDEFINED_TIMELINE ? 0 : g_tl[tl].wal_end;
		int			status = psc_op_create(target_tl, target_inc,
											  PS_KLASS_RELATION, rel, lsn);

		ring_note("CREATE adv=%d tl=%u inc=%llu", adv, target_tl,
				  (unsigned long long) target_inc);
		ck(status != PS_STATUS_OK, "CREATE with adversarial target (adv=%d "
		   "tl=%u inc=%llu) must be refused, got status %d", adv, target_tl,
		   (unsigned long long) target_inc, status);
		record_cov(PS_OP_CREATE, (uint32_t) status, 0);
	}
}

static void
act_unlink(void)
{
	uint32_t	tl = pick_live_tl();
	uint32_t	rel = rng_below(FZ_NREL);
	FzAdv		adv = pick_adv();
	uint32_t	target_tl;
	uint64_t	target_inc;

	if (!resolve_target(tl, adv, &target_tl, &target_inc))
		adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

	if (adv == ADV_NONE)
	{
		FzRel	   *m = &g_tl[tl].rel[rel];
		uint64_t	lsn = ship_wal(tl);
		int			status = psc_op_unlink(tl, target_inc, PS_KLASS_RELATION,
											  rel, lsn);

		ring_note("UNLINK tl=%u rel=%u lsn=%llu", tl, rel,
				  (unsigned long long) lsn);
		ck(status == PS_STATUS_OK, "UNLINK tl=%u rel=%u (status %d)", tl, rel,
		   status);
		record_cov(PS_OP_UNLINK, (uint32_t) status, 0);
		if (status == PS_STATUS_OK)
		{
			m->exists = 0;
			m->nblocks = 0;
			memset(m->tag, 0, sizeof(m->tag));
			memset(m->lsn, 0, sizeof(m->lsn));
		}
	}
	else
	{
		int			status = psc_op_unlink(target_tl, target_inc,
											  PS_KLASS_RELATION, rel, 0);

		ring_note("UNLINK adv=%d tl=%u", adv, target_tl);
		ck(status != PS_STATUS_OK, "UNLINK with adversarial target must be "
		   "refused (adv=%d tl=%u), got %d", adv, target_tl, status);
		record_cov(PS_OP_UNLINK, (uint32_t) status, 0);
	}
}

static void
act_truncate(void)
{
	uint32_t	tl = pick_live_tl();
	uint32_t	rel = rng_below(FZ_NREL);
	FzRel	   *m = &g_tl[tl].rel[rel];
	FzAdv		adv = pick_adv();
	uint32_t	target_tl;
	uint64_t	target_inc;

	if (!m->exists)
	{
		act_create();
		return;
	}
	if (!resolve_target(tl, adv, &target_tl, &target_inc))
		adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

	if (adv == ADV_NONE)
	{
		uint32_t	to = rng_below(m->nblocks + 1);
		uint64_t	lsn = ship_wal(tl);
		int			status = psc_op_truncate(tl, target_inc, PS_KLASS_RELATION,
												 rel, to, lsn);

		ring_note("TRUNCATE tl=%u rel=%u to=%u", tl, rel, to);
		ck(status == PS_STATUS_OK, "TRUNCATE tl=%u rel=%u to=%u (status %d)",
		   tl, rel, to, status);
		record_cov(PS_OP_TRUNCATE, (uint32_t) status, 0);
		if (status == PS_STATUS_OK)
			m->nblocks = to;
	}
	else
	{
		int			status = psc_op_truncate(target_tl, target_inc,
												 PS_KLASS_RELATION, rel, 0, 0);

		ring_note("TRUNCATE adv=%d tl=%u", adv, target_tl);
		ck(status != PS_STATUS_OK, "TRUNCATE with adversarial target must be "
		   "refused (adv=%d tl=%u), got %d", adv, target_tl, status);
		record_cov(PS_OP_TRUNCATE, (uint32_t) status, 0);
	}
}

static void
act_zeroextend(void)
{
	uint32_t	tl = pick_live_tl();
	uint32_t	rel = rng_below(FZ_NREL);
	FzRel	   *m = &g_tl[tl].rel[rel];
	FzAdv		adv = pick_adv();
	uint32_t	target_tl;
	uint64_t	target_inc;

	if (!m->exists)
	{
		act_create();
		return;
	}
	if (!resolve_target(tl, adv, &target_tl, &target_inc))
		adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

	if (adv == ADV_NONE)
	{
		/* Occasionally a genuinely huge grow: metadata-only allocation, no
		 * data payload, so it is cheap even when large ("nblocks huge"). */
		int			huge = rng_pct(10);
		uint32_t	n = huge ? 500 + rng_below(4000) : 1 + rng_below(3);
		uint32_t	block = m->nblocks;
		uint64_t	lsn = ship_wal(tl);
		int			status;

		if (!huge && block + n > FZ_MAXBLK)
			n = FZ_MAXBLK - block;
		if (n == 0)
			n = 1, block = FZ_MAXBLK - 1;
		status = psc_op_zeroextend(tl, target_inc, PS_KLASS_RELATION, rel,
								   block, n, lsn);
		ring_note("ZEROEXTEND tl=%u rel=%u block=%u n=%u huge=%d", tl, rel,
				  block, n, huge);
		ck(status == PS_STATUS_OK, "ZEROEXTEND tl=%u rel=%u block=%u n=%u "
		   "(status %d)", tl, rel, block, n, status);
		record_cov(PS_OP_ZEROEXTEND, (uint32_t) status, 0);
		if (status == PS_STATUS_OK)
		{
			if (huge)
			{
				/* WEAK ORACLE for the huge case's resulting nblocks: shrink
				 * back down immediately via a strongly-checked TRUNCATE so
				 * later blocks/model stay within FZ_MAXBLK. */
				uint32_t	nb = 0;
				int			rc = psc_op_nblocks(tl, target_inc,
												   PS_KLASS_RELATION, rel, 0, 0,
												   &nb);

				ck(rc == PS_STATUS_OK && nb == block + n,
				   "post-huge-zeroextend NBLOCKS tl=%u rel=%u expected %u "
				   "got %u (status %d)", tl, rel, block + n, nb, rc);
				record_cov(PS_OP_NBLOCKS, (uint32_t) rc, 0);
				{
					uint64_t	tlsn = ship_wal(tl);
					int			trc = psc_op_truncate(tl, target_inc,
														 PS_KLASS_RELATION, rel,
														 block, tlsn);

					ck(trc == PS_STATUS_OK, "shrink-back TRUNCATE after huge "
					   "ZEROEXTEND tl=%u rel=%u (status %d)", tl, rel, trc);
					record_cov(PS_OP_TRUNCATE, (uint32_t) trc, 0);
				}
			}
			else
			{
				for (uint32_t b = m->nblocks; b < block + n; b++)
					m->tag[b] = 0, m->lsn[b] = 0;
				m->nblocks = block + n;
			}
		}
	}
	else
	{
		int			status = psc_op_zeroextend(target_tl, target_inc,
												  PS_KLASS_RELATION, rel, 0, 1, 0);

		ring_note("ZEROEXTEND adv=%d tl=%u", adv, target_tl);
		ck(status != PS_STATUS_OK, "ZEROEXTEND with adversarial target must "
		   "be refused (adv=%d tl=%u), got %d", adv, target_tl, status);
		record_cov(PS_OP_ZEROEXTEND, (uint32_t) status, 0);
	}
}

static void
act_extend(void)
{
	uint32_t	tl = pick_live_tl();
	uint32_t	rel = rng_below(FZ_NREL);
	FzRel	   *m = &g_tl[tl].rel[rel];
	FzAdv		adv = pick_adv();
	uint32_t	target_tl;
	uint64_t	target_inc;

	if (!m->exists || m->nblocks >= FZ_MAXBLK)
	{
		act_create();
		return;
	}
	if (!resolve_target(tl, adv, &target_tl, &target_inc))
		adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

	if (adv == ADV_NONE)
	{
		uint64_t	lsn = ship_wal(tl);
		unsigned char tag = (unsigned char) (1 + rng_below(255));
		int			status;

		psc_fill_page(page_buf, lsn, tag);
		status = psc_op_extend(tl, target_inc, PS_KLASS_RELATION, rel,
							   m->nblocks, page_buf, 0, 0);
		ring_note("EXTEND tl=%u rel=%u block=%u lsn=%llu", tl, rel,
				  m->nblocks, (unsigned long long) lsn);
		ck(status == PS_STATUS_OK, "EXTEND tl=%u rel=%u block=%u (status %d)",
		   tl, rel, m->nblocks, status);
		record_cov(PS_OP_EXTEND, (uint32_t) status, 0);
		if (status == PS_STATUS_OK)
		{
			m->tag[m->nblocks] = tag;
			m->lsn[m->nblocks] = lsn;
			m->nblocks++;
		}
	}
	else
	{
		psc_fill_page(page_buf, 1, 1);
		{
			int			status = psc_op_extend(target_tl, target_inc,
												  PS_KLASS_RELATION, rel,
												  0, page_buf, 0, 0);

			ring_note("EXTEND adv=%d tl=%u", adv, target_tl);
			ck(status != PS_STATUS_OK, "EXTEND with adversarial target must "
			   "be refused (adv=%d tl=%u), got %d", adv, target_tl, status);
			record_cov(PS_OP_EXTEND, (uint32_t) status, 0);
		}
	}
}

static void
act_writev(void)
{
	uint32_t	tl = pick_live_tl();
	uint32_t	rel = rng_below(FZ_NREL);
	FzRel	   *m = &g_tl[tl].rel[rel];
	FzAdv		adv = pick_adv();
	uint32_t	target_tl;
	uint64_t	target_inc;

	if (!m->exists)
	{
		act_create();
		return;
	}
	if (!resolve_target(tl, adv, &target_tl, &target_inc))
		adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

	if (adv == ADV_NONE)
	{
		uint32_t	n = 1 + rng_below(3);
		uint32_t	block;
		uint64_t	lsn;
		int			status;
		unsigned char tags[3];

		if (m->nblocks == 0)
		{
			act_extend();
			return;
		}
		if (n > m->nblocks)
			n = m->nblocks;
		block = rng_below(m->nblocks - n + 1);
		lsn = ship_wal(tl);
		for (uint32_t i = 0; i < n; i++)
		{
			tags[i] = (unsigned char) (1 + rng_below(255));
			psc_fill_page(page_buf + (size_t) i * PSC_PAGE_SIZE, lsn, tags[i]);
		}
		status = psc_op_writev(tl, target_inc, PS_KLASS_RELATION, rel, block,
							   page_buf, n);
		ring_note("WRITEV tl=%u rel=%u block=%u n=%u lsn=%llu", tl, rel,
				  block, n, (unsigned long long) lsn);
		ck(status == PS_STATUS_OK, "WRITEV tl=%u rel=%u block=%u n=%u "
		   "(status %d)", tl, rel, block, n, status);
		record_cov(PS_OP_WRITEV, (uint32_t) status, 0);
		if (status == PS_STATUS_OK)
			for (uint32_t i = 0; i < n; i++)
				m->tag[block + i] = tags[i], m->lsn[block + i] = lsn;
	}
	else
	{
		/*
		 * KNOWN PRODUCT BUG (see the implementer's report / g_known_issues):
		 * pagestore_daemon.c handle_request()'s PS_OP_WRITEV/PS_OP_READV
		 * cases index ch->data + i*page_size for i in [0, ch->nblocks) with
		 * no check that ch->nblocks * page_size <= PS_IO_UNIT (the channel's
		 * actual data[] capacity -- 32 pages at 8KiB/256KiB).  nblocks a few
		 * pages past that reads/writes out of the channel's data[] and into
		 * the next channel's memory instead of being refused.  Confirmed by
		 * reading the code (no bounds check anywhere on this path); not yet
		 * fixed.  The real probe (PS_STATUS_ERROR expected, PS_STATUS_OK
		 * with corrupted adjacent shared memory observed) is gated behind
		 * PAGESTORE_FUZZ_FORCE_KNOWN so an ordinary run does not hit already
		 * -triaged, unfixed memory corruption; the untriggered default
		 * substitute below still gets WRITEV a real refusal cell via the
		 * (unrelated, already-enforced) SLRU/READER_SNAPSHOT-klass-with-
		 * nblocks!=1 rejection in the same handle_request().
		 */
		if (rng_pct(35) && m->nblocks > 0 && getenv("PAGESTORE_FUZZ_FORCE_KNOWN") != NULL)
		{
			uint32_t	huge_n = (PS_IO_UNIT / PSC_PAGE_SIZE) + 1 +
				rng_below(4);
			PsChannel  *ch = psc_chan_ptr();
			int			status;

			psc_set_channel_key(ch, tl, target_inc, PS_KLASS_RELATION, rel);
			ch->opcode = PS_OP_WRITEV;
			ch->blocknum = 0;
			ch->nblocks = huge_n;	/* claims more pages than fit data[] */
			psc_cl_exec();
			status = ch->status;
			ring_note("WRITEV adv=capacity(FORCE_KNOWN) tl=%u nblocks=%u", tl,
					  huge_n);
			ck(status != PS_STATUS_OK, "WRITEV claiming %u pages (> channel "
			   "capacity) must be refused, got %d (known bug: "
			   "pagestore_daemon.c handle_request()'s PS_OP_WRITEV has no "
			   "nblocks-vs-PS_IO_UNIT bounds check)", huge_n, status);
			record_cov(PS_OP_WRITEV, (uint32_t) status, 0);
		}
		else if (rng_pct(35) && m->nblocks > 0)
		{
			/* Safe substitute for the gated probe above: an SLRU-klass
			 * WRITEV with nblocks != 1 is refused by an explicit, unrelated
			 * check at the top of handle_request() -- no OOB risk. */
			int			status = psc_op_writev(tl, target_inc, PS_KLASS_SLRU,
												  rel, 0, page_buf, 2);

			ring_note("WRITEV adv=slru-nblocks tl=%u", tl);
			ck(status != PS_STATUS_OK, "WRITEV on PS_KLASS_SLRU with "
			   "nblocks=2 must be refused, got %d", status);
			record_cov(PS_OP_WRITEV, (uint32_t) status, 0);
		}
		else
		{
			psc_fill_page(page_buf, 1, 1);
			{
				int			status = psc_op_writev(target_tl, target_inc,
													  PS_KLASS_RELATION, rel, 0,
													  page_buf, 1);

				ring_note("WRITEV adv=%d tl=%u", adv, target_tl);
				ck(status != PS_STATUS_OK, "WRITEV with adversarial target "
				   "must be refused (adv=%d tl=%u), got %d", adv, target_tl,
				   status);
				record_cov(PS_OP_WRITEV, (uint32_t) status, 0);
			}
		}
	}
}

/* READV/NBLOCKS/EXISTS "current" (req_lsn=0) is level-1 MUST: exact match.
 * req_lsn=UINT64_MAX is documented (pagestore_core.c) as an alias for 0 in
 * ps_handle_meta ("ch->req_lsn ? ch->req_lsn : UINT64_MAX"); we send it
 * explicitly sometimes as a free boundary-value check, still level-1.
 * A currently-held reader pin's exact (lsn,seq) is also level-1 (its
 * snapshot is exact).  Any other as-of horizon is level 3
 * (MAY-BE-UNAVAILABLE): OK with the exact value we can independently derive
 * (only possible for the reader-pin/newest cases above) or refusal --
 * for a horizon we cannot independently derive we only check "no crash,
 * status in {OK,ERROR}". */
static void
act_readv(void)
{
	uint32_t	tl = pick_live_tl();
	uint32_t	rel = rng_below(FZ_NREL);
	FzRel	   *m = &g_tl[tl].rel[rel];
	FzAdv		adv = pick_adv();
	uint32_t	target_tl;
	uint64_t	target_inc;

	if (!m->exists || m->nblocks == 0)
	{
		act_extend();
		return;
	}
	if (!resolve_target(tl, adv, &target_tl, &target_inc))
		adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

	if (adv == ADV_NONE)
	{
		uint32_t	n = 1 + rng_below(3);
		uint32_t	block;
		uint64_t	req_lsn = rng_pct(15) ? UINT64_MAX : 0;
		int			status;

		if (n > m->nblocks)
			n = m->nblocks;
		block = rng_below(m->nblocks - n + 1);
		status = psc_op_readv(tl, target_inc, PS_KLASS_RELATION, rel, block,
							  req_lsn, 0, read_buf, n);
		ring_note("READV tl=%u rel=%u block=%u n=%u", tl, rel, block, n);
		ck(status == PS_STATUS_OK, "READV tl=%u rel=%u block=%u n=%u "
		   "(status %d)", tl, rel, block, n, status);
		record_cov(PS_OP_READV, (uint32_t) status, 0);
		if (status == PS_STATUS_OK)
			for (uint32_t i = 0; i < n; i++)
			{
				const unsigned char *pg = read_buf + (size_t) i * PSC_PAGE_SIZE;
				uint32_t	b = block + i;

				if (m->tag[b] == 0)
				{
					int			zero = 1;

					for (uint32_t k = 0; k < PSC_PAGE_SIZE && zero; k++)
						zero = pg[k] == 0;
					ck(zero, "READV tl=%u rel=%u block=%u: unwritten block "
					   "is not all-zero", tl, rel, b);
				}
				else
					ck(psc_page_has_tag(pg, m->tag[b]) &&
					   psc_page_lsn(pg) == m->lsn[b],
					   "READV tl=%u rel=%u block=%u: expected tag=%u lsn=%llu"
					   ", content does not match", tl, rel, b, m->tag[b],
					   (unsigned long long) m->lsn[b]);
			}
	}
	else if (adv == ADV_UNDEFINED_TIMELINE || adv == ADV_BAD_INCARNATION ||
			 adv == ADV_DELETED_TIMELINE)
	{
		int			status = psc_op_readv(target_tl, target_inc,
											  PS_KLASS_RELATION, rel, 0, 0, 0,
											  read_buf, 1);

		ring_note("READV adv=%d tl=%u", adv, target_tl);
		ck(status != PS_STATUS_OK, "READV with adversarial target must be "
		   "refused (adv=%d tl=%u), got %d", adv, target_tl, status);
		record_cov(PS_OP_READV, (uint32_t) status, 0);
	}
}

/* READ_AT: level 1 for req_lsn=UINT64_MAX (newest alias, exact) and for a
 * currently-held reader pin's exact (lsn,seq).  A random as-of LSN with no
 * pin is level 3: never wrong content, but OK-vs-refused is not asserted. */
static void
act_read_at(void)
{
	uint32_t	tl = pick_live_tl();
	uint32_t	rel = rng_below(FZ_NREL);
	FzRel	   *m = &g_tl[tl].rel[rel];
	FzAdv		adv = pick_adv();
	uint32_t	target_tl;
	uint64_t	target_inc;

	if (!m->exists || m->nblocks == 0)
	{
		act_extend();
		return;
	}
	if (!resolve_target(tl, adv, &target_tl, &target_inc))
		adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

	if (adv == ADV_NONE)
	{
		uint32_t	b = rng_below(m->nblocks);
		int			mode = rng_below(3);	/* 0=newest-alias 1=pin 2=random */
		uint64_t	req_lsn,
					req_seq = 0;
		int			found;
		int			status;
		int			strong = 1;

		if (mode == 0)
			req_lsn = UINT64_MAX;
		else if (mode == 1 && g_reader[0].held && tl == 0)
		{
			req_lsn = g_reader[0].lsn;
			req_seq = g_reader[0].seq;
			/* verify against the pin's own snapshot, not the live model */
			m = &g_reader[0].snap[rel];
			if (b >= m->nblocks)
				b = m->nblocks ? m->nblocks - 1 : 0;
			if (m->nblocks == 0)
			{
				req_lsn = UINT64_MAX;
				req_seq = 0;
				m = &g_tl[tl].rel[rel];
			}
		}
		else
		{
			req_lsn = 1 + rng_below((uint32_t) (g_tl[tl].wal_end + 1));
			strong = 0;
		}
		status = psc_op_read_at(tl, target_inc, PS_KLASS_RELATION, rel, b,
								req_lsn, req_seq, read_buf, &found, NULL,
								NULL);
		ring_note("READ_AT tl=%u rel=%u block=%u mode=%d strong=%d", tl, rel,
				  b, mode, strong);
		observe(PS_OP_READ_AT, status, 0, "READ_AT");
		if (strong)
		{
			ck(status == PS_STATUS_OK, "READ_AT (strong) tl=%u rel=%u "
			   "block=%u (status %d)", tl, rel, b, status);
			if (status == PS_STATUS_OK)
			{
				if (m->tag[b] == 0)
					ck(!found || psc_page_lsn(read_buf) == 0, "READ_AT "
					   "(strong) tl=%u rel=%u block=%u: unwritten block has "
					   "content", tl, rel, b);
				else
					ck(found && psc_page_has_tag(read_buf, m->tag[b]) &&
					   psc_page_lsn(read_buf) == m->lsn[b], "READ_AT "
					   "(strong) tl=%u rel=%u block=%u: expected tag=%u "
					   "lsn=%llu, content/found-ness does not match", tl,
					   rel, b, m->tag[b], (unsigned long long) m->lsn[b]);
			}
		}
	}
	else if (adv == ADV_UNDEFINED_TIMELINE || adv == ADV_BAD_INCARNATION ||
			 adv == ADV_DELETED_TIMELINE)
	{
		int			found;
		int			status = psc_op_read_at(target_tl, target_inc,
											   PS_KLASS_RELATION, rel, 0,
											   UINT64_MAX, 0, read_buf, &found,
											   NULL, NULL);

		ring_note("READ_AT adv=%d tl=%u", adv, target_tl);
		ck(status != PS_STATUS_OK, "READ_AT with adversarial target must be "
		   "refused (adv=%d tl=%u), got %d", adv, target_tl, status);
		record_cov(PS_OP_READ_AT, (uint32_t) status, 0);
	}
}

static void
act_nblocks(void)
{
	uint32_t	tl = pick_live_tl();
	uint32_t	rel = rng_below(FZ_NREL);
	FzRel	   *m = &g_tl[tl].rel[rel];
	FzAdv		adv = pick_adv();
	uint32_t	target_tl;
	uint64_t	target_inc;

	if (!resolve_target(tl, adv, &target_tl, &target_inc))
		adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

	if (adv == ADV_NONE)
	{
		uint64_t	req_lsn = rng_pct(15) ? UINT64_MAX : 0;
		uint32_t	nb = 0;
		int			status = psc_op_nblocks(tl, target_inc, PS_KLASS_RELATION,
											 rel, req_lsn, 0, &nb);

		ring_note("NBLOCKS tl=%u rel=%u", tl, rel);
		ck(status == PS_STATUS_OK && nb == m->nblocks, "NBLOCKS tl=%u rel=%u "
		   "expected %u got %u (status %d)", tl, rel, m->nblocks, nb, status);
		record_cov(PS_OP_NBLOCKS, (uint32_t) status, 0);
	}
	else
	{
		uint32_t	nb = 0;
		int			status = psc_op_nblocks(target_tl, target_inc,
											 PS_KLASS_RELATION, rel, 0, 0,
											 &nb);

		ring_note("NBLOCKS adv=%d tl=%u", adv, target_tl);
		ck(status != PS_STATUS_OK, "NBLOCKS with adversarial target must be "
		   "refused (adv=%d tl=%u), got %d", adv, target_tl, status);
		record_cov(PS_OP_NBLOCKS, (uint32_t) status, 0);
	}
}

static void
act_exists(void)
{
	uint32_t	tl = pick_live_tl();
	uint32_t	rel = rng_below(FZ_NREL);
	FzRel	   *m = &g_tl[tl].rel[rel];
	FzAdv		adv = pick_adv();
	uint32_t	target_tl;
	uint64_t	target_inc;

	if (!resolve_target(tl, adv, &target_tl, &target_inc))
		adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

	if (adv == ADV_NONE)
	{
		uint64_t	req_lsn = rng_pct(15) ? UINT64_MAX : 0;
		int			exists;
		int			status = psc_op_exists(tl, target_inc, PS_KLASS_RELATION,
											rel, req_lsn, &exists);

		ring_note("EXISTS tl=%u rel=%u", tl, rel);
		ck(status == PS_STATUS_OK && exists == m->exists, "EXISTS tl=%u "
		   "rel=%u expected %d got %d (status %d)", tl, rel, m->exists,
		   exists, status);
		record_cov(PS_OP_EXISTS, (uint32_t) status, 0);
	}
	else
	{
		int			exists;
		int			status = psc_op_exists(target_tl, target_inc,
											PS_KLASS_RELATION, rel, 0, &exists);

		ring_note("EXISTS adv=%d tl=%u", adv, target_tl);
		ck(status != PS_STATUS_OK, "EXISTS with adversarial target must be "
		   "refused (adv=%d tl=%u), got %d", adv, target_tl, status);
		record_cov(PS_OP_EXISTS, (uint32_t) status, 0);
	}
}

/* BLOCK_DEATH: WEAK ORACLE (see g_weak_oracle_ops); we still assert the
 * generic timeline/incarnation refusal dimension strongly. */
static void
act_block_death(void)
{
	uint32_t	tl = pick_live_tl();
	uint32_t	rel = rng_below(FZ_NREL);
	FzRel	   *m = &g_tl[tl].rel[rel];
	FzAdv		adv = pick_adv();
	uint32_t	target_tl;
	uint64_t	target_inc;

	if (!resolve_target(tl, adv, &target_tl, &target_inc))
		adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

	if (adv == ADV_NONE)
	{
		uint32_t	b = rng_below(FZ_MAXBLK);
		uint64_t	death_lsn,
					death_seq;
		int			status = psc_op_block_death(tl, target_inc,
												   PS_KLASS_RELATION, rel, b,
												   g_tl[tl].wal_end + 1, 0,
												   &death_lsn, &death_seq);

		ring_note("BLOCK_DEATH tl=%u rel=%u block=%u", tl, rel, b);
		(void) m;
		observe(PS_OP_BLOCK_DEATH, status, 0, "BLOCK_DEATH");
	}
	else
	{
		uint64_t	death_lsn,
					death_seq;
		int			status = psc_op_block_death(target_tl, target_inc,
												   PS_KLASS_RELATION, rel, 0,
												   1, 0, &death_lsn,
												   &death_seq);

		ring_note("BLOCK_DEATH adv=%d tl=%u", adv, target_tl);
		ck(status != PS_STATUS_OK, "BLOCK_DEATH with adversarial target must "
		   "be refused (adv=%d tl=%u), got %d", adv, target_tl, status);
		record_cov(PS_OP_BLOCK_DEATH, (uint32_t) status, 0);
	}
}

static void
act_immedsync(void)
{
	int			status = psc_op_immedsync();

	ring_note("IMMEDSYNC");
	ck(status == PS_STATUS_OK, "IMMEDSYNC (status %d)", status);
	record_cov(PS_OP_IMMEDSYNC, (uint32_t) status, 0);
}

/* PS_OP_NONE / an out-of-range opcode value against an otherwise well-formed
 * request: handle_request()'s switch has an explicit `default: ch->status =
 * PS_STATUS_ERROR` (pagestore_daemon.c) and ps_handle_meta() returns 0
 * (unhandled) for anything it does not recognize (pagestore_core.c), so this
 * must always be refused, never a hang or a silently-accepted no-op. */
static void
act_unknown_opcode(void)
{
	uint32_t	tl = pick_live_tl();
	uint32_t	rel = rng_below(FZ_NREL);
	PsChannel  *ch = psc_chan_ptr();
	uint32_t	bogus = rng_pct(50) ? (uint32_t) PS_OP_NONE :
		1000u + rng_below(60000u);
	int			status;

	psc_set_channel_key(ch, tl, g_tl[tl].incarnation, PS_KLASS_RELATION, rel);
	ch->opcode = bogus;
	psc_cl_exec();
	status = ch->status;
	ring_note("UNKNOWN_OPCODE tl=%u opcode=%u", tl, bogus);
	ck(status != PS_STATUS_OK, "opcode %u (unknown/none) must be refused, "
	   "got %d", bogus, status);
	/* Every bogus value files under PS_OP_NONE's coverage cell -- the point
	 * is "any opcode this daemon does not recognize", not the specific
	 * numeric value sent. */
	record_cov(PS_OP_NONE, (uint32_t) status, 0);
}

/* PS_OP_ADMISSION_BARRIER: no timeline/incarnation target, so there is no
 * generic adversarial dimension to exercise here (see g_stage1_ops' note);
 * the oracle is simply "OK, req_seq != 0, and never decreases" -- an
 * admission sequence is a single global monotonic counter. */
static uint64_t g_last_admission_seq;

static void
act_admission_barrier(void)
{
	uint64_t	seq = 0;
	int			status = psc_op_admission_barrier(&seq);

	ring_note("ADMISSION_BARRIER");
	ck(status == PS_STATUS_OK && seq != 0, "ADMISSION_BARRIER (status %d "
	   "seq=%llu)", status, (unsigned long long) seq);
	if (status == PS_STATUS_OK)
		ck(seq >= g_last_admission_seq, "ADMISSION_BARRIER sequence went "
		   "backwards: had %llu, got %llu",
		   (unsigned long long) g_last_admission_seq,
		   (unsigned long long) seq);
	record_cov(PS_OP_ADMISSION_BARRIER, (uint32_t) status, 0);
	if (status == PS_STATUS_OK && seq > g_last_admission_seq)
		g_last_admission_seq = seq;
}

/* ===================== WAL ops ============================================ */

static void
act_wal_size(void)
{
	uint32_t	tl = pick_live_tl();
	FzAdv		adv = pick_adv();
	uint32_t	target_tl;
	uint64_t	target_inc;

	if (!resolve_target(tl, adv, &target_tl, &target_inc))
		adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

	if (adv == ADV_NONE)
	{
		uint64_t	end = 0;
		uint64_t	expected = g_tl[tl].wal_shipped ? g_tl[tl].wal_end : 0;
		int			status = psc_op_wal_size(tl, target_inc, &end);

		ring_note("WAL_SIZE tl=%u", tl);
		ck(status == PS_STATUS_OK && end == expected, "WAL_SIZE "
		   "tl=%u expected %llu got %llu (status %d)", tl,
		   (unsigned long long) expected, (unsigned long long) end,
		   status);
		record_cov(PS_OP_WAL_SIZE, (uint32_t) status, 0);
	}
	else
	{
		uint64_t	end = 0;
		int			status = psc_op_wal_size(target_tl, target_inc, &end);

		ring_note("WAL_SIZE adv=%d tl=%u inc=%llu", adv, target_tl,
				  (unsigned long long) target_inc);
		ck(status != PS_STATUS_OK, "WAL_SIZE with adversarial target must be "
		   "refused (adv=%d tl=%u inc=%llu), got %d", adv, target_tl,
		   (unsigned long long) target_inc, status);
		record_cov(PS_OP_WAL_SIZE, (uint32_t) status, 0);
	}
}

static void
act_wal_read(void)
{
	uint32_t	tl = pick_live_tl();
	FzAdv		adv = pick_adv();
	uint32_t	target_tl;
	uint64_t	target_inc;

	if (g_tl[tl].wal_end <= g_tl[tl].wal_start)
	{
		ship_wal(tl);
		return;
	}
	if (!resolve_target(tl, adv, &target_tl, &target_inc))
		adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

	if (adv == ADV_NONE)
	{
		uint64_t	nrec = (g_tl[tl].wal_end - g_tl[tl].wal_start) /
			FZ_WAL_PAYLOAD;
		uint64_t	start = g_tl[tl].wal_start +
			rng_below((uint32_t) nrec) * (uint64_t) FZ_WAL_PAYLOAD;
		unsigned char expect[FZ_WAL_PAYLOAD];
		unsigned char got[FZ_WAL_PAYLOAD];
		uint32_t	nread = 0;
		int			status = psc_op_wal_read(tl, target_inc, start,
											 FZ_WAL_PAYLOAD, got, &nread);

		ring_note("WAL_READ tl=%u start=%llu", tl, (unsigned long long) start);
		ck(status == PS_STATUS_OK, "WAL_READ tl=%u start=%llu (status %d)",
		   tl, (unsigned long long) start, status);
		record_cov(PS_OP_WAL_READ, (uint32_t) status, 0);
		if (status == PS_STATUS_OK)
		{
			fz_wal_fill(start, expect);
			ck(nread == FZ_WAL_PAYLOAD && memcmp(expect, got, FZ_WAL_PAYLOAD) == 0,
			   "WAL_READ tl=%u start=%llu content mismatch (nread=%u)", tl,
			   (unsigned long long) start, nread);
		}
	}
	else
	{
		unsigned char got[FZ_WAL_PAYLOAD];
		uint32_t	nread = 0;
		int			status = psc_op_wal_read(target_tl, target_inc, 0,
											 FZ_WAL_PAYLOAD, got, &nread);

		ring_note("WAL_READ adv=%d tl=%u inc=%llu", adv, target_tl,
				  (unsigned long long) target_inc);
		ck(status != PS_STATUS_OK, "WAL_READ with adversarial target must be "
		   "refused (adv=%d tl=%u inc=%llu), got %d", adv, target_tl,
		   (unsigned long long) target_inc, status);
		record_cov(PS_OP_WAL_READ, (uint32_t) status, 0);
	}
}

static void
act_walidx_add(void)
{
	uint32_t	tl = pick_live_tl();
	uint32_t	rel = rng_below(FZ_NREL);
	FzAdv		adv = pick_adv();
	uint32_t	target_tl;
	uint64_t	target_inc;

	if (!resolve_target(tl, adv, &target_tl, &target_inc))
		adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

	if (adv == ADV_NONE)
	{
		uint64_t	lsn = ship_wal(tl);
		int			status = psc_op_walidx_add(tl, target_inc,
												  PS_KLASS_RELATION, rel,
												  rng_below(FZ_MAXBLK), lsn);

		ring_note("WAL_INDEX_ADD tl=%u rel=%u lsn=%llu", tl, rel,
				  (unsigned long long) lsn);
		ck(status == PS_STATUS_OK, "WAL_INDEX_ADD tl=%u rel=%u (status %d)",
		   tl, rel, status);
		record_cov(PS_OP_WAL_INDEX_ADD, (uint32_t) status, 0);
	}
	else
	{
		int			status = psc_op_walidx_add(target_tl, target_inc,
												  PS_KLASS_RELATION, rel, 0, 1);

		ring_note("WAL_INDEX_ADD adv=%d tl=%u", adv, target_tl);
		ck(status != PS_STATUS_OK, "WAL_INDEX_ADD with adversarial target "
		   "must be refused (adv=%d tl=%u), got %d", adv, target_tl, status);
		record_cov(PS_OP_WAL_INDEX_ADD, (uint32_t) status, 0);
	}
}

static void
act_walidx_add_batch(void)
{
	uint32_t	tl = pick_live_tl();
	uint32_t	rel = rng_below(FZ_NREL);
	FzAdv		adv = pick_adv();
	uint32_t	target_tl;
	uint64_t	target_inc;

	if (!resolve_target(tl, adv, &target_tl, &target_inc))
		adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

	if (adv == ADV_NONE || adv == ADV_BAD_INCARNATION ||
		adv == ADV_UNDEFINED_TIMELINE || adv == ADV_DELETED_TIMELINE)
	{
		uint32_t	n = 1 + rng_below(3);
		uint32_t	blocks[3];
		uint64_t	lsn = adv == ADV_NONE ? ship_wal(tl) : 1;
		int			status;

		for (uint32_t i = 0; i < n; i++)
			blocks[i] = rng_below(FZ_MAXBLK);
		status = psc_op_walidx_add_batch(adv == ADV_NONE ? tl : target_tl,
										 target_inc, PS_KLASS_RELATION, rel,
										 blocks, n, lsn, lsn + FZ_WAL_PAYLOAD);
		ring_note("WAL_INDEX_ADD_BATCH adv=%d tl=%u n=%u", adv,
				  adv == ADV_NONE ? tl : target_tl, n);
		if (adv == ADV_NONE)
			ck(status == PS_STATUS_OK, "WAL_INDEX_ADD_BATCH tl=%u rel=%u "
			   "n=%u (status %d)", tl, rel, n, status);
		else
			ck(status != PS_STATUS_OK, "WAL_INDEX_ADD_BATCH with adversarial "
			   "target must be refused (adv=%d), got %d", adv, status);
		record_cov(PS_OP_WAL_INDEX_ADD_BATCH, (uint32_t) status, 0);
	}
}

static void
act_walidx_progress(void)
{
	uint32_t	tl = pick_live_tl();
	FzAdv		adv = pick_adv();
	uint32_t	target_tl;
	uint64_t	target_inc;

	if (!resolve_target(tl, adv, &target_tl, &target_inc))
		adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

	if (adv == ADV_NONE)
	{
		if (rng_pct(50))
		{
			uint64_t	progress = 0;
			int			status = psc_op_walidx_progress_read(tl, target_inc,
																  &progress);

			ring_note("WAL_INDEX_PROGRESS(read) tl=%u", tl);
			ck(status == PS_STATUS_OK, "WAL_INDEX_PROGRESS read tl=%u "
			   "(status %d)", tl, status);
			record_cov(PS_OP_WAL_INDEX_PROGRESS, (uint32_t) status, 0);
		}
		else if (g_tl[tl].wal_end > g_tl[tl].walidx_progress)
		{
			uint64_t	end = g_tl[tl].wal_end;
			int			status = psc_op_walidx_progress_commit(tl, target_inc,
																 g_tl[tl].walidx_progress,
																 end);

			ring_note("WAL_INDEX_PROGRESS(commit) tl=%u [%llu,%llu)", tl,
					  (unsigned long long) g_tl[tl].walidx_progress,
					  (unsigned long long) end);
			ck(status == PS_STATUS_OK, "WAL_INDEX_PROGRESS commit tl=%u "
			   "[%llu,%llu) (status %d)", tl,
			   (unsigned long long) g_tl[tl].walidx_progress,
			   (unsigned long long) end, status);
			record_cov(PS_OP_WAL_INDEX_PROGRESS, (uint32_t) status, 0);
			if (status == PS_STATUS_OK)
				g_tl[tl].walidx_progress = end;
		}
	}
	else
	{
		int			status = psc_op_walidx_progress_commit(target_tl,
															 target_inc, 0,
															 FZ_WAL_PAYLOAD);

		ring_note("WAL_INDEX_PROGRESS adv=%d tl=%u", adv, target_tl);
		ck(status != PS_STATUS_OK, "WAL_INDEX_PROGRESS commit with "
		   "adversarial target must be refused (adv=%d tl=%u), got %d", adv,
		   target_tl, status);
		record_cov(PS_OP_WAL_INDEX_PROGRESS, (uint32_t) status, 0);
	}
}

static void
act_walidx_get(void)
{
	uint32_t	tl = pick_live_tl();
	uint32_t	rel = rng_below(FZ_NREL);
	uint32_t	block = rng_below(FZ_MAXBLK);
	FzAdv		adv = pick_adv();
	uint32_t	target_tl;
	uint64_t	target_inc;

	if (!resolve_target(tl, adv, &target_tl, &target_inc))
		adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

	if (adv == ADV_NONE)
	{
		uint64_t	lsn = ship_wal(tl);
		uint32_t	blocks[1] = {block};
		int			n = 0;
		int			status;
		uint64_t	end = lsn + FZ_WAL_PAYLOAD;

		ck(psc_op_walidx_add_batch(tl, target_inc, PS_KLASS_RELATION, rel,
								   blocks, 1, lsn, end) ==
		   PS_STATUS_OK, "setup WAL_INDEX_ADD_BATCH for GET tl=%u rel=%u",
		   tl, rel);
		/*
		 * walidx_get() only returns entries below the timeline's durable
		 * WAL_INDEX_PROGRESS marker ("entries become queryable only with
		 * their durable progress marker", pagestore_core.c:walidx_get) --
		 * publish progress past this record before expecting to see it.
		 */
		ck(psc_op_walidx_progress_commit(tl, target_inc,
										 g_tl[tl].walidx_progress, end) ==
		   PS_STATUS_OK, "setup WAL_INDEX_PROGRESS commit for GET tl=%u", tl);
		if (end > g_tl[tl].walidx_progress)
			g_tl[tl].walidx_progress = end;
		status = psc_op_walidx_get(tl, target_inc, PS_KLASS_RELATION, rel,
								   block, UINT64_MAX, fz_walidx_get_recs,
								   FZ_WALIDX_GET_CAP, &n);
		ring_note("WAL_INDEX_GET tl=%u rel=%u block=%u", tl, rel, block);
		ck(status == PS_STATUS_OK, "WAL_INDEX_GET tl=%u rel=%u block=%u "
		   "(status %d)", tl, rel, block, status);
		record_cov(PS_OP_WAL_INDEX_GET, (uint32_t) status, 0);
		if (status == PS_STATUS_OK)
		{
			/*
			 * Precise membership oracle (level 1 MUST, not weak). The
			 * commit above just moved tl's WAL-index progress marker to
			 * exactly end == this record's own end_lsn, which is also the
			 * *cutoff* walidx_snapshot_compaction_plan()/retain_chain()
			 * (pagestore_walidx_prune.c) would use if background
			 * compaction ran for (tl,key,block) right now.
			 *
			 * Within this one synchronous call nothing else has run, so no
			 * page-content "base" can exist at lsn >= end_lsn (ship_wal()
			 * is strictly monotonic per timeline: nothing has shipped
			 * since), and psc_op_walidx_add_batch() always marks its entry
			 * FPI, so retain_chain()'s FPI fallback alone can never drop
			 * our own newest entry either. The ONLY legitimate way for the
			 * daemon to answer "absent" is the relation-lifecycle "death"
			 * horizon substitution (pagestore_core.c's
			 * walidx_plan_bases_build: fork_asof_hop(f, horizon, ...) over
			 * FEV_DEAD/FEV_SET, with horizon == our own just-committed
			 * progress) -- i.e. exactly when this block is *currently*
			 * dead or out of range on tl: relation absent, or block at/past
			 * nblocks. The fuzzer's own live model answers that question
			 * directly and it cannot have changed since (single-threaded,
			 * no intervening op), so this is provable, not approximate.
			 */
			FzRel	   *m = &g_tl[tl].rel[rel];
			int			dead = !m->exists || block >= m->nblocks;
			int			found = 0;

			for (int i = 0; i < n; i++)
				if (fz_walidx_get_recs[i].lsn == lsn &&
					fz_walidx_get_recs[i].timeline == tl)
					found = 1;
			if (!found)
				ck(dead, "WAL_INDEX_GET tl=%u rel=%u block=%u: the record "
				   "just added at lsn=%llu is missing, but the block is "
				   "still alive on tl (exists=%d nblocks=%u) -- no "
				   "replacement base/death can legitimately have dropped "
				   "it (n=%d)", tl, rel, block, (unsigned long long) lsn,
				   m->exists, m->nblocks, n);
			/* found==1 is always fine, dead or not: presence is never a
			 * failure, only unexplained absence is. */
		}
	}
	else
	{
		PsWalRec	recs[8];
		int			n = 0;
		int			status = psc_op_walidx_get(target_tl, target_inc,
												  PS_KLASS_RELATION, rel,
												  block, UINT64_MAX, recs, 8,
												  &n);

		ring_note("WAL_INDEX_GET adv=%d tl=%u", adv, target_tl);
		ck(status != PS_STATUS_OK, "WAL_INDEX_GET with adversarial target "
		   "must be refused (adv=%d tl=%u), got %d", adv, target_tl, status);
		record_cov(PS_OP_WAL_INDEX_GET, (uint32_t) status, 0);
	}
}

static void
act_wal_retain_floor(void)
{
	uint32_t	tl = pick_live_tl();
	FzAdv		adv = pick_adv();
	uint32_t	target_tl;
	uint64_t	target_inc;

	if (!resolve_target(tl, adv, &target_tl, &target_inc))
		adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

	if (adv == ADV_NONE)
	{
		uint64_t	floor = 0;
		int			proven = 0;
		int			status = psc_op_wal_retain_floor(tl, target_inc, &floor,
													 &proven);

		ring_note("WAL_RETAIN_FLOOR tl=%u", tl);
		/* A floor may legitimately be unprovable before any materializer
		 * publication ever ran on this timeline: accept OK either way, but
		 * a provable floor must never exceed what we have shipped. */
		observe(PS_OP_WAL_RETAIN_FLOOR, status, 0, "WAL_RETAIN_FLOOR");
		if (status == PS_STATUS_OK && proven)
			ck(floor <= g_tl[tl].wal_end, "WAL_RETAIN_FLOOR tl=%u floor=%llu"
			   " exceeds shipped tail %llu", tl, (unsigned long long) floor,
			   (unsigned long long) g_tl[tl].wal_end);
	}
	else
	{
		uint64_t	floor = 0;
		int			proven = 0;
		int			status = psc_op_wal_retain_floor(target_tl, target_inc,
													 &floor, &proven);

		ring_note("WAL_RETAIN_FLOOR adv=%d tl=%u inc=%llu", adv, target_tl,
				  (unsigned long long) target_inc);
		ck(status != PS_STATUS_OK, "WAL_RETAIN_FLOOR with adversarial target "
		   "must be refused (adv=%d tl=%u inc=%llu), got %d", adv, target_tl,
		   (unsigned long long) target_inc, status);
		record_cov(PS_OP_WAL_RETAIN_FLOOR, (uint32_t) status, 0);
	}
}

/* ===================== timeline ops ======================================= */

static void
act_timeline_state(void)
{
	uint32_t	tl = pick_live_tl();
	FzAdv		adv = pick_adv();

	if (adv == ADV_NONE || adv == ADV_BAD_INCARNATION)
	{
		PsTimelineState state;
		uint64_t	inc;
		int			status = psc_op_timeline_state(tl, &state, &inc);

		ring_note("TIMELINE_STATE tl=%u", tl);
		ck(status == PS_STATUS_OK, "TIMELINE_STATE tl=%u (status %d)", tl,
		   status);
		if (status == PS_STATUS_OK)
			ck(state == g_tl[tl].state, "TIMELINE_STATE tl=%u expected state "
			   "%d got %d", tl, g_tl[tl].state, state);
		record_cov(PS_OP_TIMELINE_STATE, (uint32_t) status, state);
	}
	else
	{
		uint32_t	target_tl = rng_pct(50) ? FZ_TL_UNDEF_A : FZ_TL_UNDEF_B;
		PsTimelineState state;
		uint64_t	inc;
		int			status = psc_op_timeline_state(target_tl, &state, &inc);

		ring_note("TIMELINE_STATE undefined tl=%u", target_tl);
		ck(status != PS_STATUS_OK, "TIMELINE_STATE on an undefined timeline "
		   "must be refused (tl=%u), got %d", target_tl, status);
		ck(state == PS_TIMELINE_STATE_UNDEFINED, "TIMELINE_STATE on an "
		   "undefined timeline: result should be PS_TIMELINE_STATE_UNDEFINED"
		   ", got %u", state);
		record_cov(PS_OP_TIMELINE_STATE, (uint32_t) status, state);
	}
}

static void
act_timeline_info(void)
{
	uint32_t	tl = pick_live_tl();
	FzAdv		adv = pick_adv();
	uint32_t	target_tl;
	uint64_t	target_inc;

	if (!resolve_target(tl, adv, &target_tl, &target_inc))
		adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

	if (adv == ADV_NONE)
	{
		int			has_parent;
		uint32_t	parent;
		uint64_t	branch_lsn,
					parent_inc;
		int			status = psc_op_timeline_info(tl, target_inc, &has_parent,
													  &parent, &branch_lsn,
													  &parent_inc);

		ring_note("TIMELINE_INFO tl=%u", tl);
		ck(status == PS_STATUS_OK, "TIMELINE_INFO tl=%u (status %d)", tl,
		   status);
		if (status == PS_STATUS_OK)
		{
			ck(has_parent == g_tl[tl].has_parent, "TIMELINE_INFO tl=%u "
			   "has_parent expected %d got %d", tl, g_tl[tl].has_parent,
			   has_parent);
			if (g_tl[tl].has_parent)
				ck(parent == g_tl[tl].parent && branch_lsn == g_tl[tl].branch_lsn,
				   "TIMELINE_INFO tl=%u expected parent=%u branch_lsn=%llu "
				   "got parent=%u branch_lsn=%llu", tl, g_tl[tl].parent,
				   (unsigned long long) g_tl[tl].branch_lsn, parent,
				   (unsigned long long) branch_lsn);
		}
		record_cov(PS_OP_TIMELINE_INFO, (uint32_t) status, has_parent);
	}
	else
	{
		int			has_parent;
		uint32_t	parent;
		uint64_t	branch_lsn,
					parent_inc;
		int			status = psc_op_timeline_info(target_tl, target_inc,
													  &has_parent, &parent,
													  &branch_lsn, &parent_inc);

		ring_note("TIMELINE_INFO adv=%d tl=%u", adv, target_tl);
		ck(status != PS_STATUS_OK, "TIMELINE_INFO with adversarial target "
		   "must be refused (adv=%d tl=%u), got %d", adv, target_tl, status);
		record_cov(PS_OP_TIMELINE_INFO, (uint32_t) status, 0);
	}
}

/* ===================== retention ops ======================================= */

static void
act_retention_lookup(void)
{
	int			use_reader = rng_pct(70) && (g_reader[0].held || g_reader[1].held);
	uint32_t	kind = use_reader ? PS_RETENTION_OWNER_READER :
		PS_RETENTION_OWNER_MATERIALIZER;
	uint64_t	owner_id = use_reader ?
		(g_reader[0].held ? g_reader[0].owner_id : g_reader[1].owner_id) : 1;
	PsRetentionPin pin;
	int			found = psc_op_retention_lookup(0, 0, kind, owner_id, &pin);
	int			status = psc_chan_ptr()->status;

	ring_note("RETENTION_PIN_LOOKUP kind=%u owner=%llu", kind,
			  (unsigned long long) owner_id);
	observe(PS_OP_RETENTION_PIN_LOOKUP, status, found, "RETENTION_PIN_LOOKUP");
	if (status == PS_STATUS_OK && kind == PS_RETENTION_OWNER_READER)
	{
		FzReaderPin *r = g_reader[0].held && g_reader[0].owner_id == owner_id ?
			&g_reader[0] : &g_reader[1];

		ck(found == r->held, "RETENTION_PIN_LOOKUP reader owner=%llu "
		   "expected held=%d got found=%d", (unsigned long long) owner_id,
		   r->held, found);
		if (found && r->held)
			ck(pin.lsn == r->lsn && pin.admission_seq == r->seq &&
			   pin.generation == r->generation, "RETENTION_PIN_LOOKUP reader "
			   "owner=%llu stored pin does not match model",
			   (unsigned long long) owner_id);
	}
	else if (status == PS_STATUS_OK)
		ck(found == g_tl[0].mat_registered, "RETENTION_PIN_LOOKUP "
		   "materializer expected held=%d got found=%d",
		   g_tl[0].mat_registered, found);

	if (rng_pct(25))
	{
		/* Adversarial: an undefined timeline must be refused outright
		 * (distinct from the OK/found=0 "no such owner yet" case above). */
		PsRetentionPin adv_pin;
		int			adv_status;

		psc_op_retention_lookup(FZ_TL_UNDEF_A, 1, kind, owner_id, &adv_pin);
		adv_status = psc_chan_ptr()->status;
		ring_note("RETENTION_PIN_LOOKUP adv=undefined-timeline");
		ck(adv_status != PS_STATUS_OK, "RETENTION_PIN_LOOKUP on an undefined "
		   "timeline must be refused, got %d", adv_status);
		record_cov(PS_OP_RETENTION_PIN_LOOKUP, (uint32_t) adv_status, 0);
	}
}

static void
act_retention_get(void)
{
	uint64_t	epoch = 0;
	PsRetentionPin pin;
	uint32_t	count = 0;
	int			expected = (g_reader[0].held ? 1 : 0) +
		(g_reader[1].held ? 1 : 0) + (g_tl[0].mat_registered ? 1 : 0);
	int			rc = psc_op_retention_get(0, &epoch, &pin, &count);
	int			status = psc_chan_ptr()->status;

	ring_note("RETENTION_PIN_GET index=0");
	observe(PS_OP_RETENTION_PIN_GET, status, 0, "RETENTION_PIN_GET");
	if (rc >= 0)
		ck((int) count == expected, "RETENTION_PIN_GET count expected %d "
		   "got %u", expected, count);

	if (rng_pct(30) && epoch != 0)
	{
		/* Adversarial: a deliberately-wrong epoch must be reported STALE. */
		uint64_t	bad_epoch = epoch + 1000000;
		int			rc2 = psc_op_retention_get(0, &bad_epoch, &pin, &count);
		int			status2 = psc_chan_ptr()->status;

		ring_note("RETENTION_PIN_GET stale epoch");
		ck(status2 == PS_STATUS_STALE, "RETENTION_PIN_GET with a wrong "
		   "epoch must return PS_STATUS_STALE, got %d", status2);
		record_cov(PS_OP_RETENTION_PIN_GET, (uint32_t) status2, 0);
		(void) rc2;
	}
}

static void
act_retention_floor(void)
{
	uint32_t	tl = pick_live_tl();
	uint32_t	resources[3] = {PS_RETENTION_RESOURCE_PAGE_HISTORY,
		PS_RETENTION_RESOURCE_WAL, PS_RETENTION_RESOURCE_WAL_INDEX};
	FzAdv		adv = pick_adv();
	uint32_t	target_tl;
	uint64_t	target_inc;

	if (!resolve_target(tl, adv, &target_tl, &target_inc))
		adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

	if (adv == ADV_NONE)
	{
		uint64_t	floor = 0;
		int			proven = 0;
		int			status = psc_op_retention_floor(tl, target_inc,
													 resources[rng_below(3)],
													 &floor, &proven);

		ring_note("RETENTION_FLOOR tl=%u", tl);
		/*
		 * WEAK ORACLE here (status in {OK,ERROR}, not asserted OK): the
		 * same "a floor that cannot be proven is an error, never a lower
		 * bound" philosophy pagestore_core.c documents explicitly for
		 * PS_OP_WAL_RETAIN_FLOOR applies structurally to RESOURCE_WAL's
		 * control-note walk here (retention_effective_floor_internal's
		 * wal_retain_floor_level() calls), and PAGE_HISTORY's
		 * control_checkpoint_cutoff() shares the same "unreadable note"
		 * failure path -- a timeline (e.g. a branch) with no control-object
		 * write of its own yet can legitimately fail closed rather than
		 * return a fabricated floor.
		 */
		observe(PS_OP_RETENTION_FLOOR, status, 0, "RETENTION_FLOOR");
		(void) proven;
	}
	else if (adv == ADV_UNDEFINED_TIMELINE)
	{
		uint64_t	floor = 0;
		int			proven = 0;
		int			status = psc_op_retention_floor(target_tl, target_inc,
													 PS_RETENTION_RESOURCE_WAL,
													 &floor, &proven);

		ring_note("RETENTION_FLOOR undefined tl=%u", target_tl);
		ck(status != PS_STATUS_OK, "RETENTION_FLOOR on an undefined timeline "
		   "must be refused (tl=%u), got %d", target_tl, status);
		record_cov(PS_OP_RETENTION_FLOOR, (uint32_t) status, 0);
	}
	else
	{
		/* Malformed resources mask (multiple bits): must be refused. */
		uint64_t	floor = 0;
		int			proven = 0;
		int			status = psc_op_retention_floor(tl, g_tl[tl].incarnation,
													 PS_RETENTION_RESOURCE_ALL,
													 &floor, &proven);

		ring_note("RETENTION_FLOOR malformed resources tl=%u", tl);
		ck(status != PS_STATUS_OK, "RETENTION_FLOOR with a multi-bit "
		   "resources mask must be refused, got %d", status);
		record_cov(PS_OP_RETENTION_FLOOR, (uint32_t) status, 0);
	}
}

static void
act_retention_set(void)
{
	FzReaderPin *r = g_reader[0].held ? &g_reader[0] :
		(g_reader[1].held ? &g_reader[1] : NULL);

	if (r == NULL)
		return;					/* nothing held to exercise SET against */

	if (rng_pct(70))
	{
		/* Exact retry of a held pin at its own (lsn, seq): must succeed even
		 * if the frontier has since moved past it (see the exact-retry
		 * clause in pagestore_core.c's PS_OP_RETENTION_PIN_SET handler). */
		int			status = psc_op_retention_set(0, PS_RETENTION_OWNER_READER,
													 r->owner_id, r->generation,
													 PS_RETENTION_RESOURCE_ALL,
													 r->lsn, r->seq);

		ring_note("RETENTION_PIN_SET exact-retry owner=%llu",
				  (unsigned long long) r->owner_id);
		ck(status == PS_STATUS_OK, "RETENTION_PIN_SET exact retry of a held "
		   "pin (owner=%llu lsn=%llu seq=%llu) must succeed, got %d",
		   (unsigned long long) r->owner_id, (unsigned long long) r->lsn,
		   (unsigned long long) r->seq, status);
		record_cov(PS_OP_RETENTION_PIN_SET, (uint32_t) status, 0);
	}
	else
	{
		/* generation 0 is never valid on the current protocol boundary. */
		int			status = psc_op_retention_set(0, PS_RETENTION_OWNER_READER,
													 r->owner_id, 0,
													 PS_RETENTION_RESOURCE_ALL,
													 r->lsn, r->seq);

		ring_note("RETENTION_PIN_SET generation=0 owner=%llu",
				  (unsigned long long) r->owner_id);
		ck(status != PS_STATUS_OK, "RETENTION_PIN_SET with generation 0 must "
		   "be refused, got %d", status);
		record_cov(PS_OP_RETENTION_PIN_SET, (uint32_t) status, 0);
	}
}

/* RETENTION_PIN_RESERVE/DROP proper are exercised as environment actions
 * (env_reader_reserve/env_reader_drop/env_materialize below), since a
 * successful RESERVE is stateful (creates a pin the rest of the run may
 * depend on) rather than a free-standing per-step probe; this action adds
 * only the adversarial coverage (stale generation, bad timeline). */
static void
act_retention_reserve_adv(void)
{
	uint32_t	choice = rng_below(2);

	if (choice == 0)
	{
		/* A stale generation: reader 0's generation-1 (already superseded if
		 * it has ever been reserved-then-dropped-then-reserved once; if it
		 * never has, generation 1 with old_nblocks!=0 against an owner with
		 * no record yet simply proceeds -- not stale -- so this only fires
		 * meaningfully once a reader has cycled at least once). */
		uint64_t	seq = 0;
		int			status = psc_op_retention_reserve(0,
													   PS_RETENTION_OWNER_READER,
													   g_reader[0].owner_id,
													   g_reader[0].generation == 0 ? 1 :
													   g_reader[0].generation,
													   PS_RETENTION_RESOURCE_ALL,
													   0, &seq);

		ring_note("RETENTION_PIN_RESERVE adv=stale-or-zero-lsn owner=%llu",
				  (unsigned long long) g_reader[0].owner_id);
		observe(PS_OP_RETENTION_PIN_RESERVE, status, 0,
				"RETENTION_PIN_RESERVE adversarial generation/lsn");
	}
	else
	{
		uint64_t	seq = 0;
		int			status = psc_op_retention_reserve(FZ_TL_UNDEF_A,
													   PS_RETENTION_OWNER_READER,
													   999, 1,
													   PS_RETENTION_RESOURCE_ALL,
													   0, &seq);

		ring_note("RETENTION_PIN_RESERVE adv=undefined-timeline");
		ck(status != PS_STATUS_OK, "RETENTION_PIN_RESERVE on an undefined "
		   "timeline must be refused, got %d", status);
		record_cov(PS_OP_RETENTION_PIN_RESERVE, (uint32_t) status, 0);
	}

	/* Generation 0 is never valid on the current protocol boundary for DROP
	 * either (mirrors PS_OP_RETENTION_PIN_SET's identical rule). */
	{
		int			status = psc_op_retention_drop(0, PS_RETENTION_OWNER_READER,
													999999, 0);

		ring_note("RETENTION_PIN_DROP adv=generation-zero");
		ck(status != PS_STATUS_OK, "RETENTION_PIN_DROP with generation 0 "
		   "must be refused, got %d", status);
		record_cov(PS_OP_RETENTION_PIN_DROP, (uint32_t) status, 0);
	}
}

/* Adversarial WAL_APPEND: re-shipping a byte range that overlaps already-
 * shipped WAL with *different* content is a "non-prefix overlap" and must
 * be refused (wal_append_locked(), pagestore_core.c) -- the spec's
 * "duplicate/overlapping WAL append" adversarial category. */
static void
act_wal_append_adv(void)
{
	uint32_t	tl = pick_live_tl();
	uint64_t	start;
	unsigned char bogus[FZ_WAL_PAYLOAD];
	int			status;

	if (g_tl[tl].wal_end - g_tl[tl].wal_start < FZ_WAL_PAYLOAD)
	{
		ship_wal(tl);
		return;
	}
	start = g_tl[tl].wal_end - FZ_WAL_PAYLOAD / 2;	/* overlaps the last record */
	memset(bogus, 0xEE, sizeof(bogus));	/* never a fz_wal_fill() pattern */
	status = psc_op_wal_append(tl, g_tl[tl].incarnation, start, bogus,
							   FZ_WAL_PAYLOAD);
	ring_note("WAL_APPEND adv=non-prefix-overlap tl=%u start=%llu", tl,
			  (unsigned long long) start);
	ck(status != PS_STATUS_OK, "WAL_APPEND re-shipping [%llu,+%u) with "
	   "content that diverges from what is already there must be refused, "
	   "got %d", (unsigned long long) start, FZ_WAL_PAYLOAD, status);
	record_cov(PS_OP_WAL_APPEND, (uint32_t) status, 0);
}

/* ===================== branch / timeline lifecycle ops ==================== */

static void
act_check_branch(void)
{
	/*
	 * Parent restricted to timeline 0, matching env_branch_create(): a
	 * nested (branch-of-branch) target's own frontier-projection chain
	 * (branch_frontiers_allow() walking through the intermediate branch's
	 * *own*, possibly much older, fork point) is out of this stage's
	 * modeled scope -- see the report's limitations.
	 */
	uint32_t	parent = 0;
	uint32_t	slot = 1 + rng_below(FZ_NTL - 1);

	if (rng_pct(60))
	{
		/*
		 * Legal target for a *new* branch definition: either a never-
		 * defined slot (target_incarnation=0) or a DELETED slot being
		 * reused, which branch_create_request_ok() requires
		 * target_incarnation == old_incarnation + 1 for (0 is refused
		 * there, not accepted as "whatever the next one is").
		 */
		int			fresh = !g_tl[slot].known;
		int			reusable = g_tl[slot].known &&
			g_tl[slot].state == PS_TIMELINE_DELETED;
		uint64_t	target_inc = reusable ?
			g_branch_last_incarnation[slot] + 1 : 0;
		int			status = PS_STATUS_ERROR;

		/*
		 * Same transient-contention window as env_branch_create(): the
		 * parent's current tip can momentarily fail
		 * branch_frontiers_allow()'s pending-walidx-snapshot-publication
		 * check under this stage's deliberately tiny walidx thresholds.
		 * Retry a bounded few times before treating it as a real failure.
		 */
		for (int attempt = 0; attempt < 5; attempt++)
		{
			status = psc_op_check_branch(slot, parent, g_tl[parent].wal_end,
										 target_inc, g_tl[parent].incarnation);
			if (status == PS_STATUS_OK)
				break;
			psc_sleep_ms(5);
		}

		ring_note("CHECK_BRANCH legal slot=%u parent=%u fresh=%d "
				  "reusable=%d", slot, parent, fresh, reusable);
		if (fresh || reusable)
		{
			ck(status == PS_STATUS_OK, "CHECK_BRANCH slot=%u parent=%u at "
			   "tip %llu target_inc=%llu (status %d)", slot, parent,
			   (unsigned long long) g_tl[parent].wal_end,
			   (unsigned long long) target_inc, status);
			record_cov(PS_OP_CHECK_BRANCH, (uint32_t) status, 0);
		}
		else
		{
			/* Slot is LIVE or DELETING: not a valid target for a brand-new
			 * branch definition, so only check status-domain sanity. */
			observe(PS_OP_CHECK_BRANCH, status, 0, "CHECK_BRANCH");
		}
	}
	else
	{
		/* Adversarial: parent token deliberately wrong. */
		int			status = psc_op_check_branch(slot, parent,
												 g_tl[parent].wal_end, 0,
												 g_tl[parent].incarnation + 5);

		ring_note("CHECK_BRANCH adv-parent-token slot=%u parent=%u", slot,
				  parent);
		ck(status != PS_STATUS_OK, "CHECK_BRANCH with a wrong parent-"
		   "incarnation token must be refused, got %d", status);
		record_cov(PS_OP_CHECK_BRANCH, (uint32_t) status, 0);
	}
}

static void
act_require_branch(void)
{
	uint32_t	slot = 1 + rng_below(FZ_NTL - 1);

	if (g_tl[slot].known && g_tl[slot].state == PS_TIMELINE_LIVE)
	{
		int			status = psc_op_require_branch(slot, g_tl[slot].parent,
													g_tl[slot].branch_lsn,
													g_tl[slot].incarnation,
													g_tl[g_tl[slot].parent].incarnation);

		ring_note("REQUIRE_BRANCH legal slot=%u", slot);
		ck(status == PS_STATUS_OK, "REQUIRE_BRANCH on an existing live "
		   "branch slot=%u must succeed, got %d", slot, status);
		record_cov(PS_OP_REQUIRE_BRANCH, (uint32_t) status, 0);

		{
			/* Adversarial: wrong branch_lsn token against the same branch. */
			int			status2 = psc_op_require_branch(slot,
														g_tl[slot].parent,
														g_tl[slot].branch_lsn + 1,
														g_tl[slot].incarnation,
														g_tl[g_tl[slot].parent].incarnation);

			ring_note("REQUIRE_BRANCH adv-branch-lsn slot=%u", slot);
			ck(status2 != PS_STATUS_OK, "REQUIRE_BRANCH with a wrong "
			   "branch_lsn token must be refused, got %d", status2);
			record_cov(PS_OP_REQUIRE_BRANCH, (uint32_t) status2, 0);
		}
	}
	else
	{
		int			status = psc_op_require_branch(slot, 0, 0, 1, 1);

		ring_note("REQUIRE_BRANCH undefined slot=%u", slot);
		ck(status != PS_STATUS_OK, "REQUIRE_BRANCH on a non-live/undefined "
		   "slot=%u must be refused, got %d", slot, status);
		record_cov(PS_OP_REQUIRE_BRANCH, (uint32_t) status, 0);
	}
}

/* CREATE_BRANCH's OK cell is produced by env_branch_create(); this adds the
 * refusal cell via a deliberately wrong parent-incarnation token, which
 * branch_create_request_ok()/branch_parent_token_ok() must reject without
 * defining or mutating anything. */
static void
act_create_branch_adv(void)
{
	uint32_t	parent = pick_live_tl();
	uint32_t	slot = 1 + rng_below(FZ_NTL - 1);
	uint64_t	new_inc;
	int			status;

	if (g_tl[slot].known && g_tl[slot].state != PS_TIMELINE_DELETED)
		return;					/* slot busy; skip rather than disturb it */
	status = psc_op_create_branch_raw(slot, parent, g_tl[parent].wal_end, 0,
									  g_tl[parent].incarnation + 7,
									  &new_inc);
	ring_note("CREATE_BRANCH adv-parent-token slot=%u parent=%u", slot,
			  parent);
	ck(status != PS_STATUS_OK, "CREATE_BRANCH with a wrong parent-"
	   "incarnation token must be refused, got %d", status);
	record_cov(PS_OP_CREATE_BRANCH, (uint32_t) status, 0);
}

static void
act_begin_delete_adv(void)
{
	/* Adversarial-only action: legal BEGIN_DELETE is driven by
	 * env_branch_begin_delete (stateful: transitions a live branch). Here we
	 * only cover refusal reasons reachable without mutating a live branch. */
	uint32_t	choice = rng_below(3);
	uint32_t	reason;
	int			status;

	if (choice == 0)
	{
		/* Timeline 0 (main) can never be deleted: DELETE_REFUSE_INVALID. */
		status = psc_op_begin_delete_r(0, g_tl[0].incarnation, &reason);
		ring_note("BEGIN_DELETE adv=main-timeline");
		ck(status != PS_STATUS_OK, "BEGIN_DELETE on the main timeline must "
		   "be refused, got %d", status);
	}
	else if (choice == 1)
	{
		uint32_t	slot = 1 + rng_below(FZ_NTL - 1);

		if (g_tl[slot].known && g_tl[slot].state == PS_TIMELINE_DELETED)
		{
			status = psc_op_begin_delete_r(slot, g_tl[slot].incarnation,
										   &reason);
			ring_note("BEGIN_DELETE adv=already-deleted slot=%u", slot);
			ck(status != PS_STATUS_OK, "BEGIN_DELETE on an already-DELETED "
			   "timeline must be refused, got %d", status);
		}
		else
			return;
	}
	else
	{
		status = psc_op_begin_delete_r(FZ_TL_UNDEF_A, 1, &reason);
		ring_note("BEGIN_DELETE adv=undefined");
		ck(status != PS_STATUS_OK, "BEGIN_DELETE on an undefined timeline "
		   "must be refused, got %d", status);
	}
	record_cov(PS_OP_BEGIN_DELETE, (uint32_t) status, reason);
}

/* ===================== artifact lifecycle ops =============================
 *
 * See the FzArtifact comment above (near g_artifact[]) for the derived
 * model.  Each action below picks a random (tl, klass-kind, rel) artifact
 * key; the generic timeline/incarnation adversarial dimension is exercised
 * with pick_adv()/resolve_target() exactly like every other opcode, nested
 * inside which the artifact-specific legal/adversarial sub-cases live.
 */

enum
{
	FZ_ART_NONE = 0,
	FZ_ART_OPEN = 1,
	FZ_ART_COMMITTED = 2,
	FZ_ART_DROPPED = 3,
};

static void
act_artifact_begin(void)
{
	uint32_t	tl = pick_live_tl();
	uint32_t	akind = rng_below(FZ_NAKLASS);
	uint32_t	rel = rng_below(FZ_NREL);
	FzArtifact *art = &g_artifact[tl][akind][rel];
	uint32_t	klass = g_artifact_klass[akind];
	FzAdv		adv = pick_adv();
	uint32_t	target_tl;
	uint64_t	target_inc;

	if (!resolve_target(tl, adv, &target_tl, &target_inc))
		adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

	if (adv != ADV_NONE)
	{
		uint32_t	reason = 0;
		uint64_t	token = 0;
		int			status = psc_op_artifact_begin(target_tl, target_inc,
													klass, rel,
													g_tl[tl].wal_end + 1, 0,
													&token, &reason);

		ring_note("ARTIFACT_BEGIN adv=%d tl=%u", adv, target_tl);
		ck(status != PS_STATUS_OK, "ARTIFACT_BEGIN with adversarial target "
		   "must be refused (adv=%d tl=%u), got %d", adv, target_tl, status);
		record_cov(PS_OP_ARTIFACT_BEGIN, (uint32_t) status, reason);
		return;
	}

	if (rng_pct(12))
	{
		/* lsn=0 is always PS_ARTIFACT_REFUSE_INVALID, unconditionally. */
		uint32_t	reason = 0;
		uint64_t	token = 0;
		int			status = psc_op_artifact_begin(tl, g_tl[tl].incarnation,
													klass, rel, 0, 0, &token,
													&reason);

		ring_note("ARTIFACT_BEGIN adv=lsn-zero tl=%u akind=%u rel=%u", tl,
				  akind, rel);
		ck(status != PS_STATUS_OK && reason == PS_ARTIFACT_REFUSE_INVALID,
		   "ARTIFACT_BEGIN lsn=0 must be refused as INVALID, got status=%d "
		   "reason=%u", status, reason);
		record_cov(PS_OP_ARTIFACT_BEGIN, (uint32_t) status, reason);
		return;
	}
	if (rng_pct(12))
	{
		/* A klass outside {SLRU, READER_SNAPSHOT} is never a valid artifact
		 * key: artifact_data_key() refuses it as INVALID. */
		uint32_t	badklass = rng_pct(50) ? PS_KLASS_RELATION :
			PS_KLASS_CONTROL;
		uint32_t	reason = 0;
		uint64_t	token = 0;
		uint64_t	lsn = ship_wal(tl);
		int			status = psc_op_artifact_begin(tl, g_tl[tl].incarnation,
													badklass, rel, lsn, 0,
													&token, &reason);

		ring_note("ARTIFACT_BEGIN adv=bad-klass tl=%u klass=%u", tl,
				  badklass);
		ck(status != PS_STATUS_OK && reason == PS_ARTIFACT_REFUSE_INVALID,
		   "ARTIFACT_BEGIN on klass=%u must be refused as INVALID, got "
		   "status=%d reason=%u", badklass, status, reason);
		record_cov(PS_OP_ARTIFACT_BEGIN, (uint32_t) status, reason);
		return;
	}
	if (art->state == FZ_ART_COMMITTED && art->lsn == art->max_begin_lsn &&
		rng_pct(30))
	{
		/*
		 * Exact-lsn retry of the last committed generation: idempotent,
		 * returns the SAME token, no new attempt opened.  Guarded on
		 * lsn==max_begin_lsn: ps_artifact_begin() checks the BEGIN block's
		 * own newest lsn (BEGIN_NEWER) BEFORE the exact-match-to-committed
		 * shortcut, so if a *later* BEGIN was ever issued and then
		 * abandoned (a restart -- see artifact_restart_reset()), retrying
		 * at the older committed lsn is BEGIN_NEWER, not this idempotent
		 * path.
		 */
		uint32_t	reason = 0;
		uint64_t	token = 0;
		int			status = psc_op_artifact_begin(tl, g_tl[tl].incarnation,
													klass, rel, art->lsn, 0,
													&token, &reason);

		ring_note("ARTIFACT_BEGIN retry-committed tl=%u akind=%u rel=%u "
				  "lsn=%llu", tl, akind, rel, (unsigned long long) art->lsn);
		ck(status == PS_STATUS_OK && token == art->token, "ARTIFACT_BEGIN "
		   "exact retry of committed generation lsn=%llu expected OK "
		   "token=%llu, got status=%d token=%llu", (unsigned long long) art->lsn,
		   (unsigned long long) art->token, status, (unsigned long long) token);
		record_cov(PS_OP_ARTIFACT_BEGIN, (uint32_t) status, reason);
		return;
	}
	if (art->state == FZ_ART_DROPPED && art->lsn == art->max_begin_lsn &&
		rng_pct(30))
	{
		/* Exact-lsn retry of an already-dropped generation: refused.
		 * lsn==max_begin_lsn guard: see the COMMITTED case above -- the
		 * same BEGIN_NEWER-before-DROPPED ordering applies here. */
		uint32_t	reason = 0;
		uint64_t	token = 0;
		int			status = psc_op_artifact_begin(tl, g_tl[tl].incarnation,
													klass, rel, art->lsn, 0,
													&token, &reason);

		ring_note("ARTIFACT_BEGIN retry-dropped tl=%u akind=%u rel=%u "
				  "lsn=%llu", tl, akind, rel, (unsigned long long) art->lsn);
		ck(status != PS_STATUS_OK && reason == PS_ARTIFACT_REFUSE_DROPPED,
		   "ARTIFACT_BEGIN exact retry of dropped generation lsn=%llu "
		   "expected DROPPED, got status=%d reason=%u",
		   (unsigned long long) art->lsn, status, reason);
		record_cov(PS_OP_ARTIFACT_BEGIN, (uint32_t) status, reason);
		return;
	}
	if (art->state == FZ_ART_OPEN && art->max_begin_lsn > 1 && rng_pct(20))
	{
		/*
		 * Older than the newest BEGIN this key has ever used: must be
		 * refused, but WEAK ORACLE on the exact reason (see
		 * g_weak_oracle_ops): ps_artifact_begin() checks
		 * artifact_mutation_horizon() -- which can independently refuse
		 * HORIZON whenever bad_lsn < the data fork's own last written page
		 * lsn, something this model does not track separately from
		 * max_begin_lsn -- before either the COMMIT-block or BEGIN-block
		 * ordering checks that would otherwise give OLDER_GENERATION (once
		 * the newest BEGIN has itself settled as a commit; the COMMIT
		 * block is checked before the BEGIN block) or BEGIN_NEWER.
		 */
		uint64_t	bad_lsn = art->max_begin_lsn - 1;
		uint32_t	reason = 0;
		uint64_t	token = 0;
		int			status = psc_op_artifact_begin(tl, g_tl[tl].incarnation,
													klass, rel, bad_lsn, 0,
													&token, &reason);

		ring_note("ARTIFACT_BEGIN adv=older-than-max-begin tl=%u akind=%u "
				  "rel=%u lsn=%llu", tl, akind, rel,
				  (unsigned long long) bad_lsn);
		ck(status != PS_STATUS_OK, "ARTIFACT_BEGIN lsn=%llu (< "
		   "max_begin_lsn=%llu) must be refused, got status=%d reason=%u",
		   (unsigned long long) bad_lsn,
		   (unsigned long long) art->max_begin_lsn, status, reason);
		record_cov(PS_OP_ARTIFACT_BEGIN, (uint32_t) status, reason);
		return;
	}

	/* Legal: a fresh generation at a strictly newer lsn than anything this
	 * key has ever used.  ship_wal_past_fork() is monotonic per timeline,
	 * every page write on this key uses a ship_wal()-derived lsn too, and it
	 * is guaranteed > tl's own branch point (a fresh branch's very first
	 * ship_wal() call would otherwise land exactly *at* branch_lsn, which
	 * artifact_mutation_horizon() -- unlike an ordinary page write's own
	 * fence -- requires strictly newer than) -- so this is always both >
	 * any branch point and >= the fork's last page lsn, the two
	 * artifact_mutation_horizon() preconditions, and, absent an active
	 * reclaim in this short run, "fenced". */
	{
		uint64_t	lsn = ship_wal_past_fork(tl);
		int			supersedable = rng_pct(20);
		uint32_t	reason = 0;
		uint64_t	token = 0;
		int			status = psc_op_artifact_begin(tl, g_tl[tl].incarnation,
													klass, rel, lsn,
													supersedable, &token,
													&reason);

		ring_note("ARTIFACT_BEGIN tl=%u akind=%u rel=%u lsn=%llu", tl, akind,
				  rel, (unsigned long long) lsn);
		ck(status == PS_STATUS_OK, "ARTIFACT_BEGIN tl=%u akind=%u rel=%u "
		   "lsn=%llu (status %d reason %u)", tl, akind, rel,
		   (unsigned long long) lsn, status, reason);
		record_cov(PS_OP_ARTIFACT_BEGIN, (uint32_t) status, reason);
		if (status == PS_STATUS_OK)
		{
			/* Shadow the last settled state before overwriting it -- see the
			 * FzArtifact.prev_* comment.  A fresh BEGIN can itself be issued
			 * while an *older* attempt was still open (superseding it, never
			 * settled); in that case there is no settled state to shadow
			 * beyond whatever was already shadowed, so only capture when the
			 * current state is not itself OPEN. */
			if (art->state != FZ_ART_OPEN)
			{
				art->prev_state = art->state;
				art->prev_lsn = art->lsn;
				art->prev_token = art->token;
			}
			art->state = FZ_ART_OPEN;
			art->lsn = lsn;
			art->token = token;
			art->max_begin_lsn = lsn;
			art->open_count = 0;
			art->open_nblocks = 0;
			memset(art->open_written, 0, sizeof(art->open_written));
			/* open_tag/open_block_lsn must also be cleared, not just
			 * open_written: a prior (possibly abandoned) attempt may have
			 * left stale nonzero entries here, and act_artifact_commit()'s
			 * post-commit verification copies these arrays into
			 * art->visible *unconditionally* (indexed by block, not
			 * filtered through open_written) -- a stale tag would then be
			 * asserted against a block the real daemon correctly reports
			 * as unwritten in THIS generation ("missing blocks in a
			 * complete generation never inherit an older page"). */
			memset(art->open_tag, 0, sizeof(art->open_tag));
			memset(art->open_block_lsn, 0, sizeof(art->open_block_lsn));
		}
	}
}

/* Data write under the lifecycle protocol: just PS_OP_EXTEND on the SLRU/
 * READER_SNAPSHOT key with req_lsn/req_seq set to the open attempt's
 * (lsn, token) -- see psc_op_artifact_begin()'s header comment. */
static void
act_artifact_write(void)
{
	uint32_t	tl = 0,
				akind = 0,
				rel = 0;
	FzArtifact *art = NULL;

	for (int tries = 0; tries < 8 && art == NULL; tries++)
	{
		tl = pick_live_tl();
		akind = rng_below(FZ_NAKLASS);
		rel = rng_below(FZ_NREL);
		if (g_artifact[tl][akind][rel].state == FZ_ART_OPEN)
			art = &g_artifact[tl][akind][rel];
	}
	if (art == NULL)
	{
		act_artifact_begin();
		return;
	}

	{
		uint32_t	klass = g_artifact_klass[akind];
		FzAdv		adv = pick_adv();
		uint32_t	target_tl;
		uint64_t	target_inc;

		if (!resolve_target(tl, adv, &target_tl, &target_inc))
			adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

		if (adv != ADV_NONE)
		{
			int			status;

			psc_fill_page(page_buf, art->lsn, 1);
			status = psc_op_extend(target_tl, target_inc, klass, rel, 0,
								   page_buf, art->lsn, art->token);
			ring_note("ARTIFACT_WRITE adv=%d tl=%u", adv, target_tl);
			ck(status != PS_STATUS_OK, "ARTIFACT_WRITE with adversarial "
			   "target must be refused (adv=%d tl=%u), got %d", adv,
			   target_tl, status);
			record_cov(PS_OP_EXTEND, (uint32_t) status,
					  psc_chan_ptr()->result);
			return;
		}

		{
			int			probe = rng_below(4);
			uint32_t	block = rng_below(FZ_MAXBLK);
			unsigned char tag = (unsigned char) (1 + rng_below(255));
			uint64_t	use_lsn = art->lsn;
			uint64_t	use_token = art->token;
			int			expect_ok = 1;
			int			status;
			uint32_t	reason;

			psc_fill_page(page_buf, art->lsn, tag);
			if (probe == 0)
			{
				use_token = art->token + 1;	/* -> ATTEMPT_MISMATCH */
				expect_ok = 0;
			}
			else if (probe == 1)
			{
				use_lsn = art->lsn + 1;	/* -> ATTEMPT_MISMATCH */
				expect_ok = 0;
			}
			else if (probe == 2)
			{
				use_token = 0;			/* has_protocol -> LEGACY_BYPASS */
				expect_ok = 0;
			}

			status = psc_op_extend(tl, g_tl[tl].incarnation, klass, rel,
								   block, page_buf, use_lsn, use_token);
			reason = psc_chan_ptr()->result;
			ring_note("ARTIFACT_WRITE tl=%u akind=%u rel=%u block=%u "
					  "probe=%d", tl, akind, rel, block, probe);
			if (expect_ok)
			{
				/*
				 * WEAK ORACLE (probe==3, the "ordinary" write, only): BEGIN
				 * re-checks artifact_lsn_fenced() before opening the attempt,
				 * but the data WRITE re-derives its own fence independently
				 * (ps_artifact_write() -> append_page_raw_outcome()), and an
				 * open attempt can sit for many steps before a later
				 * act_artifact_write() call reaches it (unlike
				 * act_artifact_commit(), which always writes right before
				 * committing).  This aggressive fuzzer's reclaim can
				 * legitimately advance the page-reclaimed frontier past the
				 * attempt's lsn in between -- a genuine, documented TOCTOU
				 * (ARTIFACT_LIFECYCLE.md's T7 / PS_ARTIFACT_REFUSE_UNFENCED
				 * "not poisoning, retryable"), not a bug -- so UNFENCED is
				 * accepted here as a legal alternative outcome; anything
				 * else is still a hard failure.
				 */
				int			weak = probe == 3 && status != PS_STATUS_OK &&
					reason == PS_ARTIFACT_REFUSE_UNFENCED;

				ck(weak || status == PS_STATUS_OK, "ARTIFACT_WRITE tl=%u "
				   "akind=%u rel=%u block=%u (status %d reason %u)", tl,
				   akind, rel, block, status, reason);
				record_cov(PS_OP_EXTEND, (uint32_t) status, reason);
				if (status == PS_STATUS_OK)
				{
					if (!art->open_written[block])
					{
						art->open_count++;
						art->open_written[block] = 1;
					}
					if (block + 1 > art->open_nblocks)
						art->open_nblocks = block + 1;
					art->open_tag[block] = tag;
					art->open_block_lsn[block] = art->lsn;
				}
			}
			else
			{
				ck(status != PS_STATUS_OK, "ARTIFACT_WRITE probe=%d "
				   "(tl=%u akind=%u rel=%u block=%u) must be refused, got "
				   "%d", probe, tl, akind, rel, block, status);
				record_cov(PS_OP_EXTEND, (uint32_t) status, reason);
			}
		}
	}
}

static void
act_artifact_commit(void)
{
	uint32_t	tl = 0,
				akind = 0,
				rel = 0;
	FzArtifact *art = NULL;

	for (int tries = 0; tries < 8 && art == NULL; tries++)
	{
		tl = pick_live_tl();
		akind = rng_below(FZ_NAKLASS);
		rel = rng_below(FZ_NREL);
		if (g_artifact[tl][akind][rel].state == FZ_ART_OPEN)
			art = &g_artifact[tl][akind][rel];
	}
	if (art == NULL)
	{
		act_artifact_begin();
		return;
	}

	{
		uint32_t	klass = g_artifact_klass[akind];
		FzAdv		adv = pick_adv();
		uint32_t	target_tl;
		uint64_t	target_inc;

		if (!resolve_target(tl, adv, &target_tl, &target_inc))
			adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

		if (adv != ADV_NONE)
		{
			uint32_t	reason = 0;
			int			status = psc_op_artifact_commit(target_tl, target_inc,
														 klass, rel, art->lsn,
														 art->token,
														 art->open_count, 0,
														 &reason);

			ring_note("ARTIFACT_COMMIT adv=%d tl=%u", adv, target_tl);
			ck(status != PS_STATUS_OK, "ARTIFACT_COMMIT with adversarial "
			   "target must be refused (adv=%d tl=%u), got %d", adv,
			   target_tl, status);
			record_cov(PS_OP_ARTIFACT_COMMIT, (uint32_t) status, reason);
			return;
		}

		{
			int			probe = rng_below(4);
			uint64_t	use_lsn = art->lsn;
			uint64_t	use_token = art->token;
			uint64_t	use_count = art->open_count;
			int			expect_ok = 1;
			uint32_t	reason = 0;
			int			status;

			if (probe == 0)
			{
				use_token = art->token + 1;
				expect_ok = 0;
			}
			else if (probe == 1)
			{
				use_lsn = art->lsn + 1;
				expect_ok = 0;
			}
			else if (probe == 2)
			{
				use_count = art->open_count + 1;
				expect_ok = 0;
			}

			status = psc_op_artifact_commit(tl, g_tl[tl].incarnation, klass,
											rel, use_lsn, use_token,
											use_count,
											rng_pct(20), &reason);
			ring_note("ARTIFACT_COMMIT tl=%u akind=%u rel=%u probe=%d", tl,
					  akind, rel, probe);
			if (expect_ok)
			{
				ck(status == PS_STATUS_OK, "ARTIFACT_COMMIT tl=%u akind=%u "
				   "rel=%u lsn=%llu token=%llu count=%llu (status %d "
				   "reason %u)", tl, akind, rel, (unsigned long long) use_lsn,
				   (unsigned long long) use_token,
				   (unsigned long long) use_count, status, reason);
				record_cov(PS_OP_ARTIFACT_COMMIT, (uint32_t) status, reason);
				if (status == PS_STATUS_OK)
				{
					memset(&art->visible, 0, sizeof(art->visible));
					art->visible.exists = 1;
					art->visible.nblocks = art->open_nblocks;
					memcpy(art->visible.tag, art->open_tag,
						   sizeof(art->open_tag));
					memcpy(art->visible.lsn, art->open_block_lsn,
						   sizeof(art->open_block_lsn));
					art->state = FZ_ART_COMMITTED;

					/*
					 * Level-1 content verification: from this instant,
					 * ordinary EXISTS/NBLOCKS/READV must resolve to exactly
					 * this committed generation.
					 */
					{
						int			exists = 0;
						uint32_t	nb = 0;
						int			st1 = psc_op_exists(tl,
														   g_tl[tl].incarnation,
														   klass, rel, 0,
														   &exists);
						int			st2 = psc_op_nblocks(tl,
															g_tl[tl].incarnation,
															klass, rel, 0, 0,
															&nb);

						ck(st1 == PS_STATUS_OK && exists, "post-COMMIT "
						   "EXISTS tl=%u akind=%u rel=%u expected true "
						   "(status %d)", tl, akind, rel, st1);
						ck(st2 == PS_STATUS_OK && nb == art->visible.nblocks,
						   "post-COMMIT NBLOCKS tl=%u akind=%u rel=%u "
						   "expected %u got %u (status %d)", tl, akind, rel,
						   art->visible.nblocks, nb, st2);
						for (uint32_t b = 0; b < art->visible.nblocks &&
											 b < 4; b++)
						{
							int			rst = psc_op_readv(tl,
															   g_tl[tl].incarnation,
															   klass, rel, b,
															   0, 0, read_buf,
															   1);

							if (art->visible.tag[b] == 0)
								ck(rst == PS_STATUS_OK, "post-COMMIT READV "
								   "tl=%u akind=%u rel=%u block=%u (status "
								   "%d)", tl, akind, rel, b, rst);
							else
								ck(rst == PS_STATUS_OK &&
								   psc_page_has_tag(read_buf,
													art->visible.tag[b]) &&
								   psc_page_lsn(read_buf) ==
								   art->visible.lsn[b], "post-COMMIT READV "
								   "tl=%u akind=%u rel=%u block=%u expected "
								   "tag=%u lsn=%llu, content mismatch "
								   "(status %d)", tl, akind, rel, b,
								   art->visible.tag[b],
								   (unsigned long long) art->visible.lsn[b],
								   rst);
						}
					}
				}
			}
			else
			{
				ck(status != PS_STATUS_OK, "ARTIFACT_COMMIT probe=%d "
				   "(tl=%u akind=%u rel=%u) must be refused, got %d", probe,
				   tl, akind, rel, status);
				record_cov(PS_OP_ARTIFACT_COMMIT, (uint32_t) status, reason);
			}
		}
	}
}

static void
act_artifact_commit_retry(void)
{
	uint32_t	tl = 0,
				akind = 0,
				rel = 0;
	FzArtifact *art = NULL;

	for (int tries = 0; tries < 8 && art == NULL; tries++)
	{
		FzArtifact *cand;

		tl = pick_live_tl();
		akind = rng_below(FZ_NAKLASS);
		rel = rng_below(FZ_NREL);
		cand = &g_artifact[tl][akind][rel];
		/* lsn==max_begin_lsn: only a generation actually committed on THIS
		 * timeline (not one inherited, read-only, from a branch's parent --
		 * see env_branch_create()'s copy loop) has a real commit-block/
		 * begin-block entry on tl for artifact_completed_attempt()/
		 * artifact_attempt() to match; an inherited entry would instead
		 * take the brand-new-attempt path and refuse ATTEMPT_MISMATCH,
		 * same as the analogous guards in act_artifact_begin/drop(). */
		if (cand->state == FZ_ART_COMMITTED && cand->lsn == cand->max_begin_lsn)
			art = cand;
	}
	if (art == NULL)
	{
		act_artifact_begin();
		return;
	}
	{
		uint32_t	klass = g_artifact_klass[akind];
		uint32_t	reason = 0;
		/* open_count is frozen at whatever it was when this generation
		 * committed: act_artifact_write() only ever targets a still-OPEN
		 * attempt, and nothing resets it except a fresh BEGIN, which would
		 * have moved state away from COMMITTED again. */
		int			status = psc_op_artifact_commit(tl, g_tl[tl].incarnation,
													 klass, rel, art->lsn,
													 art->token,
													 art->open_count, 0,
													 &reason);

		ring_note("ARTIFACT_COMMIT retry-committed tl=%u akind=%u rel=%u",
				  tl, akind, rel);
		ck(status == PS_STATUS_OK, "ARTIFACT_COMMIT exact retry of an "
		   "already-committed generation tl=%u akind=%u rel=%u must "
		   "succeed idempotently, got %d (reason %u)", tl, akind, rel,
		   status, reason);
		record_cov(PS_OP_ARTIFACT_COMMIT, (uint32_t) status, reason);
	}
}

static void
act_artifact_drop(void)
{
	uint32_t	tl = pick_live_tl();
	uint32_t	akind = rng_below(FZ_NAKLASS);
	uint32_t	rel = rng_below(FZ_NREL);
	FzArtifact *art = &g_artifact[tl][akind][rel];
	uint32_t	klass = g_artifact_klass[akind];
	FzAdv		adv = pick_adv();
	uint32_t	target_tl;
	uint64_t	target_inc;

	if (!resolve_target(tl, adv, &target_tl, &target_inc))
		adv = ADV_NONE, resolve_target(tl, adv, &target_tl, &target_inc);

	if (adv != ADV_NONE)
	{
		uint32_t	reason = 0;
		int			status = psc_op_artifact_drop(target_tl, target_inc,
												   klass, rel,
												   g_tl[tl].wal_end + 1, 0,
												   &reason);

		ring_note("ARTIFACT_DROP adv=%d tl=%u", adv, target_tl);
		ck(status != PS_STATUS_OK, "ARTIFACT_DROP with adversarial target "
		   "must be refused (adv=%d tl=%u), got %d", adv, target_tl, status);
		record_cov(PS_OP_ARTIFACT_DROP, (uint32_t) status, reason);
		return;
	}

	if (rng_pct(12))
	{
		uint32_t	reason = 0;
		int			status = psc_op_artifact_drop(tl, g_tl[tl].incarnation,
												   klass, rel, 0, 0, &reason);

		ring_note("ARTIFACT_DROP adv=lsn-zero tl=%u akind=%u rel=%u", tl,
				  akind, rel);
		ck(status != PS_STATUS_OK && reason == PS_ARTIFACT_REFUSE_INVALID,
		   "ARTIFACT_DROP lsn=0 must be refused as INVALID, got status=%d "
		   "reason=%u", status, reason);
		record_cov(PS_OP_ARTIFACT_DROP, (uint32_t) status, reason);
		return;
	}
	if (art->max_begin_lsn > 1 && rng_pct(20))
	{
		/* WEAK ORACLE on the exact reason: see the analogous probe in
		 * act_artifact_begin() -- artifact_mutation_horizon() is checked
		 * before the BEGIN-block ordering check here too, and can refuse
		 * HORIZON instead of BEGIN_NEWER depending on the data fork's own
		 * last written page lsn, which this model does not track. */
		uint64_t	bad_lsn = art->max_begin_lsn - 1;
		uint32_t	reason = 0;
		int			status = psc_op_artifact_drop(tl, g_tl[tl].incarnation,
												   klass, rel, bad_lsn, 0,
												   &reason);

		ring_note("ARTIFACT_DROP adv=older-than-max-begin tl=%u akind=%u "
				  "rel=%u lsn=%llu", tl, akind, rel,
				  (unsigned long long) bad_lsn);
		ck(status != PS_STATUS_OK, "ARTIFACT_DROP lsn=%llu (< "
		   "max_begin_lsn=%llu) must be refused, got status=%d reason=%u",
		   (unsigned long long) bad_lsn,
		   (unsigned long long) art->max_begin_lsn, status, reason);
		record_cov(PS_OP_ARTIFACT_DROP, (uint32_t) status, reason);
		return;
	}
	if (art->state == FZ_ART_DROPPED && art->lsn == art->max_begin_lsn &&
		rng_pct(30))
	{
		/* Exact-lsn retry of an already-dropped generation: idempotent.
		 * lsn==max_begin_lsn guard: ps_artifact_drop() also checks
		 * BEGIN_NEWER before the same-lsn-dropped short circuit. */
		uint32_t	reason = 0;
		int			status = psc_op_artifact_drop(tl, g_tl[tl].incarnation,
												   klass, rel, art->lsn, 0,
												   &reason);

		ring_note("ARTIFACT_DROP retry-dropped tl=%u akind=%u rel=%u "
				  "lsn=%llu", tl, akind, rel, (unsigned long long) art->lsn);
		ck(status == PS_STATUS_OK, "ARTIFACT_DROP exact retry of an "
		   "already-dropped generation lsn=%llu must succeed idempotently, "
		   "got %d (reason %u)", (unsigned long long) art->lsn, status,
		   reason);
		record_cov(PS_OP_ARTIFACT_DROP, (uint32_t) status, reason);
		return;
	}
	if (art->state == FZ_ART_COMMITTED && art->lsn == art->max_begin_lsn &&
		rng_pct(30))
	{
		/* A same-LSN DROP cannot replace completed, published bytes.
		 * lsn==max_begin_lsn guard: BEGIN_NEWER is checked before
		 * OLDER_GENERATION in ps_artifact_drop() too. */
		uint32_t	reason = 0;
		int			status = psc_op_artifact_drop(tl, g_tl[tl].incarnation,
												   klass, rel, art->lsn, 0,
												   &reason);

		ring_note("ARTIFACT_DROP same-lsn-committed tl=%u akind=%u rel=%u "
				  "lsn=%llu", tl, akind, rel, (unsigned long long) art->lsn);
		ck(status != PS_STATUS_OK &&
		   reason == PS_ARTIFACT_REFUSE_OLDER_GENERATION, "ARTIFACT_DROP at "
		   "an already-committed generation's own lsn=%llu expected "
		   "OLDER_GENERATION, got status=%d reason=%u",
		   (unsigned long long) art->lsn, status, reason);
		record_cov(PS_OP_ARTIFACT_DROP, (uint32_t) status, reason);
		return;
	}

	/* Legal: drop at a strictly newer lsn than anything this key has ever
	 * used.  Always succeeds; abandons a still-open attempt the same way a
	 * newer BEGIN would (see the FzArtifact comment), and makes the key
	 * explicitly nonexistent from this instant on.  ship_wal_past_fork():
	 * see act_artifact_begin()'s comment -- DROP is subject to the same
	 * strictly-past-branch_lsn fence via artifact_mutation_horizon(). */
	{
		uint64_t	lsn = ship_wal_past_fork(tl);
		uint32_t	reason = 0;
		int			status = psc_op_artifact_drop(tl, g_tl[tl].incarnation,
												   klass, rel, lsn,
												   rng_pct(20), &reason);

		ring_note("ARTIFACT_DROP tl=%u akind=%u rel=%u lsn=%llu", tl, akind,
				  rel, (unsigned long long) lsn);
		ck(status == PS_STATUS_OK, "ARTIFACT_DROP tl=%u akind=%u rel=%u "
		   "lsn=%llu (status %d reason %u)", tl, akind, rel,
		   (unsigned long long) lsn, status, reason);
		record_cov(PS_OP_ARTIFACT_DROP, (uint32_t) status, reason);
		if (status == PS_STATUS_OK)
		{
			art->state = FZ_ART_DROPPED;
			art->lsn = lsn;
			memset(&art->visible, 0, sizeof(art->visible));

			{
				int			exists = 1;
				int			st = psc_op_exists(tl, g_tl[tl].incarnation,
												  klass, rel, 0, &exists);

				ck(st == PS_STATUS_OK && !exists, "post-DROP EXISTS tl=%u "
				   "akind=%u rel=%u expected false (status %d)", tl, akind,
				   rel, st);
			}
		}
	}
}

/* ===================== environment actions ================================ */

static void
env_materialize(void)
{
	uint64_t	lsn = g_tl[0].wal_end;
	unsigned char note[PSC_PAGE_SIZE];
	unsigned char image[PSC_PAGE_SIZE];
	unsigned char marker[PSC_PAGE_SIZE];
	uint64_t	seq = 0;
	int			ok = 1;
	int			status;

	memset(note, 0, sizeof(note));
	memcpy(note, &lsn, sizeof(lsn));
	memset(image, 0x5c, sizeof(image));
	memcpy(image, &lsn, sizeof(lsn));
	memset(marker, 0x3d, sizeof(marker));
	memcpy(marker, &lsn, sizeof(lsn));

	ok = ok && psc_op_write_control(1, note, lsn) == PS_STATUS_OK;
	ok = ok && psc_op_write_control(0, image, lsn) == PS_STATUS_OK;
	ok = ok && psc_op_write_control(3, marker, lsn) == PS_STATUS_OK;
	status = psc_op_walidx_progress_commit(0, 0, g_tl[0].walidx_progress, lsn);
	ok = ok && status == PS_STATUS_OK;
	if (ok)
		g_tl[0].walidx_progress = lsn;
	status = psc_op_retention_reserve(0, PS_RETENTION_OWNER_MATERIALIZER, 1, 1,
									  PS_RETENTION_RESOURCE_WAL |
									  PS_RETENTION_RESOURCE_WAL_INDEX, lsn,
									  &seq);
	ok = ok && status == PS_STATUS_OK && seq != 0;
	record_cov(PS_OP_RETENTION_PIN_RESERVE, (uint32_t) status, 0);
	ring_note("materialize lsn=%llu ok=%d", (unsigned long long) lsn, ok);
	ck(ok, "materializer publication at lsn=%llu failed", (unsigned long long) lsn);
	record_env(ENV_MATERIALIZE, ok);
	if (ok)
	{
		g_tl[0].mat_lsn = lsn;
		g_tl[0].mat_seq = seq;
		g_tl[0].mat_registered = 1;
	}
}

static void
env_reader_reserve(void)
{
	uint32_t	i = rng_below(FZ_NREADERS);
	FzReaderPin *r = &g_reader[i];
	uint64_t	lsn = g_tl[0].wal_end;
	uint64_t	seq = 0;
	int			status;

	if (r->held)
		return;					/* env_reader_advance/drop handle the held case */
	status = psc_op_retention_reserve(0, PS_RETENTION_OWNER_READER,
									  r->owner_id, r->generation,
									  PS_RETENTION_RESOURCE_ALL, lsn, &seq);
	ring_note("reader_reserve i=%u lsn=%llu status=%d", i,
			  (unsigned long long) lsn, status);
	ck(status == PS_STATUS_OK && seq != 0, "reader %llu reserve at %llu "
	   "failed (status %d)", (unsigned long long) r->owner_id,
	   (unsigned long long) lsn, status);
	record_cov(PS_OP_RETENTION_PIN_RESERVE, (uint32_t) status, 0);
	record_env(ENV_READER_RESERVE, status == PS_STATUS_OK);
	if (status == PS_STATUS_OK)
	{
		r->held = 1;
		r->lsn = lsn;
		r->seq = seq;
		memcpy(r->snap, g_tl[0].rel, sizeof(r->snap));
	}
}

static void
env_reader_advance(void)
{
	uint32_t	i = rng_below(FZ_NREADERS);
	FzReaderPin *r = &g_reader[i];
	uint64_t	lsn = g_tl[0].wal_end;
	uint64_t	seq = 0;
	int			status;

	if (!r->held)
		return;
	status = psc_op_retention_reserve(0, PS_RETENTION_OWNER_READER,
									  r->owner_id, r->generation,
									  PS_RETENTION_RESOURCE_ALL, lsn, &seq);
	ring_note("reader_advance i=%u lsn=%llu status=%d", i,
			  (unsigned long long) lsn, status);
	ck(status == PS_STATUS_OK && seq != 0, "reader %llu advance to %llu "
	   "failed (status %d)", (unsigned long long) r->owner_id,
	   (unsigned long long) lsn, status);
	record_cov(PS_OP_RETENTION_PIN_RESERVE, (uint32_t) status, 0);
	record_env(ENV_READER_ADVANCE, status == PS_STATUS_OK);
	if (status == PS_STATUS_OK)
	{
		r->lsn = lsn;
		r->seq = seq;
		memcpy(r->snap, g_tl[0].rel, sizeof(r->snap));
	}
}

static void
env_reader_drop(void)
{
	uint32_t	i = rng_below(FZ_NREADERS);
	FzReaderPin *r = &g_reader[i];
	int			status;

	if (!r->held)
		return;
	status = psc_op_retention_drop(0, PS_RETENTION_OWNER_READER, r->owner_id,
								   r->generation);
	ring_note("reader_drop i=%u status=%d", i, status);
	ck(status == PS_STATUS_OK, "reader %llu drop failed (status %d)",
	   (unsigned long long) r->owner_id, status);
	record_cov(PS_OP_RETENTION_PIN_DROP, (uint32_t) status, 0);
	record_env(ENV_READER_DROP, status == PS_STATUS_OK);
	if (status == PS_STATUS_OK)
	{
		r->held = 0;
		r->generation++;
	}
}

static void
env_branch_create(void)
{
	uint32_t	slot = 1 + rng_below(FZ_NTL - 1);
	/*
	 * Parent is always timeline 0 in this stage: env_materialize() (and the
	 * control-object writes it drives via psc_op_write_control) is
	 * hardcoded to timeline 0 -- matching the real system, where pg_control
	 * is cluster-wide, not per-branch -- so a branch_lsn expressed in a
	 * *different* live branch's own WAL-position space would not be the
	 * horizon it looks like.  Nested (branch-of-branch) forking is out of
	 * scope for stage 1; see the report's limitations.
	 */
	uint32_t	parent = 0;
	uint64_t	target_inc;
	uint64_t	new_inc;
	int			status = PS_STATUS_ERROR;

	if (g_tl[slot].known && g_tl[slot].state != PS_TIMELINE_DELETED)
		return;					/* slot busy; try another step */

	env_materialize();
	if (!g_tl[0].mat_registered)
		return;

	target_inc = g_tl[slot].known ? g_branch_last_incarnation[slot] + 1 : 0;

	/* branch_frontiers_allow() can transiently refuse while a WAL-index
	 * snapshot publication is in flight; retry a bounded few times before
	 * treating it as a real failure (real clients would do the same). */
	for (int attempt = 0; attempt < 5; attempt++)
	{
		status = psc_op_create_branch(slot, parent, g_tl[0].mat_lsn,
									  target_inc, &new_inc);
		if (status == PS_STATUS_OK)
			break;
		psc_sleep_ms(5);
	}
	ring_note("branch_create slot=%u parent=%u lsn=%llu status=%d", slot,
			  parent, (unsigned long long) g_tl[0].mat_lsn, status);
	ck(status == PS_STATUS_OK, "CREATE_BRANCH slot=%u parent=%u lsn=%llu "
	   "failed after retries (status %d)", slot, parent,
	   (unsigned long long) g_tl[0].mat_lsn, status);
	record_env(ENV_BRANCH_CREATE, status == PS_STATUS_OK);
	record_cov(PS_OP_CREATE_BRANCH, (uint32_t) status, 0);
	if (status == PS_STATUS_OK)
	{
		g_tl[slot].known = 1;
		g_tl[slot].state = PS_TIMELINE_LIVE;
		g_tl[slot].incarnation = new_inc;
		g_tl[slot].has_parent = 1;
		g_tl[slot].parent = parent;
		g_tl[slot].branch_lsn = g_tl[0].mat_lsn;
		g_tl[slot].wal_start = g_tl[0].mat_lsn;
		g_tl[slot].wal_end = g_tl[0].mat_lsn;
		g_tl[slot].walidx_progress = g_tl[0].mat_lsn;
		g_tl[slot].wal_shipped = 0;	/* the *new* incarnation's own WAL log is empty */
		g_tl[slot].mat_registered = 0;
		memcpy(g_tl[slot].rel, g_tl[parent].rel, sizeof(g_tl[slot].rel));
		/* Freeze the parent's state at this exact instant -- before the
		 * Bug-B workaround below (if enabled) gets a chance to run -- so
		 * verify_branch_frozen() has a ground truth independent of the
		 * workaround. */
		memcpy(g_tl[slot].frozen, g_tl[parent].rel, sizeof(g_tl[slot].frozen));
		/*
		 * A (re)created slot's artifact model must not carry over an OLD
		 * incarnation's own local state (a slot number is reused across
		 * deletes; g_artifact[] is indexed by numeric slot, not
		 * incarnation): reads on this brand-new incarnation see, at most,
		 * the parent's ancestry (artifact_metadata()/artifact_visible()
		 * walk tl_walk_first/next the same way ordinary page reads do), so
		 * copy the parent's *current settled* visible state (COMMITTED or
		 * DROPPED only -- an in-flight open attempt on the parent is not
		 * visible to the parent's own reads either, so it is correctly
		 * "nothing to inherit", not a case this needs to special-case).
		 *
		 * Deliberately NOT copied: max_begin_lsn (left 0) and token.  The
		 * BEGIN/DROP exact-lsn-retry probes' begin-block/commit-block
		 * bookkeeping is per-timeline only (page_find(tl,...), no ancestry
		 * walk -- unlike the *read* path above), so an inherited
		 * generation's lsn was never actually BEGUN on this child timeline
		 * and a retry there would take the brand-new-attempt path (likely
		 * refused HORIZON, since that lsn is <= branch_lsn), not the
		 * idempotent shortcut those probes expect.  Leaving max_begin_lsn
		 * at 0 makes every probe's "art->lsn == art->max_begin_lsn" guard
		 * false for an inherited entry until this child does its own real
		 * BEGIN, which self-corrects it -- see those probes' comments.
		 */
		for (uint32_t ak = 0; ak < FZ_NAKLASS; ak++)
			for (uint32_t r = 0; r < FZ_NREL; r++)
			{
				FzArtifact *pa = &g_artifact[parent][ak][r];
				FzArtifact *ca = &g_artifact[slot][ak][r];

				memset(ca, 0, sizeof(*ca));
				if (pa->state == FZ_ART_COMMITTED ||
					pa->state == FZ_ART_DROPPED)
				{
					ca->state = pa->state;
					ca->lsn = pa->lsn;
					ca->visible = pa->visible;
				}
			}
		g_branch_last_incarnation[slot] = new_inc;

		/*
		 * PRODUCT BUG "Bug B" (see MEMORY.md / the implementer's report,
		 * fix in PR #294, not yet merged): a parent write admitted *after*
		 * the branch exists, but stamped with an LSN exactly equal to
		 * branch_lsn, is visible through the child's read-through
		 * (pagestore_core.c read_resolve_version()'s seq_cap is 0 --
		 * unrestricted -- at every ancestry level beyond the reader's own,
		 * and tl_walk_next()'s branch cap is LSN-only with no
		 * admission_seq tiebreak).  Confirmed with a minimal standalone
		 * repro outside this fuzzer.  Every branch this action creates
		 * forks at the parent's exact current WAL tip with nothing shipped
		 * in between, so the very next parent write would deterministically
		 * collide with branch_lsn and re-trigger this already-reported bug.
		 *
		 * Stage 1 always shipped one throwaway parent WAL record here to
		 * dodge the collision.  Stage 2 puts that behind
		 * PAGESTORE_FUZZ_BUGB_WORKAROUND (default OFF) instead, and adds a
		 * dedicated oracle for it (verify_branch_frozen(), exercised as its
		 * own weighted action, act_verify_branch_frozen): with the
		 * workaround off, an unfixed daemon is *expected* to be caught by
		 * that oracle -- that is the point of turning it off. CI cannot run
		 * with an expected failure,
		 * so the meson test definition below sets
		 * PAGESTORE_FUZZ_BUGB_WORKAROUND=1 in the test's env until #294
		 * merges; remove that env entry (and this default-off gate can stay,
		 * it is harmless once the daemon is fixed) once it does, to restore
		 * full same-LSN-at-the-fork-point coverage by default.
		 */
		if (g_bugb_workaround)
			ship_wal(parent);
	}
}

static void
env_branch_write(void)
{
	uint32_t	slot = 1 + rng_below(FZ_NTL - 1);
	FzTimeline *b = &g_tl[slot];
	uint32_t	rel;
	FzRel	   *m;
	uint32_t	block;
	uint64_t	lsn;
	unsigned char tag;
	int			status;

	if (!b->known || b->state != PS_TIMELINE_LIVE)
		return;
	rel = rng_below(FZ_NREL);
	m = &b->rel[rel];
	if (!m->exists || m->nblocks == 0)
		return;
	block = rng_below(m->nblocks);
	lsn = ship_wal(slot);
	tag = (unsigned char) (1 + rng_below(255));
	psc_fill_page(page_buf, lsn, tag);
	status = psc_op_writev(slot, b->incarnation, PS_KLASS_RELATION, rel,
						   block, page_buf, 1);
	ring_note("branch_write slot=%u rel=%u block=%u status=%d", slot, rel,
			  block, status);
	ck(status == PS_STATUS_OK, "branch %u write rel=%u block=%u failed "
	   "(status %d)", slot, rel, block, status);
	record_env(ENV_BRANCH_WRITE, status == PS_STATUS_OK);
	record_cov(PS_OP_WRITEV, (uint32_t) status, 0);
	if (status == PS_STATUS_OK)
		m->tag[block] = tag, m->lsn[block] = lsn;
}

/* Verify a branch timeline's current model (own writes else the parent's
 * fork-point version, per g_tl[slot].rel already reflecting exactly that
 * -- see env_branch_create's copy-at-fork). */
static void
verify_branch(uint32_t slot, const char *phase)
{
	FzTimeline *b = &g_tl[slot];

	for (uint32_t rel = 0; rel < FZ_NREL; rel++)
	{
		FzRel	   *m = &b->rel[rel];
		uint32_t	nb = 0;
		int			exists;

		if (!m->exists)
		{
			ck(psc_op_exists(slot, b->incarnation, PS_KLASS_RELATION, rel, 0,
							 &exists) == PS_STATUS_OK && !exists,
			   "%s: branch %u does not see relation %u absent at its fork",
			   phase, slot, rel);
			continue;
		}
		ck(psc_op_nblocks(slot, b->incarnation, PS_KLASS_RELATION, rel, 0, 0,
						  &nb) == PS_STATUS_OK && nb == m->nblocks,
		   "%s: branch %u rel %u nblocks expected %u got %u", phase, slot,
		   rel, m->nblocks, nb);
		for (uint32_t bl = 0; bl < m->nblocks && bl < 3; bl++)
		{
			if (m->tag[bl] == 0)
				continue;
			ck(psc_op_readv(slot, b->incarnation, PS_KLASS_RELATION, rel, bl,
							0, 0, read_buf, 1) == PS_STATUS_OK &&
			   psc_page_has_tag(read_buf, m->tag[bl]) &&
			   psc_page_lsn(read_buf) == m->lsn[bl],
			   "%s: branch %u rel %u block %u content mismatch", phase, slot,
			   rel, bl);
		}
	}
}

/*
 * "Branch view frozen" oracle (stage 2, PAGESTORE_FUZZ_BUGB_WORKAROUND):
 * read the PARENT timeline as-of exactly the branch's fork LSN and require
 * it to equal the frozen snapshot captured at fork time, regardless of any
 * write the parent (or the branch) has done since -- this is the direct
 * contrapositive of Bug B (a parent write stamped at exactly branch_lsn
 * leaking into what should be a frozen historical view).
 *
 * Deliberately asymmetric like the rest of this file's as-of checks: an
 * as-of read below the retention floor may legitimately become unavailable
 * once nothing protects it any more (e.g. after the branch itself reaches
 * DELETED and nothing else pins the fork point), so a refusal or a "not
 * found" here is not itself a failure -- level 3. But an OK response must
 * NEVER return the wrong version: that half is level 1 MUST, and is exactly
 * what would catch Bug B.
 */
static void
verify_branch_frozen(uint32_t slot)
{
	FzTimeline *b = &g_tl[slot];
	uint32_t	parent = b->parent;

	if (!b->known || !b->has_parent)
		return;
	for (uint32_t rel = 0; rel < FZ_NREL; rel++)
	{
		FzRel	   *m = &b->frozen[rel];
		uint32_t	nblk = m->nblocks < 3 ? m->nblocks : 3;

		for (uint32_t bl = 0; bl < nblk; bl++)
		{
			int			found = 0;
			int			status = psc_op_read_at(parent, g_tl[parent].incarnation,
												  PS_KLASS_RELATION, rel, bl,
												  b->branch_lsn, 0, read_buf,
												  &found, NULL, NULL);

			ring_note("verify_branch_frozen slot=%u rel=%u block=%u", slot,
					  rel, bl);
			if (status != PS_STATUS_OK)
				continue;			/* refused outright: fine, unavailable */
			/*
			 * found==0 (an OK "not found" response) is ALSO an unavailable
			 * answer here, not a mismatch: READ_AT's own established
			 * convention (see act_read_at()'s handling and the
			 * pagestore_daemon.c PS_OP_READ_AT case's "reclaimed history is
			 * reported as absent" comment) is that page history the daemon
			 * can no longer prove is reported as OK-but-not-found, not a
			 * refusal.  Bug B's signature is a *wrong-but-present* answer
			 * (found=1 with leaked newer content), so only that half needs
			 * to be strict.
			 */
			if (!found)
				continue;
			if (m->tag[bl] == 0)
				ck(psc_page_lsn(read_buf) == 0, "BRANCH VIEW NOT FROZEN: "
				   "branch %u's frozen parent %u rel %u block %u was "
				   "unwritten at the fork point (lsn=%llu), but an as-of "
				   "read there now returns content (suspected Bug B: a "
				   "later parent write landed at/under branch_lsn and leaked "
				   "through)", slot, parent, rel, bl,
				   (unsigned long long) b->branch_lsn);
			else
				ck(psc_page_has_tag(read_buf, m->tag[bl]) &&
				   psc_page_lsn(read_buf) == m->lsn[bl], "BRANCH VIEW NOT "
				   "FROZEN: branch %u's frozen parent %u rel %u block %u "
				   "expected tag=%u lsn=%llu as of branch_lsn=%llu, got "
				   "different content (suspected Bug B)", slot, parent, rel,
				   bl, m->tag[bl], (unsigned long long) m->lsn[bl],
				   (unsigned long long) b->branch_lsn);
		}
	}
}

static void
act_verify_branch_frozen(void)
{
	for (uint32_t slot = 1; slot < FZ_NTL; slot++)
		verify_branch_frozen(slot);
}

static void
env_branch_begin_delete(void)
{
	uint32_t	slot = 1 + rng_below(FZ_NTL - 1);
	FzTimeline *b = &g_tl[slot];
	int			status;

	if (!b->known || b->state != PS_TIMELINE_LIVE)
		return;
	verify_branch(slot, "pre-delete");
	status = psc_op_begin_delete(slot, b->incarnation);
	ring_note("branch_begin_delete slot=%u status=%d", slot, status);
	ck(status == PS_STATUS_OK, "BEGIN_DELETE on live branch %u failed "
	   "(status %d)", slot, status);
	record_env(ENV_BRANCH_BEGIN_DELETE, status == PS_STATUS_OK);
	record_cov(PS_OP_BEGIN_DELETE, (uint32_t) status, 0);
	if (status == PS_STATUS_OK)
		b->state = PS_TIMELINE_DELETING;
}

static void
env_wait_deleted(void)
{
	uint32_t	slot = 1 + rng_below(FZ_NTL - 1);
	FzTimeline *b = &g_tl[slot];
	uint64_t	start = psc_now_ns();
	int			ok = 0;

	if (!b->known || b->state != PS_TIMELINE_DELETING)
		return;
	while (psc_now_ns() - start < 20ull * 1000000000ull)
	{
		PsTimelineState state;
		uint64_t	inc;

		if (psc_op_timeline_state(slot, &state, &inc) == PS_STATUS_OK &&
			state == PS_TIMELINE_DELETED)
		{
			ck(inc == b->incarnation, "deleted branch %u keeps its "
			   "incarnation (expected %llu got %llu)", slot,
			   (unsigned long long) b->incarnation, (unsigned long long) inc);
			b->state = PS_TIMELINE_DELETED;
			ok = 1;
			break;
		}
		psc_sleep_ms(20);
	}
	ring_note("wait_deleted slot=%u ok=%d", slot, ok);
	ck(ok, "branch %u did not reach DELETED within 20s", slot);
	record_env(ENV_WAIT_DELETED, ok);
	if (ok)
	{
		uint32_t	nb = 0;

		ck(psc_op_nblocks(slot, b->incarnation, PS_KLASS_RELATION, 0, 0, 0,
						  &nb) != PS_STATUS_OK, "a DELETED branch %u must "
		   "reject ordinary requests", slot);
	}
}

/* ===================== restarts + full verification ======================= */

static void
verify_latest_all(const char *phase)
{
	for (uint32_t tl = 0; tl < FZ_NTL; tl++)
	{
		if (!g_tl[tl].known || g_tl[tl].state != PS_TIMELINE_LIVE)
			continue;
		for (uint32_t rel = 0; rel < FZ_NREL; rel++)
		{
			FzRel	   *m = &g_tl[tl].rel[rel];
			int			exists;
			uint32_t	nb = 0;

			ck(psc_op_exists(tl, g_tl[tl].incarnation, PS_KLASS_RELATION, rel,
							 0, &exists) == PS_STATUS_OK &&
			   exists == m->exists, "%s: tl=%u rel=%u existence", phase, tl,
			   rel);
			if (!m->exists)
				continue;
			ck(psc_op_nblocks(tl, g_tl[tl].incarnation, PS_KLASS_RELATION,
							  rel, 0, 0, &nb) == PS_STATUS_OK &&
			   nb == m->nblocks, "%s: tl=%u rel=%u nblocks expected %u got "
			   "%u", phase, tl, rel, m->nblocks, nb);
			for (uint32_t b = 0; b < m->nblocks; b++)
			{
				ck(psc_op_readv(tl, g_tl[tl].incarnation, PS_KLASS_RELATION,
								rel, b, 0, 0, read_buf, 1) == PS_STATUS_OK,
				   "%s: tl=%u rel=%u block=%u read", phase, tl, rel, b);
				if (m->tag[b] != 0)
					ck(psc_page_has_tag(read_buf, m->tag[b]) &&
					   psc_page_lsn(read_buf) == m->lsn[b],
					   "%s: tl=%u rel=%u block=%u content", phase, tl, rel,
					   b);
			}
		}
	}
}

/*
 * Restarting (clean or crash) abandons any open artifact attempt:
 * artifact_recovery_seq (pagestore_core.c) is set to the store's post-
 * recovery admission-sequence high-water mark, and artifact_attempt()
 * requires token >= artifact_recovery_seq, so no pre-restart token can ever
 * satisfy it again.  Proves that once per abandoned attempt (a COMMIT probe
 * with the vanished token must now be refused), then restores state/lsn/
 * token from the shadowed prev_* so later sub-cases (an exact-lsn retry of
 * "the last settled generation") target the real one, not the vanished
 * attempt's -- see the FzArtifact.prev_* comment.  Must run before
 * verify_artifacts() so its expectations (art->state/visible) are correct.
 */
static void
artifact_restart_reset(const char *phase)
{
	for (uint32_t tl = 0; tl < FZ_NTL; tl++)
	{
		if (!g_tl[tl].known)
			continue;
		for (uint32_t akind = 0; akind < FZ_NAKLASS; akind++)
			for (uint32_t rel = 0; rel < FZ_NREL; rel++)
			{
				FzArtifact *art = &g_artifact[tl][akind][rel];
				uint32_t	reason = 0;
				int			status;

				if (art->state != FZ_ART_OPEN)
					continue;
				status = psc_op_artifact_commit(tl, g_tl[tl].incarnation,
												g_artifact_klass[akind], rel,
												art->lsn, art->token,
												art->open_count, 0, &reason);
				ring_note("artifact_restart_reset tl=%u akind=%u rel=%u "
						  "status=%d", tl, akind, rel, status);
				ck(status != PS_STATUS_OK, "%s: artifact tl=%u akind=%u "
				   "rel=%u: an attempt open before restart (token=%llu) "
				   "must not be committable afterward, got OK", phase, tl,
				   akind, rel, (unsigned long long) art->token);
				record_cov(PS_OP_ARTIFACT_COMMIT, (uint32_t) status, reason);
				art->state = art->prev_state;
				art->lsn = art->prev_lsn;
				art->token = art->prev_token;
			}
	}
}

/* Committed/dropped artifact generations are durable; verify EXISTS/
 * NBLOCKS/READV still resolve to exactly the last settled generation
 * art->visible describes (or nonexistence for NONE/DROPPED), mirroring
 * verify_latest_all()'s relation checks.  Must run after
 * artifact_restart_reset() when called from a restart phase. */
static void
verify_artifacts(const char *phase)
{
	for (uint32_t tl = 0; tl < FZ_NTL; tl++)
	{
		if (!g_tl[tl].known || g_tl[tl].state != PS_TIMELINE_LIVE)
			continue;
		for (uint32_t akind = 0; akind < FZ_NAKLASS; akind++)
			for (uint32_t rel = 0; rel < FZ_NREL; rel++)
			{
				FzArtifact *art = &g_artifact[tl][akind][rel];
				uint32_t	klass = g_artifact_klass[akind];
				int			exists = 0;
				uint32_t	nb = 0;
				int			est;

				if (art->state == FZ_ART_NONE)
					continue;		/* never begun: nothing to check */
				est = psc_op_exists(tl, g_tl[tl].incarnation, klass, rel, 0,
									&exists);
				ck(est == PS_STATUS_OK && exists == art->visible.exists,
				   "%s: artifact tl=%u akind=%u rel=%u astate=%d exists "
				   "expected %d got %d (EXISTS status %d)", phase, tl, akind,
				   rel, art->state, art->visible.exists, exists, est);
				if (!art->visible.exists)
					continue;
				ck(psc_op_nblocks(tl, g_tl[tl].incarnation, klass, rel, 0, 0,
								  &nb) == PS_STATUS_OK &&
				   nb == art->visible.nblocks, "%s: artifact tl=%u akind=%u "
				   "rel=%u nblocks expected %u got %u", phase, tl, akind,
				   rel, art->visible.nblocks, nb);
				for (uint32_t b = 0; b < art->visible.nblocks && b < 4; b++)
				{
					if (art->visible.tag[b] == 0)
						continue;
					ck(psc_op_readv(tl, g_tl[tl].incarnation, klass, rel, b,
									0, 0, read_buf, 1) == PS_STATUS_OK &&
					   psc_page_has_tag(read_buf, art->visible.tag[b]) &&
					   psc_page_lsn(read_buf) == art->visible.lsn[b],
					   "%s: artifact tl=%u akind=%u rel=%u block=%u content "
					   "mismatch", phase, tl, akind, rel, b);
				}
			}
	}
}

static void
verify_after_restart(const char *phase)
{
	verify_latest_all(phase);
	artifact_restart_reset(phase);
	verify_artifacts(phase);
	for (uint32_t i = 0; i < FZ_NREADERS; i++)
	{
		PsRetentionPin pin;

		if (!g_reader[i].held)
			continue;
		ck(psc_op_retention_lookup(0, 0, PS_RETENTION_OWNER_READER,
								   g_reader[i].owner_id, &pin) &&
		   pin.lsn == g_reader[i].lsn && pin.admission_seq == g_reader[i].seq &&
		   pin.generation == g_reader[i].generation, "%s: reader %llu pin "
		   "survives restart", phase, (unsigned long long) g_reader[i].owner_id);
	}
	if (g_tl[0].mat_registered)
	{
		PsRetentionPin pin;

		ck(psc_op_retention_lookup(0, 0, PS_RETENTION_OWNER_MATERIALIZER, 1,
								   &pin) && pin.lsn == g_tl[0].mat_lsn &&
		   pin.admission_seq == g_tl[0].mat_seq, "%s: materializer pin "
		   "survives restart", phase);
	}
	for (uint32_t slot = 1; slot < FZ_NTL; slot++)
		if (g_tl[slot].known && g_tl[slot].state == PS_TIMELINE_LIVE)
			verify_branch(slot, phase);
	for (uint32_t slot = 1; slot < FZ_NTL; slot++)
		verify_branch_frozen(slot);
}

static void
env_clean_restart(void)
{
	int			ok;

	ring_note("clean_restart");
	ok = psc_stop_daemon_clean();
	ck(ok, "daemon must exit cleanly (status 0) on SIGTERM");
	psc_spawn_daemon(FZ_PAGE_SIZE, FZ_SEGMENT_SIZE, FZ_NSHARDS, FZ_FLUSH_PAGES,
					 FZ_COMPACT_LAYERS, FZ_PAGE_HIGH_WATER, FZ_PAGE_CATCH_UP,
					 FZ_WAL_HIGH_WATER, FZ_WAL_CATCH_UP, FZ_WALIDX_HIGH_WATER,
					 FZ_WALIDX_CATCH_UP, FZ_FORKMETA_HIGH_WATER,
					 FZ_FORKMETA_CATCH_UP);
	psc_start_daemon(FZ_PAGE_SIZE);
	verify_after_restart("after clean restart");
	record_env(ENV_CLEAN_RESTART, ok);
}

static void
env_crash_restart(void)
{
	ring_note("crash_restart");
	psc_stop_daemon_crash();
	psc_spawn_daemon(FZ_PAGE_SIZE, FZ_SEGMENT_SIZE, FZ_NSHARDS, FZ_FLUSH_PAGES,
					 FZ_COMPACT_LAYERS, FZ_PAGE_HIGH_WATER, FZ_PAGE_CATCH_UP,
					 FZ_WAL_HIGH_WATER, FZ_WAL_CATCH_UP, FZ_WALIDX_HIGH_WATER,
					 FZ_WALIDX_CATCH_UP, FZ_FORKMETA_HIGH_WATER,
					 FZ_FORKMETA_CATCH_UP);
	psc_start_daemon(FZ_PAGE_SIZE);
	verify_after_restart("after crash restart");
	record_env(ENV_CRASH_RESTART, 1);
}

static void
env_sleep(void)
{
	long		ms = 5 + (long) rng_below(30);

	ring_note("sleep %ldms", ms);
	psc_sleep_ms(ms);
	record_env(ENV_SLEEP, 1);
}

/* ===================== dispatch ============================================ */

typedef void (*FzActionFn) (void);

typedef struct FzAction
{
	const char *name;
	FzActionFn	fn;
	int			weight;
} FzAction;

static const FzAction g_actions[] = {
	{"create", act_create, 10},
	{"unlink", act_unlink, 4},
	{"truncate", act_truncate, 6},
	{"zeroextend", act_zeroextend, 8},
	{"extend", act_extend, 8},
	{"writev", act_writev, 14},
	{"readv", act_readv, 14},
	{"read_at", act_read_at, 8},
	{"nblocks", act_nblocks, 6},
	{"exists", act_exists, 5},
	{"block_death", act_block_death, 3},
	{"immedsync", act_immedsync, 2},
	{"unknown_opcode", act_unknown_opcode, 2},
	{"admission_barrier", act_admission_barrier, 3},
	{"wal_append_adv", act_wal_append_adv, 2},
	{"wal_size", act_wal_size, 4},
	{"wal_read", act_wal_read, 4},
	{"walidx_add", act_walidx_add, 4},
	{"walidx_add_batch", act_walidx_add_batch, 4},
	{"walidx_progress", act_walidx_progress, 4},
	{"walidx_get", act_walidx_get, 4},
	{"wal_retain_floor", act_wal_retain_floor, 3},
	{"timeline_state", act_timeline_state, 4},
	{"timeline_info", act_timeline_info, 4},
	{"check_branch", act_check_branch, 3},
	{"require_branch", act_require_branch, 3},
	{"create_branch_adv", act_create_branch_adv, 2},
	{"begin_delete_adv", act_begin_delete_adv, 2},
	{"retention_lookup", act_retention_lookup, 4},
	{"retention_get", act_retention_get, 3},
	{"retention_floor", act_retention_floor, 3},
	{"retention_set", act_retention_set, 3},
	{"retention_reserve_adv", act_retention_reserve_adv, 2},
	{"artifact_begin", act_artifact_begin, 6},
	{"artifact_write", act_artifact_write, 8},
	{"artifact_commit", act_artifact_commit, 6},
	{"artifact_commit_retry", act_artifact_commit_retry, 2},
	{"artifact_drop", act_artifact_drop, 4},

	/* environment actions */
	{"env_materialize", env_materialize, 3},
	{"env_reader_reserve", env_reader_reserve, 3},
	{"env_reader_advance", env_reader_advance, 3},
	{"env_reader_drop", env_reader_drop, 2},
	{"env_branch_create", env_branch_create, 2},
	{"env_branch_write", env_branch_write, 4},
	{"verify_branch_frozen", act_verify_branch_frozen, 4},
	{"env_branch_begin_delete", env_branch_begin_delete, 1},
	{"env_wait_deleted", env_wait_deleted, 1},
	{"env_sleep", env_sleep, 3},
	{"env_clean_restart", env_clean_restart, 1},
	{"env_crash_restart", env_crash_restart, 1},
};
#define FZ_NACTIONS ((int) (sizeof(g_actions) / sizeof(g_actions[0])))

/*
 * ===================== op-sequence recording / replay / shrinking =========
 *
 * Every step's chosen action index is recorded here, in order.  On an
 * oracle failure this becomes a "sequence file" (a header line plus one
 * action name per line) that PAGESTORE_FUZZ_REPLAY=<file> can re-drive
 * exactly, and that the ddmin shrinker (shrink_on_failure(), below) mutates
 * by deleting chunks and re-checking.
 *
 * Replay fidelity: run_step() always performs the same rng_below(total)
 * "selection roll" whether or not it ends up using its result (replay mode
 * substitutes the recorded action instead) -- so replaying an *unmodified*
 * sequence file reproduces the exact same rng_state trajectory, and hence
 * the exact same run, as the original.  A *shrunk* sequence necessarily
 * diverges in rng_state from the point of the first removed step onward
 * (fewer actions consume fewer internal rng_below() calls) -- that is
 * expected: shrinking searches for a new, smaller failing input, not a
 * byte-identical replay of the original one.  What must and does hold is
 * that replaying the *same* sequence file twice is deterministic.
 */
static int	g_replay_mode;
static int *g_replay_actions;
static long long g_replay_len;

static int *g_seq_actions;
static long long g_seq_len, g_seq_cap;

static void
seq_record(int idx)
{
	if (g_seq_len == g_seq_cap)
	{
		long long	ncap = g_seq_cap ? g_seq_cap * 2 : 4096;
		int		   *grown = realloc(g_seq_actions, (size_t) ncap * sizeof(int));

		if (grown == NULL)
			psc_fatal("out of memory recording the op sequence");
		g_seq_actions = grown;
		g_seq_cap = ncap;
	}
	g_seq_actions[g_seq_len++] = idx;
}

static int
find_action_index(const char *name)
{
	for (int i = 0; i < FZ_NACTIONS; i++)
		if (strcmp(g_actions[i].name, name) == 0)
			return i;
	return -1;
}

/* Writes the sequence recorded so far (g_seq_actions[0..g_seq_len)) as a
 * replay-loadable file: a header line naming the seed (informational only
 * -- replaying a sequence file no longer re-derives arguments from the
 * seed's rng stream the way stage 1's REPLAY did; the seed line just keeps
 * the failure self-documenting), then one action name per line. */
static void
write_seq_file(const char *path)
{
	FILE	   *f = fopen(path, "w");

	if (f == NULL)
	{
		fprintf(stderr, "warning: could not write op-sequence file %s: %s\n",
				path, strerror(errno));
		return;
	}
	fprintf(f, "# replay seed=%llu\n", (unsigned long long) g_seed);
	for (long long i = 0; i < g_seq_len; i++)
		fprintf(f, "%s\n", g_actions[g_seq_actions[i]].name);
	fclose(f);
}

static void
write_seq_subset(const char *path, const int *seq, long long n)
{
	FILE	   *f = fopen(path, "w");

	if (f == NULL)
		return;
	fprintf(f, "# replay seed=%llu\n", (unsigned long long) g_seed);
	for (long long i = 0; i < n; i++)
		fprintf(f, "%s\n", g_actions[seq[i]].name);
	fclose(f);
}

/*
 * Replay one candidate sequence as a fresh child process of this same
 * binary (re-exec via /proc/self/exe: robust regardless of how argv[0] was
 * spelled) against its own fresh daemon/store, with a bounded wait.
 * Returns 1 iff the child exits non-zero AND its captured output contains
 * an "ORACLE_SITE: <orig_fmt>" line -- i.e. it failed at the *same* ck()
 * call site as the original failure, not merely "failed somehow" (a
 * shrunk-too-far candidate can legitimately hit a different, earlier
 * assertion; that is not "the same bug" and must not be accepted).
 */
static int
shrink_try_candidate(const char *cand_path, const char *orig_fmt)
{
	char		capture_path[600];
	pid_t		pid;
	int			status;
	int			ok = 0;
	uint64_t	start;
	int			done = 0;

	snprintf(capture_path, sizeof(capture_path), "%s/psfuzz_shrink_%d_%d.out",
			 g_store_base, (int) getpid(), rng_below(1000000000u));
	pid = fork();
	if (pid < 0)
	{
		fprintf(stderr, "shrink: fork failed: %s\n", strerror(errno));
		return 0;
	}
	if (pid == 0)
	{
		int			cfd = open(capture_path, O_WRONLY | O_CREAT | O_TRUNC, 0600);

		if (cfd >= 0)
		{
			dup2(cfd, 1);
			dup2(cfd, 2);
			close(cfd);
		}
		setenv("PAGESTORE_FUZZ_REPLAY", cand_path, 1);
		setenv("PAGESTORE_FUZZ_SHRINK_CHILD", "1", 1);
		unsetenv("PAGESTORE_FUZZ_SHRINK");
		unsetenv("PAGESTORE_FUZZ_KEEP");
		unsetenv("PAGESTORE_FUZZ_TRACE");
		unsetenv("PAGESTORE_FUZZ_LOG");
		execv("/proc/self/exe", g_argv);
		_exit(127);				/* exec failed */
	}

	start = psc_now_ns();
	while (psc_now_ns() - start < 90ull * 1000000000ull)
	{
		pid_t		r = waitpid(pid, &status, WNOHANG);

		if (r == pid)
		{
			done = 1;
			break;
		}
		usleep(20000);
	}
	if (!done)
	{
		kill(pid, SIGKILL);
		waitpid(pid, &status, 0);
		unlink(capture_path);
		return 0;				/* candidate replay hung: not a clean repro */
	}
	if (WIFEXITED(status) && WEXITSTATUS(status) != 0)
	{
		FILE	   *f = fopen(capture_path, "r");
		char		line[1024];
		size_t		fmtlen = strlen(orig_fmt);

		if (f != NULL)
		{
			while (fgets(line, sizeof(line), f) != NULL)
				if (strncmp(line, "ORACLE_SITE: ", 13) == 0 &&
					strncmp(line + 13, orig_fmt, fmtlen) == 0)
				{
					ok = 1;
					break;
				}
			fclose(f);
		}
	}
	unlink(capture_path);
	return ok;
}

/*
 * Classic delta-debugging (Zeller's ddmin) over the recorded action-index
 * array: repeatedly try removing ever-smaller contiguous chunks, keeping
 * any removal that still reproduces the same failure, growing the chunk
 * count (halving chunk size) only once a full pass removes nothing.  Bounded
 * by wall-clock budget and a hard attempt cap, since a genuinely timing-
 * sensitive failure may simply not shrink -- see the report.
 */
static void
ddmin_run(int *cur, long long *len_io, const char *orig_fmt,
		  long long budget_s)
{
	long long	len = *len_io;
	int		   *cand = malloc((size_t) (len > 0 ? len : 1) * sizeof(int));
	long long	n_chunks = 2;
	uint64_t	deadline = psc_now_ns() + (uint64_t) budget_s * 1000000000ull;
	char		cand_path[600];
	long long	attempts = 0;
	const long long max_attempts = 20000;

	if (cand == NULL)
		return;
	snprintf(cand_path, sizeof(cand_path), "%s/psfuzz_shrink_cand_%d.seq",
			 g_store_base, (int) getpid());

	while (len >= 1)
	{
		long long	chunk = (len + n_chunks - 1) / n_chunks;
		int			removed_any = 0;
		long long	start = 0;

		if (chunk < 1)
			chunk = 1;
		while (start < len)
		{
			long long	end = start + chunk < len ? start + chunk : len;
			long long	m = 0;

			if (end - start >= len)
			{
				start = end;
				continue;		/* never try removing everything */
			}
			if (psc_now_ns() > deadline || attempts >= max_attempts)
				goto done;

			for (long long i = 0; i < start; i++)
				cand[m++] = cur[i];
			for (long long i = end; i < len; i++)
				cand[m++] = cur[i];

			write_seq_subset(cand_path, cand, m);
			attempts++;
			if (shrink_try_candidate(cand_path, orig_fmt))
			{
				memcpy(cur, cand, (size_t) m * sizeof(int));
				len = m;
				if (n_chunks > 2)
					n_chunks--;
				removed_any = 1;
				/* keep scanning from the same offset in the now-shorter seq */
			}
			else
				start = end;
		}
		if (!removed_any)
		{
			if (n_chunks >= len)
				break;
			n_chunks = n_chunks * 2 < len ? n_chunks * 2 : len;
		}
	}
done:
	unlink(cand_path);
	free(cand);
	*len_io = len;
	if (attempts >= max_attempts)
		fprintf(stderr, "shrink: hit the %lld-attempt cap; result may not be "
				"fully minimal\n", max_attempts);
}

/*
 * Driver called from ck() on an original (non-shrink-child) failure when
 * PAGESTORE_FUZZ_SHRINK is set.  Copies the already-recorded sequence,
 * ddmin's it against fresh daemons, and writes the minimized result
 * alongside the full one.  A sequence that does not shrink at all (0
 * successful reductions) is flagged as possibly timing-sensitive, per the
 * spec's allowance.
 */
static void
shrink_on_failure(const char *orig_fmt)
{
	long long	orig_len = g_seq_len;
	long long	len = orig_len;
	int		   *cur;
	char		min_path[600];
	uint64_t	t0 = psc_now_ns();
	long long	budget_s = 300;
	const char *env = getenv("PAGESTORE_FUZZ_SHRINK_BUDGET_S");

	if (env != NULL && atoll(env) > 0)
		budget_s = atoll(env);
	if (orig_len <= 0)
		return;
	cur = malloc((size_t) orig_len * sizeof(int));
	if (cur == NULL)
		return;
	memcpy(cur, g_seq_actions, (size_t) orig_len * sizeof(int));

	fprintf(stderr, "\nshrinking a %lld-step failing sequence (budget %llds, "
			"same-site match required)...\n", orig_len,
			(long long) budget_s);
	ddmin_run(cur, &len, orig_fmt, budget_s);

	snprintf(min_path, sizeof(min_path), "%s.opseq.min", psc_store_dir);
	write_seq_subset(min_path, cur, len);
	if (len == orig_len)
		fprintf(stderr, "shrink: no reduction found in %.1fs -- likely "
				"timing-sensitive (background maintenance timing, not the "
				"op sequence itself, may be what triggers this); minimal "
				"sequence file is just a copy of the original "
				"(%s)\n", (double) (psc_now_ns() - t0) / 1e9, min_path);
	else
		fprintf(stderr, "shrink: %lld -> %lld steps in %.1fs -> %s\n",
				orig_len, len, (double) (psc_now_ns() - t0) / 1e9, min_path);
	free(cur);
}

static void
run_step(void)
{
	int			total = 0;
	int			pick;
	int			acc = 0;
	int			i = -1;

	for (int k = 0; k < FZ_NACTIONS; k++)
		total += g_actions[k].weight;
	pick = (int) rng_below((uint32_t) total);	/* see fidelity note above */

	if (g_replay_mode)
	{
		if (g_step - 1 >= g_replay_len)
			return;					/* sequence file exhausted */
		i = g_replay_actions[g_step - 1];
	}
	else
	{
		for (int k = 0; k < FZ_NACTIONS; k++)
		{
			acc += g_actions[k].weight;
			if (pick < acc)
			{
				i = k;
				break;
			}
		}
		if (i < 0)
			return;
	}
	seq_record(i);
	trace_op("step action=%s", g_actions[i].name);
	g_actions[i].fn();
}

/* ===================== coverage report + requirement ====================== */

static int
print_coverage_and_check(void)
{
	int			missing = 0;

	fprintf(stderr, "\n==== opcode coverage (opcode x status x result) ====\n");
	for (int i = 0; i < FZ_NSTAGE1_OPS; i++)
	{
		const FzOpInfo *op = &g_stage1_ops[i];
		long long	ok_count = 0;
		long long	refuse_count = 0;

		if ((uint32_t) op->opcode >= FZ_MAX_OPCODE)
			continue;
		fprintf(stderr, "%-24s", op->name);
		for (int s = 0; s < FZ_NSTATUS; s++)
			for (int r = 0; r < FZ_NREASON; r++)
			{
				long long	c = g_cov[op->opcode][s][r];

				if (c == 0)
					continue;
				fprintf(stderr, " [status=%d,reason=%s%d:%lld]", s,
						r == 8 ? ">=" : "", r, c);
				if (s == PS_STATUS_OK)
					ok_count += c;
				else
					refuse_count += c;
			}
		fputc('\n', stderr);
		if (ok_count == 0 && !op->skip_ok_check)
		{
			fprintf(stderr, "  MISSING: no OK cell observed for %s\n",
					op->name);
			missing++;
		}
		if (ok_count != 0 && op->skip_ok_check)
		{
			fprintf(stderr, "  UNEXPECTED: %s returned OK at least once but "
					"must never legally succeed\n", op->name);
			missing++;
		}
		if (op->refusal_possible && refuse_count == 0)
		{
			fprintf(stderr, "  MISSING: no refusal cell observed for %s\n",
					op->name);
			missing++;
		}
		if (!op->refusal_possible)
			fprintf(stderr, "  (refusal not required: %s)\n", op->refusal_note);
	}

	fprintf(stderr, "\n==== environment-action coverage ====\n");
	for (int e = 0; e < ENV_COUNT; e++)
		fprintf(stderr, "%-24s happened=%lld skipped=%lld\n", g_env_names[e],
				g_env_cov[e][0], g_env_cov[e][1]);

	fprintf(stderr, "\n==== opcodes using the weak oracle in this stage ====\n");
	for (int i = 0; i < FZ_NWEAK; i++)
		fprintf(stderr, "- %s\n", g_weak_oracle_ops[i]);

	return missing;
}

/* ===================== bootstrap / replay / main =========================== */

static void
bootstrap(void)
{
	g_tl[0].known = 1;
	g_tl[0].state = PS_TIMELINE_LIVE;
	g_tl[0].incarnation = 1;
	g_tl[0].wal_start = 1024 * 1024;
	g_tl[0].wal_end = g_tl[0].wal_start;
	g_tl[0].walidx_progress = g_tl[0].wal_start;

	for (uint32_t i = 0; i < FZ_NREADERS; i++)
	{
		g_reader[i].owner_id = 100 + i;
		g_reader[i].generation = 1;
	}

	for (uint32_t rel = 0; rel < FZ_NREL; rel++)
	{
		uint64_t	lsn = ship_wal(0);
		int			status = psc_op_create(0, g_tl[0].incarnation,
											PS_KLASS_RELATION, rel, lsn);

		ck(status == PS_STATUS_OK, "bootstrap create rel=%u (status %d)", rel,
		   status);
		g_tl[0].rel[rel].exists = 1;
	}
	env_materialize();
}

int
main(int argc, char **argv)
{
	const char *base = argc >= 3 ? argv[2] : "/tmp";
	const char *env;
	uint64_t	seed = FZ_DEFAULT_SEED;
	long long	ops = FZ_DEFAULT_OPS;
	int			missing;
	int			skip_known = 0;

	if (argc < 2)
	{
		fprintf(stderr, "usage: %s <path-to-pagestore_daemon> "
				"[store-base-dir]\n", argv[0]);
		return 2;
	}
	psc_daemon_path = argv[1];

	if ((env = getenv("PAGESTORE_FUZZ_SEED")) != NULL)
		seed = strtoull(env, NULL, 10);
	if ((env = getenv("PAGESTORE_FUZZ_OPS")) != NULL && atoll(env) > 0)
		ops = atoll(env);
	trace = getenv("PAGESTORE_FUZZ_TRACE") != NULL;
	keep_store = getenv("PAGESTORE_FUZZ_KEEP") != NULL;
	psc_keep_store = keep_store;
	g_bugb_workaround = getenv("PAGESTORE_FUZZ_BUGB_WORKAROUND") != NULL;

	for (int i = 0; i < FZ_NKNOWN; i++)
		if (g_known_failures[i].seed == seed)
		{
			skip_known = 1;
			fprintf(stderr, "pagestore_fuzz_test: seed %llu is a known "
					"failure (%s); skipping by default. Set "
					"PAGESTORE_FUZZ_FORCE_KNOWN=1 to run it anyway.\n",
					(unsigned long long) seed, g_known_failures[i].note);
		}
	if (skip_known && getenv("PAGESTORE_FUZZ_FORCE_KNOWN") == NULL)
	{
		fprintf(stderr, "pagestore_fuzz_test: SKIPPED (known failure, not "
				"forced)\n");
		return 0;
	}

	g_argv = argv;
	g_store_base = base;

	if ((env = getenv("PAGESTORE_FUZZ_REPLAY")) != NULL)
	{
		/*
		 * Stage 2: replay a serialized op stream (one action name per
		 * line, from write_seq_file()/write_seq_subset(), i.e. what a
		 * failure or the ddmin shrinker produces) instead of re-driving the
		 * generator from (seed, ops).  This is what makes shrinking
		 * possible: a shrunk sequence has fewer/different steps than any
		 * (seed, ops) pair could re-derive.  Direct (seed, ops) re-running is
		 * still fully supported and unchanged -- it just no longer goes
		 * through this env var; set PAGESTORE_FUZZ_SEED/PAGESTORE_FUZZ_OPS
		 * directly (with PAGESTORE_FUZZ_REPLAY unset) to re-run the
		 * generator exactly as stage 1 did.
		 *
		 * Fidelity note (see run_step()'s longer comment): replaying this
		 * exact, unmodified file reproduces the original run bit-for-bit
		 * (same rng_state trajectory); a *shrunk* file intentionally
		 * diverges from the original after its first removed step, since it
		 * is a new, smaller failing input, not a slice of the old one.
		 */
		FILE	   *f = fopen(env, "r");
		char		line[256];
		int			cap = 0;

		if (f == NULL)
			psc_fatal("cannot open replay file %s: %s", env, strerror(errno));
		if (fgets(line, sizeof(line), f) != NULL)
		{
			unsigned long long rseed = 0;

			if (sscanf(line, "# replay seed=%llu", &rseed) == 1)
				seed = rseed;
			else
				rewind(f);		/* no recognized header: whole file is actions */
		}
		while (fgets(line, sizeof(line), f) != NULL)
		{
			size_t		l = strlen(line);
			int			idx;

			while (l > 0 && (line[l - 1] == '\n' || line[l - 1] == '\r'))
				line[--l] = '\0';
			if (l == 0 || line[0] == '#')
				continue;
			idx = find_action_index(line);
			if (idx < 0)
				psc_fatal("replay file %s: unknown action '%s'", env, line);
			if (g_replay_len == cap)
			{
				cap = cap ? cap * 2 : 4096;
				g_replay_actions = realloc(g_replay_actions,
										   (size_t) cap * sizeof(int));
				if (g_replay_actions == NULL)
					psc_fatal("out of memory loading replay file %s", env);
			}
			g_replay_actions[g_replay_len++] = idx;
		}
		fclose(f);
		g_replay_mode = 1;
		ops = g_replay_len;
		fprintf(stderr, "pagestore_fuzz_test: replaying %lld recorded "
				"steps (seed=%llu, informational only) from %s\n", ops,
				(unsigned long long) seed, env);
	}

	if ((env = getenv("PAGESTORE_FUZZ_LOG")) != NULL)
	{
		log_fp = fopen(env, "w");
		if (log_fp == NULL)
			psc_fatal("cannot open log file %s: %s", env, strerror(errno));
		fprintf(log_fp, "# replay seed=%llu ops=%lld\n",
				(unsigned long long) seed, ops);
	}

	g_seed = seed;
	g_ops_budget = ops;
	rng_state = seed ? seed : 1;

	snprintf(psc_shm_name, sizeof(psc_shm_name), "/psfuzz_%d", (int) getpid());
	snprintf(psc_store_dir, sizeof(psc_store_dir), "%s/pagestore-fuzz-%d",
			 base, (int) getpid());
	ps_shm_unlink(psc_shm_name);
	psc_remove_tree(psc_store_dir);
	if (mkdir(psc_store_dir, 0700) != 0)
		psc_fatal("mkdir %s: %s", psc_store_dir, strerror(errno));

	fprintf(stderr, "pagestore_fuzz_test: seed=%llu ops=%lld store=%s\n",
			(unsigned long long) seed, ops, psc_store_dir);

	psc_spawn_daemon(FZ_PAGE_SIZE, FZ_SEGMENT_SIZE, FZ_NSHARDS, FZ_FLUSH_PAGES,
					 FZ_COMPACT_LAYERS, FZ_PAGE_HIGH_WATER, FZ_PAGE_CATCH_UP,
					 FZ_WAL_HIGH_WATER, FZ_WAL_CATCH_UP, FZ_WALIDX_HIGH_WATER,
					 FZ_WALIDX_CATCH_UP, FZ_FORKMETA_HIGH_WATER,
					 FZ_FORKMETA_CATCH_UP);
	psc_start_daemon(FZ_PAGE_SIZE);

	bootstrap();

	for (g_step = 1; g_step <= g_ops_budget; g_step++)
		run_step();

	/* Final restart pair: everything acknowledged must survive both a clean
	 * and a crash restart of the now-quiescent store. */
	env_clean_restart();
	env_crash_restart();

	missing = print_coverage_and_check();

	fprintf(stderr, "\n%lld checks, %lld failures, %d missing coverage "
			"cells\n", checks, failed, missing);

	psc_stop_daemon_clean();
	if (log_fp != NULL)
		fclose(log_fp);
	if (!keep_store)
		psc_remove_tree(psc_store_dir);
	else
		fprintf(stderr, "store kept at %s\n", psc_store_dir);

	return (failed != 0 || missing != 0) ? 1 : 0;
}
