#ifndef PAGESTORE_FORKMETA_SNAPSHOT_H
#define PAGESTORE_FORKMETA_SNAPSHOT_H

#include <stddef.h>
#include <stdint.h>

#define PS_FORKMETA_SNAPSHOT_PATH_MAX 4096

typedef int (*PsForkmetaSnapshotConsume)(void *arg, const void *data,
										 size_t len);
typedef int (*PsForkmetaSnapshotProduce)(void *arg,
										 PsForkmetaSnapshotConsume consume,
										 void *consume_arg);

typedef struct PsForkmetaSnapshotInput
{
	const void *data;
	uint64_t	len;
	PsForkmetaSnapshotProduce produce;
	void		*produce_arg;
} PsForkmetaSnapshotInput;

typedef struct PsForkmetaSnapshotPart
{
	uint64_t	len;
	uint32_t	crc;
} PsForkmetaSnapshotPart;

typedef struct PsForkmetaSnapshot
{
	char		directory[PS_FORKMETA_SNAPSHOT_PATH_MAX];
	int			directory_fd;
	int			checkpoint_fd;
	int			tail_fd;
	uint64_t	generation;
	uint64_t	cutoff_lsn;
	uint64_t	cutoff_admission_seq;
	PsForkmetaSnapshotPart checkpoint;
	PsForkmetaSnapshotPart tail;
} PsForkmetaSnapshot;

typedef struct PsForkmetaSnapshotPrepared
{
	char		directory[PS_FORKMETA_SNAPSHOT_PATH_MAX];
	uint64_t	generation;
	uint64_t	cutoff_lsn;
	uint64_t	cutoff_admission_seq;
	PsForkmetaSnapshotPart checkpoint;
	PsForkmetaSnapshotPart tail;
} PsForkmetaSnapshotPrepared;

/* The in-memory selected tuple is the expected identity for periodic
 * metadata observation.  A zero generation means that no snapshot is
 * selected; an unexpected manifest is then unsafe rather than free debt. */
typedef struct PsForkmetaSnapshotExpected
{
	uint64_t generation;
	uint64_t cutoff_lsn;
	uint64_t cutoff_admission_seq;
	PsForkmetaSnapshotPart checkpoint;
	PsForkmetaSnapshotPart tail;
} PsForkmetaSnapshotExpected;

/* Separate debt that can be serviced by the current GC from residue that
 * needs a newly published snapshot/cutoff before it is safe to reclaim. */
typedef struct PsForkmetaSnapshotReclaimObservation
{
	uint64_t bytes;
	uint64_t gc_serviceable_bytes;
	uint64_t gc_temp_bytes;
	uint64_t gc_canonical_bytes;
	uint64_t cutoff_dependent_bytes;
	uint64_t source_debt_bytes;
	/* The bounded observer saw more recognized temporary debris than it can
	 * account in one pass.  This is executable GC work, not an unknown error. */
	int gc_serviceable_overflow;
} PsForkmetaSnapshotReclaimObservation;

typedef void (*PsForkmetaSnapshotObservationTestHook)(unsigned int attempt,
															 void *arg);

/* Test-only seam used to change a directory after the first bounded scan and
 * prove that retry opens a fresh directory stream. */
extern void ps_test_set_forkmeta_snapshot_observation_hook(
		PsForkmetaSnapshotObservationTestHook hook, void *arg);

/*
 * MUTATOR SERIALIZATION CONTRACT
 *
 * The owning pagestore controller exclusively serializes prepare, commit,
 * abort, discard, recover, publish, next-generation allocation, and GC.  This
 * module intentionally has no internal lock.  GC runs only while publication
 * is serialized and after all readers of older snapshots have drained.
 * The controller may abort or discard an unselected durable intent only after
 * proving that no durable exact frontier covers its cutoff.  Once the frontier
 * covers an intent, the controller must commit it or pass it to recovery; a
 * parts-first abort of covered state is not recoverable.
 *
 * directory is below a trusted store root.  Intermediate path components are
 * not defended against adversarial replacement; the final directory component
 * and every entry created or opened here are protected with O_NOFOLLOW.
 * Concurrent mutators and adversarial intermediate-path races are unsupported.
 *
 * A generation consists of immutable checkpoint and captured-tail files.  The
 * fixed checksummed manifest is their only discovery point.  The cutoff is the
 * exact lexicographically ordered (cutoff_lsn, cutoff_admission_seq) tuple.
 */

/* Durably write immutable parts and a prepare intent without selecting them.
 * On failure, files newly created by this call are removed durably; pre-existing
 * retry files and any unrelated durable prepare intent are preserved. */
extern int ps_forkmeta_snapshot_prepare(
	PsForkmetaSnapshotPrepared *prepared, const char *directory,
	uint64_t generation, uint64_t cutoff_lsn, uint64_t cutoff_admission_seq,
	const PsForkmetaSnapshotInput *checkpoint,
	const PsForkmetaSnapshotInput *tail);
/* Validate the exact durable intent and both immutable parts, atomically select
 * its manifest, then clear the matching intent durably.  Identical selected
 * retries succeed after an ambiguous publication result. */
extern int ps_forkmeta_snapshot_commit(
	const PsForkmetaSnapshotPrepared *prepared);
/* Abort only the exact supplied durable intent and its unselected generation.
 * A mismatched durable intent is rejected; absence is an idempotent cleanup.
 * Controller precondition: no durable exact frontier covers this intent. */
extern int ps_forkmeta_snapshot_abort(
	const PsForkmetaSnapshotPrepared *prepared);
/* Reconcile a durable intent against a nonzero durable exact frontier.  A valid
 * newer intent is retained only when covered.  An uncovered intent is fully
 * aborted even when a prior partial abort already removed one or both parts. */
extern int ps_forkmeta_snapshot_recover_prepared(
	const char *directory, uint64_t durable_lsn,
	uint64_t durable_admission_seq);
/* Convenience prepare+commit.  Prepare failure is returned directly and this
 * wrapper performs no abort.  Commit failure likewise preserves the durable
 * intent and immutable parts for retry/recovery, including rename ambiguity. */
extern int ps_forkmeta_snapshot_publish(
	const char *directory, uint64_t generation, uint64_t cutoff_lsn,
	uint64_t cutoff_admission_seq, const PsForkmetaSnapshotInput *checkpoint,
	const PsForkmetaSnapshotInput *tail);
/* Allocate above the supplied selected generation, durable intent, and every
 * recognized final or temporary generation under controller serialization. */
extern int ps_forkmeta_snapshot_next_generation(
	const char *directory, uint64_t selected_generation,
	uint64_t *generation_out);
/* Return the durable prepared generation, or zero when no intent exists. */
extern int ps_forkmeta_snapshot_prepared_generation(
	const char *directory, uint64_t *generation_out);
/* Read and validate the durable prepare intent and both immutable parts.
 * Returns one when present, zero when absent, and minus one on corruption. */
extern int ps_forkmeta_snapshot_read_prepared(
	const char *directory, PsForkmetaSnapshotPrepared *prepared);
/* Remove one unselected generation.  A matching prepared intent is cleared;
 * an unrelated intent is preserved.  Returns one if the target is selected.
 * Controller precondition for a matching intent: no durable exact frontier
 * covers its cutoff; covered intents must instead be committed or recovered. */
extern int ps_forkmeta_snapshot_discard_generation(
	const char *directory, uint64_t generation);
/* Open and checksum-validate the selected manifest and both immutable parts.
 * Part descriptors stay open until close, giving reads stable file identity
 * even if a drained/serialized controller later changes directory entries. */
extern int ps_forkmeta_snapshot_open(PsForkmetaSnapshot *snapshot,
									 const char *directory);
/* Bounded read from the already validated descriptor for one immutable part. */
extern int ps_forkmeta_snapshot_read(const PsForkmetaSnapshot *snapshot,
									 unsigned int part, uint64_t offset, void *data,
									 uint64_t len);
/* Bounded checkpoint read with the same lifetime as the open snapshot. */
extern int ps_forkmeta_snapshot_read_checkpoint(
	const PsForkmetaSnapshot *snapshot, uint64_t offset, void *data,
	uint64_t len);
/* Bounded captured-tail read with the same lifetime as the open snapshot. */
extern int ps_forkmeta_snapshot_read_tail(
	const PsForkmetaSnapshot *snapshot, uint64_t offset, void *data,
	uint64_t len);
/* After publisher serialization and reader drain, remove recognized temp
 * debris and final files older than selected, preserving selected, newer, and
 * durable-prepared generations.  Every call fsyncs the directory, including
 * an empty retry after an ambiguous prior unlink fsync. */
extern int ps_forkmeta_snapshot_gc(const char *directory);
/* Remove a bounded batch of recognized temporary debris without requiring a
 * selected manifest.  Canonical manifests/parts and durable prepared intent
 * are never removed by this path. */
extern int ps_forkmeta_snapshot_gc_temporary(const char *directory);
#define PS_FORKMETA_SNAPSHOT_GC_DURABILITY_AMBIGUOUS (-2)
/* Metadata-only physical debt scan.  The source baseline growth is charged
 * only when source_debt_enabled is nonzero; when it is zero, source bytes are
 * observed for identity stability but contribute no reclaimable debt.
 * The selected and prepared manifest records, their file identities, and
 * canonical part lengths are checked before and after a bounded scan.  The
 * immutable payload CRCs are compared as metadata only and never reread on
 * this periodic path. */
extern int ps_forkmeta_snapshot_reclaim_bytes(const char *directory,
										 const char *source_directory,
										 uint64_t source_baseline,
										 int source_debt_enabled,
										 const PsForkmetaSnapshotExpected *expected,
										 uint64_t *bytes_out);
extern int ps_forkmeta_snapshot_reclaim_observation(
										 const char *directory,
										 const char *source_directory,
										 uint64_t source_baseline,
										 int source_debt_enabled,
										 const PsForkmetaSnapshotExpected *expected,
										 PsForkmetaSnapshotReclaimObservation *observation);
/* Release the stable part descriptors and directory descriptor. */
extern void ps_forkmeta_snapshot_close(PsForkmetaSnapshot *snapshot);

#define PS_FORKMETA_SNAPSHOT_CHECKPOINT 0u
#define PS_FORKMETA_SNAPSHOT_TAIL 1u

#endif
