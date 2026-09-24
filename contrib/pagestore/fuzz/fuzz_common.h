/*-------------------------------------------------------------------------
 *
 * fuzz_common.h
 *	  Shared driver for the pagestore persisted-format fuzz targets.
 *
 * Layer 2 of the pagestore fuzzing work: byte-level fuzzing of the files a
 * store directory persists on disk (layers.manifest, forkmeta and its
 * snapshots, image layers, seg_* page segments, the WAL/WAL-index catalogs
 * and their snapshots, retention/timeline/frontier records, the shard-count
 * file).  See fuzz/build.sh for how this is built and fuzz/run_fuzz.sh for
 * how it is run; this header is shared between the libFuzzer entry point
 * (fuzz_target.c) and the non-instrumented replay driver used by the meson
 * corpus-regression test (fuzz_standalone_driver.c).
 *
 *-------------------------------------------------------------------------
 */
#ifndef PS_FUZZ_COMMON_H
#define PS_FUZZ_COMMON_H

#include <stddef.h>
#include <stdint.h>

/* One persisted-format fuzz target: a human name and the store-relative path
 * of the single file it replaces wholesale with the fuzz input.  relpath is
 * NULL only for the synthetic "all" pseudo-target, which is not itself in
 * the table -- see ps_fuzz_run_one(). */
typedef struct PsFuzzTarget
{
	const char *name;
	const char *relpath;
} PsFuzzTarget;

extern const PsFuzzTarget ps_fuzz_targets[];
extern const int ps_fuzz_target_count;

/* One-time setup: extract the template fixture store (PS_FUZZ_FIXTURE_TGZ)
 * into a durable scratch directory that every iteration copies from.  Must
 * be called exactly once before ps_fuzz_run_one(); safe to call from
 * LLVMFuzzerInitialize() or a plain main(). */
extern void ps_fuzz_global_init(void);

/* Run one fuzz iteration: copy the template store, replace the target file
 * (selected by name, or by data[0] when target_name is NULL, empty, or
 * "all" -- the "single target picks the file kind from the first byte"
 * mode), open the store, do a bounded set of reads/inspections, close it,
 * and remove the scratch copy.  Never touches product code; a crash here is
 * ps_core_open/close (or something it calls) misbehaving on the mutated
 * file. */
extern void ps_fuzz_run_one(const char *target_name,
							 const uint8_t *data, size_t size);

/* Look up a store-relative path's pristine bytes in the in-memory template
 * cache built by ps_fuzz_global_init() (e.g. to read another file's real
 * captured field values for a cross-file checksum fixup -- see
 * fuzz_crc_fixup.c). Returns NULL if the path is not part of the template. */
extern const uint8_t *ps_fuzz_template_lookup(const char *relpath,
											   size_t *len_out);

#endif							/* PS_FUZZ_COMMON_H */
