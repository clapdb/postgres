/*-------------------------------------------------------------------------
 *
 * fuzz_crc_fixup.h
 *	  Structure-aware checksum recomputation for the persisted-format fuzz
 *	  targets (round 2, item 2).  See fuzz_crc_fixup.c for the per-format
 *	  rules and which targets have no checksum, or one this harness does
 *	  not fix up.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PS_FUZZ_CRC_FIXUP_H
#define PS_FUZZ_CRC_FIXUP_H

#include <stddef.h>
#include <stdint.h>

/*
 * Recompute target_name's on-disk checksum field(s) over the (already
 * mutated) buffer, in place, following that format's real rule -- reusing
 * the product's own hash algorithms (all either PostgreSQL CRC-32C via
 * ps_crc32c_update()/PS_CRC32C_INIT/FIN in pagestore_artifact_format.h, or
 * the project's standard FNV-1a stepping function, replicated here byte-
 * for-byte from pagestore_core.c's fnv()/pagestore_manifest.c's
 * manifest_fnv1a()/etc. -- every persisted format in this codebase uses one
 * of these two, never a bespoke one) and the product's own struct layouts
 * (mirrored locally the way the existing pagestore_*_test.c files already
 * do for formats whose struct is private to one .c file, or included
 * directly where the type is public, e.g. PsKey, PsRetentionPin,
 * PsPruneFence). Only checksum fields are touched; every other byte stays
 * exactly what libFuzzer produced, so the checksum gate stops being the
 * first thing every mutation trips over without weakening any other
 * structural check.
 *
 * work_dir is the live store directory for this iteration: two targets
 * (forkmeta_snapshot_checkpoint, forkmeta_snapshot_tail) have no checksum
 * of their own -- their length+FNV-1a are recorded in the sibling
 * forkmeta_snapshots/forkmeta_manifest_v1 file instead -- so fixing them up
 * means patching that sibling file in the live directory to match the
 * mutated payload; every other target only touches buf/len.
 */
extern void ps_fuzz_crc_fixup(const char *target_name, const char *work_dir,
							   uint8_t *buf, size_t len);

#endif							/* PS_FUZZ_CRC_FIXUP_H */
