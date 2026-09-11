/*-------------------------------------------------------------------------
 *
 * pagestore_format.h
 *	Compiled identities of every persisted pagestore format.
 *
 * Each module that owns an on-disk format reports the magic and version it
 * writes and accepts.  The fixture check compares this table against the
 * committed fixture so a format change cannot land without a fixture update.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAGESTORE_FORMAT_H
#define PAGESTORE_FORMAT_H

#include <stddef.h>
#include <stdint.h>

typedef struct PsFormatIdentity
{
	const char *family;			/* fixture family, e.g. "manifest" */
	const char *artifact;		/* file or record the identity guards */
	uint64_t	magic;			/* some formats key on a 64-bit magic */
	uint32_t	version;
} PsFormatIdentity;

/* Each returns the number of identities and points *out at a static table. */
extern size_t ps_manifest_format_identities(const PsFormatIdentity **out);
extern size_t ps_retention_format_identities(const PsFormatIdentity **out);
extern size_t ps_wal_store_format_identities(const PsFormatIdentity **out);
extern size_t ps_forkmeta_snapshot_format_identities(const PsFormatIdentity **out);
extern size_t ps_walidx_snapshot_format_identities(const PsFormatIdentity **out);
extern size_t ps_core_format_identities(const PsFormatIdentity **out);
extern size_t ps_storage_posix_format_identities(const PsFormatIdentity **out);

#endif							/* PAGESTORE_FORMAT_H */
