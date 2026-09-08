/*-------------------------------------------------------------------------
 *
 * pagestore_store_owner.h
 *
 * Same-process reference-counted ownership of a POSIX pagestore root.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAGESTORE_STORE_OWNER_H
#define PAGESTORE_STORE_OWNER_H

typedef struct PsStoreOwner PsStoreOwner;

/* Acquire an exclusive, non-blocking lease for the canonical store root. */
extern int ps_store_owner_acquire(const char *store_dir,
								  PsStoreOwner **owner_out);

/* The canonical root spelling held by an acquired lease. */
extern const char *ps_store_owner_root(const PsStoreOwner *owner);

/* Require that a lease belongs to this process.  Inherited handles fail with
 * ECHILD before callers touch cached descriptors, paths, or provider locks. */
extern int ps_store_owner_require_current(const PsStoreOwner *owner);

/* Release one same-process reference.  A handle inherited across fork is a
 * no-op in the child, so it cannot release or reuse the parent's ownership. */
extern void ps_store_owner_release(PsStoreOwner *owner);

#endif							/* PAGESTORE_STORE_OWNER_H */
