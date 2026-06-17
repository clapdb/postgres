/*-------------------------------------------------------------------------
 *
 * pagestore_daemon.c
 *	  Standalone log-structured storage daemon for the pagestore "localsvc"
 *	  backend.
 *
 * Design (see also pagestore_ipc.h):
 *
 *	- Page-size agnostic.  The logical page size is configured with --page-size
 *	  (8192 for PostgreSQL, 16384 for InnoDB, ...) and published in the shm
 *	  header; nothing about the on-disk format assumes a particular value.
 *
 *	- Log-structured storage.  Every page write is appended to a growing
 *	  segment file (seg_NNNNNNNN) as a self-describing record
 *	  [SegRecHdr | page bytes].  Writes are therefore large and sequential
 *	  regardless of how small individual logical pages are -- which is what
 *	  makes the store friendly to NVMe/SPDK and network transports.  Old
 *	  versions are never overwritten, so the log is also the COW history.
 *
 *	- Indirection map.  An in-memory index maps (key, block) -> a chain of
 *	  versions {lsn, segment, offset}.  This lets a single small logical page
 *	  be addressed inside a large physical segment (ranged read), decoupling
 *	  "address by page" from "store/transfer in large units".  The index is
 *	  rebuilt by scanning segments at startup.
 *
 * Includes only pagestore_ipc.h and libc -- never PostgreSQL headers.
 *
 * Usage: pagestore_daemon --shm NAME --store DIR [--page-size N] [--segment-size N]
 *
 * src/../contrib/pagestore/pagestore_daemon.c
 *
 *-------------------------------------------------------------------------
 */
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "pagestore_ipc.h"

static volatile sig_atomic_t stop_requested = 0;
static const char *store_dir;
static const char *shm_name;
static uint32_t page_size = PS_DEFAULT_PAGE_SIZE;
static uint64_t segment_size = 8 * 1024 * 1024;

static void
on_signal(int sig)
{
	(void) sig;
	stop_requested = 1;
}

/* ===================== segment storage (log-structured) ================= */

#define SEG_MAGIC	0x53454752	/* "SEGR" */

/*
 * On-disk layout of one appended page version: this header immediately
 * followed by 'len' page bytes.  The header is self-describing (carries the
 * full key/block/lsn), which is what lets recover() rebuild the entire
 * in-memory index by scanning segments sequentially -- no separate index file
 * to keep in sync.
 */
typedef struct SegRecHdr
{
	uint32_t	magic;			/* SEG_MAGIC; also the end-of-log sentinel */
	uint32_t	timeline;		/* timeline the version belongs to */
	PsKey		key;
	uint32_t	block;
	uint64_t	lsn;			/* the page's pd_lsn at write time */
	uint32_t	len;			/* page bytes following the header */
} SegRecHdr;

/*
 * Segments are plain append-only files seg_00000000, seg_00000001, ...  We keep
 * one OS fd open per segment (opened lazily, never closed during a run) and an
 * append cursor (cur_seg, cur_off) marking where the next record goes.
 */
static int *seg_fds;			/* open fd per segment id (-1 if not open) */
static int	segs_cap;			/* allocated length of seg_fds[] */
static int	cur_seg = -1;		/* segment currently being appended (-1: none yet) */
static uint64_t cur_off;		/* append cursor within cur_seg */

static void
seg_path(char *buf, size_t buflen, int id)
{
	snprintf(buf, buflen, "%s/seg_%08d", store_dir, id);
}

/* Return a cached fd for segment 'id', opening (optionally creating) it once. */
static int
seg_fd(int id, int create)
{
	char		path[4096];
	int			fd;

	if (id < segs_cap && seg_fds[id] >= 0)
		return seg_fds[id];

	/* grow the fd cache to cover 'id', initializing new slots to -1 */
	if (id >= segs_cap)
	{
		int			newcap = (id + 16) * 2;

		seg_fds = realloc(seg_fds, (size_t) newcap * sizeof(int));
		for (int i = segs_cap; i < newcap; i++)
			seg_fds[i] = -1;
		segs_cap = newcap;
	}

	seg_path(path, sizeof(path), id);
	fd = open(path, O_RDWR | (create ? O_CREAT : 0), 0600);
	if (fd >= 0)
		seg_fds[id] = fd;
	return fd;
}

/* ===================== in-memory indexes =============================== */

/*
 * Two chained hash tables form the indirection map that lets a single logical
 * page be located inside the large append-only segments:
 *
 *	 page_idx: (timeline, key, block) -> chain of versions {lsn, seg, off}
 *	 fork_idx: (timeline, key)        -> size of the fork on that timeline
 *
 * Entries are keyed by timeline so a branch's writes are isolated; reads that
 * miss on a timeline fall through to its parent (see read_through()).  Both
 * tables are in-memory state, rebuilt from the segments by recover().
 * (Prototype: no GC/compaction, so the version chain only grows.)
 */
#define IDX_BUCKETS		(1 << 16)
#define IDX_MASK		(IDX_BUCKETS - 1)

/* One stored version of a page: its LSN and where the bytes live on disk. */
typedef struct PageVer
{
	uint64_t	lsn;			/* the page's pd_lsn when it was written */
	int			seg;			/* segment id holding the bytes */
	uint64_t	off;			/* byte offset of the page within that segment */
} PageVer;

/* Hash entry: all versions of one (timeline, key, block), in arrival order. */
typedef struct PageEnt
{
	struct PageEnt *next;		/* bucket chain */
	uint32_t	timeline;
	PsKey		key;
	uint32_t	block;
	PageVer    *vers;			/* dynamic array, length nver, capacity cap */
	int			nver;
	int			cap;
} PageEnt;

/* Hash entry: the block count of one fork on one timeline. */
typedef struct ForkEnt
{
	struct ForkEnt *next;		/* bucket chain */
	uint32_t	timeline;
	PsKey		key;
	uint32_t	nblocks;
} ForkEnt;

static PageEnt *page_idx[IDX_BUCKETS];
static ForkEnt *fork_idx[IDX_BUCKETS];

/*
 * Timeline metadata.  Timeline 0 is the root (no parent).  A branch records its
 * parent and the LSN at which it forked; reads of pages the branch never wrote
 * fall through to the parent as-of that branch LSN, so the branch is a stable
 * copy-on-write snapshot.
 */
#define MAX_TIMELINES	1024
typedef struct TimelineMeta
{
	int			defined;		/* 1 if this timeline exists */
	int			parent;			/* parent timeline id, or -1 for the root */
	uint64_t	branch_lsn;		/* parent LSN this timeline forked at */
} TimelineMeta;

static TimelineMeta timelines[MAX_TIMELINES];

/* FNV-1a hash over a byte range (used to hash keys into buckets). */

static uint32_t
fnv(const void *p, size_t n)
{
	const unsigned char *b = p;
	uint32_t	h = 2166136261u;

	for (size_t i = 0; i < n; i++)
	{
		h ^= b[i];
		h *= 16777619u;
	}
	return h;
}

static int
key_eq(const PsKey *a, const PsKey *b)
{
	return a->spcOid == b->spcOid && a->dbOid == b->dbOid &&
		a->relNumber == b->relNumber && a->forkNum == b->forkNum;
}

/* --- page index (keyed by timeline, key, block) --- */

static uint32_t
page_hash(uint32_t timeline, const PsKey *key, uint32_t block)
{
	return fnv(key, sizeof(*key)) ^ (block * 2654435761u) ^ (timeline * 40503u);
}

static PageEnt *
page_find(uint32_t timeline, const PsKey *key, uint32_t block)
{
	uint32_t	h = page_hash(timeline, key, block);
	PageEnt    *e;

	for (e = page_idx[h & IDX_MASK]; e; e = e->next)
		if (e->timeline == timeline && e->block == block && key_eq(&e->key, key))
			return e;
	return NULL;
}

/*
 * Record a new version of (timeline, key, block).  Only ever appends to the
 * version chain -- existing versions are never dropped -- which is what makes
 * the store copy-on-write.  The version is tagged with the writing timeline, so
 * a branch's writes never disturb its parent.  Called from append_page and from
 * recover() while replaying segments.
 */
static void
page_add_version(uint32_t timeline, const PsKey *key, uint32_t block,
				 uint64_t lsn, int seg, uint64_t off)
{
	uint32_t	h = page_hash(timeline, key, block);
	PageEnt    *e = page_find(timeline, key, block);

	if (!e)
	{
		e = calloc(1, sizeof(*e));
		e->timeline = timeline;
		e->key = *key;
		e->block = block;
		e->next = page_idx[h & IDX_MASK];
		page_idx[h & IDX_MASK] = e;
	}
	if (e->nver == e->cap)		/* grow the version array geometrically */
	{
		e->cap = e->cap ? e->cap * 2 : 2;
		e->vers = realloc(e->vers, (size_t) e->cap * sizeof(PageVer));
	}
	e->vers[e->nver].lsn = lsn;
	e->vers[e->nver].seg = seg;
	e->vers[e->nver].off = off;
	e->nver++;
}

/* Newest version on this entry with lsn <= read_lsn, or NULL if none. */
static PageVer *
page_visible(PageEnt *e, uint64_t read_lsn)
{
	PageVer    *best = NULL;

	for (int i = 0; i < e->nver; i++)
	{
		PageVer    *v = &e->vers[i];

		if (v->lsn <= read_lsn && (!best || v->lsn >= best->lsn))
			best = v;
	}
	return best;
}

/* --- fork size index (keyed by timeline, key) --- */

static ForkEnt *
fork_find(uint32_t timeline, const PsKey *key)
{
	uint32_t	h = fnv(key, sizeof(*key)) ^ (timeline * 40503u);
	ForkEnt    *e;

	for (e = fork_idx[h & IDX_MASK]; e; e = e->next)
		if (e->timeline == timeline && key_eq(&e->key, key))
			return e;
	return NULL;
}

static ForkEnt *
fork_get_or_create(uint32_t timeline, const PsKey *key)
{
	uint32_t	h = fnv(key, sizeof(*key)) ^ (timeline * 40503u);
	ForkEnt    *e = fork_find(timeline, key);

	if (!e)
	{
		e = calloc(1, sizeof(*e));
		e->timeline = timeline;
		e->key = *key;
		e->nblocks = 0;
		e->next = fork_idx[h & IDX_MASK];
		fork_idx[h & IDX_MASK] = e;
	}
	return e;
}

static void
fork_grow(uint32_t timeline, const PsKey *key, uint32_t to_nblocks)
{
	ForkEnt    *e = fork_get_or_create(timeline, key);

	if (to_nblocks > e->nblocks)
		e->nblocks = to_nblocks;
}

static void
fork_remove(uint32_t timeline, const PsKey *key)
{
	uint32_t	h = fnv(key, sizeof(*key)) ^ (timeline * 40503u);
	ForkEnt   **pp = &fork_idx[h & IDX_MASK];

	while (*pp)
	{
		if ((*pp)->timeline == timeline && key_eq(&(*pp)->key, key))
		{
			ForkEnt    *dead = *pp;

			*pp = dead->next;
			free(dead);
			return;
		}
		pp = &(*pp)->next;
	}
}

/* --- timeline metadata + read-through --- */

static void
timeline_define(uint32_t id, int parent, uint64_t branch_lsn)
{
	if (id >= MAX_TIMELINES)
		return;
	timelines[id].defined = 1;
	timelines[id].parent = parent;
	timelines[id].branch_lsn = branch_lsn;
}

static int
timeline_has_parent(uint32_t timeline)
{
	return timeline < MAX_TIMELINES && timelines[timeline].defined &&
		timelines[timeline].parent >= 0;
}

/*
 * Validate a branch-creation request before it is recorded.  read_through() and
 * the fork-size walks follow the parent chain assuming it is finite and well
 * formed, so a bad CREATE_BRANCH must be rejected rather than persisted.  Refuse:
 *	- a new id that is out of range, the root (0), or already defined (otherwise
 *	  re-creating an id silently rewrites an existing branch's ancestry);
 *	- a parent that is out of range or not yet defined (the requested parent must
 *	  actually exist, else the branch silently inherits from nothing);
 *	- a parent whose ancestry already reaches the new id, which would turn the
 *	  parent walk into an infinite loop (e.g. new == parent, or A->B->A).
 * Returns 1 if (new_tl, parent) is safe to define.
 */
static int
branch_request_ok(uint32_t new_tl, int parent)
{
	if (new_tl == 0 || new_tl >= MAX_TIMELINES || timelines[new_tl].defined)
		return 0;
	if (parent < 0 || parent >= MAX_TIMELINES || !timelines[parent].defined)
		return 0;
	for (int t = parent; t >= 0 && t < MAX_TIMELINES; t = timelines[t].parent)
	{
		if ((uint32_t) t == new_tl)
			return 0;			/* cycle */
		if (!timelines[t].defined)
			return 0;			/* broken chain: refuse rather than risk a loop */
	}
	return 1;
}

/*
 * Resolve a read by walking the timeline ancestry: return the newest version of
 * (key, block) visible at read_lsn on 'timeline'; if the timeline never wrote
 * the page (or only after read_lsn), descend to the parent, capping read_lsn at
 * the branch LSN so the branch sees a frozen snapshot of the parent.  Returns
 * the chosen PageVer, or NULL if no ancestor has the page.
 */
static PageVer *
read_through(uint32_t timeline, const PsKey *key, uint32_t block,
			 uint64_t read_lsn)
{
	for (;;)
	{
		PageEnt    *e = page_find(timeline, key, block);
		PageVer    *v = e ? page_visible(e, read_lsn) : NULL;

		if (v)
			return v;
		if (!timeline_has_parent(timeline))
			return NULL;
		if (timelines[timeline].branch_lsn < read_lsn)
			read_lsn = timelines[timeline].branch_lsn;
		timeline = (uint32_t) timelines[timeline].parent;
	}
}

/*
 * Fork size visible on 'timeline': the maximum block count across the timeline
 * and all its ancestors.  A branch that has written only some blocks of a fork
 * still inherits the parent's full size (the unwritten blocks are served by
 * read_through), so we must take the max rather than the first entry found --
 * otherwise the branch would under-report the size and reads of inherited
 * blocks would look out of range.
 *
 * (Prototype limitation: nblocks is not itself versioned, so if the parent
 * *grows* a fork after the branch point the branch sees the larger size; for a
 * quiescent parent -- the usual branch case -- this is correct.)
 */
static uint32_t
fork_nblocks_through(uint32_t timeline, const PsKey *key)
{
	uint32_t	maxnb = 0;

	for (;;)
	{
		ForkEnt    *e = fork_find(timeline, key);

		if (e && e->nblocks > maxnb)
			maxnb = e->nblocks;
		if (!timeline_has_parent(timeline))
			return maxnb;
		timeline = (uint32_t) timelines[timeline].parent;
	}
}

/* Does the fork exist on 'timeline' or any ancestor? */
static int
fork_exists_through(uint32_t timeline, const PsKey *key)
{
	for (;;)
	{
		if (fork_find(timeline, key))
			return 1;
		if (!timeline_has_parent(timeline))
			return 0;
		timeline = (uint32_t) timelines[timeline].parent;
	}
}

/*
 * Timeline metadata is persisted as an append-only log of fixed records in
 * "<store>/timelines", so branches survive a daemon restart.  (The page data
 * itself is already durable in the segments.)
 */
typedef struct TimelineRec
{
	uint32_t	id;
	int32_t		parent;
	uint64_t	branch_lsn;
} TimelineRec;

static void
timeline_persist(uint32_t id, int parent, uint64_t branch_lsn)
{
	char		path[4096];
	int			fd;
	TimelineRec rec = {id, (int32_t) parent, branch_lsn};

	snprintf(path, sizeof(path), "%s/timelines", store_dir);
	fd = open(path, O_WRONLY | O_APPEND | O_CREAT, 0600);
	if (fd < 0)
		return;					/* best-effort */
	if (write(fd, &rec, sizeof(rec)) != (ssize_t) sizeof(rec))
	{
		/* ignore: best-effort */
	}
	close(fd);
}

static void
load_timelines(void)
{
	char		path[4096];
	int			fd;
	TimelineRec rec;

	snprintf(path, sizeof(path), "%s/timelines", store_dir);
	fd = open(path, O_RDONLY);
	if (fd < 0)
		return;					/* none yet */
	while (read(fd, &rec, sizeof(rec)) == (ssize_t) sizeof(rec))
		timeline_define(rec.id, rec.parent, rec.branch_lsn);
	close(fd);
}

/* ===================== write / read primitives ========================= */

/* The page's own pd_lsn lives in its first 8 bytes (xlogid, xrecoff). */
static uint64_t
page_lsn(const unsigned char *page)
{
	uint32_t	xlogid,
				xrecoff;

	memcpy(&xlogid, page, 4);
	memcpy(&xrecoff, page + 4, 4);
	return ((uint64_t) xlogid << 32) | xrecoff;
}

/*
 * Append one page version at the log head and record it in the index.  Because
 * every write lands at the moving append cursor, physical writes are large and
 * sequential even though each logical page is small -- the property we want for
 * NVMe/SPDK and network transports.
 */
static int
append_page(uint32_t timeline, const PsKey *key, uint32_t block,
			const unsigned char *page)
{
	SegRecHdr	hdr;
	uint64_t	reclen = sizeof(SegRecHdr) + page_size;
	int			fd;
	uint64_t	data_off;

	/* roll over to a fresh segment when the current one would overflow */
	if (cur_seg < 0 || cur_off + reclen > segment_size)
	{
		cur_seg = (cur_seg < 0) ? 0 : cur_seg + 1;
		cur_off = 0;
	}
	fd = seg_fd(cur_seg, 1);
	if (fd < 0)
		return -1;

	hdr.magic = SEG_MAGIC;
	hdr.timeline = timeline;
	hdr.key = *key;
	hdr.block = block;
	hdr.lsn = page_lsn(page);	/* version key taken from the page itself */
	hdr.len = page_size;

	/* write header then page bytes contiguously at the append cursor */
	if (pwrite(fd, &hdr, sizeof(hdr), cur_off) != (ssize_t) sizeof(hdr))
		return -1;
	data_off = cur_off + sizeof(hdr);
	if (pwrite(fd, page, page_size, data_off) != (ssize_t) page_size)
		return -1;

	/* index points at the page bytes (data_off), so reads skip the header */
	page_add_version(timeline, key, block, hdr.lsn, cur_seg, data_off);
	cur_off += reclen;
	return 0;
}

/* Read a specific version's page bytes into out (page_size bytes). */
static int
read_version(const PageVer *v, unsigned char *out)
{
	int			fd = seg_fd(v->seg, 0);

	if (fd < 0)
		return -1;
	if (pread(fd, out, page_size, v->off) != (ssize_t) page_size)
		return -1;
	return 0;
}

/* ===================== recovery (rebuild index from segments) ========== */

/*
 * Rebuild the entire in-memory index at startup by replaying the segments in
 * order.  Each record is self-describing, so replaying append_page's effect
 * (page_add_version + fork_grow) for every record reconstructs both indexes and
 * leaves the append cursor positioned just past the last valid record.
 *
 * Caveat (prototype): TRUNCATE and UNLINK are not logged as records, so their
 * effects are not reproduced on restart.  Also a partial/torn trailing record
 * is simply treated as end-of-log (the magic/len check below stops the scan).
 */
static void
recover(void)
{
	for (int id = 0;; id++)
	{
		char		path[4096];
		int			fd;
		uint64_t	off = 0;
		struct stat st;

		seg_path(path, sizeof(path), id);
		fd = open(path, O_RDONLY);
		if (fd < 0)
			break;				/* no more segments -> done */
		if (fstat(fd, &st) != 0)
		{
			close(fd);
			break;
		}

		/* replay records until one fails to validate (end of log) */
		for (;;)
		{
			SegRecHdr	hdr;
			ssize_t		n = pread(fd, &hdr, sizeof(hdr), off);

			if (n != (ssize_t) sizeof(hdr) || hdr.magic != SEG_MAGIC ||
				hdr.len != page_size)
				break;

			page_add_version(hdr.timeline, &hdr.key, hdr.block, hdr.lsn, id,
							 off + sizeof(hdr));
			fork_grow(hdr.timeline, &hdr.key, hdr.block + 1);
			off += sizeof(hdr) + hdr.len;
		}
		close(fd);

		/* this segment is the newest seen so far; append continues after it */
		cur_seg = id;
		cur_off = off;
	}
	if (cur_seg >= 0)
		fprintf(stderr, "pagestore_daemon: recovered through segment %d (off %llu)\n",
				cur_seg, (unsigned long long) cur_off);
}

/* ===================== request handling ================================ */

static void
handle_request(PsChannel *ch)
{
	uint32_t	tl = ch->timeline;

	ch->status = PS_STATUS_OK;
	ch->result = 0;

	switch ((PsOpcode) ch->opcode)
	{
		case PS_OP_CREATE:
			fork_get_or_create(tl, &ch->key);
			break;

		case PS_OP_EXISTS:
			ch->result = fork_exists_through(tl, &ch->key) ? 1 : 0;
			break;

		case PS_OP_UNLINK:
			fork_remove(tl, &ch->key);
			break;

		case PS_OP_NBLOCKS:
			ch->result = fork_nblocks_through(tl, &ch->key);
			break;

		case PS_OP_TRUNCATE:
			fork_get_or_create(tl, &ch->key)->nblocks = ch->nblocks;
			break;

		case PS_OP_EXTEND:
			if (append_page(tl, &ch->key, ch->blocknum, ch->data) != 0)
				ch->status = PS_STATUS_ERROR;
			else
				fork_grow(tl, &ch->key, ch->blocknum + 1);
			break;

		case PS_OP_ZEROEXTEND:
			/* allocation only: grow size, no page data stored (reads -> 0) */
			fork_grow(tl, &ch->key, ch->blocknum + ch->nblocks);
			break;

		case PS_OP_WRITEV:
			for (uint32_t i = 0; i < ch->nblocks; i++)
			{
				if (append_page(tl, &ch->key, ch->blocknum + i,
								 ch->data + (size_t) i * page_size) != 0)
				{
					ch->status = PS_STATUS_ERROR;
					break;
				}
			}
			if (ch->status == PS_STATUS_OK)
				fork_grow(tl, &ch->key, ch->blocknum + ch->nblocks);
			break;

		case PS_OP_READV:
			/* "current" read on this timeline: read-through at max LSN */
			for (uint32_t i = 0; i < ch->nblocks; i++)
			{
				unsigned char *dst = ch->data + (size_t) i * page_size;
				PageVer    *v = read_through(tl, &ch->key, ch->blocknum + i,
											 UINT64_MAX);

				if (!v || read_version(v, dst) != 0)
					memset(dst, 0, page_size);	/* unwritten -> zeros */
			}
			break;

		case PS_OP_READ_AT:
			{
				/* as-of read on this timeline, honoring branch ancestry */
				PageVer    *v = read_through(tl, &ch->key, ch->blocknum,
											 ch->req_lsn);

				if (!v || read_version(v, ch->data) != 0)
					memset(ch->data, 0, page_size);
				break;
			}

		case PS_OP_CREATE_BRANCH:
			/*
			 * Instant clone: just record metadata.  Timeline ch->timeline forks
			 * from ch->parent_timeline at LSN ch->req_lsn.  No page data is
			 * copied -- the branch shares the parent's pages by read-through
			 * until it writes (copy-on-write).
			 */
			if (branch_request_ok(ch->timeline, (int) ch->parent_timeline))
			{
				timeline_define(ch->timeline, (int) ch->parent_timeline,
								ch->req_lsn);
				timeline_persist(ch->timeline, (int) ch->parent_timeline,
								 ch->req_lsn);
			}
			else
				ch->status = PS_STATUS_ERROR;
			break;

		case PS_OP_IMMEDSYNC:
			for (int id = 0; id < segs_cap; id++)
				if (seg_fds[id] >= 0)
					fsync(seg_fds[id]);
			break;

		default:
			ch->status = PS_STATUS_ERROR;
			break;
	}
}

int
main(int argc, char **argv)
{
	int			fd;
	void	   *shm;
	PsShmHeader *hdr;
	struct sigaction sa;

	for (int i = 1; i < argc; i++)
	{
		if (strcmp(argv[i], "--shm") == 0 && i + 1 < argc)
			shm_name = argv[++i];
		else if (strcmp(argv[i], "--store") == 0 && i + 1 < argc)
			store_dir = argv[++i];
		else if (strcmp(argv[i], "--page-size") == 0 && i + 1 < argc)
			page_size = (uint32_t) strtoul(argv[++i], NULL, 10);
		else if (strcmp(argv[i], "--segment-size") == 0 && i + 1 < argc)
			segment_size = strtoull(argv[++i], NULL, 10);
		else
		{
			fprintf(stderr, "usage: %s --shm NAME --store DIR "
					"[--page-size N] [--segment-size N]\n", argv[0]);
			return 2;
		}
	}
	if (!shm_name || !store_dir || page_size == 0 || page_size > PS_IO_UNIT)
	{
		fprintf(stderr, "usage: %s --shm NAME --store DIR "
				"[--page-size N] [--segment-size N]\n", argv[0]);
		return 2;
	}

	if (mkdir(store_dir, 0700) != 0 && errno != EEXIST)
	{
		perror("mkdir store");
		return 1;
	}

	/* timeline 0 is the root; load any persisted branches, then replay data */
	timeline_define(0, -1, 0);
	load_timelines();
	recover();

	fd = shm_open(shm_name, O_CREAT | O_RDWR, 0600);
	if (fd < 0)
	{
		perror("shm_open");
		return 1;
	}
	if (ftruncate(fd, PS_SHM_SIZE) != 0)
	{
		perror("ftruncate shm");
		return 1;
	}
	shm = mmap(NULL, PS_SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (shm == MAP_FAILED)
	{
		perror("mmap");
		return 1;
	}
	close(fd);

	/*
	 * Initialize the shared region.  NB: this zeroes the whole segment, so the
	 * daemon must be (re)started while no engine is attached -- restarting it
	 * against a live shm would wipe in-flight channel state.  A production
	 * version would attach without re-initializing when the header is already
	 * valid.
	 */
	hdr = (PsShmHeader *) shm;
	memset(shm, 0, PS_SHM_SIZE);
	hdr->magic = PS_SHM_MAGIC;
	hdr->version = PS_SHM_VERSION;
	hdr->page_size = page_size;
	hdr->io_unit = PS_IO_UNIT;
	hdr->nchannels = PS_MAX_CHANNELS;
	hdr->channel_stride = PS_CHANNEL_STRIDE;
	hdr->channels_off = PS_CHANNELS_OFF;

	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = on_signal;
	sigaction(SIGINT, &sa, NULL);
	sigaction(SIGTERM, &sa, NULL);

	fprintf(stderr, "pagestore_daemon: shm=%s store=%s page_size=%u io_unit=%u "
			"channels=%d ready\n",
			shm_name, store_dir, page_size, PS_IO_UNIT, PS_MAX_CHANNELS);

	while (!stop_requested)
	{
		int			did_work = 0;

		for (uint32_t i = 0; i < PS_MAX_CHANNELS; i++)
		{
			PsChannel  *ch = ps_channel(shm, i);

			if (ps_load_acquire(&ch->state) != PS_STATE_REQUEST)
				continue;

			handle_request(ch);
			ps_store_release(&ch->state, PS_STATE_DONE);
			did_work = 1;
		}

		if (!did_work)
		{
			struct timespec ts = {0, 20000};	/* 20us */

			nanosleep(&ts, NULL);
		}
	}

	fprintf(stderr, "pagestore_daemon: shutting down\n");
	munmap(shm, PS_SHM_SIZE);
	return 0;
}
