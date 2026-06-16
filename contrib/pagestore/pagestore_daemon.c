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
 *	 page_idx: (key, block) -> chain of every stored version {lsn, seg, off}
 *	 fork_idx: (key)        -> current size of the fork in blocks
 *
 * Both are pure in-memory state, rebuilt from the segments by recover() at
 * startup.  (Prototype: no GC/compaction, so the version chain only grows.)
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

/* Hash entry: all versions ever written for one (key, block), in arrival order. */
typedef struct PageEnt
{
	struct PageEnt *next;		/* bucket chain */
	PsKey		key;
	uint32_t	block;
	PageVer    *vers;			/* dynamic array, length nver, capacity cap */
	int			nver;
	int			cap;
} PageEnt;

/* Hash entry: the block count of one relation fork. */
typedef struct ForkEnt
{
	struct ForkEnt *next;		/* bucket chain */
	PsKey		key;
	uint32_t	nblocks;
} ForkEnt;

static PageEnt *page_idx[IDX_BUCKETS];
static ForkEnt *fork_idx[IDX_BUCKETS];

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

/* --- page index --- */

static PageEnt *
page_find(const PsKey *key, uint32_t block)
{
	uint32_t	h = fnv(key, sizeof(*key)) ^ (block * 2654435761u);
	PageEnt    *e;

	for (e = page_idx[h & IDX_MASK]; e; e = e->next)
		if (e->block == block && key_eq(&e->key, key))
			return e;
	return NULL;
}

/*
 * Record a new version of (key, block).  This only ever appends to the version
 * chain -- existing versions are never dropped -- which is what makes the store
 * copy-on-write.  Called both from the live write path (append_page) and from
 * recover() while replaying segments.
 */
static void
page_add_version(const PsKey *key, uint32_t block, uint64_t lsn,
				 int seg, uint64_t off)
{
	uint32_t	h = fnv(key, sizeof(*key)) ^ (block * 2654435761u);
	PageEnt    *e = page_find(key, block);

	if (!e)
	{
		/* first version of this page: create the bucket entry */
		e = calloc(1, sizeof(*e));
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

/* Newest version overall -- what an ordinary (current) read returns. */
static PageVer *
page_current(PageEnt *e)
{
	PageVer    *best = NULL;

	for (int i = 0; i < e->nver; i++)
		if (!best || e->vers[i].lsn >= best->lsn)
			best = &e->vers[i];
	return best;
}

/*
 * Time-travel read: the version visible as-of snapshot LSN 'target', i.e. the
 * newest version with lsn <= target.  If the target predates every recorded
 * version (none qualifies) we fall back to the oldest version we have, so an
 * as-of read never returns "nothing" for a page that exists.  This is the read
 * side of COW.
 */
static PageVer *
page_asof(PageEnt *e, uint64_t target)
{
	PageVer    *best = NULL;	/* best so far with lsn <= target */
	PageVer    *oldest = NULL;	/* fallback if nothing is <= target */

	for (int i = 0; i < e->nver; i++)
	{
		PageVer    *v = &e->vers[i];

		if (!oldest || v->lsn < oldest->lsn)
			oldest = v;
		if (v->lsn <= target && (!best || v->lsn >= best->lsn))
			best = v;
	}
	return best ? best : oldest;
}

/* --- fork size index --- */

static ForkEnt *
fork_find(const PsKey *key)
{
	uint32_t	h = fnv(key, sizeof(*key));
	ForkEnt    *e;

	for (e = fork_idx[h & IDX_MASK]; e; e = e->next)
		if (key_eq(&e->key, key))
			return e;
	return NULL;
}

static ForkEnt *
fork_get_or_create(const PsKey *key)
{
	uint32_t	h = fnv(key, sizeof(*key));
	ForkEnt    *e = fork_find(key);

	if (!e)
	{
		e = calloc(1, sizeof(*e));
		e->key = *key;
		e->nblocks = 0;
		e->next = fork_idx[h & IDX_MASK];
		fork_idx[h & IDX_MASK] = e;
	}
	return e;
}

static void
fork_grow(const PsKey *key, uint32_t to_nblocks)
{
	ForkEnt    *e = fork_get_or_create(key);

	if (to_nblocks > e->nblocks)
		e->nblocks = to_nblocks;
}

static void
fork_remove(const PsKey *key)
{
	uint32_t	h = fnv(key, sizeof(*key));
	ForkEnt   **pp = &fork_idx[h & IDX_MASK];

	while (*pp)
	{
		if (key_eq(&(*pp)->key, key))
		{
			ForkEnt    *dead = *pp;

			*pp = dead->next;
			free(dead);
			return;
		}
		pp = &(*pp)->next;
	}
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
append_page(const PsKey *key, uint32_t block, const unsigned char *page)
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
	page_add_version(key, block, hdr.lsn, cur_seg, data_off);
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

			page_add_version(&hdr.key, hdr.block, hdr.lsn, id,
							 off + sizeof(hdr));
			fork_grow(&hdr.key, hdr.block + 1);
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
	ch->status = PS_STATUS_OK;
	ch->result = 0;

	switch ((PsOpcode) ch->opcode)
	{
		case PS_OP_CREATE:
			fork_get_or_create(&ch->key);
			break;

		case PS_OP_EXISTS:
			ch->result = (fork_find(&ch->key) != NULL) ? 1 : 0;
			break;

		case PS_OP_UNLINK:
			fork_remove(&ch->key);
			break;

		case PS_OP_NBLOCKS:
			{
				ForkEnt    *fe = fork_find(&ch->key);

				ch->result = fe ? fe->nblocks : 0;
				break;
			}

		case PS_OP_TRUNCATE:
			fork_get_or_create(&ch->key)->nblocks = ch->nblocks;
			break;

		case PS_OP_EXTEND:
			if (append_page(&ch->key, ch->blocknum, ch->data) != 0)
				ch->status = PS_STATUS_ERROR;
			else
				fork_grow(&ch->key, ch->blocknum + 1);
			break;

		case PS_OP_ZEROEXTEND:
			/* allocation only: grow size, no page data stored (reads -> 0) */
			fork_grow(&ch->key, ch->blocknum + ch->nblocks);
			break;

		case PS_OP_WRITEV:
			for (uint32_t i = 0; i < ch->nblocks; i++)
			{
				if (append_page(&ch->key, ch->blocknum + i,
								 ch->data + (size_t) i * page_size) != 0)
				{
					ch->status = PS_STATUS_ERROR;
					break;
				}
			}
			if (ch->status == PS_STATUS_OK)
				fork_grow(&ch->key, ch->blocknum + ch->nblocks);
			break;

		case PS_OP_READV:
			for (uint32_t i = 0; i < ch->nblocks; i++)
			{
				unsigned char *dst = ch->data + (size_t) i * page_size;
				PageEnt    *e = page_find(&ch->key, ch->blocknum + i);
				PageVer    *v = e ? page_current(e) : NULL;

				if (!v || read_version(v, dst) != 0)
					memset(dst, 0, page_size);	/* unwritten -> zeros */
			}
			break;

		case PS_OP_READ_AT:
			{
				PageEnt    *e = page_find(&ch->key, ch->blocknum);
				PageVer    *v = e ? page_asof(e, ch->req_lsn) : NULL;

				if (!v || read_version(v, ch->data) != 0)
					memset(ch->data, 0, page_size);
				break;
			}

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

	/* rebuild indexes from existing segments before serving any request */
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
