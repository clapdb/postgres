/*
 * Artifact publication is an admission-sequence interval, not the presence of
 * a page at an LSN. Included after the page/fork indexes; all calls hold the
 * data shard lock, and metadata keys deliberately route to that same shard.
 */
static int
artifact_data_key(const PsKey *key)
{
	return key->forkNum == 0 &&
		(key->klass == PS_KLASS_SLRU || key->klass == PS_KLASS_READER_SNAPSHOT);
}

static PsKey
artifact_meta_key(const PsKey *key)
{
	PsKey		meta = *key;

	meta.forkNum = (int32_t) key->klass;
	meta.klass = PS_KLASS_ARTIFACT;
	return meta;
}

static int
artifact_record(uint32_t tl, const PsKey *meta, uint32_t block,
				const PageVer *v, PsArtifactLifecycle *record, int map_locked)
{
	unsigned char *page = malloc(page_size);
	uint64_t	lsn = 0,
				seq = 0;
	int			found;

	if (!page)
		return -1;
	if (__atomic_load_n(&artifact_io_failed, __ATOMIC_ACQUIRE))
	{
		free(page);
		return -1;
	}
	if (v->seg >= 0)
		found = read_version(v, page) == 0;
	else
	{
		if (!map_locked)
			ps_lock_map_rd();
		found = layer_map_lookup_locked(tl, meta, block, v->lsn,
										v->admission_seq, v->lsn, &lsn, &seq, page);
		if (!map_locked)
			ps_unlock_map();
		found = found == 1 && lsn == v->lsn && seq == v->admission_seq;
	}
	memcpy(record, page, sizeof(*record));
	free(page);
	if (!found || record->magic != PS_ARTIFACT_LIFECYCLE_MAGIC ||
		record->version != PS_ARTIFACT_LIFECYCLE_VERSION ||
		record->generation != v->lsn || record->timeline != tl ||
		record->incarnation != __atomic_load_n(&timelines[tl].incarnation, __ATOMIC_ACQUIRE) ||
		record->klass != (uint32_t) meta->forkNum || record->spcOid != meta->spcOid ||
		record->dbOid != meta->dbOid || record->relNumber != meta->relNumber ||
		record->crc != ~ps_crc32c_update(UINT32_MAX, record,
										 offsetof(PsArtifactLifecycle, crc)) ||
		(block == PS_ARTIFACT_BEGIN_BLOCK ? record->state != 0 :
		 (record->state != PS_ARTIFACT_COMMITTED && record->state != PS_ARTIFACT_DROPPED)))
		return -1;
	if (record->state != PS_ARTIFACT_COMMITTED &&
		(record->begin_seq != 0 || record->page_count != 0 || record->nblocks != 0))
		return -1;
	if (record->state == PS_ARTIFACT_COMMITTED &&
		(record->begin_seq == 0 || record->begin_seq >= v->admission_seq ||
		 record->page_count > record->nblocks ||
		 (record->page_count == 0 && record->nblocks != 0)))
		return -1;
	return 0;
}

/* First BEGIN is an immutable legacy boundary, including across compaction. */
static uint64_t
artifact_legacy_seq(PageEnt *begins)
{
	uint64_t	first = UINT64_MAX;

	if (begins)
		for (int i = 0; i < begins->nver; i++)
			if (begins->vers[i].admission_seq < first)
				first = begins->vers[i].admission_seq;
	return first;
}

/*
 * 0 = legacy/fallback, 1 = selected complete generation, 2 = explicit absence.
 * Missing blocks in a complete generation never inherit an older page.
 */
static int
artifact_visible(uint32_t tl, const PsKey *key, uint32_t block,
				 uint64_t horizon, uint64_t seq_cap, PageVer **out, int map_locked)
{
	PsKey		meta = artifact_meta_key(key);
	PageEnt    *begins = page_find(tl, &meta, PS_ARTIFACT_BEGIN_BLOCK);
	PageEnt    *commits = page_find(tl, &meta, PS_ARTIFACT_COMMIT_BLOCK);
	PageEnt    *pages = page_find(tl, key, block);
	PageVer    *commit = commits ? page_visible(commits, horizon, seq_cap) : NULL;
	uint64_t	first = artifact_legacy_seq(begins);
	PsArtifactLifecycle record;
	PageVer    *best = NULL;

	if (!begins && !commits)
		return 0;
	if (commit)
	{
		if (artifact_record(tl, &meta, PS_ARTIFACT_COMMIT_BLOCK, commit,
							&record, map_locked) != 0)
			return -1;
		if (record.state == PS_ARTIFACT_DROPPED)
		{
			*out = NULL;
			return 2;
		}
	}
	if (pages)
		for (int i = 0; i < pages->nver; i++)
		{
			PageVer    *v = &pages->vers[i];
			int			eligible = commit ?
				(v->lsn == commit->lsn && v->admission_seq > record.begin_seq &&
				 v->admission_seq < commit->admission_seq) :
				(v->admission_seq < first && v->lsn <= horizon &&
				 (seq_cap == 0 || v->lsn < horizon || v->admission_seq <= seq_cap));

			if (eligible && (!best || v->lsn > best->lsn ||
							 (v->lsn == best->lsn && v->admission_seq > best->admission_seq)))
				best = v;
		}
	*out = best;
	return commit ? (best ? 1 : 2) : 0;
}

/*
 * Match page visibility for size/existence, including legacy and ancestry.
 * A zero-page COMMIT is an existing empty object; DROP is nonexistence.
 */
static int
artifact_metadata(uint32_t tl, const PsKey *key, uint64_t horizon,
				  uint64_t seq_cap, int *exists, uint32_t *nblocks)
{
	PsKey		meta = artifact_meta_key(key);
	TlWalk		walk = tl_walk_first(tl, horizon);
	int			rc = 0;

	*exists = 0;
	*nblocks = 0;
	ps_lock_map_rd();
	do
	{
		PageEnt    *commits = page_find(walk.tl, &meta, PS_ARTIFACT_COMMIT_BLOCK);
		PageEnt    *begins = page_find(walk.tl, &meta, PS_ARTIFACT_BEGIN_BLOCK);
		uint64_t	cap = walk.lsn == horizon ? seq_cap : 0;
		uint64_t	lsn = walk.lsn;
		PageVer    *commit = commits ? page_visible(commits, lsn, cap) : NULL;
		ForkEnt    *fork = fork_find(walk.tl, key);
		uint32_t	nb = 0;
		int			state;

		if (commit)
		{
			PsArtifactLifecycle record;

			if (artifact_record(walk.tl, &meta, PS_ARTIFACT_COMMIT_BLOCK,
								commit, &record, 1) != 0)
				rc = -1;
			else if (record.state == PS_ARTIFACT_COMMITTED)
			{
				*exists = 1;
				if (record.nblocks > *nblocks)
					*nblocks = record.nblocks;
			}
			break;
		}

		/*
		 * BEGIN is monotone in LSN. Exclude all subsequent staged growth
		 * while resolving the legacy fork, including at the same LSN.
		 */
		if (begins)
		{
			uint64_t	first = artifact_legacy_seq(begins);

			for (int i = 0; i < begins->nver; i++)
				if (begins->vers[i].admission_seq == first)
				{
					if (lsn >= begins->vers[i].lsn)
					{
						if (lsn > begins->vers[i].lsn || cap == 0 || cap >= first)

							/*
							 * BEGIN itself has no data-fork event; using its
							 * nonzero sequence also handles the first store
							 * mutation.
							 */
							cap = first;
						lsn = begins->vers[i].lsn;
					}
					break;
				}
		}
		state = fork ? fork_asof_hop(fork, lsn, cap, &nb) : FORK_HOP_NONE;
		if (state == FORK_HOP_DEAD)
			break;
		if (state != FORK_HOP_NONE)
		{
			*exists = 1;
			if (nb > *nblocks)
				*nblocks = nb;
		}
		if (state == FORK_HOP_DEF)
			break;
	} while (tl_walk_next(&walk));
	ps_unlock_map();
	return rc;
}

/* One object's decoded intervals, reused by all its page groups in a pass. */
struct ArtifactPruneCache
{
	PsKey		key;
	int			status;
	uint64_t	first;
	PageVer    *begin;
	int			pending;
	struct ArtifactPruneInterval
	{
		PsPruneVersion version;
		PsArtifactLifecycle record;
	}		   *intervals;
	uint32_t	nintervals;
	PsPruneFence *legacy;
	uint32_t	nlegacy;
};

static void
artifact_prune_cache_free(ArtifactPruneCache *cache)
{
	if (cache)
	{
		free(cache->intervals);
		free(cache->legacy);
		free(cache);
	}
}

static int
artifact_prune_cache_build(uint32_t tl, const PsKey *data, uint64_t floor,
						   const PsPruneFence *fences, uint32_t nfences, ArtifactPruneCache **out)
{
	PsKey		meta = artifact_meta_key(data);
	PageEnt    *begins = page_find(tl, &meta, PS_ARTIFACT_BEGIN_BLOCK);
	PageEnt    *commits = page_find(tl, &meta, PS_ARTIFACT_COMMIT_BLOCK);
	PageVer    *last = commits ? page_visible(commits, UINT64_MAX, 0) : NULL;
	uint32_t	n = commits ? (uint32_t) commits->nver : 0;
	unsigned char *selected;
	ArtifactPruneCache *cache;

	artifact_prune_cache_free(*out);
	*out = cache = calloc(1, sizeof(*cache));
	if (!cache)
		return -1;
	cache->key = *data;
	if (!begins && !commits)
		return 0;
	cache->status = -1;
	cache->first = artifact_legacy_seq(begins);
	cache->begin = begins ? page_visible(begins, UINT64_MAX, 0) : NULL;
	cache->pending = cache->begin && cache->begin->admission_seq >= artifact_recovery_seq &&
		(!last || last->admission_seq < cache->begin->admission_seq);
	cache->intervals = calloc(n ? n : 1, sizeof(*cache->intervals));
	cache->legacy = calloc((size_t) nfences + 1, sizeof(*cache->legacy));
	selected = calloc(n ? n : 1, 1);
	if (!cache->intervals || !cache->legacy || !selected)
	{
		free(selected);
		return -1;
	}
	for (uint32_t i = 0; i < n; i++)
		selected[i] = commits->vers[i].lsn > floor;
	for (uint32_t h = 0; h <= nfences; h++)
	{
		PsPruneFence horizon = h == 0 ? (PsPruneFence) {floor, 0} : fences[h - 1];
		PageVer    *commit = commits ? page_visible(commits, horizon.lsn, horizon.admission_seq) : NULL;

		if (commit)
			selected[commit - commits->vers] = 1;
		else
			cache->legacy[cache->nlegacy++] = horizon;
	}
	for (uint32_t i = 0; i < n; i++)
		if (selected[i])
		{
			struct ArtifactPruneInterval *interval = &cache->intervals[cache->nintervals++];

			interval->version = (PsPruneVersion)
			{
				commits->vers[i].lsn, commits->vers[i].admission_seq
			};
			if (artifact_record(tl, &meta, PS_ARTIFACT_COMMIT_BLOCK,
								&commits->vers[i], &interval->record, 1) != 0)
			{
				free(selected);
				return -1;
			}
		}
	free(selected);
	return cache->status = 1;
}

/* Last input tuple at/below an exact boundary; inputs are already sorted. */
static int
artifact_prune_at(const PsPruneVersion *versions, uint32_t n,
				  uint64_t lsn, uint64_t seq)
{
	uint32_t	lo = 0,
				hi = n;

	while (lo < hi)
	{
		uint32_t	mid = lo + (hi - lo) / 2;

		if (versions[mid].lsn < lsn ||
			(versions[mid].lsn == lsn && (seq == 0 || versions[mid].admission_seq <= seq)))
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo == 0 ? -1 : (int) (lo - 1);
}

/*
 * Called under map-wr. Decode each retained interval once per object, then
 * find its page tuple by binary search instead of nested history scans.
 */
static int
artifact_prune_versions(uint32_t tl, const PsKey *key, uint32_t block,
						const PsPruneVersion *versions, uint32_t nversions, uint64_t floor,
						const PsPruneFence *fences, uint32_t nfences, unsigned char *keep,
						ArtifactPruneCache **cachep)
{
	PsKey		data = *key;
	ArtifactPruneCache *cache;

	if (key->klass == PS_KLASS_ARTIFACT)
	{
		data.klass = (uint32_t) key->forkNum;
		data.forkNum = 0;
	}
	if (!artifact_data_key(&data) || floor == 0)
		return 0;
	if (!*cachep || !key_eq(&(*cachep)->key, &data))
		if (artifact_prune_cache_build(tl, &data, floor, fences, nfences, cachep) < 0)
			return -1;
	cache = *cachep;
	if (cache->status != 1)
		return cache->status;
	memset(keep, 0, nversions);
	for (uint32_t i = 0; i < nversions; i++)
	{
		const PsPruneVersion *v = &versions[i];

		if (key->klass == PS_KLASS_ARTIFACT && block == PS_ARTIFACT_BEGIN_BLOCK)
			keep[i] = v->admission_seq == cache->first ||
				(cache->begin && v->admission_seq == cache->begin->admission_seq);
		else if (key->klass != PS_KLASS_ARTIFACT)
			keep[i] = (cache->pending && v->lsn == cache->begin->lsn &&
					   v->admission_seq > cache->begin->admission_seq) ||
				(v->admission_seq < cache->first && v->lsn > floor);
	}
	if (key->klass == PS_KLASS_ARTIFACT && block == PS_ARTIFACT_BEGIN_BLOCK)
		return 1;
	for (uint32_t h = 0; h < cache->nintervals; h++)
	{
		const struct ArtifactPruneInterval *interval = &cache->intervals[h];
		int			i;

		if (key->klass == PS_KLASS_ARTIFACT)
		{
			i = artifact_prune_at(versions, nversions, interval->version.lsn,
								  interval->version.admission_seq);
			if (i >= 0 && versions[i].lsn == interval->version.lsn &&
				versions[i].admission_seq == interval->version.admission_seq)
				keep[i] = 1;
		}
		else if (interval->record.state == PS_ARTIFACT_COMMITTED)
		{
			i = artifact_prune_at(versions, nversions, interval->version.lsn,
								  interval->version.admission_seq - 1);
			if (i >= 0 && versions[i].lsn == interval->version.lsn &&
				versions[i].admission_seq > interval->record.begin_seq)
				keep[i] = 1;
		}
	}
	if (key->klass != PS_KLASS_ARTIFACT)
		for (uint32_t h = 0; h < cache->nlegacy; h++)
		{
			int			i = artifact_prune_at(versions, nversions, cache->legacy[h].lsn,
											  cache->legacy[h].admission_seq);

			while (i >= 0 && versions[i].admission_seq >= cache->first)
				i--;
			if (i >= 0)
				keep[i] = 1;
		}
	return 1;
}

/* Map a classified append outcome (or "append ok, sync failed") to the
 * reason a begin/commit/drop caller reports.  Both an I/O failure writing
 * the record and a failed sync after it landed are STORE_RECORD -- either
 * way durable state may now differ from in-memory state, and the caller
 * must poison. */
static PsArtifactRefuseReason
artifact_reason_for_outcome(PsAppendOutcome outcome)
{
	switch (outcome)
	{
		case PS_APPEND_OK:
			return PS_ARTIFACT_REFUSE_NONE;
		case PS_APPEND_REFUSED_INVALID:
			return PS_ARTIFACT_REFUSE_INVALID;
		case PS_APPEND_REFUSED_UNFENCED:
			return PS_ARTIFACT_REFUSE_UNFENCED;
		case PS_APPEND_REFUSED_FORKMETA_CUTOFF:
			return PS_ARTIFACT_REFUSE_FORKMETA_CUTOFF;
		case PS_APPEND_IO_FAILED:
			return PS_ARTIFACT_REFUSE_STORE_RECORD;
	}
	return PS_ARTIFACT_REFUSE_STORE_RECORD;
}

/*
 * Append a lifecycle record and, on success, sync it durable.  Poisons the
 * artifact path (artifact_io_failed) only when the append's outcome was a
 * real storage I/O failure (bytes may be on disk, in-memory state may now
 * disagree with durable state) or when the append succeeded but the
 * following sync failed (the record is indexed in memory but not proven
 * durable).  An admission refusal -- outcome != PS_APPEND_IO_FAILED --
 * leaves no state behind and must not poison anything else.  *outcome is
 * always set on a nonzero return; the caller maps it to a reason.
 */
static int
artifact_store_record(uint32_t tl, const PsKey *key, uint32_t block,
					  uint64_t lsn, PsArtifactLifecycle *record, uint64_t *seq,
					  PsAppendOutcome *outcome)
{
	PsKey		meta = artifact_meta_key(key);
	unsigned char *page = calloc(1, page_size);
	int			rc;
	PsAppendOutcome local_outcome = PS_APPEND_REFUSED_INVALID;

	if (!page)
	{
		if (outcome)
			*outcome = PS_APPEND_REFUSED_INVALID;
		return -1;
	}
	record->magic = PS_ARTIFACT_LIFECYCLE_MAGIC;
	record->version = PS_ARTIFACT_LIFECYCLE_VERSION;
	record->generation = lsn;
	record->timeline = tl;
	record->incarnation = __atomic_load_n(&timelines[tl].incarnation, __ATOMIC_ACQUIRE);
	record->klass = key->klass;
	record->spcOid = key->spcOid;
	record->dbOid = key->dbOid;
	record->relNumber = key->relNumber;
	record->crc = ~ps_crc32c_update(UINT32_MAX, record,
									offsetof(PsArtifactLifecycle, crc));
	memcpy(page, record, sizeof(*record));
	rc = append_page_raw_outcome(tl, &meta, block, page, lsn, seq, &local_outcome);
	free(page);
	if (rc == 0)
	{
		rc = ps_storage->sync();
		if (rc != 0)
			local_outcome = PS_APPEND_IO_FAILED;
	}
	if (rc != 0 && local_outcome == PS_APPEND_IO_FAILED)
		__atomic_store_n(&artifact_io_failed, 1, __ATOMIC_RELEASE);
	if (outcome)
		*outcome = local_outcome;
	return rc;
}

static int
artifact_mutation_horizon(uint32_t tl, const PsKey *key, uint64_t lsn)
{
	ForkEnt    *data = fork_find(tl, key);
	int			allowed;

	ps_lock_map_rd();
	allowed = !timeline_has_parent(tl) || lsn > timelines[tl].branch_lsn;
	ps_unlock_map();
	return allowed && (!data || lsn >= data->last_page_lsn);
}

/* Record why a caller sees this generation refused (reason may be NULL: the
 * begin/commit/drop callers a fixed-arity test wrapper doesn't care about
 * pass NULL) and return the -1 every existing caller already expects. */
static int
artifact_refuse(PsArtifactRefuseReason *reason, PsArtifactRefuseReason why)
{
	if (reason)
		*reason = why;
	return -1;
}

/* Best-effort diagnostic snapshot for a refused key: the data fork's newest
 * page LSN and the meta fork's newest commit LSN, each 0 if absent.  Callers
 * hold the key's shard lock already (the same lock ps_artifact_begin/commit/
 * drop require); this never mutates anything and is never load-bearing. */
void
ps_artifact_diag(uint32_t tl, const PsKey *key, uint64_t *last_page_lsn,
				 uint64_t *last_commit_lsn)
{
	ForkEnt    *data = tl < MAX_TIMELINES ? fork_find(tl, key) : NULL;
	PsKey		meta = artifact_meta_key(key);
	PageEnt    *e = tl < MAX_TIMELINES ? page_find(tl, &meta, PS_ARTIFACT_COMMIT_BLOCK) : NULL;
	PageVer    *v = e ? page_visible(e, UINT64_MAX, 0) : NULL;

	*last_page_lsn = data ? data->last_page_lsn : 0;
	*last_commit_lsn = v ? v->lsn : 0;
}

int
ps_artifact_begin(uint32_t tl, const PsKey *key, uint64_t lsn, uint64_t *token,
				  PsArtifactRefuseReason *reason)
{
	PsKey		meta;
	PageEnt    *e;
	PageVer    *last;
	PsArtifactLifecycle record = {0};

	if (reason)
		*reason = PS_ARTIFACT_REFUSE_NONE;
	if (__atomic_load_n(&artifact_io_failed, __ATOMIC_ACQUIRE))
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_POISONED);
	if (!core_process_valid() || tl >= MAX_TIMELINES)
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_INVALID);
	if (ps_storage->sync_needs_write_lock)
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_WRITE_LOCK);
	if (!artifact_data_key(key) || lsn == 0)
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_INVALID);
	if (!artifact_mutation_horizon(tl, key, lsn))
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_HORIZON);
	meta = artifact_meta_key(key);
	e = page_find(tl, &meta, PS_ARTIFACT_COMMIT_BLOCK);
	last = e ? page_visible(e, UINT64_MAX, 0) : NULL;
	if (last)
	{
		if (lsn < last->lsn)
			return artifact_refuse(reason, PS_ARTIFACT_REFUSE_OLDER_GENERATION);
		if (artifact_record(tl, &meta, PS_ARTIFACT_COMMIT_BLOCK, last, &record, 0) != 0)
			return artifact_refuse(reason, PS_ARTIFACT_REFUSE_RECORD_UNREADABLE);
		if (lsn == last->lsn && record.state == PS_ARTIFACT_DROPPED)
			return artifact_refuse(reason, PS_ARTIFACT_REFUSE_DROPPED);
	}
	e = page_find(tl, &meta, PS_ARTIFACT_BEGIN_BLOCK);
	last = e ? page_visible(e, UINT64_MAX, 0) : NULL;
	if (last && lsn < last->lsn)
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_BEGIN_NEWER);

	/*
	 * Completed generations are immutable even for zero-cap readers. A
	 * same-LSN retry only verifies already durable content; it never replaces
	 * it.
	 */
	if (record.state == PS_ARTIFACT_COMMITTED && record.generation == lsn)
	{
		*token = record.begin_seq;
		return 0;
	}

	/*
	 * A *new* generation must be admissible at its LSN by the same rule the
	 * data append enforces (artifact_lsn_fenced(), pagestore_core.c): at or
	 * above the durable page-reclaimed frontier, or fenced by an active
	 * page-history owner pin, or the branch point of a live child.  Checking
	 * it here too -- not only on the first data WRITE -- means a BEGIN that
	 * will never be able to write a page is refused up front instead of
	 * leaving an open attempt behind; it must run after the same-LSN
	 * completed-generation short-circuit above so an immutable re-ship of an
	 * already-published generation keeps working even once its LSN has since
	 * fallen behind the frontier (the DROP path is deliberately not fenced;
	 * see ps_artifact_drop).
	 */
	{
		int			fenced;

		ps_lock_map_rd();
		fenced = artifact_lsn_fenced(tl, lsn);
		ps_unlock_map();
		if (!fenced)
			return artifact_refuse(reason, PS_ARTIFACT_REFUSE_UNFENCED);
	}
	memset(&record, 0, sizeof(record));
	{
		PsAppendOutcome outcome;

		if (artifact_store_record(tl, key, PS_ARTIFACT_BEGIN_BLOCK, lsn, &record,
								  token, &outcome) != 0)
			return artifact_refuse(reason, artifact_reason_for_outcome(outcome));
	}
	{
		ForkEnt    *fork = fork_get_or_create(tl, key);

		fork->artifact_attempt_seq = *token;
		fork->artifact_page_count = 0;
		fork->artifact_nblocks = 0;
	}
	return 0;
}

static int
artifact_attempt(uint32_t tl, const PsKey *key, uint64_t lsn, uint64_t token)
{
	PsKey		meta = artifact_meta_key(key);
	PageEnt    *e = page_find(tl, &meta, PS_ARTIFACT_BEGIN_BLOCK);
	PageVer    *begin = e ? page_visible(e, UINT64_MAX, 0) : NULL;
	PageEnt    *commits = page_find(tl, &meta, PS_ARTIFACT_COMMIT_BLOCK);
	PageVer    *last = commits ? page_visible(commits, UINT64_MAX, 0) : NULL;

	return begin && begin->lsn == lsn && begin->admission_seq == token &&
		token >= artifact_recovery_seq &&
		(!last || last->admission_seq < token);
}

static int
artifact_has_protocol(uint32_t tl, const PsKey *key)
{
	PsKey		meta = artifact_meta_key(key);
	TlWalk		walk = tl_walk_first(tl, UINT64_MAX);
	int			found = 0;

	ps_lock_map_rd();
	do
	{
		if (page_find(walk.tl, &meta, PS_ARTIFACT_BEGIN_BLOCK) ||
			page_find(walk.tl, &meta, PS_ARTIFACT_COMMIT_BLOCK))
		{
			found = 1;
			break;
		}
	} while (tl_walk_next(&walk));
	ps_unlock_map();
	return found;
}

/* 1 = already complete, 0 = not this completed attempt, -1 = unavailable. */
static int
artifact_completed_attempt(uint32_t tl, const PsKey *key, uint64_t lsn,
						   uint64_t token, PsArtifactLifecycle *record)
{
	PsKey		meta = artifact_meta_key(key);
	PageEnt    *commits = page_find(tl, &meta, PS_ARTIFACT_COMMIT_BLOCK);
	PageVer    *last = commits ? page_visible(commits, UINT64_MAX, 0) : NULL;

	if (!last || last->lsn != lsn)
		return 0;
	if (artifact_record(tl, &meta, PS_ARTIFACT_COMMIT_BLOCK, last, record, 0) != 0)
		return -1;
	return record->state == PS_ARTIFACT_COMMITTED && record->begin_seq == token;
}

int
ps_artifact_write(uint32_t tl, const PsKey *key, uint32_t block,
				  const unsigned char *page, uint64_t lsn, uint64_t token, uint64_t *seq,
				  PsArtifactRefuseReason *reason)
{
	ForkEnt    *fork;
	PageEnt    *entry;
	PsArtifactLifecycle completed;
	int			state;
	PsAppendOutcome outcome;

	if (reason)
		*reason = PS_ARTIFACT_REFUSE_NONE;
	if (!artifact_data_key(key))
	{
		/* Not under the lifecycle protocol at all (or, for an SLRU/reader-
		 * artifact klass, a nonzero forkNum the protocol never claims): a
		 * plain append.  Classify and report its outcome's reason too --
		 * daemon.c logs and returns it for a WRITE on an SLRU/READER_
		 * SNAPSHOT klass regardless of forkNum, and discarding the outcome
		 * here would otherwise leave *reason at NONE ("refused before
		 * admission"), which is wrong for an admission refusal or I/O
		 * failure that did run.  This never poisons here, matching the
		 * pre-existing shape of every plain-append path in this function
		 * (only an append under an open attempt, below, poisons on a real
		 * I/O failure). */
		PsAppendOutcome raw_outcome;
		int			rc = append_page_raw_outcome(tl, key, block, page, lsn, seq,
												 &raw_outcome);

		if (rc != 0)
			return artifact_refuse(reason, artifact_reason_for_outcome(raw_outcome));
		return rc;
	}
	if (__atomic_load_n(&artifact_io_failed, __ATOMIC_ACQUIRE))
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_POISONED);
	if (token == 0)
	{
		if (artifact_has_protocol(tl, key))
			return artifact_refuse(reason, PS_ARTIFACT_REFUSE_LEGACY_BYPASS);
		{
			PsAppendOutcome raw_outcome;
			int			rc = append_page_raw_outcome(tl, key, block, page, lsn, seq,
													 &raw_outcome);

			if (rc != 0)
				return artifact_refuse(reason, artifact_reason_for_outcome(raw_outcome));
			return rc;
		}
	}
	state = artifact_completed_attempt(tl, key, lsn, token, &completed);
	if (state < 0)
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_RECORD_UNREADABLE);
	if (state > 0)
	{
		unsigned char *durable = malloc(page_size);
		uint64_t	resolved = 0;
		int			identical;

		if (!durable)
			return artifact_refuse(reason, PS_ARTIFACT_REFUSE_INVALID);
		identical = read_resolve(tl, key, block, lsn, 0, durable, &resolved) == 1 &&
			resolved == lsn && memcmp(durable, page, page_size) == 0;
		if (!identical)
			fprintf(stderr, "pagestore: immutable artifact mismatch tl=%u class=%u db=%u object=%u block=%u lsn=%llu resolved=%llu\n",
				tl, key->klass, key->dbOid, key->relNumber, block,
				(unsigned long long) lsn, (unsigned long long) resolved);
		free(durable);
		if (identical && seq)
			*seq = token;
		return identical ? 0 : artifact_refuse(reason, PS_ARTIFACT_REFUSE_IMMUTABLE_MISMATCH);
	}
	if (block == UINT32_MAX || !artifact_attempt(tl, key, lsn, token))
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_ATTEMPT_MISMATCH);
	fork = fork_find(tl, key);
	if (!fork || fork->artifact_attempt_seq != token)
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_ATTEMPT_MISMATCH);
	if (append_page_raw_outcome(tl, key, block, page, lsn, seq, &outcome) != 0)
	{
		/* An ambiguous append must not escape the attempt's counters -- poison
		 * only when the append's outcome was a real storage I/O failure; an
		 * admission refusal (unfenced LSN, forkmeta cutoff) leaves no state
		 * behind and must not take the rest of the artifact path down. */
		if (outcome == PS_APPEND_IO_FAILED)
			__atomic_store_n(&artifact_io_failed, 1, __ATOMIC_RELEASE);
		return artifact_refuse(reason, artifact_reason_for_outcome(outcome));
	}
	entry = page_find(tl, key, block);
	if (entry->artifact_attempt_seq != token)
	{
		entry->artifact_attempt_seq = token;
		fork->artifact_page_count++;
	}
	if (block + 1 > fork->artifact_nblocks)
		fork->artifact_nblocks = block + 1;
	return 0;
}

int
ps_artifact_commit(uint32_t tl, const PsKey *key, uint64_t lsn,
				   uint64_t token, uint64_t count, PsArtifactRefuseReason *reason)
{
	ForkEnt    *fork;
	PsArtifactLifecycle record = {.begin_seq = token, .page_count = count,
	.state = PS_ARTIFACT_COMMITTED};
	PsArtifactLifecycle completed;
	int			state;

	if (reason)
		*reason = PS_ARTIFACT_REFUSE_NONE;
	if (__atomic_load_n(&artifact_io_failed, __ATOMIC_ACQUIRE))
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_POISONED);
	if (!core_process_valid() || tl >= MAX_TIMELINES)
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_INVALID);
	if (ps_storage->sync_needs_write_lock)
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_WRITE_LOCK);
	if (!artifact_data_key(key))
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_INVALID);
	state = artifact_completed_attempt(tl, key, lsn, token, &completed);
	if (state != 0)
	{
		if (state > 0 && count == completed.page_count)
			return 0;
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_ATTEMPT_MISMATCH);
	}
	if (!artifact_attempt(tl, key, lsn, token))
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_ATTEMPT_MISMATCH);
	fork = fork_find(tl, key);
	if (!fork || fork->artifact_attempt_seq != token ||
		fork->artifact_page_count != count)
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_ATTEMPT_MISMATCH);
	/* The attempt's pages are already appended, but no COMMIT record exists
	 * yet, so nothing is indexed as complete: there is no ambiguity about
	 * completion state for a failure here to poison over.  A retry re-runs
	 * this same sync -- it must succeed before the COMMIT record below can
	 * land -- so this is retryable and must not poison the store, unlike a
	 * failure recording the COMMIT record itself. */
	if (ps_storage->sync() != 0)
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_SYNC);
	record.nblocks = fork->artifact_nblocks;
	{
		PsAppendOutcome outcome;

		if (artifact_store_record(tl, key, PS_ARTIFACT_COMMIT_BLOCK, lsn, &record,
								  NULL, &outcome) != 0)
			return artifact_refuse(reason, artifact_reason_for_outcome(outcome));
	}
	return 0;
}

int
ps_artifact_drop(uint32_t tl, const PsKey *key, uint64_t lsn,
				 PsArtifactRefuseReason *reason)
{
	PsKey		meta;
	PageEnt    *e;
	PageVer    *last;
	PsArtifactLifecycle record = {.state = PS_ARTIFACT_DROPPED};

	if (reason)
		*reason = PS_ARTIFACT_REFUSE_NONE;
	if (__atomic_load_n(&artifact_io_failed, __ATOMIC_ACQUIRE))
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_POISONED);
	if (!core_process_valid() || tl >= MAX_TIMELINES)
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_INVALID);
	if (ps_storage->sync_needs_write_lock)
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_WRITE_LOCK);
	if (!artifact_data_key(key) || lsn == 0)
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_INVALID);
	if (!artifact_mutation_horizon(tl, key, lsn))
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_HORIZON);
	meta = artifact_meta_key(key);
	e = page_find(tl, &meta, PS_ARTIFACT_BEGIN_BLOCK);
	last = e ? page_visible(e, UINT64_MAX, 0) : NULL;
	if (last && lsn < last->lsn)
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_BEGIN_NEWER);
	e = page_find(tl, &meta, PS_ARTIFACT_COMMIT_BLOCK);
	last = e ? page_visible(e, UINT64_MAX, 0) : NULL;
	if (last && lsn < last->lsn)
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_OLDER_GENERATION);
	if (last && lsn == last->lsn)
	{
		PsArtifactLifecycle prior;

		if (artifact_record(tl, &meta, PS_ARTIFACT_COMMIT_BLOCK, last, &prior, 0) != 0)
			return artifact_refuse(reason, PS_ARTIFACT_REFUSE_RECORD_UNREADABLE);
		/* A same-LSN DROP cannot replace completed bytes seen by a branch. */
		if (prior.state == PS_ARTIFACT_DROPPED)
			return 0;
		return artifact_refuse(reason, PS_ARTIFACT_REFUSE_OLDER_GENERATION);
	}
	{
		PsAppendOutcome outcome;

		if (artifact_store_record(tl, key, PS_ARTIFACT_COMMIT_BLOCK, lsn, &record,
								  NULL, &outcome) != 0)
			return artifact_refuse(reason, artifact_reason_for_outcome(outcome));
	}
	return 0;
}

static int
artifact_validate_recovery(void)
{
	for (uint32_t sh = 0; sh < core_shards(); sh++)
		for (uint32_t bucket = 0; bucket < IDX_BUCKETS; bucket++)
			for (PageEnt *e = g_shards[sh].page_idx[bucket]; e; e = e->next)
			{
				if (e->key.klass != PS_KLASS_ARTIFACT)
					continue;
				if ((e->key.forkNum != PS_KLASS_SLRU && e->key.forkNum != PS_KLASS_READER_SNAPSHOT) ||
					e->block > PS_ARTIFACT_COMMIT_BLOCK)
					return -1;
				for (int i = 0; i < e->nver; i++)
				{
					PsArtifactLifecycle record;

					if (e->vers[i].lsn == 0 || artifact_record(e->timeline,
															   &e->key, e->block, &e->vers[i], &record, 0) != 0)
						return -1;
				}
			}
	return 0;
}
