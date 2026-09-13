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

/* Match page visibility for size/existence, including legacy and ancestry.
 * A zero-page COMMIT is an existing empty object; DROP is nonexistence. */
static int
artifact_metadata(uint32_t tl, const PsKey *key, uint64_t horizon,
				  uint64_t seq_cap, int *exists, uint32_t *nblocks)
{
	PsKey meta = artifact_meta_key(key);
	TlWalk walk = tl_walk_first(tl, horizon);
	int rc = 0;

	*exists = 0;
	*nblocks = 0;
	ps_lock_map_rd();
	do
	{
		PageEnt *commits = page_find(walk.tl, &meta, PS_ARTIFACT_COMMIT_BLOCK);
		PageEnt *begins = page_find(walk.tl, &meta, PS_ARTIFACT_BEGIN_BLOCK);
		uint64_t cap = walk.lsn == horizon ? seq_cap : 0;
		uint64_t lsn = walk.lsn;
		PageVer *commit = commits ? page_visible(commits, lsn, cap) : NULL;
		ForkEnt *fork = fork_find(walk.tl, key);
		uint32_t nb = 0;
		int state;

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
		/* BEGIN is monotone in LSN. Exclude all subsequent staged growth
		 * while resolving the legacy fork, including at the same LSN. */
		if (begins)
		{
			uint64_t first = artifact_legacy_seq(begins);

			for (int i = 0; i < begins->nver; i++)
				if (begins->vers[i].admission_seq == first)
				{
					if (lsn >= begins->vers[i].lsn)
					{
						if (lsn > begins->vers[i].lsn || cap == 0 || cap >= first)
							/* BEGIN itself has no data-fork event; using its
							 * nonzero sequence also handles the first store mutation. */
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

/*
 * Called with map-wr during compaction. Keep every selected complete interval
 * above the floor, the floor base and pinned views. Also keep the current
 * process's newest unfinished attempt; recovery makes old attempts abandoned.
 */
static int
artifact_prune_versions(uint32_t tl, const PsKey *key, uint32_t block,
						const PsPruneVersion *versions, uint32_t nversions, uint64_t floor,
						const PsPruneFence *fences, uint32_t nfences, unsigned char *keep)
{
	PsKey		data = *key;
	PsKey		meta;
	PageEnt    *begins,
			   *commits;
	PageVer    *begin;
	uint64_t	first;
	uint32_t nh;

	if (key->klass == PS_KLASS_ARTIFACT)
	{
		data.klass = (uint32_t) key->forkNum;
		data.forkNum = 0;
	}
	if (!artifact_data_key(&data) || floor == 0)
		return 0;
	meta = artifact_meta_key(&data);
	begins = page_find(tl, &meta, PS_ARTIFACT_BEGIN_BLOCK);
	commits = page_find(tl, &meta, PS_ARTIFACT_COMMIT_BLOCK);
	if (!begins && !commits)
		return 0;
	first = artifact_legacy_seq(begins);
	begin = begins ? page_visible(begins, UINT64_MAX, 0) : NULL;
	nh = 1 + nfences + (commits ? (uint32_t) commits->nver : 0);
	memset(keep, 0, nversions);
	for (uint32_t i = 0; i < nversions; i++)
	{
		const PsPruneVersion *v = &versions[i];

		if (key->klass == PS_KLASS_ARTIFACT && block == PS_ARTIFACT_BEGIN_BLOCK)
		{
			keep[i] = v->admission_seq == first ||
				(begin && v->admission_seq == begin->admission_seq);
			continue;
		}
		/* Keep a pending attempt until superseded or the daemon restarts. */
		if (key->klass != PS_KLASS_ARTIFACT && begin &&
			begin->admission_seq >= artifact_recovery_seq &&
			v->lsn == begin->lsn && v->admission_seq > begin->admission_seq)
		{
			PageVer    *last = commits ? page_visible(commits, UINT64_MAX, 0) : NULL;

			if (!last || last->admission_seq < begin->admission_seq)
				keep[i] = 1;
		}

		/*
		 * Every committed generation above the floor is an admissible view;
		 * below it only the floor and explicit retention fences are needed.
		 */
		for (uint32_t h = 0; h < nh && !keep[i]; h++)
		{
			uint64_t	horizon = h == 0 ? floor : h <= nfences ?
				fences[h - 1].lsn : commits->vers[h - nfences - 1].lsn;
			uint64_t	cap = h > 0 && h <= nfences ? fences[h - 1].admission_seq : 0;

			if (h > nfences && horizon <= floor)
				continue;
			if (key->klass == PS_KLASS_ARTIFACT)
			{
				PageVer    *c = commits ? page_visible(commits, horizon, cap) : NULL;

				if (c && c->lsn == v->lsn && c->admission_seq == v->admission_seq)
					keep[i] = 1;
			}
			else
			{
				PageVer    *selected = NULL;
				int			state = artifact_visible(tl, &data, block, horizon, cap,
													 &selected, 1);

				if (state < 0)
					return -1;
				if (selected && selected->lsn == v->lsn &&
					selected->admission_seq == v->admission_seq)
					keep[i] = 1;
			}
		}
		/* Legacy history above the floor can still have unenumerated views. */
		if (key->klass != PS_KLASS_ARTIFACT &&
			v->admission_seq < first && v->lsn > floor)
			keep[i] = 1;
	}
	return 1;
}

static int
artifact_store_record(uint32_t tl, const PsKey *key, uint32_t block,
					  uint64_t lsn, PsArtifactLifecycle *record, uint64_t *seq)
{
	PsKey		meta = artifact_meta_key(key);
	unsigned char *page = calloc(1, page_size);
	int			rc;

	if (!page)
		return -1;
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
	rc = append_page_raw(tl, &meta, block, page, lsn, seq);
	free(page);
	if (rc == 0)
		rc = ps_storage->sync();
	if (rc != 0)
		__atomic_store_n(&artifact_io_failed, 1, __ATOMIC_RELEASE);
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

int
ps_artifact_begin(uint32_t tl, const PsKey *key, uint64_t lsn, uint64_t *token)
{
	PsKey		meta;
	PageEnt    *e;
	PageVer    *last;
	PsArtifactLifecycle record = {0};

	if (__atomic_load_n(&artifact_io_failed, __ATOMIC_ACQUIRE) ||
		!core_process_valid() || tl >= MAX_TIMELINES || ps_storage->sync_needs_write_lock || !artifact_data_key(key) || lsn == 0 ||
		!artifact_mutation_horizon(tl, key, lsn))
		return -1;
	meta = artifact_meta_key(key);
	e = page_find(tl, &meta, PS_ARTIFACT_COMMIT_BLOCK);
	last = e ? page_visible(e, UINT64_MAX, 0) : NULL;
	if (last && (lsn < last->lsn ||
				 artifact_record(tl, &meta, PS_ARTIFACT_COMMIT_BLOCK, last, &record, 0) != 0 ||
				 (lsn == last->lsn && record.state == PS_ARTIFACT_DROPPED)))
		return -1;
	e = page_find(tl, &meta, PS_ARTIFACT_BEGIN_BLOCK);
	last = e ? page_visible(e, UINT64_MAX, 0) : NULL;
	if (last && lsn < last->lsn)
		return -1;
	memset(&record, 0, sizeof(record));
	if (artifact_store_record(tl, key, PS_ARTIFACT_BEGIN_BLOCK, lsn, &record, token) != 0)
		return -1;
	{
		ForkEnt *fork = fork_get_or_create(tl, key);

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

int
ps_artifact_write(uint32_t tl, const PsKey *key, uint32_t block,
				  const unsigned char *page, uint64_t lsn, uint64_t token, uint64_t *seq)
{
	ForkEnt *fork;
	PageEnt *entry;

	if (!artifact_data_key(key))
		return append_page_raw(tl, key, block, page, lsn, seq);
	if (__atomic_load_n(&artifact_io_failed, __ATOMIC_ACQUIRE))
		return -1;
	if (token == 0)
	{
		if (artifact_has_protocol(tl, key))
			return -1;
		return append_page_raw(tl, key, block, page, lsn, seq);
	}
	if (block == UINT32_MAX || !artifact_attempt(tl, key, lsn, token))
		return -1;
	fork = fork_find(tl, key);
	if (!fork || fork->artifact_attempt_seq != token)
		return -1;
	if (append_page_raw(tl, key, block, page, lsn, seq) != 0)
	{
		/* An ambiguous append must not escape the attempt's counters. */
		__atomic_store_n(&artifact_io_failed, 1, __ATOMIC_RELEASE);
		return -1;
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
				   uint64_t token, uint64_t count)
{
	ForkEnt    *fork;
	PsArtifactLifecycle record = {.begin_seq = token, .page_count = count,
	.state = PS_ARTIFACT_COMMITTED};

	if (__atomic_load_n(&artifact_io_failed, __ATOMIC_ACQUIRE) ||
		!core_process_valid() || tl >= MAX_TIMELINES || ps_storage->sync_needs_write_lock || !artifact_data_key(key) ||
		!artifact_attempt(tl, key, lsn, token))
		return -1;
	fork = fork_find(tl, key);
	if (!fork || fork->artifact_attempt_seq != token ||
		fork->artifact_page_count != count || ps_storage->sync() != 0)
		return -1;
	record.nblocks = fork->artifact_nblocks;
	return artifact_store_record(tl, key, PS_ARTIFACT_COMMIT_BLOCK, lsn, &record, NULL);
}

int
ps_artifact_drop(uint32_t tl, const PsKey *key, uint64_t lsn)
{
	PsKey		meta;
	PageEnt    *e;
	PageVer    *last;
	PsArtifactLifecycle record = {.state = PS_ARTIFACT_DROPPED};

	if (__atomic_load_n(&artifact_io_failed, __ATOMIC_ACQUIRE) ||
		!core_process_valid() || tl >= MAX_TIMELINES || ps_storage->sync_needs_write_lock || !artifact_data_key(key) || lsn == 0 ||
		!artifact_mutation_horizon(tl, key, lsn))
		return -1;
	meta = artifact_meta_key(key);
	e = page_find(tl, &meta, PS_ARTIFACT_BEGIN_BLOCK);
	last = e ? page_visible(e, UINT64_MAX, 0) : NULL;
	if (last && lsn < last->lsn)
		return -1;
	e = page_find(tl, &meta, PS_ARTIFACT_COMMIT_BLOCK);
	last = e ? page_visible(e, UINT64_MAX, 0) : NULL;
	if (last && lsn < last->lsn)
		return -1;
	if (last && lsn == last->lsn)
	{
		PsArtifactLifecycle prior;

		if (artifact_record(tl, &meta, PS_ARTIFACT_COMMIT_BLOCK, last, &prior, 0) != 0)
			return -1;
		if (prior.state == PS_ARTIFACT_DROPPED)
			return 0;
	}
	return artifact_store_record(tl, key, PS_ARTIFACT_COMMIT_BLOCK, lsn, &record, NULL);
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
