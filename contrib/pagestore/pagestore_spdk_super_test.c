/*
 * pagestore_spdk_super_test.c
 *	  Unit test for the SPDK store superblock: v2 round trip, the accepted
 *	  legacy images, every fail-closed rejection, and durable publication.
 *	  Freestanding: no SPDK, no daemon.
 */
#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pagestore_spdk_super.h"

static int checks;

static void
check(bool condition, const char *message)
{
	checks++;
	if (!condition)
	{
		fprintf(stderr, "FAIL: %s\n", message);
		_exit(1);
	}
}

static void
put32(unsigned char *p, uint32_t v)
{
	memcpy(p, &v, 4);
}

static void
put64(unsigned char *p, uint64_t v)
{
	memcpy(p, &v, 8);
}

/* the sharded struct image storage_spdk.c used to fwrite(), version word 1 */
static size_t
legacy_sharded_image(unsigned char *buf, uint32_t sector, uint64_t segsize,
				uint32_t nshards, const uint32_t *counts)
{
	memset(buf, 0, 544);
	put32(buf, PS_SPDK_SUPER_MAGIC);
	put32(buf + 4, 1);
	put32(buf + 8, sector);
	put64(buf + 16, segsize);
	put32(buf + 24, nshards);
	for (uint32_t i = 0; i < nshards; i++)
		put32(buf + 28 + 4 * i, counts[i]);
	return 544;
}

/* the single-shard struct image: no version word */
static size_t
legacy_single_image(unsigned char *buf, uint32_t sector, uint64_t segsize,
				uint32_t count)
{
	memset(buf, 0, 24);
	put32(buf, PS_SPDK_SUPER_MAGIC);
	put32(buf + 4, sector);
	put64(buf + 8, segsize);
	put32(buf + 16, count);
	return 24;
}

static bool
file_exists(const char *path)
{
	struct stat st;

	return stat(path, &st) == 0;
}

int
main(void)
{
	const uint32_t sector = 512;
	const uint64_t segsize = 1 << 20;
	unsigned char buf[1024];
	uint32_t	counts[PS_SPDK_SUPER_MAX_SHARDS];
	uint32_t	out[PS_SPDK_SUPER_MAX_SHARDS];
	uint32_t	version = 0;
	size_t		len;
	char		dir[] = "/tmp/pagestore-spdk-super-XXXXXX";
	char		path[4200];
	char		tmp[4200];

	/* --- v2 encode/decode round trip --------------------------------- */
	for (uint32_t i = 0; i < 4; i++)
		counts[i] = 100 + i;
	len = ps_spdk_super_encode(buf, sector, segsize, 4, counts);
	check(len == 28 + 16 + 4, "v2 length is fixed + 4 per shard + crc");
	memset(out, 0xff, sizeof(out));
	check(ps_spdk_super_decode(buf, len, sector, segsize, 4, out, &version) == PS_SPDK_SUPER_OK,
		  "v2 image decodes");
	check(version == PS_SPDK_SUPER_VERSION, "v2 reports its version");
	check(memcmp(out, counts, 16) == 0, "v2 counts survive the round trip");
	check(ps_spdk_super_decode(buf, len + 1, sector, segsize, 4, out, NULL) == PS_SPDK_SUPER_OVERLONG,
		  "a byte beyond the announced length refuses the superblock");
	check(ps_spdk_super_encode(buf, sector, segsize, 0, counts) == 0,
		  "zero shards cannot be encoded");
	check(ps_spdk_super_encode(buf, sector, segsize, PS_SPDK_SUPER_MAX_SHARDS + 1, counts) == 0,
		  "too many shards cannot be encoded");

	/* --- fail-closed rejections ---------------------------------------- */
	len = ps_spdk_super_encode(buf, sector, segsize, 4, counts);
	check(ps_spdk_super_decode(buf, len - 1, sector, segsize, 4, out, NULL) == PS_SPDK_SUPER_TRUNCATED,
		  "a v2 image one byte short is truncated");
	check(ps_spdk_super_decode(buf, 7, sector, segsize, 4, out, NULL) == PS_SPDK_SUPER_TRUNCATED,
		  "fewer than eight bytes is truncated");
	check(ps_spdk_super_decode(buf, 20, sector, segsize, 4, out, NULL) == PS_SPDK_SUPER_TRUNCATED,
		  "a v2 image cut inside the fixed header is truncated");
	buf[30] ^= 0x01;
	check(ps_spdk_super_decode(buf, len, sector, segsize, 4, out, NULL) == PS_SPDK_SUPER_CORRUPT,
		  "a flipped count byte fails the checksum");
	buf[30] ^= 0x01;
	buf[len - 1] ^= 0x80;
	check(ps_spdk_super_decode(buf, len, sector, segsize, 4, out, NULL) == PS_SPDK_SUPER_CORRUPT,
		  "a flipped checksum byte is corrupt");
	buf[len - 1] ^= 0x80;
	check(ps_spdk_super_decode(buf, len, sector, segsize, 4, out, NULL) == PS_SPDK_SUPER_OK,
		  "the image is intact again");
	{
		unsigned char copy[1024];

		memcpy(copy, buf, len);
		put32(copy + 8, (uint32_t) len + 4);
		check(ps_spdk_super_decode(copy, len, sector, segsize, 4, out, NULL) != PS_SPDK_SUPER_OK,
			  "a length that disagrees with the shard count is rejected");
		memcpy(copy, buf, len);
		put32(copy + 4, 3);
		check(ps_spdk_super_decode(copy, len, sector, segsize, 4, out, NULL) == PS_SPDK_SUPER_NEWER,
			  "a newer version is refused, not decoded as something else");
		memcpy(copy, buf, len);
		put32(copy + 4, 0);
		check(ps_spdk_super_decode(copy, len, sector, segsize, 4, out, NULL) == PS_SPDK_SUPER_CORRUPT,
			  "a zero version word matches no layout and is corrupt");
		memcpy(copy, buf, len);
		put32(copy + 4, 511);
		check(ps_spdk_super_decode(copy, len, sector, segsize, 4, out, NULL) == PS_SPDK_SUPER_NEWER,
			  "the largest version word below a sector size is still a version");
		memcpy(copy, buf, len);
		put32(copy, 0x41424344);
		check(ps_spdk_super_decode(copy, len, sector, segsize, 4, out, NULL) == PS_SPDK_SUPER_BAD_MAGIC,
			  "a foreign magic is not a superblock");
	}
	check(ps_spdk_super_decode(buf, len, 4096, segsize, 4, out, NULL) == PS_SPDK_SUPER_GEOMETRY,
		  "a different sector size refuses the superblock");
	check(ps_spdk_super_decode(buf, len, sector, segsize * 2, 4, out, NULL) == PS_SPDK_SUPER_GEOMETRY,
		  "a different segment size refuses the superblock");
	check(ps_spdk_super_decode(buf, len, sector, segsize, 2, out, NULL) == PS_SPDK_SUPER_SHARDS,
		  "fewer configured shards than recorded refuse the superblock");
	check(ps_spdk_super_decode(buf, len, sector, segsize, 8, out, NULL) == PS_SPDK_SUPER_SHARDS,
		  "more configured shards than recorded refuse the superblock");
	check(ps_spdk_super_decode(buf, len, sector, segsize, 0, out, NULL) == PS_SPDK_SUPER_SHARDS,
		  "zero configured shards is refused");

	/* --- accepted legacy layouts --------------------------------------- */
	len = legacy_sharded_image(buf, sector, segsize, 3, counts);
	memset(out, 0xff, sizeof(out));
	check(ps_spdk_super_decode(buf, len, sector, segsize, 3, out, &version) == PS_SPDK_SUPER_OK,
		  "a sharded struct image decodes");
	check(version == PS_SPDK_SUPER_LEGACY_SHARDED && memcmp(out, counts, 12) == 0,
		  "the sharded struct reports version word 1 and its counts");
	check(ps_spdk_super_decode(buf, len - 1, sector, segsize, 3, out, NULL) == PS_SPDK_SUPER_TRUNCATED,
		  "a short sharded image is truncated");
	check(ps_spdk_super_decode(buf, len + 1, sector, segsize, 3, out, NULL) == PS_SPDK_SUPER_OVERLONG,
		  "a sharded image with a trailing byte is overlong");
	check(ps_spdk_super_decode(buf, len, sector, segsize, 2, out, NULL) == PS_SPDK_SUPER_SHARDS,
		  "a sharded image for another shard count is refused rather than sliced");
	check(ps_spdk_super_decode(buf, len, 4096, segsize, 3, out, NULL) == PS_SPDK_SUPER_GEOMETRY,
		  "a sharded image for another geometry is refused");

	len = legacy_single_image(buf, sector, segsize, 77);
	memset(out, 0xff, sizeof(out));
	check(ps_spdk_super_decode(buf, len, sector, segsize, 1, out, &version) == PS_SPDK_SUPER_OK,
		  "a single-shard struct image decodes for one shard");
	check(version == PS_SPDK_SUPER_LEGACY_SINGLE && out[0] == 77, "the single-shard struct reports identity 0 and its count");
	check(ps_spdk_super_decode(buf, len, sector, segsize, 2, out, NULL) == PS_SPDK_SUPER_SHARDS,
		  "a single-shard image cannot serve a sharded store");
	check(ps_spdk_super_decode(buf, len - 1, sector, segsize, 1, out, NULL) == PS_SPDK_SUPER_TRUNCATED,
		  "a short single-shard image is truncated");
	check(ps_spdk_super_decode(buf, len + 1, sector, segsize, 1, out, NULL) == PS_SPDK_SUPER_OVERLONG,
		  "a single-shard image with a trailing byte is overlong");
	len = legacy_single_image(buf, 4096, segsize, 77);
	check(ps_spdk_super_decode(buf, len, sector, segsize, 1, out, NULL) == PS_SPDK_SUPER_GEOMETRY,
		  "a single-shard image for another sector size is refused");

	/* --- read and durable publication ----------------------------------- */
	check(mkdtemp(dir) != NULL, "temporary store directory");
	snprintf(path, sizeof(path), "%s/%s", dir, PS_SPDK_SUPER_FILE);
	snprintf(tmp, sizeof(tmp), "%s/%s.tmp", dir, PS_SPDK_SUPER_FILE);

	check(ps_spdk_super_read(dir, sector, segsize, 4, out, NULL) == PS_SPDK_SUPER_ABSENT,
		  "a store without a superblock reads as absent");

	check(ps_spdk_super_publish(dir, sector, segsize, 4, counts) == 0, "publication succeeds");
	check(file_exists(path) && !file_exists(tmp), "publication renames the temporary file into place");
	memset(out, 0xff, sizeof(out));
	check(ps_spdk_super_read(dir, sector, segsize, 4, out, &version) == PS_SPDK_SUPER_OK,
		  "the published superblock reads back");
	check(version == PS_SPDK_SUPER_VERSION && memcmp(out, counts, 16) == 0,
		  "the published superblock is v2 with the published counts");

	/* a legacy superblock is read, then replaced by v2 at the next publication */
	{
		FILE	   *f = fopen(path, "wb");
		uint32_t	legacy[3] = {5, 6, 7};

		check(f != NULL, "rewrite the superblock as the legacy sharded struct");
		len = legacy_sharded_image(buf, sector, segsize, 3, legacy);
		check(fwrite(buf, 1, len, f) == len && fclose(f) == 0, "legacy image written");
		check(ps_spdk_super_read(dir, sector, segsize, 3, out, &version) == PS_SPDK_SUPER_OK &&
			  version == PS_SPDK_SUPER_LEGACY_SHARDED && out[2] == 7,
			  "the legacy superblock reads back with version word 1");
		check(ps_spdk_super_read(dir, sector, segsize, 4, out, NULL) == PS_SPDK_SUPER_SHARDS,
			  "opening a legacy sharded store with another shard count fails closed");
		legacy[2] = 8;
		check(ps_spdk_super_publish(dir, sector, segsize, 3, legacy) == 0, "republish after legacy");
		check(ps_spdk_super_read(dir, sector, segsize, 3, out, &version) == PS_SPDK_SUPER_OK &&
			  version == PS_SPDK_SUPER_VERSION && out[2] == 8,
			  "the next publication migrates the superblock to v2");
	}

	/* a damaged file on disk fails the read closed */
	{
		FILE	   *f = fopen(path, "r+b");

		check(f != NULL && fseek(f, 30, SEEK_SET) == 0 && fputc(0x5a, f) != EOF && fclose(f) == 0,
			  "damage a count byte on disk");
		check(ps_spdk_super_read(dir, sector, segsize, 3, out, NULL) == PS_SPDK_SUPER_CORRUPT,
			  "a damaged superblock on disk is corrupt");
		check(truncate(path, 10) == 0, "truncate the superblock on disk");
		check(ps_spdk_super_read(dir, sector, segsize, 3, out, NULL) == PS_SPDK_SUPER_TRUNCATED,
			  "a truncated superblock on disk is truncated");
		check(truncate(path, 0) == 0, "empty the superblock on disk");
		check(ps_spdk_super_read(dir, sector, segsize, 3, out, NULL) == PS_SPDK_SUPER_TRUNCATED,
			  "an empty superblock is truncated, not absent");
		check(ps_spdk_super_publish(dir, sector, segsize, 3, out) == 0, "republish a valid superblock");
		check(truncate(path, 28 + 12 + 4 + 1) == 0, "append one byte to the superblock on disk");
		check(ps_spdk_super_read(dir, sector, segsize, 3, out, NULL) == PS_SPDK_SUPER_OVERLONG,
			  "a superblock file with a trailing byte is overlong");
		check(truncate(path, 4096) == 0, "grow the superblock past every layout and the read buffer");
		check(ps_spdk_super_read(dir, sector, segsize, 3, out, NULL) == PS_SPDK_SUPER_OVERLONG,
			  "a superblock file longer than the read buffer is overlong, not silently cut");
	}

	/* a failed publication leaves the previous superblock untouched */
	{
		uint32_t	before[3];
		uint32_t	after[3];
		uint32_t	newer[3] = {9, 9, 9};
		uint32_t	prior[3] = {1, 2, 3};

		check(ps_spdk_super_publish(dir, sector, segsize, 3, prior) == 0, "publish a known state");
		check(ps_spdk_super_read(dir, sector, segsize, 3, before, NULL) == PS_SPDK_SUPER_OK, "read it");
		check(chmod(dir, 0555) == 0, "make the store directory read-only");
		if (geteuid() != 0)
		{
			check(ps_spdk_super_publish(dir, sector, segsize, 3, newer) == -EACCES,
				  "publication into an unwritable directory reports -EACCES");
			check(!file_exists(tmp), "no temporary file is left behind");
		}
		check(chmod(dir, 0755) == 0, "restore the directory mode");
		check(ps_spdk_super_read(dir, sector, segsize, 3, after, NULL) == PS_SPDK_SUPER_OK &&
			  memcmp(before, after, sizeof(before)) == 0,
			  "the previous superblock survives a failed publication");
		check(ps_spdk_super_publish("/nonexistent/pagestore-spdk-super", sector, segsize, 3, newer) == -ENOENT,
			  "publication into a missing directory reports -ENOENT");
		check(ps_spdk_super_publish(dir, sector, segsize, 0, newer) == -EINVAL,
			  "publication of zero shards is invalid");
	}

	/* identities */
	{
		const PsFormatIdentity *ids = NULL;
		size_t		n = ps_storage_spdk_format_identities(&ids);
		bool		current = false;

		check(n == 4, "four SPDK container identities");
		for (size_t i = 0; i < n; i++)
			if (strcmp(ids[i].artifact, "spdk_super") == 0)
				current = ids[i].magic == PS_SPDK_SUPER_MAGIC &&
					ids[i].version == PS_SPDK_SUPER_VERSION;
		check(current, "the current superblock identity is v2");
	}

	unlink(path);
	unlink(tmp);
	rmdir(dir);
	printf("pagestore_spdk_super: %d checks, 0 failures\n", checks);
	return 0;
}
