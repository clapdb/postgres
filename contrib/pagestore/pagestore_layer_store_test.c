/*-------------------------------------------------------------------------
 *
 * pagestore_layer_store_test.c
 *    Standalone tests for the filesystem-backed immutable layer object tier.
 *
 *-------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "pagestore_layer_store.h"
#include "pagestore_store_owner.h"

static int run = 0,
			failed = 0;

static void
check(int cond, const char *msg)
{
	run++;
	if (!cond)
	{
		failed++;
		fprintf(stderr, "  FAIL: %s\n", msg);
	}
}

static int
file_matches(const char *path, const char *want)
{
	char		buf[128];
	int			fd;
	ssize_t		n;

	fd = open(path, O_RDONLY);
	if (fd < 0)
		return 0;
	n = read(fd, buf, sizeof(buf));
	close(fd);
	return n == (ssize_t) strlen(want) && memcmp(buf, want, (size_t) n) == 0;
}

int
main(void)
{
	char		local_dir[] = "/tmp/pslayerstorelocalXXXXXX";
	char		other_local_dir[] = "/tmp/pslayerstoreotherXXXXXX";
	char		object_dir[] = "/tmp/pslayerstoreobjectXXXXXX";
	char		owner_path[sizeof(object_dir) + 32];
	char		stale_path[sizeof(object_dir) + 64];
	char		retained_path[sizeof(local_dir) + 64];
	char		configured_object_dir[sizeof(object_dir) + 2];
	char		expected_remote_uri[PS_LAYER_URI_MAX];
	char		local_uri[PS_LAYER_URI_MAX];
	char		remote_uri[PS_LAYER_URI_MAX];
	const char *contents = "sealed layer object bytes";
	const char *corrupt = "bad";
	PsLayerDesc layer;
	PsLayerDesc remote_only;

	if (mkdtemp(local_dir) == NULL || mkdtemp(other_local_dir) == NULL ||
		mkdtemp(object_dir) == NULL)
	{
		fprintf(stderr, "setup failed\n");
		return 2;
	}
	check(setenv("PAGESTORE_OBJECT_DIR", local_dir, 1) == 0 &&
		  ps_layer_store->open(local_dir) != 0,
		  "reject an object directory that aliases the local store");
	ps_layer_store->close();
	check(setenv("PAGESTORE_OBJECT_DIR", object_dir, 1) == 0 &&
		  ps_layer_store->open(local_dir) == 0,
		  "open exclusive object directory");
	ps_layer_store->close();
	snprintf(stale_path, sizeof(stale_path), "%s/layer_3_0003000000000011.tmp.999999.0",
			 object_dir);
	{
		int fd = open(stale_path, O_WRONLY | O_CREAT | O_EXCL, 0600);

		if (fd >= 0)
			close(fd);
		check(fd >= 0, "create interrupted-copy temporary");
	}
	snprintf(retained_path, sizeof(retained_path),
			 "%s/layer_3_0003000000000012.tmp.%ld.0", local_dir,
			 (long) getpid());
	{
		int fd = open(retained_path, O_WRONLY | O_CREAT | O_EXCL, 0600);

		check(fd >= 0 && close(fd) == 0,
			  "create a copy temporary with the current PID");
	}
	snprintf(configured_object_dir, sizeof(configured_object_dir), "%s/", object_dir);
	check(setenv("PAGESTORE_OBJECT_DIR", configured_object_dir, 1) == 0 &&
		  ps_layer_store->open(local_dir) == 0 && access(stale_path, F_OK) != 0 &&
		  access(retained_path, F_OK) == 0,
		  "canonicalize object directory and reap interrupted copies at startup");
	ps_layer_store->close();
	check(ps_layer_store->open(other_local_dir) != 0,
		  "reject object directory owned by another store");
	ps_layer_store->close();
	if (ps_layer_store->open(local_dir) != 0)
	{
		fprintf(stderr, "could not reopen object directory\n");
		return 2;
	}
	{
		PsLayerMap empty;

		ps_layer_map_init(&empty);
		check(ps_layer_store->recover_local_layers(&empty) == 0 &&
			  access(retained_path, F_OK) == 0,
			  "recovery skips a live-PID copy temporary by its known grammar");
		ps_layer_map_free(&empty);
	}
	{
		const uint64_t fork_layer_id = (3ULL << 48) | 16;
		const uint64_t child_layer_id = (3ULL << 48) | 26;
		char		fork_layer_uri[PS_LAYER_URI_MAX];
		char		child_layer_uri[PS_LAYER_URI_MAX];
		pid_t		pid;
		int		status = 0;

		fork_layer_uri[0] = '\0';
		check(ps_layer_store->create_local_layer(fork_layer_id,
											 fork_layer_uri,
											 sizeof(fork_layer_uri)) == 0,
				  "create a provider layer before fork");
		snprintf(child_layer_uri, sizeof(child_layer_uri),
				 "%s/layer_3_%016llx", local_dir,
				 (unsigned long long) child_layer_id);
		pid = fork();
		if (pid == 0)
		{
			char		child_uri[PS_LAYER_URI_MAX];
			int		open_rc;
			int		open_errno;
			int		create_rc;
			int		write_rc;

			open_rc = ps_layer_store->open(local_dir);
			open_errno = errno;
			create_rc = ps_layer_store->create_local_layer(child_layer_id,
											 child_uri,
											 sizeof(child_uri));
			write_rc = ps_layer_store->write_local_layer(fork_layer_id,
											 "child", 5);
			_exit(open_rc != 0 && open_errno == ECHILD &&
					create_rc != 0 && write_rc != 0 ? 0 : 1);
		}
		check(pid > 0 && waitpid(pid, &status, 0) == pid &&
			  WIFEXITED(status) && WEXITSTATUS(status) == 0 &&
			  access(child_layer_uri, F_OK) != 0,
			  "forked provider rejects inherited-owner reopen and mutations");
		if (fork_layer_uri[0] != '\0')
			unlink(fork_layer_uri);
		unlink(child_layer_uri);
	}
	{
		PsStoreOwner *owner1 = NULL;
		PsStoreOwner *owner2 = NULL;
		char		canonical[4096];

		check(realpath(local_dir, canonical) != NULL &&
			  ps_store_owner_acquire(local_dir, &owner1) == 0 &&
			  ps_store_owner_acquire(local_dir, &owner2) == 0 &&
			  strcmp(ps_store_owner_root(owner1), canonical) == 0 &&
			  strcmp(ps_store_owner_root(owner2), canonical) == 0,
			  "same-process owner references share the canonical lease");
		ps_store_owner_release(owner1);
		ps_store_owner_release(owner2);
	}
	{
		PsStoreOwner *held = NULL;
		PsStoreOwner *after = NULL;
		pid_t pid;
		int status = 0;

		check(ps_store_owner_acquire(local_dir, &held) == 0,
			  "hold owner lease across fork test");
		pid = fork();
		if (pid == 0)
		{
			PsStoreOwner *child = NULL;
			int rc;

			/* This inherited handle is not a child lease; release must not
			 * decrement the parent's refcount or unlock its flock. */
			ps_store_owner_release(held);
			errno = 0;
			rc = ps_store_owner_acquire(local_dir, &child);
			if (child != NULL)
				ps_store_owner_release(child);
			_exit(rc != 0 && (errno == EWOULDBLOCK || errno == EAGAIN) ? 0 : 1);
		}
		check(pid > 0 && waitpid(pid, &status, 0) == pid &&
			  WIFEXITED(status) && WEXITSTATUS(status) == 0,
			  "forked child cannot reuse inherited owner lease");
		ps_store_owner_release(held);
		check(ps_store_owner_acquire(local_dir, &after) == 0,
			  "owner lease is reacquirable after parent release");
		ps_store_owner_release(after);
	}

	memset(&layer, 0, sizeof(layer));
	layer.layer_id = (3ULL << 48) | 17;
	layer.location_count = 1;
	layer.locations[0].tier = PS_LAYER_TIER_LOCAL_HOT;
	layer.locations[0].available = true;
	check(ps_layer_store->create_local_layer(layer.layer_id, local_uri,
												 sizeof(local_uri)) == 0,
		  "create local layer");
	check(ps_layer_store->write_local_layer(layer.layer_id, contents,
												strlen(contents)) == 0 &&
		  ps_layer_store->seal_local_layer(layer.layer_id) == 0,
		  "write and seal local layer");
	snprintf(layer.locations[0].uri, sizeof(layer.locations[0].uri), "%s", local_uri);
	layer.locations[0].size = strlen(contents);
	{
		char		child_buf[64];
		pid_t		pid;
		int		status = 0;

		pid = fork();
		if (pid == 0)
		{
			int read_rc;
			int read_errno;

			errno = 0;
			read_rc = ps_layer_store->read_layer_block(&layer, 0,
										 child_buf, strlen(contents));
			read_errno = errno;
			_exit(read_rc != 0 && read_errno == ECHILD ? 0 : 1);
		}
		check(pid > 0 && waitpid(pid, &status, 0) == pid &&
			  WIFEXITED(status) && WEXITSTATUS(status) == 0,
			  "forked provider rejects inherited-owner reads");
	}

	check(ps_layer_store->remote_uri(layer.layer_id, remote_uri,
												 sizeof(remote_uri)) == 0,
		  "derive remote object URI");
	snprintf(expected_remote_uri, sizeof(expected_remote_uri),
			 "%s/layer_3_0003000000000011", object_dir);
	check(strcmp(remote_uri, expected_remote_uri) == 0,
		  "derive canonical remote object URI");
	layer.locations[0].size++;
	check(ps_layer_store->upload_layer(&layer) != 0,
		  "reject upload whose source size differs from layer metadata");
	layer.locations[0].size--;
	check(ps_layer_store->upload_layer(&layer) == 0,
		  "upload local layer atomically");
	check(ps_layer_store->upload_layer(&layer) == 0,
		  "re-upload matching object is idempotent");
	{
		static unsigned char image_page[PS_DEFAULT_PAGE_SIZE];
		PsKey		key = {1, 1, 16000, 0, PS_KLASS_RELATION};
		PsImgRec	rec = {.key = key, .block = 0, .lsn = 1,
						   .page = image_page};
		PsLayerDesc image;
		char		image_remote_uri[PS_LAYER_URI_MAX];
		int			fd;

		memset(image_page, 0x5a, sizeof(image_page));
		check(ps_image_layer_write((3ULL << 48) | 18, 0, &rec, 1,
								   PS_DEFAULT_PAGE_SIZE, &image) == 0,
			  "write image layer for upload verification");
		check(ps_layer_store->remote_uri(image.layer_id, image_remote_uri,
										 sizeof(image_remote_uri)) == 0,
			  "derive image upload verification remote URI");
		fd = open(image.locations[0].uri, O_RDWR);
		check(fd >= 0 && pwrite(fd, "x", 1, 0) == 1 && close(fd) == 0,
			  "simulate same-size local image layer corruption");
		check(ps_layer_store->upload_layer(&image) != 0,
			  "reject upload whose image checksum differs from metadata");
		unlink(image.locations[0].uri);
		unlink(image_remote_uri);
	}
	layer.locations[1].tier = PS_LAYER_TIER_REMOTE_OBJECT;
	layer.locations[1].available = true;
	layer.locations[1].size = layer.locations[0].size;
	snprintf(layer.locations[1].uri, sizeof(layer.locations[1].uri), "%s", remote_uri);
	layer.location_count = 2;
	check(ps_layer_store->layer_exists_remote(&layer) == 1 &&
		  file_matches(remote_uri, contents),
		  "uploaded object is present and complete");
	check(ps_layer_store->write_local_layer(layer.layer_id, corrupt,
											strlen(corrupt)) == 0,
		  "simulate corrupt manifest-owned local layer");
	check(ps_layer_store->refresh_layer_cache(&layer) != 0,
		  "do not replace local layer before remote durability is recorded");
	layer.remote_durable = true;
	check(ps_layer_store->refresh_layer_cache(&layer) == 0 &&
		  file_matches(local_uri, contents),
		  "refresh repairs remote-durable local layer");

	check(unlink(local_uri) == 0, "simulate local layer eviction");
	check(ps_layer_store->download_layer(&layer) == 0 &&
		  file_matches(local_uri, contents),
		  "download restores the complete local layer");
	remote_only = layer;
	remote_only.location_count = 1;
	remote_only.locations[0] = layer.locations[1];
	check(ps_layer_store->write_local_layer(layer.layer_id, corrupt,
											strlen(corrupt)) == 0,
		  "simulate corrupt remote-only cache");
	check(ps_layer_store->refresh_layer_cache(&remote_only) == 0 &&
		  file_matches(local_uri, contents),
		  "refresh replaces corrupt remote-only cache");
	check(ps_layer_store->delete_remote_layer(&layer) == 0,
		  "delete remote object");
	check(ps_layer_store->delete_remote_layer(&layer) == 0 &&
		  ps_layer_store->layer_exists_remote(&layer) == 0,
		  "remote delete is idempotent");
	{
		PsLayerMap map;
		char		orphan[PS_LAYER_URI_MAX];
		char		protected_path[PS_LAYER_URI_MAX];
		char		bad_name[PS_LAYER_URI_MAX];
		char		hard_name[PS_LAYER_URI_MAX];
		int		fd;
		uint64_t	orphan_id = (3ULL << 48) | 19;
		uint64_t	bad_id = (3ULL << 48) | 20;
		uint64_t	protected_id = (3ULL << 48) | 5;
		PsLayerDesc protected_layer;

		ps_layer_map_init(&map);
		check(ps_layer_map_add(&map, &layer) == 0,
			  "build recovery reference map");
		memset(&protected_layer, 0, sizeof(protected_layer));
		protected_layer.layer_id = protected_id;
		check(ps_layer_map_add(&map, &protected_layer) == 0,
			  "add an out-of-order manifest layer ID");
		check(ps_layer_store->create_local_layer(protected_id, protected_path,
										 sizeof(protected_path)) == 0 &&
			  ps_layer_store->recover_local_layers(&map) == 0 &&
			  access(protected_path, F_OK) == 0,
			  "sorted manifest IDs protect a referenced layer");
		unlink(protected_path);
		/* The recovery corruption case models a live, not-yet-remote-durable
		 * reference; test remote-durable/unavailable semantics separately below. */
		map.layers[0].remote_durable = false;
		check(ps_layer_store->create_local_layer(orphan_id, orphan,
									 sizeof(orphan)) == 0 &&
			  ps_layer_store->write_local_layer(orphan_id, contents,
									 strlen(contents)) == 0,
			  "create an unreferenced canonical layer");
		check(ps_layer_store->recover_local_layers(&map) == 0 &&
			  access(orphan, F_OK) != 0 && access(local_uri, F_OK) == 0,
			  "reconcile removes only unreferenced canonical layers");
		check(ps_layer_store->recover_local_layers(&map) == 0,
			  "reconciliation is retry-safe after a completed sweep");

		snprintf(bad_name, sizeof(bad_name), "%s/layer_3_0003000000000014.bad",
				 local_dir);
		fd = open(bad_name, O_WRONLY | O_CREAT | O_EXCL, 0600);
		if (fd >= 0)
			close(fd);
		check(fd >= 0 && ps_layer_store->create_local_layer(bad_id, orphan,
									 sizeof(orphan)) == 0 &&
			  ps_layer_store->recover_local_layers(&map) != 0 &&
			  access(bad_name, F_OK) == 0 && access(orphan, F_OK) == 0,
			  "malformed layer namespace fails closed without unlinking");
		unlink(bad_name);
		unlink(orphan);

		snprintf(hard_name, sizeof(hard_name), "%s/layer_3_0003000000000015",
				 local_dir);
		fd = link(local_uri, hard_name);
		check(fd == 0 && ps_layer_store->recover_local_layers(&map) != 0 &&
			  access(hard_name, F_OK) == 0,
			  "hard-linked canonical layer fails closed");
		if (fd == 0)
			unlink(hard_name);
		check(ps_layer_store->recover_local_layers(&map) == 0,
			  "hard-link rejection can be retried safely");
		check(truncate(local_uri, 1) == 0,
			  "prepare a size-corrupt referenced layer");
		check(ps_layer_store->create_local_layer((3ULL << 48) | 21, orphan,
									 sizeof(orphan)) == 0 &&
			  ps_layer_store->recover_local_layers(&map) != 0 &&
			  access(orphan, F_OK) == 0,
			  "size-corrupt live layer fails closed");
		fd = open(local_uri, O_WRONLY | O_TRUNC);
		if (fd >= 0)
		{
			ssize_t nw = write(fd, contents, strlen(contents));
			int close_rc = close(fd);

			check(nw == (ssize_t) strlen(contents) && close_rc == 0,
				  "restore the referenced layer after corruption");
		}
		else
			check(0, "open the referenced layer for restoration");
		check(unlink(local_uri) == 0,
			  "remove referenced local layer for deleting recovery");
		map.layers[0].deleting = true;
		check(ps_layer_store->recover_local_layers(&map) == 0 &&
			  access(orphan, F_OK) != 0 && access(local_uri, F_OK) != 0,
			  "missing deleting layer does not block retry cleanup");
		map.layers[0].deleting = false;
		map.layers[0].remote_durable = true;
		map.layers[0].locations[0].available = false;
		check(ps_layer_store->create_local_layer(layer.layer_id, local_uri,
									 sizeof(local_uri)) == 0 &&
			  ps_layer_store->write_local_layer(layer.layer_id, contents,
									 strlen(contents)) == 0 &&
			  ps_layer_store->create_local_layer((3ULL << 48) | 22, orphan,
									 sizeof(orphan)) == 0 &&
			  ps_layer_store->recover_local_layers(&map) == 0 &&
			  access(local_uri, F_OK) == 0 && access(orphan, F_OK) != 0,
			  "unavailable remote-durable reference remains protected");
		check(truncate(local_uri, 1) == 0 &&
			  ps_layer_store->create_local_layer((3ULL << 48) | 24, orphan,
									 sizeof(orphan)) == 0 &&
			  ps_layer_store->recover_local_layers(&map) != 0 &&
			  access(orphan, F_OK) == 0 &&
			  ps_layer_store->write_local_layer(layer.layer_id, contents,
									 strlen(contents)) == 0 &&
			  ps_layer_store->recover_local_layers(&map) == 0 &&
			  access(orphan, F_OK) != 0,
			  "present corrupt remote-durable cache fails closed");
		map.layers[0].remote_durable = false;
		map.layers[0].locations[0].available = true;
		map.layers[0].location_count = 2;
		snprintf(map.layers[0].locations[1].uri,
				 sizeof(map.layers[0].locations[1].uri), "%s/not-canonical",
				 local_dir);
		map.layers[0].locations[1].tier = PS_LAYER_TIER_LOCAL_COLD;
		map.layers[0].locations[1].available = false;
		check(ps_layer_store->create_local_layer((3ULL << 48) | 23, orphan,
								 sizeof(orphan)) == 0 &&
			  ps_layer_store->recover_local_layers(&map) != 0 &&
			  access(orphan, F_OK) == 0,
			  "conflicting second local location fails closed");
		map.layers[0].location_count = 1;
		unlink(orphan);
		ps_layer_map_free(&map);
	}

	ps_layer_store->close();
	unsetenv("PAGESTORE_OBJECT_DIR");
	unlink(retained_path);
	snprintf(owner_path, sizeof(owner_path), "%s/.pagestore-owner", object_dir);
	unlink(owner_path);
	snprintf(owner_path, sizeof(owner_path), "%s/.pagestore-store-id", local_dir);
	unlink(owner_path);
	snprintf(owner_path, sizeof(owner_path), "%s/.pagestore-store-id", other_local_dir);
	unlink(owner_path);
	unlink(local_uri);
	rmdir(local_dir);
	rmdir(other_local_dir);
	rmdir(object_dir);
	printf("pagestore_layer_store_test: %d checks, %d failed\n", run, failed);
	return failed ? 1 : 0;
}
