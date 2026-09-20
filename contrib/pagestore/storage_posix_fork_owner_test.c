/* Focused fork invalidation tests for the POSIX provider lease. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "pagestore_layer.h"
#include "pagestore_manifest.h"
#include "pagestore_storage.h"

static int checks;
static int failed;

static void
check(int condition, const char *name)
{
	checks++;
	if (!condition)
	{
		failed++;
		fprintf(stderr, "FAIL: %s\n", name);
	}
}

int
main(void)
{
	char store[] = "/tmp/pagestore-fork-owner-XXXXXX";
	char path[1024];
	char got[sizeof("parent")];
	PsLayerDesc manifest_layer;
	pid_t pid;
	int status = 0;

	check(mkdtemp(store) != NULL, "create fork-owner store");
	check(PsStoragePosix.open(store, 0) == 0, "open fork-owner store");
	check(PsStoragePosix.seg_write(0, 0, 0, "parent", 6) == 0,
		  "write parent segment bytes");
	memset(&manifest_layer, 0, sizeof(manifest_layer));
	manifest_layer.layer_id = (3ULL << 48) | 1;
	manifest_layer.kind = PS_LAYER_IMAGE;
	check(ps_manifest_open(store) == 0 &&
		  ps_manifest_add_layer(&manifest_layer) == 0,
		  "open and append parent manifest");

	pid = fork();
	if (pid == 0)
	{
		int rc;

		alarm(2);
		errno = 0;
		rc = PsStoragePosix.seg_write(0, 0, 0, "child!", 6);
		if (rc != -1 || errno != ECHILD)
			_exit(1);
		PsStoragePosix.close();
		PsStoragePosix.close();
		errno = 0;
		rc = PsStoragePosix.open(store, 0);
		_exit(rc == -1 && errno == ECHILD ? 0 : 2);
	}
	check(pid > 0 && waitpid(pid, &status, 0) == pid &&
		  WIFEXITED(status) && WEXITSTATUS(status) == 0,
		  "forked provider rejects writes and reopen while parent owns store");
	pid = fork();
	if (pid == 0)
	{
		PsLayerDesc child_layer = manifest_layer;
		int rc;

		alarm(2);
		child_layer.layer_id++;
		errno = 0;
		rc = ps_manifest_add_layer(&child_layer);
		if (rc != -1 || errno != ECHILD)
			_exit(3);
		errno = 0;
		rc = ps_manifest_compact();
		if (rc != -1 || errno != ECHILD)
			_exit(4);
		errno = 0;
		rc = ps_manifest_replay(&ps_layer_map);
		if (rc != -1 || errno != ECHILD)
			_exit(5);
		ps_manifest_close();
		ps_manifest_close();
		_exit(0);
	}
	check(pid > 0 && waitpid(pid, &status, 0) == pid &&
		  WIFEXITED(status) && WEXITSTATUS(status) == 0,
		  "forked manifest rejects append/compact/replay without hanging");

	memset(got, 0, sizeof(got));
	check(PsStoragePosix.seg_read(0, 0, 0, got, 6) == 0 &&
		  memcmp(got, "parent", 6) == 0,
		  "forked child cannot modify parent cached segment");
	ps_manifest_close();
	PsStoragePosix.close();

	snprintf(path, sizeof(path), "%s/seg_00000000", store);
	unlink(path);
	snprintf(path, sizeof(path), "%s/layers.manifest", store);
	unlink(path);
	snprintf(path, sizeof(path), "%s/.pagestore.lock", store);
	unlink(path);
	rmdir(store);
	printf("storage_posix_fork_owner_test: %d checks, %d failed\n",
		   checks, failed);
	return failed != 0;
}
