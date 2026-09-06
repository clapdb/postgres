#include "pagestore_materializer_marker.h"

#include <stdio.h>
#include <unistd.h>

static void
check(bool condition, const char *message)
{
	if (!condition)
	{
		fprintf(stderr, "FAIL: %s\n", message);
		_exit(1);
	}
}

int
main(void)
{
	check(!pagestore_materializer_marker_needs_publish(false, 0,
										false, 0),
			  "invalid candidate is ignored");
	check(pagestore_materializer_marker_needs_publish(false, 0,
										true, 10),
			  "missing marker publishes a valid candidate");
	check(!pagestore_materializer_marker_needs_publish(true, 10,
										true, 10),
			  "equal marker is idempotent");
	check(pagestore_materializer_marker_needs_publish(true, 10,
										true, 11),
			  "greater candidate publishes");
	check(!pagestore_materializer_marker_needs_publish(true, 11,
										true, 10),
			  "older candidate cannot regress marker");
	return 0;
}
