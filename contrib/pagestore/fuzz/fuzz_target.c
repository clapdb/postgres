/*-------------------------------------------------------------------------
 *
 * fuzz_target.c
 *	  libFuzzer entry point for the pagestore persisted-format fuzz
 *	  targets.  Build with fuzz/build.sh (clang -fsanitize=fuzzer,address,
 *	  undefined); not part of the default build.
 *
 * The compiled binary covers every registered target (see fuzz_common.c's
 * ps_fuzz_targets table).  Which single file kind an invocation mutates is
 * chosen at runtime by the PS_FUZZ_TARGET environment variable -- e.g.
 *   PS_FUZZ_TARGET=manifest ./pagestore_format_fuzz corpus/manifest
 * A run with PS_FUZZ_TARGET unset (or set to "all") instead lets the first
 * byte of each input pick the file kind, fuzzing every target from one
 * corpus directory.
 *
 *-------------------------------------------------------------------------
 */
#include <stdint.h>
#include <stdlib.h>

#include "fuzz_common.h"

int
LLVMFuzzerInitialize(int *argc, char ***argv)
{
	(void) argc;
	(void) argv;
	ps_fuzz_global_init();
	return 0;
}

int
LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
	ps_fuzz_run_one(getenv("PS_FUZZ_TARGET"), data, size);
	return 0;
}
