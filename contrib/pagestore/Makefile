# contrib/pagestore/Makefile

MODULE_big = pagestore
OBJS = \
	$(WIN32RES) \
	backend_localsvc.o \
	backend_passthrough.o \
	pagestore.o
PGFILEDESC = "pagestore - pluggable storage backend smgr shim"

# NB: the standalone pagestore_daemon executable is built via meson
# (see meson.build).  Under the PGXS/make path build it manually:
#   cc -O2 -o pagestore_daemon pagestore_daemon.c

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pagestore
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
