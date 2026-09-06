# contrib/pagestore/Makefile

MODULE_big = pagestore
EXTENSION = pagestore
DATA = pagestore--1.0.sql pagestore--1.0--1.1.sql pagestore--1.1.sql \
	pagestore--1.1--1.2.sql pagestore--1.2.sql
OBJS = \
	$(WIN32RES) \
	backend_localsvc.o \
	backend_passthrough.o \
	pagestore.o \
	pagestore_control.o \
	pagestore_fault.o \
	pagestore_slru.o \
	walredo_client.o
PGFILEDESC = "pagestore - pluggable storage backend smgr shim"

# NB: the standalone pagestore_daemon and pagestore_inspect executables and the
# materializer supervisor are built/installed via meson (see meson.build).
# Under the PGXS/make path build the C executables manually:
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
