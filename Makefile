EXTENSION    = lt_stat_activity
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
TESTS        = $(wildcard sql/*.sql)
REGRESS      = $(patsubst sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test --temp-config=$(top_srcdir)/$(subdir)/lt_stat_activity.conf

PG_CONFIG ?= lt_config

MODULE_big = lt_stat_activity
OBJS = lt_stat_activity.o

all:

DATA = $(wildcard *--*.sql)

ifdef USE_PGXS
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/lt_stat_activity
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
