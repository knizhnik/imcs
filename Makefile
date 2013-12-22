# *-------------------------------------------------------------------------
# *
# * Makefile
# *
# *
# *
# * Copyright (c) 2013, Konstantin Knizhnik, ww.garret.ru
# * Author: Konstantin Knizhnik <knizhnik@garret.ru>
# *
# *	  $Id: Makefile 28 2013-10-10 17:18:31Z lptolik $
# *
# *-------------------------------------------------------------------------

MODULE_big = imcs
OBJS = imcs.o func.o smp.o btree.o threadpool.o 

EXTENSION = imcs
DATA = imcs--1.1.sql 
REGRESS = create span operators math datetime transform scalarop grandagg groupbyagg gridagg windowagg hashagg cumagg sort spec drop

SHLIB_LINK += $(filter -lm, $(LIBS))

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/imcs
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

distrib: clean
	cd .. ; tar --exclude=.svn -chvzf imcs-1.02.tar.gz imcs