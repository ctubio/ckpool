SUBDIRS	= jansson-2.6

ACLOCAL_AMFLAGS = -I m4
JANSSON_CPPFLAGS= -I$(top_builddir)/src/jansson-2.6/src -I$(top_srcdir)/src/jansson-2.6/src

lib_LTLIBRARIES = libckpool.la
libckpool_la_CPPFLAGS = $(JANSSON_CPPFLAGS)
libckpool_la_SOURCES = libckpool.c libckpool.h sha2.c sha2.h
libckpool_la_LIBADD = @PTHREAD_LIBS@ @MATH_LIBS@ @RT_LIBS@

bin_PROGRAMS = ckpool ckpmsg ckdb
ckpool_CPPFLAGS = $(JANSSON_CPPFLAGS)
ckpool_SOURCES = ckpool.c ckpool.h generator.c generator.h bitcoin.c bitcoin.h \
		 stratifier.c stratifier.h connector.c connector.h uthash.h \
		 utlist.h
ckpool_LDADD = libckpool.la @JANSSON_LIBS@

ckpmsg_CPPFLAGS = $(JANSSON_CPPFLAGS)
ckpmsg_SOURCES = ckpmsg.c
ckpmsg_LDADD = libckpool.la @JANSSON_LIBS@

ckdb_CPPFLAGS = $(JANSSON_CPPFLAGS)
ckdb_SOURCES = ckdb.c klist.c ktree.c klist.h ktree.h
ckdb_LDADD = libckpool.la @JANSSON_LIBS@ @PQ_LIBS@
