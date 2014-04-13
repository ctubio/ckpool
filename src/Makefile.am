ACLOCAL_AMFLAGS = -I m4

lib_LTLIBRARIES = libckpool.la
libckpool_la_SOURCES = libckpool.c libckpool.h sha2.c sha2.h
libckpool_la_LIBADD = @PTHREAD_LIBS@ @MATH_LIBS@ @RT_LIBS@ @JANSSON_LIBS@

bin_PROGRAMS = ckpool
ckpool_SOURCES = ckpool.c ckpool.h bitcoin.c bitcoin.h
ckpool_LDADD = libckpool.la
