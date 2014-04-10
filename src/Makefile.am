ACLOCAL_AMFLAGS = -I m4

lib_LTLIBRARIES = libckpool.la
libckpool_la_SOURCES = libckpool.c
libckpool_la_LIBADD = @MATH_LIBS@ @PTHREAD_LIBS@

bin_PROGRAMS = ckpool
ckpool_SOURCES = ckpool.c
ckpool_LDADD = libckpool.la
