ACLOCAL_AMFLAGS = -I m4

lib_LTLIBRARIES = libckpool.la
libckpool_la_SOURCES = libckpool.c libckpool.h sha2.c sha2.h
libckpool_la_LIBADD = @PTHREAD_LIBS@ @MATH_LIBS@ @RT_LIBS@ @JANSSON_LIBS@

bin_PROGRAMS = ckpool
ckpool_SOURCES = ckpool.c ckpool.h generator.c generator.h bitcoin.c bitcoin.h \
		 stratifier.c stratifier.h connector.c connector.h
ckpool_LDADD = libckpool.la
