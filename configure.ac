AC_INIT(ckpool, 0.1, kernel@kolivas.org)

AC_CANONICAL_SYSTEM
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_SRCDIR([src/ckpool.c])
AC_CONFIG_HEADERS([config.h])

AM_INIT_AUTOMAKE([foreign subdir-objects])
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])
AC_USE_SYSTEM_EXTENSIONS

AC_CANONICAL_BUILD
AC_CANONICAL_HOST

dnl Make sure anyone changing configure.ac/Makefile.am has a clue
AM_MAINTAINER_MODE

dnl Checks for programs
AC_PROG_CC
# gl_EARLY - maybe later
AC_PROG_GCC_TRADITIONAL
AM_PROG_CC_C_O
LT_INIT([disable-shared])

# gl_INIT - maybe later

dnl Checks for header files.
AC_HEADER_STDC

AC_FUNC_ALLOCA

PKG_PROG_PKG_CONFIG()

AC_CHECK_LIB(jansson, json_loads, ,
	AC_MSG_ERROR([Could not find jansson library - please install libjansson-dev]))

AC_CHECK_HEADERS(stdio.h stdlib.h fcntl.h sys/time.h unistd.h)
AC_CHECK_HEADERS(ctype.h errno.h byteswap.h string.h time.h)
AC_CHECK_HEADERS(endian.h sys/endian.h arpa/inet.h syslog.h)
AC_CHECK_HEADERS(alloca.h pthread.h stdio.h math.h)
AC_CHECK_HEADERS(sys/types.h sys/socket.h linux/un.h netdb.h)
AC_CHECK_HEADERS(stdint.h netinet/in.h netinet/tcp.h)
AC_CHECK_HEADERS(jansson.h)

PTHREAD_LIBS="-lpthread"
MATH_LIBS="-lm"
RT_LIBS="-lrt"
JANSSON_LIBS="-ljansson"

AC_SUBST(PTHREAD_LIBS)
AC_SUBST(MATH_LIBS)
AC_SUBST(RT_LIBS)
AC_SUBST(JANSSON_LIBS)

AC_OUTPUT([Makefile] [src/Makefile])

echo
echo "Compilation............: make (or gmake)"
echo "  CPPFLAGS.............: $CPPFLAGS"
echo "  CFLAGS...............: $CFLAGS"
echo "  LDFLAGS..............: $LDFLAGS"
echo "  LDADD................: $PTHREAD_LIBS $MATH_LIBS $RT_LIBS $JANSSON_LIBS"
echo
echo "Installation...........: make install (as root if needed, with 'su' or 'sudo')"
echo "  prefix...............: $prefix"
echo
