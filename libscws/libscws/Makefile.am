# $Id: Makefile.am,v 1.6 2011/04/19 08:04:05 hightman Exp $

#LIBTOOL = @LIBTOOL@ $(QUIET:y=--quiet)

libscwsincludedir = @prefix@/include/scws

libscwsinclude_HEADERS = charset.h crc32.h pool.h scws.h xdict.h darray.h rule.h xdb.h xtree.h scws_version.h

lib_LTLIBRARIES = libscws.la

libscws_la_SOURCES = charset.c crc32.c pool.c scws.c xdict.c darray.c rule.c lock.c xdb.c xtree.c

libscws_la_LDFLAGS = -lm
libscws_la_LIBS = -lm

EXTRA_DIST = lock.h config_win32.h
