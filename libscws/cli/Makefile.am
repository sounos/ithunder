# $Id: Makefile.am,v 1.2 2010/07/10 06:58:12 hightman Exp $
#

#LIBTOOL = @LIBTOOL@ $(QUIET:y=--quiet)

libscwsdir = $(top_srcdir)/libscws

INCLUDES = -I$(top_srcdir) -I$(libscwsdir)

bin_PROGRAMS = scws gen-scws-dict

scws_SOURCES = scws_cmd.c
scws_LDADD = $(libscwsdir)/libscws.la

gen_scws_dict_SOURCES = gen_dict.c
gen_scws_dict_LDADD = $(libscwsdir)/libscws.la

