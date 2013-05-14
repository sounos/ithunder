lib_LTLIBRARIES = libmtask.la
libmtask_la_SOURCES = mtask.c mtask.h utils/xmm.h utils/xmm.c

libmtask_la_CPPFLAGS = -D_FILE_OFFSET_BITS=64 -Wall -I utils
include_HEADERS = mtask.h

sbin_PROGRAMS = qtaskd

qtaskd_SOURCES = qtaskd.c qtask.h qtask.c utils/http.h utils/http.c iniparser.h iniparser.c \
				 utils/mtrie.h utils/mtrie.c utils/stime.h utils/stime.c utils/logger.h \
				 utils/mutex.h utils/mmqueue.h utils/mmqueue.c utils/mmtrie.h utils/mmtrie.c \
				 utils/mmtree.h utils/mmtree.c utils/xmm.h utils/xmm.c utils/base64.h \
				 utils/base64.c base64qtaskdhtml.h utils/timer.h utils/db.h utils/db.c utils/logger.c
qtaskd_CPPFLAGS = -I utils -D_FILE_OFFSET_BITS=64 -Wall 
qtaskd_LDADD = libmtask.la
qtaskd_LDFLAGS = -static -levbase -lsbase

bin_PROGRAMS = btask
btask_SOURCES = btask.c utils/xmm.h utils/xmm.c
btask_CPPFLAGS = -D_FILE_OFFSET_BITS=64 -Wall -I utils
btask_LDADD = libmtask.la
btask_LDFLAGS = -static

