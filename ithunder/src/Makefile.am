#automake 
sbin_PROGRAMS = ithinkd idispatchd
ithinkd_SOURCES = kindex.h kindex.c ithinkd.c iniparser.c iniparser.h base64thinkdhtml.h \
	utils/http.h utils/http.c utils/base64.c utils/base64.h utils/logger.h utils/logger.c \
	utils/db.h utils/db.c utils/mmqueue.c utils/mmqueue.h utils/mmtrie.c utils/mmtrie.h \
	utils/mmtree.h utils/mmtree.c utils/stime.c utils/stime.h utils/mtrie.c utils/mtrie.h \
	utils/xmm.c utils/xmm.h utils/mtree.h utils/mtree.c utils/zvbcode.h utils/zvbcode.c \
	utils/iqueue.h utils/iqueue.c utils/timer.h utils/mutex.h
ithinkd_CPPFLAGS = -I utils/ -D_FILE_OFFSET_BITS=64 -Wall
ithinkd_LDFLAGS = -lz -lsbase -lmtask -ldbase

idispatchd_SOURCES = qindex.h qindex.c idispatchd.c iniparser.c iniparser.h base64dispatchdhtml.h \
	utils/http.h utils/http.c utils/base64.c utils/base64.h utils/mutex.h utils/db.h utils/db.c \
	utils/mmqueue.c utils/mmqueue.h utils/mmtrie.c utils/mmtrie.h utils/mmtree.h utils/mmtree.c \
	utils/logger.h utils/logger.c utils/stime.c utils/stime.h utils/mtrie.c utils/mtrie.h \
	utils/xmm.c utils/xmm.h utils/iqueue.h utils/iqueue.c utils/mmtree64.h utils/mmtree64.c \
	utils/timer.h
idispatchd_CPPFLAGS = -I utils/ -D_FILE_OFFSET_BITS=64 -Wall
idispatchd_LDFLAGS = -lz -lsbase -lmtask -ldbase

bin_PROGRAMS = dbtask
dbtask_SOURCES = dbtask.c utils/md5.h utils/md5.c utils/crc32.h utils/crc32.c
dbtask_CPPFLAGS = -I utils/ -D_FILE_OFFSET_BITS=64 -Wall 
dbtask_LDFLAGS = -lz -lsbase -lmtask -ldbase
LIBS += -lz -lsbase -lmtask -ldbase
