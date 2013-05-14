#automake 
sbin_PROGRAMS = himasterd hindexd hidocd 
himasterd_SOURCES = himasterd.c utils/http.c utils/http.h iniparser.c iniparser.h \
	base64masterdhtml.h mmdb.h mmdb.c utils/mmtrie.h utils/mmtrie.c utils/db.c utils/db.h \
	utils/logger.h utils/logger.c utils/mutex.h utils/timer.h utils/mtrie.h utils/mtrie.c \
	utils/stime.h utils/stime.c utils/base64.h utils/base64.c utils/md5.h utils/md5.c \
	utils/iqueue.h utils/iqueue.c utils/xmm.h utils/xmm.c utils/mtree64.h utils/mtree64.c
himasterd_CPPFLAGS = -I utils/ -D_FILE_OFFSET_BITS=64 -Wall
himasterd_LDFLAGS = -lz -lsbase

hindexd_SOURCES = hindexd.c utils/http.c utils/http.h iniparser.c iniparser.h base64indexdhtml.h \
	utils/logger.h utils/mutex.h utils/timer.h utils/mtrie.h utils/mtrie.c utils/logger.c \
	utils/stime.h utils/stime.c utils/base64.h utils/base64.c utils/xmm.h utils/xmm.c
hindexd_CPPFLAGS = -I utils/ -D_FILE_OFFSET_BITS=64 -Wall
hindexd_LDFLAGS = -lz -lsbase -libase

hidocd_SOURCES = hidocd.c utils/http.c utils/http.h iniparser.c iniparser.h utils/mtrie.h \
utils/logger.h utils/logger.c utils/mutex.h utils/timer.h html/charcode.h html/html.c html/html.h \
html/htmltags.h hidoc.h hidoc.c utils/zvbcode.c utils/zvbcode.h utils/mtrie.c   utils/mtrie.h \
utils/mtree.h utils/mtree.c utils/iqueue.h utils/iqueue.c utils/stime.h utils/stime.c \
utils/base64.h utils/base64.c  base64docdhtml.h utils/mmtrie.h utils/mmtrie.c \
utils/mmqueue.h utils/mmqueue.c utils/db.h utils/db.c utils/xmm.h utils/xmm.c \
utils/mmtree64.h utils/mmtree64.c 

hidocd_CPPFLAGS = -I utils -I html -D_FILE_OFFSET_BITS=64 -Wall
hidocd_LDFLAGS  = -lz -lsbase

bin_PROGRAMS = hidump
hidump_SOURCES = hidump.c
hidump_CPPFLAGS = -I utils -D_FILE_OFFSET_BITS=64 -Wall
hidump_LDFLAGS  = -libase
