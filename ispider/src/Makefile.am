#automake 
sbin_PROGRAMS = imonitord
imonitord_SOURCES = xtask.h xtask.c imonitord.c iniparser.c iniparser.h base64monitordhtml.h \
	utils/http.h utils/http.c utils/base64.c utils/base64.h utils/mutex.h utils/db.h utils/db.c \
	utils/mmqueue.c utils/mmqueue.h utils/mmtrie.c utils/mmtrie.h utils/vmap.h utils/vmap.c \
	utils/logger.h utils/logger.c utils/stime.c utils/stime.h utils/mtrie.c utils/mtrie.h \
	utils/xmm.c utils/xmm.h utils/iqueue.h utils/iqueue.c utils/kmap.h utils/kmap.c \
	utils/timer.h utils/evdns.h utils/evdns.c utils/zstream.h utils/zstream.c utils/rwlock.h\
	utils/mtree.h utils/mtree.c utils/md5.h utils/md5.c utils/qmap.h utils/qmap.c
imonitord_CPPFLAGS = -I utils/ -D_FILE_OFFSET_BITS=64 -D _HTTP_CHARSET_CONVERT -Wall
imonitord_LDFLAGS = -lz -lsbase -lchardet


bin_PROGRAMS = ispider iextractor

ispider_SOURCES = ispider.c iniparser.h iniparser.c utils/md5.h utils/md5.c \
		utils/http.h utils/http.c utils/mime.h utils/mime.c utils/zstream.h utils/zstream.c \
		utils/base64.h utils/base64.c utils/mtrie.h utils/mtrie.c utils/iqueue.h utils/iqueue.c \
		utils/qchardet.h utils/qchardet.c

ispider_CPPFLAGS = -I utils/ -D_FILE_OFFSET_BITS=64 -Wall 
ispider_LDFLAGS = -lz -lsbase -libase -lchardet

iextractor_SOURCES = iextractor.c iniparser.h iniparser.c utils/http.h utils/http.c \
		utils/zstream.h utils/zstream.c utils/base64.h utils/base64.c utils/mtrie.h utils/mtrie.c \
		utils/iqueue.h utils/iqueue.c utils/html.h utils/html.c utils/htmltags.h \
		utils/charcode.h  utils/url.h

iextractor_CPPFLAGS = -I utils/ -D_FILE_OFFSET_BITS=64 -Wall 
iextractor_LDFLAGS = -lz -lsbase -libase -lpcre
