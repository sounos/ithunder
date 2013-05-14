#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <pthread.h>
#include "mutex.h"
#ifndef _LOGGER_H
#define _LOGGER_H
#ifdef __cplusplus
extern "C" {
#endif
#ifndef _TYPEDEF_LOGGER
#define _TYPEDEF_LOGGER
#define LOG_ROTATE_HOUR             0x01
#define LOG_ROTATE_DAY              0x02
#define LOG_ROTATE_WEEK             0x04
#define LOG_ROTATE_MONTH            0x08
#define LOG_ROTATE_SIZE             0x10
#define ROTATE_LOG_SIZE             268435456
#define LOGGER_LINE_SIZE            1024
#define LOGGER_LINE_LIMIT  	        32768
#define LOGGER_PATH_MAX             1024
#define __REAL__		-1
#define __DEBUG__		0
#define	__WARN__ 		1
#define	__ERROR__ 		2
#define	__FATAL__ 		3
#define	__ACCESS__ 		4
#define __LEVEL__       5
typedef struct _LOGGER
{
    int rflag;
    int n;
	int fd ;
    int total;
    int level;
    int bits;
    time_t uptime;
    MUTEX *mutex;
    char file[LOGGER_PATH_MAX];
}LOGGER;
#endif
#define PLOG(xxxxx) ((LOGGER *)xxxxx)
#ifdef HAVE_PTHREAD
#define THREADID() ((long )pthread_self())
#else
#define THREADID() ((long)0)
#endif
int logger_mkdir(char *path);
void logger_rotate_check(LOGGER *logger, struct tm *ptm);
int logger_header(LOGGER *logger, char *s, int level, char *_file_, int _line_);
int logger_write(LOGGER *logger, int level, char *_file_, int _line_, char *format,...);
LOGGER *logger_init(char *file, int rotate_flag);
void logger_clean(void *ptr);
#define LOGGER_ADD(ptr, __level__, format...)logger_write(PLOG(ptr),__level__,__FILE__, __LINE__,format) 
#define LOGGER_INIT(ptr, file) (ptr = logger_init(file, 0))
#define LOGGER_ROTATE_INIT(ptr, file, flag) (ptr = logger_init(file, flag))
#define LOGGER_SET_LEVEL(ptr, num) {if(ptr){PLOG(ptr)->level = num;}}
#ifndef _DEBUG
#define DEBUG_LOGGER(ptr, format...) {if(ptr && PLOG(ptr)->level>1){LOGGER_ADD(ptr, __DEBUG__, format);}}
#define ACCESS_LOGGER(ptr, format...) {if(ptr && PLOG(ptr)->level>0){LOGGER_ADD(ptr, __ACCESS__, format);}}
#define WARN_LOGGER(ptr, format...) {if(ptr){LOGGER_ADD(ptr, __WARN__, format);}}
#define ERROR_LOGGER(ptr, format...) {if(ptr){LOGGER_ADD(ptr, __ERROR__, format);}}
#define FATAL_LOGGER(ptr, format...) {if(ptr){LOGGER_ADD(ptr, __FATAL__, format);}}
#define REALLOG(ptr, format...) {if(ptr){LOGGER_ADD(ptr, __REAL__, format);}}
#else
#define DEBUG_LOGGER(ptr, format...) 
#define ACCESS_LOGGER(ptr, format...) 
#define WARN_LOGGER(ptr, format...) 
#define ERROR_LOGGER(ptr, format...) 
#define FATAL_LOGGER(ptr, format...) 
#define REALLOG(ptr, format...) 
#endif
#define LOGGER_CLEAN(ptr) {if(ptr){logger_clean(PLOG(ptr));}}
#ifdef __cplusplus
 }
#endif
#endif
