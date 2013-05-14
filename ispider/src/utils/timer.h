#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include "mutex.h"

#ifndef _TIMER_H
#define _TIMER_H
#ifdef __cplusplus
extern "C" {
#endif

#ifndef _TYPEDEF_TIMER
#define _TYPEDEF_TIMER
typedef struct _TIMER 
{
	struct timeval tv;
	time_t start_sec;
	time_t sec_used;
	time_t last_sec;
	time_t last_sec_used;
	off_t  start_usec;
	off_t  usec_used;
	off_t  last_usec;
    off_t  last_usec_used;
    off_t  now;
	MUTEX *mutex;
}TIMER;
#define PTLL(xxxxx) ((long long int)xxxxx)
#define PT(ptr) ((TIMER *)ptr)
#define PT_SEC_U(ptr) ((PT(ptr))?PTLL(PT(ptr)->sec_used):0ll)
#define PT_USEC_U(ptr) ((PT(ptr))?PTLL(PT(ptr)->usec_used):0ll)
#define PT_L_SEC(ptr) ((PT(ptr))?PTLL(PT(ptr)->last_sec):0ll)
#define PT_LU_SEC(ptr) ((PT(ptr))?PTLL(PT(ptr)->last_sec_used):0ll)
#define PT_L_USEC(ptr) ((PT(ptr))?PTLL(PT(ptr)->last_usec):0ll)
#define PT_LU_USEC(ptr) ((PT(ptr))?PTLL(PT(ptr)->last_usec_used):0ll)
#define PT_NOW(ptr) (PTLL(PT(ptr)->now))
#define TIMER_NOW(ptr) ((ptr && gettimeofday(&(PT(ptr)->tv), NULL) == 0)?                   \
        (PT_NOW(ptr) = PTLL(PT(ptr)->tv.tv_sec) * 1000000ll + PTLL(PT(ptr)->tv.tv_usec) * 1ll) : -1)
#define TIMER_INIT(ptr)                                                         \
do{                                                                             \
    if((ptr = (calloc(1, sizeof(TIMER)))))                                      \
    {                                                                           \
        gettimeofday(&(PT(ptr)->tv), NULL);                                     \
        PT(ptr)->start_sec    = PT(ptr)->tv.tv_sec;                             \
        PT(ptr)->start_usec   = PT(ptr)->tv.tv_sec * 1000000ll                  \
            + PT(ptr)->tv.tv_usec * 1ll;                                        \
        PT(ptr)->last_sec     = PT(ptr)->start_sec;                             \
        PT(ptr)->last_usec    = PT(ptr)->start_usec;                            \
        MUTEX_INIT(PT(ptr)->mutex);                                            \
    }                                                                           \
}while(0)                                                                      
#define TIMER_SAMPLE(ptr)                                                       \
do{                                                                             \
    if(ptr)                                                                     \
    {                                                                           \
        MUTEX_LOCK(PT(ptr)->mutex);                                             \
        gettimeofday(&(PT(ptr)->tv), NULL);                                     \
        PT(ptr)->last_sec_used    = PT(ptr)->tv.tv_sec - PT(ptr)->last_sec;     \
        PT(ptr)->last_usec_used   = PT(ptr)->tv.tv_sec * 1000000ll              \
            + PT(ptr)->tv.tv_usec - PT(ptr)->last_usec;                         \
        PT(ptr)->last_sec         = PT(ptr)->tv.tv_sec;                         \
        PT(ptr)->last_usec        = PT(ptr)->tv.tv_sec * 1000000ll              \
            + PT(ptr)->tv.tv_usec;                                              \
        PT(ptr)->sec_used     = PT(ptr)->tv.tv_sec - PT(ptr)->start_sec;        \
        PT(ptr)->usec_used    = PT(ptr)->last_usec - PT(ptr)->start_usec;       \
        MUTEX_UNLOCK(PT(ptr)->mutex);                                           \
    }                                                                           \
}while(0)
#define TIMER_RESET(ptr)                                                        \
do{                                                                               \
    if(ptr)                                                                     \
    {                                                                           \
        MUTEX_LOCK(PT(ptr)->mutex);                                             \
        gettimeofday(&(PT(ptr)->tv), NULL);                                     \
        PT(ptr)->start_sec    = PT(ptr)->tv.tv_sec;                             \
        PT(ptr)->start_usec   = PT(ptr)->tv.tv_sec * 1000000ll                  \
            + PT(ptr)->tv.tv_usec * 1ll;                                        \
        PT(ptr)->last_sec     = PT(ptr)->start_sec;                             \
        PT(ptr)->last_usec    = PT(ptr)->start_usec;                            \
        PT(ptr)->last_sec_used     = 0ll;                                       \
        PT(ptr)->last_usec_used    = 0ll;                                       \
        MUTEX_UNLOCK(PT(ptr)->mutex);                                           \
    }                                                                           \
}while(0)

#define TIMER_CHECK(ptr, interval)                                              \
    (PT(ptr) && (gettimeofday(&(PT(ptr)->tv), NULL) == 0                        \
     && ((PT(ptr)->tv.tv_sec * 1000000ll + PT(ptr)->tv.tv_usec)                 \
        - PT(ptr)->last_usec) > interval) ? 0 : -1)
#define TIMER_CLEAN(ptr)                                                        \
do{                                                                               \
    if(ptr)                                                                     \
    {                                                                           \
        MUTEX_DESTROY(PT(ptr)->mutex);                                          \
        free(ptr);                                                              \
        ptr = NULL;                                                             \
    }                                                                           \
}while(0)
#endif 

#ifdef __cplusplus
 }
#endif

#endif
