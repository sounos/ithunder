#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include "mutex.h"
#ifndef _QUEUE_H
#define _QUEUE_H
#ifdef __cplusplus
extern "C" {
#endif
#define QUEUE_BLOCK_SIZE  1024
typedef struct _QUEUE
{
    int pos;
    int total;
    int left;
    int count;
    int newcount;
    int head;
    int tail;
    int bits;
    void *table;
    void *newtable;
    MUTEX *mutex;
}QUEUE;

#define Q(ptr) ((QUEUE *)ptr)
#define QTOTAL(ptr) ((Q(ptr)->total))
#define QNTAB(ptr) ((Q(ptr)->newtable))
#define QTAB(ptr) ((Q(ptr)->table))
#define QTI(ptr, type, n) (((type *)(Q(ptr)->table))[n])
#define QNTI(ptr, type, n) (((type *)(Q(ptr)->newtable))[n])
#define QPOS(ptr) (Q(ptr)->pos)
#define QHEAD(ptr) (Q(ptr)->head)
#define QTAIL(ptr) (Q(ptr)->tail)
#define QLEFT(ptr) (Q(ptr)->left)
#define QCOUNT(ptr) (Q(ptr)->count)
#define QNCOUNT(ptr) (Q(ptr)->newcount)
#define QMUTEX(ptr) (Q(ptr)->mutex)
#define QUEUE_INIT(ptr)                                                                     \
do                                                                                          \
{                                                                                           \
    if((ptr = calloc(1, sizeof(QUEUE))))                                                    \
    {                                                                                       \
        MUTEX_INIT(Q(ptr)->mutex);                                                          \
    }                                                                                       \
}while(0)
//                fprintf(stdout, "head:%d tail:%d\n", QHEAD(ptr), QTAIL(ptr));
#define QUEUE_RESIZE(ptr, type)                                                             \
do{                                                                                         \
    if(QLEFT(ptr) <= 0)                                                                     \
    {                                                                                       \
        QNCOUNT(ptr) = QCOUNT(ptr) + QUEUE_BLOCK_SIZE;                                      \
        if((QNTAB(ptr) = calloc(QNCOUNT(ptr), sizeof(type))))                               \
        {                                                                                   \
            if(QTAB(ptr) && QCOUNT(ptr) > 0)                                                \
            {                                                                               \
                QPOS(ptr) = 0;                                                              \
                while(QPOS(ptr) < QCOUNT(ptr))                                              \
                {                                                                           \
                    if(QHEAD(ptr) == QCOUNT(ptr)) QHEAD(ptr) = 0;                           \
                    memcpy(&(QNTI(ptr, type, QPOS(ptr))),                                   \
                            &(QTI(ptr, type, QHEAD(ptr))), sizeof(type));                   \
                    QPOS(ptr)++;QHEAD(ptr)++;                                               \
                }                                                                           \
                free(QTAB(ptr));                                                            \
                QTAIL(ptr) = QPOS(ptr);                                                     \
                QHEAD(ptr) = 0;                                                             \
            }                                                                               \
            QLEFT(ptr) += QUEUE_BLOCK_SIZE;                                                 \
            QTAB(ptr) = QNTAB(ptr);                                                         \
            QCOUNT(ptr) = QNCOUNT(ptr);                                                     \
            QNTAB(ptr) = NULL;                                                              \
            QNCOUNT(ptr) = 0;                                                               \
            QPOS(ptr) = 0;                                                                  \
        }                                                                                   \
    }                                                                                       \
}while(0)

#define QUEUE_PUSH(ptr, type, dptr)                                                         \
do{                                                                                         \
    MUTEX_LOCK(QMUTEX(ptr));                                                                \
    if(ptr && dptr)                                                                         \
    {                                                                                       \
        QUEUE_RESIZE(ptr, type);                                                            \
        if(QTAB(ptr) && QLEFT(ptr) > 0)                                                     \
        {                                                                                   \
            if(QTAIL(ptr) == QCOUNT(ptr)) QTAIL(ptr) = 0;                                   \
            memcpy(&(QTI(ptr, type, QTAIL(ptr))), dptr, sizeof(type));                      \
            QTAIL(ptr)++;QLEFT(ptr)--;QTOTAL(ptr)++;                                        \
        }                                                                                   \
    }                                                                                       \
    MUTEX_UNLOCK(QMUTEX(ptr));                                                              \
}while(0)

#define QUEUE_POP(ptr, type, dptr) ((ptr && QLEFT(ptr) < QCOUNT(ptr)                        \
        && MUTEX_LOCK(QMUTEX(ptr)) == 0                                                     \
        && (QHEAD(ptr) = ((QHEAD(ptr) == QCOUNT(ptr))? 0 : QHEAD(ptr))) >= 0                \
        && memcpy(dptr, &(QTI(ptr, type, QHEAD(ptr))), sizeof(type))                        \
        && QHEAD(ptr)++ >= 0 && QLEFT(ptr)++ >= 0 && QTOTAL(ptr)--  >= 0                    \
        && MUTEX_UNLOCK(QMUTEX(ptr)) == 0)? 0: -1)

#define QUEUE_HEAD(ptr, type, dptr) ((ptr && QLEFT(ptr) < QCOUNT(ptr)                       \
        && MUTEX_LOCK(QMUTEX(ptr)) == 0                                                     \
        && (QHEAD(ptr) = ((QHEAD(ptr) == QCOUNT(ptr))? 0 : QHEAD(ptr))) >= 0                \
        && memcpy(dptr, &(QTI(ptr, type, QHEAD(ptr))), sizeof(type))                        \
        && MUTEX_UNLOCK(QMUTEX(ptr)) == 0) ? 0 : -1)

#define QUEUE_RESET(ptr)                                                                    \
do{                                                                                         \
    MUTEX_LOCK(QMUTEX(ptr));                                                                \
    if(ptr)                                                                                 \
    {                                                                                       \
        if(QTAB(ptr)) free(QTAB(ptr));                                                      \
        memset(ptr, 0, (sizeof(QUEUE) - sizeof(void *)));                                   \
    }                                                                                       \
    MUTEX_UNLOCK(QMUTEX(ptr));                                                              \
}while(0)
#define QUEUE_CLEAN(ptr)                                                                    \
do{                                                                                         \
    if(ptr)                                                                                 \
    {                                                                                       \
        if(QTAB(ptr)) free(QTAB(ptr));                                                      \
        MUTEX_DESTROY(QMUTEX(ptr));                                                         \
        free(ptr);                                                                          \
        ptr = NULL;                                                                         \
    }                                                                                       \
}while(0)
#ifdef __cplusplus
     }
#endif
#endif
