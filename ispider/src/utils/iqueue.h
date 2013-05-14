#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include "mutex.h"
#ifndef _QUEUE_H
#define _QUEUE_H
#ifdef __cplusplus
extern "C" {
#endif
typedef struct _QNODE
{
    //void *ptr;
    int data;
    struct _QNODE *next;
}QNODE;
#define QNODE_LINE_MAX   10240
#define QNODE_LINE_NUM   1024
typedef struct _QUEUE
{
    int total;
    int nlist;
    QNODE *list[QNODE_LINE_MAX];
    QNODE *left;
    QNODE *first;
    QNODE *last;
    MUTEX *mutex;
}QUEUE;
void *iqueue_init();
void iqueue_push(void *q, int data);
void iqueue_pop(void *q, int *data);
void iqueue_head(void *q, int *data);
void iqueue_clean(void *q);
#define QTOTAL(q) (((QUEUE *)q)->total)
#ifdef __cplusplus
     }
#endif
#endif
