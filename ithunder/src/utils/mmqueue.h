#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include "mutex.h"
#ifndef _MMQUEUE64_H
#define _MMQUEUE64_H
#ifdef __cplusplus
extern "C" {
#endif
typedef struct _MMQODE
{
    //void *ptr;
    unsigned int data;
    unsigned int next;
}MMQNODE;
typedef struct _MMQROOT
{
    int status;
    int total;
    int first;
    int last;
}MMQROOT;
#define MMQ_INCRE_NUM       1000000
#define MMQ_NODE_MAX        100000000
#define MMQ_ROOT_MAX        5000000
typedef struct _MMQSTATE
{
    int     qtotal;
    int     qleft;
    int     nroots;
    MMQROOT  roots[MMQ_ROOT_MAX];
}MMQSTATE;
typedef struct _MMQUEUE
{
    int      fd;
    int      bits;
    off_t    end;
    off_t    size;
    off_t    old;
    void     *map;
    MUTEX    *mutex;
    MMQSTATE *state;
    MMQNODE  *nodes;
}MMQUEUE;
MMQUEUE *mmqueue_init(char *qfile);
int mmqueue_new(MMQUEUE *mmq);
int mmqueue_total(MMQUEUE *mmq, int rootid);
int mmqueue_close(MMQUEUE *mmq, int rootid);
int mmqueue_push(MMQUEUE *mmq, int rootid, int data);
int mmqueue_pop(MMQUEUE *mmq, int rootid, int *data);
int mmqueue_head(MMQUEUE *mmq, int rootid, int  *data);
void mmqueue_clean(MMQUEUE *mmq);
#define MMQ(x) ((MMQUEUE*)x)
#ifdef __cplusplus
     }
#endif
#endif
