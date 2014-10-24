#ifndef _DTREE_H
#define _DTREE_H
#define DTREE_INCRE_NUM    1000000
#define DTREE_NODES_MAX    2000000000
#define DTREE_MUTEX_MAX    256
#define DTREE_ROOT_MAX     1000000
#include <inttypes.h>
#include "mutex.h"
typedef struct _DTNODE
{
    uint32_t left;
    uint32_t right;
    uint32_t parent;
    uint32_t color;
    int data;
    int bit;
    double key;
}DTNODE;
typedef struct _DTROOT
{
    int16_t  status;
    int16_t  flag;
    uint32_t total;
    uint32_t rootid;
}DTROOT;
typedef struct _DTSTATE
{
    double kmax;
    double kmin;
    uint32_t nmax;
    uint32_t nmin;
    uint32_t count;
    uint32_t left;
    uint32_t current;
    uint32_t total;
    uint32_t qleft;
    uint32_t qfirst;
    uint32_t qlast;
    uint32_t nroots;
    DTROOT roots[DTREE_ROOT_MAX];
}DTSTATE;
typedef struct _DTREE
{
    int fd;
    int status;
    off_t size;
    off_t end;
    off_t old;
    char    *start;
    DTSTATE *state;
    DTNODE  *map;
    MUTEX   *mutex;
#ifdef HAVE_PTHREAD
        pthread_mutex_t mutexs[DTREE_MUTEX_MAX];
#endif
}DTREE;
void *dtree_init(char *file);
int dtree_new_tree(void *dtree);
uint32_t dtree_total(void *dtree, int rootid);
uint32_t dtree_try_insert(void *dtree, int rootid, double key, int data, int *old);
uint32_t dtree_insert(void *dtree, int rootid, double key, int data, int *old);
uint32_t dtree_get(void *dtree, uint32_t nodeid, double *key, int *data);
uint32_t dtree_find(void *dtree, int rootid, double key, int *data);
uint32_t dtree_min(void *dtree, int rootid, double *key, int *data);
uint32_t dtree_max(void *dtree, int rootid, double *key, int *data);
uint32_t dtree_next(void *dtree, int rootid, uint32_t nodeid, double *key, int *data);
uint32_t dtree_prev(void *dtree, int rootid, uint32_t nodeid, double *key, int *data);
int dtree_set_data(void *dtree, uint32_t nodeid, int data);
void dtree_view_tree(void *dtree, int rootid, FILE *fp);
void dtree_remove(void *dtree, int rootid, uint32_t nodeid, double *key, int *data);
void dtree_remove_tree(void *dtree, int rootid);
void dtree_close(void *dtree);
#endif
