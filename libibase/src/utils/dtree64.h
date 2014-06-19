#ifndef _DTREE64_H
#define _DTREE64_H
#define DTREE64_INCRE_NUM    1000000
#define DTREE64_NODES_MAX    2000000000
#define DTREE64_MUTEX_MAX    256
#define DTREE64_ROOT_MAX     10240
#include <inttypes.h>
#include "mutex.h"
typedef struct _DTNODE64
{
    uint32_t left;
    uint32_t right;
    uint32_t parent;
    uint32_t color;
    int data;
    int bit;
    double key;
}DTNODE64;
typedef struct _DTROOT64
{
    int status;
    int bits;
    uint32_t total;
    uint32_t rootid;
}DTROOT64;
typedef struct _DTSTATE64
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
    DTROOT64 roots[DTREE64_ROOT_MAX];
}DTSTATE64;
typedef struct _DTREE64
{
    int fd;
    int status;
    off_t size;
    off_t end;
    off_t old;
    char    *start;
    DTSTATE64 *state;
    DTNODE64  *map;
    MUTEX   *mutex;
#ifdef HAVE_PTHREAD
        pthread_mutex_t mutexs[DTREE64_MUTEX_MAX];
#endif
}DTREE64;
void *dtree64_init(char *file);
int dtree64_new_tree(void *dtree);
uint32_t dtree64_total(void *dtree, int rootid);
uint32_t dtree64_try_insert(void *dtree, int rootid, double key, int data, int *old);
uint32_t dtree64_insert(void *dtree, int rootid, double key, int data, int *old);
uint32_t dtree64_get(void *dtree, uint32_t nodeid, double *key, int *data);
uint32_t dtree64_find(void *dtree, int rootid, double key, int *data);
uint32_t dtree64_min(void *dtree, int rootid, double *key, int *data);
uint32_t dtree64_max(void *dtree, int rootid, double *key, int *data);
uint32_t dtree64_next(void *dtree, int rootid, uint32_t nodeid, double *key, int *data);
uint32_t dtree64_prev(void *dtree, int rootid, uint32_t nodeid, double *key, int *data);
int dtree64_set_data(void *dtree, uint32_t nodeid, int data);
void dtree64_view_tree(void *dtree, int rootid, FILE *fp);
void dtree64_remove(void *dtree, int rootid, uint32_t nodeid, double *key, int *data);
void dtree64_remove_tree(void *dtree, int rootid);
void dtree64_close(void *dtree);
#endif
