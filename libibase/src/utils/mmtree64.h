#ifndef _MMTREE64_H
#define _MMTREE64_H
#define MMTREE64_INCRE_NUM    1000000
#define MMTREE64_NODES_MAX    2000000000
#define MMTREE64_MUTEX_MAX    256
#define MMTREE64_ROOT_MAX     10240
#include <inttypes.h>
#include "mutex.h"
typedef struct _MTNODE64
{
    uint32_t left;
    uint32_t right;
    uint32_t parent;
    uint32_t color;
    int data;
    int bit;
    int64_t key;
}MTNODE64;
typedef struct _MMROOT64
{
    int status;
    int bits;
    uint32_t total;
    uint32_t rootid;
}MMROOT64;
typedef struct _MTSTATE64
{
    int64_t kmax;
    int64_t kmin;
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
    MMROOT64 roots[MMTREE64_ROOT_MAX];
}MTSTATE64;
typedef struct _MMTREE64
{
    int fd;
    int status;
    off_t size;
    off_t end;
    off_t old;
    char    *start;
    MTSTATE64 *state;
    MTNODE64  *map;
    MUTEX   *mutex;
#ifdef HAVE_PTHREAD
        pthread_mutex_t mutexs[MMTREE64_MUTEX_MAX];
#endif
}MMTREE64;
void *mmtree64_init(char *file);
int mmtree64_new_tree(void *mmtree);
uint32_t mmtree64_total(void *mmtree, int rootid);
uint32_t mmtree64_try_insert(void *mmtree, int rootid, int64_t key, int data, int *old);
uint32_t mmtree64_insert(void *mmtree, int rootid, int64_t key, int data, int *old);
uint32_t mmtree64_get(void *mmtree, uint32_t nodeid, int64_t *key, int *data);
uint32_t mmtree64_find(void *mmtree, int rootid, int64_t key, int *data);
uint32_t mmtree64_min(void *mmtree, int rootid, int64_t *key, int *data);
uint32_t mmtree64_max(void *mmtree, int rootid, int64_t *key, int *data);
uint32_t mmtree64_next(void *mmtree, int rootid, uint32_t nodeid, int64_t *key, int *data);
uint32_t mmtree64_prev(void *mmtree, int rootid, uint32_t nodeid, int64_t *key, int *data);
int mmtree64_set_data(void *mmtree, uint32_t nodeid, int data);
void mmtree64_view_tree(void *mmtree, int rootid, FILE *fp);
void mmtree64_remove(void *mmtree, int rootid, uint32_t nodeid, int64_t *key, int *data);
void mmtree64_remove_tree(void *mmtree, int rootid);
void mmtree64_close(void *mmtree);
#endif
