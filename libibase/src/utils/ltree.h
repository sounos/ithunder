#ifndef _LTREE_H
#define _LTREE_H
#define LTREE_INCRE_NUM    1000000
#define LTREE_NODES_MAX    2000000000
#define LTREE_MUTEX_MAX    256
#define LTREE_ROOT_MAX     1000000
#include <inttypes.h>
#include "mutex.h"
typedef struct _LTNODE
{
    uint32_t left;
    uint32_t right;
    uint32_t parent;
    uint32_t color;
    int data;
    int bit;
    int64_t key;
}LTNODE;
typedef struct _LTROOT
{
    int16_t  status;
    int16_t  flag;
    uint32_t total;
    uint32_t rootid;
}LTROOT;
typedef struct _LTSTATE
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
    LTROOT roots[LTREE_ROOT_MAX];
}LTSTATE;
typedef struct _LTREE
{
    int fd;
    int status;
    off_t size;
    off_t end;
    off_t old;
    char    *start;
    LTSTATE *state;
    LTNODE  *map;
    MUTEX   *mutex;
#ifdef HAVE_PTHREAD
        pthread_mutex_t mutexs[LTREE_MUTEX_MAX];
#endif
}LTREE;
void *ltree_init(char *file);
int ltree_new_tree(void *ltree);
uint32_t ltree_total(void *ltree, int rootid);
uint32_t ltree_try_insert(void *ltree, int rootid, int64_t key, int data, int *old);
uint32_t ltree_insert(void *ltree, int rootid, int64_t key, int data, int *old);
uint32_t ltree_get(void *ltree, uint32_t nodeid, int64_t *key, int *data);
uint32_t ltree_find(void *ltree, int rootid, int64_t key, int *data);
uint32_t ltree_min(void *ltree, int rootid, int64_t *key, int *data);
uint32_t ltree_max(void *ltree, int rootid, int64_t *key, int *data);
uint32_t ltree_next(void *ltree, int rootid, uint32_t nodeid, int64_t *key, int *data);
uint32_t ltree_prev(void *ltree, int rootid, uint32_t nodeid, int64_t *key, int *data);
int ltree_set_data(void *ltree, uint32_t nodeid, int data);
void ltree_view_tree(void *ltree, int rootid, FILE *fp);
void ltree_remove(void *ltree, int rootid, uint32_t nodeid, int64_t *key, int *data);
void ltree_remove_tree(void *ltree, int rootid);
void ltree_close(void *ltree);
#endif
