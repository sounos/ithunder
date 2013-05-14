#ifndef _MMTREE64_H
#define _MMTREE64_H
#define MMTREE64_INCRE_NUM    10000000
#define MMTREE64_NODES_MAX    2000000000
#define MMTREE64_ROOT_MAX     10240
#include "mutex.h"
typedef struct _MTNODE
{
    unsigned int left;
    unsigned int right;
    unsigned int parent;
    unsigned int color;
    int data;
    int bit;
    int64_t key;
}MTNODE;
typedef struct _MMROOT
{
    int status;
    int bits;
    unsigned int total;
    unsigned int rootid;
}MMROOT;
typedef struct _MTSTATE
{
    int64_t kmax;
    int64_t kmin;
    unsigned int nmax;
    unsigned int nmin;
    unsigned int count;
    unsigned int left;
    unsigned int current;
    unsigned int total;
    unsigned int qleft;
    unsigned int qfirst;
    unsigned int qlast;
    unsigned int nroots;
    MMROOT roots[MMTREE64_ROOT_MAX];
}MTSTATE;
typedef struct _MMTREE64
{
    int fd;
    int status;
    off_t size;
    off_t end;
    off_t old;
    char    *start;
    MTSTATE *state;
    MTNODE  *map;
    MUTEX   *mutex;
}MMTREE64;
void *mmtree64_init(char *file);
int mmtree64_new_tree(void *mmtree);
unsigned int mmtree64_total(void *mmtree, int rootid);
unsigned int mmtree64_try_insert(void *mmtree, int rootid, int64_t key, int data, int *old);
unsigned int mmtree64_insert(void *mmtree, int rootid, int64_t key, int data, int *old);
unsigned int mmtree64_get(void *mmtree, unsigned int nodeid, int64_t *key, int *data);
unsigned int mmtree64_find(void *mmtree, int rootid, int64_t key, int *data);
unsigned int mmtree64_min(void *mmtree, int rootid, int64_t *key, int *data);
unsigned int mmtree64_max(void *mmtree, int rootid, int64_t *key, int *data);
unsigned int mmtree64_next(void *mmtree, int rootid, unsigned int nodeid, int64_t *key, int *data);
unsigned int mmtree64_prev(void *mmtree, int rootid, unsigned int nodeid, int64_t *key, int *data);
int mmtree64_set_data(void *mmtree, unsigned int nodeid, int data);
void mmtree64_view_tree(void *mmtree, int rootid, FILE *fp);
void mmtree64_remove(void *mmtree, int rootid, unsigned int nodeid, int64_t *key, int *data);
void mmtree64_remove_tree(void *mmtree, int rootid);
void mmtree64_close(void *mmtree);
#endif
