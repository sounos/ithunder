#ifndef _MMTREE_H
#define _MMTREE_H
#include "mutex.h"
#define MMTREE_INCRE_NUM    1000000
#define MMTREE_NODES_MAX    2000000000
#define MMTREE_ROOT_MAX     10240
typedef struct _MTNODE
{
    uint32_t left;
    uint32_t right;
    uint32_t parent;
    uint32_t color;
    int data;
    int key;
}MTNODE;
typedef struct _MMROOT
{
    int status;
    int bits;
    uint32_t total;
    uint32_t rootid;
}MMROOT;
typedef struct _MTSTATE
{
    int kmax;
    int kmin;
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
    MMROOT roots[MMTREE_ROOT_MAX];
}MTSTATE;
typedef struct _MMTREE
{
    int fd;
    int status;
    off_t size;
    off_t end;
    off_t old;
    void    *start;
    MTSTATE *state;
    MTNODE  *map;
    MUTEX   *mutex;
}MMTREE;
void *mmtree_init(char *file);
int mmtree_new_tree(void *mmtree);
uint32_t mmtree_total(void *mmtree, int rootid);
uint32_t mmtree_try_insert(void *mmtree, int rootid, int key, int data, int *old);
uint32_t mmtree_insert(void *mmtree, int rootid, int key, int data, int *old);
uint32_t mmtree_get(void *mmtree, uint32_t nodeid, int *key, int *data);
uint32_t mmtree_find(void *mmtree, int rootid, int key, int *data);
uint32_t mmtree_min(void *mmtree, int rootid, int *key, int *data);
uint32_t mmtree_max(void *mmtree, int rootid, int *key, int *data);
uint32_t mmtree_next(void *mmtree, int rootid, uint32_t nodeid, int *key, int *data);
uint32_t mmtree_prev(void *mmtree, int rootid, uint32_t nodeid, int *key, int *data);
int mmtree_set_data(void *mmtree, uint32_t nodeid, int data);
void mmtree_view_tree(void *mmtree, int rootid, FILE *fp);
void mmtree_remove(void *mmtree, int rootid, uint32_t nodeid, int *key, int *data);
void mmtree_remove_tree(void *mmtree, int rootid);
void mmtree_close(void *mmtree);
#endif
