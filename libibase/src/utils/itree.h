#ifndef _ITREE_H
#define _ITREE_H
#include <inttypes.h>
#include "mutex.h"
#define ITREE_INCRE_NUM    1000000
#define ITREE_NODES_MAX    2000000000
#define ITREE_ROOT_MAX     1000000
typedef struct _ITNODE
{
    uint32_t left;
    uint32_t right;
    uint32_t parent;
    uint32_t color;
    int data;
    int key;
}ITNODE;
typedef struct _ITROOT
{
    uint16_t status;
    uint16_t flag;
    uint32_t total;
    uint32_t rootid;
}ITROOT;
typedef struct _ITSTATE
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
    ITROOT roots[ITREE_ROOT_MAX];
}ITSTATE;
typedef struct _ITREE
{
    int fd;
    int status;
    off_t size;
    off_t end;
    off_t old;
    void    *start;
    ITSTATE *state;
    ITNODE  *map;
    MUTEX   *mutex;
}ITREE;
void *itree_init(char *file);
int itree_new_tree(void *itree);
uint32_t itree_total(void *itree, int rootid);
uint32_t itree_try_insert(void *itree, int rootid, int key, int data, int *old);
uint32_t itree_insert(void *itree, int rootid, int key, int data, int *old);
uint32_t itree_get(void *itree, uint32_t nodeid, int *key, int *data);
uint32_t itree_find(void *itree, int rootid, int key, int *data);
uint32_t itree_min(void *itree, int rootid, int *key, int *data);
uint32_t itree_max(void *itree, int rootid, int *key, int *data);
uint32_t itree_next(void *itree, int rootid, uint32_t nodeid, int *key, int *data);
uint32_t itree_prev(void *itree, int rootid, uint32_t nodeid, int *key, int *data);
int itree_set_data(void *itree, uint32_t nodeid, int data);
void itree_view_tree(void *itree, int rootid, FILE *fp);
void itree_remove(void *itree, int rootid, uint32_t nodeid, int *key, int *data);
void itree_remove_tree(void *itree, int rootid);
void itree_close(void *itree);
#endif
