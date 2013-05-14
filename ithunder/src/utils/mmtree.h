#ifndef _MMTREE_H
#define _MMTREE_H
#include "mutex.h"
#define MMTREE_INCRE_NUM    1000000
#define MMTREE_NODES_MAX    2000000000
#define MMTREE_ROOT_MAX     10240
typedef struct _MTNODE
{
    unsigned int left;
    unsigned int right;
    unsigned int parent;
    unsigned int color;
    int data;
    int key;
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
    int kmax;
    int kmin;
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
unsigned int mmtree_total(void *mmtree, int rootid);
unsigned int mmtree_try_insert(void *mmtree, int rootid, int key, int data, int *old);
unsigned int mmtree_insert(void *mmtree, int rootid, int key, int data, int *old);
unsigned int mmtree_get(void *mmtree, unsigned int nodeid, int *key, int *data);
unsigned int mmtree_find(void *mmtree, int rootid, int key, int *data);
unsigned int mmtree_min(void *mmtree, int rootid, int *key, int *data);
unsigned int mmtree_max(void *mmtree, int rootid, int *key, int *data);
unsigned int mmtree_next(void *mmtree, int rootid, unsigned int nodeid, int *key, int *data);
unsigned int mmtree_prev(void *mmtree, int rootid, unsigned int nodeid, int *key, int *data);
int mmtree_set_data(void *mmtree, unsigned int nodeid, int data);
void mmtree_view_tree(void *mmtree, int rootid, FILE *fp);
void mmtree_remove(void *mmtree, int rootid, unsigned int nodeid, int *key, int *data);
void mmtree_remove_tree(void *mmtree, int rootid);
void mmtree_close(void *mmtree);
#endif
