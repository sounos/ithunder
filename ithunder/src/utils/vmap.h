#ifndef _VMAP_H
#define _VMAP_H
#include "mutex.h"
#define VMAP_INCRE_NUM    100000
#define VMAP_NODES_MAX    2000000000
#define VMROOT_INCRE_NUM  10000
#define VMAP_ROOT_MAX     2000000000
typedef struct _VMNODE
{
    unsigned int left;
    unsigned int right;
    unsigned int parent;
    unsigned int color;
    int data;
    int key;
}VMNODE;
typedef struct _VMROOT
{
    int status;
    unsigned int total;
    unsigned int rootid;
}VMROOT;
typedef struct _VMSTATE
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
    unsigned int rleft;
    unsigned int rcurrent;
    unsigned int rtotal;
    unsigned int qlast;
    unsigned int qrleft;
    unsigned int qrfirst;
    unsigned int qrlast;
    int bits;
}VMSTATE;
typedef struct _VMAP
{
    int fd;
    int rfd;
    off_t size;
    off_t end;
    off_t old;
    off_t rsize;
    off_t rend;
    off_t rold;
    void    *start;
    VMSTATE *state;
    VMNODE  *map;
    VMROOT  *roots;
    MUTEX   *mutex;
}VMAP;
void *vmap_init(char *basedir);
int vmap_new_tree(void *vmap);
unsigned int vmap_total(void *vmap, int rootid);
unsigned int vmap_try_insert(void *vmap, int rootid, int key, int data, int *old);
unsigned int vmap_insert(void *vmap, int rootid, int key, int data, int *old);
unsigned int vmap_get(void *vmap, unsigned int nodeid, int *key, int *data);
unsigned int vmap_find(void *vmap, int rootid, int key, int *data);
unsigned int vmap_min(void *vmap, int rootid, int *key, int *data);
unsigned int vmap_max(void *vmap, int rootid, int *key, int *data);
unsigned int vmap_next(void *vmap, int rootid, unsigned int nodeid, int *key, int *data);
unsigned int vmap_prev(void *vmap, int rootid, unsigned int nodeid, int *key, int *data);
int vmap_set_data(void *vmap, unsigned int nodeid, int data);
void vmap_view_tree(void *vmap, int rootid, FILE *fp);
void vmap_remove(void *vmap, int rootid, unsigned int nodeid, int *key, int *data);
void vmap_remove_tree(void *vmap, int rootid);
void vmap_close(void *vmap);
#endif
