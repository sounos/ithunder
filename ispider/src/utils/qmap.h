#ifndef _QMAP_H
#define _QMAP_H
#include "mutex.h"
#define QMAP_INCRE_NUM    100000
#define QMAP_NODES_MAX    2000000000
#define QMROOT_INCRE_NUM  10000
#define QMAP_ROOT_MAX     2000000000
typedef struct _QMNODE
{
    unsigned int left;
    unsigned int right;
    unsigned int parent;
    unsigned int color;
    int data;
    int key;
}QMNODE;
typedef struct _QMROOT
{
    int status;
    unsigned int total;
    unsigned int rootid;
}QMROOT;
typedef struct _QMSTATE
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
}QMSTATE;
typedef struct _QMAP
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
    QMSTATE *state;
    QMNODE  *map;
    QMROOT  *roots;
    MUTEX   *mutex;
}QMAP;
void *qmap_init(char *basedir);
int qmap_new_tree(void *qmap);
unsigned int qmap_total(void *qmap, int rootid);
unsigned int qmap_try_insert(void *qmap, int rootid, int key, int data, int *old);
unsigned int qmap_insert(void *qmap, int rootid, int key, int data, int *old);
unsigned int qmap_get(void *qmap, unsigned int nodeid, int *key, int *data);
unsigned int qmap_find(void *qmap, int rootid, int key, int *data);
unsigned int qmap_min(void *qmap, int rootid, int *key, int *data);
unsigned int qmap_max(void *qmap, int rootid, int *key, int *data);
unsigned int qmap_next(void *qmap, int rootid, unsigned int nodeid, int *key, int *data);
unsigned int qmap_prev(void *qmap, int rootid, unsigned int nodeid, int *key, int *data);
int qmap_set_data(void *qmap, unsigned int nodeid, int data);
void qmap_view_tree(void *qmap, int rootid, FILE *fp);
void qmap_remove(void *qmap, int rootid, unsigned int nodeid, int *key, int *data);
void qmap_remove_tree(void *qmap, int rootid);
void qmap_close(void *qmap);
#endif
