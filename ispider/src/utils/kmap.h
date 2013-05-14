#ifndef _KMAP_H
#define _KMAP_H
#include "mutex.h"
#define KMAP_INCRE_NUM    100000
#define KMAP_NODES_MAX    2000000000
#define KMROOT_INCRE_NUM  10000
#define KMAP_ROOT_MAX     2000000000
typedef struct _KMNODE
{
    unsigned int left;
    unsigned int right;
    unsigned int parent;
    unsigned int color;
    int data;
    int bits;
    int64_t key;
}KMNODE;
typedef struct _KMROOT
{
    int status;
    unsigned int total;
    unsigned int rootid;
}KMROOT;
typedef struct _KMSTATE
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
    unsigned int rleft;
    unsigned int rcurrent;
    unsigned int rtotal;
    unsigned int qlast;
    unsigned int qrleft;
    unsigned int qrfirst;
    unsigned int qrlast;
    int bits;
}KMSTATE;
typedef struct _KMAP
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
    KMSTATE *state;
    KMNODE  *map;
    KMROOT  *roots;
    MUTEX   *mutex;
}KMAP;
void *kmap_init(char *basedir);
int kmap_new_tree(void *kmap);
unsigned int kmap_total(void *kmap, int rootid);
unsigned int kmap_try_insert(void *kmap, int rootid, int64_t key, int data, int *old);
unsigned int kmap_insert(void *kmap, int rootid, int64_t key, int data, int *old);
unsigned int kmap_get(void *kmap, unsigned int nodeid, int64_t *key, int *data);
unsigned int kmap_find(void *kmap, int rootid, int64_t key, int *data);
unsigned int kmap_min(void *kmap, int rootid, int64_t *key, int *data);
unsigned int kmap_max(void *kmap, int rootid, int64_t *key, int *data);
unsigned int kmap_next(void *kmap, int rootid, unsigned int nodeid, int64_t *key, int *data);
unsigned int kmap_prev(void *kmap, int rootid, unsigned int nodeid, int64_t *key, int *data);
int kmap_set_data(void *kmap, unsigned int nodeid, int data);
void kmap_view_tree(void *kmap, int rootid, FILE *fp);
void kmap_remove(void *kmap, int rootid, unsigned int nodeid, int64_t *key, int *data);
void kmap_remove_tree(void *kmap, int rootid);
void kmap_close(void *kmap);
#endif
