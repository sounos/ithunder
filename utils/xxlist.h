#ifndef __MMLIST__H
#define __MMLIST__H
#include <inttypes.h>
#define MM_SLOT_MAX     524288
#define MM_KV_NUM       1024
#define MM_KV2_NUM      512 /* half of MM_KV_NUM */
#define MM_VNODE_INC    1000000
#define MM_NODES_MAX    4294967296
typedef struct MMKV
{
    int32_t key;
    int32_t val;
}MMKV;
typedef struct _VNODE
{
    int32_t val;
    ushort  slot;
    ushort  off;
}VNODE;
typedef struct _MMSLOT
{
    int32_t min;
    int32_t max;
    int32_t count;
    int32_t bits;
    MMKV kvs[MM_KV_NUM];
}MMSLOT;
typedef struct _MMSTATE
{
    int32_t  roots[MM_SLOT_MAX];
    uint32_t left;
    int count;
}MMSTATE;
typedef struct _MMLIST
{
    MMSTATE *state;
    VNODE *vmap;
    MMSLOT *slots;
    int fd;
    int vfd;
    off_t size;
    off_t msize;
    off_t vsize;
    off_t vmsize;
}MMLIST;
MMLIST *mmlist_init(char *file);
int mmlist_insert(MMLIST *mmlist, int no, int32_t key);
//int mmlist_remove(MMLIST *mmlist, int no, int32_t nodeid);
int mmlist_update(MMLIST *mmlist, int no, int32_t nodeid);
int mmlist_range(MMLIST *mmlist, int32_t from, int32_t to);
int mmlist_range1(MMLIST *mmlist, int32_t from);
int mmlist_range2(MMLIST *mmlist, int32_t from);
void mmlist_close(MMLIST *mmlist);
#endif
