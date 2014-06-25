#ifndef __MMLIST__H
#define __MMLIST__H
#include <inttypes.h>
#define MM_SLOT_MAX     65536
#define MM_SLOT_NUM     4096
#define MM_SLOT2_NUM    2048 /* half of MM_SLOT_NUM */
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
    uint32_t nodeid;
    int count;
}MMSLOT;
typedef struct _MMSTATE
{
    MMSLOT slots[MM_SLOT_MAX];
    int count;
    uint32_t left;
}MMSTATE;
typedef struct _MMLIST
{
    MMSTATE *state;
    MMKV   *map;
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
