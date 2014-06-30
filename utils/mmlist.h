#ifndef __MMLIST__H
#define __MMLIST__H
#include <inttypes.h>
#define MM_SLOT_MAX     524288
#define MM_SLOT_NUM     1024
#define MM_SLOT2_NUM    512 /* half of MM_SLOT_NUM */
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
    pthread_rwlock_t  rwlock;
}MMLIST;
MMLIST *mmlist_init(char *file);
int mmlist_insert(MMLIST *mmlist, int no, int32_t key);
int mmlist_update(MMLIST *mmlist, int no, int32_t key);
int mmlist_delete(MMLIST *mmlist, int no);
/* return number of the hits */
int mmlist_range(MMLIST *mmlist, int32_t from, int32_t to, int32_t *list);
int mmlist_rangefrom(MMLIST *mmlist, int32_t key, int32_t *list); /* key = from */
int mmlist_rangeto(MMLIST *mmlist, int32_t key, int32_t *list); /* key = to */
int mmlist_in(MMLIST *mlist, int32_t key, int32_t *list);
int mmlist_ins(MMLIST *mmlist, int32_t *keys, int nkeys, int32_t *list);
/* set list[] if (list != NULL) */
void mmlist_close(MMLIST *mmlist);
#endif
