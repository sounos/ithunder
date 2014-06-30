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
    int32_t key; /* 数值 */
    uint32_t val; /* 文档序号 */
}MMKV;
typedef struct _VNODE
{
    int32_t val; /* 数值 */
    uint32_t off; /* 节点偏移 */
}VNODE;
typedef struct _MMSLOT
{
    int32_t min; /* 当前桶的最小值 */
    int32_t max; /* 当前桶的最大值 */
    uint32_t nodeid; /* 桶偏移地址 */
    int count; /* 桶元素个数 */
}MMSLOT;
typedef struct _MMSTATE
{
    MMSLOT slots[MM_SLOT_MAX]; /* 槽 */
    uint32_t qleft[MM_SLOT_MAX]; /* 空闲的桶 */
    uint32_t roots[MM_SLOT_MAX]; /* 桶和槽的衍射 */
    uint32_t nleft; /* 空闲桶个数 */
    uint32_t count; /* 槽的个数 */
    uint32_t max; /* 序号最大值 */
    uint32_t bits;
}MMSTATE;
typedef struct _MMLIST
{
    MMSTATE *state;
    MMKV   *map;
    VNODE *vmap;
    MMSLOT *slots;
    uint32_t *roots;
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
