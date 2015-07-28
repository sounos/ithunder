#ifndef __LKV__H
#define __LKV__H
#include <inttypes.h>
#include <pthread.h>
#define LVV_SLOT_MAX     524288
#define LVV_SLOT_INC     2000
#define LVV_SLOT_NUM     1024
#define LVV_SLOT2_NUM    512 /* half of LVV_SLOT_NUM */
#define LVV_LVVV_INC     1000000
#define LVV_NODES_MAX    4294967296
//#define __LKV_USE_IDX__    1
#pragma pack(push, 4)
#ifndef U32_T
#define U32_T
typedef uint32_t u32_t;
#endif
typedef struct LVVKV
{
    int64_t key; /* 数值 */
    u32_t val; /* 文档序号 */
}LVVKV;
typedef struct _LVVV
{
    int64_t val; /* 数值 */
#ifdef __LKV_USE_IDX__
    int off; /* 节点偏移 */
#endif
}LVVV;
typedef struct _LVVSLOT
{
    int64_t min; /* 当前桶的最小值 */
    int64_t max; /* 当前桶的最大值 */
    int nodeid; /* 桶偏移地址 */
    int count; /* 桶元素个数 */
}LVVSLOT;
typedef struct _LVVSTATE
{
    LVVSLOT slots[LVV_SLOT_MAX]; /* 槽 */
    u32_t qleft[LVV_SLOT_MAX]; /* 空闲的桶 */
    u32_t roots[LVV_SLOT_MAX]; /* 桶和槽的衍射 */
    u32_t nleft; /* 空闲桶个数 */
    u32_t count; /* 槽的个数 */
}LVVSTATE;
typedef struct _LKV
{
    LVVSTATE *state;
    LVVKV   *map;
    LVVV *vmap;
    LVVSLOT *slots;
    u32_t *roots;
    int fd;
    int vfd;
    off_t size;
    off_t msize;
    off_t vsize;
    off_t vmsize;
    pthread_rwlock_t  rwlock;
}LKV;
LKV *lkv_init(char *file);
int lkv_set(LKV *lkv, u32_t no, int64_t key);
int lkv_get(LKV *lkv, u32_t no, u32_t *val);
void lkv_close(LKV *lkv);
#ifdef __LKV_USE_IDX__
/* return number of the hits */
int lkv_del(LKV *lkv, u32_t no);
int lkv_range(LKV *lkv, int64_t from, int64_t to, u32_t *list);
int lkv_rangefrom(LKV *lkv, int64_t key, u32_t *list); /* key = from */
int lkv_rangeto(LKV *lkv, int64_t key, u32_t *list); /* key = to */
int lkv_in(LKV *lkv, int64_t key, u32_t *list);
int lkv_ins(LKV *lkv, int64_t *keys, int nkeys, u32_t *list);
/* set list[] if (list != NULL) */
#define LKV_DEL(x, no) lkv_del(((LKV *)x), no)
#else
#define LKV_DEL(x, no)
#endif
#define LKV_GET(x, no) ((LKV *)x)->vmap[no].val
#define LKV_SET(x, no, key) lkv_set(((LKV *)x), no, key)
#pragma pack(pop)
#endif
