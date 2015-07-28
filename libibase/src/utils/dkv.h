#ifndef __DKV__H
#define __DKV__H
#include <inttypes.h>
#include <pthread.h>
#define DVV_SLOT_MAX     524288
#define DVV_SLOT_INC     2000
#define DVV_SLOT_NUM     1024
#define DVV_SLOT2_NUM    512 /* half of DVV_SLOT_NUM */
#define DVV_DVVV_INC     1000000
#define DVV_NODES_MAX    4294967296
//#define __DKV_USE_IDX__    1
#pragma pack(push, 4)
#ifndef U32_T
#define U32_T
typedef uint32_t u32_t;
#endif
typedef struct DVVKV
{
    double key; /* 数值 */
    u32_t val; /* 文档序号 */
}DVVKV;
typedef struct _DVVV
{
    double val; /* 数值 */
#ifdef __DKV_USE_IDX__
    int off; /* 节点偏移 */
#endif
}DVVV;
typedef struct _DVVSLOT
{
    double min; /* 当前桶的最小值 */
    double max; /* 当前桶的最大值 */
    int nodeid; /* 桶偏移地址 */
    int count; /* 桶元素个数 */
}DVVSLOT;
typedef struct _DVVSTATE
{
    DVVSLOT slots[DVV_SLOT_MAX]; /* 槽 */
    u32_t qleft[DVV_SLOT_MAX]; /* 空闲的桶 */
    u32_t roots[DVV_SLOT_MAX]; /* 桶和槽的衍射 */
    u32_t nleft; /* 空闲桶个数 */
    u32_t count; /* 槽的个数 */
}DVVSTATE;
typedef struct _DKV
{
    DVVSTATE *state;
    DVVKV   *map;
    DVVV *vmap;
    DVVSLOT *slots;
    u32_t *roots;
    int fd;
    int vfd;
    off_t size;
    off_t msize;
    off_t vsize;
    off_t vmsize;
    pthread_rwlock_t  rwlock;
}DKV;
DKV *dkv_init(char *file);
int dkv_set(DKV *dkv, u32_t no, double key);
int dkv_get(DKV *dkv, u32_t no, u32_t *val);
void dkv_close(DKV *dkv);
#ifdef __DKV_USE_IDX__
/* return number of the hits */
int dkv_del(DKV *dkv, u32_t no);
int dkv_range(DKV *dkv, double from, double to, u32_t *list);
int dkv_rangefrom(DKV *dkv, double key, u32_t *list); /* key = from */
int dkv_rangeto(DKV *dkv, double key, u32_t *list); /* key = to */
int dkv_in(DKV *dkv, double key, u32_t *list);
int dkv_ins(DKV *dkv, double *keys, int nkeys, u32_t *list);
/* set list[] if (list != NULL) */
#define DKV_DEL(x, no) dkv_del(((DKV *)x), no)
#else
#define DKV_DEL(x, no)
#endif
#define DKV_GET(x, no) ((DKV *)x)->vmap[no].val
#define DKV_SET(x, no, key) dkv_set(((DKV *)x), no, key)
#pragma pack(pop)
#endif
