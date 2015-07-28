#ifndef __IKV__H
#define __IKV__H
#include <inttypes.h>
#include <pthread.h>
#define IVV_SLOT_MAX     524288
#define IVV_SLOT_INC     2000
#define IVV_SLOT_NUM     1024
#define IVV_SLOT2_NUM    512 /* half of IVV_SLOT_NUM */
#define IVV_IVVV_INC     1000000
#define IVV_NODES_MAX    4294967296
//#define __IKV_USE_IDX__    1
#pragma pack(push, 4)
#ifndef U32_T
#define U32_T
typedef uint32_t u32_t;
#endif
typedef struct IVVKV
{
    int32_t key; /* 数值 */
    u32_t val; /* 文档序号 */
}IVVKV;
typedef struct _IVVV
{
    int32_t val; /* 数值 */
#ifdef __IKV_USE_IDX__
    int off; /* 节点偏移 */
#endif
}IVVV;
typedef struct _IVVSLOT
{
    int32_t min; /* 当前桶的最小值 */
    int32_t max; /* 当前桶的最大值 */
    int nodeid; /* 桶偏移地址 */
    int count; /* 桶元素个数 */
}IVVSLOT;
typedef struct _IVVSTATE
{
    IVVSLOT slots[IVV_SLOT_MAX]; /* 槽 */
    u32_t qleft[IVV_SLOT_MAX]; /* 空闲的桶 */
    u32_t roots[IVV_SLOT_MAX]; /* 桶和槽的衍射 */
    u32_t nleft; /* 空闲桶个数 */
    u32_t count; /* 槽的个数 */
}IVVSTATE;
typedef struct _IKV
{
    IVVSTATE *state;
    IVVKV   *map;
    IVVV *vmap;
    IVVSLOT *slots;
    u32_t *roots;
    int fd;
    int vfd;
    off_t size;
    off_t msize;
    off_t vsize;
    off_t vmsize;
    pthread_rwlock_t  rwlock;
}IKV;
IKV *ikv_init(char *file);
int ikv_set(IKV *ikv, u32_t no, int32_t key);
int ikv_get(IKV *ikv, u32_t no, u32_t *val);
void ikv_close(IKV *ikv);
#ifdef __IKV_USE_IDX__
/* return number of the hits */
int ikv_del(IKV *ikv, u32_t no);
int ikv_range(IKV *ikv, int32_t from, int32_t to, u32_t *list);
int ikv_rangefrom(IKV *ikv, int32_t key, u32_t *list); /* key = from */
int ikv_rangeto(IKV *ikv, int32_t key, u32_t *list); /* key = to */
int ikv_in(IKV *ikv, int32_t key, u32_t *list);
int ikv_ins(IKV *ikv, int32_t *keys, int nkeys, u32_t *list);
/* set list[] if (list != NULL) */
#define IKV_DEL(x, no) ikv_del(((IKV *)x), no)
#else
#define IKV_DEL(x, no)
#endif
#define IKV_GET(x, no) ((IKV *)x)->vmap[no].val
#define IKV_SET(x, no, key) ikv_set(((IKV *)x), no, key)
#pragma pack(pop)
#endif
