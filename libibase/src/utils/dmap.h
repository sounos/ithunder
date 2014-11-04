#ifndef __DMAP__H
#define __DMAP__H
#include <inttypes.h>
#include <pthread.h>
#define DMM_SLOT_MAX     524288
#define DMM_SLOT_INC     2000
#define DMM_SLOT_NUM     1024
#define DMM_SLOT2_NUM    512 /* half of DMM_SLOT_NUM */
#define DMM_DMMV_INC     2000000
#define DMM_NODES_MAX    4294967296
#pragma pack(push, 4)
#ifndef U32_T
#define U32_T
typedef uint32_t u32_t;
#endif
typedef struct DMMKV
{
    double key; /* 数值 */
    u32_t val; /* 文档序号 */
}DMMKV;
typedef struct _DMMV
{
    double val; /* 数值 */
    int off; /* 节点偏移 */
}DMMV;
typedef struct _DMMSLOT
{
    double min; /* 当前桶的最小值 */
    double max; /* 当前桶的最大值 */
    int nodeid; /* 桶偏移地址 */
    int count; /* 桶元素个数 */
}DMMSLOT;
typedef struct _DMMSTATE
{
    DMMSLOT slots[DMM_SLOT_MAX]; /* 槽 */
    u32_t qleft[DMM_SLOT_MAX]; /* 空闲的桶 */
    u32_t roots[DMM_SLOT_MAX]; /* 桶和槽的衍射 */
    u32_t nleft; /* 空闲桶个数 */
    u32_t count; /* 槽的个数 */
}DMMSTATE;
typedef struct _DMAP
{
    DMMSTATE *state;
    DMMKV   *map;
    DMMV *vmap;
    DMMSLOT *slots;
    u32_t *roots;
    int fd;
    int vfd;
    off_t size;
    off_t msize;
    off_t vsize;
    off_t vmsize;
    pthread_rwlock_t  rwlock;
}DMAP;
DMAP *dmap_init(char *file);
int dmap_set(DMAP *dmap, u32_t no, double key);
int dmap_del(DMAP *dmap, u32_t no);
/* return number of the hits */
int dmap_range(DMAP *dmap, double from, double to, u32_t *list);
int dmap_rangefrom(DMAP *dmap, double key, u32_t *list); /* key = from */
int dmap_rangeto(DMAP *dmap, double key, u32_t *list); /* key = to */
int dmap_in(DMAP *dmap, double key, u32_t *list);
int dmap_ins(DMAP *dmap, double *keys, int nkeys, u32_t *list);
/* set list[] if (list != NULL) */
void dmap_close(DMAP *dmap);
#define DMAP_GET(x, no) ((DMAP *)x)->vmap[no].val
#define DMAP_SET(x, no, key) dmap_set(((DMAP *)x), no, key)
#define DMAP_DEL(x, no, key) dmap_del(((DMAP *)x), no)
#pragma pack(pop)
#endif
