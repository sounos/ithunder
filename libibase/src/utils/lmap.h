#ifndef __LMAP__H
#define __LMAP__H
#include <inttypes.h>
#include <pthread.h>
#define LMM_SLOT_MAX     524288
#define LMM_SLOT_INC     2000
#define LMM_SLOT_NUM     1024
#define LMM_SLOT2_NUM    512 /* half of LMM_SLOT_NUM */
#define LMM_LMMV_INC     2000000
#define LMM_NODES_MAX    4294967296
#pragma pack(push, 4)
#ifndef U32_T
#define U32_T
typedef uint32_t u32_t;
#endif
typedef struct LMMKV
{
    int64_t key; /* 数值 */
    u32_t val; /* 文档序号 */
}LMMKV;
typedef struct _LMMV
{
    int64_t val; /* 数值 */
    int off; /* 节点偏移 */
}LMMV;
typedef struct _LMMSLOT
{
    int64_t min; /* 当前桶的最小值 */
    int64_t max; /* 当前桶的最大值 */
    int nodeid; /* 桶偏移地址 */
    int count; /* 桶元素个数 */
}LMMSLOT;
typedef struct _LMMSTATE
{
    LMMSLOT slots[LMM_SLOT_MAX]; /* 槽 */
    u32_t qleft[LMM_SLOT_MAX]; /* 空闲的桶 */
    u32_t roots[LMM_SLOT_MAX]; /* 桶和槽的衍射 */
    u32_t nleft; /* 空闲桶个数 */
    u32_t count; /* 槽的个数 */
}LMMSTATE;
typedef struct _LMAP
{
    LMMSTATE *state;
    LMMKV   *map;
    LMMV *vmap;
    LMMSLOT *slots;
    u32_t *roots;
    int fd;
    int vfd;
    off_t size;
    off_t msize;
    off_t vsize;
    off_t vmsize;
    pthread_rwlock_t  rwlock;
}LMAP;
LMAP *lmap_init(char *file);
int lmap_set(LMAP *lmap, u32_t no, int64_t key);
int lmap_del(LMAP *lmap, u32_t no);
/* return number of the hits */
int lmap_range(LMAP *lmap, int64_t from, int64_t to, u32_t *list);
int lmap_rangefrom(LMAP *lmap, int64_t key, u32_t *list); /* key = from */
int lmap_rangeto(LMAP *lmap, int64_t key, u32_t *list); /* key = to */
int lmap_in(LMAP *lmap, int64_t key, u32_t *list);
int lmap_ins(LMAP *lmap, int64_t *keys, int nkeys, u32_t *list);
/* set list[] if (list != NULL) */
void lmap_close(LMAP *lmap);
#define LMAP_GET(x, no) ((LMAP *)x)->vmap[no].val
#define LMAP_SET(x, no, key) lmap_set(((LMAP *)x), no, key)
#define LMAP_DEL(x, no, key) lmap_del(((LMAP *)x), no)
#pragma pack(pop)
#endif
