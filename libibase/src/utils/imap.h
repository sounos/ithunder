#ifndef __IMAP__H
#define __IMAP__H
#include <inttypes.h>
#include <pthread.h>
#define IMM_SLOT_MAX     524288
#define IMM_SLOT_INC     2000
#define IMM_SLOT_NUM     1024
#define IMM_SLOT2_NUM    512 /* half of IMM_SLOT_NUM */
#define IMM_IMMV_INC     2000000
#define IMM_NODES_MAX    4294967296
#pragma pack(push, 4)
#ifndef U32_T
#define U32_T
typedef uint32_t u32_t;
#endif
typedef struct IMMKV
{
    int32_t key; /* 数值 */
    u32_t val; /* 文档序号 */
}IMMKV;
typedef struct _IMMV
{
    int32_t val; /* 数值 */
    int off; /* 节点偏移 */
}IMMV;
typedef struct _IMMSLOT
{
    int32_t min; /* 当前桶的最小值 */
    int32_t max; /* 当前桶的最大值 */
    int nodeid; /* 桶偏移地址 */
    int count; /* 桶元素个数 */
}IMMSLOT;
typedef struct _IMMSTATE
{
    IMMSLOT slots[IMM_SLOT_MAX]; /* 槽 */
    u32_t qleft[IMM_SLOT_MAX]; /* 空闲的桶 */
    u32_t roots[IMM_SLOT_MAX]; /* 桶和槽的衍射 */
    u32_t nleft; /* 空闲桶个数 */
    u32_t count; /* 槽的个数 */
}IMMSTATE;
typedef struct _IMAP
{
    IMMSTATE *state;
    IMMKV   *map;
    IMMV *vmap;
    IMMSLOT *slots;
    u32_t *roots;
    int fd;
    int vfd;
    off_t size;
    off_t msize;
    off_t vsize;
    off_t vmsize;
    pthread_rwlock_t  rwlock;
}IMAP;
IMAP *imap_init(char *file);
int imap_set(IMAP *imap, u32_t no, int32_t key);
int imap_del(IMAP *imap, u32_t no);
/* return number of the hits */
int imap_range(IMAP *imap, int32_t from, int32_t to, u32_t *list);
int imap_rangefrom(IMAP *imap, int32_t key, u32_t *list); /* key = from */
int imap_rangeto(IMAP *imap, int32_t key, u32_t *list); /* key = to */
int imap_in(IMAP *imap, int32_t key, u32_t *list);
int imap_ins(IMAP *imap, int32_t *keys, int nkeys, u32_t *list);
/* set list[] if (list != NULL) */
void imap_close(IMAP *imap);
#define IMAP_GET(x, no) ((IMAP *)x)->vmap[no].val
#define IMAP_SET(x, no, key) imap_set(((IMAP *)x), no, key)
#define IMAP_DEL(x, no, key) imap_del(((IMAP *)x), no)
#pragma pack(pop)
#endif
