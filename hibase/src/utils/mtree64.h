#ifndef _MTREE64_H_
#define _MTREE64_H_
typedef struct _MTRNODE64
{
    int64_t key;
    int64_t data;
    struct _MTRNODE64 *left;
    struct _MTRNODE64 *right;
    struct _MTRNODE64 *parent;
}MTRNODE64;
#define MTRNODE64_LINE_NUM    2048
#define MTRNODE64_LINE_MAX    1024 
#define MTRNODE64_INCRE_NUM   2048
typedef struct _MTREE64
{
    int total;
    int bits;
    int x;
    int nlines;
    MTRNODE64 *kmax;
    MTRNODE64 *kmin;
    MTRNODE64 *xnode;
    MTRNODE64 *root;
    MTRNODE64 *tmp;
    MTRNODE64 *list;
    MTRNODE64 *lines[MTRNODE64_LINE_MAX];
    MTRNODE64 init[MTRNODE64_LINE_NUM];
}MTREE64;
#define MTRNODE64_PUSH(mtree64, mtrnode)                                                \
do                                                                                      \
{                                                                                       \
    if((mtrnode->parent = mtree64->list))                                               \
        mtree64->list = mtrnode;                                                        \
    else                                                                                \
    {                                                                                   \
        mtree64->list = mtrnode;                                                        \
        mtrnode->parent = NULL;                                                         \
    }                                                                                   \
}while(0)
#define MTRNODE64_POP(mtree64, mtrnode)                                                 \
do                                                                                      \
{                                                                                       \
    if((mtrnode = mtree64->list))                                                       \
    {                                                                                   \
        mtree64->list = mtrnode->parent;                                                \
    }                                                                                   \
    else                                                                                \
    {                                                                                   \
        if(mtree64->nlines < MTRNODE64_LINE_MAX                                         \
                && (mtrnode = mtree64->lines[mtree64->nlines++]                         \
                = (MTRNODE64 *)calloc(MTRNODE64_LINE_NUM, sizeof(MTRNODE64))))          \
        {                                                                               \
            mtree64->x = MTRNODE64_LINE_NUM;                                            \
            while(mtree64->x > 1)                                                       \
            {                                                                           \
                mtree64->tmp = &(mtrnode[--(mtree64->x)]);                              \
                MTRNODE64_PUSH(mtree64, mtree64->tmp);                                  \
            }                                                                           \
        }                                                                               \
    }                                                                                   \
}while(0)
#define MTR64(x) ((MTREE64 *)x)
#define MTREE64_MAXK(x)  MTR64(x)->kmax->key
#define MTREE64_MINK(x)  MTR64(x)->kmin->key
#define MTREE64_TOTAL(x) MTR64(x)->total
MTREE64 *mtree64_init();
int mtree64_add(MTREE64 *mtree64, int64_t key, int64_t data, int64_t *old);
int mtree64_push(MTREE64 *mtree64, int64_t key, int64_t data);
int mtree64_get(MTREE64 *mtree64, int64_t key, int64_t *data);
int mtree64_pop_max(MTREE64 *mtree64, int64_t *key, int64_t *data);
int mtree64_pop_min(MTREE64 *mtree64, int64_t *key, int64_t *data);
void mtree64_reset(MTREE64 *mtree64);
void mtree64_clean(MTREE64 *mtree64);
#define MTREE64_ADD(map, key, data, old) mtree64_add(MTR64(map), (int64_t)key, (int64_t)data, old)
#define MTREE64_PUSH(map, key, data) mtree64_push(MTR64(map), (int64_t)key, (int64_t)data)
#define MTREE64_GET(map, key, pdata) mtree64_get(MTR64(map), (int64_t)key, (int64_t *)pdata)
#define MTREE64_POP_MAX(map, pkey, pdata) mtree64_pop_max(MTR64(map), pkey, pdata)
#define MTREE64_POP_MIN(map, pkey, pdata) mtree64_pop_min(MTR64(map), pkey, pdata)
#define MTREE64_RESET(map) mtree64_reset(MTR64(map))
#define MTREE64_CLEAN(map) mtree64_clean(MTR64(map))
#endif
