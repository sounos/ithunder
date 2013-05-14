#ifndef _MTREE_H_
#define _MTREE_H_
typedef struct _MTRNODE
{
    int key;
    int data;
    struct _MTRNODE *left;
    struct _MTRNODE *right;
    struct _MTRNODE *parent;
}MTRNODE;
#define MTRNODE_LINE_NUM    2048
#define MTRNODE_LINE_MAX    1024 
#define MTRNODE_INCRE_NUM   2048
typedef struct _MTREE
{
    int total;
    int bits;
    int x;
    int nlines;
    MTRNODE *kmax;
    MTRNODE *kmin;
    MTRNODE *xnode;
    MTRNODE *root;
    MTRNODE *tmp;
    MTRNODE *list;
    MTRNODE *lines[MTRNODE_LINE_MAX];
    MTRNODE init[MTRNODE_LINE_NUM];
}MTREE;
#define MTRNODE_PUSH(mtree, mtrnode)                                                    \
do                                                                                      \
{                                                                                       \
    if((mtrnode->parent = mtree->list))                                                 \
        mtree->list = mtrnode;                                                          \
    else                                                                                \
    {                                                                                   \
        mtree->list = mtrnode;                                                          \
        mtrnode->parent = NULL;                                                         \
    }                                                                                   \
}while(0)
#define MTRNODE_POP(mtree, mtrnode)                                                     \
do                                                                                      \
{                                                                                       \
    if((mtrnode = mtree->list))                                                         \
    {                                                                                   \
        mtree->list = mtrnode->parent;                                                  \
    }                                                                                   \
    else                                                                                \
    {                                                                                   \
        if(mtree->nlines < MTRNODE_LINE_MAX && (mtrnode = mtree->lines[mtree->nlines++] \
                = (MTRNODE *)calloc(MTRNODE_LINE_NUM, sizeof(MTRNODE))))                \
        {                                                                               \
            mtree->x = MTRNODE_LINE_NUM;                                                \
            while(mtree->x > 1)                                                         \
            {                                                                           \
                mtree->tmp = &(mtrnode[--(mtree->x)]);                                  \
                MTRNODE_PUSH(mtree, mtree->tmp);                                        \
            }                                                                           \
        }                                                                               \
    }                                                                                   \
}while(0)
#define MTR(x) ((MTREE *)x)
#define MTREE_MAXK(x)  MTR(x)->kmax->key
#define MTREE_MINK(x)  MTR(x)->kmin->key
#define MTREE_TOTAL(x) MTR(x)->total
MTREE *mtree_init();
int mtree_add(MTREE *mtree, int key, int data, int *old);
int mtree_push(MTREE *mtree, int key, int data);
int mtree_get(MTREE *mtree, int key, int *data);
int mtree_pop_max(MTREE *mtree, int *key, int *data);
int mtree_pop_min(MTREE *mtree, int *key, int *data);
void mtree_reset(MTREE *mtree);
void mtree_clean(MTREE *mtree);
#define MTREE_ADD(map, key, data, old) mtree_add(MTR(map), (int)key, (int)data, old)
#define MTREE_PUSH(map, key, data) mtree_push(MTR(map), (int)key, (int)data)
#define MTREE_GET(map, key, pdata) mtree_get(MTR(map), (int)key, (int *)pdata)
#define MTREE_POP_MAX(map, pkey, pdata) mtree_pop_max(MTR(map), pkey, pdata)
#define MTREE_POP_MIN(map, pkey, pdata) mtree_pop_min(MTR(map), pkey, pdata)
#define MTREE_RESET(map) mtree_reset(MTR(map))
#define MTREE_CLEAN(map) mtree_clean(MTR(map))
#endif
