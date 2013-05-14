#ifndef _MTREE_H_
#define _MTREE_H_
typedef struct _MTREENODE
{
    int key;
    int data;
    struct _MTREENODE *left;
    struct _MTREENODE *right;
    struct _MTREENODE *parent;
}MTREENODE;
#define MTREENODE_LINE_NUM    2048
#define MTREENODE_LINE_MAX    1024 
#define MTREENODE_INCRE_NUM   2048
typedef struct _MTREE
{
    int total;
    int bits;
    int x;
    int nlines;
    MTREENODE *kmax;
    MTREENODE *kmin;
    MTREENODE *xnode;
    MTREENODE *root;
    MTREENODE *tmp;
    MTREENODE *list;
    MTREENODE *lines[MTREENODE_LINE_MAX];
    MTREENODE init[MTREENODE_LINE_NUM];
}MTREE;
#define MTREENODE_PUSH(mtree, mtrnode)                                                    \
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
#define MTREENODE_POP(mtree, mtrnode)                                                     \
do                                                                                      \
{                                                                                       \
    if((mtrnode = mtree->list))                                                         \
    {                                                                                   \
        mtree->list = mtrnode->parent;                                                  \
    }                                                                                   \
    else                                                                                \
    {                                                                                   \
        if(mtree->nlines < MTREENODE_LINE_MAX && (mtrnode = mtree->lines[mtree->nlines++] \
                = (MTREENODE *)calloc(MTREENODE_LINE_NUM, sizeof(MTREENODE))))                \
        {                                                                               \
            mtree->x = MTREENODE_LINE_NUM;                                                \
            while(mtree->x > 1)                                                         \
            {                                                                           \
                mtree->tmp = &(mtrnode[--(mtree->x)]);                                  \
                MTREENODE_PUSH(mtree, mtree->tmp);                                        \
            }                                                                           \
        }                                                                               \
    }                                                                                   \
}while(0)
#define MTRX(x) ((MTREE *)x)
#define MTREE_MAXK(x)  MTRX(x)->kmax->key
#define MTREE_MINK(x)  MTRX(x)->kmin->key
#define MTREE_TOTAL(x) MTRX(x)->total
MTREE *mtree_init();
int mtree_add(MTREE *mtree, int key, int data, int *old);
int mtree_push(MTREE *mtree, int key, int data);
int mtree_get(MTREE *mtree, int key, int *data);
int mtree_pop_max(MTREE *mtree, int *key, int *data);
int mtree_pop_min(MTREE *mtree, int *key, int *data);
void mtree_reset(MTREE *mtree);
void mtree_clean(MTREE *mtree);
#define MTREE_ADD(map, key, data, old) mtree_add(MTRX(map), (int)key, (int)data, old)
#define MTREE_PUSH(map, key, data) mtree_push(MTRX(map), (int)key, (int)data)
#define MTREE_GET(map, key, pdata) mtree_get(MTRX(map), (int)key, (int *)pdata)
#define MTREE_POP_MAX(map, pkey, pdata) mtree_pop_max(MTRX(map), pkey, pdata)
#define MTREE_POP_MIN(map, pkey, pdata) mtree_pop_min(MTRX(map), pkey, pdata)
#define MTREE_RESET(map) mtree_reset(MTRX(map))
#define MTREE_CLEAN(map) mtree_clean(MTRX(map))
#endif
