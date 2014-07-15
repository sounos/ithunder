#ifndef	_IMMX_H
#define	_IMMX_H
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "xmm.h"
typedef struct _IMXNODE
{
    int64_t  key;
    int data;
    int rbe_color;         
    struct _IMXNODE *rbe_left;     
    struct _IMXNODE *rbe_right;    
    struct _IMXNODE *rbe_parent;   
}IMXNODE;
#define IMX_LINE_MAX     1024
#define IMX_LINE_NUM     1024
typedef struct _IMMX
{
    IMXNODE node;
    IMXNODE *rbh_root;
    IMXNODE *old;
    IMXNODE *max;
    IMXNODE *min;
    IMXNODE *bnode;
    IMXNODE *p;
    IMXNODE *pp;
    IMXNODE *head;
    IMXNODE *tail;
    IMXNODE *tmp;
    IMXNODE *xtmp;
    IMXNODE *freelist[IMX_LINE_MAX];
    int    nfreelist;
    int loop;
    int count;
    int total;
    int free_count;
}IMMX;
#define PIMX(ptr) ((IMMX *)ptr)
#define IMX_NEGINF	-1
#define IMX_INF	    1
#define IMX_BLACK	0
#define IMX_RED		1
#define IMX_LEFT(elm)		(elm->rbe_left)
#define IMX_RIGHT(elm)		(elm->rbe_right)
#define IMX_PARENT(elm)		(elm->rbe_parent)
#define IMX_COLOR(elm)		(elm->rbe_color)
#define IMX_ROOT(head)       (PIMX(head)->rbh_root)
#define PIMX_PARENT(head)    (PIMX(head)->parent)
#define PIMX_GPARENT(head)   (PIMX(head)->gparent)
#define PIMX_CHILD(head)     (PIMX(head)->child)
#define PIMX_OLEFT(head)     (PIMX(head)->oleft)
#define PIMX_ORIGHT(head)    (PIMX(head)->oright)
#define PIMX_COLOR(head)     (PIMX(head)->color)
#define PIMX_OLD(head)       (PIMX(head)->old)
#define PIMX_TMP(head)       (PIMX(head)->tmp)
#define PIMX_MAX(head)       (PIMX(head)->max)
#define PIMX_MIN(head)       (PIMX(head)->min)
#define PIMX_MAXK(head)      (PIMX(head)->max->key)
#define PIMX_MINK(head)      (PIMX(head)->min->key)
#define IMX_EMPTY(head)      (IMX_ROOT(head) == NULL)
#define IMX_NODE_SET(elm, old)                                                              \
do                                                                                          \
{                                                                                           \
    IMX_PARENT(elm) = IMX_PARENT(old);                                                      \
    IMX_LEFT(elm) = IMX_LEFT(old);                                                          \
    IMX_RIGHT(elm) = IMX_RIGHT(old);                                                        \
    IMX_COLOR(elm) = IMX_COLOR(old);                                                        \
}while(0)
#define IMX_SET(elm, parent)                                                                \
do                                                                                          \
{					                                                                        \
	IMX_PARENT(elm) = parent;					                                            \
	IMX_LEFT(elm) = IMX_RIGHT(elm) = NULL;		                                            \
	IMX_COLOR(elm) = IMX_RED;					                                            \
}while(0)

#define IMX_SET_BLACKRED(black, red)                                                        \
do                                                                                          \
{				                                                                            \
	IMX_COLOR(black) = IMX_BLACK;				                                            \
	IMX_COLOR(red) = IMX_RED;					                                            \
}while(0)

#ifndef IMX_AUGMENT
#define IMX_AUGMENT(x)
#endif

#define IMX_ROTATE_LEFT(head, elm, tmp)                                                     \
do                                                                                          \
{			                                                                                \
	(tmp) = IMX_RIGHT(elm);					                                                \
	if ((IMX_RIGHT(elm) = IMX_LEFT(tmp)))                                                   \
    {		                                                                                \
		IMX_PARENT(IMX_LEFT(tmp)) = (elm);		                                            \
	}								                                                        \
	IMX_AUGMENT(elm);						                                                \
	if ((IMX_PARENT(tmp) = IMX_PARENT(elm)))                                                \
    {		                                                                                \
		if ((elm) == IMX_LEFT(IMX_PARENT(elm)))	                                            \
			IMX_LEFT(IMX_PARENT(elm)) = (tmp);	                                            \
		else							                                                    \
			IMX_RIGHT(IMX_PARENT(elm)) = (tmp);	                                            \
	} else								                                                    \
		IMX_ROOT(head) = (tmp);				                                                \
	IMX_LEFT(tmp) = (elm);					                                                \
	IMX_PARENT(elm) = (tmp);					                                            \
	IMX_AUGMENT(tmp);						                                                \
	if ((IMX_PARENT(tmp))) IMX_AUGMENT(IMX_PARENT(tmp));			                        \
}while(0)

#define IMX_ROTATE_RIGHT(head, elm, tmp)                                                    \
do                                                                                          \
{			                                                                                \
	(tmp) = IMX_LEFT(elm);					                                                \
	if ((IMX_LEFT(elm) = IMX_RIGHT(tmp)))                                                   \
    {		                                                                                \
		IMX_PARENT(IMX_RIGHT(tmp)) = (elm);		                                            \
	}								                                                        \
	IMX_AUGMENT(elm);						                                                \
	if ((IMX_PARENT(tmp) = IMX_PARENT(elm)))                                                \
    {		                                                                                \
		if ((elm) == IMX_LEFT(IMX_PARENT(elm)))	                                            \
			IMX_LEFT(IMX_PARENT(elm)) = (tmp);	                                            \
		else							                                                    \
			IMX_RIGHT(IMX_PARENT(elm)) = (tmp);	                                            \
	} else								                                                    \
		IMX_ROOT(head) = (tmp);				                                                \
	IMX_RIGHT(tmp) = (elm);					                                                \
	IMX_PARENT(elm) = (tmp);					                                            \
	IMX_AUGMENT(tmp);						                                                \
	if ((IMX_PARENT(tmp)))IMX_AUGMENT(IMX_PARENT(tmp));			                            \
}while(0)
/* remove node */
IMXNODE *imx_remove(IMMX *map, IMXNODE *elm);
/* insert new node */
IMXNODE *imx_insert(IMMX *map, IMXNODE *elm);
/* find node */
IMXNODE *imx_find(IMMX *map, IMXNODE *elm);
/* next node for great */
IMXNODE *imx_next(IMXNODE *elm);
/* prev node for less */
IMXNODE *imx_prev(IMXNODE *elm);
/* find min/max node */
IMXNODE *imx_minmax(IMMX *map, int val);

#define IMX_MIN(map) imx_minmax(map, IMX_NEGINF)
#define IMX_MAX(map)	imx_minmax(map, IMX_INF)
#define IMMX_MAX(ptr) PIMX(ptr)->max
#define IMMX_MIN(ptr) PIMX(ptr)->min
#define IMMX_INIT() xmm_mnew(sizeof(IMMX))

/* add to min/max */
#define IMMX_MINMAX_ADD(ptr, node)                                                          \
do                                                                                          \
{                                                                                           \
    if(PIMX(ptr)->max  == NULL || node->key > PIMX(ptr)->max->key)                          \
    {                                                                                       \
        PIMX(ptr)->max = node;                                                              \
    }                                                                                       \
    if(PIMX(ptr)->min == NULL || node->key < PIMX(ptr)->min->key)                           \
        PIMX(ptr)->min = node;                                                              \
}while(0)

/* rebuild min/max */
#define IMMX_MINMAX_REBUILD(ptr, node)                                                      \
do                                                                                          \
{                                                                                           \
    if(node == PIMX(ptr)->max)                                                              \
    {                                                                                       \
        PIMX(ptr)->max = imx_prev(node);                                                    \
    }                                                                                       \
    if(node == PIMX(ptr)->min)                                                              \
    {                                                                                       \
        PIMX(ptr)->min = imx_next(node);                                                    \
    }                                                                                       \
}while(0)

/* push free node to freelist */
#define IMMX_PUSH_NODE(ptr, node)                                                           \
do                                                                                          \
{                                                                                           \
    memset(node, 0, sizeof(IMXNODE));                                                       \
    if(PIMX(ptr)->tail)                                                                     \
    {                                                                                       \
        PIMX(ptr)->tail->rbe_parent = node;                                                 \
        PIMX(ptr)->tail = node;                                                             \
    }                                                                                       \
    else                                                                                    \
    {                                                                                       \
        PIMX(ptr)->head = PIMX(ptr)->tail = node;                                           \
    }                                                                                       \
    PIMX(ptr)->free_count++;                                                                \
}while(0)

/* pop node from freelist */
#define IMMX_POP_NODE(ptr, node)                                                            \
do                                                                                          \
{                                                                                           \
    if(PIMX(ptr)->head)                                                                     \
    {                                                                                       \
        node = PIMX(ptr)->head;                                                             \
        PIMX(ptr)->head = PIMX(ptr)->head->rbe_parent;                                      \
        if(PIMX(ptr)->head == NULL) PIMX(ptr)->tail = NULL;                                 \
        memset(node, 0, sizeof(IMXNODE));                                                   \
        PIMX(ptr)->free_count--;                                                            \
    }                                                                                       \
    else                                                                                    \
    {                                                                                       \
        if(PIMX(ptr)->nfreelist < IMX_LINE_MAX)                                             \
        {                                                                                   \
            if((PIMX(ptr)->tmp = (IMXNODE *)xmm_new(sizeof(IMXNODE) * IMX_LINE_NUM)))       \
            {                                                                               \
                PIMX(ptr)->freelist[PIMX(ptr)->nfreelist] = PIMX(ptr)->tmp;                 \
                PIMX(ptr)->nfreelist++;                                                     \
                node = &(PIMX(ptr)->tmp[0]);                                                \
                memset(node, 0, sizeof(IMXNODE));                                           \
                PIMX(ptr)->loop = 1;                                                        \
                PIMX(ptr)->total += IMX_LINE_NUM;                                           \
                while(PIMX(ptr)->loop < IMX_LINE_NUM)                                       \
                {                                                                           \
                    PIMX(ptr)->xtmp = &(PIMX(ptr)->tmp[PIMX(ptr)->loop]);                   \
                    IMMX_PUSH_NODE(ptr, PIMX(ptr)->xtmp);                                   \
                    PIMX(ptr)->loop++;                                                      \
                }                                                                           \
            }                                                                               \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* add key/val(dp) olddp is return if key exists */
#define IMMX_ADD(ptr, nkey)                                                                 \
do                                                                                          \
{                                                                                           \
    if(ptr)                                                                                 \
    {                                                                                       \
        IMMX_POP_NODE(ptr, PIMX(ptr)->p);                                                   \
        if((PIMX(ptr)->pp = PIMX(ptr)->p))                                                  \
        {                                                                                   \
            PIMX(ptr)->p->key = nkey;                                                       \
            PIMX(ptr)->p->data = 1;                                                         \
            PIMX(ptr)->old = imx_insert(PIMX(ptr), PIMX(ptr)->p);                           \
            if(PIMX(ptr)->old)                                                              \
            {                                                                               \
                PIMX(ptr)->old->data++;                                                     \
                IMMX_PUSH_NODE(ptr, PIMX(ptr)->pp);                                         \
            }                                                                               \
            else                                                                            \
            {                                                                               \
                IMMX_MINMAX_ADD(ptr, PIMX(ptr)->pp);                                        \
                PIMX(ptr)->count++;                                                         \
            }                                                                               \
        }                                                                                   \
    }                                                                                       \
}while(0)

#define IMMX_SUM(ptr, nkey, ndata)                                                          \
do                                                                                          \
{                                                                                           \
    if(ptr)                                                                                 \
    {                                                                                       \
        IMMX_POP_NODE(ptr, PIMX(ptr)->p);                                                   \
        if((PIMX(ptr)->pp = PIMX(ptr)->p))                                                  \
        {                                                                                   \
            PIMX(ptr)->p->key = nkey;                                                       \
            PIMX(ptr)->p->data = ndata;                                                     \
            PIMX(ptr)->old = imx_insert(PIMX(ptr), PIMX(ptr)->p);                           \
            if(PIMX(ptr)->old)                                                              \
            {                                                                               \
                PIMX(ptr)->old->data += ndata;                                              \
                IMMX_PUSH_NODE(ptr, PIMX(ptr)->pp);                                         \
            }                                                                               \
            else                                                                            \
            {                                                                               \
                IMMX_MINMAX_ADD(ptr, PIMX(ptr)->pp);                                        \
                PIMX(ptr)->count++;                                                         \
            }                                                                               \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* get val(dp) of nkey */
#define IMMX_GET(ptr, nkey, ret)                                                            \
do                                                                                          \
{                                                                                           \
    if(ptr)                                                                                 \
    {                                                                                       \
        PIMX(ptr)->node.key = nkey;                                                         \
        PIMX(ptr)->bnode = &(PIMX(ptr)->node);                                              \
        PIMX(ptr)->p = imx_find(PIMX(ptr), PIMX(ptr)->bnode);                               \
        if(PIMX(ptr)->p)                                                                    \
        {                                                                                   \
            ret = PIMX(ptr)->p->data;                                                       \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* delete node(key/val) from map */
#define IMMX_DEL(ptr, nkey, ret)                                                            \
do                                                                                          \
{                                                                                           \
    if(ptr)                                                                                 \
    {                                                                                       \
        PIMX(ptr)->node.key = nkey;                                                         \
        PIMX(ptr)->bnode = &(PIMX(ptr)->node);                                              \
        PIMX(ptr)->old = imx_remove(PIMX(ptr), PIMX(ptr)->bnode);                           \
        if(PIMX(ptr)->old)                                                                  \
        {                                                                                   \
            IMMX_PUSH_NODE(ptr, PIMX(ptr)->old);                                            \
            PIMX(ptr)->count--;                                                             \
            ret = PIMX(ptr)->old->data;                                                     \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* pop min node */
#define IMMX_POP_MIN(ptr, nkey, ret)                                                        \
do                                                                                          \
{                                                                                           \
    if(ptr)                                                                                 \
    {                                                                                       \
        if(PIMX(ptr)->min)                                                                  \
        {                                                                                   \
            nkey = PIMX(ptr)->min->key;                                                     \
            ret  = PIMX(ptr)->min->data;                                                    \
            PIMX(ptr)->p = PIMX(ptr)->min;                                                  \
            PIMX(ptr)->old = imx_remove(PIMX(ptr), PIMX(ptr)->p);                           \
            IMMX_PUSH_NODE(ptr, PIMX(ptr)->old);                                            \
            PIMX(ptr)->count--;                                                             \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* pop max node */
#define IMMX_POP_MAX(ptr, nkey, ret)                                                        \
do                                                                                          \
{                                                                                           \
    if(ptr)                                                                                 \
    {                                                                                       \
        if(PIMX(ptr)->max)                                                                  \
        {                                                                                   \
            nkey = PIMX(ptr)->max->key;                                                     \
            ret  = PIMX(ptr)->max->data;                                                    \
            PIMX(ptr)->p = PIMX(ptr)->max;                                                  \
            PIMX(ptr)->old = imx_remove(PIMX(ptr), PIMX(ptr)->p);                           \
            IMMX_PUSH_NODE(ptr, PIMX(ptr)->old);                                            \
            PIMX(ptr)->count--;                                                             \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* find max node and pop it */
#define IMMX_POP_RMAX(ptr, nkey, ret)                                                       \
do                                                                                          \
{                                                                                           \
    if(ptr)                                                                                 \
    {                                                                                       \
        if(PIMX(ptr)->max)                                                                  \
        {                                                                                   \
            PIMX(ptr)->bnode = NULL;                                                        \
            PIMX(ptr)->bnode = IMX_MAX(PIMX(ptr));                                          \
            if(PIMX(ptr)->bnode)                                                            \
            {                                                                               \
                nkey = PIMX(ptr)->bnode->key;                                               \
                ret = PIMX(ptr)->bnode->data;                                               \
                PIMX(ptr)->old = imx_remove(PIMX(ptr), PIMX(ptr)->bnode);                   \
                IMMX_PUSH_NODE(ptr, PIMX(ptr)->old);                                        \
                PIMX(ptr)->count--;                                                         \
            }                                                                               \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* find min node and pop it */
#define IMMX_POP_RMIN(ptr, nkey, ret)                                                       \
do                                                                                          \
{                                                                                           \
    if(ptr)                                                                                 \
    {                                                                                       \
        if(PIMX(ptr)->min)                                                                  \
        {                                                                                   \
            PIMX(ptr)->bnode = NULL;                                                        \
            PIMX(ptr)->bnode = IMX_MIN(PIMX(ptr));                                          \
            if(PIMX(ptr)->bnode)                                                            \
            {                                                                               \
                nkey = PIMX(ptr)->bnode->key;                                               \
                ret = PIMX(ptr)->bnode->data;                                               \
                PIMX(ptr)->old = imx_remove(PIMX(ptr), PIMX(ptr)->bnode);                   \
                IMMX_PUSH_NODE(ptr, PIMX(ptr)->old);                                        \
                PIMX(ptr)->count--;                                                         \
            }                                                                               \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* reset map */
#define IMMX_RESET(ptr)                                                                     \
do                                                                                          \
{                                                                                           \
    if(ptr)                                                                                 \
    {                                                                                       \
        PIMX(ptr)->p = NULL;                                                                \
        while((PIMX(ptr)->p = IMX_MAX(PIMX(ptr))))                                          \
        {                                                                                   \
            PIMX(ptr)->old = imx_remove(PIMX(ptr), PIMX(ptr)->p);                           \
            IMMX_PUSH_NODE(ptr, PIMX(ptr)->old);                                            \
            PIMX(ptr)->count--;                                                             \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* clean map */
#define IMMX_CLEAN(ptr)                                                                     \
do                                                                                          \
{                                                                                           \
    if(ptr)                                                                                 \
    {                                                                                       \
        PIMX(ptr)->loop = 0;                                                                \
        while(PIMX(ptr)->loop < PIMX(ptr)->nfreelist)                                       \
        {                                                                                   \
            PIMX(ptr)->tmp = PIMX(ptr)->freelist[PIMX(ptr)->loop];                          \
            xmm_free(PIMX(ptr)->tmp, sizeof(IMXNODE) * IMX_LINE_NUM);                       \
            PIMX(ptr)->loop++;                                                              \
        }                                                                                   \
        xmm_free(ptr, sizeof(IMMX));                                                        \
        ptr = NULL;                                                                         \
    }                                                                                       \
}while(0)
#endif
