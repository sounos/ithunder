#ifndef	_IMMX_H
#define	_IMMX_H
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "xmm.h"
typedef struct _IMMXNODE
{
    int  key;
    int  data;
    struct _IMMXNODE *rbe_left;     
    struct _IMMXNODE *rbe_right;    
    struct _IMMXNODE *rbe_parent;   
    int rbe_color;         
}IMMXNODE;
#define IMMX_LINE_MAX     1024
#define IMMX_LINE_NUM     1024
typedef struct _IMMX
{
    IMMXNODE node;
    IMMXNODE *rbh_root;
    IMMXNODE *old;
    IMMXNODE *max;
    IMMXNODE *min;
    IMMXNODE *bnode;
    IMMXNODE *p;
    IMMXNODE *pp;
    IMMXNODE *head;
    IMMXNODE *tail;
    IMMXNODE *tmp;
    IMMXNODE *xtmp;
    IMMXNODE *freelist[IMMX_LINE_MAX];
    int    nfreelist;
    int loop;
    int count;
    int total;
    int free_count;
}IMMX;
#define PIMMX(ptr) ((IMMX *)ptr)
#define IMMX_NEGINF	-1
#define IMMX_INF	    1
#define IMMX_BLACK	0
#define IMMX_RED		1
#define IMMX_LEFT(elm)		(elm->rbe_left)
#define IMMX_RIGHT(elm)		(elm->rbe_right)
#define IMMX_PARENT(elm)		(elm->rbe_parent)
#define IMMX_COLOR(elm)		(elm->rbe_color)
#define IMMX_ROOT(head)       (PIMMX(head)->rbh_root)
#define PIMMX_PARENT(head)    (PIMMX(head)->parent)
#define PIMMX_GPARENT(head)   (PIMMX(head)->gparent)
#define PIMMX_CHILD(head)     (PIMMX(head)->child)
#define PIMMX_OLEFT(head)     (PIMMX(head)->oleft)
#define PIMMX_ORIGHT(head)    (PIMMX(head)->oright)
#define PIMMX_COLOR(head)     (PIMMX(head)->color)
#define PIMMX_OLD(head)       (PIMMX(head)->old)
#define PIMMX_TMP(head)       (PIMMX(head)->tmp)
#define PIMMX_MAX(head)       (PIMMX(head)->max)
#define PIMMX_MIN(head)       (PIMMX(head)->min)
#define PIMMX_MAXK(head)      (PIMMX(head)->max->key)
#define PIMMX_MINK(head)      (PIMMX(head)->min->key)
#define IMMX_EMPTY(head)      (IMMX_ROOT(head) == NULL)
#define IMMX_NODE_SET(elm, old)                                                             \
do                                                                                          \
{                                                                                           \
    IMMX_PARENT(elm) = IMMX_PARENT(old);                                                    \
    IMMX_LEFT(elm) = IMMX_LEFT(old);                                                        \
    IMMX_RIGHT(elm) = IMMX_RIGHT(old);                                                      \
    IMMX_COLOR(elm) = IMMX_COLOR(old);                                                      \
}while(0)
#define IMMX_SET(elm, parent)                                                               \
do                                                                                          \
{					                                                                        \
	IMMX_PARENT(elm) = parent;					                                            \
	IMMX_LEFT(elm) = IMMX_RIGHT(elm) = NULL;		                                        \
	IMMX_COLOR(elm) = IMMX_RED;					                                            \
}while(0)

#define IMMX_SET_BLACKRED(black, red)                                                       \
do                                                                                          \
{				                                                                            \
	IMMX_COLOR(black) = IMMX_BLACK;				                                            \
	IMMX_COLOR(red) = IMMX_RED;					                                            \
}while(0)

#ifndef IMMX_AUGMENT
#define IMMX_AUGMENT(x)
#endif

#define IMMX_ROTATE_LEFT(head, elm, tmp)                                                    \
do                                                                                          \
{			                                                                                \
	(tmp) = IMMX_RIGHT(elm);					                                            \
	if ((IMMX_RIGHT(elm) = IMMX_LEFT(tmp)))                                                 \
    {		                                                                                \
		IMMX_PARENT(IMMX_LEFT(tmp)) = (elm);		                                        \
	}								                                                        \
	IMMX_AUGMENT(elm);						                                                \
	if ((IMMX_PARENT(tmp) = IMMX_PARENT(elm)))                                              \
    {		                                                                                \
		if ((elm) == IMMX_LEFT(IMMX_PARENT(elm)))	                                        \
			IMMX_LEFT(IMMX_PARENT(elm)) = (tmp);	                                        \
		else							                                                    \
			IMMX_RIGHT(IMMX_PARENT(elm)) = (tmp);	                                        \
	} else								                                                    \
		IMMX_ROOT(head) = (tmp);				                                            \
	IMMX_LEFT(tmp) = (elm);					                                                \
	IMMX_PARENT(elm) = (tmp);					                                            \
	IMMX_AUGMENT(tmp);						                                                \
	if ((IMMX_PARENT(tmp))) IMMX_AUGMENT(IMMX_PARENT(tmp));			                        \
}while(0)

#define IMMX_ROTATE_RIGHT(head, elm, tmp)                                                   \
do                                                                                          \
{			                                                                                \
	(tmp) = IMMX_LEFT(elm);					                                                \
	if ((IMMX_LEFT(elm) = IMMX_RIGHT(tmp)))                                                 \
    {		                                                                                \
		IMMX_PARENT(IMMX_RIGHT(tmp)) = (elm);		                                        \
	}								                                                        \
	IMMX_AUGMENT(elm);						                                                \
	if ((IMMX_PARENT(tmp) = IMMX_PARENT(elm)))                                              \
    {		                                                                                \
		if ((elm) == IMMX_LEFT(IMMX_PARENT(elm)))	                                        \
			IMMX_LEFT(IMMX_PARENT(elm)) = (tmp);	                                        \
		else							                                                    \
			IMMX_RIGHT(IMMX_PARENT(elm)) = (tmp);	                                        \
	} else								                                                    \
		IMMX_ROOT(head) = (tmp);				                                            \
	IMMX_RIGHT(tmp) = (elm);					                                            \
	IMMX_PARENT(elm) = (tmp);					                                            \
	IMMX_AUGMENT(tmp);						                                                \
	if ((IMMX_PARENT(tmp)))IMMX_AUGMENT(IMMX_PARENT(tmp));			                        \
}while(0)
/* remove node */
IMMXNODE *immx_remove(IMMX *map, IMMXNODE *elm);
/* insert new node */
IMMXNODE *immx_insert(IMMX *map, IMMXNODE *elm);
/* find node */
IMMXNODE *immx_find(IMMX *map, IMMXNODE *elm);
/* next node for great */
IMMXNODE *immx_next(IMMXNODE *elm);
/* prev node for less */
IMMXNODE *immx_prev(IMMXNODE *elm);
/* find min/max node */
IMMXNODE *immx_minmax(IMMX *map, int val);

#define IMMX_MIN(map) immx_minmax(map, IMMX_NEGINF)
#define IMMX_MAX(map)	immx_minmax(map, IMMX_INF)
#define IMMX_MAX(ptr) PIMMX(ptr)->max
#define IMMX_MIN(ptr) PIMMX(ptr)->min
#define IMMX_INIT() xmm_mnew(sizeof(IMMX))

/* add to min/max */
#define IMMX_MINMAX_ADD(ptr, node)                                                          \
do                                                                                          \
{                                                                                           \
    if(PIMMX(ptr)->max  == NULL || node->key > PIMMX(ptr)->max->key)                        \
    {                                                                                       \
        PIMMX(ptr)->max = node;                                                             \
    }                                                                                       \
    if(PIMMX(ptr)->min == NULL || node->key < PIMMX(ptr)->min->key)                         \
        PIMMX(ptr)->min = node;                                                             \
}while(0)

/* rebuild min/max */
#define IMMX_MINMAX_REBUILD(ptr, node)                                                      \
do                                                                                          \
{                                                                                           \
    if(node == PIMMX(ptr)->max)                                                             \
    {                                                                                       \
        PIMMX(ptr)->max = immx_prev(PIMMX(ptr));                                            \
    }                                                                                       \
    if(node == PIMMX(ptr)->min)                                                             \
    {                                                                                       \
        PIMMX(ptr)->min = immx_next(PIMMX(ptr));                                            \
    }                                                                                       \
}while(0)

/* push free node to freelist */
#define IMMX_PUSH_NODE(ptr, node)                                                           \
do                                                                                          \
{                                                                                           \
    memset(node, 0, sizeof(IMMXNODE));                                                      \
    if(PIMMX(ptr)->tail)                                                                    \
    {                                                                                       \
        PIMMX(ptr)->tail->rbe_parent = node;                                                \
        PIMMX(ptr)->tail = node;                                                            \
    }                                                                                       \
    else                                                                                    \
    {                                                                                       \
        PIMMX(ptr)->head = PIMMX(ptr)->tail = node;                                         \
    }                                                                                       \
    PIMMX(ptr)->free_count++;                                                               \
}while(0)

/* pop node from freelist */
#define IMMX_POP_NODE(ptr, node)                                                            \
do                                                                                          \
{                                                                                           \
    if(PIMMX(ptr)->head)                                                                    \
    {                                                                                       \
        node = PIMMX(ptr)->head;                                                            \
        PIMMX(ptr)->head = PIMMX(ptr)->head->rbe_parent;                                    \
        if(PIMMX(ptr)->head == NULL) PIMMX(ptr)->tail = NULL;                               \
        memset(node, 0, sizeof(IMMXNODE));                                                  \
        PIMMX(ptr)->free_count--;                                                           \
    }                                                                                       \
    else                                                                                    \
    {                                                                                       \
        if(PIMMX(ptr)->nfreelist < IMMX_LINE_MAX)                                           \
        {                                                                                   \
            if((PIMMX(ptr)->tmp = (IMMXNODE *)xmm_new(sizeof(IMMXNODE) * IMMX_LINE_NUM)))   \
            {                                                                               \
                PIMMX(ptr)->freelist[PIMMX(ptr)->nfreelist] = PIMMX(ptr)->tmp;              \
                PIMMX(ptr)->nfreelist++;                                                    \
                node = &(PIMMX(ptr)->tmp[0]);                                               \
                memset(node, 0, sizeof(IMMXNODE));                                          \
                PIMMX(ptr)->loop = 1;                                                       \
                PIMMX(ptr)->total += IMMX_LINE_NUM;                                         \
                while(PIMMX(ptr)->loop < IMMX_LINE_NUM)                                     \
                {                                                                           \
                    PIMMX(ptr)->xtmp = &(PIMMX(ptr)->tmp[PIMMX(ptr)->loop]);                \
                    IMMX_PUSH_NODE(ptr, PIMMX(ptr)->xtmp);                                  \
                    PIMMX(ptr)->loop++;                                                     \
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
        IMMX_POP_NODE(ptr, PIMMX(ptr)->p);                                                  \
        if((PIMMX(ptr)->pp = PIMMX(ptr)->p))                                                \
        {                                                                                   \
            PIMMX(ptr)->p->key = nkey;                                                      \
            PIMMX(ptr)->p->data = 1;                                                        \
            PIMMX(ptr)->old = immx_insert(PIMMX(ptr), PIMMX(ptr)->p);                       \
            if(PIMMX(ptr)->old)                                                             \
            {                                                                               \
                PIMMX(ptr)->old->data++;                                                    \
                IMMX_PUSH_NODE(ptr, PIMMX(ptr)->pp);                                        \
            }                                                                               \
            else                                                                            \
            {                                                                               \
                IMMX_MINMAX_ADD(ptr, PIMMX(ptr)->pp);                                       \
                PIMMX(ptr)->count++;                                                        \
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
        PIMMX(ptr)->node.key = nkey;                                                        \
        PIMMX(ptr)->bnode = &(PIMMX(ptr)->node);                                            \
        PIMMX(ptr)->p = immx_find(PIMMX(ptr), PIMMX(ptr)->bnode);                           \
        if(PIMMX(ptr)->p)                                                                   \
        {                                                                                   \
            ret = PIMMX(ptr)->p->data;                                                      \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* delete node(key/val) from map */
#define IMMX_DEL(ptr, nkey, ret)                                                            \
do                                                                                          \
{                                                                                           \
    if(ptr)                                                                                 \
    {                                                                                       \
        PIMMX(ptr)->node.key = nkey;                                                        \
        PIMMX(ptr)->bnode = &(PIMMX(ptr)->node);                                            \
        PIMMX(ptr)->old = immx_remove(PIMMX(ptr), PIMMX(ptr)->bnode);                       \
        if(PIMMX(ptr)->old)                                                                 \
        {                                                                                   \
            IMMX_PUSH_NODE(ptr, PIMMX(ptr)->old);                                           \
            PIMMX(ptr)->count--;                                                            \
            ret = PIMMX(ptr)->old->data;                                                    \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* pop min node */
#define IMMX_POP_MIN(ptr, nkey, ret)                                                        \
do                                                                                          \
{                                                                                           \
    if(ptr)                                                                                 \
    {                                                                                       \
        if(PIMMX(ptr)->min)                                                                 \
        {                                                                                   \
            nkey = PIMMX(ptr)->min->key;                                                    \
            ret  = PIMMX(ptr)->min->data;                                                   \
            PIMMX(ptr)->p = PIMMX(ptr)->min;                                                \
            PIMMX(ptr)->old = immx_remove(PIMMX(ptr), PIMMX(ptr)->p);                       \
            IMMX_PUSH_NODE(ptr, PIMMX(ptr)->old);                                           \
            PIMMX(ptr)->count--;                                                            \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* pop max node */
#define IMMX_POP_MAX(ptr, nkey, ret)                                                        \
do                                                                                          \
{                                                                                           \
    if(ptr)                                                                                 \
    {                                                                                       \
        if(PIMMX(ptr)->max)                                                                 \
        {                                                                                   \
            nkey = PIMMX(ptr)->max->key;                                                    \
            ret  = PIMMX(ptr)->max->data;                                                   \
            PIMMX(ptr)->p = PIMMX(ptr)->max;                                                \
            PIMMX(ptr)->old = immx_remove(PIMMX(ptr), PIMMX(ptr)->p);                       \
            IMMX_PUSH_NODE(ptr, PIMMX(ptr)->old);                                           \
            PIMMX(ptr)->count--;                                                            \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* find max node and pop it */
#define IMMX_POP_RMAX(ptr, nkey, ret)                                                       \
do                                                                                          \
{                                                                                           \
    if(ptr)                                                                                 \
    {                                                                                       \
        if(PIMMX(ptr)->max)                                                                 \
        {                                                                                   \
            PIMMX(ptr)->bnode = NULL;                                                       \
            PIMMX(ptr)->bnode = IMMX_MAX(PIMMX(ptr));                                       \
            if(PIMMX(ptr)->bnode)                                                           \
            {                                                                               \
                nkey = PIMMX(ptr)->bnode->key;                                              \
                ret = PIMMX(ptr)->bnode->data;                                              \
                PIMMX(ptr)->old = immx_remove(PIMMX(ptr), PIMMX(ptr)->bnode);               \
                IMMX_PUSH_NODE(ptr, PIMMX(ptr)->old);                                       \
                PIMMX(ptr)->count--;                                                        \
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
        if(PIMMX(ptr)->min)                                                                 \
        {                                                                                   \
            PIMMX(ptr)->bnode = NULL;                                                       \
            PIMMX(ptr)->bnode = IMMX_MIN(PIMMX(ptr));                                       \
            if(PIMMX(ptr)->bnode)                                                           \
            {                                                                               \
                nkey = PIMMX(ptr)->bnode->key;                                              \
                ret = PIMMX(ptr)->bnode->data;                                              \
                PIMMX(ptr)->old = immx_remove(PIMMX(ptr), PIMMX(ptr)->bnode);               \
                IMMX_PUSH_NODE(ptr, PIMMX(ptr)->old);                                       \
                PIMMX(ptr)->count--;                                                        \
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
        PIMMX(ptr)->p = NULL;                                                               \
        while((PIMMX(ptr)->p = IMMX_MAX(PIMMX(ptr))))                                       \
        {                                                                                   \
            PIMMX(ptr)->old = immx_remove(PIMMX(ptr), PIMMX(ptr)->p);                       \
            IMMX_PUSH_NODE(ptr, PIMMX(ptr)->old);                                           \
            PIMMX(ptr)->count--;                                                            \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* clean map */
#define IMMX_CLEAN(ptr)                                                                     \
do                                                                                          \
{                                                                                           \
    if(ptr)                                                                                 \
    {                                                                                       \
        PIMMX(ptr)->loop = 0;                                                               \
        while(PIMMX(ptr)->loop < PIMMX(ptr)->nfreelist)                                     \
        {                                                                                   \
            PIMMX(ptr)->tmp = PIMMX(ptr)->freelist[PIMMX(ptr)->loop];                       \
            xmm_free(PIMMX(ptr)->tmp, sizeof(IMMXNODE) * IMMX_LINE_NUM);                    \
            PIMMX(ptr)->loop++;                                                             \
        }                                                                                   \
        xmm_free(ptr, sizeof(IMMX));                                                        \
        ptr = NULL;                                                                         \
    }                                                                                       \
}while(0)
#endif
