#include "mutex.h"
#ifndef	_RBMAP_H
#define	_RBMAP_H
typedef struct _RBNODE
{
    double  key;
    void *dptr;
    struct _RBNODE *rbe_left;     
    struct _RBNODE *rbe_right;    
    struct _RBNODE *rbe_parent;   
    int rbe_color;         
}RBNODE;
typedef struct _RBMAP
{
    RBNODE node;
    RBNODE *rbh_root;
    RBNODE *old;
    RBNODE *max;
    RBNODE *min;
    RBNODE *bnode;
    RBNODE *p;
    RBNODE *pp;
    RBNODE *head;
    RBNODE *tail;
    int count;
    int total;
    int free_count;
    MUTEX mutex;
}RBMAP;
#define PRB(ptr) ((RBMAP *)ptr)
#define RB_NEGINF	-1
#define RB_INF	    1
#define RB_BLACK	0
#define RB_RED		1
#define RB_LEFT(elm)		(elm->rbe_left)
#define RB_RIGHT(elm)		(elm->rbe_right)
#define RB_PARENT(elm)		(elm->rbe_parent)
#define RB_COLOR(elm)		(elm->rbe_color)
#define RB_ROOT(head)       (PRB(head)->rbh_root)
#define PRB_PARENT(head)    (PRB(head)->parent)
#define PRB_GPARENT(head)   (PRB(head)->gparent)
#define PRB_CHILD(head)     (PRB(head)->child)
#define PRB_OLEFT(head)     (PRB(head)->oleft)
#define PRB_ORIGHT(head)    (PRB(head)->oright)
#define PRB_COLOR(head)     (PRB(head)->color)
#define PRB_OLD(head)       (PRB(head)->old)
#define PRB_TMP(head)       (PRB(head)->tmp)
#define PRB_MAX(head)       (PRB(head)->max)
#define PRB_MIN(head)       (PRB(head)->min)
#define PRB_MUTEX(head)     (PRB(head)->mutex)
#define PRB_MAXK(head)      (PRB(head)->max->key)
#define PRB_MINK(head)      (PRB(head)->min->key)
#define RB_EMPTY(head)      (RB_ROOT(head) == NULL)
#define RB_NODE_SET(elm, old)                                                               \
do                                                                                          \
{                                                                                           \
    RB_PARENT(elm) = RB_PARENT(old);                                                        \
    RB_LEFT(elm) = RB_LEFT(old);                                                            \
    RB_RIGHT(elm) = RB_RIGHT(old);                                                          \
    RB_COLOR(elm) = RB_COLOR(old);                                                          \
}while(0)
#define RB_SET(elm, parent)                                                                 \
do                                                                                          \
{					                                                                        \
	RB_PARENT(elm) = parent;					                                            \
	RB_LEFT(elm) = RB_RIGHT(elm) = NULL;		                                            \
	RB_COLOR(elm) = RB_RED;					                                                \
}while(0)

#define RB_SET_BLACKRED(black, red)                                                         \
do                                                                                          \
{				                                                                            \
	RB_COLOR(black) = RB_BLACK;				                                                \
	RB_COLOR(red) = RB_RED;					                                                \
}while(0)

#ifndef RB_AUGMENT
#define RB_AUGMENT(x)
#endif

#define RB_ROTATE_LEFT(head, elm, tmp)                                                      \
do                                                                                          \
{			                                                                                \
	(tmp) = RB_RIGHT(elm);					                                                \
	if ((RB_RIGHT(elm) = RB_LEFT(tmp)))                                                     \
    {		                                                                                \
		RB_PARENT(RB_LEFT(tmp)) = (elm);		                                            \
	}								                                                        \
	RB_AUGMENT(elm);						                                                \
	if ((RB_PARENT(tmp) = RB_PARENT(elm)))                                                  \
    {		                                                                                \
		if ((elm) == RB_LEFT(RB_PARENT(elm)))	                                            \
			RB_LEFT(RB_PARENT(elm)) = (tmp);	                                            \
		else							                                                    \
			RB_RIGHT(RB_PARENT(elm)) = (tmp);	                                            \
	} else								                                                    \
		RB_ROOT(head) = (tmp);				                                                \
	RB_LEFT(tmp) = (elm);					                                                \
	RB_PARENT(elm) = (tmp);					                                                \
	RB_AUGMENT(tmp);						                                                \
	if ((RB_PARENT(tmp))) RB_AUGMENT(RB_PARENT(tmp));			                            \
}while(0)

#define RB_ROTATE_RIGHT(head, elm, tmp)                                                     \
do                                                                                          \
{			                                                                                \
	(tmp) = RB_LEFT(elm);					                                                \
	if ((RB_LEFT(elm) = RB_RIGHT(tmp)))                                                     \
    {		                                                                                \
		RB_PARENT(RB_RIGHT(tmp)) = (elm);		                                            \
	}								                                                        \
	RB_AUGMENT(elm);						                                                \
	if ((RB_PARENT(tmp) = RB_PARENT(elm)))                                                  \
    {		                                                                                \
		if ((elm) == RB_LEFT(RB_PARENT(elm)))	                                            \
			RB_LEFT(RB_PARENT(elm)) = (tmp);	                                            \
		else							                                                    \
			RB_RIGHT(RB_PARENT(elm)) = (tmp);	                                            \
	} else								                                                    \
		RB_ROOT(head) = (tmp);				                                                \
	RB_RIGHT(tmp) = (elm);					                                                \
	RB_PARENT(elm) = (tmp);					                                                \
	RB_AUGMENT(tmp);						                                                \
	if ((RB_PARENT(tmp)))RB_AUGMENT(RB_PARENT(tmp));			                            \
}while(0)
/* remove node */
RBNODE *rb_remove(RBMAP *map, RBNODE *elm);
/* insert new node */
RBNODE *rb_insert(RBMAP *map, RBNODE *elm);
/* find node */
RBNODE *rb_find(RBMAP *map, RBNODE *elm);
/* next node for great */
RBNODE *rb_next(RBNODE *elm);
/* prev node for less */
RBNODE *rb_prev(RBNODE *elm);
/* find min/max node */
RBNODE *rb_minmax(RBMAP *map, int val);

#define RB_MIN(map) rb_minmax(map, RB_NEGINF)
#define RB_MAX(map)	rb_minmax(map, RB_INF)
#define RBMAP_MAX(ptr) PRB(ptr)->max
#define RBMAP_MIN(ptr) PRB(ptr)->min
#define RBMAP_INIT(ptr)                                                                     \
do                                                                                          \
{                                                                                           \
    if((ptr = calloc(1, sizeof(RBMAP))))                                                    \
    {                                                                                       \
        MUTEX_INIT(PRB(ptr)->mutex);                                                        \
    }                                                                                       \
}while(0)

/* add to min/max */
#define RBMAP_MINMAX_ADD(ptr, node)                                                         \
do                                                                                          \
{                                                                                           \
    if(PRB(ptr)->max  == NULL || node->key > PRB(ptr)->max->key)                            \
    {                                                                                       \
        PRB(ptr)->max = node;                                                               \
    }                                                                                       \
    if(PRB(ptr)->min == NULL || node->key < PRB(ptr)->min->key)                             \
        PRB(ptr)->min = node;                                                               \
}while(0)

/* rebuild min/max */
#define RBMAP_MINMAX_REBUILD(ptr, node)                                                     \
do                                                                                          \
{                                                                                           \
    if(node == PRB(ptr)->max)                                                               \
    {                                                                                       \
        PRB(ptr)->max = RB_MAX(PRB(ptr));                                                   \
    }                                                                                       \
    if(node == PRB(ptr)->min)                                                               \
    {                                                                                       \
        PRB(ptr)->min = RB_MIN(PRB(ptr));                                                   \
    }                                                                                       \
}while(0)

/* pop node from freelist */
#define RBMAP_POP_NODE(ptr, node)                                                           \
do                                                                                          \
{                                                                                           \
    if(PRB(ptr)->head)                                                                      \
    {                                                                                       \
        node = PRB(ptr)->head;                                                              \
        PRB(ptr)->head = PRB(ptr)->head->rbe_parent;                                        \
        if(PRB(ptr)->head == NULL) PRB(ptr)->tail = NULL;                                   \
        memset(node, 0, sizeof(RBNODE));                                                    \
        PRB(ptr)->free_count--;                                                             \
    }                                                                                       \
    else                                                                                    \
    {                                                                                       \
        node = (RBNODE *)calloc(1, sizeof(RBNODE));                                         \
        PRB(ptr)->total++;                                                                  \
    }                                                                                       \
}while(0)

/* push free node to freelist */
#define RBMAP_PUSH_NODE(ptr, node)                                                          \
do                                                                                          \
{                                                                                           \
    memset(node, 0, sizeof(RBNODE));                                                        \
    if(PRB(ptr)->tail)                                                                      \
    {                                                                                       \
        PRB(ptr)->tail->rbe_parent = node;                                                  \
        PRB(ptr)->tail = node;                                                              \
    }                                                                                       \
    else                                                                                    \
    {                                                                                       \
        PRB(ptr)->head = PRB(ptr)->tail = node;                                             \
    }                                                                                       \
    PRB(ptr)->free_count++;                                                                 \
}while(0)

/* free all nodes */
#define RBMAP_FREE_NODES(ptr)                                                               \
do                                                                                          \
{                                                                                           \
    while(PRB(ptr)->free_count > 0)                                                         \
    {                                                                                       \
        if(PRB(ptr)->head)                                                                  \
        {                                                                                   \
            PRB(ptr)->p = PRB(ptr)->head;                                                   \
            PRB(ptr)->head = PRB(ptr)->head->rbe_parent;                                    \
            free(PRB(ptr)->p);                                                              \
            PRB(ptr)->p = NULL;                                                             \
        }                                                                                   \
        PRB(ptr)->free_count--;                                                             \
    }                                                                                       \
}while(0)

/* add key/val(dp) olddp is return if key exists */
#define RBMAP_ADD(ptr, nkey, dp, olddp)                                                     \
do                                                                                          \
{                                                                                           \
    olddp = NULL;                                                                           \
    if(ptr)                                                                                 \
    {                                                                                       \
        MUTEX_LOCK(PRB(ptr)->mutex);                                                        \
        RBMAP_POP_NODE(ptr, PRB(ptr)->p);                                                   \
        if((PRB(ptr)->pp = PRB(ptr)->p))                                                    \
        {                                                                                   \
            PRB(ptr)->p->key = (double  )nkey;                                              \
            PRB(ptr)->p->dptr = dp;                                                         \
            PRB(ptr)->old = rb_insert(PRB(ptr), PRB(ptr)->p);                               \
            if(PRB(ptr)->old)                                                               \
            {                                                                               \
                olddp  = PRB(ptr)->old->dptr;                                               \
                RBMAP_PUSH_NODE(ptr, PRB(ptr)->pp);                                         \
            }                                                                               \
            else                                                                            \
            {                                                                               \
                RBMAP_MINMAX_ADD(ptr, PRB(ptr)->pp);                                        \
                PRB(ptr)->count++;                                                          \
            }                                                                               \
        }                                                                                   \
        MUTEX_UNLOCK(PRB(ptr)->mutex);                                                      \
    }                                                                                       \
}while(0)

/* get val(dp) of nkey */
#define RBMAP_GET(ptr, nkey, dp)                                                            \
do                                                                                          \
{                                                                                           \
    dp = NULL;                                                                              \
    if(ptr)                                                                                 \
    {                                                                                       \
        MUTEX_LOCK(PRB(ptr)->mutex);                                                        \
        PRB(ptr)->node.key = (double )nkey;                                                 \
        PRB(ptr)->bnode = &(PRB(ptr)->node);                                                \
        PRB(ptr)->p = rb_find(PRB(ptr), PRB(ptr)->bnode);                                   \
        if(PRB(ptr)->p)                                                                     \
        {                                                                                   \
            dp = PRB(ptr)->p->dptr;                                                         \
        }                                                                                   \
        MUTEX_UNLOCK(PRB(ptr)->mutex);                                                      \
    }                                                                                       \
}while(0)

/* delete node(key/val) from map */
#define RBMAP_DEL(ptr, nkey, dp)                                                            \
do                                                                                          \
{                                                                                           \
    dp = NULL;                                                                              \
    if(ptr)                                                                                 \
    {                                                                                       \
        MUTEX_LOCK(PRB(ptr)->mutex);                                                        \
        PRB(ptr)->node.key = (double )nkey;                                                 \
        PRB(ptr)->bnode = &(PRB(ptr)->node);                                                \
        PRB(ptr)->old = rb_remove(PRB(ptr), PRB(ptr)->bnode);                               \
        if(PRB(ptr)->old)                                                                   \
        {                                                                                   \
            RBMAP_PUSH_NODE(ptr, PRB(ptr)->old);                                            \
            RBMAP_MINMAX_REBUILD(ptr, PRB(ptr)->old);                                       \
            PRB(ptr)->count--;                                                              \
            dp = PRB(ptr)->old->dptr;                                                       \
        }                                                                                   \
        MUTEX_UNLOCK(PRB(ptr)->mutex);                                                      \
    }                                                                                       \
}while(0)

/* pop min node */
#define RBMAP_POP_MIN(ptr, nkey, dp)                                                        \
do                                                                                          \
{                                                                                           \
    dp = NULL;                                                                              \
    if(ptr)                                                                                 \
    {                                                                                       \
        MUTEX_LOCK(PRB(ptr)->mutex);                                                        \
        if(PRB(ptr)->min)                                                                   \
        {                                                                                   \
            nkey = PRB(ptr)->min->key;                                                      \
            dp  = PRB(ptr)->min->dptr;                                                      \
            PRB(ptr)->p = PRB(ptr)->min;                                                    \
            PRB(ptr)->old = rb_remove(PRB(ptr), PRB(ptr)->p);                               \
            RBMAP_PUSH_NODE(ptr, PRB(ptr)->old);                                            \
            RBMAP_MINMAX_REBUILD(ptr, PRB(ptr)->old);                                       \
            PRB(ptr)->count--;                                                              \
        }                                                                                   \
        MUTEX_UNLOCK(PRB(ptr)->mutex);                                                      \
    }                                                                                       \
}while(0)

/* pop max node */
#define RBMAP_POP_MAX(ptr, nkey, dp)                                                        \
do                                                                                          \
{                                                                                           \
    dp = NULL;                                                                              \
    if(ptr)                                                                                 \
    {                                                                                       \
        MUTEX_LOCK(PRB(ptr)->mutex);                                                        \
        if(PRB(ptr)->max)                                                                   \
        {                                                                                   \
            nkey = PRB(ptr)->max->key;                                                      \
            dp  = PRB(ptr)->max->dptr;                                                      \
            PRB(ptr)->p = PRB(ptr)->max;                                                    \
            PRB(ptr)->old = rb_remove(PRB(ptr), PRB(ptr)->p);                               \
            RBMAP_PUSH_NODE(ptr, PRB(ptr)->old);                                            \
            RBMAP_MINMAX_REBUILD(ptr, PRB(ptr)->old);                                       \
            PRB(ptr)->count--;                                                              \
        }                                                                                   \
        MUTEX_UNLOCK(PRB(ptr)->mutex);                                                      \
    }                                                                                       \
}while(0)

/* find max node and pop it */
#define RBMAP_POP_RMAX(ptr, nkey, dp)                                                       \
do                                                                                          \
{                                                                                           \
    dp = NULL;                                                                              \
    if(ptr)                                                                                 \
    {                                                                                       \
        MUTEX_LOCK(PRB(ptr)->mutex);                                                        \
        if(PRB(ptr)->max)                                                                   \
        {                                                                                   \
            PRB(ptr)->bnode = NULL;                                                         \
            PRB(ptr)->bnode = RB_MAX(PRB(ptr));                                             \
            if(PRB(ptr)->bnode)                                                             \
            {                                                                               \
                nkey = PRB(ptr)->bnode->key;                                                \
                dp = PRB(ptr)->bnode->dptr;                                                 \
                PRB(ptr)->old = rb_remove(PRB(ptr), PRB(ptr)->bnode);                       \
                RBMAP_PUSH_NODE(ptr, PRB(ptr)->old);                                        \
                PRB(ptr)->count--;                                                          \
            }                                                                               \
        }                                                                                   \
        MUTEX_UNLOCK(PRB(ptr)->mutex);                                                      \
    }                                                                                       \
}while(0)

/* find min node and pop it */
#define RBMAP_POP_RMIN(ptr, nkey, dp)                                                       \
do                                                                                          \
{                                                                                           \
    dp = NULL;                                                                              \
    if(ptr)                                                                                 \
    {                                                                                       \
        MUTEX_LOCK(PRB(ptr)->mutex);                                                        \
        if(PRB(ptr)->min)                                                                   \
        {                                                                                   \
            PRB(ptr)->bnode = NULL;                                                         \
            PRB(ptr)->bnode = RB_MIN(PRB(ptr));                                             \
            if(PRB(ptr)->bnode)                                                             \
            {                                                                               \
                nkey = PRB(ptr)->bnode->key;                                                \
                dp = PRB(ptr)->bnode->dptr;                                                 \
                PRB(ptr)->old = rb_remove(PRB(ptr), PRB(ptr)->bnode);                       \
                RBMAP_PUSH_NODE(ptr, PRB(ptr)->old);                                        \
                PRB(ptr)->count--;                                                          \
            }                                                                               \
        }                                                                                   \
        MUTEX_UNLOCK(PRB(ptr)->mutex);                                                      \
    }                                                                                       \
}while(0)

/* reset map */
#define RBMAP_RESET(ptr)                                                                    \
do                                                                                          \
{                                                                                           \
    if(ptr)                                                                                 \
    {                                                                                       \
        PRB(ptr)->p = NULL;                                                                 \
        while((PRB(ptr)->p = RB_MAX(PRB(ptr))))                                             \
        {                                                                                   \
            PRB(ptr)->old = rb_remove(PRB(ptr), PRB(ptr)->p);                               \
            RBMAP_PUSH_NODE(ptr, PRB(ptr)->old);                                            \
            PRB(ptr)->count--;                                                              \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* clean map */
#define RBMAP_CLEAN(ptr)                                                                    \
do                                                                                          \
{                                                                                           \
    if(ptr)                                                                                 \
    {                                                                                       \
        PRB(ptr)->p = NULL;                                                                 \
        while((PRB(ptr)->p = RB_MAX(PRB(ptr))))                                             \
        {                                                                                   \
            PRB(ptr)->old = rb_remove(PRB(ptr), PRB(ptr)->p);                               \
            if(PRB(ptr)->old) free(PRB(ptr)->old);                                          \
            PRB(ptr)->p = NULL;                                                             \
            PRB(ptr)->old = NULL;                                                           \
        }                                                                                   \
        MUTEX_DESTROY(PRB(ptr)->mutex);                                                     \
        RBMAP_FREE_NODES(ptr);                                                              \
        free(ptr);                                                                          \
        ptr = NULL;                                                                         \
    }                                                                                       \
}while(0)
#endif
