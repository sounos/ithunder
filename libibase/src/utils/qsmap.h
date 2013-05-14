#ifndef	_QSMAP_H
#define	_QSMAP_H
typedef struct _QSNODE
{
    int64_t  key;
    void *dptr;
    struct _QSNODE *rbe_left;     
    struct _QSNODE *rbe_right;    
    struct _QSNODE *rbe_parent;   
    int rbe_color;         
}QSNODE;
typedef struct _QSMAP
{
    QSNODE node;
    QSNODE *rbh_root;
    QSNODE *old;
    QSNODE *max;
    QSNODE *min;
    QSNODE *bnode;
    QSNODE *p;
    QSNODE *pp;
    QSNODE *head;
    QSNODE *tail;
    int count;
    int total;
    int free_count;
}QSMAP;
#define PQS(ptr) ((QSMAP *)ptr)
#define QS_NEGINF	-1
#define QS_INF	    1
#define QS_BLACK	0
#define QS_RED		1
#define QS_LEFT(elm)		(elm->rbe_left)
#define QS_RIGHT(elm)		(elm->rbe_right)
#define QS_PARENT(elm)		(elm->rbe_parent)
#define QS_COLOR(elm)		(elm->rbe_color)
#define QS_ROOT(head)       (PQS(head)->rbh_root)
#define PQS_PARENT(head)    (PQS(head)->parent)
#define PQS_GPARENT(head)   (PQS(head)->gparent)
#define PQS_CHILD(head)     (PQS(head)->child)
#define PQS_OLEFT(head)     (PQS(head)->oleft)
#define PQS_ORIGHT(head)    (PQS(head)->oright)
#define PQS_COLOR(head)     (PQS(head)->color)
#define PQS_OLD(head)       (PQS(head)->old)
#define PQS_TMP(head)       (PQS(head)->tmp)
#define PQS_MAX(head)       (PQS(head)->max)
#define PQS_MIN(head)       (PQS(head)->min)
#define PQS_MAXK(head)      (PQS(head)->max->key)
#define PQS_MINK(head)      (PQS(head)->min->key)
#define QS_EMPTY(head)      (QS_ROOT(head) == NULL)
#define QS_NODE_SET(elm, old)                                                               \
do                                                                                          \
{                                                                                           \
    QS_PARENT(elm) = QS_PARENT(old);                                                        \
    QS_LEFT(elm) = QS_LEFT(old);                                                            \
    QS_RIGHT(elm) = QS_RIGHT(old);                                                          \
    QS_COLOR(elm) = QS_COLOR(old);                                                          \
}while(0)
#define QS_SET(elm, parent)                                                                 \
do                                                                                          \
{					                                                                        \
	QS_PARENT(elm) = parent;					                                            \
	QS_LEFT(elm) = QS_RIGHT(elm) = NULL;		                                            \
	QS_COLOR(elm) = QS_RED;					                                                \
}while(0)

#define QS_SET_BLACKRED(black, red)                                                         \
do                                                                                          \
{				                                                                            \
	QS_COLOR(black) = QS_BLACK;				                                                \
	QS_COLOR(red) = QS_RED;					                                                \
}while(0)

#ifndef QS_AUGMENT
#define QS_AUGMENT(x)
#endif

#define QS_ROTATE_LEFT(head, elm, tmp)                                                      \
do                                                                                          \
{			                                                                                \
	(tmp) = QS_RIGHT(elm);					                                                \
	if ((QS_RIGHT(elm) = QS_LEFT(tmp)))                                                     \
    {		                                                                                \
		QS_PARENT(QS_LEFT(tmp)) = (elm);		                                            \
	}								                                                        \
	QS_AUGMENT(elm);						                                                \
	if ((QS_PARENT(tmp) = QS_PARENT(elm)))                                                  \
    {		                                                                                \
		if ((elm) == QS_LEFT(QS_PARENT(elm)))	                                            \
			QS_LEFT(QS_PARENT(elm)) = (tmp);	                                            \
		else							                                                    \
			QS_RIGHT(QS_PARENT(elm)) = (tmp);	                                            \
	} else								                                                    \
		QS_ROOT(head) = (tmp);				                                                \
	QS_LEFT(tmp) = (elm);					                                                \
	QS_PARENT(elm) = (tmp);					                                                \
	QS_AUGMENT(tmp);						                                                \
	if ((QS_PARENT(tmp))) QS_AUGMENT(QS_PARENT(tmp));			                            \
}while(0)

#define QS_ROTATE_RIGHT(head, elm, tmp)                                                     \
do                                                                                          \
{			                                                                                \
	(tmp) = QS_LEFT(elm);					                                                \
	if ((QS_LEFT(elm) = QS_RIGHT(tmp)))                                                     \
    {		                                                                                \
		QS_PARENT(QS_RIGHT(tmp)) = (elm);		                                            \
	}								                                                        \
	QS_AUGMENT(elm);						                                                \
	if ((QS_PARENT(tmp) = QS_PARENT(elm)))                                                  \
    {		                                                                                \
		if ((elm) == QS_LEFT(QS_PARENT(elm)))	                                            \
			QS_LEFT(QS_PARENT(elm)) = (tmp);	                                            \
		else							                                                    \
			QS_RIGHT(QS_PARENT(elm)) = (tmp);	                                            \
	} else								                                                    \
		QS_ROOT(head) = (tmp);				                                                \
	QS_RIGHT(tmp) = (elm);					                                                \
	QS_PARENT(elm) = (tmp);					                                                \
	QS_AUGMENT(tmp);						                                                \
	if ((QS_PARENT(tmp)))QS_AUGMENT(QS_PARENT(tmp));			                            \
}while(0)
/* remove node */
QSNODE *qs_remove(QSMAP *map, QSNODE *elm);
/* insert new node */
QSNODE *qs_insert(QSMAP *map, QSNODE *elm);
/* find node */
QSNODE *qs_find(QSMAP *map, QSNODE *elm);
/* next node for great */
QSNODE *qs_next(QSNODE *elm);
/* prev node for less */
QSNODE *qs_prev(QSNODE *elm);
/* find min/max node */
QSNODE *qs_minmax(QSMAP *map, int val);

#define QS_MIN(map) qs_minmax(map, QS_NEGINF)
#define QS_MAX(map)	qs_minmax(map, QS_INF)
#define QSMAP_MAX(ptr) PQS(ptr)->max
#define QSMAP_MIN(ptr) PQS(ptr)->min
#define QSMAP_INIT() (calloc(1, sizeof(QSMAP)))

/* add to min/max */
#define QSMAP_MINMAX_ADD(ptr, node)                                                         \
do                                                                                          \
{                                                                                           \
    if(PQS(ptr)->max  == NULL || node->key > PQS(ptr)->max->key)                            \
    {                                                                                       \
        PQS(ptr)->max = node;                                                               \
    }                                                                                       \
    if(PQS(ptr)->min == NULL || node->key < PQS(ptr)->min->key)                             \
        PQS(ptr)->min = node;                                                               \
}while(0)

/* rebuild min/max */
#define QSMAP_MINMAX_REBUILD(ptr, node)                                                     \
do                                                                                          \
{                                                                                           \
    if(node == PQS(ptr)->max)                                                               \
    {                                                                                       \
        PQS(ptr)->max = qs_prev(node);                                                      \
    }                                                                                       \
    if(node == PQS(ptr)->min)                                                               \
    {                                                                                       \
        PQS(ptr)->min = qs_next(node);                                                      \
    }                                                                                       \
}while(0)

/* pop node from freelist */
#define QSMAP_POP_NODE(ptr, node)                                                           \
do                                                                                          \
{                                                                                           \
    if(PQS(ptr)->head)                                                                      \
    {                                                                                       \
        node = PQS(ptr)->head;                                                              \
        PQS(ptr)->head = PQS(ptr)->head->rbe_parent;                                        \
        if(PQS(ptr)->head == NULL) PQS(ptr)->tail = NULL;                                   \
        memset(node, 0, sizeof(QSNODE));                                                    \
        PQS(ptr)->free_count--;                                                             \
    }                                                                                       \
    else                                                                                    \
    {                                                                                       \
        node = (QSNODE *)calloc(1, sizeof(QSNODE));                                         \
        PQS(ptr)->total++;                                                                  \
    }                                                                                       \
}while(0)

/* push free node to freelist */
#define QSMAP_PUSH_NODE(ptr, node)                                                          \
do                                                                                          \
{                                                                                           \
    memset(node, 0, sizeof(QSNODE));                                                        \
    if(PQS(ptr)->tail)                                                                      \
    {                                                                                       \
        PQS(ptr)->tail->rbe_parent = node;                                                  \
        PQS(ptr)->tail = node;                                                              \
    }                                                                                       \
    else                                                                                    \
    {                                                                                       \
        PQS(ptr)->head = PQS(ptr)->tail = node;                                             \
    }                                                                                       \
    PQS(ptr)->free_count++;                                                                 \
}while(0)

/* free all nodes */
#define QSMAP_FREE_NODES(ptr)                                                               \
do                                                                                          \
{                                                                                           \
    while(PQS(ptr)->free_count > 0)                                                         \
    {                                                                                       \
        if(PQS(ptr)->head)                                                                  \
        {                                                                                   \
            PQS(ptr)->p = PQS(ptr)->head;                                                   \
            PQS(ptr)->head = PQS(ptr)->head->rbe_parent;                                    \
            free(PQS(ptr)->p);                                                              \
            PQS(ptr)->p = NULL;                                                             \
        }                                                                                   \
        PQS(ptr)->free_count--;                                                             \
    }                                                                                       \
}while(0)

/* add key/val(dp) olddp is return if key exists */
#define QSMAP_ADD(ptr, nkey, dp, olddp)                                                     \
do                                                                                          \
{                                                                                           \
    olddp = NULL;                                                                           \
    if(ptr)                                                                                 \
    {                                                                                       \
        QSMAP_POP_NODE(ptr, PQS(ptr)->p);                                                   \
        if((PQS(ptr)->pp = PQS(ptr)->p))                                                    \
        {                                                                                   \
            PQS(ptr)->p->key = (int64_t)nkey;                                               \
            PQS(ptr)->p->dptr = dp;                                                         \
            PQS(ptr)->old = qs_insert(PQS(ptr), PQS(ptr)->p);                               \
            if(PQS(ptr)->old)                                                               \
            {                                                                               \
                olddp  = PQS(ptr)->old->dptr;                                               \
                QSMAP_PUSH_NODE(ptr, PQS(ptr)->pp);                                         \
            }                                                                               \
            else                                                                            \
            {                                                                               \
                QSMAP_MINMAX_ADD(ptr, PQS(ptr)->pp);                                        \
                PQS(ptr)->count++;                                                          \
            }                                                                               \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* get val(dp) of nkey */
#define QSMAP_GET(ptr, nkey, dp)                                                            \
do                                                                                          \
{                                                                                           \
    dp = NULL;                                                                              \
    if(ptr)                                                                                 \
    {                                                                                       \
        PQS(ptr)->node.key = (int64_t)nkey;                                                 \
        PQS(ptr)->bnode = &(PQS(ptr)->node);                                                \
        PQS(ptr)->p = qs_find(PQS(ptr), PQS(ptr)->bnode);                                   \
        if(PQS(ptr)->p)                                                                     \
        {                                                                                   \
            dp = PQS(ptr)->p->dptr;                                                         \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* delete node(key/val) from map */
#define QSMAP_DEL(ptr, nkey, dp)                                                            \
do                                                                                          \
{                                                                                           \
    dp = NULL;                                                                              \
    if(ptr)                                                                                 \
    {                                                                                       \
        PQS(ptr)->node.key = (int64_t)nkey;                                                 \
        PQS(ptr)->bnode = &(PQS(ptr)->node);                                                \
        PQS(ptr)->old = qs_remove(PQS(ptr), PQS(ptr)->bnode);                               \
        if(PQS(ptr)->old)                                                                   \
        {                                                                                   \
            QSMAP_PUSH_NODE(ptr, PQS(ptr)->old);                                            \
            PQS(ptr)->count--;                                                              \
            dp = PQS(ptr)->old->dptr;                                                       \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* pop min node */
#define QSMAP_POP_MIN(ptr, nkey, dp)                                                        \
do                                                                                          \
{                                                                                           \
    dp = NULL;                                                                              \
    if(ptr)                                                                                 \
    {                                                                                       \
        if(PQS(ptr)->min)                                                                   \
        {                                                                                   \
            nkey = PQS(ptr)->min->key;                                                      \
            dp  = PQS(ptr)->min->dptr;                                                      \
            PQS(ptr)->p = PQS(ptr)->min;                                                    \
            PQS(ptr)->old = qs_remove(PQS(ptr), PQS(ptr)->p);                               \
            QSMAP_PUSH_NODE(ptr, PQS(ptr)->old);                                            \
            PQS(ptr)->count--;                                                              \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* pop max node */
#define QSMAP_POP_MAX(ptr, nkey, dp)                                                        \
do                                                                                          \
{                                                                                           \
    dp = NULL;                                                                              \
    if(ptr)                                                                                 \
    {                                                                                       \
        if(PQS(ptr)->max)                                                                   \
        {                                                                                   \
            nkey = PQS(ptr)->max->key;                                                      \
            dp  = PQS(ptr)->max->dptr;                                                      \
            PQS(ptr)->p = PQS(ptr)->max;                                                    \
            PQS(ptr)->old = qs_remove(PQS(ptr), PQS(ptr)->p);                               \
            QSMAP_PUSH_NODE(ptr, PQS(ptr)->old);                                            \
            PQS(ptr)->count--;                                                              \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* find max node and pop it */
#define QSMAP_POP_RMAX(ptr, nkey, dp)                                                       \
do                                                                                          \
{                                                                                           \
    dp = NULL;                                                                              \
    if(ptr)                                                                                 \
    {                                                                                       \
        if(PQS(ptr)->max)                                                                   \
        {                                                                                   \
            PQS(ptr)->bnode = NULL;                                                         \
            PQS(ptr)->bnode = QS_MAX(PQS(ptr));                                             \
            if(PQS(ptr)->bnode)                                                             \
            {                                                                               \
                nkey = PQS(ptr)->bnode->key;                                                \
                dp = PQS(ptr)->bnode->dptr;                                                 \
                PQS(ptr)->old = qs_remove(PQS(ptr), PQS(ptr)->bnode);                       \
                QSMAP_PUSH_NODE(ptr, PQS(ptr)->old);                                        \
                PQS(ptr)->count--;                                                          \
            }                                                                               \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* find min node and pop it */
#define QSMAP_POP_RMIN(ptr, nkey, dp)                                                       \
do                                                                                          \
{                                                                                           \
    dp = NULL;                                                                              \
    if(ptr)                                                                                 \
    {                                                                                       \
        if(PQS(ptr)->min)                                                                   \
        {                                                                                   \
            PQS(ptr)->bnode = NULL;                                                         \
            PQS(ptr)->bnode = QS_MIN(PQS(ptr));                                             \
            if(PQS(ptr)->bnode)                                                             \
            {                                                                               \
                nkey = PQS(ptr)->bnode->key;                                                \
                dp = PQS(ptr)->bnode->dptr;                                                 \
                PQS(ptr)->old = qs_remove(PQS(ptr), PQS(ptr)->bnode);                       \
                QSMAP_PUSH_NODE(ptr, PQS(ptr)->old);                                        \
                PQS(ptr)->count--;                                                          \
            }                                                                               \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* reset map */
#define QSMAP_RESET(ptr)                                                                    \
do                                                                                          \
{                                                                                           \
    if(ptr)                                                                                 \
    {                                                                                       \
        PQS(ptr)->p = NULL;                                                                 \
        while((PQS(ptr)->p = QS_MAX(PQS(ptr))))                                             \
        {                                                                                   \
            PQS(ptr)->old = qs_remove(PQS(ptr), PQS(ptr)->p);                               \
            QSMAP_PUSH_NODE(ptr, PQS(ptr)->old);                                            \
            PQS(ptr)->count--;                                                              \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* clean map */
#define QSMAP_CLEAN(ptr)                                                                    \
do                                                                                          \
{                                                                                           \
    if(ptr)                                                                                 \
    {                                                                                       \
        PQS(ptr)->p = NULL;                                                                 \
        while((PQS(ptr)->p = QS_MAX(PQS(ptr))))                                             \
        {                                                                                   \
            PQS(ptr)->old = qs_remove(PQS(ptr), PQS(ptr)->p);                               \
            if(PQS(ptr)->old) free(PQS(ptr)->old);                                          \
            PQS(ptr)->p = NULL;                                                             \
            PQS(ptr)->old = NULL;                                                           \
        }                                                                                   \
        QSMAP_FREE_NODES(ptr);                                                              \
        free(ptr);                                                                          \
        ptr = NULL;                                                                         \
    }                                                                                       \
}while(0)
#endif
