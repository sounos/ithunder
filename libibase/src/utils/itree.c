#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include "itree.h"
#include "mutex.h"
#ifdef MAP_LOCKED
#define MMAP_SHARED MAP_SHARED|MAP_LOCKED
#else
#define MMAP_SHARED MAP_SHARED
#endif
#define ITR(px) ((ITREE *)px)
#define ITR_COLOR_BLACK  0
#define ITR_COLOR_RED    1
#define ITR_MIN_MAX(x, key, xid)                                                \
do                                                                              \
{                                                                               \
    if(ITR(x) && ITR(x)->state)                                                 \
    {                                                                           \
        if(ITR(x)->state->count == 0)                                           \
        {                                                                       \
            ITR(x)->state->nmin = ITR(x)->state->nmax = xid;                    \
            ITR(x)->state->kmin = ITR(x)->state->kmax = key;                    \
        }                                                                       \
        else if(key > ITR(x)->state->kmax)                                      \
        {                                                                       \
            ITR(x)->state->nmax = xid;                                          \
            ITR(x)->state->kmax = key;                                          \
        }                                                                       \
        else if(key < ITR(x)->state->kmin)                                      \
        {                                                                       \
            ITR(x)->state->nmin = xid;                                          \
            ITR(x)->state->kmin = key;                                          \
        }                                                                       \
    }                                                                           \
}while(0)
#define ITR_MUNMAP(x)                                                           \
do                                                                              \
{                                                                               \
    if(x && ITR(x)->size > 0)                                                   \
    {                                                                           \
        if(ITR(x)->start && ITR(x)->start != (void *)-1)                        \
        {                                                                       \
            munmap(ITR(x)->start, ITR(x)->size);                                \
            ITR(x)->start = NULL;                                               \
            ITR(x)->state = NULL;                                               \
            ITR(x)->map = NULL;                                                 \
        }                                                                       \
    }                                                                           \
}while(0)

#define ITR_MMAP(x)                                                             \
do                                                                              \
{                                                                               \
    if(x)                                                                       \
    {                                                                           \
        if((ITR(x)->start = (char*)mmap(NULL,ITR(x)->size,PROT_READ|PROT_WRITE, \
                    MAP_SHARED, ITR(x)->fd, 0)) != (void *)-1)                  \
        {                                                                       \
            ITR(x)->state = (ITSTATE *)ITR(x)->start;                           \
            ITR(x)->map = (ITNODE *)(ITR(x)->start + sizeof(ITSTATE));          \
        }                                                                       \
    }                                                                           \
}while(0)

#define ITR_INCRE(x)                                                            \
do                                                                              \
{                                                                               \
    if(x &&  ITR(x)->end <  ITR(x)->size)                                       \
    {                                                                           \
        ITR(x)->old = ITR(x)->end ;                                             \
        ITR(x)->end += (off_t)ITREE_INCRE_NUM * (off_t)sizeof(ITNODE);         \
        if(ftruncate(ITR(x)->fd, ITR(x)->end) == 0)                             \
        {                                                                       \
            if(ITR(x)->old == sizeof(ITSTATE))                                  \
            {                                                                   \
                memset(ITR(x)->state, 0, sizeof(ITSTATE));                      \
                ITR(x)->state->left += ITREE_INCRE_NUM - 1;                    \
            }                                                                   \
            else ITR(x)->state->left += ITREE_INCRE_NUM;                       \
            ITR(x)->state->total += ITREE_INCRE_NUM;                           \
            memset(ITR(x)->start + ITR(x)->old, 0, ITR(x)->end - ITR(x)->old);  \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            _exit(-1);                                                          \
        }                                                                       \
    }                                                                           \
}while(0)
#define ITR_ROTATE_LEFT(x, prootid, oid, rid, lid, ppid)                        \
do                                                                              \
{                                                                               \
    if(x && (rid = ITR(x)->map[oid].right) > 0)                                 \
    {                                                                           \
        if((lid = ITR(x)->map[oid].right = ITR(x)->map[rid].left) > 0)          \
        {                                                                       \
            ITR(x)->map[lid].parent = oid;                                      \
        }                                                                       \
        if((ppid = ITR(x)->map[rid].parent = ITR(x)->map[oid].parent) > 0)      \
        {                                                                       \
            if(ITR(x)->map[ppid].left == oid)                                   \
                ITR(x)->map[ppid].left = rid;                                   \
            else                                                                \
                ITR(x)->map[ppid].right = rid;                                  \
        }else *prootid = rid;                                                   \
        ITR(x)->map[rid].left = oid;                                            \
        ITR(x)->map[oid].parent = rid;                                          \
    }                                                                           \
}while(0)

#define ITR_ROTATE_RIGHT(x, prootid, oid, lid, rid, ppid)                       \
do                                                                              \
{                                                                               \
    if(x && (lid = ITR(x)->map[oid].left) > 0)                                  \
    {                                                                           \
        if((rid = ITR(x)->map[oid].left = ITR(x)->map[lid].right) > 0)          \
        {                                                                       \
            ITR(x)->map[rid].parent = oid;                                      \
        }                                                                       \
        if((ppid = ITR(x)->map[lid].parent = ITR(x)->map[oid].parent) > 0)      \
        {                                                                       \
            if(ITR(x)->map[ppid].left == oid)                                   \
                ITR(x)->map[ppid].left = lid;                                   \
            else                                                                \
                ITR(x)->map[ppid].right = lid;                                  \
        }                                                                       \
        else *prootid = lid;                                                    \
        ITR(x)->map[lid].right = oid;                                           \
        ITR(x)->map[oid].parent = lid;                                          \
    }                                                                           \
}while(0)

#define ITR_INSERT_COLOR(x, prootid, oid, lid, rid, uid, pid, gpid, ppid)       \
do                                                                              \
{                                                                               \
    while((pid = ITR(x)->map[oid].parent)> 0                                    \
            && ITR(x)->map[pid].color == ITR_COLOR_RED)                         \
    {                                                                           \
        gpid = ITR(x)->map[pid].parent;                                         \
        if(pid == ITR(x)->map[gpid].left)                                       \
        {                                                                       \
            uid = ITR(x)->map[gpid].right;                                      \
            if(uid > 0 && ITR(x)->map[uid].color == ITR_COLOR_RED)              \
            {                                                                   \
                ITR(x)->map[uid].color = ITR_COLOR_BLACK;                       \
                ITR(x)->map[pid].color = ITR_COLOR_BLACK;                       \
                ITR(x)->map[gpid].color = ITR_COLOR_RED;                        \
                oid = gpid;                                                     \
                continue;                                                       \
            }                                                                   \
            if(ITR(x)->map[pid].right == oid)                                   \
            {                                                                   \
                ITR_ROTATE_LEFT(x, prootid, pid, rid, lid, ppid);               \
                uid = pid; pid = oid; oid = uid;                                \
            }                                                                   \
            ITR(x)->map[pid].color = ITR_COLOR_BLACK;                           \
            ITR(x)->map[gpid].color = ITR_COLOR_RED;                            \
            ITR_ROTATE_RIGHT(x, prootid, gpid, lid, rid, ppid);                 \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            uid = ITR(x)->map[gpid].left;                                       \
            if(uid > 0 && ITR(x)->map[uid].color == ITR_COLOR_RED)              \
            {                                                                   \
                ITR(x)->map[uid].color = ITR_COLOR_BLACK;                       \
                ITR(x)->map[pid].color = ITR_COLOR_BLACK;                       \
                ITR(x)->map[gpid].color = ITR_COLOR_RED;                        \
                oid = gpid;                                                     \
                continue;                                                       \
            }                                                                   \
            if(ITR(x)->map[pid].left == oid)                                    \
            {                                                                   \
                ITR_ROTATE_RIGHT(x, prootid, pid, lid, rid, ppid);              \
                uid = pid; pid = oid; oid = uid;                                \
            }                                                                   \
            ITR(x)->map[pid].color = ITR_COLOR_BLACK;                           \
            ITR(x)->map[gpid].color = ITR_COLOR_RED;                            \
            ITR_ROTATE_LEFT(x, prootid, gpid, rid, lid, ppid);                  \
        }                                                                       \
    }                                                                           \
    if(*prootid > 0)ITR(x)->map[*prootid].color = ITR_COLOR_BLACK;              \
}while(0)

#define ITR_REMOVE_COLOR(x, prootid, oid, xpid, lid, rid, uid, ppid)            \
do                                                                              \
{                                                                               \
    while((oid == 0 || ITR(x)->map[oid].color == ITR_COLOR_BLACK)               \
            && oid != *prootid)                                                 \
    {                                                                           \
        if(ITR(x)->map[xpid].left == oid)                                       \
        {                                                                       \
            uid = ITR(x)->map[xpid].right;                                      \
            if(ITR(x)->map[uid].color == ITR_COLOR_RED)                         \
            {                                                                   \
                ITR(x)->map[uid].color = ITR_COLOR_BLACK;                       \
                ITR(x)->map[xpid].color = ITR_COLOR_RED;                        \
                ITR_ROTATE_LEFT(x, prootid, xpid, rid, lid, ppid);              \
                uid = ITR(x)->map[xpid].right;                                  \
            }                                                                   \
            lid = ITR(x)->map[uid].left;                                        \
            rid = ITR(x)->map[uid].right;                                       \
            if((lid == 0 || ITR(x)->map[lid].color == ITR_COLOR_BLACK)          \
                && (rid == 0 || ITR(x)->map[rid].color == ITR_COLOR_BLACK))     \
            {                                                                   \
                ITR(x)->map[uid].color = ITR_COLOR_RED;                         \
                oid = xpid;                                                     \
                xpid = ITR(x)->map[oid].parent;                                 \
            }                                                                   \
            else                                                                \
            {                                                                   \
                rid = ITR(x)->map[uid].right;                                   \
                lid = ITR(x)->map[uid].left;                                    \
                if(rid == 0 || ITR(x)->map[rid].color == ITR_COLOR_BLACK)       \
                {                                                               \
                    if(lid > 0)ITR(x)->map[lid].color = ITR_COLOR_BLACK;        \
                    ITR(x)->map[uid].color = ITR_COLOR_RED;                     \
                    ITR_ROTATE_RIGHT(x, prootid, uid, lid, rid, ppid);          \
                    uid = ITR(x)->map[xpid].right;                              \
                }                                                               \
                ITR(x)->map[uid].color = ITR(x)->map[xpid].color;               \
                ITR(x)->map[xpid].color = ITR_COLOR_BLACK;                      \
                if((rid = ITR(x)->map[uid].right) > 0)                          \
                    ITR(x)->map[rid].color = ITR_COLOR_BLACK;                   \
                ITR_ROTATE_LEFT(x, prootid, xpid, rid, lid, ppid);              \
                oid = *prootid;                                                 \
                break;                                                          \
            }                                                                   \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            uid = ITR(x)->map[xpid].left;                                       \
            if(ITR(x)->map[uid].color == ITR_COLOR_RED)                         \
            {                                                                   \
                ITR(x)->map[uid].color = ITR_COLOR_BLACK;                       \
                ITR(x)->map[xpid].color = ITR_COLOR_RED;                        \
                ITR_ROTATE_RIGHT(x, prootid, xpid, lid, rid, ppid);             \
                uid = ITR(x)->map[xpid].left;                                   \
            }                                                                   \
            lid = ITR(x)->map[uid].left;                                        \
            rid = ITR(x)->map[uid].right;                                       \
            if((lid == 0 || ITR(x)->map[lid].color == ITR_COLOR_BLACK)          \
                && (rid == 0 || ITR(x)->map[rid].color == ITR_COLOR_BLACK))     \
            {                                                                   \
                ITR(x)->map[uid].color = ITR_COLOR_RED;                         \
                oid = xpid;                                                     \
                xpid = ITR(x)->map[oid].parent;                                 \
            }                                                                   \
            else                                                                \
            {                                                                   \
                rid = ITR(x)->map[uid].right;                                   \
                lid = ITR(x)->map[uid].left;                                    \
                if(lid == 0 || ITR(x)->map[lid].color == ITR_COLOR_BLACK)       \
                {                                                               \
                    if(rid > 0)ITR(x)->map[rid].color = ITR_COLOR_BLACK;        \
                    ITR(x)->map[uid].color = ITR_COLOR_RED;                     \
                    ITR_ROTATE_LEFT(x, prootid, uid, rid, lid, ppid);           \
                    uid = ITR(x)->map[xpid].left;                               \
                }                                                               \
                ITR(x)->map[uid].color = ITR(x)->map[xpid].color;               \
                ITR(x)->map[xpid].color = ITR_COLOR_BLACK;                      \
                if((lid = ITR(x)->map[uid].left) > 0)                           \
                    ITR(x)->map[lid].color = ITR_COLOR_BLACK;                   \
                ITR_ROTATE_RIGHT(x, prootid, xpid, lid, rid, ppid);             \
                oid = *prootid;                                                 \
                break;                                                          \
            }                                                                   \
        }                                                                       \
    }                                                                           \
    if(oid > 0) ITR(x)->map[oid].color = ITR_COLOR_BLACK;                       \
}while(0)
/* init itree */
void *itree_init(char *file)
{
    void *x = NULL;
    struct stat  st = {0};

    if((x = (ITREE *)calloc(1, sizeof(ITREE))))
    {
        if((ITR(x)->fd = open(file, O_CREAT|O_RDWR, 0644)) > 0 
                && fstat(ITR(x)->fd, &st) == 0)
        {
            MUTEX_INIT(ITR(x)->mutex);
            ITR(x)->end = st.st_size;
            ITR(x)->size = (off_t)sizeof(ITSTATE) + (off_t)sizeof(ITNODE) * (off_t)ITREE_NODES_MAX;
            //mmap
            ITR_MMAP(x);
            //init truncate
            if(st.st_size == 0)
            {
                ITR(x)->end = (off_t)sizeof(ITSTATE);
                ITR_INCRE(x);
            }
        }
        else 
        {
            if(ITR(x)->fd > 0) close(ITR(x)->fd);
            free(x);
            x = NULL;
        }
    }
    return x;
}

/* insert new root */
int itree_new_tree(void *x)
{
    int id = 0, i = 0;
    if(x)
    {
        MUTEX_LOCK(ITR(x)->mutex);
        if(ITR(x)->state->nroots == 0) ITR(x)->state->nroots = 1;
        if(ITR(x)->state && ITR(x)->state->nroots < ITREE_ROOT_MAX)
        {
            for(i = 1; i < ITREE_ROOT_MAX; i++)
            {
                if(ITR(x)->state->roots[i].status == 0)
                {
                    ITR(x)->state->roots[i].status = 1;
                    ITR(x)->state->nroots++;
                    id = i;
                    break;
                }
            }
        }
        MUTEX_UNLOCK(ITR(x)->mutex);
    }
    return id;
}

/* total */
uint32_t itree_total(void *x, int rootid)
{
    uint32_t total = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(ITR(x)->mutex);
        if(ITR(x)->state && ITR(x)->map && rootid < ITREE_ROOT_MAX)
        {
            total =  ITR(x)->state->roots[rootid].total;
        }
        MUTEX_UNLOCK(ITR(x)->mutex);
    }
    return total;
}

/* insert new node */
uint32_t itree_insert(void *x, int rootid, int key, int data, int *old)
{
    uint32_t id = 0, nodeid = 0, rid = 0, lid = 0, uid = 0, pid = 0, 
        gpid = 0, ppid = 0, *prootid = NULL;
    ITNODE *node = NULL;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(ITR(x)->mutex);
        if(ITR(x)->state && ITR(x)->map && rootid < ITREE_ROOT_MAX
                && ITR(x)->state->roots[rootid].status > 0)
        {
            nodeid = ITR(x)->state->roots[rootid].rootid;
            while(nodeid > 0 && nodeid < ITR(x)->state->total)
            {
                node = &(ITR(x)->map[nodeid]);
                if(key > node->key)
                {
                    if(node->right == 0) break;
                    nodeid = node->right;
                }
                else 
                {
                    if(node->left == 0) break;
                    nodeid = node->left;
                }
            }
            //new node
            if(id == 0)
            {
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, ITR(x)->start, ITR(x)->state, ITR(x)->map, ITR(x)->state->current, ITR(x)->state->left, ITR(x)->state->total, ITR(x)->state->qleft, ITR(x)->state->qfirst, ITR(x)->state->qlast);
                if(ITR(x)->state->left == 0)
                {
                    ITR_INCRE(x);
                }
                if(ITR(x)->state->qleft > 0)
                {
                    id = ITR(x)->state->qfirst;
                    ITR(x)->state->qfirst = ITR(x)->map[id].parent;
                    ITR(x)->state->qleft--;
                }
                else
                {
                    id = ++(ITR(x)->state->current);
                }
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, ITR(x)->start, ITR(x)->state, ITR(x)->map, ITR(x)->state->current, ITR(x)->state->left, ITR(x)->state->total, ITR(x)->state->qleft, ITR(x)->state->qfirst, ITR(x)->state->qlast);
                ITR(x)->state->left--;
                //memset(&(ITR(x)->map[id]), 0, sizeof(ITNODE));
                ITR(x)->map[id].parent = nodeid;
                ITR(x)->map[id].key = key;
                ITR(x)->map[id].data = data;
                ITR_MIN_MAX(x, id, key);
                if(nodeid > 0)
                {
                    if(key > ITR(x)->map[nodeid].key) 
                        ITR(x)->map[nodeid].right = id;
                    else
                        ITR(x)->map[nodeid].left = id;
                }
                ITR(x)->state->roots[rootid].total++;
            }
            else
            {
                //fprintf(stdout, "%s::%d old id:%d pid:%d key:%d\n", __FILE__, __LINE__, id, parentid, key);
            }
        }
        if((nodeid = id) > 0)
        {
            if(ITR(x)->state->roots[rootid].rootid > 0)
            {
                prootid = &(ITR(x)->state->roots[rootid].rootid);
                ITR(x)->map[nodeid].color = ITR_COLOR_RED;
                ITR_INSERT_COLOR(x, prootid, nodeid, lid, rid, uid, pid, gpid, ppid);
            }
            else
            {
                ITR(x)->state->roots[rootid].rootid = nodeid;
            }
        }
end:
        MUTEX_UNLOCK(ITR(x)->mutex);
    }
    return id;
}

/* try insert  node */
uint32_t itree_try_insert(void *x, int rootid, int key, int data, int *old)
{
    uint32_t id = 0, nodeid = 0, rid = 0, lid = 0, uid = 0, pid = 0, 
        gpid = 0, ppid = 0, *prootid = NULL;
    ITNODE *node = NULL;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(ITR(x)->mutex);
        if(ITR(x)->state && ITR(x)->map && rootid < ITREE_ROOT_MAX
                && ITR(x)->state->roots[rootid].status > 0)
        {
            nodeid = ITR(x)->state->roots[rootid].rootid;
            while(nodeid > 0 && nodeid < ITR(x)->state->total)
            {
                node = &(ITR(x)->map[nodeid]);
                if(key == node->key)
                {
                    id = nodeid;
                    if(old) *old = node->data;
                    //fprintf(stdout, "%s::%d id:%d key:%lld old[%lld]->data:%d\n", __FILE__, __LINE__, nodeid, (long long)key, (long long)node->key, node->data);
                    goto end;
                }
                else if(key > node->key)
                {
                    if(node->right == 0) break;
                    nodeid = node->right;
                }
                else 
                {
                    if(node->left == 0) break;
                    nodeid = node->left;
                }
            }
            //new node
            if(id == 0)
            {
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, ITR(x)->start, ITR(x)->state, ITR(x)->map, ITR(x)->state->current, ITR(x)->state->left, ITR(x)->state->total, ITR(x)->state->qleft, ITR(x)->state->qfirst, ITR(x)->state->qlast);
                if(ITR(x)->state->left == 0)
                {
                    ITR_INCRE(x);
                }
                if(ITR(x)->state->qleft > 0)
                {
                    id = ITR(x)->state->qfirst;
                    ITR(x)->state->qfirst = ITR(x)->map[id].parent;
                    ITR(x)->state->qleft--;
                }
                else
                {
                    id = ++(ITR(x)->state->current);
                }
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, ITR(x)->start, ITR(x)->state, ITR(x)->map, ITR(x)->state->current, ITR(x)->state->left, ITR(x)->state->total, ITR(x)->state->qleft, ITR(x)->state->qfirst, ITR(x)->state->qlast);
                ITR(x)->state->left--;
                //memset(&(ITR(x)->map[id]), 0, sizeof(ITNODE));
                ITR(x)->map[id].parent = nodeid;
                ITR(x)->map[id].key = key;
                ITR(x)->map[id].data = data;
                ITR_MIN_MAX(x, id, key);
                if(nodeid > 0)
                {
                    if(key > ITR(x)->map[nodeid].key) 
                        ITR(x)->map[nodeid].right = id;
                    else
                        ITR(x)->map[nodeid].left = id;
                }
                ITR(x)->state->roots[rootid].total++;
            }
            else
            {
                //fprintf(stdout, "%s::%d old id:%d pid:%d key:%d\n", __FILE__, __LINE__, id, parentid, key);
            }
        }
        if((nodeid = id) > 0)
        {
            if(ITR(x)->state->roots[rootid].rootid > 0)
            {
                prootid = &(ITR(x)->state->roots[rootid].rootid);
                ITR(x)->map[nodeid].color = ITR_COLOR_RED;
                ITR_INSERT_COLOR(x, prootid, nodeid, lid, rid, uid, pid, gpid, ppid);
            }
            else
            {
                ITR(x)->state->roots[rootid].rootid = nodeid;
            }
        }
end:
        MUTEX_UNLOCK(ITR(x)->mutex);
    }
    return id;
}


/* get node key/data */
uint32_t itree_get(void *x, uint32_t tnodeid, int *key, int *data)
{
    uint32_t id = 0;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(ITR(x)->mutex);
        if(ITR(x)->map && ITR(x)->state && tnodeid <  ITR(x)->state->total)
        {
            if(key) *key = ITR(x)->map[tnodeid].key;
            if(data) *data = ITR(x)->map[tnodeid].data;
            id = tnodeid;
        }
        MUTEX_UNLOCK(ITR(x)->mutex);
    }
    return id;
}

/* range key/data */
int itree_range(void *x, int rootid, int from, int to)
{
    int ret = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(ITR(x)->mutex);
        if(ITR(x)->map && ITR(x)->state && rootid < ITREE_ROOT_MAX
                && ITR(x)->state->roots[rootid].status > 0)
        {
            //fprintf(stdout, "%s::%d rootid:%d key:%d data:%d total:%d\n", __FILE__, __LINE__, rootid, key, *data, ITR(x)->state->total);
            id = ITR(x)->state->roots[rootid].rootid;
            while(id > 0 && id < ITR(x)->state->total)
            {
                if(key == ITR(x)->map[id].key)
                {
                    if(data) *data = ITR(x)->map[id].data;
                    break;
                }
                else if(key > ITR(x)->map[id].key)
                {
                    id = ITR(x)->map[id].right;
                }
                else
                {
                    id = ITR(x)->map[id].left;
                }
            }
        }
        //fprintf(stdout, "%s::%d rootid:%d key:%d data:%d\n", __FILE__, __LINE__, rootid, key, *data);
        MUTEX_UNLOCK(ITR(x)->mutex);
    }
    return id;
}

/* find key/data */
uint32_t itree_find(void *x, int rootid, int key, int *data)
{
    uint32_t id = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(ITR(x)->mutex);
        if(ITR(x)->map && ITR(x)->state && rootid < ITREE_ROOT_MAX
                && ITR(x)->state->roots[rootid].status > 0)
        {
            //fprintf(stdout, "%s::%d rootid:%d key:%d data:%d total:%d\n", __FILE__, __LINE__, rootid, key, *data, ITR(x)->state->total);
            id = ITR(x)->state->roots[rootid].rootid;
            while(id > 0 && id < ITR(x)->state->total)
            {
                if(key == ITR(x)->map[id].key)
                {
                    if(data) *data = ITR(x)->map[id].data;
                    break;
                }
                else if(key > ITR(x)->map[id].key)
                {
                    id = ITR(x)->map[id].right;
                }
                else
                {
                    id = ITR(x)->map[id].left;
                }
            }
        }
        //fprintf(stdout, "%s::%d rootid:%d key:%d data:%d\n", __FILE__, __LINE__, rootid, key, *data);
        MUTEX_UNLOCK(ITR(x)->mutex);
    }
    return id;
}

/* get tree->min key/data */
uint32_t itree_min(void *x, int rootid, int *key, int *data)
{
    uint32_t id = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(ITR(x)->mutex);
        if(ITR(x)->map && ITR(x)->state && rootid <  ITREE_ROOT_MAX
                && ITR(x)->state->roots[rootid].status > 0)
        {
            id = ITR(x)->state->roots[rootid].rootid;
            while(ITR(x)->map[id].left > 0)
            {
                id = ITR(x)->map[id].left;
            }
            if(id > 0 && ITR(x)->state->total)
            {
                if(key) *key = ITR(x)->map[id].key;
                if(data) *data = ITR(x)->map[id].data;
            }
        }
        MUTEX_UNLOCK(ITR(x)->mutex);
    }
    return id;
}

/* get tree->max key/data */
unsigned  int itree_max(void *x, int rootid, int *key, int *data)
{
    uint32_t id = 0, tmp = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(ITR(x)->mutex);
        if(ITR(x)->map && ITR(x)->state && rootid <  ITREE_ROOT_MAX
                && ITR(x)->state->roots[rootid].status > 0)
        {
            tmp = ITR(x)->state->roots[rootid].rootid;
            do
            {
                id = tmp;
            }while(id > 0 && (tmp = ITR(x)->map[id].right) > 0);
            if(id > 0 && ITR(x)->state->total)
            {
                if(key) *key = ITR(x)->map[id].key;
                if(data) *data = ITR(x)->map[id].data;
            }
        }
        MUTEX_UNLOCK(ITR(x)->mutex);
    }
    return id;
}

/* get next node key/data */
uint32_t itree_next(void *x, int rootid, uint32_t tnodeid, int *key, int *data)
{
    uint32_t id = 0, parentid = 0;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(ITR(x)->mutex);
        if(ITR(x)->map && ITR(x)->state && tnodeid <  ITR(x)->state->total)
        {
            id = tnodeid;
            if(ITR(x)->map[id].right > 0)
            {
                id = ITR(x)->map[id].right;
                while(ITR(x)->map[id].left  > 0)
                {
                    id = ITR(x)->map[id].left;
                }
                //fprintf(stdout, "%s::%d id:%d\n", __FILE__, __LINE__, id);
            }
            else
            {
                while(id > 0)
                {
                    parentid = ITR(x)->map[id].parent;
                    if(ITR(x)->map[id].key < ITR(x)->map[parentid].key)
                    {
                        id = parentid;
                        goto end;
                    }
                    else
                    {
                        id = parentid;
                    }
                }
                //fprintf(stdout, "%s::%d id:%d\n", __FILE__, __LINE__, id);
            }
end:
            if(id > 0 && id < ITR(x)->state->total)
            {
                if(key) *key = ITR(x)->map[id].key;
                if(data) *data = ITR(x)->map[id].data;
            }
            //fprintf(stdout, "%s::%d rootid:%d tnodeid:%d id:%d\n",__FILE__, __LINE__, rootid, tnodeid, id);
        }
        MUTEX_UNLOCK(ITR(x)->mutex);
    }
    return id;
}

/* get prev node key/data */
uint32_t itree_prev(void *x, int rootid, uint32_t tnodeid, int *key, int *data)
{
    uint32_t id = 0, parentid = 0;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(ITR(x)->mutex);
        if(ITR(x)->map && ITR(x)->state && tnodeid <  ITR(x)->state->total)
        {
            id = tnodeid;
            if(ITR(x)->map[id].left > 0)
            {
                id = ITR(x)->map[id].left;
                while(ITR(x)->map[id].right  > 0)
                {
                    id = ITR(x)->map[id].right;
                }
                //fprintf(stdout, "%s::%d id:%d\n", __FILE__, __LINE__, id);
            }
            else
            {
                while(id > 0)
                {
                    parentid = ITR(x)->map[id].parent;
                    if(ITR(x)->map[id].key > ITR(x)->map[parentid].key)
                    {
                        id = parentid;
                        goto end;
                    }
                    else
                    {
                        id = parentid;
                    }
                }
                //fprintf(stdout, "%s::%d id:%d\n", __FILE__, __LINE__, id);
            }
end:
            if(id > 0 && id < ITR(x)->state->total)
            {
                if(key)*key = ITR(x)->map[id].key;
                if(data)*data = ITR(x)->map[id].data;
            }
            //fprintf(stdout, "%s::%d rootid:%d tnodeid:%d id:%d\n",__FILE__, __LINE__, rootid, tnodeid, id);
        }
        MUTEX_UNLOCK(ITR(x)->mutex);
    }
    return id;
}

/* view node */
void itree_view_tnode(void *x, uint32_t tnodeid, FILE *fp)
{
    if(x)
    {
        if(ITR(x)->map[tnodeid].left > 0 && ITR(x)->map[tnodeid].left < ITR(x)->state->total)
        {
            itree_view_tnode(x, ITR(x)->map[tnodeid].left, fp);
        }
        fprintf(fp, "[%d:%lld:%d]\n", tnodeid, (long long)ITR(x)->map[tnodeid].key, ITR(x)->map[tnodeid].data);
        if(ITR(x)->map[tnodeid].right > 0 && ITR(x)->map[tnodeid].right < ITR(x)->state->total)
        {
            itree_view_tnode(x, ITR(x)->map[tnodeid].right, fp);
        }
    }
    return ;
}

void itree_view_tree(void *x, int rootid, FILE *fp)
{
    if(x && rootid > 0)
    {
        MUTEX_LOCK(ITR(x)->mutex);
        if(ITR(x)->map && ITR(x)->state && rootid < ITREE_ROOT_MAX)
        {
            fprintf(stdout, "%s::%d rootid:%d\n", __FILE__, __LINE__, ITR(x)->state->roots[rootid].rootid);
             itree_view_tnode(x, ITR(x)->state->roots[rootid].rootid, fp);
        }
        MUTEX_UNLOCK(ITR(x)->mutex);
    }
    return ;
}

/* set data */
int itree_set_data(void *x, uint32_t tnodeid, int data)
{
    int old = -1;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(ITR(x)->mutex);
        if(ITR(x)->map && ITR(x)->state && tnodeid < ITR(x)->state->total)
        {
            old = ITR(x)->map[tnodeid].data;
            ITR(x)->map[tnodeid].data = data;
        }
        MUTEX_UNLOCK(ITR(x)->mutex);
    }
    return old;
}

/* remove node */
void itree_remove(void *x, int rootid, uint32_t tnodeid, int *key, int *data)
{
    uint32_t id = 0, pid = 0, parent = 0, child = 0, rid = 0, lid = 0,
        uid = 0, ppid = 0, z = 0, color = 0, *prootid = NULL;

    if(x && rootid > 0 && tnodeid > 0)
    {
        MUTEX_LOCK(ITR(x)->mutex);
        if(ITR(x)->map && ITR(x)->state && tnodeid < ITR(x)->state->total)
        {
            if(key) *key = ITR(x)->map[tnodeid].key;
            if(data) *data = ITR(x)->map[tnodeid].data;
            id = tnodeid;
            if(ITR(x)->map[tnodeid].left == 0)
            {
                child = ITR(x)->map[tnodeid].right;
            }
            else if(ITR(x)->map[tnodeid].right == 0)
            {
                child = ITR(x)->map[tnodeid].left;
            }
            else 
            {
                id = ITR(x)->map[tnodeid].right;
                while(ITR(x)->map[id].left > 0)
                    id = ITR(x)->map[id].left;
                parent = ITR(x)->map[id].parent;
                color = ITR(x)->map[id].color;
                if((child = ITR(x)->map[id].right) > 0)
                    ITR(x)->map[child].parent = parent;
                if((pid = parent) > 0)
                {
                    if(ITR(x)->map[pid].left == id)
                        ITR(x)->map[pid].left = child;
                    else
                        ITR(x)->map[pid].right = child;
                }
                else
                {
                    ITR(x)->state->roots[rootid].rootid = child;
                }
                if(ITR(x)->map[id].parent == tnodeid) parent = id;
                ITR(x)->map[id].color = ITR(x)->map[tnodeid].color;
                ITR(x)->map[id].parent = ITR(x)->map[tnodeid].parent;
                ITR(x)->map[id].left = ITR(x)->map[tnodeid].left;
                ITR(x)->map[id].right = ITR(x)->map[tnodeid].right;
                if((pid = ITR(x)->map[tnodeid].parent) > 0)
                {
                    if(ITR(x)->map[pid].left == tnodeid)
                        ITR(x)->map[pid].left = id;
                    else
                        ITR(x)->map[pid].right = id;
                }
                else
                {
                    ITR(x)->state->roots[rootid].rootid = id;
                }
                lid = ITR(x)->map[tnodeid].left;
                ITR(x)->map[lid].parent = id;
                if((rid = ITR(x)->map[tnodeid].right) > 0)
                    ITR(x)->map[rid].parent = id;
                goto color_remove;
            }
            parent =  ITR(x)->map[tnodeid].parent;
            color = ITR(x)->map[tnodeid].color;
            if(child > 0) 
            {
                ITR(x)->map[child].parent = parent;
            }
            if((pid = parent) > 0)
            {
                if(tnodeid == ITR(x)->map[pid].left) 
                    ITR(x)->map[pid].left = child;
                else 
                    ITR(x)->map[pid].right = child;
            }
            else 
            {
                ITR(x)->state->roots[rootid].rootid = child;
            }
            //remove color set
color_remove:
            ITR(x)->state->roots[rootid].total--;
            if(color == ITR_COLOR_BLACK)
            {
                //fprintf(stdout, "%s::%d node:%d parent:%d left:%d right:%d key:%d data:%d\n", __FILE__, __LINE__, tnodeid, ITR(x)->map[tnodeid].parent, ITR(x)->map[tnodeid].left, ITR(x)->map[tnodeid].right, ITR(x)->map[tnodeid].key, ITR(x)->map[tnodeid].data);
                prootid = &(ITR(x)->state->roots[rootid].rootid);
                ITR_REMOVE_COLOR(x, prootid, child, parent, lid, rid, uid, ppid);
            }
            //add to qleft
            memset(&(ITR(x)->map[tnodeid]), 0, sizeof(ITNODE));
            if(ITR(x)->state->qleft == 0)
            {
                ITR(x)->state->qfirst = ITR(x)->state->qlast = tnodeid;
            }
            else
            {
                z = ITR(x)->state->qlast;
                ITR(x)->map[z].parent = tnodeid;
                ITR(x)->state->qlast = tnodeid;
            }
            ITR(x)->state->qleft++;
            ITR(x)->state->left++;
            //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, ITR(x)->start, ITR(x)->state, ITR(x)->map, ITR(x)->state->current, ITR(x)->state->left, ITR(x)->state->total, ITR(x)->state->qleft, ITR(x)->state->qfirst, ITR(x)->state->qlast);
  
        }
        MUTEX_UNLOCK(ITR(x)->mutex);
    }
    return ;
}

/* remove node */
void itree_remove_tnode(void *x, uint32_t tnodeid)
{
    uint32_t id = 0;

    if(x)
    {
        if(ITR(x)->map[tnodeid].left > 0 && ITR(x)->map[tnodeid].left < ITR(x)->state->total)
        {
            itree_remove_tnode(x, ITR(x)->map[tnodeid].left);
        }
        if(ITR(x)->map[tnodeid].right > 0 && ITR(x)->map[tnodeid].right < ITR(x)->state->total)
        {
            itree_remove_tnode(x, ITR(x)->map[tnodeid].right);
        }
        memset(&(ITR(x)->map[tnodeid]), 0, sizeof(ITNODE));
        if(ITR(x)->state->qleft == 0)
        {
            ITR(x)->state->qfirst = ITR(x)->state->qlast = tnodeid;
        }
        else
        {
            id = ITR(x)->state->qlast;
            ITR(x)->map[id].parent = tnodeid;
            ITR(x)->state->qlast = tnodeid;
        }
        ITR(x)->state->qleft++;
        ITR(x)->state->left++;
    }
    return ;
}

/* remove tree */
void itree_remove_tree(void *x, int rootid)
{
    if(x && rootid > 0 && rootid < ITREE_ROOT_MAX)
    {
        MUTEX_LOCK(ITR(x)->mutex);
        itree_remove_tnode(x, ITR(x)->state->roots[rootid].rootid);
        ITR(x)->state->roots[rootid].rootid = 0;
        ITR(x)->state->roots[rootid].status = 0;
        //fprintf(stdout, "%s::%d rootid:%d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, rootid, ITR(x)->start, ITR(x)->state, ITR(x)->map, ITR(x)->state->current, ITR(x)->state->left, ITR(x)->state->total, ITR(x)->state->qleft, ITR(x)->state->qfirst, ITR(x)->state->qlast);
 
        MUTEX_UNLOCK(ITR(x)->mutex);
    }
    return ;
}

//close itree
void itree_close(void *x)
{
    if(x)
    {
        //fprintf(stdout, "%s::%d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d sizeof(ITSTATE):%d\n", __FILE__, __LINE__, ITR(x)->start, ITR(x)->state, ITR(x)->map, ITR(x)->state->current, ITR(x)->state->left, ITR(x)->state->total, ITR(x)->state->qleft, ITR(x)->state->qfirst, ITR(x)->state->qlast, sizeof(ITSTATE));
        ITR_MUNMAP(x);
        MUTEX_DESTROY(ITR(x)->mutex);
        if(ITR(x)->fd) close(ITR(x)->fd);
        free(x);
    }
}


#ifdef _DEBUG_ITREE
#include "md5.h"
#include "timer.h"
int main(int argc, char **argv) 
{
    int i = 0, rootid = 0, id = 0, j = 0, old = 0, data = 0, n = 0, count = 50000000;
    unsigned char digest[MD5_LEN];
    void *itree = NULL;
    void *timer = NULL;
    char line[1024];
    int key = 0;

    if((itree = itree_init("/tmp/test.itree")))
    {
        rootid = itree_new_tree(itree);
        TIMER_INIT(timer);
        for(j = 1; j <= count; j++)
        {
            n = sprintf(line, "http://www.demo.com/%d.html", j);
            md5(line, n, digest);
            key = *((int *)digest);
            old = -1;
            data = j;
            id = itree_insert(itree, rootid, key, data, &old);
            if(old > 0 || id <= 0) 
            {
                fprintf(stdout, "%d:{id:%d key:%d rootid:%d old:%d}\n", j, id, key, rootid, old);
            }
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "%s::%d insert:%d time:%lld\n", __FILE__,__LINE__, count, PT_LU_USEC(timer));
        for(j = 1; j <= count; j++)
        {
            n = sprintf(line, "http://www.demo.com/%d.html", j);
            md5(line, n, digest);
            key = *((int *)digest);
            old = -1;
            data = j;
            id = itree_try_insert(itree, rootid, key, data, &old);
            if(old > 0 && old != j) 
            {
                fprintf(stdout, "%d:{id:%d key:%d rootid:%d old:%d}\n", j, id, key, rootid, old);
                _exit(-1);
            }
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "%s::%d try_insert:%d time:%lld\n", __FILE__,__LINE__, count, PT_LU_USEC(timer));
        TIMER_CLEAN(timer);
        itree_close(itree);
    }
}
//gcc -o mtree64 itree.c md5.c -D_DEBUG_ITREE -g && ./mtree64
#endif
