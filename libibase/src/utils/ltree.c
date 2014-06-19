#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include "ltree.h"
#include "mutex.h"
#define MMAP_SHARED MAP_SHARED|MAP_LOCKED
#define LTR(px) ((LTREE *)px)
#define LTR_COLOR_BLACK  0
#define LTR_COLOR_RED    1
#define LTR_MIN_MAX(x, key, xid)                                                \
do                                                                              \
{                                                                               \
    if(LTR(x) && LTR(x)->state)                                                 \
    {                                                                           \
        if(LTR(x)->state->count == 0)                                           \
        {                                                                       \
            LTR(x)->state->nmin = LTR(x)->state->nmax = xid;                    \
            LTR(x)->state->kmin = LTR(x)->state->kmax = key;                    \
        }                                                                       \
        else if(key > LTR(x)->state->kmax)                                      \
        {                                                                       \
            LTR(x)->state->nmax = xid;                                          \
            LTR(x)->state->kmax = key;                                          \
        }                                                                       \
        else if(key < LTR(x)->state->kmin)                                      \
        {                                                                       \
            LTR(x)->state->nmin = xid;                                          \
            LTR(x)->state->kmin = key;                                          \
        }                                                                       \
    }                                                                           \
}while(0)
#define LTR_MUNMAP(x)                                                           \
do                                                                              \
{                                                                               \
    if(x && LTR(x)->size > 0)                                                   \
    {                                                                           \
        if(LTR(x)->start && LTR(x)->start != (void *)-1)                        \
        {                                                                       \
            munmap(LTR(x)->start, LTR(x)->size);                                \
            LTR(x)->start = NULL;                                               \
            LTR(x)->state = NULL;                                               \
            LTR(x)->map = NULL;                                                 \
        }                                                                       \
    }                                                                           \
}while(0)

#define LTR_MMAP(x)                                                             \
do                                                                              \
{                                                                               \
    if(x)                                                                       \
    {                                                                           \
        if((LTR(x)->start = (char *)mmap(NULL,LTR(x)->size,PROT_READ|PROT_WRITE,\
                    MMAP_SHARED, LTR(x)->fd, 0)) != (void *)-1)                  \
        {                                                                       \
            LTR(x)->state = (LTSTATE *)LTR(x)->start;                           \
            LTR(x)->map = (LTNODE *)(LTR(x)->start + sizeof(LTSTATE));          \
        }                                                                       \
    }                                                                           \
}while(0)

#define LTR_INCRE(x)                                                            \
do                                                                              \
{                                                                               \
    if(x &&  LTR(x)->end <  LTR(x)->size)                                       \
    {                                                                           \
        LTR(x)->old = LTR(x)->end;                                              \
        LTR(x)->end += (off_t)LTREE_INCRE_NUM * (off_t)sizeof(LTNODE);       \
        if(ftruncate(LTR(x)->fd, LTR(x)->end) == 0)                             \
        {                                                                       \
            if(LTR(x)->old == sizeof(LTSTATE))                                  \
            {                                                                   \
                memset(LTR(x)->state, 0, sizeof(LTSTATE));                      \
                LTR(x)->state->left += LTREE_INCRE_NUM - 1;                  \
            }                                                                   \
            else                                                                \
            {                                                                   \
                LTR(x)->state->left += LTREE_INCRE_NUM;                      \
            }                                                                   \
            LTR(x)->state->total += LTREE_INCRE_NUM;                         \
            memset(LTR(x)->start + LTR(x)->old, 0, LTR(x)->end - LTR(x)->old);  \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            _exit(-1);                                                          \
        }                                                                       \
    }                                                                           \
}while(0)
#define LTR_ROTATE_LEFT(x, prootid, oid, rid, lid, ppid)                        \
do                                                                              \
{                                                                               \
    if(x && (rid = LTR(x)->map[oid].right) > 0)                                 \
    {                                                                           \
        if((lid = LTR(x)->map[oid].right = LTR(x)->map[rid].left) > 0)          \
        {                                                                       \
            LTR(x)->map[lid].parent = oid;                                      \
        }                                                                       \
        if((ppid = LTR(x)->map[rid].parent = LTR(x)->map[oid].parent) > 0)      \
        {                                                                       \
            if(LTR(x)->map[ppid].left == oid)                                   \
                LTR(x)->map[ppid].left = rid;                                   \
            else                                                                \
                LTR(x)->map[ppid].right = rid;                                  \
        }else *prootid = rid;                                                   \
        LTR(x)->map[rid].left = oid;                                            \
        LTR(x)->map[oid].parent = rid;                                          \
    }                                                                           \
}while(0)

#define LTR_ROTATE_RIGHT(x, prootid, oid, lid, rid, ppid)                       \
do                                                                              \
{                                                                               \
    if(x && (lid = LTR(x)->map[oid].left) > 0)                                  \
    {                                                                           \
        if((rid = LTR(x)->map[oid].left = LTR(x)->map[lid].right) > 0)          \
        {                                                                       \
            LTR(x)->map[rid].parent = oid;                                      \
        }                                                                       \
        if((ppid = LTR(x)->map[lid].parent = LTR(x)->map[oid].parent) > 0)      \
        {                                                                       \
            if(LTR(x)->map[ppid].left == oid)                                   \
                LTR(x)->map[ppid].left = lid;                                   \
            else                                                                \
                LTR(x)->map[ppid].right = lid;                                  \
        }                                                                       \
        else *prootid = lid;                                                    \
        LTR(x)->map[lid].right = oid;                                           \
        LTR(x)->map[oid].parent = lid;                                          \
    }                                                                           \
}while(0)

#define LTR_INSERT_COLOR(x, prootid, oid, lid, rid, uid, pid, gpid, ppid)       \
do                                                                              \
{                                                                               \
    while((pid = LTR(x)->map[oid].parent)> 0                                    \
            && LTR(x)->map[pid].color == LTR_COLOR_RED)                         \
    {                                                                           \
        gpid = LTR(x)->map[pid].parent;                                         \
        if(pid == LTR(x)->map[gpid].left)                                       \
        {                                                                       \
            uid = LTR(x)->map[gpid].right;                                      \
            if(uid > 0 && LTR(x)->map[uid].color == LTR_COLOR_RED)              \
            {                                                                   \
                LTR(x)->map[uid].color = LTR_COLOR_BLACK;                       \
                LTR(x)->map[pid].color = LTR_COLOR_BLACK;                       \
                LTR(x)->map[gpid].color = LTR_COLOR_RED;                        \
                oid = gpid;                                                     \
                continue;                                                       \
            }                                                                   \
            if(LTR(x)->map[pid].right == oid)                                   \
            {                                                                   \
                LTR_ROTATE_LEFT(x, prootid, pid, rid, lid, ppid);               \
                uid = pid; pid = oid; oid = uid;                                \
            }                                                                   \
            LTR(x)->map[pid].color = LTR_COLOR_BLACK;                           \
            LTR(x)->map[gpid].color = LTR_COLOR_RED;                            \
            LTR_ROTATE_RIGHT(x, prootid, gpid, lid, rid, ppid);                 \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            uid = LTR(x)->map[gpid].left;                                       \
            if(uid > 0 && LTR(x)->map[uid].color == LTR_COLOR_RED)              \
            {                                                                   \
                LTR(x)->map[uid].color = LTR_COLOR_BLACK;                       \
                LTR(x)->map[pid].color = LTR_COLOR_BLACK;                       \
                LTR(x)->map[gpid].color = LTR_COLOR_RED;                        \
                oid = gpid;                                                     \
                continue;                                                       \
            }                                                                   \
            if(LTR(x)->map[pid].left == oid)                                    \
            {                                                                   \
                LTR_ROTATE_RIGHT(x, prootid, pid, lid, rid, ppid);              \
                uid = pid; pid = oid; oid = uid;                                \
            }                                                                   \
            LTR(x)->map[pid].color = LTR_COLOR_BLACK;                           \
            LTR(x)->map[gpid].color = LTR_COLOR_RED;                            \
            LTR_ROTATE_LEFT(x, prootid, gpid, rid, lid, ppid);                  \
        }                                                                       \
    }                                                                           \
    if(*prootid > 0)LTR(x)->map[*prootid].color = LTR_COLOR_BLACK;              \
}while(0)

#define LTR_REMOVE_COLOR(x, prootid, oid, xpid, lid, rid, uid, ppid)            \
do                                                                              \
{                                                                               \
    while((oid == 0 || LTR(x)->map[oid].color == LTR_COLOR_BLACK)               \
            && oid != *prootid)                                                 \
    {                                                                           \
        if(LTR(x)->map[xpid].left == oid)                                       \
        {                                                                       \
            uid = LTR(x)->map[xpid].right;                                      \
            if(LTR(x)->map[uid].color == LTR_COLOR_RED)                         \
            {                                                                   \
                LTR(x)->map[uid].color = LTR_COLOR_BLACK;                       \
                LTR(x)->map[xpid].color = LTR_COLOR_RED;                        \
                LTR_ROTATE_LEFT(x, prootid, xpid, rid, lid, ppid);              \
                uid = LTR(x)->map[xpid].right;                                  \
            }                                                                   \
            lid = LTR(x)->map[uid].left;                                        \
            rid = LTR(x)->map[uid].right;                                       \
            if((lid == 0 || LTR(x)->map[lid].color == LTR_COLOR_BLACK)          \
                && (rid == 0 || LTR(x)->map[rid].color == LTR_COLOR_BLACK))     \
            {                                                                   \
                LTR(x)->map[uid].color = LTR_COLOR_RED;                         \
                oid = xpid;                                                      \
                xpid = LTR(x)->map[oid].parent;                                  \
            }                                                                   \
            else                                                                \
            {                                                                   \
                rid = LTR(x)->map[uid].right;                                   \
                lid = LTR(x)->map[uid].left;                                    \
                if(rid == 0 || LTR(x)->map[rid].color == LTR_COLOR_BLACK)       \
                {                                                               \
                    if(lid > 0)LTR(x)->map[lid].color = LTR_COLOR_BLACK;        \
                    LTR(x)->map[uid].color = LTR_COLOR_RED;                     \
                    LTR_ROTATE_RIGHT(x, prootid, uid, lid, rid, ppid);          \
                    uid = LTR(x)->map[xpid].right;                              \
                }                                                               \
                LTR(x)->map[uid].color = LTR(x)->map[xpid].color;               \
                LTR(x)->map[xpid].color = LTR_COLOR_BLACK;                      \
                if((rid = LTR(x)->map[uid].right) > 0)                          \
                    LTR(x)->map[rid].color = LTR_COLOR_BLACK;                   \
                LTR_ROTATE_LEFT(x, prootid, xpid, rid, lid, ppid);              \
                oid = *prootid;                                                 \
                break;                                                          \
            }                                                                   \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            uid = LTR(x)->map[xpid].left;                                       \
            if(LTR(x)->map[uid].color == LTR_COLOR_RED)                         \
            {                                                                   \
                LTR(x)->map[uid].color = LTR_COLOR_BLACK;                       \
                LTR(x)->map[xpid].color = LTR_COLOR_RED;                        \
                LTR_ROTATE_RIGHT(x, prootid, xpid, lid, rid, ppid);             \
                uid = LTR(x)->map[xpid].left;                                   \
            }                                                                   \
            lid = LTR(x)->map[uid].left;                                        \
            rid = LTR(x)->map[uid].right;                                       \
            if((lid == 0 || LTR(x)->map[lid].color == LTR_COLOR_BLACK)          \
                && (rid == 0 || LTR(x)->map[rid].color == LTR_COLOR_BLACK))     \
            {                                                                   \
                LTR(x)->map[uid].color = LTR_COLOR_RED;                         \
                oid = xpid;                                                     \
                xpid = LTR(x)->map[oid].parent;                                 \
            }                                                                   \
            else                                                                \
            {                                                                   \
                rid = LTR(x)->map[uid].right;                                   \
                lid = LTR(x)->map[uid].left;                                    \
                if(lid == 0 || LTR(x)->map[lid].color == LTR_COLOR_BLACK)       \
                {                                                               \
                    if(rid > 0)LTR(x)->map[rid].color = LTR_COLOR_BLACK;        \
                    LTR(x)->map[uid].color = LTR_COLOR_RED;                     \
                    LTR_ROTATE_LEFT(x, prootid, uid, rid, lid, ppid);           \
                    uid = LTR(x)->map[xpid].left;                               \
                }                                                               \
                LTR(x)->map[uid].color = LTR(x)->map[xpid].color;               \
                LTR(x)->map[xpid].color = LTR_COLOR_BLACK;                      \
                if((lid = LTR(x)->map[uid].left) > 0)                           \
                    LTR(x)->map[lid].color = LTR_COLOR_BLACK;                   \
                LTR_ROTATE_RIGHT(x, prootid, xpid, lid, rid, ppid);             \
                oid = *prootid;                                                 \
                break;                                                          \
            }                                                                   \
        }                                                                       \
    }                                                                           \
    if(oid > 0) LTR(x)->map[oid].color = LTR_COLOR_BLACK;                       \
}while(0)
void ltree_mutex_lock(void *x, int id)
{
    if(x)
    {
#ifdef HAVE_PTHREAD
        pthread_mutex_lock(&(LTR(x)->mutexs[id%LTREE_MUTEX_MAX]));
#endif
    }
    return ;
}
void ltree_mutex_unlock(void *x, int id)
{
    if(x)
    {
#ifdef HAVE_PTHREAD
        pthread_mutex_unlock(&(LTR(x)->mutexs[id%LTREE_MUTEX_MAX]));
#endif
    }
    return ;
}

/* init ltree */
void *ltree_init(char *file)
{
    int i = 0;
    void *x = NULL;
    struct stat  st = {0};

    if((x = (LTREE *)calloc(1, sizeof(LTREE))))
    {
        if((LTR(x)->fd = open(file, O_CREAT|O_RDWR, 0644)) > 0 
                && fstat(LTR(x)->fd, &st) == 0)
        {
            MUTEX_INIT(LTR(x)->mutex);
            LTR(x)->end = st.st_size;
            LTR(x)->size = (off_t)sizeof(LTSTATE) + (off_t)sizeof(LTNODE) * (off_t)LTREE_NODES_MAX;
            //mmap
            LTR_MMAP(x);
            //init truncate
            if(st.st_size == 0)
            {
                LTR(x)->end = (off_t)sizeof(LTSTATE);
                LTR_INCRE(x);
            }
            /* initialize mutexs  */
#ifdef HAVE_PTHREAD
            for(i = 0; i < LTREE_MUTEX_MAX; i++)
            {
                pthread_mutex_init(&(LTR(x)->mutexs[i]), NULL);
            }
#endif
        }
        else 
        {
            if(LTR(x)->fd > 0) close(LTR(x)->fd);
            free(x);
            x = NULL;
        }
    }
    return x;
}

/* insert new root */
int ltree_new_tree(void *x)
{
    int id = 0, i = 0;
    if(x)
    {
        MUTEX_LOCK(LTR(x)->mutex);
        if(LTR(x)->state->nroots == 0) LTR(x)->state->nroots = 1;
        if(LTR(x)->state && LTR(x)->state->nroots < LTREE_ROOT_MAX)
        {
            for(i = 1; i < LTREE_ROOT_MAX; i++)
            {
                if(LTR(x)->state->roots[i].status == 0)
                {
                    LTR(x)->state->roots[i].status = 1;
                    LTR(x)->state->nroots++;
                    id = i;
                    break;
                }
            }
        }
        MUTEX_UNLOCK(LTR(x)->mutex);
    }
    return id;
}

/* total */
uint32_t ltree_total(void *x, int rootid)
{
    uint32_t total = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(LTR(x)->mutex);
        if(LTR(x)->state && LTR(x)->map && rootid < LTREE_ROOT_MAX)
        {
            total =  LTR(x)->state->roots[rootid].total;
        }
        MUTEX_UNLOCK(LTR(x)->mutex);
    }
    return total;
}

//add nodeid to qleft
void ltree_qleft(void *x, int tnodeid)
{
    int z = 0;
    if(x)
    {
        MUTEX_LOCK(LTR(x)->mutex);
        memset(&(LTR(x)->map[tnodeid]), 0, sizeof(LTNODE));
        if(LTR(x)->state->qleft == 0)
        {
            LTR(x)->state->qfirst = LTR(x)->state->qlast = tnodeid;
        }
        else
        {
            z = LTR(x)->state->qlast;
            LTR(x)->map[z].parent = tnodeid;
            LTR(x)->state->qlast = tnodeid;
        }
        LTR(x)->state->qleft++;
        LTR(x)->state->left++;
        MUTEX_UNLOCK(LTR(x)->mutex);
    }
    return ;
}
//new node
uint32_t ltree_new_node(void *x, int rootid, int nodeid, int64_t key, int data)
{
    uint32_t id = 0;

    if(x)
    {
        MUTEX_LOCK(LTR(x)->mutex);
        //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, LTR(x)->start, LTR(x)->state, LTR(x)->map, LTR(x)->state->current, LTR(x)->state->left, LTR(x)->state->total, LTR(x)->state->qleft, LTR(x)->state->qfirst, LTR(x)->state->qlast);
        if(LTR(x)->state->left == 0)
        {
            LTR_INCRE(x);
        }
        if(LTR(x)->state->qleft > 0)
        {
            id = LTR(x)->state->qfirst;
            LTR(x)->state->qfirst = LTR(x)->map[id].parent;
            LTR(x)->state->qleft--;
        }
        else
        {
            id = ++(LTR(x)->state->current);
        }
        //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, LTR(x)->start, LTR(x)->state, LTR(x)->map, LTR(x)->state->current, LTR(x)->state->left, LTR(x)->state->total, LTR(x)->state->qleft, LTR(x)->state->qfirst, LTR(x)->state->qlast);
        LTR(x)->state->left--;
        //memset(&(LTR(x)->map[id]), 0, sizeof(LTNODE));
        LTR(x)->map[id].parent = nodeid;
        LTR(x)->map[id].key = key;
        LTR(x)->map[id].data = data;
        LTR_MIN_MAX(x, id, key);
        if(nodeid > 0)
        {
            if(key > LTR(x)->map[nodeid].key) 
                LTR(x)->map[nodeid].right = id;
            else
                LTR(x)->map[nodeid].left = id;
        }
        LTR(x)->state->roots[rootid].total++;
        MUTEX_UNLOCK(LTR(x)->mutex);
    }
    return id;
}

/* insert new node */
uint32_t ltree_insert(void *x, int rootid, int64_t key, int data, int *old)
{
    uint32_t id = 0, nodeid = 0, rid = 0, lid = 0, uid = 0, pid = 0, 
        gpid = 0, ppid = 0, *prootid = NULL;
    LTNODE *node = NULL;

    if(x && rootid > 0)
    {
        ltree_mutex_lock(x, rootid);
        if(LTR(x)->state && LTR(x)->map && rootid < LTREE_ROOT_MAX
                && LTR(x)->state->roots[rootid].status > 0)
        {
            nodeid = LTR(x)->state->roots[rootid].rootid;
            while(nodeid > 0 && nodeid < LTR(x)->state->total)
            {
                node = &(LTR(x)->map[nodeid]);
                if(key == node->key)
                {
                    id = nodeid;
                    if(old) *old = node->data;
                    node->data = data;
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
            if(id == 0) id = ltree_new_node(x, rootid, nodeid, key, data);
        }
        if((nodeid = id) > 0)
        {
            if(LTR(x)->state->roots[rootid].rootid > 0)
            {
                prootid = &(LTR(x)->state->roots[rootid].rootid);
                LTR(x)->map[nodeid].color = LTR_COLOR_RED;
                LTR_INSERT_COLOR(x, prootid, nodeid, lid, rid, uid, pid, gpid, ppid);
            }
            else
            {
                LTR(x)->state->roots[rootid].rootid = nodeid;
            }
        }
end:
        ltree_mutex_unlock(x, rootid);
    }
    return id;
}


/* try insert  node */
uint32_t ltree_try_insert(void *x, int rootid, int64_t key, int data, int *old)
{
    uint32_t id = 0, nodeid = 0, rid = 0, lid = 0, uid = 0, pid = 0, 
        gpid = 0, ppid = 0, *prootid = NULL;
    LTNODE *node = NULL;

    if(x && rootid > 0)
    {
        ltree_mutex_lock(x, rootid);
        if(LTR(x)->state && LTR(x)->map && rootid < LTREE_ROOT_MAX
                && LTR(x)->state->roots[rootid].status > 0)
        {
            nodeid = LTR(x)->state->roots[rootid].rootid;
            while(nodeid > 0 && nodeid < LTR(x)->state->total)
            {
                node = &(LTR(x)->map[nodeid]);
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
        }
        if(id == 0) id = ltree_new_node(x, rootid, nodeid, key, data);
        if((nodeid = id) > 0)
        {
            if(LTR(x)->state->roots[rootid].rootid > 0)
            {
                prootid = &(LTR(x)->state->roots[rootid].rootid);
                LTR(x)->map[nodeid].color = LTR_COLOR_RED;
                LTR_INSERT_COLOR(x, prootid, nodeid, lid, rid, uid, pid, gpid, ppid);
            }
            else
            {
                LTR(x)->state->roots[rootid].rootid = nodeid;
            }
        }
end:
        ltree_mutex_unlock(x, rootid);
    }
    return id;
}


/* get node key/data */
uint32_t ltree_get(void *x, uint32_t tnodeid, int64_t *key, int *data)
{
    uint32_t id = 0;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(LTR(x)->mutex);
        if(LTR(x)->map && LTR(x)->state && tnodeid <  LTR(x)->state->total)
        {
            if(key) *key = LTR(x)->map[tnodeid].key;
            if(data) *data = LTR(x)->map[tnodeid].data;
            id = tnodeid;
        }
        MUTEX_UNLOCK(LTR(x)->mutex);
    }
    return id;
}

/* find key/data */
uint32_t ltree_find(void *x, int rootid, int64_t key, int *data)
{
    uint32_t id = 0;

    if(x && rootid > 0)
    {
        ltree_mutex_lock(x, rootid);
        if(LTR(x)->map && LTR(x)->state && rootid < LTREE_ROOT_MAX
                && LTR(x)->state->roots[rootid].status > 0)
        {
            //fprintf(stdout, "%s::%d rootid:%d key:%d data:%d total:%d\n", __FILE__, __LINE__, rootid, key, *data, LTR(x)->state->total);
            id = LTR(x)->state->roots[rootid].rootid;
            while(id > 0 && id < LTR(x)->state->total)
            {
                if(key == LTR(x)->map[id].key)
                {
                    if(data) *data = LTR(x)->map[id].data;
                    break;
                }
                else if(key > LTR(x)->map[id].key)
                {
                    id = LTR(x)->map[id].right;
                }
                else
                {
                    id = LTR(x)->map[id].left;
                }
            }
        }
        //fprintf(stdout, "%s::%d rootid:%d key:%d data:%d\n", __FILE__, __LINE__, rootid, key, *data);
        ltree_mutex_unlock(x, rootid);
    }
    return id;
}

/* get tree->min key/data */
uint32_t ltree_min(void *x, int rootid, int64_t *key, int *data)
{
    uint32_t id = 0;

    if(x && rootid > 0)
    {
        ltree_mutex_lock(x, rootid);
        if(LTR(x)->map && LTR(x)->state && rootid <  LTREE_ROOT_MAX
                && LTR(x)->state->roots[rootid].status > 0)
        {
            id = LTR(x)->state->roots[rootid].rootid;
            while(LTR(x)->map[id].left > 0)
            {
                id = LTR(x)->map[id].left;
            }
            if(id > 0 && LTR(x)->state->total)
            {
                if(key) *key = LTR(x)->map[id].key;
                if(data) *data = LTR(x)->map[id].data;
            }
        }
        ltree_mutex_unlock(x, rootid);
    }
    return id;
}

/* get tree->max key/data */
unsigned  int ltree_max(void *x, int rootid, int64_t *key, int *data)
{
    uint32_t id = 0, tmp = 0;

    if(x && rootid > 0)
    {
        ltree_mutex_lock(x, rootid);
        if(LTR(x)->map && LTR(x)->state && rootid <  LTREE_ROOT_MAX
                && LTR(x)->state->roots[rootid].status > 0)
        {
            tmp = LTR(x)->state->roots[rootid].rootid;
            do
            {
                id = tmp;
            }while(id > 0 && (tmp = LTR(x)->map[id].right) > 0);
            if(id > 0 && LTR(x)->state->total)
            {
                if(key) *key = LTR(x)->map[id].key;
                if(data) *data = LTR(x)->map[id].data;
            }
        }
        ltree_mutex_unlock(x, rootid);
    }
    return id;
}

/* get next node key/data */
uint32_t ltree_next(void *x, int rootid, uint32_t tnodeid, int64_t *key, int *data)
{
    uint32_t id = 0, parentid = 0;

    if(x && tnodeid > 0 && rootid > 0)
    {
        ltree_mutex_lock(x, rootid);
        if(LTR(x)->map && LTR(x)->state && tnodeid <  LTR(x)->state->total)
        {
            id = tnodeid;
            if(LTR(x)->map[id].right > 0)
            {
                id = LTR(x)->map[id].right;
                while(LTR(x)->map[id].left  > 0)
                {
                    id = LTR(x)->map[id].left;
                }
                //fprintf(stdout, "%s::%d id:%d\n", __FILE__, __LINE__, id);
            }
            else
            {
                while(id > 0)
                {
                    parentid = LTR(x)->map[id].parent;
                    if(LTR(x)->map[id].key < LTR(x)->map[parentid].key)
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
            if(id > 0 && id < LTR(x)->state->total)
            {
                if(key) *key = LTR(x)->map[id].key;
                if(data) *data = LTR(x)->map[id].data;
            }
            //fprintf(stdout, "%s::%d rootid:%d tnodeid:%d id:%d\n",__FILE__, __LINE__, rootid, tnodeid, id);
        }
        ltree_mutex_unlock(x, rootid);
    }
    return id;
}

/* get prev node key/data */
uint32_t ltree_prev(void *x, int rootid, uint32_t tnodeid, int64_t *key, int *data)
{
    uint32_t id = 0, parentid = 0;

    if(x && tnodeid > 0 && rootid > 0)
    {
        ltree_mutex_lock(x, rootid);
        if(LTR(x)->map && LTR(x)->state && tnodeid <  LTR(x)->state->total)
        {
            id = tnodeid;
            if(LTR(x)->map[id].left > 0)
            {
                id = LTR(x)->map[id].left;
                while(LTR(x)->map[id].right  > 0)
                {
                    id = LTR(x)->map[id].right;
                }
                //fprintf(stdout, "%s::%d id:%d\n", __FILE__, __LINE__, id);
            }
            else
            {
                while(id > 0)
                {
                    parentid = LTR(x)->map[id].parent;
                    if(LTR(x)->map[id].key > LTR(x)->map[parentid].key)
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
            if(id > 0 && id < LTR(x)->state->total)
            {
                if(key)*key = LTR(x)->map[id].key;
                if(data)*data = LTR(x)->map[id].data;
            }
            //fprintf(stdout, "%s::%d rootid:%d tnodeid:%d id:%d\n",__FILE__, __LINE__, rootid, tnodeid, id);
        }
        ltree_mutex_unlock(x, rootid);
    }
    return id;
}

/* view node */
void ltree_view_tnode(void *x, uint32_t tnodeid, FILE *fp)
{
    if(x)
    {
        if(LTR(x)->map[tnodeid].left > 0 && LTR(x)->map[tnodeid].left < LTR(x)->state->total)
        {
            ltree_view_tnode(x, LTR(x)->map[tnodeid].left, fp);
        }
        fprintf(fp, "[%d:%lld:%d]\n", tnodeid, (long long)LTR(x)->map[tnodeid].key, LTR(x)->map[tnodeid].data);
        if(LTR(x)->map[tnodeid].right > 0 && LTR(x)->map[tnodeid].right < LTR(x)->state->total)
        {
            ltree_view_tnode(x, LTR(x)->map[tnodeid].right, fp);
        }
    }
    return ;
}

void ltree_view_tree(void *x, int rootid, FILE *fp)
{
    if(x && rootid > 0)
    {
        ltree_mutex_lock(x, rootid);
        if(LTR(x)->map && LTR(x)->state && rootid < LTREE_ROOT_MAX)
        {
            fprintf(stdout, "%s::%d rootid:%d\n", __FILE__, __LINE__, LTR(x)->state->roots[rootid].rootid);
             ltree_view_tnode(x, LTR(x)->state->roots[rootid].rootid, fp);
        }
        ltree_mutex_unlock(x, rootid);
    }
    return ;
}

/* set data */
int ltree_set_data(void *x, uint32_t tnodeid, int data)
{
    int old = -1;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(LTR(x)->mutex);
        if(LTR(x)->map && LTR(x)->state && tnodeid < LTR(x)->state->total)
        {
            old = LTR(x)->map[tnodeid].data;
            LTR(x)->map[tnodeid].data = data;
        }
        MUTEX_UNLOCK(LTR(x)->mutex);
    }
    return old;
}

/* remove node */
void ltree_remove(void *x, int rootid, uint32_t tnodeid, int64_t *key, int *data)
{
    uint32_t id = 0, pid = 0, parent = 0, child = 0, rid = 0, lid = 0,
        uid = 0, ppid = 0, color = 0, *prootid = NULL;

    if(x && rootid > 0 && tnodeid > 0)
    {
        ltree_mutex_lock(x, rootid);
        if(LTR(x)->map && LTR(x)->state && tnodeid < LTR(x)->state->total)
        {
            if(key) *key = LTR(x)->map[tnodeid].key;
            if(data) *data = LTR(x)->map[tnodeid].data;
            id = tnodeid;
            if(LTR(x)->map[tnodeid].left == 0)
            {
                child = LTR(x)->map[tnodeid].right;
            }
            else if(LTR(x)->map[tnodeid].right == 0)
            {
                child = LTR(x)->map[tnodeid].left;
            }
            else 
            {
                id = LTR(x)->map[tnodeid].right;
                while(LTR(x)->map[id].left > 0)
                    id = LTR(x)->map[id].left;
                parent = LTR(x)->map[id].parent;
                color = LTR(x)->map[id].color;
                if((child = LTR(x)->map[id].right) > 0)
                    LTR(x)->map[child].parent = parent;
                if((pid = parent) > 0)
                {
                    if(LTR(x)->map[pid].left == id)
                        LTR(x)->map[pid].left = child;
                    else
                        LTR(x)->map[pid].right = child;
                }
                else
                {
                    LTR(x)->state->roots[rootid].rootid = child;
                }
                if(LTR(x)->map[id].parent == tnodeid) parent = id;
                LTR(x)->map[id].color = LTR(x)->map[tnodeid].color;
                LTR(x)->map[id].parent = LTR(x)->map[tnodeid].parent;
                LTR(x)->map[id].left = LTR(x)->map[tnodeid].left;
                LTR(x)->map[id].right = LTR(x)->map[tnodeid].right;
                if((pid = LTR(x)->map[tnodeid].parent) > 0)
                {
                    if(LTR(x)->map[pid].left == tnodeid)
                        LTR(x)->map[pid].left = id;
                    else
                        LTR(x)->map[pid].right = id;
                }
                else
                {
                    LTR(x)->state->roots[rootid].rootid = id;
                }
                lid = LTR(x)->map[tnodeid].left;
                LTR(x)->map[lid].parent = id;
                if((rid = LTR(x)->map[tnodeid].right) > 0)
                    LTR(x)->map[rid].parent = id;
                goto color_remove;
            }
            parent =  LTR(x)->map[tnodeid].parent;
            color = LTR(x)->map[tnodeid].color;
            if(child > 0) 
            {
                LTR(x)->map[child].parent = parent;
            }
            if((pid = parent) > 0)
            {
                if(tnodeid == LTR(x)->map[pid].left) 
                    LTR(x)->map[pid].left = child;
                else 
                    LTR(x)->map[pid].right = child;
            }
            else 
            {
                LTR(x)->state->roots[rootid].rootid = child;
            }
            //remove color set
color_remove:
            LTR(x)->state->roots[rootid].total--;
            if(color == LTR_COLOR_BLACK)
            {
                //fprintf(stdout, "%s::%d node:%d parent:%d left:%d right:%d key:%d data:%d\n", __FILE__, __LINE__, tnodeid, LTR(x)->map[tnodeid].parent, LTR(x)->map[tnodeid].left, LTR(x)->map[tnodeid].right, LTR(x)->map[tnodeid].key, LTR(x)->map[tnodeid].data);
                prootid = &(LTR(x)->state->roots[rootid].rootid);
                LTR_REMOVE_COLOR(x, prootid, child, parent, lid, rid, uid, ppid);
            }
            //add to qleft
            ltree_qleft(x, tnodeid);
            //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, LTR(x)->start, LTR(x)->state, LTR(x)->map, LTR(x)->state->current, LTR(x)->state->left, LTR(x)->state->total, LTR(x)->state->qleft, LTR(x)->state->qfirst, LTR(x)->state->qlast);
  
        }
        ltree_mutex_unlock(x, rootid);
    }
    return ;
}

/* remove node */
void ltree_remove_tnode(void *x, uint32_t tnodeid)
{
    if(x)
    {
        if(LTR(x)->map[tnodeid].left > 0 && LTR(x)->map[tnodeid].left < LTR(x)->state->total)
        {
            ltree_remove_tnode(x, LTR(x)->map[tnodeid].left);
        }
        if(LTR(x)->map[tnodeid].right > 0 && LTR(x)->map[tnodeid].right < LTR(x)->state->total)
        {
            ltree_remove_tnode(x, LTR(x)->map[tnodeid].right);
        }
        ltree_qleft(x, tnodeid);
    }
    return ;
}

/* remove tree */
void ltree_remove_tree(void *x, int rootid)
{
    if(x && rootid > 0 && rootid < LTREE_ROOT_MAX)
    {
        ltree_mutex_lock(x, rootid);
        ltree_remove_tnode(x, LTR(x)->state->roots[rootid].rootid);
        LTR(x)->state->roots[rootid].rootid = 0;
        LTR(x)->state->roots[rootid].status = 0;
        //fprintf(stdout, "%s::%d rootid:%d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, rootid, LTR(x)->start, LTR(x)->state, LTR(x)->map, LTR(x)->state->current, LTR(x)->state->left, LTR(x)->state->total, LTR(x)->state->qleft, LTR(x)->state->qfirst, LTR(x)->state->qlast);
 
        ltree_mutex_unlock(x, rootid);
    }
    return ;
}

//close ltree
void ltree_close(void *x)
{
    int i = 0;
    if(x)
    {
        //fprintf(stdout, "%s::%d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d sizeof(LTSTATE):%d\n", __FILE__, __LINE__, LTR(x)->start, LTR(x)->state, LTR(x)->map, LTR(x)->state->current, LTR(x)->state->left, LTR(x)->state->total, LTR(x)->state->qleft, LTR(x)->state->qfirst, LTR(x)->state->qlast, sizeof(LTSTATE));
        LTR_MUNMAP(x);
        MUTEX_DESTROY(LTR(x)->mutex);
#ifdef HAVE_PTHREAD
        for(i = 0; i < LTREE_MUTEX_MAX; i++)
        {
            pthread_mutex_destroy(&(LTR(x)->mutexs[i]));
        }
#endif
        if(LTR(x)->fd) close(LTR(x)->fd);
        free(x);
    }
    return ;
}


#ifdef _DEBUG_LTREE
#include "md5.h"
#include "timer.h"
int main(int argc, char **argv) 
{
    int i = 0, rootid = 0, id = 0, j = 0, old = 0, data = 0, n = 0, count = 50000000;
    unsigned char digest[MD5_LEN];
    void *ltree = NULL;
    void *timer = NULL;
    char line[1024];
    int64_t key = 0;

    if((ltree = ltree_init("/tmp/test.ltree")))
    {
        rootid = ltree_new_tree(ltree);
        TIMER_INIT(timer);
        for(j = 1; j <= count; j++)
        {
            n = sprintf(line, "http://www.demo.com/%d.html", j);
            md5(line, n, digest);
            key = *((int64_t *)digest);
            old = -1;
            data = j;
            id = ltree_insert(ltree, rootid, key, data, &old);
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
            key = *((int64_t *)digest);
            old = -1;
            data = j;
            id = ltree_try_insert(ltree, rootid, key, data, &old);
            if(old > 0 && old != j) 
            {
                fprintf(stdout, "%d:{id:%d key:%d rootid:%d old:%d}\n", j, id, key, rootid, old);
                _exit(-1);
            }
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "%s::%d try_insert:%d time:%lld\n", __FILE__,__LINE__, count, PT_LU_USEC(timer));
        TIMER_CLEAN(timer);
        ltree_close(ltree);
    }
}
//gcc -o mtree64 ltree.c md5.c -D_DEBUG_LTREE -g && ./mtree64
#endif
