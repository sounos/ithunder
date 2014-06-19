#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include "dtree.h"
#include "mutex.h"
#define MMAP_SHARED MAP_SHARED|MAP_LOCKED
#define DTR(px) ((DTREE *)px)
#define DTR_COLOR_BLACK  0
#define DTR_COLOR_RED    1
#define DTR_MIN_MAX(x, key, xid)                                                \
do                                                                              \
{                                                                               \
    if(DTR(x) && DTR(x)->state)                                                 \
    {                                                                           \
        if(DTR(x)->state->count == 0)                                           \
        {                                                                       \
            DTR(x)->state->nmin = DTR(x)->state->nmax = xid;                    \
            DTR(x)->state->kmin = DTR(x)->state->kmax = key;                    \
        }                                                                       \
        else if(key > DTR(x)->state->kmax)                                      \
        {                                                                       \
            DTR(x)->state->nmax = xid;                                          \
            DTR(x)->state->kmax = key;                                          \
        }                                                                       \
        else if(key < DTR(x)->state->kmin)                                      \
        {                                                                       \
            DTR(x)->state->nmin = xid;                                          \
            DTR(x)->state->kmin = key;                                          \
        }                                                                       \
    }                                                                           \
}while(0)
#define DTR_MUNMAP(x)                                                           \
do                                                                              \
{                                                                               \
    if(x && DTR(x)->size > 0)                                                   \
    {                                                                           \
        if(DTR(x)->start && DTR(x)->start != (void *)-1)                        \
        {                                                                       \
            munmap(DTR(x)->start, DTR(x)->size);                                \
            DTR(x)->start = NULL;                                               \
            DTR(x)->state = NULL;                                               \
            DTR(x)->map = NULL;                                                 \
        }                                                                       \
    }                                                                           \
}while(0)

#define DTR_MMAP(x)                                                             \
do                                                                              \
{                                                                               \
    if(x)                                                                       \
    {                                                                           \
        if((DTR(x)->start = (char *)mmap(NULL,DTR(x)->size,PROT_READ|PROT_WRITE,\
                    MMAP_SHARED, DTR(x)->fd, 0)) != (void *)-1)                  \
        {                                                                       \
            DTR(x)->state = (DTSTATE *)DTR(x)->start;                           \
            DTR(x)->map = (DTNODE *)(DTR(x)->start + sizeof(DTSTATE));          \
        }                                                                       \
    }                                                                           \
}while(0)

#define DTR_INCRE(x)                                                            \
do                                                                              \
{                                                                               \
    if(x &&  DTR(x)->end <  DTR(x)->size)                                       \
    {                                                                           \
        DTR(x)->old = DTR(x)->end;                                              \
        DTR(x)->end += (off_t)DTREE_INCRE_NUM * (off_t)sizeof(DTNODE);       \
        if(ftruncate(DTR(x)->fd, DTR(x)->end) == 0)                             \
        {                                                                       \
            if(DTR(x)->old == sizeof(DTSTATE))                                  \
            {                                                                   \
                memset(DTR(x)->state, 0, sizeof(DTSTATE));                      \
                DTR(x)->state->left += DTREE_INCRE_NUM - 1;                  \
            }                                                                   \
            else                                                                \
            {                                                                   \
                DTR(x)->state->left += DTREE_INCRE_NUM;                      \
            }                                                                   \
            DTR(x)->state->total += DTREE_INCRE_NUM;                         \
            memset(DTR(x)->start + DTR(x)->old, 0, DTR(x)->end - DTR(x)->old);  \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            _exit(-1);                                                          \
        }                                                                       \
    }                                                                           \
}while(0)
#define DTR_ROTATE_LEFT(x, prootid, oid, rid, lid, ppid)                        \
do                                                                              \
{                                                                               \
    if(x && (rid = DTR(x)->map[oid].right) > 0)                                 \
    {                                                                           \
        if((lid = DTR(x)->map[oid].right = DTR(x)->map[rid].left) > 0)          \
        {                                                                       \
            DTR(x)->map[lid].parent = oid;                                      \
        }                                                                       \
        if((ppid = DTR(x)->map[rid].parent = DTR(x)->map[oid].parent) > 0)      \
        {                                                                       \
            if(DTR(x)->map[ppid].left == oid)                                   \
                DTR(x)->map[ppid].left = rid;                                   \
            else                                                                \
                DTR(x)->map[ppid].right = rid;                                  \
        }else *prootid = rid;                                                   \
        DTR(x)->map[rid].left = oid;                                            \
        DTR(x)->map[oid].parent = rid;                                          \
    }                                                                           \
}while(0)

#define DTR_ROTATE_RIGHT(x, prootid, oid, lid, rid, ppid)                       \
do                                                                              \
{                                                                               \
    if(x && (lid = DTR(x)->map[oid].left) > 0)                                  \
    {                                                                           \
        if((rid = DTR(x)->map[oid].left = DTR(x)->map[lid].right) > 0)          \
        {                                                                       \
            DTR(x)->map[rid].parent = oid;                                      \
        }                                                                       \
        if((ppid = DTR(x)->map[lid].parent = DTR(x)->map[oid].parent) > 0)      \
        {                                                                       \
            if(DTR(x)->map[ppid].left == oid)                                   \
                DTR(x)->map[ppid].left = lid;                                   \
            else                                                                \
                DTR(x)->map[ppid].right = lid;                                  \
        }                                                                       \
        else *prootid = lid;                                                    \
        DTR(x)->map[lid].right = oid;                                           \
        DTR(x)->map[oid].parent = lid;                                          \
    }                                                                           \
}while(0)

#define DTR_INSERT_COLOR(x, prootid, oid, lid, rid, uid, pid, gpid, ppid)       \
do                                                                              \
{                                                                               \
    while((pid = DTR(x)->map[oid].parent)> 0                                    \
            && DTR(x)->map[pid].color == DTR_COLOR_RED)                         \
    {                                                                           \
        gpid = DTR(x)->map[pid].parent;                                         \
        if(pid == DTR(x)->map[gpid].left)                                       \
        {                                                                       \
            uid = DTR(x)->map[gpid].right;                                      \
            if(uid > 0 && DTR(x)->map[uid].color == DTR_COLOR_RED)              \
            {                                                                   \
                DTR(x)->map[uid].color = DTR_COLOR_BLACK;                       \
                DTR(x)->map[pid].color = DTR_COLOR_BLACK;                       \
                DTR(x)->map[gpid].color = DTR_COLOR_RED;                        \
                oid = gpid;                                                     \
                continue;                                                       \
            }                                                                   \
            if(DTR(x)->map[pid].right == oid)                                   \
            {                                                                   \
                DTR_ROTATE_LEFT(x, prootid, pid, rid, lid, ppid);               \
                uid = pid; pid = oid; oid = uid;                                \
            }                                                                   \
            DTR(x)->map[pid].color = DTR_COLOR_BLACK;                           \
            DTR(x)->map[gpid].color = DTR_COLOR_RED;                            \
            DTR_ROTATE_RIGHT(x, prootid, gpid, lid, rid, ppid);                 \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            uid = DTR(x)->map[gpid].left;                                       \
            if(uid > 0 && DTR(x)->map[uid].color == DTR_COLOR_RED)              \
            {                                                                   \
                DTR(x)->map[uid].color = DTR_COLOR_BLACK;                       \
                DTR(x)->map[pid].color = DTR_COLOR_BLACK;                       \
                DTR(x)->map[gpid].color = DTR_COLOR_RED;                        \
                oid = gpid;                                                     \
                continue;                                                       \
            }                                                                   \
            if(DTR(x)->map[pid].left == oid)                                    \
            {                                                                   \
                DTR_ROTATE_RIGHT(x, prootid, pid, lid, rid, ppid);              \
                uid = pid; pid = oid; oid = uid;                                \
            }                                                                   \
            DTR(x)->map[pid].color = DTR_COLOR_BLACK;                           \
            DTR(x)->map[gpid].color = DTR_COLOR_RED;                            \
            DTR_ROTATE_LEFT(x, prootid, gpid, rid, lid, ppid);                  \
        }                                                                       \
    }                                                                           \
    if(*prootid > 0)DTR(x)->map[*prootid].color = DTR_COLOR_BLACK;              \
}while(0)

#define DTR_REMOVE_COLOR(x, prootid, oid, xpid, lid, rid, uid, ppid)            \
do                                                                              \
{                                                                               \
    while((oid == 0 || DTR(x)->map[oid].color == DTR_COLOR_BLACK)               \
            && oid != *prootid)                                                 \
    {                                                                           \
        if(DTR(x)->map[xpid].left == oid)                                       \
        {                                                                       \
            uid = DTR(x)->map[xpid].right;                                      \
            if(DTR(x)->map[uid].color == DTR_COLOR_RED)                         \
            {                                                                   \
                DTR(x)->map[uid].color = DTR_COLOR_BLACK;                       \
                DTR(x)->map[xpid].color = DTR_COLOR_RED;                        \
                DTR_ROTATE_LEFT(x, prootid, xpid, rid, lid, ppid);              \
                uid = DTR(x)->map[xpid].right;                                  \
            }                                                                   \
            lid = DTR(x)->map[uid].left;                                        \
            rid = DTR(x)->map[uid].right;                                       \
            if((lid == 0 || DTR(x)->map[lid].color == DTR_COLOR_BLACK)          \
                && (rid == 0 || DTR(x)->map[rid].color == DTR_COLOR_BLACK))     \
            {                                                                   \
                DTR(x)->map[uid].color = DTR_COLOR_RED;                         \
                oid = xpid;                                                      \
                xpid = DTR(x)->map[oid].parent;                                  \
            }                                                                   \
            else                                                                \
            {                                                                   \
                rid = DTR(x)->map[uid].right;                                   \
                lid = DTR(x)->map[uid].left;                                    \
                if(rid == 0 || DTR(x)->map[rid].color == DTR_COLOR_BLACK)       \
                {                                                               \
                    if(lid > 0)DTR(x)->map[lid].color = DTR_COLOR_BLACK;        \
                    DTR(x)->map[uid].color = DTR_COLOR_RED;                     \
                    DTR_ROTATE_RIGHT(x, prootid, uid, lid, rid, ppid);          \
                    uid = DTR(x)->map[xpid].right;                              \
                }                                                               \
                DTR(x)->map[uid].color = DTR(x)->map[xpid].color;               \
                DTR(x)->map[xpid].color = DTR_COLOR_BLACK;                      \
                if((rid = DTR(x)->map[uid].right) > 0)                          \
                    DTR(x)->map[rid].color = DTR_COLOR_BLACK;                   \
                DTR_ROTATE_LEFT(x, prootid, xpid, rid, lid, ppid);              \
                oid = *prootid;                                                 \
                break;                                                          \
            }                                                                   \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            uid = DTR(x)->map[xpid].left;                                       \
            if(DTR(x)->map[uid].color == DTR_COLOR_RED)                         \
            {                                                                   \
                DTR(x)->map[uid].color = DTR_COLOR_BLACK;                       \
                DTR(x)->map[xpid].color = DTR_COLOR_RED;                        \
                DTR_ROTATE_RIGHT(x, prootid, xpid, lid, rid, ppid);             \
                uid = DTR(x)->map[xpid].left;                                   \
            }                                                                   \
            lid = DTR(x)->map[uid].left;                                        \
            rid = DTR(x)->map[uid].right;                                       \
            if((lid == 0 || DTR(x)->map[lid].color == DTR_COLOR_BLACK)          \
                && (rid == 0 || DTR(x)->map[rid].color == DTR_COLOR_BLACK))     \
            {                                                                   \
                DTR(x)->map[uid].color = DTR_COLOR_RED;                         \
                oid = xpid;                                                     \
                xpid = DTR(x)->map[oid].parent;                                 \
            }                                                                   \
            else                                                                \
            {                                                                   \
                rid = DTR(x)->map[uid].right;                                   \
                lid = DTR(x)->map[uid].left;                                    \
                if(lid == 0 || DTR(x)->map[lid].color == DTR_COLOR_BLACK)       \
                {                                                               \
                    if(rid > 0)DTR(x)->map[rid].color = DTR_COLOR_BLACK;        \
                    DTR(x)->map[uid].color = DTR_COLOR_RED;                     \
                    DTR_ROTATE_LEFT(x, prootid, uid, rid, lid, ppid);           \
                    uid = DTR(x)->map[xpid].left;                               \
                }                                                               \
                DTR(x)->map[uid].color = DTR(x)->map[xpid].color;               \
                DTR(x)->map[xpid].color = DTR_COLOR_BLACK;                      \
                if((lid = DTR(x)->map[uid].left) > 0)                           \
                    DTR(x)->map[lid].color = DTR_COLOR_BLACK;                   \
                DTR_ROTATE_RIGHT(x, prootid, xpid, lid, rid, ppid);             \
                oid = *prootid;                                                 \
                break;                                                          \
            }                                                                   \
        }                                                                       \
    }                                                                           \
    if(oid > 0) DTR(x)->map[oid].color = DTR_COLOR_BLACK;                       \
}while(0)
void dtree_mutex_lock(void *x, int id)
{
    if(x)
    {
#ifdef HAVE_PTHREAD
        pthread_mutex_lock(&(DTR(x)->mutexs[id%DTREE_MUTEX_MAX]));
#endif
    }
    return ;
}
void dtree_mutex_unlock(void *x, int id)
{
    if(x)
    {
#ifdef HAVE_PTHREAD
        pthread_mutex_unlock(&(DTR(x)->mutexs[id%DTREE_MUTEX_MAX]));
#endif
    }
    return ;
}

/* init dtree */
void *dtree_init(char *file)
{
    int i = 0;
    void *x = NULL;
    struct stat  st = {0};

    if((x = (DTREE *)calloc(1, sizeof(DTREE))))
    {
        if((DTR(x)->fd = open(file, O_CREAT|O_RDWR, 0644)) > 0 
                && fstat(DTR(x)->fd, &st) == 0)
        {
            MUTEX_INIT(DTR(x)->mutex);
            DTR(x)->end = st.st_size;
            DTR(x)->size = (off_t)sizeof(DTSTATE) + (off_t)sizeof(DTNODE) * (off_t)DTREE_NODES_MAX;
            //mmap
            DTR_MMAP(x);
            //init truncate
            if(st.st_size == 0)
            {
                DTR(x)->end = (off_t)sizeof(DTSTATE);
                DTR_INCRE(x);
            }
            /* initialize mutexs  */
#ifdef HAVE_PTHREAD
            for(i = 0; i < DTREE_MUTEX_MAX; i++)
            {
                pthread_mutex_init(&(DTR(x)->mutexs[i]), NULL);
            }
#endif
        }
        else 
        {
            if(DTR(x)->fd > 0) close(DTR(x)->fd);
            free(x);
            x = NULL;
        }
    }
    return x;
}

/* insert new root */
int dtree_new_tree(void *x)
{
    int id = 0, i = 0;
    if(x)
    {
        MUTEX_LOCK(DTR(x)->mutex);
        if(DTR(x)->state->nroots == 0) DTR(x)->state->nroots = 1;
        if(DTR(x)->state && DTR(x)->state->nroots < DTREE_ROOT_MAX)
        {
            for(i = 1; i < DTREE_ROOT_MAX; i++)
            {
                if(DTR(x)->state->roots[i].status == 0)
                {
                    DTR(x)->state->roots[i].status = 1;
                    DTR(x)->state->nroots++;
                    id = i;
                    break;
                }
            }
        }
        MUTEX_UNLOCK(DTR(x)->mutex);
    }
    return id;
}

/* total */
uint32_t dtree_total(void *x, int rootid)
{
    uint32_t total = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(DTR(x)->mutex);
        if(DTR(x)->state && DTR(x)->map && rootid < DTREE_ROOT_MAX)
        {
            total =  DTR(x)->state->roots[rootid].total;
        }
        MUTEX_UNLOCK(DTR(x)->mutex);
    }
    return total;
}

//add nodeid to qleft
void dtree_qleft(void *x, int tnodeid)
{
    int z = 0;
    if(x)
    {
        MUTEX_LOCK(DTR(x)->mutex);
        memset(&(DTR(x)->map[tnodeid]), 0, sizeof(DTNODE));
        if(DTR(x)->state->qleft == 0)
        {
            DTR(x)->state->qfirst = DTR(x)->state->qlast = tnodeid;
        }
        else
        {
            z = DTR(x)->state->qlast;
            DTR(x)->map[z].parent = tnodeid;
            DTR(x)->state->qlast = tnodeid;
        }
        DTR(x)->state->qleft++;
        DTR(x)->state->left++;
        MUTEX_UNLOCK(DTR(x)->mutex);
    }
    return ;
}
//new node
uint32_t dtree_new_node(void *x, int rootid, int nodeid, double key, int data)
{
    uint32_t id = 0;

    if(x)
    {
        MUTEX_LOCK(DTR(x)->mutex);
        //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, DTR(x)->start, DTR(x)->state, DTR(x)->map, DTR(x)->state->current, DTR(x)->state->left, DTR(x)->state->total, DTR(x)->state->qleft, DTR(x)->state->qfirst, DTR(x)->state->qlast);
        if(DTR(x)->state->left == 0)
        {
            DTR_INCRE(x);
        }
        if(DTR(x)->state->qleft > 0)
        {
            id = DTR(x)->state->qfirst;
            DTR(x)->state->qfirst = DTR(x)->map[id].parent;
            DTR(x)->state->qleft--;
        }
        else
        {
            id = ++(DTR(x)->state->current);
        }
        //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, DTR(x)->start, DTR(x)->state, DTR(x)->map, DTR(x)->state->current, DTR(x)->state->left, DTR(x)->state->total, DTR(x)->state->qleft, DTR(x)->state->qfirst, DTR(x)->state->qlast);
        DTR(x)->state->left--;
        //memset(&(DTR(x)->map[id]), 0, sizeof(DTNODE));
        DTR(x)->map[id].parent = nodeid;
        DTR(x)->map[id].key = key;
        DTR(x)->map[id].data = data;
        DTR_MIN_MAX(x, id, key);
        if(nodeid > 0)
        {
            if(key > DTR(x)->map[nodeid].key) 
                DTR(x)->map[nodeid].right = id;
            else
                DTR(x)->map[nodeid].left = id;
        }
        DTR(x)->state->roots[rootid].total++;
        MUTEX_UNLOCK(DTR(x)->mutex);
    }
    return id;
}

/* insert new node */
uint32_t dtree_insert(void *x, int rootid, double key, int data, int *old)
{
    uint32_t id = 0, nodeid = 0, rid = 0, lid = 0, uid = 0, pid = 0, 
        gpid = 0, ppid = 0, *prootid = NULL;
    DTNODE *node = NULL;

    if(x && rootid > 0)
    {
        dtree_mutex_lock(x, rootid);
        if(DTR(x)->state && DTR(x)->map && rootid < DTREE_ROOT_MAX
                && DTR(x)->state->roots[rootid].status > 0)
        {
            nodeid = DTR(x)->state->roots[rootid].rootid;
            while(nodeid > 0 && nodeid < DTR(x)->state->total)
            {
                node = &(DTR(x)->map[nodeid]);
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
            if(id == 0) id = dtree_new_node(x, rootid, nodeid, key, data);
        }
        if((nodeid = id) > 0)
        {
            if(DTR(x)->state->roots[rootid].rootid > 0)
            {
                prootid = &(DTR(x)->state->roots[rootid].rootid);
                DTR(x)->map[nodeid].color = DTR_COLOR_RED;
                DTR_INSERT_COLOR(x, prootid, nodeid, lid, rid, uid, pid, gpid, ppid);
            }
            else
            {
                DTR(x)->state->roots[rootid].rootid = nodeid;
            }
        }
end:
        dtree_mutex_unlock(x, rootid);
    }
    return id;
}


/* try insert  node */
uint32_t dtree_try_insert(void *x, int rootid, double key, int data, int *old)
{
    uint32_t id = 0, nodeid = 0, rid = 0, lid = 0, uid = 0, pid = 0, 
        gpid = 0, ppid = 0, *prootid = NULL;
    DTNODE *node = NULL;

    if(x && rootid > 0)
    {
        dtree_mutex_lock(x, rootid);
        if(DTR(x)->state && DTR(x)->map && rootid < DTREE_ROOT_MAX
                && DTR(x)->state->roots[rootid].status > 0)
        {
            nodeid = DTR(x)->state->roots[rootid].rootid;
            while(nodeid > 0 && nodeid < DTR(x)->state->total)
            {
                node = &(DTR(x)->map[nodeid]);
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
        if(id == 0) id = dtree_new_node(x, rootid, nodeid, key, data);
        if((nodeid = id) > 0)
        {
            if(DTR(x)->state->roots[rootid].rootid > 0)
            {
                prootid = &(DTR(x)->state->roots[rootid].rootid);
                DTR(x)->map[nodeid].color = DTR_COLOR_RED;
                DTR_INSERT_COLOR(x, prootid, nodeid, lid, rid, uid, pid, gpid, ppid);
            }
            else
            {
                DTR(x)->state->roots[rootid].rootid = nodeid;
            }
        }
end:
        dtree_mutex_unlock(x, rootid);
    }
    return id;
}


/* get node key/data */
uint32_t dtree_get(void *x, uint32_t tnodeid, double *key, int *data)
{
    uint32_t id = 0;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(DTR(x)->mutex);
        if(DTR(x)->map && DTR(x)->state && tnodeid <  DTR(x)->state->total)
        {
            if(key) *key = DTR(x)->map[tnodeid].key;
            if(data) *data = DTR(x)->map[tnodeid].data;
            id = tnodeid;
        }
        MUTEX_UNLOCK(DTR(x)->mutex);
    }
    return id;
}

/* find key/data */
uint32_t dtree_find(void *x, int rootid, double key, int *data)
{
    uint32_t id = 0;

    if(x && rootid > 0)
    {
        dtree_mutex_lock(x, rootid);
        if(DTR(x)->map && DTR(x)->state && rootid < DTREE_ROOT_MAX
                && DTR(x)->state->roots[rootid].status > 0)
        {
            //fprintf(stdout, "%s::%d rootid:%d key:%d data:%d total:%d\n", __FILE__, __LINE__, rootid, key, *data, DTR(x)->state->total);
            id = DTR(x)->state->roots[rootid].rootid;
            while(id > 0 && id < DTR(x)->state->total)
            {
                if(key == DTR(x)->map[id].key)
                {
                    if(data) *data = DTR(x)->map[id].data;
                    break;
                }
                else if(key > DTR(x)->map[id].key)
                {
                    id = DTR(x)->map[id].right;
                }
                else
                {
                    id = DTR(x)->map[id].left;
                }
            }
        }
        //fprintf(stdout, "%s::%d rootid:%d key:%d data:%d\n", __FILE__, __LINE__, rootid, key, *data);
        dtree_mutex_unlock(x, rootid);
    }
    return id;
}

/* get tree->min key/data */
uint32_t dtree_min(void *x, int rootid, double *key, int *data)
{
    uint32_t id = 0;

    if(x && rootid > 0)
    {
        dtree_mutex_lock(x, rootid);
        if(DTR(x)->map && DTR(x)->state && rootid <  DTREE_ROOT_MAX
                && DTR(x)->state->roots[rootid].status > 0)
        {
            id = DTR(x)->state->roots[rootid].rootid;
            while(DTR(x)->map[id].left > 0)
            {
                id = DTR(x)->map[id].left;
            }
            if(id > 0 && DTR(x)->state->total)
            {
                if(key) *key = DTR(x)->map[id].key;
                if(data) *data = DTR(x)->map[id].data;
            }
        }
        dtree_mutex_unlock(x, rootid);
    }
    return id;
}

/* get tree->max key/data */
unsigned  int dtree_max(void *x, int rootid, double *key, int *data)
{
    uint32_t id = 0, tmp = 0;

    if(x && rootid > 0)
    {
        dtree_mutex_lock(x, rootid);
        if(DTR(x)->map && DTR(x)->state && rootid <  DTREE_ROOT_MAX
                && DTR(x)->state->roots[rootid].status > 0)
        {
            tmp = DTR(x)->state->roots[rootid].rootid;
            do
            {
                id = tmp;
            }while(id > 0 && (tmp = DTR(x)->map[id].right) > 0);
            if(id > 0 && DTR(x)->state->total)
            {
                if(key) *key = DTR(x)->map[id].key;
                if(data) *data = DTR(x)->map[id].data;
            }
        }
        dtree_mutex_unlock(x, rootid);
    }
    return id;
}

/* get next node key/data */
uint32_t dtree_next(void *x, int rootid, uint32_t tnodeid, double *key, int *data)
{
    uint32_t id = 0, parentid = 0;

    if(x && tnodeid > 0 && rootid > 0)
    {
        dtree_mutex_lock(x, rootid);
        if(DTR(x)->map && DTR(x)->state && tnodeid <  DTR(x)->state->total)
        {
            id = tnodeid;
            if(DTR(x)->map[id].right > 0)
            {
                id = DTR(x)->map[id].right;
                while(DTR(x)->map[id].left  > 0)
                {
                    id = DTR(x)->map[id].left;
                }
                //fprintf(stdout, "%s::%d id:%d\n", __FILE__, __LINE__, id);
            }
            else
            {
                while(id > 0)
                {
                    parentid = DTR(x)->map[id].parent;
                    if(DTR(x)->map[id].key < DTR(x)->map[parentid].key)
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
            if(id > 0 && id < DTR(x)->state->total)
            {
                if(key) *key = DTR(x)->map[id].key;
                if(data) *data = DTR(x)->map[id].data;
            }
            //fprintf(stdout, "%s::%d rootid:%d tnodeid:%d id:%d\n",__FILE__, __LINE__, rootid, tnodeid, id);
        }
        dtree_mutex_unlock(x, rootid);
    }
    return id;
}

/* get prev node key/data */
uint32_t dtree_prev(void *x, int rootid, uint32_t tnodeid, double *key, int *data)
{
    uint32_t id = 0, parentid = 0;

    if(x && tnodeid > 0 && rootid > 0)
    {
        dtree_mutex_lock(x, rootid);
        if(DTR(x)->map && DTR(x)->state && tnodeid <  DTR(x)->state->total)
        {
            id = tnodeid;
            if(DTR(x)->map[id].left > 0)
            {
                id = DTR(x)->map[id].left;
                while(DTR(x)->map[id].right  > 0)
                {
                    id = DTR(x)->map[id].right;
                }
                //fprintf(stdout, "%s::%d id:%d\n", __FILE__, __LINE__, id);
            }
            else
            {
                while(id > 0)
                {
                    parentid = DTR(x)->map[id].parent;
                    if(DTR(x)->map[id].key > DTR(x)->map[parentid].key)
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
            if(id > 0 && id < DTR(x)->state->total)
            {
                if(key)*key = DTR(x)->map[id].key;
                if(data)*data = DTR(x)->map[id].data;
            }
            //fprintf(stdout, "%s::%d rootid:%d tnodeid:%d id:%d\n",__FILE__, __LINE__, rootid, tnodeid, id);
        }
        dtree_mutex_unlock(x, rootid);
    }
    return id;
}

/* view node */
void dtree_view_tnode(void *x, uint32_t tnodeid, FILE *fp)
{
    if(x)
    {
        if(DTR(x)->map[tnodeid].left > 0 && DTR(x)->map[tnodeid].left < DTR(x)->state->total)
        {
            dtree_view_tnode(x, DTR(x)->map[tnodeid].left, fp);
        }
        fprintf(fp, "[%d:%lld:%d]\n", tnodeid, (long long)DTR(x)->map[tnodeid].key, DTR(x)->map[tnodeid].data);
        if(DTR(x)->map[tnodeid].right > 0 && DTR(x)->map[tnodeid].right < DTR(x)->state->total)
        {
            dtree_view_tnode(x, DTR(x)->map[tnodeid].right, fp);
        }
    }
    return ;
}

void dtree_view_tree(void *x, int rootid, FILE *fp)
{
    if(x && rootid > 0)
    {
        dtree_mutex_lock(x, rootid);
        if(DTR(x)->map && DTR(x)->state && rootid < DTREE_ROOT_MAX)
        {
            fprintf(stdout, "%s::%d rootid:%d\n", __FILE__, __LINE__, DTR(x)->state->roots[rootid].rootid);
             dtree_view_tnode(x, DTR(x)->state->roots[rootid].rootid, fp);
        }
        dtree_mutex_unlock(x, rootid);
    }
    return ;
}

/* set data */
int dtree_set_data(void *x, uint32_t tnodeid, int data)
{
    int old = -1;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(DTR(x)->mutex);
        if(DTR(x)->map && DTR(x)->state && tnodeid < DTR(x)->state->total)
        {
            old = DTR(x)->map[tnodeid].data;
            DTR(x)->map[tnodeid].data = data;
        }
        MUTEX_UNLOCK(DTR(x)->mutex);
    }
    return old;
}

/* remove node */
void dtree_remove(void *x, int rootid, uint32_t tnodeid, double *key, int *data)
{
    uint32_t id = 0, pid = 0, parent = 0, child = 0, rid = 0, lid = 0,
        uid = 0, ppid = 0, color = 0, *prootid = NULL;

    if(x && rootid > 0 && tnodeid > 0)
    {
        dtree_mutex_lock(x, rootid);
        if(DTR(x)->map && DTR(x)->state && tnodeid < DTR(x)->state->total)
        {
            if(key) *key = DTR(x)->map[tnodeid].key;
            if(data) *data = DTR(x)->map[tnodeid].data;
            id = tnodeid;
            if(DTR(x)->map[tnodeid].left == 0)
            {
                child = DTR(x)->map[tnodeid].right;
            }
            else if(DTR(x)->map[tnodeid].right == 0)
            {
                child = DTR(x)->map[tnodeid].left;
            }
            else 
            {
                id = DTR(x)->map[tnodeid].right;
                while(DTR(x)->map[id].left > 0)
                    id = DTR(x)->map[id].left;
                parent = DTR(x)->map[id].parent;
                color = DTR(x)->map[id].color;
                if((child = DTR(x)->map[id].right) > 0)
                    DTR(x)->map[child].parent = parent;
                if((pid = parent) > 0)
                {
                    if(DTR(x)->map[pid].left == id)
                        DTR(x)->map[pid].left = child;
                    else
                        DTR(x)->map[pid].right = child;
                }
                else
                {
                    DTR(x)->state->roots[rootid].rootid = child;
                }
                if(DTR(x)->map[id].parent == tnodeid) parent = id;
                DTR(x)->map[id].color = DTR(x)->map[tnodeid].color;
                DTR(x)->map[id].parent = DTR(x)->map[tnodeid].parent;
                DTR(x)->map[id].left = DTR(x)->map[tnodeid].left;
                DTR(x)->map[id].right = DTR(x)->map[tnodeid].right;
                if((pid = DTR(x)->map[tnodeid].parent) > 0)
                {
                    if(DTR(x)->map[pid].left == tnodeid)
                        DTR(x)->map[pid].left = id;
                    else
                        DTR(x)->map[pid].right = id;
                }
                else
                {
                    DTR(x)->state->roots[rootid].rootid = id;
                }
                lid = DTR(x)->map[tnodeid].left;
                DTR(x)->map[lid].parent = id;
                if((rid = DTR(x)->map[tnodeid].right) > 0)
                    DTR(x)->map[rid].parent = id;
                goto color_remove;
            }
            parent =  DTR(x)->map[tnodeid].parent;
            color = DTR(x)->map[tnodeid].color;
            if(child > 0) 
            {
                DTR(x)->map[child].parent = parent;
            }
            if((pid = parent) > 0)
            {
                if(tnodeid == DTR(x)->map[pid].left) 
                    DTR(x)->map[pid].left = child;
                else 
                    DTR(x)->map[pid].right = child;
            }
            else 
            {
                DTR(x)->state->roots[rootid].rootid = child;
            }
            //remove color set
color_remove:
            DTR(x)->state->roots[rootid].total--;
            if(color == DTR_COLOR_BLACK)
            {
                //fprintf(stdout, "%s::%d node:%d parent:%d left:%d right:%d key:%d data:%d\n", __FILE__, __LINE__, tnodeid, DTR(x)->map[tnodeid].parent, DTR(x)->map[tnodeid].left, DTR(x)->map[tnodeid].right, DTR(x)->map[tnodeid].key, DTR(x)->map[tnodeid].data);
                prootid = &(DTR(x)->state->roots[rootid].rootid);
                DTR_REMOVE_COLOR(x, prootid, child, parent, lid, rid, uid, ppid);
            }
            //add to qleft
            dtree_qleft(x, tnodeid);
            //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, DTR(x)->start, DTR(x)->state, DTR(x)->map, DTR(x)->state->current, DTR(x)->state->left, DTR(x)->state->total, DTR(x)->state->qleft, DTR(x)->state->qfirst, DTR(x)->state->qlast);
  
        }
        dtree_mutex_unlock(x, rootid);
    }
    return ;
}

/* remove node */
void dtree_remove_tnode(void *x, uint32_t tnodeid)
{
    if(x)
    {
        if(DTR(x)->map[tnodeid].left > 0 && DTR(x)->map[tnodeid].left < DTR(x)->state->total)
        {
            dtree_remove_tnode(x, DTR(x)->map[tnodeid].left);
        }
        if(DTR(x)->map[tnodeid].right > 0 && DTR(x)->map[tnodeid].right < DTR(x)->state->total)
        {
            dtree_remove_tnode(x, DTR(x)->map[tnodeid].right);
        }
        dtree_qleft(x, tnodeid);
    }
    return ;
}

/* remove tree */
void dtree_remove_tree(void *x, int rootid)
{
    if(x && rootid > 0 && rootid < DTREE_ROOT_MAX)
    {
        dtree_mutex_lock(x, rootid);
        dtree_remove_tnode(x, DTR(x)->state->roots[rootid].rootid);
        DTR(x)->state->roots[rootid].rootid = 0;
        DTR(x)->state->roots[rootid].status = 0;
        //fprintf(stdout, "%s::%d rootid:%d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, rootid, DTR(x)->start, DTR(x)->state, DTR(x)->map, DTR(x)->state->current, DTR(x)->state->left, DTR(x)->state->total, DTR(x)->state->qleft, DTR(x)->state->qfirst, DTR(x)->state->qlast);
 
        dtree_mutex_unlock(x, rootid);
    }
    return ;
}

//close dtree
void dtree_close(void *x)
{
    int i = 0;
    if(x)
    {
        //fprintf(stdout, "%s::%d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d sizeof(DTSTATE):%d\n", __FILE__, __LINE__, DTR(x)->start, DTR(x)->state, DTR(x)->map, DTR(x)->state->current, DTR(x)->state->left, DTR(x)->state->total, DTR(x)->state->qleft, DTR(x)->state->qfirst, DTR(x)->state->qlast, sizeof(DTSTATE));
        DTR_MUNMAP(x);
        MUTEX_DESTROY(DTR(x)->mutex);
#ifdef HAVE_PTHREAD
        for(i = 0; i < DTREE_MUTEX_MAX; i++)
        {
            pthread_mutex_destroy(&(DTR(x)->mutexs[i]));
        }
#endif
        if(DTR(x)->fd) close(DTR(x)->fd);
        free(x);
    }
    return ;
}


#ifdef _DEBUG_DTREE
#include "md5.h"
#include "timer.h"
int main(int argc, char **argv) 
{
    int i = 0, rootid = 0, id = 0, j = 0, old = 0, data = 0, n = 0, count = 50000000;
    unsigned char digest[MD5_LEN];
    void *dtree = NULL;
    void *timer = NULL;
    char line[1024];
    double key = 0;

    if((dtree = dtree_init("/tmp/test.dtree")))
    {
        rootid = dtree_new_tree(dtree);
        TIMER_INIT(timer);
        for(j = 1; j <= count; j++)
        {
            n = sprintf(line, "http://www.demo.com/%d.html", j);
            md5(line, n, digest);
            key = *((double *)digest);
            old = -1;
            data = j;
            id = dtree_insert(dtree, rootid, key, data, &old);
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
            key = *((double *)digest);
            old = -1;
            data = j;
            id = dtree_try_insert(dtree, rootid, key, data, &old);
            if(old > 0 && old != j) 
            {
                fprintf(stdout, "%d:{id:%d key:%d rootid:%d old:%d}\n", j, id, key, rootid, old);
                _exit(-1);
            }
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "%s::%d try_insert:%d time:%lld\n", __FILE__,__LINE__, count, PT_LU_USEC(timer));
        TIMER_CLEAN(timer);
        dtree_close(dtree);
    }
}
//gcc -o mtree64 dtree.c md5.c -D_DEBUG_DTREE -g && ./mtree64
#endif
