#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include "mmtree.h"
#include "mutex.h"
#ifdef MAP_LOCKED
#define MMAP_SHARED MAP_SHARED|MAP_LOCKED
#else
#define MMAP_SHARED MAP_SHARED
#endif
#define MMT(px) ((MMTREE *)px)
#define MMT_COLOR_BLACK  0
#define MMT_COLOR_RED    1
#define MMT_MIN_MAX(x, key, xid)                                                \
do                                                                              \
{                                                                               \
    if(MMT(x) && MMT(x)->state)                                                 \
    {                                                                           \
        if(MMT(x)->state->count == 0)                                           \
        {                                                                       \
            MMT(x)->state->nmin = MMT(x)->state->nmax = xid;                    \
            MMT(x)->state->kmin = MMT(x)->state->kmax = key;                    \
        }                                                                       \
        else if(key > MMT(x)->state->kmax)                                      \
        {                                                                       \
            MMT(x)->state->nmax = xid;                                          \
            MMT(x)->state->kmax = key;                                          \
        }                                                                       \
        else if(key < MMT(x)->state->kmin)                                      \
        {                                                                       \
            MMT(x)->state->nmin = xid;                                          \
            MMT(x)->state->kmin = key;                                          \
        }                                                                       \
    }                                                                           \
}while(0)
#define MMT_MUNMAP(x)                                                           \
do                                                                              \
{                                                                               \
    if(x && MMT(x)->size > 0)                                                   \
    {                                                                           \
        if(MMT(x)->start && MMT(x)->start != (void *)-1)                        \
        {                                                                       \
            munmap(MMT(x)->start, MMT(x)->size);                                \
            MMT(x)->start = NULL;                                               \
            MMT(x)->state = NULL;                                               \
            MMT(x)->map = NULL;                                                 \
        }                                                                       \
    }                                                                           \
}while(0)

#define MMT_MMAP(x)                                                             \
do                                                                              \
{                                                                               \
    if(x)                                                                       \
    {                                                                           \
        if((MMT(x)->start = (char*)mmap(NULL,MMT(x)->size,PROT_READ|PROT_WRITE, \
                    MAP_SHARED, MMT(x)->fd, 0)) != (void *)-1)                  \
        {                                                                       \
            MMT(x)->state = (MTSTATE *)MMT(x)->start;                           \
            MMT(x)->map = (MTNODE *)(MMT(x)->start + sizeof(MTSTATE));          \
        }                                                                       \
    }                                                                           \
}while(0)

#define MMT_INCRE(x)                                                            \
do                                                                              \
{                                                                               \
    if(x &&  MMT(x)->end <  MMT(x)->size)                                       \
    {                                                                           \
        MMT(x)->old += MMT(x)->end ;                                            \
        MMT(x)->end += (off_t)MMTREE_INCRE_NUM * (off_t)sizeof(MTNODE);         \
        if(ftruncate(MMT(x)->fd, MMT(x)->end) == 0)                             \
        {                                                                       \
            if(MMT(x)->old == sizeof(MTSTATE))                                  \
            {                                                                   \
                memset(MMT(x)->state, 0, sizeof(MTSTATE));                      \
                MMT(x)->state->left += MMTREE_INCRE_NUM - 1;                    \
            }                                                                   \
            else MMT(x)->state->left += MMTREE_INCRE_NUM;                       \
            MMT(x)->state->total += MMTREE_INCRE_NUM;                           \
            memset(MMT(x)->start + MMT(x)->old, 0, MMT(x)->end - MMT(x)->old);  \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            _exit(-1);                                                          \
        }                                                                       \
    }                                                                           \
}while(0)
#define MMT_ROTATE_LEFT(x, prootid, oid, rid, lid, ppid)                        \
do                                                                              \
{                                                                               \
    if(x && (rid = MMT(x)->map[oid].right) > 0)                                 \
    {                                                                           \
        if((lid = MMT(x)->map[oid].right = MMT(x)->map[rid].left) > 0)          \
        {                                                                       \
            MMT(x)->map[lid].parent = oid;                                      \
        }                                                                       \
        if((ppid = MMT(x)->map[rid].parent = MMT(x)->map[oid].parent) > 0)      \
        {                                                                       \
            if(MMT(x)->map[ppid].left == oid)                                   \
                MMT(x)->map[ppid].left = rid;                                   \
            else                                                                \
                MMT(x)->map[ppid].right = rid;                                  \
        }else *prootid = rid;                                                   \
        MMT(x)->map[rid].left = oid;                                            \
        MMT(x)->map[oid].parent = rid;                                          \
    }                                                                           \
}while(0)

#define MMT_ROTATE_RIGHT(x, prootid, oid, lid, rid, ppid)                       \
do                                                                              \
{                                                                               \
    if(x && (lid = MMT(x)->map[oid].left) > 0)                                  \
    {                                                                           \
        if((rid = MMT(x)->map[oid].left = MMT(x)->map[lid].right) > 0)          \
        {                                                                       \
            MMT(x)->map[rid].parent = oid;                                      \
        }                                                                       \
        if((ppid = MMT(x)->map[lid].parent = MMT(x)->map[oid].parent) > 0)      \
        {                                                                       \
            if(MMT(x)->map[ppid].left == oid)                                   \
                MMT(x)->map[ppid].left = lid;                                   \
            else                                                                \
                MMT(x)->map[ppid].right = lid;                                  \
        }                                                                       \
        else *prootid = lid;                                                    \
        MMT(x)->map[lid].right = oid;                                           \
        MMT(x)->map[oid].parent = lid;                                          \
    }                                                                           \
}while(0)

#define MMT_INSERT_COLOR(x, prootid, oid, lid, rid, uid, pid, gpid, ppid)       \
do                                                                              \
{                                                                               \
    while((pid = MMT(x)->map[oid].parent)> 0                                    \
            && MMT(x)->map[pid].color == MMT_COLOR_RED)                         \
    {                                                                           \
        gpid = MMT(x)->map[pid].parent;                                         \
        if(pid == MMT(x)->map[gpid].left)                                       \
        {                                                                       \
            uid = MMT(x)->map[gpid].right;                                      \
            if(uid > 0 && MMT(x)->map[uid].color == MMT_COLOR_RED)              \
            {                                                                   \
                MMT(x)->map[uid].color = MMT_COLOR_BLACK;                       \
                MMT(x)->map[pid].color = MMT_COLOR_BLACK;                       \
                MMT(x)->map[gpid].color = MMT_COLOR_RED;                        \
                oid = gpid;                                                     \
                continue;                                                       \
            }                                                                   \
            if(MMT(x)->map[pid].right == oid)                                   \
            {                                                                   \
                MMT_ROTATE_LEFT(x, prootid, pid, rid, lid, ppid);               \
                uid = pid; pid = oid; oid = uid;                                \
            }                                                                   \
            MMT(x)->map[pid].color = MMT_COLOR_BLACK;                           \
            MMT(x)->map[gpid].color = MMT_COLOR_RED;                            \
            MMT_ROTATE_RIGHT(x, prootid, gpid, lid, rid, ppid);                 \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            uid = MMT(x)->map[gpid].left;                                       \
            if(uid > 0 && MMT(x)->map[uid].color == MMT_COLOR_RED)              \
            {                                                                   \
                MMT(x)->map[uid].color = MMT_COLOR_BLACK;                       \
                MMT(x)->map[pid].color = MMT_COLOR_BLACK;                       \
                MMT(x)->map[gpid].color = MMT_COLOR_RED;                        \
                oid = gpid;                                                     \
                continue;                                                       \
            }                                                                   \
            if(MMT(x)->map[pid].left == oid)                                    \
            {                                                                   \
                MMT_ROTATE_RIGHT(x, prootid, pid, lid, rid, ppid);              \
                uid = pid; pid = oid; oid = uid;                                \
            }                                                                   \
            MMT(x)->map[pid].color = MMT_COLOR_BLACK;                           \
            MMT(x)->map[gpid].color = MMT_COLOR_RED;                            \
            MMT_ROTATE_LEFT(x, prootid, gpid, rid, lid, ppid);                  \
        }                                                                       \
    }                                                                           \
    if(*prootid > 0)MMT(x)->map[*prootid].color = MMT_COLOR_BLACK;              \
}while(0)

#define MMT_REMOVE_COLOR(x, prootid, oid, xpid, lid, rid, uid, ppid)            \
do                                                                              \
{                                                                               \
    while((oid == 0 || MMT(x)->map[oid].color == MMT_COLOR_BLACK)               \
            && oid != *prootid)                                                 \
    {                                                                           \
        if(MMT(x)->map[xpid].left == oid)                                       \
        {                                                                       \
            uid = MMT(x)->map[xpid].right;                                      \
            if(MMT(x)->map[uid].color == MMT_COLOR_RED)                         \
            {                                                                   \
                MMT(x)->map[uid].color = MMT_COLOR_BLACK;                       \
                MMT(x)->map[xpid].color = MMT_COLOR_RED;                        \
                MMT_ROTATE_LEFT(x, prootid, xpid, rid, lid, ppid);              \
                uid = MMT(x)->map[xpid].right;                                  \
            }                                                                   \
            lid = MMT(x)->map[uid].left;                                        \
            rid = MMT(x)->map[uid].right;                                       \
            if((lid == 0 || MMT(x)->map[lid].color == MMT_COLOR_BLACK)          \
                && (rid == 0 || MMT(x)->map[rid].color == MMT_COLOR_BLACK))     \
            {                                                                   \
                MMT(x)->map[uid].color = MMT_COLOR_RED;                         \
                oid = xpid;                                                     \
                xpid = MMT(x)->map[oid].parent;                                 \
            }                                                                   \
            else                                                                \
            {                                                                   \
                rid = MMT(x)->map[uid].right;                                   \
                lid = MMT(x)->map[uid].left;                                    \
                if(rid == 0 || MMT(x)->map[rid].color == MMT_COLOR_BLACK)       \
                {                                                               \
                    if(lid > 0)MMT(x)->map[lid].color = MMT_COLOR_BLACK;        \
                    MMT(x)->map[uid].color = MMT_COLOR_RED;                     \
                    MMT_ROTATE_RIGHT(x, prootid, uid, lid, rid, ppid);          \
                    uid = MMT(x)->map[xpid].right;                              \
                }                                                               \
                MMT(x)->map[uid].color = MMT(x)->map[xpid].color;               \
                MMT(x)->map[xpid].color = MMT_COLOR_BLACK;                      \
                if((rid = MMT(x)->map[uid].right) > 0)                          \
                    MMT(x)->map[rid].color = MMT_COLOR_BLACK;                   \
                MMT_ROTATE_LEFT(x, prootid, xpid, rid, lid, ppid);              \
                oid = *prootid;                                                 \
                break;                                                          \
            }                                                                   \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            uid = MMT(x)->map[xpid].left;                                       \
            if(MMT(x)->map[uid].color == MMT_COLOR_RED)                         \
            {                                                                   \
                MMT(x)->map[uid].color = MMT_COLOR_BLACK;                       \
                MMT(x)->map[xpid].color = MMT_COLOR_RED;                        \
                MMT_ROTATE_RIGHT(x, prootid, xpid, lid, rid, ppid);             \
                uid = MMT(x)->map[xpid].left;                                   \
            }                                                                   \
            lid = MMT(x)->map[uid].left;                                        \
            rid = MMT(x)->map[uid].right;                                       \
            if((lid == 0 || MMT(x)->map[lid].color == MMT_COLOR_BLACK)          \
                && (rid == 0 || MMT(x)->map[rid].color == MMT_COLOR_BLACK))     \
            {                                                                   \
                MMT(x)->map[uid].color = MMT_COLOR_RED;                         \
                oid = xpid;                                                     \
                xpid = MMT(x)->map[oid].parent;                                 \
            }                                                                   \
            else                                                                \
            {                                                                   \
                rid = MMT(x)->map[uid].right;                                   \
                lid = MMT(x)->map[uid].left;                                    \
                if(lid == 0 || MMT(x)->map[lid].color == MMT_COLOR_BLACK)       \
                {                                                               \
                    if(rid > 0)MMT(x)->map[rid].color = MMT_COLOR_BLACK;        \
                    MMT(x)->map[uid].color = MMT_COLOR_RED;                     \
                    MMT_ROTATE_LEFT(x, prootid, uid, rid, lid, ppid);           \
                    uid = MMT(x)->map[xpid].left;                               \
                }                                                               \
                MMT(x)->map[uid].color = MMT(x)->map[xpid].color;               \
                MMT(x)->map[xpid].color = MMT_COLOR_BLACK;                      \
                if((lid = MMT(x)->map[uid].left) > 0)                           \
                    MMT(x)->map[lid].color = MMT_COLOR_BLACK;                   \
                MMT_ROTATE_RIGHT(x, prootid, xpid, lid, rid, ppid);             \
                oid = *prootid;                                                 \
                break;                                                          \
            }                                                                   \
        }                                                                       \
    }                                                                           \
    if(oid > 0) MMT(x)->map[oid].color = MMT_COLOR_BLACK;                       \
}while(0)
/* init mmtree */
void *mmtree_init(char *file)
{
    void *x = NULL;
    struct stat  st = {0};

    if((x = (MMTREE *)calloc(1, sizeof(MMTREE))))
    {
        if((MMT(x)->fd = open(file, O_CREAT|O_RDWR, 0644)) > 0 
                && fstat(MMT(x)->fd, &st) == 0)
        {
            MUTEX_INIT(MMT(x)->mutex);
            MMT(x)->end = st.st_size;
            MMT(x)->size = (off_t)sizeof(MTSTATE) + (off_t)sizeof(MTNODE) * (off_t)MMTREE_NODES_MAX;
            //mmap
            MMT_MMAP(x);
            //init truncate
            if(st.st_size == 0)
            {
                MMT(x)->end = (off_t)sizeof(MTSTATE);
                MMT_INCRE(x);
            }
        }
        else 
        {
            if(MMT(x)->fd > 0) close(MMT(x)->fd);
            free(x);
            x = NULL;
        }
    }
    return x;
}

/* insert new root */
int mmtree_new_tree(void *x)
{
    int id = 0, i = 0;
    if(x)
    {
        MUTEX_LOCK(MMT(x)->mutex);
        if(MMT(x)->state->nroots == 0) MMT(x)->state->nroots = 1;
        if(MMT(x)->state && MMT(x)->state->nroots < MMTREE_ROOT_MAX)
        {
            for(i = 1; i < MMTREE_ROOT_MAX; i++)
            {
                if(MMT(x)->state->roots[i].status == 0)
                {
                    MMT(x)->state->roots[i].status = 1;
                    MMT(x)->state->nroots++;
                    id = i;
                    break;
                }
            }
        }
        MUTEX_UNLOCK(MMT(x)->mutex);
    }
    return id;
}

/* total */
unsigned int mmtree_total(void *x, int rootid)
{
    unsigned int total = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(MMT(x)->mutex);
        if(MMT(x)->state && MMT(x)->map && rootid < MMTREE_ROOT_MAX)
        {
            total =  MMT(x)->state->roots[rootid].total;
        }
        MUTEX_UNLOCK(MMT(x)->mutex);
    }
    return total;
}

/* insert new node */
unsigned int mmtree_insert(void *x, int rootid, int key, int data, int *old)
{
    unsigned int id = 0, nodeid = 0, rid = 0, lid = 0, uid = 0, pid = 0, 
        gpid = 0, ppid = 0, *prootid = NULL;
    MTNODE *node = NULL;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(MMT(x)->mutex);
        if(MMT(x)->state && MMT(x)->map && rootid < MMTREE_ROOT_MAX
                && MMT(x)->state->roots[rootid].status > 0)
        {
            nodeid = MMT(x)->state->roots[rootid].rootid;
            while(nodeid > 0 && nodeid < MMT(x)->state->total)
            {
                node = &(MMT(x)->map[nodeid]);
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
            if(id == 0)
            {
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, MMT(x)->start, MMT(x)->state, MMT(x)->map, MMT(x)->state->current, MMT(x)->state->left, MMT(x)->state->total, MMT(x)->state->qleft, MMT(x)->state->qfirst, MMT(x)->state->qlast);
                if(MMT(x)->state->left == 0)
                {
                    MMT_INCRE(x);
                }
                if(MMT(x)->state->qleft > 0)
                {
                    id = MMT(x)->state->qfirst;
                    MMT(x)->state->qfirst = MMT(x)->map[id].parent;
                    MMT(x)->state->qleft--;
                }
                else
                {
                    id = ++(MMT(x)->state->current);
                }
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, MMT(x)->start, MMT(x)->state, MMT(x)->map, MMT(x)->state->current, MMT(x)->state->left, MMT(x)->state->total, MMT(x)->state->qleft, MMT(x)->state->qfirst, MMT(x)->state->qlast);
                MMT(x)->state->left--;
                //memset(&(MMT(x)->map[id]), 0, sizeof(MTNODE));
                MMT(x)->map[id].parent = nodeid;
                MMT(x)->map[id].key = key;
                MMT(x)->map[id].data = data;
                MMT_MIN_MAX(x, id, key);
                if(nodeid > 0)
                {
                    if(key > MMT(x)->map[nodeid].key) 
                        MMT(x)->map[nodeid].right = id;
                    else
                        MMT(x)->map[nodeid].left = id;
                }
                MMT(x)->state->roots[rootid].total++;
            }
            else
            {
                //fprintf(stdout, "%s::%d old id:%d pid:%d key:%d\n", __FILE__, __LINE__, id, parentid, key);
            }
        }
        if((nodeid = id) > 0)
        {
            if(MMT(x)->state->roots[rootid].rootid > 0)
            {
                prootid = &(MMT(x)->state->roots[rootid].rootid);
                MMT(x)->map[nodeid].color = MMT_COLOR_RED;
                MMT_INSERT_COLOR(x, prootid, nodeid, lid, rid, uid, pid, gpid, ppid);
            }
            else
            {
                MMT(x)->state->roots[rootid].rootid = nodeid;
            }
        }
end:
        MUTEX_UNLOCK(MMT(x)->mutex);
    }
    return id;
}

/* try insert  node */
unsigned int mmtree_try_insert(void *x, int rootid, int key, int data, int *old)
{
    unsigned int id = 0, nodeid = 0, rid = 0, lid = 0, uid = 0, pid = 0, 
        gpid = 0, ppid = 0, *prootid = NULL;
    MTNODE *node = NULL;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(MMT(x)->mutex);
        if(MMT(x)->state && MMT(x)->map && rootid < MMTREE_ROOT_MAX
                && MMT(x)->state->roots[rootid].status > 0)
        {
            nodeid = MMT(x)->state->roots[rootid].rootid;
            while(nodeid > 0 && nodeid < MMT(x)->state->total)
            {
                node = &(MMT(x)->map[nodeid]);
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
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, MMT(x)->start, MMT(x)->state, MMT(x)->map, MMT(x)->state->current, MMT(x)->state->left, MMT(x)->state->total, MMT(x)->state->qleft, MMT(x)->state->qfirst, MMT(x)->state->qlast);
                if(MMT(x)->state->left == 0)
                {
                    MMT_INCRE(x);
                }
                if(MMT(x)->state->qleft > 0)
                {
                    id = MMT(x)->state->qfirst;
                    MMT(x)->state->qfirst = MMT(x)->map[id].parent;
                    MMT(x)->state->qleft--;
                }
                else
                {
                    id = ++(MMT(x)->state->current);
                }
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, MMT(x)->start, MMT(x)->state, MMT(x)->map, MMT(x)->state->current, MMT(x)->state->left, MMT(x)->state->total, MMT(x)->state->qleft, MMT(x)->state->qfirst, MMT(x)->state->qlast);
                MMT(x)->state->left--;
                //memset(&(MMT(x)->map[id]), 0, sizeof(MTNODE));
                MMT(x)->map[id].parent = nodeid;
                MMT(x)->map[id].key = key;
                MMT(x)->map[id].data = data;
                MMT_MIN_MAX(x, id, key);
                if(nodeid > 0)
                {
                    if(key > MMT(x)->map[nodeid].key) 
                        MMT(x)->map[nodeid].right = id;
                    else
                        MMT(x)->map[nodeid].left = id;
                }
                MMT(x)->state->roots[rootid].total++;
            }
            else
            {
                //fprintf(stdout, "%s::%d old id:%d pid:%d key:%d\n", __FILE__, __LINE__, id, parentid, key);
            }
        }
        if((nodeid = id) > 0)
        {
            if(MMT(x)->state->roots[rootid].rootid > 0)
            {
                prootid = &(MMT(x)->state->roots[rootid].rootid);
                MMT(x)->map[nodeid].color = MMT_COLOR_RED;
                MMT_INSERT_COLOR(x, prootid, nodeid, lid, rid, uid, pid, gpid, ppid);
            }
            else
            {
                MMT(x)->state->roots[rootid].rootid = nodeid;
            }
        }
end:
        MUTEX_UNLOCK(MMT(x)->mutex);
    }
    return id;
}


/* get node key/data */
unsigned int mmtree_get(void *x, unsigned int tnodeid, int *key, int *data)
{
    unsigned int id = 0;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(MMT(x)->mutex);
        if(MMT(x)->map && MMT(x)->state && tnodeid <  MMT(x)->state->total)
        {
            if(key) *key = MMT(x)->map[tnodeid].key;
            if(data) *data = MMT(x)->map[tnodeid].data;
            id = tnodeid;
        }
        MUTEX_UNLOCK(MMT(x)->mutex);
    }
    return id;
}

/* find key/data */
unsigned int mmtree_find(void *x, int rootid, int key, int *data)
{
    unsigned int id = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(MMT(x)->mutex);
        if(MMT(x)->map && MMT(x)->state && rootid < MMTREE_ROOT_MAX
                && MMT(x)->state->roots[rootid].status > 0)
        {
            //fprintf(stdout, "%s::%d rootid:%d key:%d data:%d total:%d\n", __FILE__, __LINE__, rootid, key, *data, MMT(x)->state->total);
            id = MMT(x)->state->roots[rootid].rootid;
            while(id > 0 && id < MMT(x)->state->total)
            {
                if(key == MMT(x)->map[id].key)
                {
                    if(data) *data = MMT(x)->map[id].data;
                    break;
                }
                else if(key > MMT(x)->map[id].key)
                {
                    id = MMT(x)->map[id].right;
                }
                else
                {
                    id = MMT(x)->map[id].left;
                }
            }
        }
        //fprintf(stdout, "%s::%d rootid:%d key:%d data:%d\n", __FILE__, __LINE__, rootid, key, *data);
        MUTEX_UNLOCK(MMT(x)->mutex);
    }
    return id;
}

/* get tree->min key/data */
unsigned int mmtree_min(void *x, int rootid, int *key, int *data)
{
    unsigned int id = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(MMT(x)->mutex);
        if(MMT(x)->map && MMT(x)->state && rootid <  MMTREE_ROOT_MAX
                && MMT(x)->state->roots[rootid].status > 0)
        {
            id = MMT(x)->state->roots[rootid].rootid;
            while(MMT(x)->map[id].left > 0)
            {
                id = MMT(x)->map[id].left;
            }
            if(id > 0 && MMT(x)->state->total)
            {
                if(key) *key = MMT(x)->map[id].key;
                if(data) *data = MMT(x)->map[id].data;
            }
        }
        MUTEX_UNLOCK(MMT(x)->mutex);
    }
    return id;
}

/* get tree->max key/data */
unsigned  int mmtree_max(void *x, int rootid, int *key, int *data)
{
    unsigned int id = 0, tmp = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(MMT(x)->mutex);
        if(MMT(x)->map && MMT(x)->state && rootid <  MMTREE_ROOT_MAX
                && MMT(x)->state->roots[rootid].status > 0)
        {
            tmp = MMT(x)->state->roots[rootid].rootid;
            do
            {
                id = tmp;
            }while(id > 0 && (tmp = MMT(x)->map[id].right) > 0);
            if(id > 0 && MMT(x)->state->total)
            {
                if(key) *key = MMT(x)->map[id].key;
                if(data) *data = MMT(x)->map[id].data;
            }
        }
        MUTEX_UNLOCK(MMT(x)->mutex);
    }
    return id;
}

/* get next node key/data */
unsigned int mmtree_next(void *x, int rootid, unsigned int tnodeid, int *key, int *data)
{
    unsigned int id = 0, parentid = 0;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(MMT(x)->mutex);
        if(MMT(x)->map && MMT(x)->state && tnodeid <  MMT(x)->state->total)
        {
            id = tnodeid;
            if(MMT(x)->map[id].right > 0)
            {
                id = MMT(x)->map[id].right;
                while(MMT(x)->map[id].left  > 0)
                {
                    id = MMT(x)->map[id].left;
                }
                //fprintf(stdout, "%s::%d id:%d\n", __FILE__, __LINE__, id);
            }
            else
            {
                while(id > 0)
                {
                    parentid = MMT(x)->map[id].parent;
                    if(MMT(x)->map[id].key < MMT(x)->map[parentid].key)
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
            if(id > 0 && id < MMT(x)->state->total)
            {
                if(key) *key = MMT(x)->map[id].key;
                if(data) *data = MMT(x)->map[id].data;
            }
            //fprintf(stdout, "%s::%d rootid:%d tnodeid:%d id:%d\n",__FILE__, __LINE__, rootid, tnodeid, id);
        }
        MUTEX_UNLOCK(MMT(x)->mutex);
    }
    return id;
}

/* get prev node key/data */
unsigned int mmtree_prev(void *x, int rootid, unsigned int tnodeid, int *key, int *data)
{
    unsigned int id = 0, parentid = 0;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(MMT(x)->mutex);
        if(MMT(x)->map && MMT(x)->state && tnodeid <  MMT(x)->state->total)
        {
            id = tnodeid;
            if(MMT(x)->map[id].left > 0)
            {
                id = MMT(x)->map[id].left;
                while(MMT(x)->map[id].right  > 0)
                {
                    id = MMT(x)->map[id].right;
                }
                //fprintf(stdout, "%s::%d id:%d\n", __FILE__, __LINE__, id);
            }
            else
            {
                while(id > 0)
                {
                    parentid = MMT(x)->map[id].parent;
                    if(MMT(x)->map[id].key > MMT(x)->map[parentid].key)
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
            if(id > 0 && id < MMT(x)->state->total)
            {
                if(key)*key = MMT(x)->map[id].key;
                if(data)*data = MMT(x)->map[id].data;
            }
            //fprintf(stdout, "%s::%d rootid:%d tnodeid:%d id:%d\n",__FILE__, __LINE__, rootid, tnodeid, id);
        }
        MUTEX_UNLOCK(MMT(x)->mutex);
    }
    return id;
}

/* view node */
void mmtree_view_tnode(void *x, unsigned int tnodeid, FILE *fp)
{
    if(x)
    {
        if(MMT(x)->map[tnodeid].left > 0 && MMT(x)->map[tnodeid].left < MMT(x)->state->total)
        {
            mmtree_view_tnode(x, MMT(x)->map[tnodeid].left, fp);
        }
        fprintf(fp, "[%d:%lld:%d]\n", tnodeid, (long long)MMT(x)->map[tnodeid].key, MMT(x)->map[tnodeid].data);
        if(MMT(x)->map[tnodeid].right > 0 && MMT(x)->map[tnodeid].right < MMT(x)->state->total)
        {
            mmtree_view_tnode(x, MMT(x)->map[tnodeid].right, fp);
        }
    }
    return ;
}

void mmtree_view_tree(void *x, int rootid, FILE *fp)
{
    if(x && rootid > 0)
    {
        MUTEX_LOCK(MMT(x)->mutex);
        if(MMT(x)->map && MMT(x)->state && rootid < MMTREE_ROOT_MAX)
        {
            fprintf(stdout, "%s::%d rootid:%d\n", __FILE__, __LINE__, MMT(x)->state->roots[rootid].rootid);
             mmtree_view_tnode(x, MMT(x)->state->roots[rootid].rootid, fp);
        }
        MUTEX_UNLOCK(MMT(x)->mutex);
    }
    return ;
}

/* set data */
int mmtree_set_data(void *x, unsigned int tnodeid, int data)
{
    int old = -1;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(MMT(x)->mutex);
        if(MMT(x)->map && MMT(x)->state && tnodeid < MMT(x)->state->total)
        {
            old = MMT(x)->map[tnodeid].data;
            MMT(x)->map[tnodeid].data = data;
        }
        MUTEX_UNLOCK(MMT(x)->mutex);
    }
    return old;
}

/* remove node */
void mmtree_remove(void *x, int rootid, unsigned int tnodeid, int *key, int *data)
{
    unsigned int id = 0, pid = 0, parent = 0, child = 0, rid = 0, lid = 0,
        uid = 0, ppid = 0, z = 0, color = 0, *prootid = NULL;

    if(x && rootid > 0 && tnodeid > 0)
    {
        MUTEX_LOCK(MMT(x)->mutex);
        if(MMT(x)->map && MMT(x)->state && tnodeid < MMT(x)->state->total)
        {
            if(key) *key = MMT(x)->map[tnodeid].key;
            if(data) *data = MMT(x)->map[tnodeid].data;
            id = tnodeid;
            if(MMT(x)->map[tnodeid].left == 0)
            {
                child = MMT(x)->map[tnodeid].right;
            }
            else if(MMT(x)->map[tnodeid].right == 0)
            {
                child = MMT(x)->map[tnodeid].left;
            }
            else 
            {
                id = MMT(x)->map[tnodeid].right;
                while(MMT(x)->map[id].left > 0)
                    id = MMT(x)->map[id].left;
                parent = MMT(x)->map[id].parent;
                color = MMT(x)->map[id].color;
                if((child = MMT(x)->map[id].right) > 0)
                    MMT(x)->map[child].parent = parent;
                if((pid = parent) > 0)
                {
                    if(MMT(x)->map[pid].left == id)
                        MMT(x)->map[pid].left = child;
                    else
                        MMT(x)->map[pid].right = child;
                }
                else
                {
                    MMT(x)->state->roots[rootid].rootid = child;
                }
                if(MMT(x)->map[id].parent == tnodeid) parent = id;
                MMT(x)->map[id].color = MMT(x)->map[tnodeid].color;
                MMT(x)->map[id].parent = MMT(x)->map[tnodeid].parent;
                MMT(x)->map[id].left = MMT(x)->map[tnodeid].left;
                MMT(x)->map[id].right = MMT(x)->map[tnodeid].right;
                if((pid = MMT(x)->map[tnodeid].parent) > 0)
                {
                    if(MMT(x)->map[pid].left == tnodeid)
                        MMT(x)->map[pid].left = id;
                    else
                        MMT(x)->map[pid].right = id;
                }
                else
                {
                    MMT(x)->state->roots[rootid].rootid = id;
                }
                lid = MMT(x)->map[tnodeid].left;
                MMT(x)->map[lid].parent = id;
                if((rid = MMT(x)->map[tnodeid].right) > 0)
                    MMT(x)->map[rid].parent = id;
                goto color_remove;
            }
            parent =  MMT(x)->map[tnodeid].parent;
            color = MMT(x)->map[tnodeid].color;
            if(child > 0) 
            {
                MMT(x)->map[child].parent = parent;
            }
            if((pid = parent) > 0)
            {
                if(tnodeid == MMT(x)->map[pid].left) 
                    MMT(x)->map[pid].left = child;
                else 
                    MMT(x)->map[pid].right = child;
            }
            else 
            {
                MMT(x)->state->roots[rootid].rootid = child;
            }
            //remove color set
color_remove:
            MMT(x)->state->roots[rootid].total--;
            if(color == MMT_COLOR_BLACK)
            {
                //fprintf(stdout, "%s::%d node:%d parent:%d left:%d right:%d key:%d data:%d\n", __FILE__, __LINE__, tnodeid, MMT(x)->map[tnodeid].parent, MMT(x)->map[tnodeid].left, MMT(x)->map[tnodeid].right, MMT(x)->map[tnodeid].key, MMT(x)->map[tnodeid].data);
                prootid = &(MMT(x)->state->roots[rootid].rootid);
                MMT_REMOVE_COLOR(x, prootid, child, parent, lid, rid, uid, ppid);
            }
            //add to qleft
            memset(&(MMT(x)->map[tnodeid]), 0, sizeof(MTNODE));
            if(MMT(x)->state->qleft == 0)
            {
                MMT(x)->state->qfirst = MMT(x)->state->qlast = tnodeid;
            }
            else
            {
                z = MMT(x)->state->qlast;
                MMT(x)->map[z].parent = tnodeid;
                MMT(x)->state->qlast = tnodeid;
            }
            MMT(x)->state->qleft++;
            MMT(x)->state->left++;
            //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, MMT(x)->start, MMT(x)->state, MMT(x)->map, MMT(x)->state->current, MMT(x)->state->left, MMT(x)->state->total, MMT(x)->state->qleft, MMT(x)->state->qfirst, MMT(x)->state->qlast);
  
        }
        MUTEX_UNLOCK(MMT(x)->mutex);
    }
    return ;
}

/* remove node */
void mmtree_remove_tnode(void *x, unsigned int tnodeid)
{
    unsigned int id = 0;

    if(x)
    {
        if(MMT(x)->map[tnodeid].left > 0 && MMT(x)->map[tnodeid].left < MMT(x)->state->total)
        {
            mmtree_remove_tnode(x, MMT(x)->map[tnodeid].left);
        }
        if(MMT(x)->map[tnodeid].right > 0 && MMT(x)->map[tnodeid].right < MMT(x)->state->total)
        {
            mmtree_remove_tnode(x, MMT(x)->map[tnodeid].right);
        }
        memset(&(MMT(x)->map[tnodeid]), 0, sizeof(MTNODE));
        if(MMT(x)->state->qleft == 0)
        {
            MMT(x)->state->qfirst = MMT(x)->state->qlast = tnodeid;
        }
        else
        {
            id = MMT(x)->state->qlast;
            MMT(x)->map[id].parent = tnodeid;
            MMT(x)->state->qlast = tnodeid;
        }
        MMT(x)->state->qleft++;
        MMT(x)->state->left++;
    }
    return ;
}

/* remove tree */
void mmtree_remove_tree(void *x, int rootid)
{
    if(x && rootid > 0 && rootid < MMTREE_ROOT_MAX)
    {
        MUTEX_LOCK(MMT(x)->mutex);
        mmtree_remove_tnode(x, MMT(x)->state->roots[rootid].rootid);
        MMT(x)->state->roots[rootid].rootid = 0;
        MMT(x)->state->roots[rootid].status = 0;
        //fprintf(stdout, "%s::%d rootid:%d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, rootid, MMT(x)->start, MMT(x)->state, MMT(x)->map, MMT(x)->state->current, MMT(x)->state->left, MMT(x)->state->total, MMT(x)->state->qleft, MMT(x)->state->qfirst, MMT(x)->state->qlast);
 
        MUTEX_UNLOCK(MMT(x)->mutex);
    }
    return ;
}

//close mmtree
void mmtree_close(void *x)
{
    if(x)
    {
        //fprintf(stdout, "%s::%d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d sizeof(MTSTATE):%d\n", __FILE__, __LINE__, MMT(x)->start, MMT(x)->state, MMT(x)->map, MMT(x)->state->current, MMT(x)->state->left, MMT(x)->state->total, MMT(x)->state->qleft, MMT(x)->state->qfirst, MMT(x)->state->qlast, sizeof(MTSTATE));
        MMT_MUNMAP(x);
        MUTEX_DESTROY(MMT(x)->mutex);
        if(MMT(x)->fd) close(MMT(x)->fd);
        free(x);
    }
}


#ifdef _DEBUG_MMTREE
#include "md5.h"
#include "timer.h"
int main(int argc, char **argv) 
{
    int i = 0, rootid = 0, id = 0, j = 0, old = 0, data = 0, n = 0, count = 50000000;
    unsigned char digest[MD5_LEN];
    void *mmtree = NULL;
    void *timer = NULL;
    char line[1024];
    int key = 0;

    if((mmtree = mmtree_init("/tmp/test.mmtree")))
    {
        rootid = mmtree_new_tree(mmtree);
        TIMER_INIT(timer);
        for(j = 1; j <= count; j++)
        {
            n = sprintf(line, "http://www.demo.com/%d.html", j);
            md5(line, n, digest);
            key = *((int *)digest);
            old = -1;
            data = j;
            id = mmtree_insert(mmtree, rootid, key, data, &old);
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
            id = mmtree_try_insert(mmtree, rootid, key, data, &old);
            if(old > 0 && old != j) 
            {
                fprintf(stdout, "%d:{id:%d key:%d rootid:%d old:%d}\n", j, id, key, rootid, old);
                _exit(-1);
            }
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "%s::%d try_insert:%d time:%lld\n", __FILE__,__LINE__, count, PT_LU_USEC(timer));
        TIMER_CLEAN(timer);
        mmtree_close(mmtree);
    }
}
//gcc -o mtree64 mmtree.c md5.c -D_DEBUG_MMTREE -g && ./mtree64
#endif
