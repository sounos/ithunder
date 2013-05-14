#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include "kmap.h"
#include "mutex.h"
#ifdef MAP_LOCKED
#define MMAP_SHARED MAP_SHARED|MAP_LOCKED
#else
#define MMAP_SHARED MAP_SHARED
#endif
#define KMP(px) ((KMAP *)px)
#define KMP_COLOR_BLACK  0
#define KMP_COLOR_RED    1
#define KMP_MIN_MAX(x, key, xid)                                                \
do                                                                              \
{                                                                               \
    if(KMP(x) && KMP(x)->state)                                                 \
    {                                                                           \
        if(KMP(x)->state->count == 0)                                           \
        {                                                                       \
            KMP(x)->state->nmin = KMP(x)->state->nmax = xid;                    \
            KMP(x)->state->kmin = KMP(x)->state->kmax = key;                    \
        }                                                                       \
        else if(key > KMP(x)->state->kmax)                                      \
        {                                                                       \
            KMP(x)->state->nmax = xid;                                          \
            KMP(x)->state->kmax = key;                                          \
        }                                                                       \
        else if(key < KMP(x)->state->kmin)                                      \
        {                                                                       \
            KMP(x)->state->nmin = xid;                                          \
            KMP(x)->state->kmin = key;                                          \
        }                                                                       \
    }                                                                           \
}while(0)
#define KMP_MUNMAP(x)                                                           \
do                                                                              \
{                                                                               \
    if(x && KMP(x)->size > 0)                                                   \
    {                                                                           \
        if(KMP(x)->start && KMP(x)->start != (void *)-1)                        \
        {                                                                       \
            munmap(KMP(x)->start, KMP(x)->size);                                \
            KMP(x)->start = NULL;                                               \
            KMP(x)->state = NULL;                                               \
            KMP(x)->map = NULL;                                                 \
        }                                                                       \
    }                                                                           \
}while(0)

#define KMP_MMAP(x)                                                             \
do                                                                              \
{                                                                               \
    if(x)                                                                       \
    {                                                                           \
        if((KMP(x)->start = (char*)mmap(NULL,KMP(x)->size,PROT_READ|PROT_WRITE, \
                    MAP_SHARED, KMP(x)->fd, 0)) != (void *)-1)                  \
        {                                                                       \
            KMP(x)->state = (KMSTATE *)KMP(x)->start;                           \
            KMP(x)->map = (KMNODE *)(KMP(x)->start + sizeof(KMSTATE));          \
        }                                                                       \
    }                                                                           \
}while(0)

#define KMP_INCRE(x)                                                            \
do                                                                              \
{                                                                               \
    if(x &&  KMP(x)->end <  KMP(x)->size)                                       \
    {                                                                           \
        KMP(x)->old = KMP(x)->end ;                                             \
        KMP(x)->end += (off_t)KMAP_INCRE_NUM * (off_t)sizeof(KMNODE);          \
        if(ftruncate(KMP(x)->fd, KMP(x)->end) == 0)                             \
        {                                                                       \
            if(KMP(x)->old == sizeof(KMSTATE))                                  \
            {                                                                   \
                memset(KMP(x)->state, 0, sizeof(KMSTATE));                      \
                KMP(x)->state->left += KMAP_INCRE_NUM - 1;                     \
            }                                                                   \
            else KMP(x)->state->left += KMAP_INCRE_NUM;                        \
            KMP(x)->state->total += KMAP_INCRE_NUM;                            \
            memset(KMP(x)->start + KMP(x)->old, 0, KMP(x)->end - KMP(x)->old);  \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            _exit(-1);                                                          \
        }                                                                       \
    }                                                                           \
}while(0)
#define KMP_ROOT_MUNMAP(x)                                                      \
do                                                                              \
{                                                                               \
    if(x && KMP(x)->rsize > 0)                                                  \
    {                                                                           \
        if(KMP(x)->roots && KMP(x)->roots != (void *)-1)                        \
        {                                                                       \
            munmap(KMP(x)->roots, KMP(x)->rsize);                               \
            KMP(x)->roots = NULL;                                               \
        }                                                                       \
    }                                                                           \
}while(0)

#define KMP_ROOT_MMAP(x)                                                        \
do                                                                              \
{                                                                               \
    if(x)                                                                       \
    {                                                                           \
        if((KMP(x)->roots = (KMROOT *)mmap(NULL,KMP(x)->rsize,                  \
            PROT_READ|PROT_WRITE, MAP_SHARED, KMP(x)->rfd, 0)) == MAP_FAILED)   \
        {                                                                       \
            break;                                                              \
        }                                                                       \
    }                                                                           \
}while(0)

#define KMP_ROOT_INCRE(x)                                                       \
do                                                                              \
{                                                                               \
    if(x &&  KMP(x)->rend <  KMP(x)->rsize)                                     \
    {                                                                           \
        KMP(x)->rold = KMP(x)->rend ;                                           \
        KMP(x)->rend += (off_t)KMROOT_INCRE_NUM * (off_t)sizeof(KMROOT);        \
        if(ftruncate(KMP(x)->rfd, KMP(x)->rend) == 0)                           \
        {                                                                       \
            KMP(x)->state->rleft += KMROOT_INCRE_NUM;                           \
            if(KMP(x)->rold == 0) KMP(x)->state->rleft--;                       \
            KMP(x)->state->rtotal += KMROOT_INCRE_NUM;                          \
            memset((char *)(KMP(x)->roots) + KMP(x)->rold,                      \
                    0, KMP(x)->rend - KMP(x)->rold);                            \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            _exit(-1);                                                          \
        }                                                                       \
    }                                                                           \
}while(0)

#define KMP_ROTATE_LEFT(x, prootid, oid, rid, lid, ppid)                        \
do                                                                              \
{                                                                               \
    if(x && (rid = KMP(x)->map[oid].right) > 0)                                 \
    {                                                                           \
        if((lid = KMP(x)->map[oid].right = KMP(x)->map[rid].left) > 0)          \
        {                                                                       \
            KMP(x)->map[lid].parent = oid;                                      \
        }                                                                       \
        if((ppid = KMP(x)->map[rid].parent = KMP(x)->map[oid].parent) > 0)      \
        {                                                                       \
            if(KMP(x)->map[ppid].left == oid)                                   \
                KMP(x)->map[ppid].left = rid;                                   \
            else                                                                \
                KMP(x)->map[ppid].right = rid;                                  \
        }else *prootid = rid;                                                   \
        KMP(x)->map[rid].left = oid;                                            \
        KMP(x)->map[oid].parent = rid;                                          \
    }                                                                           \
}while(0)

#define KMP_ROTATE_RIGHT(x, prootid, oid, lid, rid, ppid)                       \
do                                                                              \
{                                                                               \
    if(x && (lid = KMP(x)->map[oid].left) > 0)                                  \
    {                                                                           \
        if((rid = KMP(x)->map[oid].left = KMP(x)->map[lid].right) > 0)          \
        {                                                                       \
            KMP(x)->map[rid].parent = oid;                                      \
        }                                                                       \
        if((ppid = KMP(x)->map[lid].parent = KMP(x)->map[oid].parent) > 0)      \
        {                                                                       \
            if(KMP(x)->map[ppid].left == oid)                                   \
                KMP(x)->map[ppid].left = lid;                                   \
            else                                                                \
                KMP(x)->map[ppid].right = lid;                                  \
        }                                                                       \
        else *prootid = lid;                                                    \
        KMP(x)->map[lid].right = oid;                                           \
        KMP(x)->map[oid].parent = lid;                                          \
    }                                                                           \
}while(0)

#define KMP_INSERT_COLOR(x, prootid, oid, lid, rid, uid, pid, gpid, ppid)       \
do                                                                              \
{                                                                               \
    while((pid = KMP(x)->map[oid].parent)> 0                                    \
            && KMP(x)->map[pid].color == KMP_COLOR_RED)                         \
    {                                                                           \
        gpid = KMP(x)->map[pid].parent;                                         \
        if(pid == KMP(x)->map[gpid].left)                                       \
        {                                                                       \
            uid = KMP(x)->map[gpid].right;                                      \
            if(uid > 0 && KMP(x)->map[uid].color == KMP_COLOR_RED)              \
            {                                                                   \
                KMP(x)->map[uid].color = KMP_COLOR_BLACK;                       \
                KMP(x)->map[pid].color = KMP_COLOR_BLACK;                       \
                KMP(x)->map[gpid].color = KMP_COLOR_RED;                        \
                oid = gpid;                                                     \
                continue;                                                       \
            }                                                                   \
            if(KMP(x)->map[pid].right == oid)                                   \
            {                                                                   \
                KMP_ROTATE_LEFT(x, prootid, pid, rid, lid, ppid);               \
                uid = pid; pid = oid; oid = uid;                                \
            }                                                                   \
            KMP(x)->map[pid].color = KMP_COLOR_BLACK;                           \
            KMP(x)->map[gpid].color = KMP_COLOR_RED;                            \
            KMP_ROTATE_RIGHT(x, prootid, gpid, lid, rid, ppid);                 \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            uid = KMP(x)->map[gpid].left;                                       \
            if(uid > 0 && KMP(x)->map[uid].color == KMP_COLOR_RED)              \
            {                                                                   \
                KMP(x)->map[uid].color = KMP_COLOR_BLACK;                       \
                KMP(x)->map[pid].color = KMP_COLOR_BLACK;                       \
                KMP(x)->map[gpid].color = KMP_COLOR_RED;                        \
                oid = gpid;                                                     \
                continue;                                                       \
            }                                                                   \
            if(KMP(x)->map[pid].left == oid)                                    \
            {                                                                   \
                KMP_ROTATE_RIGHT(x, prootid, pid, lid, rid, ppid);              \
                uid = pid; pid = oid; oid = uid;                                \
            }                                                                   \
            KMP(x)->map[pid].color = KMP_COLOR_BLACK;                           \
            KMP(x)->map[gpid].color = KMP_COLOR_RED;                            \
            KMP_ROTATE_LEFT(x, prootid, gpid, rid, lid, ppid);                  \
        }                                                                       \
    }                                                                           \
    if(*prootid > 0)KMP(x)->map[*prootid].color = KMP_COLOR_BLACK;              \
}while(0)

#define KMP_REMOVE_COLOR(x, prootid, oid, xpid, lid, rid, uid, ppid)            \
do                                                                              \
{                                                                               \
    while((oid == 0 || KMP(x)->map[oid].color == KMP_COLOR_BLACK)               \
            && oid != *prootid)                                                 \
    {                                                                           \
        if(KMP(x)->map[xpid].left == oid)                                       \
        {                                                                       \
            uid = KMP(x)->map[xpid].right;                                      \
            if(KMP(x)->map[uid].color == KMP_COLOR_RED)                         \
            {                                                                   \
                KMP(x)->map[uid].color = KMP_COLOR_BLACK;                       \
                KMP(x)->map[xpid].color = KMP_COLOR_RED;                        \
                KMP_ROTATE_LEFT(x, prootid, xpid, rid, lid, ppid);              \
                uid = KMP(x)->map[xpid].right;                                  \
            }                                                                   \
            lid = KMP(x)->map[uid].left;                                        \
            rid = KMP(x)->map[uid].right;                                       \
            if((lid == 0 || KMP(x)->map[lid].color == KMP_COLOR_BLACK)          \
                && (rid == 0 || KMP(x)->map[rid].color == KMP_COLOR_BLACK))     \
            {                                                                   \
                KMP(x)->map[uid].color = KMP_COLOR_RED;                         \
                oid = xpid;                                                     \
                xpid = KMP(x)->map[oid].parent;                                 \
            }                                                                   \
            else                                                                \
            {                                                                   \
                rid = KMP(x)->map[uid].right;                                   \
                lid = KMP(x)->map[uid].left;                                    \
                if(rid == 0 || KMP(x)->map[rid].color == KMP_COLOR_BLACK)       \
                {                                                               \
                    if(lid > 0)KMP(x)->map[lid].color = KMP_COLOR_BLACK;        \
                    KMP(x)->map[uid].color = KMP_COLOR_RED;                     \
                    KMP_ROTATE_RIGHT(x, prootid, uid, lid, rid, ppid);          \
                    uid = KMP(x)->map[xpid].right;                              \
                }                                                               \
                KMP(x)->map[uid].color = KMP(x)->map[xpid].color;               \
                KMP(x)->map[xpid].color = KMP_COLOR_BLACK;                      \
                if((rid = KMP(x)->map[uid].right) > 0)                          \
                    KMP(x)->map[rid].color = KMP_COLOR_BLACK;                   \
                KMP_ROTATE_LEFT(x, prootid, xpid, rid, lid, ppid);              \
                oid = *prootid;                                                 \
                break;                                                          \
            }                                                                   \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            uid = KMP(x)->map[xpid].left;                                       \
            if(KMP(x)->map[uid].color == KMP_COLOR_RED)                         \
            {                                                                   \
                KMP(x)->map[uid].color = KMP_COLOR_BLACK;                       \
                KMP(x)->map[xpid].color = KMP_COLOR_RED;                        \
                KMP_ROTATE_RIGHT(x, prootid, xpid, lid, rid, ppid);             \
                uid = KMP(x)->map[xpid].left;                                   \
            }                                                                   \
            lid = KMP(x)->map[uid].left;                                        \
            rid = KMP(x)->map[uid].right;                                       \
            if((lid == 0 || KMP(x)->map[lid].color == KMP_COLOR_BLACK)          \
                && (rid == 0 || KMP(x)->map[rid].color == KMP_COLOR_BLACK))     \
            {                                                                   \
                KMP(x)->map[uid].color = KMP_COLOR_RED;                         \
                oid = xpid;                                                     \
                xpid = KMP(x)->map[oid].parent;                                 \
            }                                                                   \
            else                                                                \
            {                                                                   \
                rid = KMP(x)->map[uid].right;                                   \
                lid = KMP(x)->map[uid].left;                                    \
                if(lid == 0 || KMP(x)->map[lid].color == KMP_COLOR_BLACK)       \
                {                                                               \
                    if(rid > 0)KMP(x)->map[rid].color = KMP_COLOR_BLACK;        \
                    KMP(x)->map[uid].color = KMP_COLOR_RED;                     \
                    KMP_ROTATE_LEFT(x, prootid, uid, rid, lid, ppid);           \
                    uid = KMP(x)->map[xpid].left;                               \
                }                                                               \
                KMP(x)->map[uid].color = KMP(x)->map[xpid].color;               \
                KMP(x)->map[xpid].color = KMP_COLOR_BLACK;                      \
                if((lid = KMP(x)->map[uid].left) > 0)                           \
                    KMP(x)->map[lid].color = KMP_COLOR_BLACK;                   \
                KMP_ROTATE_RIGHT(x, prootid, xpid, lid, rid, ppid);             \
                oid = *prootid;                                                 \
                break;                                                          \
            }                                                                   \
        }                                                                       \
    }                                                                           \
    if(oid > 0) KMP(x)->map[oid].color = KMP_COLOR_BLACK;                       \
}while(0)

/* mkdir force */
int kmap_mkdir(char *path)
{
    struct stat st;
    char fullpath[256];
    char *p = NULL;
    int level = -1, ret = -1;

    if(path)
    {
        strcpy(fullpath, path);
        p = fullpath;
        while(*p != '\0')
        {
            if(*p == '/' )
            {
                level++;
                while(*p != '\0' && *p == '/' && *(p+1) == '/')++p;
                if(level > 0)
                {
                    *p = '\0';
                    memset(&st, 0, sizeof(struct stat));
                    ret = stat(fullpath, &st);
                    if(ret == 0 && !S_ISDIR(st.st_mode)) return -1;
                    if(ret != 0 && mkdir(fullpath, 0755) != 0) return -1;
                    *p = '/';
                }
            }
            ++p;
        }
        return 0;
    }
    return -1;
}

/* init kmap */
void *kmap_init(char *basedir)
{
    struct stat  st = {0};
    void *x = NULL;
    char path[256];
    int n = 0;

    if(basedir && (x = (KMAP *)calloc(1, sizeof(KMAP))))
    {
        MUTEX_INIT(KMP(x)->mutex);
        n = sprintf(path, "%s/kmap.map", basedir); 
        kmap_mkdir(path);
        if((KMP(x)->fd = open(path, O_CREAT|O_RDWR, 0644)) > 0 
                && fstat(KMP(x)->fd, &st) == 0)
        {
            KMP(x)->end = st.st_size;
            KMP(x)->size = (off_t)sizeof(KMSTATE) + (off_t)sizeof(KMNODE) * (off_t)KMAP_NODES_MAX;
            //mmap
            KMP_MMAP(x);
            //init truncate
            if(st.st_size == 0)
            {
                KMP(x)->end = (off_t)sizeof(KMSTATE);
                KMP_INCRE(x);
            }
        }
        else 
        {
            if(KMP(x)->fd > 0) close(KMP(x)->fd);
            free(x);
            x = NULL;
            _exit(-1);
        }
        n = sprintf(path, "%s/kmap.root", basedir); 
        if((KMP(x)->rfd = open(path, O_CREAT|O_RDWR, 0644)) > 0 
                && fstat(KMP(x)->rfd, &st) == 0)
        {
            KMP(x)->rend = st.st_size;
            KMP(x)->rsize = (off_t)sizeof(KMROOT) * (off_t)KMAP_ROOT_MAX;
            //mmap
            KMP_ROOT_MMAP(x);
            //init truncate
            if(st.st_size == 0)
            {
                KMP_ROOT_INCRE(x);
            }
        }
        else 
        {
            if(KMP(x)->rfd > 0) close(KMP(x)->rfd);
            free(x);
            x = NULL;
            _exit(-1);
        }
    }
    return x;
}

/* insert new root */
int kmap_new_tree(void *x)
{
    int id = 0;
    if(x)
    {
        MUTEX_LOCK(KMP(x)->mutex);
        if(KMP(x)->state->rleft == 0)
        {
            KMP_ROOT_INCRE(x);
        }
        if(KMP(x)->state->qrleft > 0)
        {
            id = KMP(x)->state->qrfirst;
            KMP(x)->state->qrfirst = KMP(x)->roots[id].total;
            KMP(x)->state->qrleft--;
        }
        else
        {
            id = ++(KMP(x)->state->rcurrent);
        }
        KMP(x)->state->rleft--;
        KMP(x)->roots[id].status = 1;
        MUTEX_UNLOCK(KMP(x)->mutex);
    }
    return id;
}

/* total */
unsigned int kmap_total(void *x, int rootid)
{
    unsigned int total = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(KMP(x)->mutex);
        if(KMP(x)->state && KMP(x)->map && rootid < KMAP_ROOT_MAX)
        {
            total =  KMP(x)->roots[rootid].total;
        }
        MUTEX_UNLOCK(KMP(x)->mutex);
    }
    return total;
}

/* insert new node */
unsigned int kmap_insert(void *x, int rootid, int64_t key, int data, int *old)
{
    unsigned int id = 0, nodeid = 0, rid = 0, lid = 0, uid = 0, pid = 0, 
        gpid = 0, ppid = 0, *prootid = NULL;
    KMNODE *node = NULL;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(KMP(x)->mutex);
        if(KMP(x)->state && KMP(x)->map && rootid < KMAP_ROOT_MAX
                && KMP(x)->roots[rootid].status > 0)
        {
            nodeid = KMP(x)->roots[rootid].rootid;
            while(nodeid > 0 && nodeid < KMP(x)->state->total)
            {
                node = &(KMP(x)->map[nodeid]);
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
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, KMP(x)->start, KMP(x)->state, KMP(x)->map, KMP(x)->state->current, KMP(x)->state->left, KMP(x)->state->total, KMP(x)->state->qleft, KMP(x)->state->qfirst, KMP(x)->state->qlast);
                if(KMP(x)->state->left == 0)
                {
                    KMP_INCRE(x);
                }
                if(KMP(x)->state->qleft > 0)
                {
                    id = KMP(x)->state->qfirst;
                    KMP(x)->state->qfirst = KMP(x)->map[id].parent;
                    KMP(x)->state->qleft--;
                }
                else
                {
                    id = ++(KMP(x)->state->current);
                }
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, KMP(x)->start, KMP(x)->state, KMP(x)->map, KMP(x)->state->current, KMP(x)->state->left, KMP(x)->state->total, KMP(x)->state->qleft, KMP(x)->state->qfirst, KMP(x)->state->qlast);
                KMP(x)->state->left--;
                //memset(&(KMP(x)->map[id]), 0, sizeof(KMNODE));
                KMP(x)->map[id].parent = nodeid;
                KMP(x)->map[id].key = key;
                KMP(x)->map[id].data = data;
                KMP_MIN_MAX(x, id, key);
                if(nodeid > 0)
                {
                    if(key > KMP(x)->map[nodeid].key) 
                        KMP(x)->map[nodeid].right = id;
                    else
                        KMP(x)->map[nodeid].left = id;
                }
                KMP(x)->roots[rootid].total++;
            }
            else
            {
                //fprintf(stdout, "%s::%d old id:%d pid:%d key:%d\n", __FILE__, __LINE__, id, parentid, key);
            }
        }
        if((nodeid = id) > 0)
        {
            if(KMP(x)->roots[rootid].rootid > 0)
            {
                prootid = &(KMP(x)->roots[rootid].rootid);
                KMP(x)->map[nodeid].color = KMP_COLOR_RED;
                KMP_INSERT_COLOR(x, prootid, nodeid, lid, rid, uid, pid, gpid, ppid);
            }
            else
            {
                KMP(x)->roots[rootid].rootid = nodeid;
            }
        }
end:
        MUTEX_UNLOCK(KMP(x)->mutex);
    }
    return id;
}

/* try insert  node */
unsigned int kmap_try_insert(void *x, int rootid, int64_t key, int data, int *old)
{
    unsigned int id = 0, nodeid = 0, rid = 0, lid = 0, uid = 0, pid = 0, 
        gpid = 0, ppid = 0, *prootid = NULL;
    KMNODE *node = NULL;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(KMP(x)->mutex);
        if(KMP(x)->state && KMP(x)->map && rootid < KMAP_ROOT_MAX
                && KMP(x)->roots[rootid].status > 0)
        {
            nodeid = KMP(x)->roots[rootid].rootid;
            while(nodeid > 0 && nodeid < KMP(x)->state->total)
            {
                node = &(KMP(x)->map[nodeid]);
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
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, KMP(x)->start, KMP(x)->state, KMP(x)->map, KMP(x)->state->current, KMP(x)->state->left, KMP(x)->state->total, KMP(x)->state->qleft, KMP(x)->state->qfirst, KMP(x)->state->qlast);
                if(KMP(x)->state->left == 0)
                {
                    KMP_INCRE(x);
                }
                if(KMP(x)->state->qleft > 0)
                {
                    id = KMP(x)->state->qfirst;
                    KMP(x)->state->qfirst = KMP(x)->map[id].parent;
                    KMP(x)->state->qleft--;
                }
                else
                {
                    id = ++(KMP(x)->state->current);
                }
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, KMP(x)->start, KMP(x)->state, KMP(x)->map, KMP(x)->state->current, KMP(x)->state->left, KMP(x)->state->total, KMP(x)->state->qleft, KMP(x)->state->qfirst, KMP(x)->state->qlast);
                KMP(x)->state->left--;
                //memset(&(KMP(x)->map[id]), 0, sizeof(KMNODE));
                KMP(x)->map[id].parent = nodeid;
                KMP(x)->map[id].key = key;
                KMP(x)->map[id].data = data;
                KMP_MIN_MAX(x, id, key);
                if(nodeid > 0)
                {
                    if(key > KMP(x)->map[nodeid].key) 
                        KMP(x)->map[nodeid].right = id;
                    else
                        KMP(x)->map[nodeid].left = id;
                }
                KMP(x)->roots[rootid].total++;
            }
            else
            {
                //fprintf(stdout, "%s::%d old id:%d pid:%d key:%d\n", __FILE__, __LINE__, id, parentid, key);
            }
        }
        if((nodeid = id) > 0)
        {
            if(KMP(x)->roots[rootid].rootid > 0)
            {
                prootid = &(KMP(x)->roots[rootid].rootid);
                KMP(x)->map[nodeid].color = KMP_COLOR_RED;
                KMP_INSERT_COLOR(x, prootid, nodeid, lid, rid, uid, pid, gpid, ppid);
            }
            else
            {
                KMP(x)->roots[rootid].rootid = nodeid;
            }
        }
end:
        MUTEX_UNLOCK(KMP(x)->mutex);
    }
    return id;
}


/* get node key/data */
unsigned int kmap_get(void *x, unsigned int tnodeid, int64_t *key, int *data)
{
    unsigned int id = 0;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(KMP(x)->mutex);
        if(KMP(x)->map && KMP(x)->state && tnodeid <  KMP(x)->state->total)
        {
            if(key) *key = KMP(x)->map[tnodeid].key;
            if(data) *data = KMP(x)->map[tnodeid].data;
            id = tnodeid;
        }
        MUTEX_UNLOCK(KMP(x)->mutex);
    }
    return id;
}

/* find key/data */
unsigned int kmap_find(void *x, int rootid, int64_t key, int *data)
{
    unsigned int id = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(KMP(x)->mutex);
        if(KMP(x)->map && KMP(x)->state && rootid < KMAP_ROOT_MAX
                && KMP(x)->roots[rootid].status > 0)
        {
            //fprintf(stdout, "%s::%d rootid:%d key:%d data:%d total:%d\n", __FILE__, __LINE__, rootid, key, *data, KMP(x)->state->total);
            id = KMP(x)->roots[rootid].rootid;
            while(id > 0 && id < KMP(x)->state->total)
            {
                if(key == KMP(x)->map[id].key)
                {
                    if(data) *data = KMP(x)->map[id].data;
                    break;
                }
                else if(key > KMP(x)->map[id].key)
                {
                    id = KMP(x)->map[id].right;
                }
                else
                {
                    id = KMP(x)->map[id].left;
                }
            }
        }
        //fprintf(stdout, "%s::%d rootid:%d key:%d data:%d\n", __FILE__, __LINE__, rootid, key, *data);
        MUTEX_UNLOCK(KMP(x)->mutex);
    }
    return id;
}

/* get tree->min key/data */
unsigned int kmap_min(void *x, int rootid, int64_t *key, int *data)
{
    unsigned int id = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(KMP(x)->mutex);
        if(KMP(x)->map && KMP(x)->state && rootid <  KMAP_ROOT_MAX
                && KMP(x)->roots[rootid].status > 0)
        {
            id = KMP(x)->roots[rootid].rootid;
            while(KMP(x)->map[id].left > 0)
            {
                id = KMP(x)->map[id].left;
            }
            if(id > 0 && KMP(x)->state->total)
            {
                if(key) *key = KMP(x)->map[id].key;
                if(data) *data = KMP(x)->map[id].data;
            }
        }
        MUTEX_UNLOCK(KMP(x)->mutex);
    }
    return id;
}

/* get tree->max key/data */
unsigned  int kmap_max(void *x, int rootid, int64_t *key, int *data)
{
    unsigned int id = 0, tmp = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(KMP(x)->mutex);
        if(KMP(x)->map && KMP(x)->state && rootid <  KMAP_ROOT_MAX
                && KMP(x)->roots[rootid].status > 0)
        {
            tmp = KMP(x)->roots[rootid].rootid;
            do
            {
                id = tmp;
            }while(id > 0 && (tmp = KMP(x)->map[id].right) > 0);
            if(id > 0 && KMP(x)->state->total)
            {
                if(key) *key = KMP(x)->map[id].key;
                if(data) *data = KMP(x)->map[id].data;
            }
        }
        MUTEX_UNLOCK(KMP(x)->mutex);
    }
    return id;
}

/* get next node key/data */
unsigned int kmap_next(void *x, int rootid, unsigned int tnodeid, int64_t *key, int *data)
{
    unsigned int id = 0, parentid = 0;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(KMP(x)->mutex);
        if(KMP(x)->map && KMP(x)->state && tnodeid <  KMP(x)->state->total)
        {
            id = tnodeid;
            if(KMP(x)->map[id].right > 0)
            {
                id = KMP(x)->map[id].right;
                while(KMP(x)->map[id].left  > 0)
                {
                    id = KMP(x)->map[id].left;
                }
                //fprintf(stdout, "%s::%d id:%d\n", __FILE__, __LINE__, id);
            }
            else
            {
                while(id > 0)
                {
                    parentid = KMP(x)->map[id].parent;
                    if(KMP(x)->map[id].key < KMP(x)->map[parentid].key)
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
            if(id > 0 && id < KMP(x)->state->total)
            {
                if(key) *key = KMP(x)->map[id].key;
                if(data) *data = KMP(x)->map[id].data;
            }
            //fprintf(stdout, "%s::%d rootid:%d tnodeid:%d id:%d\n",__FILE__, __LINE__, rootid, tnodeid, id);
        }
        MUTEX_UNLOCK(KMP(x)->mutex);
    }
    return id;
}

/* get prev node key/data */
unsigned int kmap_prev(void *x, int rootid, unsigned int tnodeid, int64_t *key, int *data)
{
    unsigned int id = 0, parentid = 0;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(KMP(x)->mutex);
        if(KMP(x)->map && KMP(x)->state && tnodeid <  KMP(x)->state->total)
        {
            id = tnodeid;
            if(KMP(x)->map[id].left > 0)
            {
                id = KMP(x)->map[id].left;
                while(KMP(x)->map[id].right  > 0)
                {
                    id = KMP(x)->map[id].right;
                }
                //fprintf(stdout, "%s::%d id:%d\n", __FILE__, __LINE__, id);
            }
            else
            {
                while(id > 0)
                {
                    parentid = KMP(x)->map[id].parent;
                    if(KMP(x)->map[id].key > KMP(x)->map[parentid].key)
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
            if(id > 0 && id < KMP(x)->state->total)
            {
                if(key)*key = KMP(x)->map[id].key;
                if(data)*data = KMP(x)->map[id].data;
            }
            //fprintf(stdout, "%s::%d rootid:%d tnodeid:%d id:%d\n",__FILE__, __LINE__, rootid, tnodeid, id);
        }
        MUTEX_UNLOCK(KMP(x)->mutex);
    }
    return id;
}

/* view node */
void kmap_view_tnode(void *x, unsigned int tnodeid, FILE *fp)
{
    if(x)
    {
        if(KMP(x)->map[tnodeid].left > 0 && KMP(x)->map[tnodeid].left < KMP(x)->state->total)
        {
            kmap_view_tnode(x, KMP(x)->map[tnodeid].left, fp);
        }
        fprintf(fp, "[%d:%lld:%d]\n", tnodeid, (long long)KMP(x)->map[tnodeid].key, KMP(x)->map[tnodeid].data);
        if(KMP(x)->map[tnodeid].right > 0 && KMP(x)->map[tnodeid].right < KMP(x)->state->total)
        {
            kmap_view_tnode(x, KMP(x)->map[tnodeid].right, fp);
        }
    }
    return ;
}

void kmap_view_tree(void *x, int rootid, FILE *fp)
{
    if(x && rootid > 0)
    {
        MUTEX_LOCK(KMP(x)->mutex);
        if(KMP(x)->map && KMP(x)->state && rootid < KMAP_ROOT_MAX)
        {
            fprintf(stdout, "%s::%d rootid:%d\n", __FILE__, __LINE__, KMP(x)->roots[rootid].rootid);
             kmap_view_tnode(x, KMP(x)->roots[rootid].rootid, fp);
        }
        MUTEX_UNLOCK(KMP(x)->mutex);
    }
    return ;
}

/* set data */
int kmap_set_data(void *x, unsigned int tnodeid, int data)
{
    int old = -1;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(KMP(x)->mutex);
        if(KMP(x)->map && KMP(x)->state && tnodeid < KMP(x)->state->total)
        {
            old = KMP(x)->map[tnodeid].data;
            KMP(x)->map[tnodeid].data = data;
        }
        MUTEX_UNLOCK(KMP(x)->mutex);
    }
    return old;
}

/* remove node */
void kmap_remove(void *x, int rootid, unsigned int tnodeid, int64_t *key, int *data)
{
    unsigned int id = 0, pid = 0, parent = 0, child = 0, rid = 0, lid = 0,
        uid = 0, ppid = 0, z = 0, color = 0, *prootid = NULL;

    if(x && rootid > 0 && tnodeid > 0)
    {
        MUTEX_LOCK(KMP(x)->mutex);
        if(KMP(x)->map && KMP(x)->state && tnodeid < KMP(x)->state->total)
        {
            if(key) *key = KMP(x)->map[tnodeid].key;
            if(data) *data = KMP(x)->map[tnodeid].data;
            id = tnodeid;
            if(KMP(x)->map[tnodeid].left == 0)
            {
                child = KMP(x)->map[tnodeid].right;
            }
            else if(KMP(x)->map[tnodeid].right == 0)
            {
                child = KMP(x)->map[tnodeid].left;
            }
            else 
            {
                id = KMP(x)->map[tnodeid].right;
                while(KMP(x)->map[id].left > 0)
                    id = KMP(x)->map[id].left;
                parent = KMP(x)->map[id].parent;
                color = KMP(x)->map[id].color;
                if((child = KMP(x)->map[id].right) > 0)
                    KMP(x)->map[child].parent = parent;
                if((pid = parent) > 0)
                {
                    if(KMP(x)->map[pid].left == id)
                        KMP(x)->map[pid].left = child;
                    else
                        KMP(x)->map[pid].right = child;
                }
                else
                {
                    KMP(x)->roots[rootid].rootid = child;
                }
                if(KMP(x)->map[id].parent == tnodeid) parent = id;
                KMP(x)->map[id].color = KMP(x)->map[tnodeid].color;
                KMP(x)->map[id].parent = KMP(x)->map[tnodeid].parent;
                KMP(x)->map[id].left = KMP(x)->map[tnodeid].left;
                KMP(x)->map[id].right = KMP(x)->map[tnodeid].right;
                if((pid = KMP(x)->map[tnodeid].parent) > 0)
                {
                    if(KMP(x)->map[pid].left == tnodeid)
                        KMP(x)->map[pid].left = id;
                    else
                        KMP(x)->map[pid].right = id;
                }
                else
                {
                    KMP(x)->roots[rootid].rootid = id;
                }
                lid = KMP(x)->map[tnodeid].left;
                KMP(x)->map[lid].parent = id;
                if((rid = KMP(x)->map[tnodeid].right) > 0)
                    KMP(x)->map[rid].parent = id;
                goto color_remove;
            }
            parent =  KMP(x)->map[tnodeid].parent;
            color = KMP(x)->map[tnodeid].color;
            if(child > 0) 
            {
                KMP(x)->map[child].parent = parent;
            }
            if((pid = parent) > 0)
            {
                if(tnodeid == KMP(x)->map[pid].left) 
                    KMP(x)->map[pid].left = child;
                else 
                    KMP(x)->map[pid].right = child;
            }
            else 
            {
                KMP(x)->roots[rootid].rootid = child;
            }
            //remove color set
color_remove:
            KMP(x)->roots[rootid].total--;
            if(color == KMP_COLOR_BLACK)
            {
                //fprintf(stdout, "%s::%d node:%d parent:%d left:%d right:%d key:%d data:%d\n", __FILE__, __LINE__, tnodeid, KMP(x)->map[tnodeid].parent, KMP(x)->map[tnodeid].left, KMP(x)->map[tnodeid].right, KMP(x)->map[tnodeid].key, KMP(x)->map[tnodeid].data);
                prootid = &(KMP(x)->roots[rootid].rootid);
                KMP_REMOVE_COLOR(x, prootid, child, parent, lid, rid, uid, ppid);
            }
            //add to qleft
            memset(&(KMP(x)->map[tnodeid]), 0, sizeof(KMNODE));
            if(KMP(x)->state->qleft == 0)
            {
                KMP(x)->state->qfirst = KMP(x)->state->qlast = tnodeid;
            }
            else
            {
                z = KMP(x)->state->qlast;
                KMP(x)->map[z].parent = tnodeid;
                KMP(x)->state->qlast = tnodeid;
            }
            KMP(x)->state->qleft++;
            KMP(x)->state->left++;
            //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, KMP(x)->start, KMP(x)->state, KMP(x)->map, KMP(x)->state->current, KMP(x)->state->left, KMP(x)->state->total, KMP(x)->state->qleft, KMP(x)->state->qfirst, KMP(x)->state->qlast);
  
        }
        MUTEX_UNLOCK(KMP(x)->mutex);
    }
    return ;
}

/* remove node */
void kmap_remove_tnode(void *x, unsigned int tnodeid)
{
    unsigned int id = 0;

    if(x)
    {
        if(KMP(x)->map[tnodeid].left > 0 && KMP(x)->map[tnodeid].left < KMP(x)->state->total)
        {
            kmap_remove_tnode(x, KMP(x)->map[tnodeid].left);
        }
        if(KMP(x)->map[tnodeid].right > 0 && KMP(x)->map[tnodeid].right < KMP(x)->state->total)
        {
            kmap_remove_tnode(x, KMP(x)->map[tnodeid].right);
        }
        memset(&(KMP(x)->map[tnodeid]), 0, sizeof(KMNODE));
        if(KMP(x)->state->qleft == 0)
        {
            KMP(x)->state->qfirst = KMP(x)->state->qlast = tnodeid;
        }
        else
        {
            id = KMP(x)->state->qlast;
            KMP(x)->map[id].parent = tnodeid;
            KMP(x)->state->qlast = tnodeid;
        }
        KMP(x)->state->qleft++;
        KMP(x)->state->left++;
    }
    return ;
}

/* remove tree */
void kmap_remove_tree(void *x, int rootid)
{
    int id = 0;

    if(x && rootid > 0 && rootid < KMAP_ROOT_MAX)
    {
        MUTEX_LOCK(KMP(x)->mutex);
        kmap_remove_tnode(x, KMP(x)->roots[rootid].rootid);
        memset(&(KMP(x)->roots[rootid]), 0, sizeof(KMROOT));
        if(KMP(x)->state->qrleft == 0)
        {
            KMP(x)->state->qrfirst = KMP(x)->state->qrlast = rootid;
        }
        else
        {
            id = KMP(x)->state->qrlast;
            KMP(x)->roots[id].total = rootid;
            KMP(x)->state->qrlast = rootid;
        }
        KMP(x)->state->qrleft++;
        KMP(x)->state->rleft++;
        MUTEX_UNLOCK(KMP(x)->mutex);
    }
    return ;
}

//close kmap
void kmap_close(void *x)
{
    if(x)
    {
        //fprintf(stdout, "%s::%d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d sizeof(KMSTATE):%d\n", __FILE__, __LINE__, KMP(x)->start, KMP(x)->state, KMP(x)->map, KMP(x)->state->current, KMP(x)->state->left, KMP(x)->state->total, KMP(x)->state->qleft, KMP(x)->state->qfirst, KMP(x)->state->qlast, sizeof(KMSTATE));
        KMP_MUNMAP(x);
        if(KMP(x)->fd) close(KMP(x)->fd);
        KMP_ROOT_MUNMAP(x);
        if(KMP(x)->rfd) close(KMP(x)->rfd);
        MUTEX_DESTROY(KMP(x)->mutex);
        free(x);
    }
}


#ifdef _DEBUG_KMAP
#include "md5.h"
#include "timer.h"
int main(int argc, char **argv) 
{
    int i = 0, rootid = 0, id = 0, j = 0, old = 0, data = 0, n = 0, count = 50000000;
    unsigned char digest[MD5_LEN];
    void *kmap = NULL;
    void *timer = NULL;
    char line[1024];
    int64_t key = 0;

    if((kmap = kmap_init("/tmp/test.kmap")))
    {
        rootid = kmap_new_tree(kmap);
        TIMER_INIT(timer);
        for(j = 1; j <= count; j++)
        {
            n = sprintf(line, "http://www.demo.com/%d.html", j);
            md5(line, n, digest);
            key = *((int64_t *)digest);
            old = -1;
            data = j;
            id = kmap_insert(kmap, rootid, key, data, &old);
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
            id = kmap_try_insert(kmap, rootid, key, data, &old);
            if(old > 0 && old != j) 
            {
                fprintf(stdout, "%d:{id:%d key:%d rootid:%d old:%d}\n", j, id, key, rootid, old);
                _exit(-1);
            }
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "%s::%d try_insert:%d time:%lld\n", __FILE__,__LINE__, count, PT_LU_USEC(timer));
        TIMER_CLEAN(timer);
        kmap_close(kmap);
    }
}
//gcc -o kmap kmap.c md5.c -D_DEBUG_KMAP -g && ./kmap
#endif
