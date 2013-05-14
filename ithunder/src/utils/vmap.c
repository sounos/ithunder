#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include "vmap.h"
#include "mutex.h"
#ifdef MAP_LOCKED
#define MMAP_SHARED MAP_SHARED|MAP_LOCKED
#else
#define MMAP_SHARED MAP_SHARED
#endif
#define VMP(px) ((VMAP *)px)
#define VMP_COLOR_BLACK  0
#define VMP_COLOR_RED    1
#define VMP_MIN_MAX(x, key, xid)                                                \
do                                                                              \
{                                                                               \
    if(VMP(x) && VMP(x)->state)                                                 \
    {                                                                           \
        if(VMP(x)->state->count == 0)                                           \
        {                                                                       \
            VMP(x)->state->nmin = VMP(x)->state->nmax = xid;                    \
            VMP(x)->state->kmin = VMP(x)->state->kmax = key;                    \
        }                                                                       \
        else if(key > VMP(x)->state->kmax)                                      \
        {                                                                       \
            VMP(x)->state->nmax = xid;                                          \
            VMP(x)->state->kmax = key;                                          \
        }                                                                       \
        else if(key < VMP(x)->state->kmin)                                      \
        {                                                                       \
            VMP(x)->state->nmin = xid;                                          \
            VMP(x)->state->kmin = key;                                          \
        }                                                                       \
    }                                                                           \
}while(0)
#define VMP_MUNMAP(x)                                                           \
do                                                                              \
{                                                                               \
    if(x && VMP(x)->size > 0)                                                   \
    {                                                                           \
        if(VMP(x)->start && VMP(x)->start != (void *)-1)                        \
        {                                                                       \
            munmap(VMP(x)->start, VMP(x)->size);                                \
            VMP(x)->start = NULL;                                               \
            VMP(x)->state = NULL;                                               \
            VMP(x)->map = NULL;                                                 \
        }                                                                       \
    }                                                                           \
}while(0)

#define VMP_MMAP(x)                                                             \
do                                                                              \
{                                                                               \
    if(x)                                                                       \
    {                                                                           \
        if((VMP(x)->start = (char*)mmap(NULL,VMP(x)->size,PROT_READ|PROT_WRITE, \
                    MAP_SHARED, VMP(x)->fd, 0)) != (void *)-1)                  \
        {                                                                       \
            VMP(x)->state = (VMSTATE *)VMP(x)->start;                           \
            VMP(x)->map = (VMNODE *)(VMP(x)->start + sizeof(VMSTATE));          \
        }                                                                       \
    }                                                                           \
}while(0)

#define VMP_INCRE(x)                                                            \
do                                                                              \
{                                                                               \
    if(x &&  VMP(x)->end <  VMP(x)->size)                                       \
    {                                                                           \
        VMP(x)->old = VMP(x)->end ;                                             \
        VMP(x)->end += (off_t)VMAP_INCRE_NUM * (off_t)sizeof(VMNODE);           \
        if(ftruncate(VMP(x)->fd, VMP(x)->end) == 0)                             \
        {                                                                       \
            if(VMP(x)->old == sizeof(VMSTATE))                                  \
            {                                                                   \
                memset(VMP(x)->state, 0, sizeof(VMSTATE));                      \
                VMP(x)->state->left += VMAP_INCRE_NUM - 1;                      \
            }                                                                   \
            else VMP(x)->state->left += VMAP_INCRE_NUM;                         \
            VMP(x)->state->total += VMAP_INCRE_NUM;                             \
            memset(VMP(x)->start + VMP(x)->old, 0, VMP(x)->end - VMP(x)->old);  \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            _exit(-1);                                                          \
        }                                                                       \
    }                                                                           \
}while(0)
#define VMP_ROOT_MUNMAP(x)                                                      \
do                                                                              \
{                                                                               \
    if(x && VMP(x)->rsize > 0)                                                  \
    {                                                                           \
        if(VMP(x)->roots && VMP(x)->roots != (void *)-1)                        \
        {                                                                       \
            munmap(VMP(x)->roots, VMP(x)->rsize);                               \
            VMP(x)->roots = NULL;                                               \
        }                                                                       \
    }                                                                           \
}while(0)

#define VMP_ROOT_MMAP(x)                                                        \
do                                                                              \
{                                                                               \
    if(x)                                                                       \
    {                                                                           \
        if((VMP(x)->roots = (VMROOT *)mmap(NULL,VMP(x)->rsize,                  \
            PROT_READ|PROT_WRITE, MAP_SHARED, VMP(x)->rfd, 0)) == MAP_FAILED)   \
        {                                                                       \
            break;                                                              \
        }                                                                       \
    }                                                                           \
}while(0)

#define VMP_ROOT_INCRE(x)                                                       \
do                                                                              \
{                                                                               \
    if(x &&  VMP(x)->rend <  VMP(x)->rsize)                                     \
    {                                                                           \
        VMP(x)->rold = VMP(x)->rend;                                            \
        VMP(x)->rend += (off_t)VMROOT_INCRE_NUM * (off_t)sizeof(VMROOT);        \
        if(ftruncate(VMP(x)->rfd, VMP(x)->rend) == 0)                           \
        {                                                                       \
            VMP(x)->state->rleft += VMROOT_INCRE_NUM;                           \
            VMP(x)->state->rtotal += VMROOT_INCRE_NUM;                          \
            memset((char *)(VMP(x)->roots)+VMP(x)->rold,                        \
                    0, VMP(x)->rend - VMP(x)->rold);                            \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            _exit(-1);                                                          \
        }                                                                       \
    }                                                                           \
}while(0)

#define VMP_ROTATE_LEFT(x, prootid, oid, rid, lid, ppid)                        \
do                                                                              \
{                                                                               \
    if(x && (rid = VMP(x)->map[oid].right) > 0)                                 \
    {                                                                           \
        if((lid = VMP(x)->map[oid].right = VMP(x)->map[rid].left) > 0)          \
        {                                                                       \
            VMP(x)->map[lid].parent = oid;                                      \
        }                                                                       \
        if((ppid = VMP(x)->map[rid].parent = VMP(x)->map[oid].parent) > 0)      \
        {                                                                       \
            if(VMP(x)->map[ppid].left == oid)                                   \
                VMP(x)->map[ppid].left = rid;                                   \
            else                                                                \
                VMP(x)->map[ppid].right = rid;                                  \
        }else *prootid = rid;                                                   \
        VMP(x)->map[rid].left = oid;                                            \
        VMP(x)->map[oid].parent = rid;                                          \
    }                                                                           \
}while(0)

#define VMP_ROTATE_RIGHT(x, prootid, oid, lid, rid, ppid)                       \
do                                                                              \
{                                                                               \
    if(x && (lid = VMP(x)->map[oid].left) > 0)                                  \
    {                                                                           \
        if((rid = VMP(x)->map[oid].left = VMP(x)->map[lid].right) > 0)          \
        {                                                                       \
            VMP(x)->map[rid].parent = oid;                                      \
        }                                                                       \
        if((ppid = VMP(x)->map[lid].parent = VMP(x)->map[oid].parent) > 0)      \
        {                                                                       \
            if(VMP(x)->map[ppid].left == oid)                                   \
                VMP(x)->map[ppid].left = lid;                                   \
            else                                                                \
                VMP(x)->map[ppid].right = lid;                                  \
        }                                                                       \
        else *prootid = lid;                                                    \
        VMP(x)->map[lid].right = oid;                                           \
        VMP(x)->map[oid].parent = lid;                                          \
    }                                                                           \
}while(0)

#define VMP_INSERT_COLOR(x, prootid, oid, lid, rid, uid, pid, gpid, ppid)       \
do                                                                              \
{                                                                               \
    while((pid = VMP(x)->map[oid].parent)> 0                                    \
            && VMP(x)->map[pid].color == VMP_COLOR_RED)                         \
    {                                                                           \
        gpid = VMP(x)->map[pid].parent;                                         \
        if(pid == VMP(x)->map[gpid].left)                                       \
        {                                                                       \
            uid = VMP(x)->map[gpid].right;                                      \
            if(uid > 0 && VMP(x)->map[uid].color == VMP_COLOR_RED)              \
            {                                                                   \
                VMP(x)->map[uid].color = VMP_COLOR_BLACK;                       \
                VMP(x)->map[pid].color = VMP_COLOR_BLACK;                       \
                VMP(x)->map[gpid].color = VMP_COLOR_RED;                        \
                oid = gpid;                                                     \
                continue;                                                       \
            }                                                                   \
            if(VMP(x)->map[pid].right == oid)                                   \
            {                                                                   \
                VMP_ROTATE_LEFT(x, prootid, pid, rid, lid, ppid);               \
                uid = pid; pid = oid; oid = uid;                                \
            }                                                                   \
            VMP(x)->map[pid].color = VMP_COLOR_BLACK;                           \
            VMP(x)->map[gpid].color = VMP_COLOR_RED;                            \
            VMP_ROTATE_RIGHT(x, prootid, gpid, lid, rid, ppid);                 \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            uid = VMP(x)->map[gpid].left;                                       \
            if(uid > 0 && VMP(x)->map[uid].color == VMP_COLOR_RED)              \
            {                                                                   \
                VMP(x)->map[uid].color = VMP_COLOR_BLACK;                       \
                VMP(x)->map[pid].color = VMP_COLOR_BLACK;                       \
                VMP(x)->map[gpid].color = VMP_COLOR_RED;                        \
                oid = gpid;                                                     \
                continue;                                                       \
            }                                                                   \
            if(VMP(x)->map[pid].left == oid)                                    \
            {                                                                   \
                VMP_ROTATE_RIGHT(x, prootid, pid, lid, rid, ppid);              \
                uid = pid; pid = oid; oid = uid;                                \
            }                                                                   \
            VMP(x)->map[pid].color = VMP_COLOR_BLACK;                           \
            VMP(x)->map[gpid].color = VMP_COLOR_RED;                            \
            VMP_ROTATE_LEFT(x, prootid, gpid, rid, lid, ppid);                  \
        }                                                                       \
    }                                                                           \
    if(*prootid > 0)VMP(x)->map[*prootid].color = VMP_COLOR_BLACK;              \
}while(0)

#define VMP_REMOVE_COLOR(x, prootid, oid, xpid, lid, rid, uid, ppid)            \
do                                                                              \
{                                                                               \
    while((oid == 0 || VMP(x)->map[oid].color == VMP_COLOR_BLACK)               \
            && oid != *prootid)                                                 \
    {                                                                           \
        if(VMP(x)->map[xpid].left == oid)                                       \
        {                                                                       \
            uid = VMP(x)->map[xpid].right;                                      \
            if(VMP(x)->map[uid].color == VMP_COLOR_RED)                         \
            {                                                                   \
                VMP(x)->map[uid].color = VMP_COLOR_BLACK;                       \
                VMP(x)->map[xpid].color = VMP_COLOR_RED;                        \
                VMP_ROTATE_LEFT(x, prootid, xpid, rid, lid, ppid);              \
                uid = VMP(x)->map[xpid].right;                                  \
            }                                                                   \
            lid = VMP(x)->map[uid].left;                                        \
            rid = VMP(x)->map[uid].right;                                       \
            if((lid == 0 || VMP(x)->map[lid].color == VMP_COLOR_BLACK)          \
                && (rid == 0 || VMP(x)->map[rid].color == VMP_COLOR_BLACK))     \
            {                                                                   \
                VMP(x)->map[uid].color = VMP_COLOR_RED;                         \
                oid = xpid;                                                     \
                xpid = VMP(x)->map[oid].parent;                                 \
            }                                                                   \
            else                                                                \
            {                                                                   \
                rid = VMP(x)->map[uid].right;                                   \
                lid = VMP(x)->map[uid].left;                                    \
                if(rid == 0 || VMP(x)->map[rid].color == VMP_COLOR_BLACK)       \
                {                                                               \
                    if(lid > 0)VMP(x)->map[lid].color = VMP_COLOR_BLACK;        \
                    VMP(x)->map[uid].color = VMP_COLOR_RED;                     \
                    VMP_ROTATE_RIGHT(x, prootid, uid, lid, rid, ppid);          \
                    uid = VMP(x)->map[xpid].right;                              \
                }                                                               \
                VMP(x)->map[uid].color = VMP(x)->map[xpid].color;               \
                VMP(x)->map[xpid].color = VMP_COLOR_BLACK;                      \
                if((rid = VMP(x)->map[uid].right) > 0)                          \
                    VMP(x)->map[rid].color = VMP_COLOR_BLACK;                   \
                VMP_ROTATE_LEFT(x, prootid, xpid, rid, lid, ppid);              \
                oid = *prootid;                                                 \
                break;                                                          \
            }                                                                   \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            uid = VMP(x)->map[xpid].left;                                       \
            if(VMP(x)->map[uid].color == VMP_COLOR_RED)                         \
            {                                                                   \
                VMP(x)->map[uid].color = VMP_COLOR_BLACK;                       \
                VMP(x)->map[xpid].color = VMP_COLOR_RED;                        \
                VMP_ROTATE_RIGHT(x, prootid, xpid, lid, rid, ppid);             \
                uid = VMP(x)->map[xpid].left;                                   \
            }                                                                   \
            lid = VMP(x)->map[uid].left;                                        \
            rid = VMP(x)->map[uid].right;                                       \
            if((lid == 0 || VMP(x)->map[lid].color == VMP_COLOR_BLACK)          \
                && (rid == 0 || VMP(x)->map[rid].color == VMP_COLOR_BLACK))     \
            {                                                                   \
                VMP(x)->map[uid].color = VMP_COLOR_RED;                         \
                oid = xpid;                                                     \
                xpid = VMP(x)->map[oid].parent;                                 \
            }                                                                   \
            else                                                                \
            {                                                                   \
                rid = VMP(x)->map[uid].right;                                   \
                lid = VMP(x)->map[uid].left;                                    \
                if(lid == 0 || VMP(x)->map[lid].color == VMP_COLOR_BLACK)       \
                {                                                               \
                    if(rid > 0)VMP(x)->map[rid].color = VMP_COLOR_BLACK;        \
                    VMP(x)->map[uid].color = VMP_COLOR_RED;                     \
                    VMP_ROTATE_LEFT(x, prootid, uid, rid, lid, ppid);           \
                    uid = VMP(x)->map[xpid].left;                               \
                }                                                               \
                VMP(x)->map[uid].color = VMP(x)->map[xpid].color;               \
                VMP(x)->map[xpid].color = VMP_COLOR_BLACK;                      \
                if((lid = VMP(x)->map[uid].left) > 0)                           \
                    VMP(x)->map[lid].color = VMP_COLOR_BLACK;                   \
                VMP_ROTATE_RIGHT(x, prootid, xpid, lid, rid, ppid);             \
                oid = *prootid;                                                 \
                break;                                                          \
            }                                                                   \
        }                                                                       \
    }                                                                           \
    if(oid > 0) VMP(x)->map[oid].color = VMP_COLOR_BLACK;                       \
}while(0)

/* mkdir force */
int vmap_mkdir(char *path)
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

/* init vmap */
void *vmap_init(char *basedir)
{
    struct stat  st = {0};
    void *x = NULL;
    char path[256];
    int n = 0;

    if(basedir && (x = (VMAP *)calloc(1, sizeof(VMAP))))
    {
        MUTEX_INIT(VMP(x)->mutex);
        n = sprintf(path, "%s/vmap.map", basedir); 
        vmap_mkdir(path);
        if((VMP(x)->fd = open(path, O_CREAT|O_RDWR, 0644)) > 0 
                && fstat(VMP(x)->fd, &st) == 0)
        {
            VMP(x)->end = st.st_size;
            VMP(x)->size = (off_t)sizeof(VMSTATE) + (off_t)sizeof(VMNODE) * (off_t)VMAP_NODES_MAX;
            //mmap
            VMP_MMAP(x);
            //init truncate
            if(st.st_size == 0)
            {
                VMP(x)->end = (off_t)sizeof(VMSTATE);
                VMP_INCRE(x);
            }
        }
        else 
        {
            if(VMP(x)->fd > 0) close(VMP(x)->fd);
            free(x);
            x = NULL;
            _exit(-1);
        }
        n = sprintf(path, "%s/vmap.root", basedir); 
        if((VMP(x)->rfd = open(path, O_CREAT|O_RDWR, 0644)) > 0 
                && fstat(VMP(x)->rfd, &st) == 0)
        {
            VMP(x)->rend = st.st_size;
            VMP(x)->rsize = (off_t)sizeof(VMROOT) * (off_t)VMAP_ROOT_MAX;
            //mmap
            VMP_ROOT_MMAP(x);
            //init truncate
            if(st.st_size == 0)
            {
                VMP_ROOT_INCRE(x);
            }
        }
        else 
        {
            if(VMP(x)->rfd > 0) close(VMP(x)->rfd);
            free(x);
            x = NULL;
            _exit(-1);
        }
    }
    return x;
}

/* insert new root */
int vmap_new_tree(void *x)
{
    int id = 0;
    if(x)
    {
        MUTEX_LOCK(VMP(x)->mutex);
        if(VMP(x)->state->rleft == 0)
        {
            VMP_ROOT_INCRE(x);
        }
        if(VMP(x)->state->qrleft > 0)
        {
            id = VMP(x)->state->qrfirst;
            VMP(x)->state->qrfirst = VMP(x)->roots[id].total;
            VMP(x)->state->qrleft--;
        }
        else
        {
            id = ++(VMP(x)->state->rcurrent);
        }
        VMP(x)->state->rleft--;
        VMP(x)->roots[id].status = 1;
        MUTEX_UNLOCK(VMP(x)->mutex);
    }
    return id;
}

/* total */
unsigned int vmap_total(void *x, int rootid)
{
    unsigned int total = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(VMP(x)->mutex);
        if(VMP(x)->state && VMP(x)->map && rootid < VMAP_ROOT_MAX)
        {
            total =  VMP(x)->roots[rootid].total;
        }
        MUTEX_UNLOCK(VMP(x)->mutex);
    }
    return total;
}

/* insert new node */
unsigned int vmap_insert(void *x, int rootid, int key, int data, int *old)
{
    unsigned int id = 0, nodeid = 0, rid = 0, lid = 0, uid = 0, pid = 0, 
        gpid = 0, ppid = 0, *prootid = NULL;
    VMNODE *node = NULL;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(VMP(x)->mutex);
        if(VMP(x)->state && VMP(x)->map && rootid < VMAP_ROOT_MAX
                && VMP(x)->roots[rootid].status > 0)
        {
            nodeid = VMP(x)->roots[rootid].rootid;
            while(nodeid > 0 && nodeid < VMP(x)->state->total)
            {
                node = &(VMP(x)->map[nodeid]);
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
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, VMP(x)->start, VMP(x)->state, VMP(x)->map, VMP(x)->state->current, VMP(x)->state->left, VMP(x)->state->total, VMP(x)->state->qleft, VMP(x)->state->qfirst, VMP(x)->state->qlast);
                if(VMP(x)->state->left == 0)
                {
                    VMP_INCRE(x);
                }
                if(VMP(x)->state->qleft > 0)
                {
                    id = VMP(x)->state->qfirst;
                    VMP(x)->state->qfirst = VMP(x)->map[id].parent;
                    VMP(x)->state->qleft--;
                }
                else
                {
                    id = ++(VMP(x)->state->current);
                }
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, VMP(x)->start, VMP(x)->state, VMP(x)->map, VMP(x)->state->current, VMP(x)->state->left, VMP(x)->state->total, VMP(x)->state->qleft, VMP(x)->state->qfirst, VMP(x)->state->qlast);
                VMP(x)->state->left--;
                //memset(&(VMP(x)->map[id]), 0, sizeof(VMNODE));
                VMP(x)->map[id].parent = nodeid;
                VMP(x)->map[id].key = key;
                VMP(x)->map[id].data = data;
                VMP_MIN_MAX(x, id, key);
                if(nodeid > 0)
                {
                    if(key > VMP(x)->map[nodeid].key) 
                        VMP(x)->map[nodeid].right = id;
                    else
                        VMP(x)->map[nodeid].left = id;
                }
                VMP(x)->roots[rootid].total++;
            }
            else
            {
                //fprintf(stdout, "%s::%d old id:%d pid:%d key:%d\n", __FILE__, __LINE__, id, parentid, key);
            }
        }
        if((nodeid = id) > 0)
        {
            if(VMP(x)->roots[rootid].rootid > 0)
            {
                prootid = &(VMP(x)->roots[rootid].rootid);
                VMP(x)->map[nodeid].color = VMP_COLOR_RED;
                VMP_INSERT_COLOR(x, prootid, nodeid, lid, rid, uid, pid, gpid, ppid);
            }
            else
            {
                VMP(x)->roots[rootid].rootid = nodeid;
            }
        }
end:
        MUTEX_UNLOCK(VMP(x)->mutex);
    }
    return id;
}

/* try insert  node */
unsigned int vmap_try_insert(void *x, int rootid, int key, int data, int *old)
{
    unsigned int id = 0, nodeid = 0, rid = 0, lid = 0, uid = 0, pid = 0, 
        gpid = 0, ppid = 0, *prootid = NULL;
    VMNODE *node = NULL;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(VMP(x)->mutex);
        if(VMP(x)->state && VMP(x)->map && rootid < VMAP_ROOT_MAX
                && VMP(x)->roots[rootid].status > 0)
        {
            nodeid = VMP(x)->roots[rootid].rootid;
            while(nodeid > 0 && nodeid < VMP(x)->state->total)
            {
                node = &(VMP(x)->map[nodeid]);
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
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, VMP(x)->start, VMP(x)->state, VMP(x)->map, VMP(x)->state->current, VMP(x)->state->left, VMP(x)->state->total, VMP(x)->state->qleft, VMP(x)->state->qfirst, VMP(x)->state->qlast);
                if(VMP(x)->state->left == 0)
                {
                    VMP_INCRE(x);
                }
                if(VMP(x)->state->qleft > 0)
                {
                    id = VMP(x)->state->qfirst;
                    VMP(x)->state->qfirst = VMP(x)->map[id].parent;
                    VMP(x)->state->qleft--;
                }
                else
                {
                    id = ++(VMP(x)->state->current);
                }
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, VMP(x)->start, VMP(x)->state, VMP(x)->map, VMP(x)->state->current, VMP(x)->state->left, VMP(x)->state->total, VMP(x)->state->qleft, VMP(x)->state->qfirst, VMP(x)->state->qlast);
                VMP(x)->state->left--;
                //memset(&(VMP(x)->map[id]), 0, sizeof(VMNODE));
                VMP(x)->map[id].parent = nodeid;
                VMP(x)->map[id].key = key;
                VMP(x)->map[id].data = data;
                VMP_MIN_MAX(x, id, key);
                if(nodeid > 0)
                {
                    if(key > VMP(x)->map[nodeid].key) 
                        VMP(x)->map[nodeid].right = id;
                    else
                        VMP(x)->map[nodeid].left = id;
                }
                VMP(x)->roots[rootid].total++;
            }
            else
            {
                //fprintf(stdout, "%s::%d old id:%d pid:%d key:%d\n", __FILE__, __LINE__, id, parentid, key);
            }
        }
        if((nodeid = id) > 0)
        {
            if(VMP(x)->roots[rootid].rootid > 0)
            {
                prootid = &(VMP(x)->roots[rootid].rootid);
                VMP(x)->map[nodeid].color = VMP_COLOR_RED;
                VMP_INSERT_COLOR(x, prootid, nodeid, lid, rid, uid, pid, gpid, ppid);
            }
            else
            {
                VMP(x)->roots[rootid].rootid = nodeid;
            }
        }
end:
        MUTEX_UNLOCK(VMP(x)->mutex);
    }
    return id;
}


/* get node key/data */
unsigned int vmap_get(void *x, unsigned int tnodeid, int *key, int *data)
{
    unsigned int id = 0;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(VMP(x)->mutex);
        if(VMP(x)->map && VMP(x)->state && tnodeid <  VMP(x)->state->total)
        {
            if(key) *key = VMP(x)->map[tnodeid].key;
            if(data) *data = VMP(x)->map[tnodeid].data;
            id = tnodeid;
        }
        MUTEX_UNLOCK(VMP(x)->mutex);
    }
    return id;
}

/* find key/data */
unsigned int vmap_find(void *x, int rootid, int key, int *data)
{
    unsigned int id = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(VMP(x)->mutex);
        if(VMP(x)->map && VMP(x)->state && rootid < VMAP_ROOT_MAX
                && VMP(x)->roots[rootid].status > 0)
        {
            //fprintf(stdout, "%s::%d rootid:%d key:%d data:%d total:%d\n", __FILE__, __LINE__, rootid, key, *data, VMP(x)->state->total);
            id = VMP(x)->roots[rootid].rootid;
            while(id > 0 && id < VMP(x)->state->total)
            {
                if(key == VMP(x)->map[id].key)
                {
                    if(data) *data = VMP(x)->map[id].data;
                    break;
                }
                else if(key > VMP(x)->map[id].key)
                {
                    id = VMP(x)->map[id].right;
                }
                else
                {
                    id = VMP(x)->map[id].left;
                }
            }
        }
        //fprintf(stdout, "%s::%d rootid:%d key:%d data:%d\n", __FILE__, __LINE__, rootid, key, *data);
        MUTEX_UNLOCK(VMP(x)->mutex);
    }
    return id;
}

/* get tree->min key/data */
unsigned int vmap_min(void *x, int rootid, int *key, int *data)
{
    unsigned int id = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(VMP(x)->mutex);
        if(VMP(x)->map && VMP(x)->state && rootid <  VMAP_ROOT_MAX
                && VMP(x)->roots[rootid].status > 0)
        {
            id = VMP(x)->roots[rootid].rootid;
            while(VMP(x)->map[id].left > 0)
            {
                id = VMP(x)->map[id].left;
            }
            if(id > 0 && VMP(x)->state->total)
            {
                if(key) *key = VMP(x)->map[id].key;
                if(data) *data = VMP(x)->map[id].data;
            }
        }
        MUTEX_UNLOCK(VMP(x)->mutex);
    }
    return id;
}

/* get tree->max key/data */
unsigned  int vmap_max(void *x, int rootid, int *key, int *data)
{
    unsigned int id = 0, tmp = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(VMP(x)->mutex);
        if(VMP(x)->map && VMP(x)->state && rootid <  VMAP_ROOT_MAX
                && VMP(x)->roots[rootid].status > 0)
        {
            tmp = VMP(x)->roots[rootid].rootid;
            do
            {
                id = tmp;
            }while(id > 0 && (tmp = VMP(x)->map[id].right) > 0);
            if(id > 0 && VMP(x)->state->total)
            {
                if(key) *key = VMP(x)->map[id].key;
                if(data) *data = VMP(x)->map[id].data;
            }
        }
        MUTEX_UNLOCK(VMP(x)->mutex);
    }
    return id;
}

/* get next node key/data */
unsigned int vmap_next(void *x, int rootid, unsigned int tnodeid, int *key, int *data)
{
    unsigned int id = 0, parentid = 0;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(VMP(x)->mutex);
        if(VMP(x)->map && VMP(x)->state && tnodeid <  VMP(x)->state->total)
        {
            id = tnodeid;
            if(VMP(x)->map[id].right > 0)
            {
                id = VMP(x)->map[id].right;
                while(VMP(x)->map[id].left  > 0)
                {
                    id = VMP(x)->map[id].left;
                }
                //fprintf(stdout, "%s::%d id:%d\n", __FILE__, __LINE__, id);
            }
            else
            {
                while(id > 0)
                {
                    parentid = VMP(x)->map[id].parent;
                    if(VMP(x)->map[id].key < VMP(x)->map[parentid].key)
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
            if(id > 0 && id < VMP(x)->state->total)
            {
                if(key) *key = VMP(x)->map[id].key;
                if(data) *data = VMP(x)->map[id].data;
            }
            //fprintf(stdout, "%s::%d rootid:%d tnodeid:%d id:%d\n",__FILE__, __LINE__, rootid, tnodeid, id);
        }
        MUTEX_UNLOCK(VMP(x)->mutex);
    }
    return id;
}

/* get prev node key/data */
unsigned int vmap_prev(void *x, int rootid, unsigned int tnodeid, int *key, int *data)
{
    unsigned int id = 0, parentid = 0;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(VMP(x)->mutex);
        if(VMP(x)->map && VMP(x)->state && tnodeid <  VMP(x)->state->total)
        {
            id = tnodeid;
            if(VMP(x)->map[id].left > 0)
            {
                id = VMP(x)->map[id].left;
                while(VMP(x)->map[id].right  > 0)
                {
                    id = VMP(x)->map[id].right;
                }
                //fprintf(stdout, "%s::%d id:%d\n", __FILE__, __LINE__, id);
            }
            else
            {
                while(id > 0)
                {
                    parentid = VMP(x)->map[id].parent;
                    if(VMP(x)->map[id].key > VMP(x)->map[parentid].key)
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
            if(id > 0 && id < VMP(x)->state->total)
            {
                if(key)*key = VMP(x)->map[id].key;
                if(data)*data = VMP(x)->map[id].data;
            }
            //fprintf(stdout, "%s::%d rootid:%d tnodeid:%d id:%d\n",__FILE__, __LINE__, rootid, tnodeid, id);
        }
        MUTEX_UNLOCK(VMP(x)->mutex);
    }
    return id;
}

/* view node */
void vmap_view_tnode(void *x, unsigned int tnodeid, FILE *fp)
{
    if(x)
    {
        if(VMP(x)->map[tnodeid].left > 0 && VMP(x)->map[tnodeid].left < VMP(x)->state->total)
        {
            vmap_view_tnode(x, VMP(x)->map[tnodeid].left, fp);
        }
        fprintf(fp, "[%d:%lld:%d]\n", tnodeid, (long long)VMP(x)->map[tnodeid].key, VMP(x)->map[tnodeid].data);
        if(VMP(x)->map[tnodeid].right > 0 && VMP(x)->map[tnodeid].right < VMP(x)->state->total)
        {
            vmap_view_tnode(x, VMP(x)->map[tnodeid].right, fp);
        }
    }
    return ;
}

void vmap_view_tree(void *x, int rootid, FILE *fp)
{
    if(x && rootid > 0)
    {
        MUTEX_LOCK(VMP(x)->mutex);
        if(VMP(x)->map && VMP(x)->state && rootid < VMAP_ROOT_MAX)
        {
            fprintf(stdout, "%s::%d rootid:%d\n", __FILE__, __LINE__, VMP(x)->roots[rootid].rootid);
             vmap_view_tnode(x, VMP(x)->roots[rootid].rootid, fp);
        }
        MUTEX_UNLOCK(VMP(x)->mutex);
    }
    return ;
}

/* set data */
int vmap_set_data(void *x, unsigned int tnodeid, int data)
{
    int old = -1;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(VMP(x)->mutex);
        if(VMP(x)->map && VMP(x)->state && tnodeid < VMP(x)->state->total)
        {
            old = VMP(x)->map[tnodeid].data;
            VMP(x)->map[tnodeid].data = data;
        }
        MUTEX_UNLOCK(VMP(x)->mutex);
    }
    return old;
}

/* remove node */
void vmap_remove(void *x, int rootid, unsigned int tnodeid, int *key, int *data)
{
    unsigned int id = 0, pid = 0, parent = 0, child = 0, rid = 0, lid = 0,
        uid = 0, ppid = 0, z = 0, color = 0, *prootid = NULL;

    if(x && rootid > 0 && tnodeid > 0)
    {
        MUTEX_LOCK(VMP(x)->mutex);
        if(VMP(x)->map && VMP(x)->state && tnodeid < VMP(x)->state->total)
        {
            if(key) *key = VMP(x)->map[tnodeid].key;
            if(data) *data = VMP(x)->map[tnodeid].data;
            id = tnodeid;
            if(VMP(x)->map[tnodeid].left == 0)
            {
                child = VMP(x)->map[tnodeid].right;
            }
            else if(VMP(x)->map[tnodeid].right == 0)
            {
                child = VMP(x)->map[tnodeid].left;
            }
            else 
            {
                id = VMP(x)->map[tnodeid].right;
                while(VMP(x)->map[id].left > 0)
                    id = VMP(x)->map[id].left;
                parent = VMP(x)->map[id].parent;
                color = VMP(x)->map[id].color;
                if((child = VMP(x)->map[id].right) > 0)
                    VMP(x)->map[child].parent = parent;
                if((pid = parent) > 0)
                {
                    if(VMP(x)->map[pid].left == id)
                        VMP(x)->map[pid].left = child;
                    else
                        VMP(x)->map[pid].right = child;
                }
                else
                {
                    VMP(x)->roots[rootid].rootid = child;
                }
                if(VMP(x)->map[id].parent == tnodeid) parent = id;
                VMP(x)->map[id].color = VMP(x)->map[tnodeid].color;
                VMP(x)->map[id].parent = VMP(x)->map[tnodeid].parent;
                VMP(x)->map[id].left = VMP(x)->map[tnodeid].left;
                VMP(x)->map[id].right = VMP(x)->map[tnodeid].right;
                if((pid = VMP(x)->map[tnodeid].parent) > 0)
                {
                    if(VMP(x)->map[pid].left == tnodeid)
                        VMP(x)->map[pid].left = id;
                    else
                        VMP(x)->map[pid].right = id;
                }
                else
                {
                    VMP(x)->roots[rootid].rootid = id;
                }
                lid = VMP(x)->map[tnodeid].left;
                VMP(x)->map[lid].parent = id;
                if((rid = VMP(x)->map[tnodeid].right) > 0)
                    VMP(x)->map[rid].parent = id;
                goto color_remove;
            }
            parent =  VMP(x)->map[tnodeid].parent;
            color = VMP(x)->map[tnodeid].color;
            if(child > 0) 
            {
                VMP(x)->map[child].parent = parent;
            }
            if((pid = parent) > 0)
            {
                if(tnodeid == VMP(x)->map[pid].left) 
                    VMP(x)->map[pid].left = child;
                else 
                    VMP(x)->map[pid].right = child;
            }
            else 
            {
                VMP(x)->roots[rootid].rootid = child;
            }
            //remove color set
color_remove:
            VMP(x)->roots[rootid].total--;
            if(color == VMP_COLOR_BLACK)
            {
                //fprintf(stdout, "%s::%d node:%d parent:%d left:%d right:%d key:%d data:%d\n", __FILE__, __LINE__, tnodeid, VMP(x)->map[tnodeid].parent, VMP(x)->map[tnodeid].left, VMP(x)->map[tnodeid].right, VMP(x)->map[tnodeid].key, VMP(x)->map[tnodeid].data);
                prootid = &(VMP(x)->roots[rootid].rootid);
                VMP_REMOVE_COLOR(x, prootid, child, parent, lid, rid, uid, ppid);
            }
            //add to qleft
            memset(&(VMP(x)->map[tnodeid]), 0, sizeof(VMNODE));
            if(VMP(x)->state->qleft == 0)
            {
                VMP(x)->state->qfirst = VMP(x)->state->qlast = tnodeid;
            }
            else
            {
                z = VMP(x)->state->qlast;
                VMP(x)->map[z].parent = tnodeid;
                VMP(x)->state->qlast = tnodeid;
            }
            VMP(x)->state->qleft++;
            VMP(x)->state->left++;
            //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, VMP(x)->start, VMP(x)->state, VMP(x)->map, VMP(x)->state->current, VMP(x)->state->left, VMP(x)->state->total, VMP(x)->state->qleft, VMP(x)->state->qfirst, VMP(x)->state->qlast);
  
        }
        MUTEX_UNLOCK(VMP(x)->mutex);
    }
    return ;
}

/* remove node */
void vmap_remove_tnode(void *x, unsigned int tnodeid)
{
    unsigned int id = 0;

    if(x)
    {
        if(VMP(x)->map[tnodeid].left > 0 && VMP(x)->map[tnodeid].left < VMP(x)->state->total)
        {
            vmap_remove_tnode(x, VMP(x)->map[tnodeid].left);
        }
        if(VMP(x)->map[tnodeid].right > 0 && VMP(x)->map[tnodeid].right < VMP(x)->state->total)
        {
            vmap_remove_tnode(x, VMP(x)->map[tnodeid].right);
        }
        memset(&(VMP(x)->map[tnodeid]), 0, sizeof(VMNODE));
        if(VMP(x)->state->qleft == 0)
        {
            VMP(x)->state->qfirst = VMP(x)->state->qlast = tnodeid;
        }
        else
        {
            id = VMP(x)->state->qlast;
            VMP(x)->map[id].parent = tnodeid;
            VMP(x)->state->qlast = tnodeid;
        }
        VMP(x)->state->qleft++;
        VMP(x)->state->left++;
    }
    return ;
}

/* remove tree */
void vmap_remove_tree(void *x, int rootid)
{
    int id = 0;

    if(x && rootid > 0 && rootid < VMAP_ROOT_MAX)
    {
        MUTEX_LOCK(VMP(x)->mutex);
        vmap_remove_tnode(x, VMP(x)->roots[rootid].rootid);
        memset(&(VMP(x)->roots[rootid]), 0, sizeof(VMROOT));
        if(VMP(x)->state->qrleft == 0)
        {
            VMP(x)->state->qrfirst = VMP(x)->state->qrlast = rootid;
        }
        else
        {
            id = VMP(x)->state->qrlast;
            VMP(x)->roots[id].total = rootid;
            VMP(x)->state->qrlast = rootid;
        }
        VMP(x)->state->qrleft++;
        VMP(x)->state->rleft++;
        MUTEX_UNLOCK(VMP(x)->mutex);
    }
    return ;
}

//close vmap
void vmap_close(void *x)
{
    if(x)
    {
        //fprintf(stdout, "%s::%d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d sizeof(VMSTATE):%d\n", __FILE__, __LINE__, VMP(x)->start, VMP(x)->state, VMP(x)->map, VMP(x)->state->current, VMP(x)->state->left, VMP(x)->state->total, VMP(x)->state->qleft, VMP(x)->state->qfirst, VMP(x)->state->qlast, sizeof(VMSTATE));
        VMP_MUNMAP(x);
        if(VMP(x)->fd) close(VMP(x)->fd);
        VMP_ROOT_MUNMAP(x);
        if(VMP(x)->rfd) close(VMP(x)->rfd);
        MUTEX_DESTROY(VMP(x)->mutex);
        free(x);
    }
}


#ifdef _DEBUG_VMAP
#include "md5.h"
#include "timer.h"
int main(int argc, char **argv) 
{
    int i = 0, rootid = 0, id = 0, j = 0, old = 0, data = 0, n = 0, count = 50000000;
    unsigned char digest[MD5_LEN];
    void *vmap = NULL;
    void *timer = NULL;
    char line[1024];
    int key = 0;

    if((vmap = vmap_init("/tmp/test.vmap")))
    {
        rootid = vmap_new_tree(vmap);
        TIMER_INIT(timer);
        for(j = 1; j <= count; j++)
        {
            n = sprintf(line, "http://www.demo.com/%d.html", j);
            md5(line, n, digest);
            key = *((int *)digest);
            old = -1;
            data = j;
            id = vmap_insert(vmap, rootid, key, data, &old);
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
            id = vmap_try_insert(vmap, rootid, key, data, &old);
            if(old > 0 && old != j) 
            {
                fprintf(stdout, "%d:{id:%d key:%d rootid:%d old:%d}\n", j, id, key, rootid, old);
                _exit(-1);
            }
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "%s::%d try_insert:%d time:%lld\n", __FILE__,__LINE__, count, PT_LU_USEC(timer));
        TIMER_CLEAN(timer);
        vmap_close(vmap);
    }
}
//gcc -o vmap vmap.c md5.c -D_DEBUG_VMAP -g && ./vmap
#endif
