#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include "qmap.h"
#include "mutex.h"
#ifdef MAP_LOCKED
#define MMAP_SHARED MAP_SHARED|MAP_LOCKED
#else
#define MMAP_SHARED MAP_SHARED
#endif
#define QMP(px) ((QMAP *)px)
#define QMP_COLOR_BLACK  0
#define QMP_COLOR_RED    1
#define QMP_MIN_MAX(x, key, xid)                                                \
do                                                                              \
{                                                                               \
    if(QMP(x) && QMP(x)->state)                                                 \
    {                                                                           \
        if(QMP(x)->state->count == 0)                                           \
        {                                                                       \
            QMP(x)->state->nmin = QMP(x)->state->nmax = xid;                    \
            QMP(x)->state->kmin = QMP(x)->state->kmax = key;                    \
        }                                                                       \
        else if(key > QMP(x)->state->kmax)                                      \
        {                                                                       \
            QMP(x)->state->nmax = xid;                                          \
            QMP(x)->state->kmax = key;                                          \
        }                                                                       \
        else if(key < QMP(x)->state->kmin)                                      \
        {                                                                       \
            QMP(x)->state->nmin = xid;                                          \
            QMP(x)->state->kmin = key;                                          \
        }                                                                       \
    }                                                                           \
}while(0)
#define QMP_MUNMAP(x)                                                           \
do                                                                              \
{                                                                               \
    if(x && QMP(x)->size > 0)                                                   \
    {                                                                           \
        if(QMP(x)->start && QMP(x)->start != (void *)-1)                        \
        {                                                                       \
            munmap(QMP(x)->start, QMP(x)->size);                                \
            QMP(x)->start = NULL;                                               \
            QMP(x)->state = NULL;                                               \
            QMP(x)->map = NULL;                                                 \
        }                                                                       \
    }                                                                           \
}while(0)

#define QMP_MMAP(x)                                                             \
do                                                                              \
{                                                                               \
    if(x)                                                                       \
    {                                                                           \
        if((QMP(x)->start = (char*)mmap(NULL,QMP(x)->size,PROT_READ|PROT_WRITE, \
                    MAP_SHARED, QMP(x)->fd, 0)) != (void *)-1)                  \
        {                                                                       \
            QMP(x)->state = (QMSTATE *)QMP(x)->start;                           \
            QMP(x)->map = (QMNODE *)(QMP(x)->start + sizeof(QMSTATE));          \
        }                                                                       \
    }                                                                           \
}while(0)

#define QMP_INCRE(x)                                                            \
do                                                                              \
{                                                                               \
    if(x &&  QMP(x)->end <  QMP(x)->size)                                       \
    {                                                                           \
        QMP(x)->old = QMP(x)->end ;                                             \
        QMP(x)->end += (off_t)QMAP_INCRE_NUM * (off_t)sizeof(QMNODE);           \
        if(ftruncate(QMP(x)->fd, QMP(x)->end) == 0)                             \
        {                                                                       \
            if(QMP(x)->old == sizeof(QMSTATE))                                  \
            {                                                                   \
                memset(QMP(x)->state, 0, sizeof(QMSTATE));                      \
                QMP(x)->state->left += QMAP_INCRE_NUM - 1;                      \
            }                                                                   \
            else QMP(x)->state->left += QMAP_INCRE_NUM;                         \
            QMP(x)->state->total += QMAP_INCRE_NUM;                             \
            memset(QMP(x)->start + QMP(x)->old, 0, QMP(x)->end - QMP(x)->old);  \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            _exit(-1);                                                          \
        }                                                                       \
    }                                                                           \
}while(0)
#define QMP_ROOT_MUNMAP(x)                                                      \
do                                                                              \
{                                                                               \
    if(x && QMP(x)->rsize > 0)                                                  \
    {                                                                           \
        if(QMP(x)->roots && QMP(x)->roots != (void *)-1)                        \
        {                                                                       \
            munmap(QMP(x)->roots, QMP(x)->rsize);                               \
            QMP(x)->roots = NULL;                                               \
        }                                                                       \
    }                                                                           \
}while(0)

#define QMP_ROOT_MMAP(x)                                                        \
do                                                                              \
{                                                                               \
    if(x)                                                                       \
    {                                                                           \
        if((QMP(x)->roots = (QMROOT *)mmap(NULL,QMP(x)->rsize,                  \
            PROT_READ|PROT_WRITE, MAP_SHARED, QMP(x)->rfd, 0)) == MAP_FAILED)   \
        {                                                                       \
            break;                                                              \
        }                                                                       \
    }                                                                           \
}while(0)

#define QMP_ROOT_INCRE(x)                                                       \
do                                                                              \
{                                                                               \
    if(x &&  QMP(x)->rend <  QMP(x)->rsize)                                     \
    {                                                                           \
        QMP(x)->rold = QMP(x)->rend;                                            \
        QMP(x)->rend += (off_t)QMROOT_INCRE_NUM * (off_t)sizeof(QMROOT);        \
        if(ftruncate(QMP(x)->rfd, QMP(x)->rend) == 0)                           \
        {                                                                       \
            QMP(x)->state->rleft += QMROOT_INCRE_NUM;                           \
            if(QMP(x)->rold == 0) QMP(x)->state->rleft--;                       \
            QMP(x)->state->rtotal += QMROOT_INCRE_NUM;                          \
            memset((char *)(QMP(x)->roots)+QMP(x)->rold,                        \
                    0, QMP(x)->rend - QMP(x)->rold);                            \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            _exit(-1);                                                          \
        }                                                                       \
    }                                                                           \
}while(0)

#define QMP_ROTATE_LEFT(x, prootid, oid, rid, lid, ppid)                        \
do                                                                              \
{                                                                               \
    if(x && (rid = QMP(x)->map[oid].right) > 0)                                 \
    {                                                                           \
        if((lid = QMP(x)->map[oid].right = QMP(x)->map[rid].left) > 0)          \
        {                                                                       \
            QMP(x)->map[lid].parent = oid;                                      \
        }                                                                       \
        if((ppid = QMP(x)->map[rid].parent = QMP(x)->map[oid].parent) > 0)      \
        {                                                                       \
            if(QMP(x)->map[ppid].left == oid)                                   \
                QMP(x)->map[ppid].left = rid;                                   \
            else                                                                \
                QMP(x)->map[ppid].right = rid;                                  \
        }else *prootid = rid;                                                   \
        QMP(x)->map[rid].left = oid;                                            \
        QMP(x)->map[oid].parent = rid;                                          \
    }                                                                           \
}while(0)

#define QMP_ROTATE_RIGHT(x, prootid, oid, lid, rid, ppid)                       \
do                                                                              \
{                                                                               \
    if(x && (lid = QMP(x)->map[oid].left) > 0)                                  \
    {                                                                           \
        if((rid = QMP(x)->map[oid].left = QMP(x)->map[lid].right) > 0)          \
        {                                                                       \
            QMP(x)->map[rid].parent = oid;                                      \
        }                                                                       \
        if((ppid = QMP(x)->map[lid].parent = QMP(x)->map[oid].parent) > 0)      \
        {                                                                       \
            if(QMP(x)->map[ppid].left == oid)                                   \
                QMP(x)->map[ppid].left = lid;                                   \
            else                                                                \
                QMP(x)->map[ppid].right = lid;                                  \
        }                                                                       \
        else *prootid = lid;                                                    \
        QMP(x)->map[lid].right = oid;                                           \
        QMP(x)->map[oid].parent = lid;                                          \
    }                                                                           \
}while(0)

#define QMP_INSERT_COLOR(x, prootid, oid, lid, rid, uid, pid, gpid, ppid)       \
do                                                                              \
{                                                                               \
    while((pid = QMP(x)->map[oid].parent)> 0                                    \
            && QMP(x)->map[pid].color == QMP_COLOR_RED)                         \
    {                                                                           \
        gpid = QMP(x)->map[pid].parent;                                         \
        if(pid == QMP(x)->map[gpid].left)                                       \
        {                                                                       \
            uid = QMP(x)->map[gpid].right;                                      \
            if(uid > 0 && QMP(x)->map[uid].color == QMP_COLOR_RED)              \
            {                                                                   \
                QMP(x)->map[uid].color = QMP_COLOR_BLACK;                       \
                QMP(x)->map[pid].color = QMP_COLOR_BLACK;                       \
                QMP(x)->map[gpid].color = QMP_COLOR_RED;                        \
                oid = gpid;                                                     \
                continue;                                                       \
            }                                                                   \
            if(QMP(x)->map[pid].right == oid)                                   \
            {                                                                   \
                QMP_ROTATE_LEFT(x, prootid, pid, rid, lid, ppid);               \
                uid = pid; pid = oid; oid = uid;                                \
            }                                                                   \
            QMP(x)->map[pid].color = QMP_COLOR_BLACK;                           \
            QMP(x)->map[gpid].color = QMP_COLOR_RED;                            \
            QMP_ROTATE_RIGHT(x, prootid, gpid, lid, rid, ppid);                 \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            uid = QMP(x)->map[gpid].left;                                       \
            if(uid > 0 && QMP(x)->map[uid].color == QMP_COLOR_RED)              \
            {                                                                   \
                QMP(x)->map[uid].color = QMP_COLOR_BLACK;                       \
                QMP(x)->map[pid].color = QMP_COLOR_BLACK;                       \
                QMP(x)->map[gpid].color = QMP_COLOR_RED;                        \
                oid = gpid;                                                     \
                continue;                                                       \
            }                                                                   \
            if(QMP(x)->map[pid].left == oid)                                    \
            {                                                                   \
                QMP_ROTATE_RIGHT(x, prootid, pid, lid, rid, ppid);              \
                uid = pid; pid = oid; oid = uid;                                \
            }                                                                   \
            QMP(x)->map[pid].color = QMP_COLOR_BLACK;                           \
            QMP(x)->map[gpid].color = QMP_COLOR_RED;                            \
            QMP_ROTATE_LEFT(x, prootid, gpid, rid, lid, ppid);                  \
        }                                                                       \
    }                                                                           \
    if(*prootid > 0)QMP(x)->map[*prootid].color = QMP_COLOR_BLACK;              \
}while(0)

#define QMP_REMOVE_COLOR(x, prootid, oid, xpid, lid, rid, uid, ppid)            \
do                                                                              \
{                                                                               \
    while((oid == 0 || QMP(x)->map[oid].color == QMP_COLOR_BLACK)               \
            && oid != *prootid)                                                 \
    {                                                                           \
        if(QMP(x)->map[xpid].left == oid)                                       \
        {                                                                       \
            uid = QMP(x)->map[xpid].right;                                      \
            if(QMP(x)->map[uid].color == QMP_COLOR_RED)                         \
            {                                                                   \
                QMP(x)->map[uid].color = QMP_COLOR_BLACK;                       \
                QMP(x)->map[xpid].color = QMP_COLOR_RED;                        \
                QMP_ROTATE_LEFT(x, prootid, xpid, rid, lid, ppid);              \
                uid = QMP(x)->map[xpid].right;                                  \
            }                                                                   \
            lid = QMP(x)->map[uid].left;                                        \
            rid = QMP(x)->map[uid].right;                                       \
            if((lid == 0 || QMP(x)->map[lid].color == QMP_COLOR_BLACK)          \
                && (rid == 0 || QMP(x)->map[rid].color == QMP_COLOR_BLACK))     \
            {                                                                   \
                QMP(x)->map[uid].color = QMP_COLOR_RED;                         \
                oid = xpid;                                                     \
                xpid = QMP(x)->map[oid].parent;                                 \
            }                                                                   \
            else                                                                \
            {                                                                   \
                rid = QMP(x)->map[uid].right;                                   \
                lid = QMP(x)->map[uid].left;                                    \
                if(rid == 0 || QMP(x)->map[rid].color == QMP_COLOR_BLACK)       \
                {                                                               \
                    if(lid > 0)QMP(x)->map[lid].color = QMP_COLOR_BLACK;        \
                    QMP(x)->map[uid].color = QMP_COLOR_RED;                     \
                    QMP_ROTATE_RIGHT(x, prootid, uid, lid, rid, ppid);          \
                    uid = QMP(x)->map[xpid].right;                              \
                }                                                               \
                QMP(x)->map[uid].color = QMP(x)->map[xpid].color;               \
                QMP(x)->map[xpid].color = QMP_COLOR_BLACK;                      \
                if((rid = QMP(x)->map[uid].right) > 0)                          \
                    QMP(x)->map[rid].color = QMP_COLOR_BLACK;                   \
                QMP_ROTATE_LEFT(x, prootid, xpid, rid, lid, ppid);              \
                oid = *prootid;                                                 \
                break;                                                          \
            }                                                                   \
        }                                                                       \
        else                                                                    \
        {                                                                       \
            uid = QMP(x)->map[xpid].left;                                       \
            if(QMP(x)->map[uid].color == QMP_COLOR_RED)                         \
            {                                                                   \
                QMP(x)->map[uid].color = QMP_COLOR_BLACK;                       \
                QMP(x)->map[xpid].color = QMP_COLOR_RED;                        \
                QMP_ROTATE_RIGHT(x, prootid, xpid, lid, rid, ppid);             \
                uid = QMP(x)->map[xpid].left;                                   \
            }                                                                   \
            lid = QMP(x)->map[uid].left;                                        \
            rid = QMP(x)->map[uid].right;                                       \
            if((lid == 0 || QMP(x)->map[lid].color == QMP_COLOR_BLACK)          \
                && (rid == 0 || QMP(x)->map[rid].color == QMP_COLOR_BLACK))     \
            {                                                                   \
                QMP(x)->map[uid].color = QMP_COLOR_RED;                         \
                oid = xpid;                                                     \
                xpid = QMP(x)->map[oid].parent;                                 \
            }                                                                   \
            else                                                                \
            {                                                                   \
                rid = QMP(x)->map[uid].right;                                   \
                lid = QMP(x)->map[uid].left;                                    \
                if(lid == 0 || QMP(x)->map[lid].color == QMP_COLOR_BLACK)       \
                {                                                               \
                    if(rid > 0)QMP(x)->map[rid].color = QMP_COLOR_BLACK;        \
                    QMP(x)->map[uid].color = QMP_COLOR_RED;                     \
                    QMP_ROTATE_LEFT(x, prootid, uid, rid, lid, ppid);           \
                    uid = QMP(x)->map[xpid].left;                               \
                }                                                               \
                QMP(x)->map[uid].color = QMP(x)->map[xpid].color;               \
                QMP(x)->map[xpid].color = QMP_COLOR_BLACK;                      \
                if((lid = QMP(x)->map[uid].left) > 0)                           \
                    QMP(x)->map[lid].color = QMP_COLOR_BLACK;                   \
                QMP_ROTATE_RIGHT(x, prootid, xpid, lid, rid, ppid);             \
                oid = *prootid;                                                 \
                break;                                                          \
            }                                                                   \
        }                                                                       \
    }                                                                           \
    if(oid > 0) QMP(x)->map[oid].color = QMP_COLOR_BLACK;                       \
}while(0)

/* mkdir force */
int qmap_mkdir(char *path)
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

/* init qmap */
void *qmap_init(char *basedir)
{
    struct stat  st = {0};
    void *x = NULL;
    char path[256];
    int n = 0;

    if(basedir && (x = (QMAP *)calloc(1, sizeof(QMAP))))
    {
        MUTEX_INIT(QMP(x)->mutex);
        n = sprintf(path, "%s/qmap.map", basedir); 
        qmap_mkdir(path);
        if((QMP(x)->fd = open(path, O_CREAT|O_RDWR, 0644)) > 0 
                && fstat(QMP(x)->fd, &st) == 0)
        {
            QMP(x)->end = st.st_size;
            QMP(x)->size = (off_t)sizeof(QMSTATE) + (off_t)sizeof(QMNODE) * (off_t)QMAP_NODES_MAX;
            //mmap
            QMP_MMAP(x);
            //init truncate
            if(st.st_size == 0)
            {
                QMP(x)->end = (off_t)sizeof(QMSTATE);
                QMP_INCRE(x);
            }
        }
        else 
        {
            if(QMP(x)->fd > 0) close(QMP(x)->fd);
            free(x);
            x = NULL;
            _exit(-1);
        }
        n = sprintf(path, "%s/qmap.root", basedir); 
        if((QMP(x)->rfd = open(path, O_CREAT|O_RDWR, 0644)) > 0 
                && fstat(QMP(x)->rfd, &st) == 0)
        {
            QMP(x)->rend = st.st_size;
            QMP(x)->rsize = (off_t)sizeof(QMROOT) * (off_t)QMAP_ROOT_MAX;
            //mmap
            QMP_ROOT_MMAP(x);
            //init truncate
            if(st.st_size == 0)
            {
                QMP_ROOT_INCRE(x);
            }
        }
        else 
        {
            if(QMP(x)->rfd > 0) close(QMP(x)->rfd);
            free(x);
            x = NULL;
            _exit(-1);
        }
    }
    return x;
}

/* insert new root */
int qmap_new_tree(void *x)
{
    int id = 0;
    if(x)
    {
        MUTEX_LOCK(QMP(x)->mutex);
        if(QMP(x)->state->rleft == 0)
        {
            QMP_ROOT_INCRE(x);
        }
        if(QMP(x)->state->qrleft > 0)
        {
            id = QMP(x)->state->qrfirst;
            QMP(x)->state->qrfirst = QMP(x)->roots[id].total;
            QMP(x)->state->qrleft--;
        }
        else
        {
            id = ++(QMP(x)->state->rcurrent);
        }
        QMP(x)->state->rleft--;
        QMP(x)->roots[id].status = 1;
        MUTEX_UNLOCK(QMP(x)->mutex);
    }
    return id;
}

/* total */
unsigned int qmap_total(void *x, int rootid)
{
    unsigned int total = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(QMP(x)->mutex);
        if(QMP(x)->state && QMP(x)->map && rootid < QMAP_ROOT_MAX)
        {
            total =  QMP(x)->roots[rootid].total;
        }
        MUTEX_UNLOCK(QMP(x)->mutex);
    }
    return total;
}

/* insert new node */
unsigned int qmap_insert(void *x, int rootid, int key, int data, int *old)
{
    unsigned int id = 0, nodeid = 0, rid = 0, lid = 0, uid = 0, pid = 0, 
        gpid = 0, ppid = 0, *prootid = NULL;
    QMNODE *node = NULL;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(QMP(x)->mutex);
        if(QMP(x)->state && QMP(x)->map && rootid < QMAP_ROOT_MAX
                && QMP(x)->roots[rootid].status > 0)
        {
            nodeid = QMP(x)->roots[rootid].rootid;
            while(nodeid > 0 && nodeid < QMP(x)->state->total)
            {
                node = &(QMP(x)->map[nodeid]);
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
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, QMP(x)->start, QMP(x)->state, QMP(x)->map, QMP(x)->state->current, QMP(x)->state->left, QMP(x)->state->total, QMP(x)->state->qleft, QMP(x)->state->qfirst, QMP(x)->state->qlast);
                if(QMP(x)->state->left == 0)
                {
                    QMP_INCRE(x);
                }
                if(QMP(x)->state->qleft > 0)
                {
                    id = QMP(x)->state->qfirst;
                    QMP(x)->state->qfirst = QMP(x)->map[id].parent;
                    QMP(x)->state->qleft--;
                }
                else
                {
                    id = ++(QMP(x)->state->current);
                }
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, QMP(x)->start, QMP(x)->state, QMP(x)->map, QMP(x)->state->current, QMP(x)->state->left, QMP(x)->state->total, QMP(x)->state->qleft, QMP(x)->state->qfirst, QMP(x)->state->qlast);
                QMP(x)->state->left--;
                //memset(&(QMP(x)->map[id]), 0, sizeof(QMNODE));
                QMP(x)->map[id].parent = nodeid;
                QMP(x)->map[id].key = key;
                QMP(x)->map[id].data = data;
                QMP_MIN_MAX(x, id, key);
                if(nodeid > 0)
                {
                    if(key > QMP(x)->map[nodeid].key) 
                        QMP(x)->map[nodeid].right = id;
                    else
                        QMP(x)->map[nodeid].left = id;
                }
                QMP(x)->roots[rootid].total++;
            }
            else
            {
                //fprintf(stdout, "%s::%d old id:%d pid:%d key:%d\n", __FILE__, __LINE__, id, parentid, key);
            }
        }
        if((nodeid = id) > 0)
        {
            if(QMP(x)->roots[rootid].rootid > 0)
            {
                prootid = &(QMP(x)->roots[rootid].rootid);
                QMP(x)->map[nodeid].color = QMP_COLOR_RED;
                QMP_INSERT_COLOR(x, prootid, nodeid, lid, rid, uid, pid, gpid, ppid);
            }
            else
            {
                QMP(x)->roots[rootid].rootid = nodeid;
            }
        }
end:
        MUTEX_UNLOCK(QMP(x)->mutex);
    }
    return id;
}

/* try insert  node */
unsigned int qmap_try_insert(void *x, int rootid, int key, int data, int *old)
{
    unsigned int id = 0, nodeid = 0, rid = 0, lid = 0, uid = 0, pid = 0, 
        gpid = 0, ppid = 0, *prootid = NULL;
    QMNODE *node = NULL;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(QMP(x)->mutex);
        if(QMP(x)->state && QMP(x)->map && rootid < QMAP_ROOT_MAX
                && QMP(x)->roots[rootid].status > 0)
        {
            nodeid = QMP(x)->roots[rootid].rootid;
            while(nodeid > 0 && nodeid < QMP(x)->state->total)
            {
                node = &(QMP(x)->map[nodeid]);
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
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, QMP(x)->start, QMP(x)->state, QMP(x)->map, QMP(x)->state->current, QMP(x)->state->left, QMP(x)->state->total, QMP(x)->state->qleft, QMP(x)->state->qfirst, QMP(x)->state->qlast);
                if(QMP(x)->state->left == 0)
                {
                    QMP_INCRE(x);
                }
                if(QMP(x)->state->qleft > 0)
                {
                    id = QMP(x)->state->qfirst;
                    QMP(x)->state->qfirst = QMP(x)->map[id].parent;
                    QMP(x)->state->qleft--;
                }
                else
                {
                    id = ++(QMP(x)->state->current);
                }
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, QMP(x)->start, QMP(x)->state, QMP(x)->map, QMP(x)->state->current, QMP(x)->state->left, QMP(x)->state->total, QMP(x)->state->qleft, QMP(x)->state->qfirst, QMP(x)->state->qlast);
                QMP(x)->state->left--;
                //memset(&(QMP(x)->map[id]), 0, sizeof(QMNODE));
                QMP(x)->map[id].parent = nodeid;
                QMP(x)->map[id].key = key;
                QMP(x)->map[id].data = data;
                QMP_MIN_MAX(x, id, key);
                if(nodeid > 0)
                {
                    if(key > QMP(x)->map[nodeid].key) 
                        QMP(x)->map[nodeid].right = id;
                    else
                        QMP(x)->map[nodeid].left = id;
                }
                QMP(x)->roots[rootid].total++;
            }
            else
            {
                //fprintf(stdout, "%s::%d old id:%d pid:%d key:%d\n", __FILE__, __LINE__, id, parentid, key);
            }
        }
        if((nodeid = id) > 0)
        {
            if(QMP(x)->roots[rootid].rootid > 0)
            {
                prootid = &(QMP(x)->roots[rootid].rootid);
                QMP(x)->map[nodeid].color = QMP_COLOR_RED;
                QMP_INSERT_COLOR(x, prootid, nodeid, lid, rid, uid, pid, gpid, ppid);
            }
            else
            {
                QMP(x)->roots[rootid].rootid = nodeid;
            }
        }
end:
        MUTEX_UNLOCK(QMP(x)->mutex);
    }
    return id;
}


/* get node key/data */
unsigned int qmap_get(void *x, unsigned int tnodeid, int *key, int *data)
{
    unsigned int id = 0;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(QMP(x)->mutex);
        if(QMP(x)->map && QMP(x)->state && tnodeid <  QMP(x)->state->total)
        {
            if(key) *key = QMP(x)->map[tnodeid].key;
            if(data) *data = QMP(x)->map[tnodeid].data;
            id = tnodeid;
        }
        MUTEX_UNLOCK(QMP(x)->mutex);
    }
    return id;
}

/* find key/data */
unsigned int qmap_find(void *x, int rootid, int key, int *data)
{
    unsigned int id = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(QMP(x)->mutex);
        if(QMP(x)->map && QMP(x)->state && rootid < QMAP_ROOT_MAX
                && QMP(x)->roots[rootid].status > 0)
        {
            //fprintf(stdout, "%s::%d rootid:%d key:%d data:%d total:%d\n", __FILE__, __LINE__, rootid, key, *data, QMP(x)->state->total);
            id = QMP(x)->roots[rootid].rootid;
            while(id > 0 && id < QMP(x)->state->total)
            {
                if(key == QMP(x)->map[id].key)
                {
                    if(data) *data = QMP(x)->map[id].data;
                    break;
                }
                else if(key > QMP(x)->map[id].key)
                {
                    id = QMP(x)->map[id].right;
                }
                else
                {
                    id = QMP(x)->map[id].left;
                }
            }
        }
        //fprintf(stdout, "%s::%d rootid:%d key:%d data:%d\n", __FILE__, __LINE__, rootid, key, *data);
        MUTEX_UNLOCK(QMP(x)->mutex);
    }
    return id;
}

/* get tree->min key/data */
unsigned int qmap_min(void *x, int rootid, int *key, int *data)
{
    unsigned int id = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(QMP(x)->mutex);
        if(QMP(x)->map && QMP(x)->state && rootid <  QMAP_ROOT_MAX
                && QMP(x)->roots[rootid].status > 0)
        {
            id = QMP(x)->roots[rootid].rootid;
            while(QMP(x)->map[id].left > 0)
            {
                id = QMP(x)->map[id].left;
            }
            if(id > 0 && QMP(x)->state->total)
            {
                if(key) *key = QMP(x)->map[id].key;
                if(data) *data = QMP(x)->map[id].data;
            }
        }
        MUTEX_UNLOCK(QMP(x)->mutex);
    }
    return id;
}

/* get tree->max key/data */
unsigned  int qmap_max(void *x, int rootid, int *key, int *data)
{
    unsigned int id = 0, tmp = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(QMP(x)->mutex);
        if(QMP(x)->map && QMP(x)->state && rootid <  QMAP_ROOT_MAX
                && QMP(x)->roots[rootid].status > 0)
        {
            tmp = QMP(x)->roots[rootid].rootid;
            do
            {
                id = tmp;
            }while(id > 0 && (tmp = QMP(x)->map[id].right) > 0);
            if(id > 0 && QMP(x)->state->total)
            {
                if(key) *key = QMP(x)->map[id].key;
                if(data) *data = QMP(x)->map[id].data;
            }
        }
        MUTEX_UNLOCK(QMP(x)->mutex);
    }
    return id;
}

/* get next node key/data */
unsigned int qmap_next(void *x, int rootid, unsigned int tnodeid, int *key, int *data)
{
    unsigned int id = 0, parentid = 0;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(QMP(x)->mutex);
        if(QMP(x)->map && QMP(x)->state && tnodeid <  QMP(x)->state->total)
        {
            id = tnodeid;
            //int rid = QMP(x)->roots[rootid].rootid;
            //fprintf(stdout, "%s::%d rootid:%d{parent:%d left:%d right:%d} node:%d{parent:%d left:%d right:%d)\n", __FILE__, __LINE__, rid, QMP(x)->map[rid].parent, QMP(x)->map[rid].left, QMP(x)->map[rid].right, id, QMP(x)->map[id].parent, QMP(x)->map[id].left, QMP(x)->map[id].right);
            if(QMP(x)->map[id].right > 0)
            {
                id = QMP(x)->map[id].right;
                while(QMP(x)->map[id].left  > 0)
                {
                    id = QMP(x)->map[id].left;
                }
            }
            else
            {
                while(id > 0)
                {
                    parentid = QMP(x)->map[id].parent;
                    if(QMP(x)->map[id].key < QMP(x)->map[parentid].key)
                    {
                        id = parentid;
                        goto end;
                    }
                    else
                    {
                        id = parentid;
                    }
                }
            }
end:
            if(id > 0 && id < QMP(x)->state->total)
            {
                if(key) *key = QMP(x)->map[id].key;
                if(data) *data = QMP(x)->map[id].data;
            }
            //fprintf(stdout, "%s::%d rootid:%d tnodeid:%d id:%d\n",__FILE__, __LINE__, rootid, tnodeid, id);
        }
        MUTEX_UNLOCK(QMP(x)->mutex);
    }
    return id;
}

/* get prev node key/data */
unsigned int qmap_prev(void *x, int rootid, unsigned int tnodeid, int *key, int *data)
{
    unsigned int id = 0, parentid = 0;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(QMP(x)->mutex);
        if(QMP(x)->map && QMP(x)->state && tnodeid <  QMP(x)->state->total)
        {
            id = tnodeid;
            if(QMP(x)->map[id].left > 0)
            {
                id = QMP(x)->map[id].left;
                while(QMP(x)->map[id].right  > 0)
                {
                    id = QMP(x)->map[id].right;
                }
            }
            else
            {
                while(id > 0)
                {
                    parentid = QMP(x)->map[id].parent;
                    if(QMP(x)->map[id].key > QMP(x)->map[parentid].key)
                    {
                        id = parentid;
                        goto end;
                    }
                    else
                    {
                        id = parentid;
                    }
                }
            }
end:
            if(id > 0 && id < QMP(x)->state->total)
            {
                if(key)*key = QMP(x)->map[id].key;
                if(data)*data = QMP(x)->map[id].data;
            }
            //fprintf(stdout, "%s::%d rootid:%d tnodeid:%d id:%d\n",__FILE__, __LINE__, rootid, tnodeid, id);
        }
        MUTEX_UNLOCK(QMP(x)->mutex);
    }
    return id;
}

/* view node */
void qmap_view_tnode(void *x, unsigned int tnodeid, FILE *fp)
{
    if(x)
    {
        if(QMP(x)->map[tnodeid].left > 0 && QMP(x)->map[tnodeid].left < QMP(x)->state->total)
        {
            qmap_view_tnode(x, QMP(x)->map[tnodeid].left, fp);
        }
        fprintf(fp, "[%d:%lld:%d]\n", tnodeid, (long long)QMP(x)->map[tnodeid].key, QMP(x)->map[tnodeid].data);
        if(QMP(x)->map[tnodeid].right > 0 && QMP(x)->map[tnodeid].right < QMP(x)->state->total)
        {
            qmap_view_tnode(x, QMP(x)->map[tnodeid].right, fp);
        }
    }
    return ;
}

void qmap_view_tree(void *x, int rootid, FILE *fp)
{
    if(x && rootid > 0)
    {
        MUTEX_LOCK(QMP(x)->mutex);
        if(QMP(x)->map && QMP(x)->state && rootid < QMAP_ROOT_MAX)
        {
             qmap_view_tnode(x, QMP(x)->roots[rootid].rootid, fp);
        }
        MUTEX_UNLOCK(QMP(x)->mutex);
    }
    return ;
}

/* set data */
int qmap_set_data(void *x, unsigned int tnodeid, int data)
{
    int old = -1;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(QMP(x)->mutex);
        if(QMP(x)->map && QMP(x)->state && tnodeid < QMP(x)->state->total)
        {
            old = QMP(x)->map[tnodeid].data;
            QMP(x)->map[tnodeid].data = data;
        }
        MUTEX_UNLOCK(QMP(x)->mutex);
    }
    return old;
}

/* remove node */
void qmap_remove(void *x, int rootid, unsigned int tnodeid, int *key, int *data)
{
    unsigned int id = 0, pid = 0, parent = 0, child = 0, rid = 0, lid = 0,
        uid = 0, ppid = 0, z = 0, color = 0, *prootid = NULL;

    if(x && rootid > 0 && tnodeid > 0)
    {
        //fprintf(stdout, "%s::%d rootid:%d tnodeid:%d\r\n", __FILE__, __LINE__, rootid, tnodeid);
        MUTEX_LOCK(QMP(x)->mutex);
        if(QMP(x)->map && QMP(x)->state && tnodeid < QMP(x)->state->total)
        {
            if(key) *key = QMP(x)->map[tnodeid].key;
            if(data) *data = QMP(x)->map[tnodeid].data;
            id = tnodeid;
            if(QMP(x)->map[tnodeid].left == 0)
            {
                child = QMP(x)->map[tnodeid].right;
            }
            else if(QMP(x)->map[tnodeid].right == 0)
            {
                child = QMP(x)->map[tnodeid].left;
            }
            else 
            {
                id = QMP(x)->map[tnodeid].right;
                while(QMP(x)->map[id].left > 0)
                    id = QMP(x)->map[id].left;
                parent = QMP(x)->map[id].parent;
                color = QMP(x)->map[id].color;
                if((child = QMP(x)->map[id].right) > 0)
                    QMP(x)->map[child].parent = parent;
                if((pid = parent) > 0)
                {
                    if(QMP(x)->map[pid].left == id)
                        QMP(x)->map[pid].left = child;
                    else
                        QMP(x)->map[pid].right = child;
                }
                else
                {
                    QMP(x)->roots[rootid].rootid = child;
                }
                if(QMP(x)->map[id].parent == tnodeid) parent = id;
                QMP(x)->map[id].color = QMP(x)->map[tnodeid].color;
                QMP(x)->map[id].parent = QMP(x)->map[tnodeid].parent;
                QMP(x)->map[id].left = QMP(x)->map[tnodeid].left;
                QMP(x)->map[id].right = QMP(x)->map[tnodeid].right;
                if((pid = QMP(x)->map[tnodeid].parent) > 0)
                {
                    if(QMP(x)->map[pid].left == tnodeid)
                        QMP(x)->map[pid].left = id;
                    else
                        QMP(x)->map[pid].right = id;
                }
                else
                {
                    QMP(x)->roots[rootid].rootid = id;
                }
                lid = QMP(x)->map[tnodeid].left;
                QMP(x)->map[lid].parent = id;
                if((rid = QMP(x)->map[tnodeid].right) > 0)
                    QMP(x)->map[rid].parent = id;
                goto color_remove;
            }
            parent =  QMP(x)->map[tnodeid].parent;
            color = QMP(x)->map[tnodeid].color;
            if(child > 0) 
            {
                QMP(x)->map[child].parent = parent;
            }
            if((pid = parent) > 0)
            {
                if(tnodeid == QMP(x)->map[pid].left) 
                    QMP(x)->map[pid].left = child;
                else 
                    QMP(x)->map[pid].right = child;
            }
            else 
            {
                QMP(x)->roots[rootid].rootid = child;
            }
            //remove color set
color_remove:
            QMP(x)->roots[rootid].total--;
            if(color == QMP_COLOR_BLACK)
            {
                //fprintf(stdout, "%s::%d node:%d parent:%d left:%d right:%d key:%d data:%d\n", __FILE__, __LINE__, tnodeid, QMP(x)->map[tnodeid].parent, QMP(x)->map[tnodeid].left, QMP(x)->map[tnodeid].right, QMP(x)->map[tnodeid].key, QMP(x)->map[tnodeid].data);
                prootid = &(QMP(x)->roots[rootid].rootid);
                QMP_REMOVE_COLOR(x, prootid, child, parent, lid, rid, uid, ppid);
            }
            //add to qleft
            memset(&(QMP(x)->map[tnodeid]), 0, sizeof(QMNODE));
            if(QMP(x)->state->qleft == 0)
            {
                QMP(x)->state->qfirst = QMP(x)->state->qlast = tnodeid;
            }
            else
            {
                z = QMP(x)->state->qlast;
                QMP(x)->map[z].parent = tnodeid;
                QMP(x)->state->qlast = tnodeid;
            }
            QMP(x)->state->qleft++;
            QMP(x)->state->left++;
            //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, QMP(x)->start, QMP(x)->state, QMP(x)->map, QMP(x)->state->current, QMP(x)->state->left, QMP(x)->state->total, QMP(x)->state->qleft, QMP(x)->state->qfirst, QMP(x)->state->qlast);
  
        }
        MUTEX_UNLOCK(QMP(x)->mutex);
    }
    return ;
}

/* remove node */
void qmap_remove_tnode(void *x, unsigned int tnodeid)
{
    unsigned int id = 0;

    if(x)
    {
        if(QMP(x)->map[tnodeid].left > 0 && QMP(x)->map[tnodeid].left < QMP(x)->state->total)
        {
            qmap_remove_tnode(x, QMP(x)->map[tnodeid].left);
        }
        if(QMP(x)->map[tnodeid].right > 0 && QMP(x)->map[tnodeid].right < QMP(x)->state->total)
        {
            qmap_remove_tnode(x, QMP(x)->map[tnodeid].right);
        }
        memset(&(QMP(x)->map[tnodeid]), 0, sizeof(QMNODE));
        if(QMP(x)->state->qleft == 0)
        {
            QMP(x)->state->qfirst = QMP(x)->state->qlast = tnodeid;
        }
        else
        {
            id = QMP(x)->state->qlast;
            QMP(x)->map[id].parent = tnodeid;
            QMP(x)->state->qlast = tnodeid;
        }
        QMP(x)->state->qleft++;
        QMP(x)->state->left++;
    }
    return ;
}

/* remove tree */
void qmap_remove_tree(void *x, int rootid)
{
    int id = 0;

    if(x && rootid > 0 && rootid < QMAP_ROOT_MAX)
    {
        MUTEX_LOCK(QMP(x)->mutex);
        qmap_remove_tnode(x, QMP(x)->roots[rootid].rootid);
        memset(&(QMP(x)->roots[rootid]), 0, sizeof(QMROOT));
        if(QMP(x)->state->qrleft == 0)
        {
            QMP(x)->state->qrfirst = QMP(x)->state->qrlast = rootid;
        }
        else
        {
            id = QMP(x)->state->qrlast;
            QMP(x)->roots[id].total = rootid;
            QMP(x)->state->qrlast = rootid;
        }
        QMP(x)->state->qrleft++;
        QMP(x)->state->rleft++;
        MUTEX_UNLOCK(QMP(x)->mutex);
    }
    return ;
}

//close qmap
void qmap_close(void *x)
{
    if(x)
    {
        //fprintf(stdout, "%s::%d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d sizeof(QMSTATE):%d\n", __FILE__, __LINE__, QMP(x)->start, QMP(x)->state, QMP(x)->map, QMP(x)->state->current, QMP(x)->state->left, QMP(x)->state->total, QMP(x)->state->qleft, QMP(x)->state->qfirst, QMP(x)->state->qlast, sizeof(QMSTATE));
        QMP_MUNMAP(x);
        if(QMP(x)->fd) close(QMP(x)->fd);
        QMP_ROOT_MUNMAP(x);
        if(QMP(x)->rfd) close(QMP(x)->rfd);
        MUTEX_DESTROY(QMP(x)->mutex);
        free(x);
    }
}


#ifdef _DEBUG_QMAP
#include "md5.h"
#include "timer.h"
int main(int argc, char **argv) 
{
    int i = 0, rootid = 0, id = 0, j = 0, old = 0, data = 0, n = 0, count = 50000000;
    unsigned char digest[MD5_LEN];
    void *qmap = NULL;
    void *timer = NULL;
    char line[1024];
    int key = 0;

    if((qmap = qmap_init("/tmp/test.qmap")))
    {
        rootid = qmap_new_tree(qmap);
        TIMER_INIT(timer);
        for(j = 1; j <= count; j++)
        {
            n = sprintf(line, "http://www.demo.com/%d.html", j);
            md5(line, n, digest);
            key = *((int *)digest);
            old = -1;
            data = j;
            id = qmap_insert(qmap, rootid, key, data, &old);
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
            id = qmap_try_insert(qmap, rootid, key, data, &old);
            if(old > 0 && old != j) 
            {
                fprintf(stdout, "%d:{id:%d key:%d rootid:%d old:%d}\n", j, id, key, rootid, old);
                _exit(-1);
            }
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "%s::%d try_insert:%d time:%lld\n", __FILE__,__LINE__, count, PT_LU_USEC(timer));
        TIMER_CLEAN(timer);
        qmap_close(qmap);
    }
}
//gcc -o qmap qmap.c md5.c -D_DEBUG_QMAP -g && ./qmap
#endif
