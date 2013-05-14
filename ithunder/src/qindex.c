#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/file.h>
#include <zlib.h>
#include <ibase.h>
#include <dbase.h>
#include "xmm.h"
#include "qindex.h"
#include "mutex.h"
#include "timer.h"
#include "logger.h"
#include "mmtrie.h"
#include "mmtree64.h"
#include "mmqueue.h"
#include "iqueue.h"
#include "db.h"
#define QINDEX_STATE_NAME       "qindex.state"
#define QINDEX_LOG_NAME         "qindex.log"
#define QINDEX_XPACKET_NAME     "qindex.xpacket"
#define QINDEX_XDICT_NAME       "qindex.xdict"
#define QINDEX_NODEMAP_NAME     "qindex.namemap"
#define QINDEX_IDMAP_NAME       "qindex.idmap"
#define QINDEX_MMQUEUE_NAME     "qindex.mmqueue"
#define QINDEX_MAP_NAME         "qindex.map"
#define QINDEX_BSTERM_NAME      "qindex.bsterm"
#define QINDEX_DB_DIR           "db"
#define QINDEX_UPDATE_DIR       "update"
#ifndef LLI
#define LL(x) ((long long int ) x)
#define LLI(x) ((long long int ) x)
#endif
#define CHECK_BSTERMIO(qindex, xid)                                                         \
do                                                                                          \
{                                                                                           \
    if((off_t)xid*(off_t)sizeof(BSTERM) >= qindex->bstermio.end)                            \
    {                                                                                       \
        qindex->bstermio.old = qindex->bstermio.end;                                        \
        qindex->bstermio.end = (xid / Q_BSTERM_BASE)+1;                                     \
        qindex->bstermio.end *= (off_t)(sizeof(BSTERM) * (off_t)Q_BSTERM_BASE);             \
        if(ftruncate(qindex->bstermio.fd, qindex->bstermio.end) != 0)break;                 \
        memset(qindex->bstermio.map + qindex->bstermio.old, 0,                              \
                qindex->bstermio.end - qindex->bstermio.old);                               \
    }                                                                                       \
}while(0)
#define CHECK_XPACKETIO(qindex)                                                             \
do                                                                                          \
{                                                                                           \
    if(((off_t)qindex->state->xpackettotal*(off_t)sizeof(XPACKET))>=qindex->xpacketio.end)  \
    {                                                                                       \
        qindex->xpacketio.old = qindex->xpacketio.end;                                      \
        qindex->xpacketio.end = (off_t)(qindex->state->xpackettotal / Q_XPACKET_BASE)+1;    \
        qindex->xpacketio.end *= (off_t)(sizeof(XPACKET) * Q_XPACKET_BASE);                 \
        if(ftruncate(qindex->xpacketio.fd, qindex->xpacketio.end) != 0)break;               \
        memset(qindex->xpacketio.map+qindex->xpacketio.old, 0,                              \
                qindex->xpacketio.end - qindex->xpacketio.old);                             \
    }                                                                                       \
}while(0)
static char *server_type_list[] = {"DocNode", "ParserNode", "IndexNode"};
/* mkdir force */
int pmkdir(char *path)
{
    char fullpath[IB_PATH_MAX];
    int level = -1, ret = -1;
    struct stat st = {0};
    char *p = NULL;

    if(path)
    {
        strcpy(fullpath, path);
        p = fullpath;
        while(*p != '\0')
        {
            if(*p == '/' )
            {
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
                level++;
            }
            ++p;
        }
        return 0;
    }
    return -1;
}

/* set document basedir  */
int qindex_set_basedir(QINDEX *qindex, char *basedir)
{
    int i = 0, j = 0, taskid = 0;
    char path[Q_PATH_MAX];
    QTASK *tasks = NULL;
    struct stat st = {0};
    off_t size = 0;

    if(qindex)
    {
        strcpy(qindex->basedir, basedir);
        /* log */
        sprintf(path, "%s/%s", basedir, QINDEX_LOG_NAME);
        pmkdir(path);
        LOGGER_INIT(qindex->logger, path);
        LOGGER_SET_LEVEL(qindex->logger, qindex->log_level);
        /* xdict */
        sprintf(path, "%s/%s", basedir, QINDEX_XDICT_NAME);
        if((qindex->xdict = mmtrie_init(path)) == NULL)
        {
            fprintf(stderr, "Initialize xdict(%s) failed, %s\n", path, strerror(errno));
            _exit(-1);
            return -1;
        }
        /* mmqueue */
        sprintf(path, "%s/%s", basedir, QINDEX_MMQUEUE_NAME);
        qindex->mmqueue = mmqueue_init(path);
        /* db */
        sprintf(path, "%s/%s", basedir, QINDEX_DB_DIR);
        qindex->db = db_init(path, 0);
        /* update */
        sprintf(path, "%s/%s", basedir, QINDEX_UPDATE_DIR);
        qindex->update = db_init(path, 0);
        /* qstate */
        sprintf(path, "%s/%s", basedir, QINDEX_STATE_NAME);
        if((qindex->qstatefd = open(path, O_CREAT|O_RDWR, 0644)) <= 0)
        {
            fprintf(stderr, "open state file(%s) failed, %s\n", path, strerror(errno));
            _exit(-1);
        }
        else
        {
            fstat(qindex->qstatefd, &st);
            if(st.st_size < sizeof(QSTATE))
            {
                if(ftruncate(qindex->qstatefd, 0) != 0
                        || ftruncate(qindex->qstatefd, sizeof(QSTATE)) != 0)_exit(-1);
            }
            if((qindex->state = (QSTATE *)mmap(NULL, sizeof(QSTATE), PROT_READ|PROT_WRITE,
                    MAP_SHARED, qindex->qstatefd, 0)) == NULL || qindex->state == (void *)-1)
            {
                fprintf(stderr, "mmap state failed, %s\n", strerror(errno));
                _exit(-1);
            }
            if(st.st_size < sizeof(QSTATE)) memset(qindex->state, 0, sizeof(QSTATE));
            if(qindex->state->nnodes > 0)
            {
                for(i = 1; i < Q_NODE_MAX; i++)
                {
                    if(qindex->state->nodes[i].status > 0 && qindex->state->nodes[i].ntasks > 0
                            && (tasks = qindex->state->nodes[i].tasks))
                    {
                        for(j = 0; j < Q_TASKS_MAX; j++)
                        {
                            if(tasks[j].status > 0)
                            {
                                taskid = i * Q_TASKS_MAX + j;
                                iqueue_push(qindex->queue, taskid);
                            }
                        }
                    }
                }
            }
        }
        /* xpacket */
        sprintf(path, "%s/%s", basedir, QINDEX_XPACKET_NAME);
        if((qindex->xpacketio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(qindex->xpacketio.fd, &st) == 0)
        {
            qindex->xpacketio.end = st.st_size;
            size = sizeof(XPACKET) * Q_XPACKET_MAX;
            if(st.st_size > size) size = st.st_size;
            if((qindex->xpacketio.map = (char *)mmap(NULL, size, PROT_READ|PROT_WRITE,
                            MAP_SHARED, qindex->xpacketio.fd, 0))
                    && qindex->xpacketio.map != (void *)-1)
            {
                qindex->xpacketio.size = size;
            }
            else
            {
                fprintf(stderr, "mmap xpacket file (%s) failed, %s\n", path, strerror(errno));
                _exit(-1);
            }
        }
        else
        {
            fprintf(stderr, "open xpacket file(%s) failed, %s\n", path, strerror(errno));
            _exit(-1);
        }
        /* block terms map */
        sprintf(path, "%s/%s", basedir, QINDEX_MAP_NAME);
        qindex->map = mmtrie_init(path);
        /* bsterm file */
        sprintf(path, "%s/%s", basedir, QINDEX_BSTERM_NAME);
        if((qindex->bstermio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(qindex->bstermio.fd, &st) == 0)
        {
            qindex->bstermio.end = st.st_size;
            size = sizeof(BSTERM) * Q_BSTERM_MAX;
            if(st.st_size > size) size = st.st_size;
            if((qindex->bstermio.map = (char *)mmap(NULL, size, PROT_READ|PROT_WRITE,
                            MAP_SHARED, qindex->bstermio.fd, 0))
                    && qindex->bstermio.map != (void *)-1)
            {
                qindex->bstermio.size = size;
            }
            else
            {
                fprintf(stderr, "mmap bsterm file (%s) failed, %s\n", path, strerror(errno));
                _exit(-1);
            }
        }
        else
        {
            fprintf(stderr, "open bsterm file(%s) failed, %s\n", path, strerror(errno));
            _exit(-1);
        }
        /* name map */
        sprintf(path, "%s/%s", basedir, QINDEX_NODEMAP_NAME);
        qindex->namemap = mmtrie_init(path);
        /* id map */
        sprintf(path, "%s/%s", basedir, QINDEX_IDMAP_NAME);
        qindex->idmap = mmtree64_init(path);
        if(qindex->state->rootid == 0)
        {
            qindex->state->rootid = mmtree64_new_tree(qindex->idmap);
        }
        return 0;
    }
    return -1;
}

/* read index */
int qindex_read_index(QINDEX *qindex, int taskid, char *data, int *len, int *count)
{
    int id = -1, nodeid = 0, x = 0, k = 0, *px = NULL, left = 0, last = 0, n = 0;
    DOCHEADER *docheader = NULL;
    XPACKET *xpackets = NULL;
    QTNODE *nodes = NULL;
    QTASK *tasks = NULL;
    char *p = NULL;

    if(qindex && qindex->state && taskid >= 0 && taskid < (Q_NODE_MAX * Q_TASKS_MAX)
            && data && len && *len > 0 && count) 
    {
        MUTEX_LOCK(qindex->mutex);
        *count = 0;
        k = taskid % Q_TASKS_MAX;
        if((nodeid = (taskid / Q_TASKS_MAX)) < Q_NODE_MAX 
                && (xpackets = (XPACKET *)(qindex->xpacketio.map))
                && (nodes = qindex->state->nodes) && (tasks = nodes[nodeid].tasks) 
                && tasks[k].status > 0)
        {
            tasks[k].count = 0;
            //check limit 
            if(nodes[nodeid].limit > 0  && tasks[k].nxpackets >= nodes[nodeid].limit) goto end;
            //check last over
            if(tasks[k].popid != tasks[k].over) 
            {
                tasks[k].popid = tasks[k].last;
            }
            else 
                tasks[k].last = tasks[k].popid;
            p = data;
            left = *len;
            while(left > Q_LEFT_LEN)
            {
                last = tasks[k].popid;
                if(nodes[nodeid].type == Q_NODE_DOCD || nodes[nodeid].type == Q_NODE_PARSERD)
                {
                    //if(tasks[k].popid == 0) tasks[k].popid = 1;
                    if((tasks[k].popid+1) < qindex->state->xpackettotal)
                    {
                        if(tasks[k].popid > 0) id = ++(tasks[k].popid);
                        else id = tasks[k].popid = 1;
                        if(id > qindex->state->docpopid) 
                            qindex->state->docpopid = id;
                        if(id > nodes[nodeid].total) 
                            nodes[nodeid].total = id;
                    }
                    else 
                    {
                        id = -1;
                        break;
                    }
                }
                else if(nodes[nodeid].type == Q_NODE_INDEXD)
                {
                    if((x = tasks[k].popid) == nodes[nodeid].last)
                    {
                        if(qindex->state->popid == 0) qindex->state->popid = 1;
                        if((id = qindex->state->popid) > 0 && id <= qindex->state->docpopid)
                        {
                            if(nodes[nodeid].total == 0) 
                            {
                                nodes[nodeid].last = nodes[nodeid].first = id;
                            }
                            else
                            {
                                xpackets[x].next = id;
                                xpackets[id].prev = x;
                            }
                            xpackets[id].nodeid = nodeid;
                            //xpackets[id].status = 1;
                            tasks[k].popid = id;
                            nodes[nodeid].last = id;
                            nodes[nodeid].total++;
                            qindex->state->popid++;
                        }
                        else id = -1;
                    }
                    else
                    {
                        if(nodes[nodeid].total > 0 && tasks[k].popid < nodes[nodeid].last)
                        {
                            if(tasks[k].nxpackets == 0 && tasks[k].count == 0)
                            {
                                tasks[k].popid = id = nodes[nodeid].first;
                            }
                            else
                            {
                                x = tasks[k].popid;
                                id = xpackets[x].next;
                                tasks[k].popid = id;
                            }
                        }
                        else id = -1;
                    }
                }
                else id = -1;
                if(id > 0)
                {
                    if(nodes[nodeid].type == Q_NODE_INDEXD && xpackets[id].nodeid != nodeid)
                    {
                        FATAL_LOGGER(qindex->logger, "Invalid xpacket[%d].nodeid[%d] to task[%s:%d].nodeid:%d", id, xpackets[id].nodeid, tasks[k].ip, tasks[k].port, nodeid);
                        _exit(-1);
                    }
                    if((n = db_get_data_len(PDB(qindex->db), id)) <= (left - Q_LEFT_LEN))
                    {
                        px = (int *)p;
                        p += sizeof(int);
                        docheader = (DOCHEADER *)p;
                        if((n = db_read_data(PDB(qindex->db), id, p)) > sizeof(DOCHEADER)) 
                        {
                            if(docheader->size < 0 || docheader->size != n) 
                            {
                                FATAL_LOGGER(qindex->logger, "Invalid data id:%d size:%d n:%d", id, docheader->size, n);
                                break;
                            }
                            //docheader->globalid = id;
                            *px = n;
                            left -= n + sizeof(int);
                            p += n;
                            tasks[k].count++;
                        }
                        else
                        {
                            p -= sizeof(int);
                            FATAL_LOGGER(qindex->logger, "Invalid data id:%d to document:%lld", id, LL(docheader->globalid));
                            //_exit(-1);
                            break;
                        }
                        last = tasks[k].popid;
                    }
                    else 
                    {
                        tasks[k].popid = last;
                        break;
                    }
                }
                else 
                {
                    break;
                }
            }
end:
            *len -= left;
            if((*count = tasks[k].count) > 0) 
            {
                id = tasks[k].popid = last ;
            }
            else id = -1;
        }
        else id = -2;
        MUTEX_UNLOCK(qindex->mutex);
    }
    return id;
}

/* over index */
int qindex_over_index(QINDEX *qindex, int taskid, int id)
{
    int ret = -1, k = 0, nodeid = -1;
    QTASK *tasks = NULL;

    if(qindex && taskid >= 0 && taskid < Q_NODE_MAX * Q_TASKS_MAX 
            && qindex->state && id > 0 && id < qindex->state->xpackettotal)
    {
        MUTEX_LOCK(qindex->mutex);
        k = taskid % Q_TASKS_MAX;
        if((nodeid = (taskid / Q_TASKS_MAX)) < Q_NODE_MAX 
                && (tasks = qindex->state->nodes[nodeid].tasks)
                && tasks[k].status > 0 && tasks[k].popid == id)
        {
            //fprintf(stdout, "%s::%d over_index(id:%d over:%d)\n", __FILE__, __LINE__, tasks[k].popid, tasks[k].over);
            tasks[k].over = id;
            tasks[k].nxpackets += tasks[k].count;
            ret = 0;
            //LOG_ACCESS(qindex, "taskid:%d id:%d popid:%d count:%d nindexs:%d node->total:%d indextotal:%d docpopid:%d", taskid, id, tasks[k].popid, tasks[k].count, tasks[k].nxpackets, qindex->state->nodes[nodeid].total, qindex->state->xpackettotal, qindex->state->docpopid);
            tasks[k].count = 0;
        }
        MUTEX_UNLOCK(qindex->mutex);
    }
    return ret;
}

/* add node */
int qindex_add_node(QINDEX *qindex, int type, char *name, int limit)
{
    int nodeid = -1, i = 0, n = 0;

    if(qindex && qindex->state->nnodes < Q_NODE_MAX && type >= 0 
            && name && (n = strlen(name)) > 0 && limit >= 0) 
    {
        //fprintf(stdout, "%s::%d nodeid:%d type:%d name:%s limit:%d\n", __FILE__, __LINE__, nodeid, type, name, limit);
        MUTEX_LOCK(qindex->mutex);
        if((nodeid = (mmtrie_get(qindex->namemap, name, n) - 1)) < 0)
        {
            for(i = 1; i < Q_NODE_MAX; i++)
            {
                if(qindex->state->nodes[i].status == 0)
                {
                    strcpy(qindex->state->nodes[i].name, name);
                    qindex->state->nodes[i].status = 1;  
                    qindex->state->nodes[i].type = type;  
                    qindex->state->nodes[i].limit = limit;  
                    mmtrie_add(qindex->namemap, name, n, i);
                    nodeid = i;
                    qindex->state->nnodes++;
                    if(type == Q_NODE_INDEXD) qindex->state->nidxnodes++;
                    break;
                }
            }
        }
        MUTEX_UNLOCK(qindex->mutex);
    }
    return nodeid;
}

/* delete node */
int qindex_del_node(QINDEX *qindex, int nodeid)
{
    char taskstr[Q_LINE_MAX], *name = NULL;
    int id = -1, i = 0, n = 0, x = 0;
    QTASK *tasks = NULL;

    if(qindex && nodeid > 0 && nodeid < Q_NODE_MAX)
    {
        MUTEX_LOCK(qindex->mutex);
        if(qindex->state->nodes[nodeid].status > 0 && (name = qindex->state->nodes[nodeid].name)
                && (n = strlen(name)) > 0 && (tasks = qindex->state->nodes[nodeid].tasks))
        {
            for(i = 0; i < Q_TASKS_MAX; i++)
            {
                if(tasks[i].status > 0 && (x = sprintf(taskstr, "%d:%s:%d", 
                    qindex->state->nodes[nodeid].type, tasks[i].ip, tasks[i].port)) > 0)
                    mmtrie_del(qindex->namemap, taskstr, x);
                if(tasks[i].mmqid > 0)mmqueue_close(MMQ(qindex->mmqueue), tasks[i].mmqid);
                tasks[i].mmqid = 0;
                tasks[i].nqueue = 0;
            }
            mmtrie_del(qindex->namemap, name, n);
            if(qindex->state->nodes[nodeid].type == Q_NODE_INDEXD 
                    && --(qindex->state->nidxnodes) == 0)
                qindex->state->popid = 0;
            memset(&(qindex->state->nodes[nodeid]), 0, sizeof(QTNODE));
            qindex->state->popid = 0;
            if(--(qindex->state->nnodes) == 0)
                qindex->state->docpopid = 0;
            id = nodeid;
        }
        MUTEX_UNLOCK(qindex->mutex);
    }
    return id;
}

/* set node limit */
int qindex_set_node_limit(QINDEX *qindex, int nodeid, int limit)
{
    int id = -1;

    if(qindex && nodeid > 0 && nodeid < Q_NODE_MAX && limit >= 0)
    {
        MUTEX_LOCK(qindex->mutex);
        qindex->state->nodes[nodeid].limit = limit;
        id = nodeid;
        MUTEX_UNLOCK(qindex->mutex);
    }
    return id;
}

int qindex_list_nodes(QINDEX *qindex, char *out, char *end)
{
    char *p = NULL, *pp = NULL;
    int n = 0, i = 0;

    if(qindex && out && out < end)
    {
        MUTEX_LOCK(qindex->mutex);
        p = out;
        p += sprintf(p, "({'count':'%d','nodes':{", qindex->state->nnodes);
        pp = p;
        for(i = 1; i < Q_NODE_MAX; i++)
        {
            if(qindex->state->nodes[i].status > 0 && p < end)
            {
                p += sprintf(p, "'%d':{'type':'%d', 'name':'%s', 'limit':'%d', "
                        "'ntasks':'%d', 'total':'%d'},", i, qindex->state->nodes[i].type,
                        qindex->state->nodes[i].name, qindex->state->nodes[i].limit, 
                        qindex->state->nodes[i].ntasks, 
                        qindex->state->nodes[i].total);
            }
        }
        if(p != pp)--p;
        p += sprintf(p, "}})");
        n = p - out;
        MUTEX_UNLOCK(qindex->mutex);
    }
    return n;
}

/* add task */
int qindex_add_task(QINDEX *qindex, int nodeid, char *ip, int port)
{
    int taskid = -1, i = 0, n = 0;
    char taskstr[Q_LINE_MAX];
    QTNODE *nodes = NULL;
    QTASK *tasks = NULL;

    if(qindex && ip && port > 0 && nodeid < Q_NODE_MAX 
            && (nodes = qindex->state->nodes) && nodes[nodeid].status > 0
            && nodes[nodeid].ntasks < Q_TASKS_MAX
            && (n = sprintf(taskstr, "%d:%s:%d", nodes[nodeid].type, ip, port)) > 0
            && (taskid = (mmtrie_get(qindex->namemap, taskstr, n) - 1)) < 0
            && (tasks = nodes[nodeid].tasks))
    {
        MUTEX_LOCK(qindex->mutex);
        for(i = 0; i < Q_TASKS_MAX; i++)
        {
            if(tasks[i].status == 0)
            {
                tasks[i].status = 1;
                strcpy(tasks[i].ip, ip);
                tasks[i].port = port;
                taskid = nodeid * Q_TASKS_MAX + i; 
                mmtrie_add(qindex->namemap, taskstr, n, taskid+1);
                //fprintf(stdout, "%s::%d nodeid:%d taskid:%d\n", __FILE__, __LINE__, nodeid, taskid);
                iqueue_push(qindex->queue, taskid);
                nodes[nodeid].ntasks++;
                tasks[i].mmqid = mmqueue_new(MMQ(qindex->mmqueue));
                break;
            }
        }
        MUTEX_UNLOCK(qindex->mutex);
    }
    return taskid;
}

/* delete task */
int qindex_del_task(QINDEX *qindex, int taskid)
{
    int id = -1, nodeid = 0, n = 0;
    char taskstr[Q_LINE_MAX];
    QTASK *tasks = NULL;
    QTNODE *nodes = NULL;

    if(qindex && taskid >= 0 && taskid < (Q_NODE_MAX * Q_TASKS_MAX)
            && (nodeid = (taskid/Q_TASKS_MAX)) < Q_NODE_MAX
            && (nodes = qindex->state->nodes) && (tasks = nodes[nodeid].tasks))
    {
        MUTEX_LOCK(qindex->mutex);
        id = taskid % Q_TASKS_MAX;
        //fprintf(stdout, "%s::%d nodeid:%d id:%d\n", __FILE__, __LINE__, nodeid, id);
        if((n = sprintf(taskstr, "%d:%s:%d", nodes[nodeid].type, 
                        tasks[id].ip, tasks[id].port)) > 0)
                mmtrie_del(qindex->namemap, taskstr, n);
        if(tasks[id].status > 0) nodes[nodeid].ntasks--;
        if(tasks[id].mmqid > 0)mmqueue_close(MMQ(qindex->mmqueue), tasks[id].mmqid);
        memset(&(tasks[id]), 0, sizeof(QTASK));
        MUTEX_UNLOCK(qindex->mutex);
    }
    return id;
}

/* pop task */
int qindex_pop_task(QINDEX *qindex, QTASK *task)
{
    int taskid = -1, nodeid = -1, id = -1;
    QTASK *tasks = NULL;

    if(qindex && task && QTOTAL(qindex->queue) > 0)
    {
        MUTEX_LOCK(qindex->mutex);
        iqueue_pop(qindex->queue, &taskid);
        if(taskid >= 0 && taskid < (Q_TASKS_MAX * Q_NODE_MAX)
                && (nodeid = taskid/Q_TASKS_MAX) < Q_NODE_MAX
                && (tasks = qindex->state->nodes[nodeid].tasks)
                && (id = (taskid % Q_TASKS_MAX)) >= 0)
        {
            memcpy(task, &(tasks[id]), sizeof(QTASK));
        }
        else taskid = -1;
        MUTEX_UNLOCK(qindex->mutex);
    }
    return taskid;
}

/* push task to queue */
int qindex_push_task(QINDEX *qindex, int taskid)
{
    int id = -1, nodeid = -1;
    QTASK *tasks = NULL;

    if(qindex && qindex->queue && taskid >= 0 && taskid < (Q_NODE_MAX * Q_TASKS_MAX)
            && (nodeid = (taskid / Q_TASKS_MAX)) < Q_NODE_MAX)
    {
        MUTEX_LOCK(qindex->mutex);
        id = taskid % Q_TASKS_MAX;
        if((tasks = qindex->state->nodes[nodeid].tasks) 
                && tasks[id].status > 0)
        {
            iqueue_push(qindex->queue, taskid);
            id = taskid;
        }
        MUTEX_UNLOCK(qindex->mutex);
    }
    return id;
}

/* list tasks */
int qindex_list_tasks(QINDEX *qindex, int nodeid, char *out, char *end)
{
    char *p = NULL, *pp = NULL;
    int n = 0, id = 0, i = 0;
    QTASK *tasks = NULL;
    QTNODE *nodes = NULL;

    if(qindex && out && end && out < end && nodeid > 0 && nodeid < Q_NODE_MAX)
    {
        MUTEX_LOCK(qindex->mutex);
        if((nodes = qindex->state->nodes) && nodes[nodeid].status > 0 
                && (tasks = nodes[nodeid].tasks))
        {
            p = out;
            p += sprintf(p, "({'id':'%d', 'name':'%s', 'total':'%d', 'type':'%d', 'limit':'%d',"
                    "'count':'%d',", nodeid, nodes[nodeid].name, nodes[nodeid].total,
                    nodes[nodeid].type, nodes[nodeid].limit, nodes[nodeid].ntasks);
            pp = p;
            if(nodes[nodeid].ntasks > 0)
            {
                p += sprintf(p, "'tasks':{");
                for(i = 0; i < Q_TASKS_MAX; i++)
                {
                    if(tasks[i].status > 0)
                    {
                        id = nodeid * Q_TASKS_MAX + i;
                        p += sprintf(p, "'%d':{'host':'%s:%d', 'npackets':'%d'},",
                            id, tasks[i].ip, tasks[i].port, tasks[i].nxpackets);
                    }
                }
                --p;
                p += sprintf(p, "}");
            }
            if(p == pp) --p;
            p += sprintf(p, "})");
            n = p - out;
        }
        MUTEX_UNLOCK(qindex->mutex);
    }
    return n;
}

/* push upindex */
void qindex_update(QINDEX *qindex, int mid, int flag)
{
    int i = 0, j = 0, id = 0;
    XPACKET *xpackets = NULL;
    QTNODE *nodes = NULL;

    if(qindex && (xpackets = (XPACKET *)(qindex->xpacketio.map)))
    {
        if((nodes = qindex->state->nodes))
        {
            //added to node[id]->tasks queue
            if((id = xpackets[mid].nodeid) > 0 && nodes[id].status > 0 && nodes[id].ntasks > 0 && xpackets[mid].nodeid > 0)    
            {
                for(j = 0; j < Q_TASKS_MAX; j++)
                {
                    if(nodes[id].tasks[j].status > 0)
                    {
                        if(nodes[id].tasks[j].mmqid <= 0)
                            nodes[id].tasks[j].mmqid = mmqueue_new(MMQ(qindex->mmqueue));
                        if(nodes[id].tasks[j].mmqid > 0)
                        {
                            mmqueue_push(MMQ(qindex->mmqueue), nodes[id].tasks[j].mmqid, mid);
                            nodes[id].tasks[j].nqueue++;
                        }
                    }
                }
            }
            //added to docNode/ParserNode queue
            if(mid <= qindex->state->docpopid)
            {
                for(i = 1; i < Q_NODE_MAX; i++)
                {
                    if((nodes[i].type == Q_NODE_DOCD || (nodes[i].type == Q_NODE_PARSERD && flag))
                            && nodes[i].status > 0 && nodes[i].ntasks > 0)
                    {
                        for(j = 0; j < Q_TASKS_MAX; j++)
                        {
                            if(nodes[i].tasks[j].status > 0)
                            {
                                if(nodes[i].tasks[j].mmqid <= 0)
                                    nodes[i].tasks[j].mmqid = mmqueue_new(MMQ(qindex->mmqueue));
                                if(nodes[i].tasks[j].mmqid > 0)
                                {
                                    mmqueue_push(MMQ(qindex->mmqueue), nodes[i].tasks[j].mmqid, mid);
                                    nodes[i].tasks[j].nqueue++;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    return ;
}

/* read upindex */
int qindex_read_upindex(QINDEX *qindex, int taskid, char *data, int *len, int *count)
{
    int ret = -1, mid = -1, nodeid = -1, id = 0, k = 0, n = 0, 
        left = 0, prev = 0, next = 0, *px = NULL;
    DOCHEADER *docheader = NULL;
    XPACKET *xpackets = NULL;
    QTNODE *nodes = NULL;
    QTASK *tasks = NULL;
    char *p = NULL;

    if(qindex && taskid >= 0 && data && taskid < Q_NODE_MAX * Q_TASKS_MAX
            && len && count && *len > 0)
    {
        MUTEX_LOCK(qindex->mutex);
        *count = 0;
        k = taskid % Q_TASKS_MAX;
        if((nodeid = (taskid / Q_TASKS_MAX)) < Q_NODE_MAX  && (nodes = qindex->state->nodes)
                && (tasks = qindex->state->nodes[nodeid].tasks) && tasks[k].status > 0
                && (xpackets =  (XPACKET *)(qindex->xpacketio.map)))
        {
            if(tasks[k].nupdates > 0 && tasks[k].upid != tasks[k].upover)
            {
                if((*len = db_read_data(PDB(qindex->update), taskid, p)) > 0)
                {
                    *count =  tasks[k].upcount;
                    ret = mid = tasks[k].upid;
                }
                else
                {
                    tasks[k].upid = tasks[k].upover = 0;
                }
            }
            else if(tasks[k].nqueue > 0 && tasks[k].mmqid > 0)
            {
                tasks[k].upcount = 0;
                left = *len;
                p = data;
                id = mmqueue_head(MMQ(qindex->mmqueue), tasks[k].mmqid, &mid);
                do
                {
                    if(id > 0 && mid > 0)
                    {
                        if(nodes[nodeid].type == Q_NODE_INDEXD && xpackets[mid].nodeid != nodeid)
                        {
                            prev = xpackets[mid].prev;
                            next = xpackets[mid].next;
                            FATAL_LOGGER(qindex->logger, "Invalid rootid:%d id:%d prev:%d[nodeid:%d] next:%d[nodeid:%d] xpacket[%d].nodeid[%d] to task[%s:%d].nodeid:%d nqueue:%d", tasks[k].mmqid, id, prev, xpackets[prev].nodeid, next, xpackets[next].nodeid, mid, xpackets[mid].nodeid, tasks[k].ip, tasks[k].port, nodeid, tasks[k].nqueue);
                            tasks[k].mmqid = 0;tasks[k].nqueue = 0;
                            break;
                        }
                        if(db_get_data_len(PDB(qindex->db), mid) > (left-Q_LEFT_LEN)) break;
                        tasks[k].upid = mid;
                        px = (int *)p;
                        p += sizeof(int);
                        docheader = (DOCHEADER *)p;
                        if((n = db_read_data(PDB(qindex->db), mid, p)) > sizeof(DOCHEADER)) 
                        {
                            ACCESS_LOGGER(qindex->logger, "update_index(%d) globalid:%lld mid:%d status:%d nodeid:%d task[%s:%d]", id,  LL(docheader->globalid), mid, xpackets[mid].status, xpackets[mid].nodeid, tasks[k].ip, tasks[k].port);
                            left -= n + sizeof(int);
                            p += n;
                            *px = n;
                            tasks[k].upcount++;
                        }
                        else
                        {
                            p -= sizeof(int);
                            FATAL_LOGGER(qindex->logger, "Invalid data id:%d to document:%lld", id, LL(docheader->globalid));
                            mid = -1;
                            //_exit(-1);
                            break;
                        }
                        mmqueue_pop(MMQ(qindex->mmqueue),tasks[k].mmqid, &mid);
                        tasks[k].nqueue--;
                        id = mmqueue_head(MMQ(qindex->mmqueue), tasks[k].mmqid, &mid);
                    }
                    else 
                    {
                        FATAL_LOGGER(qindex->logger, "Invalid qid:%d mid:%d", id, mid);
                        tasks[k].mmqid = 0;tasks[k].nqueue = 0;
                        mid = -1;
                        break;
                    }
                }while(tasks[k].nqueue > 0 && left > Q_LEFT_LEN && id > 0 && mid > 0);
                if((*count = tasks[k].upcount) > 0 && (*len -= left) > 0
                        && db_set_data(PDB(qindex->update), taskid, data, *len) >= 0)
                {
                    ret = tasks[k].upid;
                }
            }
            else ret = -1;
        }
        else ret = -2;
        MUTEX_UNLOCK(qindex->mutex);
    }
    return ret;
}

/* over upindex */
int qindex_over_upindex(QINDEX *qindex, int taskid, int upid)
{
    int ret = -1, k = 0, nodeid = -1;
    QTASK *tasks = NULL;

    if(qindex && taskid >= 0 && taskid < Q_NODE_MAX * Q_TASKS_MAX 
            && qindex->state && upid > 0 && upid < qindex->state->xpackettotal)
    {
        MUTEX_LOCK(qindex->mutex);
        k = taskid % Q_TASKS_MAX;
        if((nodeid = (taskid / Q_TASKS_MAX)) < Q_NODE_MAX 
                && (tasks = qindex->state->nodes[nodeid].tasks) 
                && tasks[k].status > 0)
        {
            tasks[k].upover = upid;
            tasks[k].nupdates += tasks[k].upcount;
            ret = 0;
        }
        MUTEX_UNLOCK(qindex->mutex);
    }
    return ret;
}

/* set block terms status */
int qindex_set_bterm(QINDEX *qindex, char *term, int nterm, int status)
{
    int ret = -1, termid = 0, xid = 0, n = 0;
    char line[Q_LINE_SIZE];
    BSTERM *bsterms = NULL;

    if(qindex && term && nterm > 0 
            && (termid = mmtrie_xadd((MMTRIE *)(qindex->xdict), term, nterm)) > 0
            &&  (n = sprintf(line, "b:%d", termid)) > 0
            && (xid = mmtrie_xadd((MMTRIE *)(qindex->map), line, n)) > 0
            && (bsterms = (BSTERM *)(qindex->bstermio.map)))
    {
        MUTEX_LOCK(qindex->mutex);
        CHECK_BSTERMIO(qindex, xid);
        if(xid > qindex->state->bterm_id_max) qindex->state->bterm_id_max = xid;
        if(nterm > Q_TERM_SIZE)
        {
            WARN_LOGGER(qindex->logger, "term:%.*s too long than len:%d", nterm, term, Q_TERM_SIZE);
        }
        else
        {
            bsterms[xid].bterm.id = termid;
            bsterms[xid].bterm.status = status;
            bsterms[xid].bterm.len = nterm;
            memcpy(bsterms[xid].term, term, nterm);
            ret = 0;
        }
        MUTEX_UNLOCK(qindex->mutex);
    }
    return ret;
}

/* update bterms status */
int qindex_update_bterm(QINDEX *qindex, int termid, int status)
{
    BSTERM *bsterms = NULL;
    int ret = -1;

    if(qindex && termid > 0 && (bsterms = (BSTERM *)(qindex->bstermio.map)))
    {
        MUTEX_LOCK(qindex->mutex);
        if(termid <= qindex->state->bterm_id_max)
        {
            bsterms[termid].bterm.status = status;
            ret = 0;
        }
        MUTEX_UNLOCK(qindex->mutex);
    }
    return ret;
}

/* delete bterm */
int qindex_del_bterm(QINDEX *qindex, int termid)
{
    BSTERM *bsterms = NULL;
    int ret = -1;

    if(qindex && termid > 0 && (bsterms = (BSTERM *)(qindex->bstermio.map)))
    {
        MUTEX_LOCK(qindex->mutex);
        if(termid <= qindex->state->bterm_id_max)
        {
            bsterms[termid].bterm.status = 0;
            ret = 0;
        }
        MUTEX_UNLOCK(qindex->mutex);
    }
    return ret;
}

/* list bterms */
int qindex_list_bterms(QINDEX *qindex, char *out)
{
    char *p = NULL, *pp = NULL;
    BSTERM *bsterms = NULL;
    int i = 0, ret = 0;

    if(qindex && (p = out) && (bsterms = (BSTERM *)(qindex->bstermio.map)))
    {
        MUTEX_LOCK(qindex->mutex);
        if(qindex->state->bterm_id_max > 0)
        {
            p += sprintf(p, "({\"bterms\":{");
            pp = p;
            for(i = 1; i <= qindex->state->bterm_id_max; i++)
            {
                if(bsterms[i].bterm.status > 0)
                    p += sprintf(p, "\"%d\":{\"id\":\"%d\", \"status\":\"%d\", \"text\":\"%s\"},", i, bsterms[i].bterm.id, bsterms[i].bterm.status, bsterms[i].term);
            }
            if(p > pp)--p;
            p += sprintf(p, "}})");
            ret = p - out;
        }
        MUTEX_UNLOCK(qindex->mutex);
    }
    return ret;
}

/* add block terms */
int qindex_add_bterm(QINDEX *qindex, char *term, int nterm)
{
    int ret = -1, termid = 0, xid = 0, n = 0;
    char line[Q_LINE_SIZE];
    BSTERM *bsterms = NULL;

    if(qindex && term && nterm > 0 
            && (termid = mmtrie_xadd((MMTRIE *)(qindex->xdict), term, nterm)) > 0
            &&  (n = sprintf(line, "b:%d", termid)) > 0
            && (xid = mmtrie_xadd((MMTRIE *)(qindex->map), line, n)) > 0
            && (bsterms = (BSTERM *)(qindex->bstermio.map)))
    {
        MUTEX_LOCK(qindex->mutex);
        CHECK_BSTERMIO(qindex, xid);
        if(xid > qindex->state->bterm_id_max) qindex->state->bterm_id_max = xid;
        if(nterm > Q_TERM_SIZE)
        {
            WARN_LOGGER(qindex->logger, "term:%.*s too long than len:%d", nterm, term, Q_TERM_SIZE);
        }
        else
        {
            bsterms[xid].bterm.id = termid;
            bsterms[xid].bterm.status = IB_BTERM_BLOCK;
            bsterms[xid].bterm.len = nterm;
            memcpy(bsterms[xid].term, term, nterm);
            ret = 0;
        }
        MUTEX_UNLOCK(qindex->mutex);
    }
    return ret;
}


/* update bterm */
int qindex_sync_bterms(QINDEX *qindex)
{
    struct timeval tv = {0};
    int ret = -1;

    if(qindex && qindex->state)
    {
        MUTEX_LOCK(qindex->mutex);
        gettimeofday(&tv, NULL);
        qindex->state->bterm_mod_time = (off_t)tv.tv_sec * (off_t)10000000 + (off_t)tv.tv_usec;
        MUTEX_UNLOCK(qindex->mutex);
    }
    return ret;
}

/* read bterm */
int qindex_read_bterms(QINDEX *qindex, int taskid, char *data, int ndata)
{
    int k = 0, nodeid = -1, n = -1, i = 0, left = 0, ret = -1;
    BSTERM *bsterms = NULL;
    QTNODE *nodes = NULL;
    QTASK *tasks = NULL;
    char *p = NULL;

    if(qindex && qindex->state && (p = data) && (left = ndata) > 0 && qindex->state->bterm_id_max > 0
            && taskid > 0 && taskid < (Q_NODE_MAX * Q_TASKS_MAX))
    {
        MUTEX_LOCK(qindex->mutex);
        k = taskid % Q_TASKS_MAX;
        if((nodeid = (taskid / Q_TASKS_MAX)) < Q_NODE_MAX 
                && (nodes = qindex->state->nodes) && (tasks = nodes[nodeid].tasks) 
                && tasks[k].status > 0 && tasks[k].bterm_mod_time < qindex->state->bterm_mod_time
                && (bsterms = (BSTERM *)(qindex->bstermio.map)))
        {
            for(i = 0; i <= qindex->state->bterm_id_max; i++)
            {
                if(bsterms[i].bterm.len > 0)
                {
                    n = bsterms[i].bterm.len + sizeof(BTERM);
                    if(left < n)
                    {
                        WARN_LOGGER(qindex->logger, "Nospace bsterms[%d] taskid:%d", i, taskid);
                        goto err;
                    }
                    else
                    {
                        memcpy(p, &(bsterms[i]), n);
                        p += n;
                        left -= n;
                    }
                }
                else
                {
                        WARN_LOGGER(qindex->logger, "Nocontent bsterms[%d] taskid:%d", i, taskid);
                }
            }
            ret =  p - data;
            tasks[k].bterm_last_time = qindex->state->bterm_mod_time;
        }
err:
        MUTEX_UNLOCK(qindex->mutex);
    }
    return ret;
}

/* over update bterm */
int qindex_over_bterms(QINDEX *qindex, int taskid)
{
    int k = 0, nodeid = -1, ret = -1;
    QTNODE *nodes = NULL;
    QTASK *tasks = NULL;

    if(qindex && qindex->state && taskid > 0 && taskid < (Q_NODE_MAX * Q_TASKS_MAX))
    {
        MUTEX_LOCK(qindex->mutex);
        k = taskid % Q_TASKS_MAX;
        if((nodeid = (taskid / Q_TASKS_MAX)) < Q_NODE_MAX 
                && (nodes = qindex->state->nodes) && (tasks = nodes[nodeid].tasks) 
                && tasks[k].status > 0)
        {
            tasks[k].bterm_mod_time = tasks[k].bterm_last_time;
            ret = 0;
        }
        MUTEX_UNLOCK(qindex->mutex);
    }
    return ret;
}

/* push index */
int qindex_push_index(QINDEX *qindex, int64_t key, IBDATA *block)
{
    DOCHEADER *docheader = NULL;
    XPACKET *xpackets = NULL;
    int mid = 0, old = 0;

    if(qindex && block->ndata > 0 && (docheader = (DOCHEADER *)block->data)) 
    {
        MUTEX_LOCK(qindex->mutex);
        if(docheader->size == block->ndata && (xpackets = (XPACKET *)(qindex->xpacketio.map))
            && (mid = mmtree64_try_insert(qindex->idmap, qindex->state->rootid, 
                            key, (qindex->state->id_max+1), &old)) > 0)
        {
            if(old > 0) mid = old;
            else mid = ++(qindex->state->id_max);
            db_set_data(PDB(qindex->db), mid, block->data, block->ndata);
            if(mid <= qindex->state->xpackettotal)
            {
                if(xpackets[mid].crc != docheader->crc)
                    qindex_update(qindex, mid, 1);
                else
                    qindex_update(qindex, mid, 0);
                xpackets[mid].status = docheader->status;
                xpackets[mid].crc = docheader->crc;
                ACCESS_LOGGER(qindex->logger, "update-index{gloablid:%lld mid:%d total:%d}", LL(docheader->globalid), mid, qindex->state->xpackettotal);
            }
            else
            {
                qindex->state->xpackettotal = mid;
                CHECK_XPACKETIO(qindex);
                if((xpackets = (XPACKET *)(qindex->xpacketio.map)))
                {
                    xpackets[mid].status = docheader->status;
                    xpackets[mid].crc = docheader->crc;
                    ACCESS_LOGGER(qindex->logger, "new-index{gloablid:%lld mid:%d}", LL(docheader->globalid), mid);
                }
            }
        }
        else
        {
            FATAL_LOGGER(qindex->logger, "Invalid document size:%d ndata:%d id:%lld crc:%d", docheader->size, block->ndata, LL(docheader->globalid), docheader->crc);
            _exit(-1);
        }
        MUTEX_UNLOCK(qindex->mutex);
    }
    return mid;
}

/* new mindex */
MINDEX *mindex_new(QINDEX *qindex)
{
    MINDEX *mindex = NULL;
    if(strlen(qindex->qtask_server_host) > 0 && qindex->qtask_server_port > 0
            && (qindex->qtask_commitid > 0 || qindex->qtask_queueid > 0)
            && (mindex = xmm_mnew(sizeof(MINDEX))))
    {
        /* set mtask */
        mtask_set(&(mindex->mtask), qindex->qtask_server_host, qindex->qtask_server_port,
                qindex->qtask_commitid, qindex->qtask_queueid);
        /* set db connection*/
        if(dbase_set(&(mindex->db), qindex->db_host, qindex->db_port) != 0
                || dbase_connect(&(mindex->db)) != 0)
        {
            FATAL_LOGGER(qindex->logger, "connect_source_db(%s:%d) failed, %s", qindex->db_host, qindex->db_port, strerror(errno));
        }
    }
    return mindex;
}

/* xinde check db */
int mindex_check(MINDEX *mindex)
{
    if(mindex)
    {
        if(mindex->mtask.fd <= 0 && mtask_connect(&(mindex->mtask)) <= 0)
            return -2;
        return 0;
    }
    return -1;
}

/* clean hindex */
void mindex_clean(MINDEX *mindex)
{
    if(mindex)
    {
        mtask_close(&(mindex->mtask));
        dbase_close(&(mindex->db));
        bjson_clean(&(mindex->request));
        xmm_free(mindex, sizeof(MINDEX));
    }
    return ;
}
/* working for get_data() and push_index() */
int qindex_work(QINDEX *qindex, MINDEX *mindex)
{
    int ret = -1, i = 0, x = 0 , n = 0, mid = 0;
    int64_t *xid = NULL, key = 0, vlong = 0;
    BELEMENT *root = NULL, *sub = NULL;
    DOCHEADER *docheader = NULL;
    STERM *termlist = NULL;
    IBDATA block = {0};
    char *term = NULL;
    BRES *res = NULL;


    if(qindex && mindex && mindex_check(mindex) == 0)
    {
        if(mtask_pop(&(mindex->mtask)) > 0 && (xid = (int64_t *)mindex->mtask.packet)
                && mindex->mtask.length > sizeof(int64_t))
        {
            n = mindex->mtask.length/sizeof(int64_t);
            brequest_reset(&(mindex->request));
            brequest_append_keys(&(mindex->request), xid, n);
            brequest_finish(&(mindex->request));
            if((res = dbase_get_records(&(mindex->db), &(mindex->request))))
            {
                x = 0;
                while((root = dbase_next_record(res, root, &key))) 
                {
                    ACCESS_LOGGER(qindex->logger, "index_record[%d/%d] packetid:%d key:%lld", x, n, mindex->mtask.packetid, LLI(key));
                    vlong = 0;
                    if((sub = belement_find(root, qindex->db_key_name)))
                    {
                        belement_v_long(sub, &vlong);
                    }
                    if(key != vlong)
                    {
                        ACCESS_LOGGER(qindex->logger, "Invalid record[%d/%d] packetid:%d key:%lld/%lld", x, n, mindex->mtask.packetid, LLI(vlong), LLI(key));
                        return ret;
                    }
                    if((sub = belement_find(root, qindex->db_index_block_name))
                            && (block.ndata = belement_v_blob(sub, &(block.data))) > 0)
                    {
                        docheader = (DOCHEADER *)block.data;
                        termlist = (STERM *)(block.data + sizeof(DOCHEADER)
                                + docheader->nfields * sizeof(XFIELD));
                        term = block.data + docheader->textblock_off;
                        for(i = 0; i < docheader->nterms; i++)
                        {
                            termlist[i].termid = mmtrie_xadd(qindex->xdict, term, termlist[i].term_len); 
                            term += termlist[i].term_len;
                        }
                        mid = qindex_push_index(qindex, key, &block);
                        ACCESS_LOGGER(qindex->logger, "over_record[%d/%d] packetid:%d key:%lld", x, n, mindex->mtask.packetid, LLI(key));
                    }
                    else
                    {
                        FATAL_LOGGER(qindex->logger, "Invalid data feilds[%s]",  qindex->db_index_block_name);
                        return ret;
                    }
                    x++;
                }
                ret = 0;
                mtask_finish(&(mindex->mtask), 0);
            }
            else
            {
                FATAL_LOGGER(qindex->logger, "get_records(%d) failed, %s", mindex->mtask.packetid, strerror(errno));
                return ret;
            }
        }
    }
    return ret;
}

/* set qtask server */
int qindex_set_qtask_server(QINDEX *qindex, char *ip, int port, int commitid, int queueid)
{
    int ret = -1;

    if(qindex && ip && port > 0 && strlen(ip) < Q_IP_MAX && (commitid > 0 || queueid > 0))
    {
        qindex->qtask_commitid = commitid;
        qindex->qtask_queueid = queueid;
        qindex->qtask_server_port = port;
        strcpy(qindex->qtask_server_host, ip);
        ret = 0;
    }
    return ret;
}


/* set res-data  db */
int qindex_set_db(QINDEX *qindex, char *ip, int port, char *key_name, char *index_block_name)
{
    int ret = -1;

    if(qindex && ip && port > 0 && key_name && index_block_name && strlen(ip) < Q_IP_MAX 
            && strlen(key_name) < Q_FIELDNAME_MAX
            && strlen(index_block_name) < Q_FIELDNAME_MAX)
    {
        qindex->db_port = port;
        strcpy(qindex->db_host, ip);
        strcpy(qindex->db_key_name, key_name);
        strcpy(qindex->db_index_block_name, index_block_name);
        ret = 0;
    }
    return ret;
}

/*  clean */
void qindex_clean(QINDEX *qindex)
{
    if(qindex)
    {
        if(qindex->state) munmap(qindex->state, sizeof(QSTATE));
        if(qindex->qstatefd > 0)close(qindex->qstatefd);
        if(qindex->db) db_clean(PDB(qindex->db));
        if(qindex->update) db_clean(PDB(qindex->update));
        MUTEX_DESTROY(qindex->mutex);
        if(qindex->xdict){mmtrie_clean(qindex->xdict);}
        if(qindex->namemap){mmtrie_clean(qindex->namemap);}
        if(qindex->idmap){mmtree64_close(qindex->idmap);}
        if(qindex->queue){iqueue_clean(qindex->queue);}
        if(qindex->mmqueue){mmqueue_clean(MMQ(qindex->mmqueue));}
        if(qindex->xpacketio.map) munmap(qindex->xpacketio.map, qindex->xpacketio.size);
        if(qindex->xpacketio.fd) close(qindex->xpacketio.fd);
        if(qindex->bstermio.map) munmap(qindex->bstermio.map, qindex->bstermio.size);
        if(qindex->bstermio.fd) close(qindex->bstermio.fd);
        if(qindex->map) mmtrie_clean(qindex->map);
        if(qindex->logger){LOGGER_CLEAN(qindex->logger);}
        xmm_free(qindex, sizeof(QINDEX));
    }
    return ;
}

/* initialize */
QINDEX *qindex_init()
{
    QINDEX *qindex = NULL;

    if((qindex = (QINDEX *)xmm_mnew(sizeof(QINDEX))))
    {
        MUTEX_INIT(qindex->mutex);
        qindex->queue                    = iqueue_init();
    }
    return qindex;
}
