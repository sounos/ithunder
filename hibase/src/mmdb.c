#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <ibase.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/time.h>
#include "mmtrie.h"
#include "logger.h"
#include "iqueue.h"
#include "db.h"
#include "md5.h"
#include "mmdb.h"
#include "xmm.h"
#include "immx.h"
#include "mtree64.h"
#define  M_QMAP_NAME    "mmdb.qmap"
#define  M_LOG_NAME     "mmdb.log"
#define  M_STATE_NAME   "mmdb.state"
#define  M_QTASK_NAME   "mmdb.qtask"
#define  M_QPAGE_NAME   "mmdb.qpage"
#define  M_RDB_DIR      "record"
#define  M_PDB_DIR      "page"
#ifndef  LL
#define  LL64(x) ((long long int)x)
#endif
#ifdef MAP_LOCKED
#define MMAP_SHARED MAP_SHARED|MAP_LOCKED
#else
#define MMAP_SHARED MAP_SHARED
#endif
/* over merge */
int mmdb_over_merge(MMDB *mmdb, int qid, int pid, CQRES *cqres, 
	IQSET *qset, IRECORD *recs, int *nrecs, int error);
int pmkdir(char *path)
{
    struct stat st;
    char fullpath[M_PATH_MAX];
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

/* set base dir */
int mmdb_set_basedir(MMDB *mmdb, char *basedir)
{
    char path[M_PATH_MAX];
    struct stat st = {0};
    SNODE *nodes = NULL;
    int i = 0;

    /* qmap */
    sprintf(path, "%s/%s", basedir, M_QMAP_NAME);
    pmkdir(path);
    if((mmdb->qmap = mmtrie_init(path)) == NULL)
    {
        fprintf(stderr, "Initialize mmtrie(%s) failed, %s\n", path, strerror(errno));
        _exit(-1);
        return -1;
    }
    /* state */
    sprintf(path, "%s/%s", basedir, M_STATE_NAME);
    if((mmdb->stateio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
            && fstat(mmdb->stateio.fd, &st) == 0)
    {
        if(st.st_size < sizeof(MSTATE) && ftruncate(mmdb->stateio.fd, sizeof(MSTATE)) != 0)
             _exit(-1);
        if((mmdb->stateio.map = (char *)mmap(NULL, sizeof(MSTATE), PROT_READ|PROT_WRITE,
            MAP_SHARED, mmdb->stateio.fd, 0)) && mmdb->stateio.map != (void *)-1)
        {
            mmdb->state = (MSTATE *)mmdb->stateio.map;
            mmdb->stateio.size = sizeof(MSTATE);
            if((nodes = mmdb->state->nodes))
            {
                for(i = 0; i < M_NODES_MAX; i++)
                {
                    if(nodes[i].status > 0)
                    {
                        iqueue_push(mmdb->queue, i);
                    }
                }
            }
        }
        else
        {
            fprintf(stderr, "mmap state file (%s) failed, %s\n", path, strerror(errno));
            _exit(-1);
        }
    }
    else
    {
        fprintf(stderr, "open node file(%s) failed, %s\n", path, strerror(errno));
        _exit(-1);
    }
    /* qtask */
    sprintf(path, "%s/%s", basedir, M_QTASK_NAME);
    if((mmdb->qtaskio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
            && fstat(mmdb->qtaskio.fd, &st) == 0)
    {
        mmdb->qtaskio.end = st.st_size;
        mmdb->qtaskio.size = (off_t)M_QTASK_MAX * (off_t)sizeof(QTASK);
        if((mmdb->qtaskio.map = (char *)mmap(NULL, mmdb->qtaskio.size, PROT_READ|PROT_WRITE,
            MAP_SHARED, mmdb->qtaskio.fd, 0)) == NULL || mmdb->qtaskio.map == (void *)-1)
        {
            fprintf(stderr, "mmap record file (%s) failed, %s\n", path, strerror(errno));
            _exit(-1);
        }
        if(st.st_size == 0)
        {
            mmdb->qtaskio.end = (off_t)sizeof(QTASK) * (off_t)M_QTASK_BASE;
            if(ftruncate(mmdb->qtaskio.fd,  mmdb->qtaskio.end) != 0)
            {
                fprintf(stderr, "ftruncate %s failed, %s\n", path, strerror(errno));
                _exit(-1);
            }
            memset(mmdb->qtaskio.map, 0, mmdb->qtaskio.end);
        }
        else if(st.st_size > mmdb->qtaskio.size)
        {
            mmdb->qtaskio.end = mmdb->qtaskio.size = st.st_size;
        }
        mmdb->qtaskio.old = mmdb->qtaskio.end;
    }
    else
    {
        fprintf(stderr, "open record file(%s) failed, %s\n", path, strerror(errno));
        _exit(-1);
    }
    /* qpage */
    sprintf(path, "%s/%s", basedir, M_QPAGE_NAME);
    if((mmdb->qpageio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
            && fstat(mmdb->qpageio.fd, &st) == 0)
    {
        mmdb->qpageio.end = st.st_size;
        mmdb->qpageio.size = (off_t)M_QPAGE_MAX * (off_t)sizeof(QPAGE);
        if((mmdb->qpageio.map = (char *)mmap(NULL, mmdb->qpageio.size, PROT_READ|PROT_WRITE,
            MAP_SHARED, mmdb->qpageio.fd, 0)) == NULL || mmdb->qpageio.map == (void *)-1)
        {
            fprintf(stderr, "mmap record file (%s) failed, %s\n", path, strerror(errno));
            _exit(-1);
        }
        if(st.st_size == 0)
        {
            mmdb->qpageio.end = (off_t)M_QPAGE_BASE * (off_t)sizeof(QPAGE);
            if(ftruncate(mmdb->qpageio.fd,  mmdb->qpageio.end) != 0)
            {
                fprintf(stderr, "ftruncate %s failed, %s\n", path, strerror(errno));
                _exit(-1);
            }
            memset(mmdb->qpageio.map, 0, mmdb->qpageio.end);
        }
        else if(st.st_size > mmdb->qpageio.size)
        {
            mmdb->qpageio.end = mmdb->qpageio.size = st.st_size;
        }
        mmdb->qpageio.old = mmdb->qpageio.end;
    }
    else
    {
        fprintf(stderr, "open record file(%s) failed, %s\n", path, strerror(errno));
        _exit(-1);
    }
    /* db */
    sprintf(path, "%s/%s", basedir, M_RDB_DIR);
    mmdb->rdb = db_init(path, 0);
    sprintf(path, "%s/%s", basedir, M_PDB_DIR);
    mmdb->pdb = db_init(path, 0);
    /* logger */
    sprintf(path, "%s/%s", basedir, M_LOG_NAME);
    LOGGER_INIT(mmdb->logger, path);
    //gettimeofday(&tv, NULL);
    mmdb->start_time = mmdb->state->last_time = time(NULL);
    mmdb->state->last_querys = 0;
    mmdb->state->last_hits = 0;
    mmdb->state->querys_total = 0;
    mmdb->state->cache_hits = 0;
    //(off_t)tv.tv_sec * (off_t)1000000 + (off_t)tv.tv_usec;
    return 0;
}

/* set log level */
int mmdb_set_log_level(MMDB *mmdb, int log_level)
{
    if(mmdb)
    {
        if(mmdb->logger && log_level > 0)
        {
            LOGGER_SET_LEVEL(mmdb->logger, log_level);
        }
        return 0;
    }
    return -1;
}

/* push mmx */
void mmdb_push_mmx(MMDB *mmdb, void *mmx)
{
    int x = 0;

    if(mmdb && mmx)
    {
        MUTEX_LOCK(mmdb->mutex_mmx);
        if(mmdb->nqmmxs < IB_MMX_MAX)
        {
            IMMX_RESET(mmx);
            x = mmdb->nqmmxs++;
            mmdb->qmmxs[x] = mmx;
        }
        else
        {
            IMMX_CLEAN(mmx);
        }
        MUTEX_UNLOCK(mmdb->mutex_mmx);
    }
    return ;
}

/* mmdb pop mmx */
void *mmdb_pop_mmx(MMDB *mmdb)
{
    void *mmx = NULL;
    int x = 0;

    if(mmdb)
    {
        MUTEX_LOCK(mmdb->mutex_mmx);
        if(mmdb->nqmmxs > 0)
        {
            x = --(mmdb->nqmmxs);
            mmx = mmdb->qmmxs[x];
            mmdb->qmmxs[x] = NULL;
        }
        else
        {
            mmx = IMMX_INIT();
        }
        MUTEX_UNLOCK(mmdb->mutex_mmx);
    }
    return mmx;
}


/* mmdb set cache life time (seconds) */
int mmdb_set_cache_life_time(MMDB *mmdb, int cache_life_time)
{
    if(mmdb)
    {
        MUTEX_LOCK(mmdb->mutex_state);
        if(mmdb->state)
        {
            mmdb->state->cache_life_time = cache_life_time;
        }
        MUTEX_UNLOCK(mmdb->mutex_state);
        return 0;
    }
    return -1;
}

/* add node */
int mmdb_add_node(MMDB *mmdb, int type, char *ip, int port, int limit)
{
    int i = 0, n = 0, ret = 0, nodeid = 0, pnodeid = 0, dnodeid = 0;
    char buf[M_LINE_SIZE];
    SNODE *nodes = NULL;

    if(mmdb && mmdb->state && type > 0 && ip && port > 0 
            && (n = sprintf(buf, "%d:%s:%d", type, ip, port)) > 0
            && (nodeid = (mmtrie_get(mmdb->qmap, buf, n))) == 0
            && (nodes = mmdb->state->nodes))
    {
        MUTEX_LOCK(mmdb->mutex_state);
        for(i = 1; i < M_NODES_MAX; i++)
        {
            if(nodes[i].status == 0 && nodeid == 0)
                nodeid = i;
            if(nodes[i].type == M_NODE_QPARSERD)
                pnodeid = i;
            if(nodes[i].type == M_NODE_QDOCD)
                dnodeid = i;
        }
        if(nodeid > 0)
        {
            if(type == M_NODE_QPARSERD && pnodeid != 0) goto end;
            if(type == M_NODE_QDOCD && dnodeid != 0) goto end;
            strcpy(nodes[nodeid].ip, ip);
            nodes[nodeid].port = port;
            nodes[nodeid].type = type;
            nodes[nodeid].limit = limit;
            nodes[nodeid].status = 1;
            mmtrie_add(mmdb->qmap, buf, n, nodeid);
            mmdb->state->nnodes++;
            iqueue_push(mmdb->queue, nodeid);
            ret = nodeid;
        }
end:
        MUTEX_UNLOCK(mmdb->mutex_state);
    }
    return ret;
}

/* del node */
int mmdb_del_node(MMDB *mmdb, int nodeid)
{
    char buf[M_LINE_SIZE];
    int ret = -1, n = 0;
    SNODE *nodes = NULL;

    if(mmdb && mmdb->state && nodeid > 0 && nodeid < M_NODES_MAX)
    {
        MUTEX_LOCK(mmdb->mutex_state);
        if((nodes = mmdb->state->nodes) && nodes[nodeid].status > 0 
                && (n = sprintf(buf, "%d:%s:%d", nodes[nodeid].type, 
                        nodes[nodeid].ip, nodes[nodeid].port)) > 0)
        {
            mmtrie_del(mmdb->qmap, buf, n);
            nodes[nodeid].status = 0;
            nodes[nodeid].type = 0;
            --(mmdb->state->nnodes);
            ret = nodeid;
        }
        MUTEX_UNLOCK(mmdb->mutex_state);
    }
    return ret;
}

/* update node */
int mmdb_update_node(MMDB *mmdb, int nodeid, int limit)
{
    SNODE *nodes = NULL;
    int ret = -1;

    if(mmdb && mmdb->state && nodeid > 0 && nodeid < M_NODES_MAX)
    {
        MUTEX_LOCK(mmdb->mutex_state);
        if((nodes = mmdb->state->nodes) && nodes[nodeid].status > 0) 
        {
            nodes[nodeid].limit = limit;
            ret = nodeid;
        }
        MUTEX_UNLOCK(mmdb->mutex_state);
    }
    return ret;
}

/* pop node */
int mmdb_pop_node(MMDB *mmdb, SNODE *node)
{
    SNODE *nodes = NULL;
    int nodeid = -1;

    if(mmdb && mmdb->state && node && QTOTAL(mmdb->queue) > 0)
    {
        MUTEX_LOCK(mmdb->mutex_state);
        iqueue_pop(mmdb->queue, &nodeid);
        if(nodeid >= 0 && nodeid < M_NODES_MAX
                && (nodes = mmdb->state->nodes))
        {
            memcpy(node, &(nodes[nodeid]), sizeof(SNODE));
        }
        else nodeid = -1;
        MUTEX_UNLOCK(mmdb->mutex_state);
    }
    return nodeid;
}

/* list nodes */
int mmdb_list_nodes(MMDB *mmdb, char *s, char *es)
{
    SNODE *nodes  = NULL;
    int i = 0, n = 0;
    char *p = NULL;

    if(mmdb && s && mmdb->state && (nodes = mmdb->state->nodes))
    {
        MUTEX_LOCK(mmdb->mutex_state);
        if(mmdb->state->nnodes > 0 && s < es)
        {
            p = s;
            p += sprintf(p, "({'count':'%d', 'nodes':{", mmdb->state->nnodes);
            for(i = 1; i < M_NODES_MAX; i++)
            {
                if(nodes[i].status > 0 && s < es)
                {
                    p += sprintf(p, "'%d':{'type':'%d', 'limit':'%d','ip':'%s', 'port':'%d'},",
                            i, nodes[i].type, nodes[i].limit, nodes[i].ip, nodes[i].port);
                }
            }
            --p;
            p += sprintf(p, "}})");
            n = p - s;
        }
        MUTEX_UNLOCK(mmdb->mutex_state);
    }
    return n;
}
void mmdb_qmutex_lock(MMDB *mmdb, int qid)
{
    if(mmdb && qid > 0)
    {
#ifdef HAVE_PTHREAD
        pthread_mutex_lock(&(mmdb->qmutexs[qid%M_MUTEX_MAX]));
#endif
    }
    return ;
}

void mmdb_qmutex_unlock(MMDB *mmdb, int qid)
{
    if(mmdb && qid > 0)
    {
#ifdef HAVE_PTHREAD
        pthread_mutex_unlock(&(mmdb->qmutexs[qid%M_MUTEX_MAX]));
#endif
    }
    return ;
}

void mmdb_pmutex_lock(MMDB *mmdb, int pid)
{
    if(mmdb && pid > 0)
    {
#ifdef HAVE_PTHREAD
        pthread_mutex_lock(&(mmdb->pmutexs[pid%M_MUTEX_MAX]));
#endif
    }
    return ;
}

void mmdb_pmutex_unlock(MMDB *mmdb, int pid)
{
    if(mmdb && pid > 0)
    {
#ifdef HAVE_PTHREAD
        pthread_mutex_unlock(&(mmdb->pmutexs[pid%M_MUTEX_MAX]));
#endif
    }
    return ;
}

/* mmdb pop sorting map */
void *mmdb_pop_stree(MMDB *mmdb)
{
    void *stree = NULL;
    int x = 0;

    if(mmdb)
    {
        MUTEX_LOCK(mmdb->mutex_stree);
        if(mmdb->nqstrees > 0)
        {
            x = --(mmdb->nqstrees);
            stree = mmdb->qstrees[x];
            mmdb->qstrees[x] = NULL;
        }
        else
        {
            stree = mtree64_init();
            mmdb->stree_total++;
            //ACCESS_LOGGER(mmdb->logger, "qres_total:%d stree_total:%d", mmdb->qres_total, mmdb->stree_total);
        }
        MUTEX_UNLOCK(mmdb->mutex_stree);
    }
    return stree;
}

/* push mtree */
void mmdb_push_stree(MMDB *mmdb, void *stree)
{
    int x = 0;

    if(mmdb && stree)
    {
        MUTEX_LOCK(mmdb->mutex_stree);
        if(mmdb->nqstrees < M_STREE_MAX)
        {
            MTREE64_RESET(stree);
            x = mmdb->nqstrees++;
            mmdb->qstrees[x] = stree;
        }
        else
        {
            MTREE64_CLEAN(stree);
            mmdb->stree_total--;
        }
        MUTEX_UNLOCK(mmdb->mutex_stree);
    }
    return ;
}

/* mmdb pop qres */
void *mmdb_pop_qres(MMDB *mmdb)
{
    void *qres = NULL;
    int x = 0;

    if(mmdb)
    {
        MUTEX_LOCK(mmdb->mutex_qres);
        if(mmdb->nqqres > 0)
        {
            x = --(mmdb->nqqres);
            qres = mmdb->qqres[x];
            mmdb->qqres[x] = NULL;
        }
        else
        {
            if((qres = xmm_mnew(sizeof(QRES))))
                mmdb->qres_total++;
            memset(qres, 0, sizeof(QRES));
            //ACCESS_LOGGER(mmdb->logger, "qres_total:%d stree_total:%d", mmdb->qres_total, mmdb->stree_total);
        }
        MUTEX_UNLOCK(mmdb->mutex_qres);
    }
    return qres;
}

/* push qres */
void mmdb_push_qres(MMDB *mmdb, void *qres)
{
    int x = 0;

    if(mmdb && qres)
    {
        MUTEX_LOCK(mmdb->mutex_qres);
        if(mmdb->nqqres < M_QRES_MAX)
        {
            memset(qres, 0, sizeof(QRES));
            x = mmdb->nqqres++;
            mmdb->qqres[x] = qres;
        }
        else
        {
            xmm_free(qres, sizeof(QRES));
            mmdb->qres_total--;
        }
        MUTEX_UNLOCK(mmdb->mutex_qres);
    }
    return ;
}

#define CHECK_QTASKIO(mmdb, rid)                                                            \
do                                                                                          \
{                                                                                           \
    if((off_t)((off_t)rid * (off_t)sizeof(QTASK)) >= mmdb->qtaskio.end)                     \
    {                                                                                       \
        mmdb->qtaskio.old = mmdb->qtaskio.end;                                              \
        mmdb->qtaskio.end = (off_t)((rid / M_QTASK_BASE)+1)                                 \
        * (off_t)sizeof(QTASK) * (off_t)M_QTASK_BASE;                                       \
        if(ftruncate(mmdb->qtaskio.fd, mmdb->qtaskio.end) != 0)break;                       \
        if(mmdb->qtaskio.map)                                                               \
        {                                                                                   \
            memset(mmdb->qtaskio.map+mmdb->qtaskio.old,                                     \
                    0,mmdb->qtaskio.end - mmdb->qtaskio.old);                               \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* query taskid */
int mmdb_qid(MMDB *mmdb, char *s, char *es)
{
    char line[M_BUF_SIZE], *p = NULL;
    unsigned char key[MD5_LEN];
    int qid = -2, n = 0;

    if(mmdb && s && es && mmdb->qmap && es > s)
    {
        p = line;
        while(s < es)
        {
            if(*s >= 'A' && *s <= 'Z')
            {
                *p++ = *s++ - ('A' - 'a');
            }
            else *p++ = *s++; 
        }
        n = p - line;
        memset(key, 0, MD5_LEN);
        md5((unsigned char *)line, n, key);
        if((qid = mmtrie_xadd(MMTR(mmdb->qmap), (char *)key, MD5_LEN)) > 0)
        {
            MUTEX_LOCK(mmdb->mutex_qid);
            if(qid > mmdb->state->qid_max) 
            {
                CHECK_QTASKIO(mmdb, qid);
                mmdb->state->qid_max = qid;
            }
            MUTEX_UNLOCK(mmdb->mutex_qid);
        }
        else
        {
            FATAL_LOGGER(mmdb->logger, "id:%d mmtrie_xadd[%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x], %s", qid, key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7], key[8], key[9], key[10], key[11], key[12], key[13], key[14],key[15], strerror(errno));
        }
    }
    return qid;
}

/* query taskid */
int mmdb_qstatus(MMDB *mmdb, int qid, int lifetime, int *new_query, int *have_res_cache)
{
    int status = -1, res_cache = 0;
    time_t now = 0, modtime = 0;
    QTASK *qtasks = NULL;

    if(mmdb && qid <= mmdb->state->qid_max)
    {
        mmdb->state->querys_total++;
        mmdb_qmutex_lock(mmdb, qid);
        if((qtasks = (QTASK *)mmdb->qtaskio.map))
        {
            if(qtasks[qid].mod_time < mmdb->state->cache_mod_time) 
            {
                qtasks[qid].recid = 0;
                qtasks[qid].mod_time = 0;
            }
            now = time(NULL);
            if(qtasks[qid].mod_time > mmdb->start_time 
                    && (qtasks[qid].mod_time +  lifetime) > now && qtasks[qid].recid > 0 
                    && (modtime = db_get_modtime(PDB(mmdb->rdb), qtasks[qid].recid)) > 0
                    && (modtime +  lifetime) > now)
            {
                res_cache = qtasks[qid].recid;
                mmdb->state->cache_hits++;
            }
            if(qtasks[qid].mod_time < mmdb->start_time)
            {
                memset(&(qtasks[qid]), 0, sizeof(QTASK));
                qtasks[qid].mod_time = time(NULL);
                qtasks[qid].status = M_STATUS_QUERY;
                status = M_STATUS_FREE;
                *new_query = 1;
            }
            else
            {
                status = qtasks[qid].status;
                if((status == M_STATUS_FREE && res_cache == 0)
                    || (qtasks[qid].mod_time + lifetime) < now) 
                {
                    *new_query = 1;
                    qtasks[qid].status = M_STATUS_QUERY;
                }
            }
            if(have_res_cache) *have_res_cache = res_cache;
        }
        mmdb_qmutex_unlock(mmdb, qid);
    }
    return status;
}

/* reset status */
int mmdb_reset_qstatus(MMDB *mmdb, int qid)
{
    QTASK *qtasks = NULL;
    int ret = -1;

    if(mmdb && qid > 0 && qid <= mmdb->state->qid_max)
    {
        mmdb_qmutex_lock(mmdb, qid);
        if((qtasks = (QTASK *)mmdb->qtaskio.map))
        {
            qtasks[qid].status = M_STATUS_FREE;
            qtasks[qid].mod_time = time(NULL);
            ret = 0;
        }
        mmdb_qmutex_unlock(mmdb, qid);
    }
    return ret;
}

/* over query status */
int mmdb_over_qstatus(MMDB *mmdb, int qid)
{
    unsigned char key[MD5_LEN], *str = NULL;
    QTASK *qtasks = NULL;
    int ret = -1, n = 0;
    time_t mod_time = 0;
    IQUERY lquery = {0};

    if(mmdb && qid > 0 && qid <= mmdb->state->qid_max)
    {
        mmdb_qmutex_lock(mmdb, qid);
        if((qtasks = (QTASK *)mmdb->qtaskio.map))
        {
            str = (unsigned char *)(&lquery);
            md5(str, sizeof(IQUERY), key);
            qtasks[qid].recid = db_xcheck(PDB(mmdb->rdb), (char *)key, MD5_LEN, &n, &mod_time);
            qtasks[qid].status = M_STATUS_FREE;
            qtasks[qid].mod_time = time(NULL);
            ret = 0;
        }
        mmdb_qmutex_unlock(mmdb, qid);
    }
    return ret;
}
/* mmdb set qparser */
int mmdb_set_query(MMDB *mmdb, int qid, IQUERY *query, int nquerys, 
        int *new_query, int lifetime, int *have_res_cache)
{
    unsigned char key[MD5_LEN], *str = NULL;
    int ret = -1, n = 0, i = 0, recid = 0;
    QTASK *qtasks = NULL;
    time_t mod_time = 0;
    QRES *qres = NULL;
    IQUERY lquery = {0};

    if(mmdb && qid > 0 && qid <= mmdb->state->qid_max && query)
    {
        if(qid != query->qid || query->nqterms > IB_FIELDS_MAX)
        {
            FATAL_LOGGER(mmdb->logger, "qid:%d query->qid:%d query->ntop:%d nterms:%d", qid, query->qid, query->ntop, query->nqterms);
            return ret;

        }
        memcpy(&lquery, query, sizeof(IQUERY));
        lquery.from = 0;
        lquery.count = 0;
        lquery.qid = 0;
        lquery.ravgdl = 0.0f;
        memset(&(lquery.display), 0, sizeof(IDISPLAY) * IB_FIELDS_MAX);
        for(i = 0; i < lquery.nqterms; ++i) 
        {
            lquery.qterms[i].idf = 0.0f;
        }
        str = (unsigned char *)(&lquery);
        md5(str, sizeof(IQUERY), key);
        //REALLOG(mmdb->logger, "qid:%d md5[%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x] operator[%d/%d] flag:%d", query->qid, key[0], key[1],key[2],key[3],key[4],key[5],key[6],key[7],key[8],key[9],key[10],key[11],key[12],key[13],key[14],key[15], lquery.operators.bitsand, lquery.operators.bitsnot, lquery.flag);
        //if(lifetime == 0) lifetime = mmdb->state->cache_life_time;
        recid = db_xcheck(PDB(mmdb->rdb), (char *)key, MD5_LEN, &n, &mod_time);
        mmdb_qmutex_lock(mmdb, qid);
        if((qtasks = (QTASK *)(mmdb->qtaskio.map)))
        {
            ret = qid;
            if(qtasks[qid].mod_time < mmdb->state->cache_mod_time) 
            {
                qtasks[qid].mod_time = 0;
                qtasks[qid].recid = 0;
            }
            qtasks[qid].recid = recid;
            if(new_query) *new_query = 1;
            if(qtasks[qid].mod_time > mmdb->start_time 
                    && (mod_time + lifetime) > time(NULL) 
                    && qtasks[qid].recid > 0 && n > 0)
            {
                if(have_res_cache) 
                {
                    *have_res_cache = 1;
                    mmdb->state->cache_hits++;
                }
                if(new_query) *new_query = 0;
                qtasks[qid].status = M_STATUS_FREE;
            }
            if(new_query && *new_query > 0)
            {
                if((qres = qtasks[qid].qres) == NULL)
                {
                    qtasks[qid].qres = qres = mmdb_pop_qres(mmdb);
                    if(qres) qres->ntasks = nquerys;
                }
                qtasks[qid].left = nquerys;
                qtasks[qid].ntop = query->ntop;
                qtasks[qid].flag = query->flag;
                if(nquerys == 0)
                {
                    qtasks[qid].status = M_STATUS_FREE;
                    qtasks[qid].mod_time = 0;
                }
                if(qres)
                {
                    qres->qset.nqterms = query->nqterms;
                    memcpy(&(qres->qset.qterms), &(query->qterms), 
                            sizeof(QTERM) * query->nqterms);
                    memcpy(&(qres->qset.displaylist), &(query->display), 
                            sizeof(IDISPLAY) * IB_FIELDS_MAX);
                }
                else ret = -1;
            }
        }
        mmdb_qmutex_unlock(mmdb, qid);
    }
    return ret;
}


/* mmdb over merge */
int mmdb_over_merge(MMDB *mmdb, int qid, int pid, CQRES *cqres, 
        IQSET *qset, IRECORD *recs, int *nrecs, int error)
{
    int64_t score = 0, id = 0, xlong = 0, xdata = 0;
    void *map = NULL, *groupby = NULL;
    int i = 0, j = 0, x = 0, n = 0, to = 0; 
    QTASK *qtasks = NULL;
    QPAGE *qpages = NULL;

    if(mmdb && qid > 0 && (qtasks = (QTASK *)(mmdb->qtaskio.map)) 
            && (map = qtasks[qid].map) && cqres)
    {
        ACCESS_LOGGER(mmdb->logger, "over_query(%d/%d) pid:%d/%d cqres:%p qset:%p recs:%p nrecs:%p total:%d", qid, mmdb->state->qid_max, pid, mmdb->state->pid_max, cqres, qset, recs, nrecs, MTREE64_TOTAL(map));
        //save records
        i = 0;
        while(MTREE64_TOTAL(map) > 0)
        {
            id = 0;
            if((IB_QUERY_RSORT & qtasks[qid].flag)){MTREE64_POP_MAX(map, &score, &id);}
            else {MTREE64_POP_MIN(map, &score, &id);}
            if(id && i < IB_TOPK_NUM)
            {
                cqres->records[i].score = score;
                cqres->records[i].globalid = id;
            }
            ++i;
        }
        if((groupby = qtasks[qid].groupby)
                && (cqres->qset.res.ngroups = PIMX(groupby)->count) > 0)
        {
            j = 0;
            do
            {
                IMMX_POP_MIN(groupby, xlong, xdata);
                if(j < IB_GROUP_MAX)
                {
                    cqres->qset.res.groups[j].key = xlong;
                    cqres->qset.res.groups[j].val = xdata;
                }
                ++j;
            }while(PIMX(groupby)->count > 0);
            if(cqres->qset.res.ngroups > IB_GROUP_MAX)
            {
                WARN_LOGGER(mmdb->logger, "large groups[%d] qid:%d", cqres->qset.res.ngroups, qid);
            }
        }
        if(i > 0)
        {
            if(i > IB_TOPK_NUM) i = IB_TOPK_NUM;
            cqres->qset.res.qid = qid;
            cqres->qset.res.count = i;
            if(pid > 0 && qset && recs && nrecs
                    && pid <= (mmdb->qpageio.end/(off_t)sizeof(QPAGE)) 
                    && (qpages = (QPAGE *)(mmdb->qpageio.map)))
            {
                memcpy(qset, &(cqres->qset), sizeof(IQSET));
                x = qpages[pid].from;
                to = x + qpages[pid].count;
                if(to > qset->res.count) n = qset->res.count - x;
                else n = qpages[pid].count;
                if((qset->count = n) > 0)
                {
                    memcpy(recs, &(cqres->records[x]), sizeof(IRECORD) * n);
                    *nrecs = n;
                }
            }
        }
        mmdb_push_qres(mmdb, qtasks[qid].qres);
        mmdb_push_stree(mmdb, qtasks[qid].map);
        mmdb_push_mmx(mmdb, qtasks[qid].groupby);
        qtasks[qid].qres = NULL;
        qtasks[qid].map = NULL;
        qtasks[qid].groupby = NULL;
        if(error) qtasks[qid].mod_time = 0;
        else qtasks[qid].mod_time = time(NULL);
        qtasks[qid].status = M_STATUS_FREE;
        return i;
    }
    return 0;
}

/* over query /merge result */
int mmdb_merge(MMDB *mmdb, int qid, int nodeid, IRES *res, IRECORD *records, 
        int pid, CQRES *cqres, IQSET *qset, IRECORD *recs, int *nrecs, int *error)
{
    void *map = NULL;// *old = NULL;
    int i = 0, ret = -1;
    QTASK *qtasks = NULL;
    QRES *qres = NULL;
    void *groupby = NULL;
    //int64_t score = 0;

    if(mmdb && qid > 0 && res)
    {
        if(qid != res->qid)
        {
            FATAL_LOGGER(mmdb->logger, "Invalid qid:%d node:%d to res->qid:%d res:%p records:%p", qid, nodeid, res->qid, res, records);
            return -1;
        }
        mmdb_qmutex_lock(mmdb, qid);
        if((qtasks = (QTASK *)mmdb->qtaskio.map))
        {
            if((map = qtasks[qid].map) == NULL)
                qtasks[qid].map = map = mmdb_pop_stree(mmdb);
            if((groupby = qtasks[qid].groupby) == NULL)
                qtasks[qid].groupby = groupby = mmdb_pop_mmx(mmdb);
            if((qres = qtasks[qid].qres) == NULL)
            {
                qtasks[qid].qres = qres = mmdb_pop_qres(mmdb);
                if(qres) qres->ntasks = qtasks[qid].left;
            }
            if(qres && res->doctotal <= 0) qres->nerrors++;
            if(res->count > 0 && records && map && qres)
            {
                if(qtasks[qid].ntop == 0)
                {
                    qtasks[qid].ntop = IB_TOPK_NUM;
                    FATAL_LOGGER(mmdb->logger, "map[%p]->count:%d ntop:%d left:%d status:%d records:%p node:%d res[%p]->qid:%d res->count:%d ", map, MTREE64_TOTAL(map), qtasks[qid].ntop, qtasks[qid].left, qtasks[qid].status, records, nodeid,res,  res->qid, res->count);
                }
                if(qtasks[qid].flag & IB_QUERY_RSORT)
                {
                    for(i = 0; i < res->count; i++)
                    {
                        if(MTREE64_TOTAL(map) > 0 && MTREE64_TOTAL(map) >= qtasks[qid].ntop
                                && records[i].score < MTREE64_MINK(map)) break;
                        if(MTREE64_TOTAL(map) == 0 || MTREE64_TOTAL(map) < qtasks[qid].ntop)
                        {
                            MTREE64_PUSH(map, records[i].score, records[i].globalid);
                        }
                        else
                        {
                            if(MTREE64_TOTAL(map) >= qtasks[qid].ntop && records[i].score >= MTREE64_MINK(map))
                            {
                                MTREE64_POP_MIN(map, NULL, NULL);
                                MTREE64_PUSH(map, records[i].score, records[i].globalid);
                            }
                        }
                    }
                }
                else
                {
                    for(i = 0; i < res->count; i++)
                    {
                        if(MTREE64_TOTAL(map) > 0 && MTREE64_TOTAL(map) >= qtasks[qid].ntop
                                && records[i].score > MTREE64_MAXK(map)) break;
                        if(MTREE64_TOTAL(map) == 0 || MTREE64_TOTAL(map) < qtasks[qid].ntop)
                        {
                            MTREE64_PUSH(map, records[i].score, records[i].globalid);
                        }
                        else
                        {
                            if(MTREE64_TOTAL(map) >= qtasks[qid].ntop && records[i].score <= MTREE64_MAXK(map))
                            {
                                MTREE64_POP_MAX(map, NULL, NULL);
                                MTREE64_PUSH(map, records[i].score, records[i].globalid);
                            }
                        }
                    }
                }
            }
            /* merge count */
            if(qres && res)
            {
                qres->qset.res.total += res->total; 
                qres->qset.res.doctotal += res->doctotal; 
                if(res->io_time > qres->qset.res.io_time) 
                    qres->qset.res.io_time = res->io_time;
                if(res->sort_time > qres->qset.res.sort_time) 
                    qres->qset.res.sort_time = res->sort_time;
            }
            /* merge groups */
            if(res->ncatgroups > 0)
            {
                for(i = 0; i < IB_CATEGORY_MAX; i++)
                {
                    if(res->catgroups[i] > 0)
                    {
                        if(qres->qset.res.catgroups[i] == 0)
                            qres->qset.res.ncatgroups++;
                        qres->qset.res.catgroups[i] += res->catgroups[i];
                    }
                }
            }
            if(res->ngroups > 0)
            {
                for(i = 0; i < res->ngroups; i++)
                {
                    IMMX_SUM(groupby, (res->groups[i].key), (res->groups[i].val));
                }
                qres->qset.res.flag |= res->flag;
            }
            if(qres && qres->ntasks > 0 && qres->nerrors > 0) 
            {
                //WARN_LOGGER(mmdb->logger, "qid:%d nodeid:%d ntasks:%d errors:%d doctotal:%d", qid, nodeid, qres->ntasks, qres->nerrors, res->doctotal);
                *error = qres->nerrors;
            }
            if(--(qtasks[qid].left) == 0) 
            {
                cqres->recid = qtasks[qid].recid;
                cqres->qid = qid;
                memcpy(&(cqres->qset), &(qres->qset), sizeof(IQSET));
                mmdb_over_merge(mmdb, qid, pid, cqres, qset, recs, nrecs, qres->nerrors);
            }
            //ACCESS_LOGGER(mmdb->logger, "qid:%d left:%d", qid, qtasks[qid].left);
            ret = qtasks[qid].left;
        }
        mmdb_qmutex_unlock(mmdb, qid);
    }
    return ret;
}

#define CHECK_QPAGEIO(mmdb, rid)                                                            \
do                                                                                          \
{                                                                                           \
    if(((off_t)rid * (off_t)sizeof(QPAGE)) >= mmdb->qpageio.end)                            \
    {                                                                                       \
        mmdb->qpageio.old = mmdb->qpageio.end;                                              \
        mmdb->qpageio.end = (off_t)((rid / M_QPAGE_BASE)+1)                                 \
            * (off_t)sizeof(QPAGE) * (off_t)M_QPAGE_BASE;                                   \
        if(ftruncate(mmdb->qpageio.fd, mmdb->qpageio.end) != 0)break;                       \
        if(mmdb->qpageio.map)                                                               \
        {                                                                                   \
            memset(mmdb->qpageio.map+mmdb->qpageio.old, 0,                                  \
                    mmdb->qpageio.end - mmdb->qpageio.old);                                 \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* set qres */
int mmdb_set_cqres(MMDB *mmdb, CQRES *cqres)
{
    if(mmdb && cqres && cqres->qid > 0 && cqres->qid <= mmdb->state->qid_max)
    {
        if(cqres->qset.res.count > 0)
            return db_set_data(PDB(mmdb->rdb), cqres->recid, (char *)cqres, sizeof(CQRES));
        else
        {
            return db_del_data(PDB(mmdb->rdb), cqres->recid);
        }
    }
    return -1;
}

/* pop summary qres */
int mmdb_get_cqres(MMDB *mmdb, int pid, IQSET *qset, IRECORD *records)
{
    int qid = -1, i = 0, to = 0, n = 0, recid = 0;
    //char *data = NULL, *s = NULL;
    QTASK *qtasks = NULL;
    QPAGE *qpages = NULL;
    CQRES cqres;

    if(mmdb && pid > 0 && pid <= mmdb->state->pid_max && qset && records)
    {
        mmdb_pmutex_lock(mmdb, pid);
        //ACCESS_LOGGER(mmdb->logger, "get_qres() pid:%d", pid);
        if((qtasks = (QTASK *)mmdb->qtaskio.map) && (qpages = (QPAGE *)mmdb->qpageio.map) 
                && qpages[pid].count > 0 && (qid =  qpages[pid].qid) > 0 
                && qtasks[qid].recid > 0)
        {
            //&& (qpages[pid].mod_time != qtasks[qid].mod_time) /* check is update */
            //ACCESS_LOGGER(mmdb->logger, "get_qres() qid:%d pid:%d recid:%d", qid, pid, qtasks[qid].recid);
            mmdb_qmutex_lock(mmdb, qid);
            /* check/read page N records */
            if(db_get_data_len(PDB(mmdb->rdb), qtasks[qid].recid) > 0)
            {
                recid = qtasks[qid].recid;
                /* set modified time */
                qpages[pid].mod_time = qtasks[qid].mod_time;
                i = qpages[pid].from;
                to = i + qpages[pid].count;
                n = qpages[pid].count;
            }
            else 
                qtasks[qid].recid = 0;
            mmdb_qmutex_unlock(mmdb, qid);
            //ACCESS_LOGGER(mmdb->logger, "get_qres() qid:%d pid:%d recid:%d from:%d to:%d n:%d", qid, pid, qtasks[qid].recid, i, to);
        }
        mmdb_pmutex_unlock(mmdb, pid);
        if(recid > 0 && (db_read_data(PDB(mmdb->rdb), recid, (char *)&cqres)) > 0)
        {
            memcpy(qset, &(cqres.qset), sizeof(IQSET));
            if(to > qset->res.count) n = qset->res.count - i;
            //ACCESS_LOGGER(mmdb->logger, "qid:%d pid:%d recid:%d n:%d from:%d to:%d count:%d", qid, pid, qtasks[qid].recid, n, i, to, qset->res.count);
            if((qset->count = n) > 0)
            {
                memcpy(records, &(cqres.records[i]), sizeof(IRECORD) * n);
                //ACCESS_LOGGER(mmdb->logger, "qid:%d pid:%d recid:%d n:%d from:%d to:%d", qid, pid, qtasks[qid].recid, n, i, to);
            }
        }
    }
    return n;
}

/* check task */
int mmdb_pid(MMDB *mmdb, int qid, int from, int count)
{
    char line[M_LINE_SIZE];
    QPAGE *qpages = NULL;
    int pid = -1, n = 0;

    if(mmdb && qid > 0 && qid <= mmdb->state->qid_max)
    {
        if((n = sprintf(line, "%d:%d-%d", qid, from, count)) > 0
                && (pid = db_data_id(PDB(mmdb->pdb), line, n)) > 0)
        {
            MUTEX_LOCK(mmdb->mutex_pid);
            if(pid > mmdb->state->pid_max) 
            {
                CHECK_QPAGEIO(mmdb, pid);
                mmdb->state->pid_max = pid;
            }
            MUTEX_UNLOCK(mmdb->mutex_pid);
            mmdb_pmutex_lock(mmdb, pid);
            if((qpages = (QPAGE *)mmdb->qpageio.map)) 
            {
                qpages[pid].from = from;
                qpages[pid].count = count;
                qpages[pid].qid  = qid;
            }
            mmdb_pmutex_unlock(mmdb, pid);
        }
    }
    return pid;
}
#define LLD(x) ((long long int) x)
#define IU(x) ((unsigned int) x)
/* state */
int mmdb_qstate(MMDB *mmdb, char *line)
{
    time_t now = 0, secs = 0, last_time = 0;
    int64_t last_querys = 0, last_hits = 0;
    float scale = 0.0, last_scale = 0.0;
    int n = -1, last_avg = 0;
    char *s =  NULL;

    if(mmdb && (s = line) && mmdb->state)
    {
        MUTEX_LOCK(mmdb->mutex_state);
        if((now = time(NULL)) > 0 && (secs = (now - mmdb->start_time)) > 0) 
        {
            if(mmdb->state->querys_total > 0)
                scale = (float)mmdb->state->cache_hits/(float)mmdb->state->querys_total;
            last_hits = mmdb->state->cache_hits - mmdb->state->last_hits;
            if((last_querys = (mmdb->state->querys_total - mmdb->state->last_querys)) > 0)
            {
                last_scale = (float)last_hits/(float)last_querys;
            }
            if((last_time = (now - mmdb->state->last_time)) > 0)
            {
                last_avg = (int)(last_querys / last_time);
            }
            mmdb->state->last_time = now;
            mmdb->state->last_querys = mmdb->state->querys_total;
            mmdb->state->last_hits = mmdb->state->cache_hits;
            n = sprintf(line, "({'time':'%u', 'querys':'%lld','querys_avg':'%u',"
                    "'cache_hits':'%lld','hits_scale':'%f','last_time':'%u',"
                    "'last_querys':'%lld', 'last_avg':'%u', 'last_hits':'%lld', "
                    "'last_hits_scale':'%f'})", IU(secs), LLD(mmdb->state->querys_total),
                    IU(mmdb->state->querys_total/IU(secs)), LLD(mmdb->state->cache_hits),
                    scale, IU(last_time), LLD(last_querys), IU(last_avg), 
                    LLD(last_hits), last_scale);
        }
        MUTEX_UNLOCK(mmdb->mutex_state);
    }
    return n;
}

int mmdb_check_summary_len(MMDB *mmdb, int pid)
{
    QPAGE *qpages = NULL;
    QTASK *qtasks = NULL;
    int n = 0, qid = 0;

    if(mmdb)
    {
        if(pid <= mmdb->state->pid_max && (qpages = (QPAGE *)mmdb->qpageio.map)
                && (qid = qpages[pid].qid) <= mmdb->state->qid_max
                && (qtasks = (QTASK *)mmdb->qtaskio.map))
        {
            if(qpages[pid].mod_time == qtasks[qid].mod_time)
            {
                n =  db_get_data_len(PDB(mmdb->pdb), pid);
            }
        }
    }
    return n;
}

/* read summary */
int mmdb_read_summary(MMDB *mmdb, int pid, char *summary)
{

    if(mmdb && pid > 0 && summary)
    {
        return db_read_data(PDB(mmdb->pdb), pid, summary);
    }
    return -1;
}

/* over summary */
int mmdb_set_summary(MMDB *mmdb, int pid, char *summary, int nsummary)
{
    QPAGE *qpages = NULL;
    int ret = -1, ok = -1;

    if(mmdb && pid > 0 && summary && nsummary > 0 && pid <= mmdb->state->pid_max)
    {
        if((qpages = (QPAGE *)mmdb->qpageio.map) && qpages[pid].count > 0)
        {
            qpages[pid].up_time = time(NULL);
            ok = 0;
        }
        if(ok == 0) ret = db_set_data(PDB(mmdb->pdb), pid, summary, nsummary);
    }
    return ret;
}

/* mmdb free summary */
void mmdb_free_summary(MMDB *mmdb, char *summary, int nsummary)
{
    if(mmdb && summary)
    {
        db_free_data(PDB(mmdb->pdb), summary, nsummary);
    }
    return ;
}

/* clear cache */
void mmdb_clear_cache(MMDB *mmdb)
{
    if(mmdb)
    {
        MUTEX_LOCK(mmdb->mutex);
        MUTEX_LOCK(mmdb->mutex_qid);
        MUTEX_LOCK(mmdb->mutex_pid);
        mmtrie_destroy(MMTR(mmdb->qmap));
        db_destroy(PDB(mmdb->rdb)); 
        db_destroy(PDB(mmdb->pdb)); 
        mmdb->state->qid_max = 0;
        mmdb->state->pid_max = 0;
        mmdb->state->cache_mod_time = time(NULL);
        MUTEX_UNLOCK(mmdb->mutex_pid);
        MUTEX_UNLOCK(mmdb->mutex_qid);
        MUTEX_UNLOCK(mmdb->mutex);
    }
    return ;
}

/* clean mmdb */
void mmdb_clean(MMDB *mmdb)
{
    int i = 0;

    if(mmdb)
    {
        if(mmdb->queue){iqueue_clean(mmdb->queue);}
        MUTEX_DESTROY(mmdb->mutex);
        MUTEX_DESTROY(mmdb->mutex_qid);
        MUTEX_DESTROY(mmdb->mutex_pid);
        MUTEX_DESTROY(mmdb->mutex_mmx);
        MUTEX_DESTROY(mmdb->mutex_state);
        MUTEX_DESTROY(mmdb->mutex_stree);
        MUTEX_DESTROY(mmdb->mutex_qres);
        if(mmdb->stateio.map) munmap(mmdb->stateio.map, mmdb->stateio.size);
        if(mmdb->stateio.fd > 0) close(mmdb->stateio.fd);
        if(mmdb->qtaskio.map) munmap(mmdb->qtaskio.map, mmdb->qtaskio.size);
        if(mmdb->qtaskio.fd > 0) close(mmdb->qtaskio.fd);
        if(mmdb->qpageio.map) munmap(mmdb->qpageio.map, mmdb->qpageio.size);
        if(mmdb->qpageio.fd > 0) close(mmdb->qpageio.fd);
        for(i = 0; i < mmdb->nqstrees; i++)
        {
            if(mmdb->qstrees[i]) MTREE64_CLEAN(mmdb->qstrees[i]);
        }
        for(i = 0; i < mmdb->nqmmxs; i++)
        {
            if(mmdb->qmmxs[i]) IMMX_CLEAN(mmdb->qmmxs[i]);
        }
        for(i = 0; i < mmdb->nqqres; i++)
        {
            if(mmdb->qqres[i]) xmm_free(mmdb->qqres[i], sizeof(QRES));
        }
#ifdef HAVE_PTHREAD
        for(i = 0; i < M_MUTEX_MAX; i++)
        {
            pthread_mutex_destroy(&(mmdb->qmutexs[i]));
            pthread_mutex_destroy(&(mmdb->pmutexs[i]));
        }
#endif
        if(mmdb->qmap) mmtrie_clean(MMTR(mmdb->qmap));
        if(mmdb->rdb)db_clean(PDB(mmdb->rdb));
        if(mmdb->pdb)db_clean(PDB(mmdb->pdb));
        LOGGER_CLEAN(mmdb->logger);
        xmm_free(mmdb, sizeof(MMDB));
    }
    return ;
}

/* initialize mmdb */
MMDB *mmdb_init()
{
    MMDB *mmdb = NULL;

    if((mmdb = (MMDB *)xmm_mnew(sizeof(MMDB))))
    {
        mmdb->set_basedir   = mmdb_set_basedir;
        mmdb->add_node      = mmdb_add_node;
        mmdb->del_node      = mmdb_del_node;
        mmdb->list_nodes    = mmdb_list_nodes;
        mmdb->pop_node      = mmdb_pop_node;
        mmdb->clean         = mmdb_clean;
        mmdb->queue         = iqueue_init();
        MUTEX_INIT(mmdb->mutex);
        MUTEX_INIT(mmdb->mutex_qid);
        MUTEX_INIT(mmdb->mutex_pid);
        MUTEX_INIT(mmdb->mutex_mmx);
        MUTEX_INIT(mmdb->mutex_state);
        MUTEX_INIT(mmdb->mutex_stree);
        MUTEX_INIT(mmdb->mutex_qres);
#ifdef HAVE_PTHREAD
        int i = 0;
        for(i = 0; i < M_MUTEX_MAX; i++)
        {
            pthread_mutex_init(&(mmdb->qmutexs[i]), NULL);
            pthread_mutex_init(&(mmdb->pmutexs[i]), NULL);
        }
#endif
    }
    else mmdb = NULL;
    return mmdb;
}

#ifdef _DEBUG_MMDB
int main()
{
    char *dir = "/tmp/mmdb", buf[M_BUF_SIZE], *s = NULL, *es = NULL;
    int n = 0, nodeid = 0, id = 0;
    SNODE node = {0};
    MMDB *mmdb = NULL;

    if((mmdb = mmdb_init()))
    {
        mmdb_set_basedir(mmdb, dir);
        mmdb_add_node(mmdb, M_NODE_DOCD, "10.0.6.81", 4936, 256);
        mmdb_add_node(mmdb, M_NODE_INDEXD, "10.0.6.82", 4936, 256);
        mmdb_add_node(mmdb, M_NODE_INDEXD, "10.0.6.83", 4936, 256);
        mmdb_add_node(mmdb, M_NODE_INDEXD, "10.0.6.84", 4936, 256);
        mmdb_add_node(mmdb, M_NODE_INDEXD, "10.0.6.85", 4936, 256);
        /* list */
        s = buf;
        es = buf + M_BUF_SIZE;
        if((n = mmdb_list_nodes(mmdb, s, es)) > 0)
        {
            buf[n] = '\0';
            fprintf(stdout, "%s\n", buf);
        }
        /* pop node */
        while((nodeid = mmdb_pop_node(mmdb, &node)) >= 0)
        {
            fprintf(stdout, "pop_node(%d):host[%s:%d] type[%d] limit[%d]\n", 
                    nodeid, node.ip, node.port, node.type, node.limit);
            id = nodeid;
        }
        /* del node */
        mmdb_del_node(mmdb, id);
        /* list */
        if((n = mmdb_list_nodes(mmdb, s, es)) > 0)
        {
            buf[n] = '\0';
            fprintf(stdout, "%s\n", buf);
        }
        mmdb_clean(mmdb);
    }
}
#endif
