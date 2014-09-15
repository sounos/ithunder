#ifndef _MMDB_H
#define _MMDB_H
#include <ibase.h>
#include "mutex.h"
#define M_IP_MAX            16
#define M_PATH_MAX          256
#define M_NODES_MAX         1024
#define M_NODE_QPARSERD     0x01
#define M_NODE_QDOCD        0x02
#define M_NODE_INDEXD       0x04
#define M_MAPS_MAX          2048
#define M_STREE_MAX         2048
#define M_QRES_MAX          2048
#define M_MMXS_MAX          2048
#define M_BUF_SIZE          65536
#define M_LINE_SIZE         1024
#define M_QTASK_MAX         200000000   
#define M_QTASK_BASE        1000000   
#define M_QPAGE_MAX         200000000   
#define M_QPAGE_BASE        1000000   
#define M_STATUS_FREE       0x00
#define M_STATUS_QUERY      0x01
#define M_PAGE_NUM_MAX      2000
#define M_CACHE_LIFE_TIME   60
#define M_TASK_CACHED       0x01
#define M_MUTEX_MAX         65536
/* indexd NODE */
typedef struct _SNODE
{
    int     status;
    int     port;
    int     type;
    int     limit;
    char    ip[M_IP_MAX];
}SNODE;
/* MMDB FILE IO */
typedef struct _MIO
{
    int     fd;
    int     status;
    char    *map;
    off_t   old;
    off_t   end;
    off_t   size;
}MIO;
/* MMDB STATE */
typedef struct _MSTATE
{
    int     cache_life_time;
    int     qid_max;
    int     pid_max;
    int     nnodes;
    time_t  last_time;
    time_t  cache_mod_time;
    int64_t querys_total;
    int64_t cache_hits;
    int64_t last_querys;
    int64_t last_hits;
    SNODE   nodes[M_NODES_MAX];
}MSTATE;
typedef struct _CQRES
{
    IQSET       qset;
    IRECORD     records[IB_TOPK_NUM];
    int 	    recid;
    int 	    qid;
}CQRES;
typedef struct _QRES
{
    IQSET       qset;
    int         ntasks;
    int         nerrors;
}QRES;
/* MMDB record */
typedef struct _QTASK
{
    short   status;
    short   left;
    int     ntop;
    int     recid;
    int     flag;
    time_t  mod_time;
    QRES    *qres;
    void    *map;
    void    *groupby;
}QTASK;
typedef struct _QPAGE
{
    short   from;
    short   count;
    int     qid;
    time_t  mod_time;
    time_t  up_time;
}QPAGE;
/* MMDB */
typedef struct _MMDB
{
    MIO     stateio;
    MIO     qtaskio;
    MIO     qpageio;
    MSTATE  *state;
    time_t  start_time;
    void    *qmap;
    void    *qstrees[M_STREE_MAX];
    void    *qqres[M_QRES_MAX];
    void    *qmmxs[M_MMXS_MAX];
    short     nqstrees;
    short    nqqres;
    int     nqmmxs;
    int     stree_total;
    int     qres_total;
    void    *rdb;
    void    *pdb;
    void    *queue;
    void    *logger;
    MUTEX   *mutex;
    MUTEX   *mutex_qid;
    MUTEX   *mutex_pid;
    MUTEX   *mutex_mmx;
    MUTEX   *mutex_state;
    MUTEX   *mutex_stree;
    MUTEX   *mutex_qres;
#ifdef HAVE_PTHREAD
    pthread_mutex_t qmutexs[M_MUTEX_MAX];
    pthread_mutex_t pmutexs[M_MUTEX_MAX];
#endif

    int     (*set_basedir)(struct _MMDB *, char *path);
    int     (*add_node)(struct _MMDB *, int type, char *ip, int port, int limit);
    int     (*update_node)(struct _MMDB *, int nodeid, char *ip, int port);
    int     (*del_node)(struct _MMDB *, int nodeid);
    int     (*pop_node)(struct _MMDB *, SNODE *);
    int     (*list_nodes)(struct _MMDB *, char *s, char *es);
    void    (*clean)(struct _MMDB *);
}MMDB;
/* initialize MMDB */
MMDB *mmdb_init();
/* set log level */
int mmdb_set_log_level(MMDB *mmdb, int log_level);
/* mmdb set cache life time (seconds) */
int mmdb_set_cache_life_time(MMDB *mmdb, int cache_life_time);
/* query id */
int mmdb_qid(MMDB *mmdb, char *s, char *es);
/* query status */
int mmdb_qstatus(MMDB *mmdb, int qid, int lifetime, int *new_query, int *have_res_cache);
/* reset qid status */
int mmdb_reset_qstatus(MMDB *mmdb, int qid);
/* over query status */
int mmdb_over_qstatus(MMDB *mmdb, int qid);
/* mmdb set qparser */
int mmdb_set_query(MMDB *mmdb, int qid, IQUERY *query, int nquerys, 
        int *new_query, int lifetime, int *have_res_cache);
/* merge query result */
int mmdb_merge(MMDB *mmdb, int qid, int nodeid, IRES *res, IRECORD *records, 
        int pid, CQRES *cqres, IQSET *qset, IRECORD *recs, int *nrecs, int *error);
/* set cache */
int mmdb_set_cqres(MMDB *mmdb, CQRES *cqres);
/* pop summary qset/records */
int mmdb_get_cqres(MMDB *mmdb, int pid, IQSET *qset, IRECORD *records);
/* state */
int mmdb_qstate(MMDB *mmdb, char *line);
/* get pid */
int mmdb_pid(MMDB *mmdb, int qid, int from, int count);
/* over summary */
int mmdb_set_summary(MMDB *mmdb, int pid, char *summary, int nsummary);
/* get summary */
int mmdb_check_summary_len(MMDB *mmdb, int pid);
/* check summary */
int mmdb_read_summary(MMDB *mmdb, int pid, char *summary);
/* free summary */
void mmdb_free_summary(MMDB *mmdb, char *summary, int nsummery);
/* mmdb destroy */
void mmdb_clear_cache(MMDB *mmdb);
/* clean mmdb */
void mmdb_clean(MMDB *);
#endif
