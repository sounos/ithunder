#ifndef __QINDEX__H__
#define __QINDEX__H__
#include <mtask.h>
#include <dbase.h>
#include "mutex.h"
#define Q_IP_MAX               16
#define Q_NAME_MAX             64
#define Q_TASKS_MAX            64
#define Q_NODE_MAX             64
#define Q_PATH_MAX             256
#define Q_NODE_DOCD            0x00
#define Q_NODE_PARSERD         0x01
#define Q_NODE_INDEXD          0x02
#define Q_LINE_MAX             1024
#define Q_DBLINE_MAX           512
#define Q_LEFT_LEN             4096
#define Q_DBNAME_MAX           64
#define Q_FIELDNAME_MAX        64
#define Q_XPACKET_MAX          2000000000
#define Q_XPACKET_BASE         20000000
#define Q_TERM_SIZE            96
#define Q_BSTERM_MAX           10000000
#define Q_BSTERM_BASE          100000
#define Q_LINE_SIZE            1024
typedef struct _QTASK
{
    short status;
    short port;
    int   nodeid;
    int   last;
    int   popid;
    int   over;
    int   nxpackets;
    int   count;
    int   mmqid;
    int   nqueue;
    int   upid;
    int   upover;
    int   nupdates;
    int   upcount;
    off_t  bterm_mod_time;
    off_t  bterm_last_time;
    char  ip[Q_IP_MAX];
}QTASK;
typedef struct _QTNODE
{
    short status;
    short type;
    int first;
    int last;
    int total;
    int limit;
    int ntasks;
    char name[Q_NAME_MAX];
    QTASK tasks[Q_TASKS_MAX];
}QTNODE;

typedef struct _XPACKET
{
    short   status;
    short   nodeid;
    int     prev;
    int     next;
    int     crc;
}XPACKET;

typedef struct _BSTERM
{
    BTERM bterm;
    char term[Q_TERM_SIZE];
}BSTERM;

typedef struct _MINDEX
{
    MTASK mtask;
    DBASE db;
    BJSON request;
}MINDEX;

typedef struct _QXIO
{
    int     fd;
    int     bits;
    char    *map;
    off_t   old;
    off_t   end;
    off_t   size;
}QXIO;
typedef struct _QSTATE
{
    int   taskqid;
    int   xpackettotal;
    int   docpopid;
    int   popid;
    int   nnodes;
    int   nidxnodes;
    int   id_max;
    int   rootid;
    int   bterm_id_max;
    int   bits;
    off_t bterm_mod_time;
    QTNODE nodes[Q_NODE_MAX];
}QSTATE;
typedef struct _QINDEX
{
    int     log_level;
    int     qstatefd;
    int     qtask_server_port;
    int     qtask_commitid;
    int     qtask_queueid;
    int     db_port;
    MUTEX   *mutex;
    void    *logger;
    QSTATE  *state;
    void    *queue;
    void    *map;
    void    *idmap;
    void    *xdict;
    void    *mmqueue;
    void    *db;
    void    *update;
    void    *namemap;
    QXIO    xpacketio;
    QXIO    bstermio;
    char    qtask_server_host[Q_IP_MAX];
    char    db_host[Q_IP_MAX];
    char    db_key_name[Q_FIELDNAME_MAX];
    char    db_index_block_name[Q_FIELDNAME_MAX];
    char    basedir[Q_PATH_MAX];
}QINDEX;

/* initialize qindex */
QINDEX *qindex_init();
/* set basedir */
int qindex_set_basedir(QINDEX *, char *basedir);
/* added index node */
int qindex_add_node(QINDEX *, int type, char *name, int limit);
/* delete index node */
int qindex_del_node(QINDEX *, int nodeid);
/* set node limit */
int qindex_set_node_limit(QINDEX *, int nodeid, int limit);
/* list index nodes */
int qindex_list_nodes(QINDEX *, char *out, char *end);
/* add index task to node */
int qindex_add_task(QINDEX *, int nodeid, char *ip, int port);
/* delete index task from node */
int qindex_del_task(QINDEX *, int taskid);
/* get index task */
int qindex_pop_task(QINDEX *, QTASK *task);
/* push task to queue */
int qindex_push_task(QINDEX *qindex, int taskid);
/* list task as index nodeid */
int qindex_list_tasks(QINDEX *, int nodeid, char *out, char *end);
/* set qtask host */
int qindex_set_qtask_server(QINDEX *qindex, char *ip, int port, int commitid, int queueid);
/* set db */
int qindex_set_db(QINDEX *qindex, char *host, int port, char *key_name, char *index_block_name);
/* new xindex */
MINDEX *mindex_new();
/* clean hindex */
void mindex_clean(MINDEX *mindex);
/* working for get_data() and push_index() */
int qindex_work(QINDEX *qindex, MINDEX *mindex);
/* read index */
int qindex_read_index(QINDEX *qindex, int taskid, char *data, int *len, int *count);
/* over index */
int qindex_over_index(QINDEX *qindex, int taskid, int id);
/* read upindex */
int qindex_read_upindex(QINDEX *qindex, int taskid, char *data, int *len, int *count);
/* over upindex */
int qindex_over_upindex(QINDEX *qindex, int taskid, int upid);
/* set block terms status */
int qindex_set_bterm(QINDEX *qindex, char *term, int nterm, int status);
/* update bterms status */
int qindex_update_bterm(QINDEX *qindex, int termid, int status);
/* add block terms */
int qindex_add_bterm(QINDEX *qindex, char *term, int nterm);
/* del block terms */
int qindex_del_bterm(QINDEX *qindex, int termid);
/* list bterms */
int qindex_list_bterms(QINDEX *qindex, char *out);
/* update bterm */
int qindex_sync_bterms(QINDEX *qindex);
/* read bterm */
int qindex_read_bterms(QINDEX *qindex, int taskid, char *data, int ndata);
/* over update bterm */
int qindex_over_bterms(QINDEX *qindex, int taskid);
/* clean qindex */
void qindex_clean(QINDEX *qindex);
#endif
