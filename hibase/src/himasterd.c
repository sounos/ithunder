#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <sys/resource.h>
#include <locale.h>
#include <sbase.h>
#include <ibase.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include "stime.h"
#include "base64.h"
#include "base64masterdhtml.h"
#include "http.h"
#include "iniparser.h"
#include "logger.h"
#include "timer.h"
#include "mtrie.h"
#include "mmdb.h"
#include "xmm.h"
#ifndef Q_BUF_SIZE
#define Q_BUF_SIZE              131072
#define Q_LINE_SIZE             65536
#endif
#define HTTP_LINE_MAX           65536
#define HTTP_RESP_OK            "HTTP/1.0 200 OK\r\nContent-Length: 0\r\n\r\n"
#define HTTP_BAD_REQUEST        "HTTP/1.0 400 Bad Request\r\nContent-Length: 0\r\n\r\n"
#define HTTP_NOT_FOUND          "HTTP/1.0 404 Not Found\r\nContent-Length: 0\r\n\r\n" 
#define HTTP_NOT_MODIFIED       "HTTP/1.0 304 Not Modified\r\nContent-Length: 0\r\n\r\n"
#define HTTP_NO_CONTENT         "HTTP/1.0 204 No Content\r\nContent-Length: 0\r\n\r\n"
#define EVWAIT_TIMEOUT          100000
#ifndef LL64
#define LL64(x) ((long long int)x)
#define LLU(x) ((unsigned long long int)x)
#endif
typedef struct _QGROUP
{
    int nodeid;
    int groupid;
}QGROUP;
typedef struct _TSTATE
{
    time_t start_time;
    time_t last_time;
    time_t qt_last_time;
    off_t querys_total;
    off_t usecs_used;
    off_t last_querys;
    off_t last_usecs;
    off_t qt_last_querys;
    off_t qt_last_usecs;
}TSTATE;
static char *http_default_charset = "UTF-8";
static char *httpd_home = NULL;
static int is_inside_html = 1;
static unsigned char *httpd_index_html_code = NULL;
static int  nhttpd_index_html_code = 0;
static SBASE *sbase = NULL;
static SERVICE *httpd = NULL;
static SERVICE *qservice = NULL;
static SERVICE *dservice = NULL;
static MMDB *mmdb = NULL;
static QGROUP groups[M_NODES_MAX];
static int ngroups = 0;
static int nqnodes = 0;
static QGROUP qdocgroup = {0};
static QGROUP qparsergroup = {0};
static TSTATE tstate = {0};
static dictionary *dict = NULL;
static void *http_headers_map = NULL;
static void *argvmap = NULL;
static void *logger = NULL;
static int running_status = 1;
static int http_timeout = 2000000;
static int qparser_timeout = 1000000;
static int query_timeout = 1000000;
static int summary_timeout = 1000000;
static int cache_life_time = 120000000;
static int log_access = 0;
static int error_mode = 0;
#define LOG_ACCESS(format...)           \
do                                      \
{                                       \
    if(log_access > 1)                  \
    {                                   \
        REALLOG(logger,format);         \
    }                                   \
}while(0)
#define XLOG(format...)                 \
do                                      \
{                                       \
    if(log_access > 0)                  \
    {                                   \
        REALLOG(logger,format);         \
    }                                   \
}while(0)
#define E_OP_ADD_NODE       0x01
#define E_OP_DEL_NODE       0x02
#define E_OP_UPDATE_NODE    0x04
#define E_OP_LIST_NODE      0x08
#define E_OP_CLEAR_CACHE    0x10
#define E_OP_STOP           0x20
#define E_OP_RESTART        0x40
static char *e_argvs[] =
{
    "op",
#define E_ARGV_OP           0
    "ip",
#define E_ARGV_IP           1
    "port",
#define E_ARGV_PORT         2
    "type",
#define E_ARGV_TYPE         3
    "nodeid",
#define E_ARGV_NODEID       4
    "limit"
#define E_ARGV_LIMIT        5
};
#define  E_ARGV_NUM         6
#define TSTATE_INIT()                                                                   \
do                                                                                      \
{                                                                                       \
    tstate.start_time = tstate.last_time = time(NULL);                                  \
}while(0)
#define START_QUERY(conn, tval)                                                         \
do                                                                                      \
{                                                                                       \
    gettimeofday(&tval, NULL);                                                          \
    conn->xids[7] = tval.tv_sec;                                                        \
    conn->xids[8] = tval.tv_usec;                                                       \
}while(0)
#define STATE_QUERY(conn, tval, times)                                                  \
do                                                                                      \
{                                                                                       \
    if((times = (off_t)conn->xids[7]*(off_t)1000000+(off_t)(conn->xids[8]))             \
            > ((off_t)tstate.start_time * (off_t)1000000))                              \
    {                                                                                   \
        gettimeofday(&tval, NULL);                                                      \
        if((times = ((off_t)tval.tv_sec*(off_t)1000000+(off_t)(tval.tv_usec)-times))>0) \
        {                                                                               \
            tstate.usecs_used += times;                                                 \
            tstate.querys_total++;                                                      \
        }                                                                               \
    }                                                                                   \
}while(0)
/* httpd out summary */
int httpd_push_summary(CONN *conn, char *summary, int nsummary, int ret_count)
{
    char buf[Q_LINE_SIZE], *p = NULL;
    struct timeval tv = {0};
    off_t usecs = 0;

    if(conn)
    {
        p = buf;
        p += sprintf(p, "HTTP/1.0 200 OK\r\nContent-Type: text/html;charset=%s\r\n"
                "Content-Length: %d\r\nETag:%d\r\nConnection: Keep-Alive\r\n\r\n", 
                http_default_charset, nsummary, conn->xids[4]);
        STATE_QUERY(conn, tv, usecs);
        XLOG("OUT_SUMMARY{index:%d qid:%d pid:%d rid:%d fd:%d length:%d count:%d time:%lld remote[%s:%d] via %d}", conn->xids[0], conn->xids[1], conn->xids[2], conn->xids[4], conn->xids[5], nsummary, ret_count, (long long)usecs, conn->remote_ip, conn->remote_port, conn->fd);
        //fprintf(stdout, "%s\n", buf);
        conn->reset_xids(conn);
        conn->push_chunk(conn, buf, p - buf);
        conn->push_chunk(conn, summary, nsummary);
        //if(conn->xids[9] <= 0) conn->over(conn);
        return 0;
    }
    return -1;
}

/* time state  */
int httpd_qstate(CONN *conn, int keepalive)
{
    char buf[Q_BUF_SIZE], line[Q_LINE_SIZE];
    int n = 0, x = 0;

    if(conn)
    {
        if((x = mmdb_qstate(mmdb, line)) > 0)
        {
            n = sprintf(buf, "HTTP/1.0 200 OK\r\nContent-Type: text/html;charset=%s\r\n"
                    "Content-Length: %d\r\n\r\n%s", http_default_charset, x, line);
        }
        else
        {
            n = sprintf(buf, "HTTP/1.0 200 OK\r\nContent-Type: text/html;charset=%s\r\n"
                    "Content-Length: 0\r\n\r\n", http_default_charset);
        }
        conn->push_chunk(conn, buf, n);
        if(keepalive <= 0) conn->over(conn);
        return 0;
    }
    return -1;
}

/* output state info  */
int httpd_tstate(CONN *conn, int keepalive)
{
    int n = 0, x = 0, avg = 0, last_avg = 0, usec_avg = 0;
    time_t now = 0, time_used = 0, last_time = 0;
    char buf[Q_BUF_SIZE], line[Q_LINE_SIZE];
    off_t q = 0, last_usecs = 0;

    if(conn)
    {
        now = time(NULL);
        if((time_used = (now - tstate.start_time)) > 0)
            avg = (int)(tstate.querys_total/(off_t)time_used);
        if(tstate.querys_total > 0)
        {
            usec_avg = (int)(tstate.usecs_used / tstate.querys_total);
        }
        last_usecs = tstate.usecs_used - tstate.last_usecs;
        if((q = (tstate.querys_total - tstate.last_querys)) > 0 && last_usecs > 0) 
        {
            last_avg = (int)(last_usecs/q);
        }
        last_time = now - tstate.last_time;
        x = sprintf(line, "({'querys':'%lld', 'time':'%u', 'avg':'%d', 'usec_used':'%lld', 'usec_avg':'%d', 'last_querys':'%lld','last_usec':'%lld', 'last_usec_avg':'%d', 'last_time':'%u'})", LL64(tstate.querys_total), (unsigned int)time_used, avg, LL64(tstate.usecs_used), usec_avg, LL64(q), LL64(last_usecs), last_avg, (unsigned int)last_time);
        tstate.last_querys = tstate.querys_total;
        tstate.last_time = now; 
        tstate.last_usecs = tstate.usecs_used;
        n = sprintf(buf, "HTTP/1.0 200 OK\r\nContent-Type: text/html;charset=%s\r\n"
                "Content-Length: %d\r\n\r\n%s", http_default_charset, x, line);
        conn->push_chunk(conn, buf, n);
        if(keepalive <= 0) conn->over(conn);
        return 0;
    }
    return -1;
}

/* output query state info  */
int httpd_qtstate(CONN *conn, int keepalive)
{
    int n = 0, x = 0, avg = 0, last_avg = 0, usec_avg = 0;
    time_t now = 0, time_used = 0, last_time = 0;
    char buf[Q_BUF_SIZE], line[Q_LINE_SIZE];
    off_t q = 0, last_usecs = 0;

    if(conn)
    {
        now = time(NULL);
        if((time_used = (now - tstate.start_time)) > 0)
            avg = (int)(tstate.querys_total/(off_t)time_used);
        if(tstate.querys_total > 0)
        {
            usec_avg = (int)(tstate.usecs_used / tstate.querys_total);
        }
        last_usecs = tstate.usecs_used - tstate.qt_last_usecs;
        if((q = (tstate.querys_total - tstate.qt_last_querys)) > 0 && last_usecs > 0) 
        {
            last_avg = (int)(last_usecs/q);
        }
        last_time = now - tstate.qt_last_time;
        x = sprintf(line, "({'querys':'%lld', 'time':'%u', 'avg':'%d', 'usec_used':'%lld', 'usec_avg':'%d', 'last_querys':'%lld','last_usec':'%lld', 'last_usec_avg':'%d', 'last_time':'%u'})", LL64(tstate.querys_total), (unsigned int)time_used, avg, LL64(tstate.usecs_used), usec_avg, LL64(q), LL64(last_usecs), last_avg, (unsigned int)last_time);
        tstate.qt_last_querys = tstate.querys_total;
        tstate.qt_last_time = now; 
        tstate.qt_last_usecs = tstate.usecs_used;
        n = sprintf(buf, "HTTP/1.0 200 OK\r\nContent-Type: text/html;charset=%s\r\n"
                "Content-Length: %d\r\n\r\n%s", http_default_charset, x, line);
        conn->push_chunk(conn, buf, n);
        if(keepalive <= 0) conn->over(conn);
        return 0;
    }
    return -1;
}

/* request out error */
int request_server_error(CONN *conn)
{
    char buf[Q_LINE_SIZE], *p = NULL;
    struct timeval tv = {0};
    off_t usecs = 0;

    if(conn)
    {
        if(error_mode) 
        {
            conn->reset_xids(conn);
            conn->close(conn);
        }
        else
        {
            p = buf;
            p += sprintf(p, "HTTP/1.1 203 Query Timeout\r\n"
                    "Content-Type: text/html;charset=%s\r\n"
                    "Content-Length: 0\r\nCache-Control: no-cache\r\n"
                    "ETag:%d\r\nConnection: Keep-Alive\r\n\r\n", 
                    http_default_charset, conn->xids[4]);
            STATE_QUERY(conn, tv, usecs);
            ERROR_LOGGER(logger, "SERVER_ERROR{index:%d qid:%d pid:%d rid:%d via fd:%d time:%lld remote[%s:%d] via %d}", conn->xids[0], conn->xids[1], conn->xids[2], conn->xids[4], conn->xids[5], (long long)usecs, conn->remote_ip, conn->remote_port, conn->fd);
            conn->reset_xids(conn);
            conn->push_chunk(conn, buf, p - buf);
        }
        //if(conn->xids[9] <= 0) conn->over(conn);
        return 0;
    }
    return -1;
}

/* httpd out server error */
int httpd_server_error(int index, int uid, int pid)
{
    char buf[Q_LINE_SIZE], *p = NULL;
    struct timeval tv = {0};
    off_t usecs = 0;
    CONN *conn = NULL;

    if((conn = httpd->findconn(httpd, index)) && conn->fd == uid
            && conn->xids[2] > 0 && conn->xids[2] == pid)
    {
        if(error_mode) 
        {
            conn->reset_xids(conn);
            conn->close(conn);
        }
        else
        {
            p = buf;
            p += sprintf(p, "HTTP/1.1 203 Query Timeout\r\n"
                    "Content-Type: text/html;charset=%s\r\n"
                    "Content-Length: 0\r\nCache-Control: no-cache\r\n"
                    "ETag:%d\r\nConnection: Keep-Alive\r\n\r\n", 
                    http_default_charset, conn->xids[4]);
            STATE_QUERY(conn, tv, usecs);
            ERROR_LOGGER(logger, "SERVER_ERROR{index:%d qid:%d pid:%d rid:%d via fd:%d time:%lld remote[%s:%d] via %d}", conn->xids[0], conn->xids[1], conn->xids[2], conn->xids[4], conn->xids[5], (long long)usecs, conn->remote_ip, conn->remote_port, conn->fd);
            conn->reset_xids(conn);
            conn->push_chunk(conn, buf, p - buf);
            //if(conn->xids[9] <= 0) conn->over(conn);
        }
        return 0;
    }
    return -1;
}

/* request out no result */
int request_no_result(CONN *conn)
{
    char buf[Q_LINE_SIZE], *p = NULL;
    struct timeval tv = {0};
    off_t usecs = 0;

    if(conn)
    {
        p = buf;
        p += sprintf(p, "HTTP/1.0 204 OK\r\nContent-Type: text/html;charset=%s\r\n"
                "Content-Length: 0\r\nETag:%d\r\nConnection: Keep-Alive\r\n\r\n", 
                http_default_charset, conn->xids[4]);
        STATE_QUERY(conn, tv, usecs);
        XLOG("NO_RESULT{index:%d qid:%d pid:%d rid:%d via fd:%d time:%lld remote[%s:%d] via %d}", conn->xids[0], conn->xids[1], conn->xids[2], conn->xids[4], conn->xids[5], (long long)usecs, conn->remote_ip, conn->remote_port, conn->fd);
        conn->reset_xids(conn);
        conn->push_chunk(conn, buf, p - buf);
        //if(conn->xids[9] <= 0) conn->over(conn);
        return 0;
    }
    return -1;
}

/* httpd out no result */
int httpd_no_result(int index, int uid, int pid)
{
    char buf[Q_LINE_SIZE], *p = NULL;
    CONN *conn = NULL;
    struct timeval tv = {0};
    off_t usecs = 0;

    if((conn = httpd->findconn(httpd, index)) && conn->fd == uid
            && conn->xids[2] > 0 && conn->xids[2] == pid)
    {
        p = buf;
        p += sprintf(p, "HTTP/1.0 204 OK\r\nContent-Type: text/html;charset=%s\r\n"
                "Content-Length: 0\r\nETag:%d\r\nConnection: Keep-Alive\r\n\r\n", 
                http_default_charset, conn->xids[4]);
        STATE_QUERY(conn, tv, usecs);
        XLOG("NO_RESULT{index:%d qid:%d pid:%d rid:%d via fd:%d time:%lld remote[%s:%d] via %d}", conn->xids[0], conn->xids[1], conn->xids[2], conn->xids[4], conn->xids[5], (long long)usecs, conn->remote_ip, conn->remote_port, conn->fd);
        conn->reset_xids(conn);
        conn->push_chunk(conn, buf, p - buf);
        //if(conn->xids[9] <= 0) conn->over(conn);
        return 0;
    }
    return -1;
}

/* httpd clear cache over */
int request_clear_cache_over(CONN *conn)
{
    char buf[Q_LINE_SIZE], *p = NULL;

    if(conn)
    {
        p = buf;
        p += sprintf(p, "HTTP/1.0 200 OK\r\nContent-Type: text/html;charset=%s\r\n"
                "Content-Length: 0\r\n\r\n", http_default_charset);
        XLOG("CLEAR_CACHE_OVER{index:%d remote[%s:%d] via %d}", conn->index, conn->remote_ip, conn->remote_port, conn->fd);
        conn->reset_xids(conn);
        conn->push_chunk(conn, buf, p - buf);
        return 0;
    }
    return -1;
}
/* httpd clear cache failed */
int request_clear_cache_fail(CONN *conn)
{
    char buf[Q_LINE_SIZE], *p = NULL;

    if(conn)
    {
        p = buf;
        p += sprintf(p, "HTTP/1.0 504 FAIL\r\nContent-Type: text/html;charset=%s\r\n"
                "Content-Length: 0\r\n\r\n", http_default_charset);
        XLOG("CLEAR_CACHE_FAIL{index:%d remote[%s:%d] via %d}", conn->index, conn->remote_ip, conn->remote_port, conn->fd);
        conn->reset_xids(conn);
        conn->push_chunk(conn, buf, p - buf);
        return 0;
    }
    return -1;
}

/* merge query result */
int qres_merge(CONN *conn, IRES *res, IRECORD *records)
{
    int ret = -1,  len = 0, i = 0, x = 0, pid = 0, n = 0, error = 0;
    char buf[Q_LINE_SIZE], line[Q_BUF_SIZE], *p = NULL;
    CONN *xconn = NULL, *dconn = NULL;
    IRECORD *recs = NULL;
    IQSET *qset = NULL;
    IHEAD *head = NULL;
    CQRES cqres;

    if(conn)
    {
        if((pid = conn->xids[2]) > 0)
        {
            head = (IHEAD *)buf;
            qset = (IQSET *)(buf + sizeof(IHEAD));
            recs = (IRECORD *)(buf + sizeof(IHEAD) + sizeof(IQSET)); 
            memset(buf, 0, Q_LINE_SIZE);
        }
        LOG_ACCESS("READY_MERGE_RES[%d]{qid:%d pid:%d rid:%d n:%d ngroups:%d}", conn->xids[3], conn->xids[1], conn->xids[2], conn->xids[4], n, res->ngroups);
        if(mmdb_merge(mmdb, conn->xids[1], conn->xids[3], res, records, 
                    pid, &cqres, qset, recs, &n, &error) == 0)
        {
            LOG_ACCESS("OVER_QUERY[%d]{qid:%d pid:%d rid:%d n:%d}", conn->xids[3], conn->xids[1], conn->xids[2], conn->xids[4], n);
            if(error == 0) mmdb_set_cqres(mmdb, &cqres);
            LOG_ACCESS("OVER_SET_CACHE[%d]{qid:%d pid:%d rid:%d recid:%d count:%d ngroups:%d}", conn->xids[3], conn->xids[1], conn->xids[2], conn->xids[4], cqres.recid, cqres.qset.res.count, cqres.qset.res.ngroups);
            if(pid > 0 && (xconn = httpd->findconn(httpd, conn->xids[0])) 
                    && xconn->fd == conn->xids[5] && xconn->xids[2] > 0 && xconn->xids[2] == pid)
            {
                LOG_ACCESS("READY_REQ_SUMMARY[qid:%d pid:%d cid:%d rid:%d nqterms:%d count:%d remote[%s:%d] local[%s:%d] via %d]", conn->xids[1], pid, conn->xids[0], conn->xids[4], cqres.qset.nqterms, qset->count, xconn->remote_ip, xconn->remote_port, xconn->local_ip, xconn->local_port, xconn->fd);
                if(n > 0)
                {
                    head->id = pid;
                    head->cid = conn->xids[0];
                    head->cmd = IB_REQ_SUMMARY;
                    head->length = sizeof(IQSET) + n * sizeof(IRECORD);
                    len = sizeof(IHEAD) + sizeof(IQSET) + n * sizeof(IRECORD);
                    //fprintf(stdout, "%s::%d n:%d OK\n", __FILE__, __LINE__, n);
                    if(pid > 0 && conn->xids[6] <= 0 && (dconn = dservice->getconn(dservice, qdocgroup.groupid)))
                    {
                        LOG_ACCESS("REQ_SUMMARY[qid:%d pid:%d cid:%d rid:%d nqterms:%d count:%d remote[%s:%d] local[%s:%d] via %d]", conn->xids[1], pid, conn->xids[0], conn->xids[4], qset->nqterms, qset->count, dconn->remote_ip, dconn->remote_port, dconn->local_ip, dconn->local_port, dconn->fd);
                        dconn->xids[0] = conn->xids[0];
                        dconn->xids[1] = 0;
                        dconn->xids[2] = pid;
                        dconn->xids[4] = conn->xids[4];
                        dconn->xids[5] = conn->xids[5];
                        dconn->xids[6] = conn->xids[6];
                        dconn->xids[9] = conn->xids[9];
                        dconn->xids[10] = conn->xids[10];
                        dconn->xids[11] = n;
                        dconn->wait_evstate(dconn);
                        dconn->set_timeout(dconn, summary_timeout);
                        return dconn->push_chunk(dconn, buf, len);
                    }
                    res = &(qset->res);
                    p = line;
                    p += sprintf(p, "({\"io\":\"%d\", \"sort\":\"%d\", \"doctotal\":\"%d\","
                            "\"total\":\"%d\", \"count\":\"%d\",", res->io_time, res->sort_time,
                            res->doctotal, res->total, res->count);
                    if(res->ncatgroups > 0)
                    {
                        p += sprintf(p, "\"catgroups\":{");
                        for(i = 0; i < IB_CATEGORY_MAX; i++)
                        {
                            if((x = res->catgroups[i]) > 0)
                                p += sprintf(p, "\"%d\":\"%d\",", i, x);
                        }
                        --p;
                        p += sprintf(p, "},");
                    }
                    if(res->ngroups > 0)
                    {
                        p += sprintf(p, "\"groups\":{");
                        for(i = 0; i < res->ngroups; i++)
                        {
                            if(res->flag & IB_GROUPBY_DOUBLE)
                            {
                                p += sprintf(p, "\"%f\":\"%lld\",", 
                                    IB_LONG2FLOAT(res->groups[i].val), LL64(res->groups[i].val));
                            }
                            else
                            {
                                p += sprintf(p, "\"%lld\":\"%lld\",", 
                                    LL64(res->groups[i].key), LL64(res->groups[i].val));
                            }
                        }
                        --p;
                        p += sprintf(p, "},");
                    }
                    p += sprintf(p, "\"records\":{");
                    for(i = 0; i < n; i++)
                    {
                        p += sprintf(p, "\"%d\":{\"id\":\"%lld\",\"score\":\"%lld\"},",
                                i, LL64(recs[i].globalid), LL64(recs[i].score));
                    }
                    --p;
                    *p++ = '}';
                    *p++ = '}';
                    *p++ = ')';
                    *p = '\0';
                    return httpd_push_summary(xconn, line, p - line, n);
                }
                else 
                {
                    LOG_ACCESS("no_result rid:%d n:%d error:%d doctotal:%d", conn->xids[4], n, error, res->doctotal);
                    if(error > 0) request_server_error(xconn);
                    else request_no_result(xconn);
                }
            }
            ret = 0;
        }
    }
    return ret;
}

/* httpd page cache  */
int request_page_cache(CONN *conn, int pid)
{
    char buf[Q_LINE_SIZE], line[Q_BUF_SIZE], *p = NULL;
    int n = 0, x = 0, i = 0;
    IRECORD *records = NULL;
    IQSET *qset = NULL;
    IHEAD *head = NULL;
    IRES *res = NULL;

    if(conn && pid > 0)
    {
        head = (IHEAD *)buf;
        qset = (IQSET *)(buf + sizeof(IHEAD));
        records = (IRECORD *)(buf + sizeof(IHEAD) + sizeof(IQSET));
        if((n = mmdb_get_cqres(mmdb, pid, qset, records)) > 0)
        {
            res = &(qset->res);
            p = line;
            p += sprintf(p, "({\"io\":\"%d\", \"sort\":\"%d\", \"doctotal\":\"%d\","
                    "\"total\":\"%d\", \"count\":\"%d\",", res->io_time, res->sort_time,
                    res->doctotal, res->total, res->count);
            if(res->ncatgroups > 0)
            {
                p += sprintf(p, "\"catgroups\":{");
                for(i = 0; i < IB_CATEGORY_MAX; i++)
                {
                    if((x = res->catgroups[i]) > 0)
                        p += sprintf(p, "\"%d\":\"%d\",", i, x);
                }
                --p;
                p += sprintf(p, "},");
            }
            p += sprintf(p, "\"records\":{");
            for(i = 0; i < qset->count; i++)
            {
                p += sprintf(p, "\"%d\":{\"id\":\"%lld\",\"score\":\"%lld\"},",
                        i, LL64(records[i].globalid), LL64(records[i].score));
            }
            --p;
            *p++ = '}';
            *p++ = '}';
            *p++ = ')';
            *p = '\0';
            return  httpd_push_summary(conn, line, p - line, qset->count);
        }
        else
        {
            return request_no_result(conn);    
        }
    }
    return n;
}

/* request summary */
int request_summary(CONN *conn, int pid)
{
    char buf[Q_LINE_SIZE], line[Q_BUF_SIZE], *p = NULL;
    int n = 0, len = 0, x = 0, i = 0;
    IRECORD *records = NULL;
    CONN *dconn = NULL;
    IQSET *qset = NULL;
    IHEAD *head = NULL;
    IRES *res = NULL;

    if(conn && pid > 0)
    {
        head = (IHEAD *)buf;
        qset = (IQSET *)(buf + sizeof(IHEAD));
        records = (IRECORD *)(buf + sizeof(IHEAD) + sizeof(IQSET));
        LOG_ACCESS("READY_SUMMARY{index:%d qid:%d pid:%d rid:%d fd:%d remote[%s:%d] via %d}", conn->xids[0], conn->xids[1], conn->xids[2], conn->xids[4], conn->xids[5], conn->remote_ip, conn->remote_port, conn->fd);
        if((n = mmdb_get_cqres(mmdb, pid, qset, records)) > 0)
        {
            head->id = pid;
            head->cid = conn->xids[0];
            head->cmd = IB_REQ_SUMMARY;
            head->length = sizeof(IQSET) + n * sizeof(IRECORD);
            len = sizeof(IHEAD) + sizeof(IQSET) + n * sizeof(IRECORD);
            //fprintf(stdout, "%s::%d n:%d OK\n", __FILE__, __LINE__, n);
            if((dconn = dservice->getconn(dservice, qdocgroup.groupid)))
            {
                //fprintf(stdout, "%s::%d OK\n", __FILE__, __LINE__);
                LOG_ACCESS("REQ_SUMMARY[qid:%d pid:%d cid:%d rid:%d nqterms:%d count:%d remote[%s:%d] local[%s:%d] via %d]", conn->xids[1], pid, conn->xids[0], conn->xids[4], qset->nqterms, qset->count, dconn->remote_ip, dconn->remote_port, dconn->local_ip, dconn->local_port, dconn->fd);
                dconn->xids[0] = conn->index;
                dconn->xids[1] = 0;
                dconn->xids[2] = pid;
                dconn->xids[4] = conn->xids[4];
                dconn->xids[5] = conn->xids[5];
                dconn->xids[6] = conn->xids[6];
                dconn->xids[9] = conn->xids[9];
                dconn->xids[10] = conn->xids[10];
                dconn->wait_evstate(dconn);
                dconn->set_timeout(dconn, summary_timeout);
                return dconn->push_chunk(dconn, buf, len);
            }
            else 
            {
                res = &(qset->res);
                p = line;
                p += sprintf(p, "({\"io\":\"%d\", \"sort\":\"%d\", \"doctotal\":\"%d\","
                        "\"total\":\"%d\", \"count\":\"%d\",", res->io_time, res->sort_time,
                        res->doctotal, res->total, res->count);
                if(res->ncatgroups > 0)
                {
                    p += sprintf(p, "\"catgroups\":{");
                    for(i = 0; i < IB_CATEGORY_MAX; i++)
                    {
                        if((x = res->catgroups[i]) > 0)
                            p += sprintf(p, "\"%d\":\"%d\",", i, x);
                    }
                    --p;
                    p += sprintf(p, "},");
                }
                p += sprintf(p, "\"records\":{");
                for(i = 0; i < qset->count; i++)
                {
                    p += sprintf(p, "\"%d\":{\"id\":\"%lld\",\"score\":\"%lld\"},",
                            i, LL64(records[i].globalid), LL64(records[i].score));
                }
                --p;
                *p++ = '}';
                *p++ = '}';
                *p++ = ')';
                *p = '\0';
                return  httpd_push_summary(conn, line, p - line, qset->count);
            }

        }
        else
        {
            return request_no_result(conn);    
        }
    }
    return n;
}

/* httpd summary */
int httpd_summary(int pid, int cid, int uid)
{
    char buf[Q_LINE_SIZE], line[Q_BUF_SIZE], *p = NULL;
    CONN *dconn = NULL, *conn = NULL;
    int n = -1, x = 0, i = 0, len = 0;
    IRECORD *records = NULL;
    IQSET *qset = NULL;
    IHEAD *head = NULL;
    IRES *res = NULL;

    if(pid > 0 && (conn = httpd->findconn(httpd, cid)) && conn->fd == uid 
            && conn->xids[2] > 0 && conn->xids[2] == pid)
    {
        LOG_ACCESS("READY_SUMMARY{index:%d qid:%d pid:%d rid:%d fd:%d remote[%s:%d] via %d}", conn->xids[0], conn->xids[1], conn->xids[2], conn->xids[4], conn->xids[5], conn->remote_ip, conn->remote_port, conn->fd);
        head = (IHEAD *)buf;
        qset = (IQSET *)(buf + sizeof(IHEAD));
        records = (IRECORD *)(buf + sizeof(IHEAD) + sizeof(IQSET));
        if((n = mmdb_get_cqres(mmdb, pid, qset, records)) > 0)
        {
            head->id = pid;
            head->cid = cid;
            head->cmd = IB_REQ_SUMMARY;
            head->length = sizeof(IQSET) + n * sizeof(IRECORD);
            len = sizeof(IHEAD) + sizeof(IQSET) + n * sizeof(IRECORD);
            //fprintf(stdout, "%s::%d n:%d OK\n", __FILE__, __LINE__, n);
            if(conn->xids[6] <= 0 && (dconn = dservice->getconn(dservice, qdocgroup.groupid)))
            {
                LOG_ACCESS("REQ_SUMMARY[qid:%d pid:%d cid:%d rid:%d nqterms:%d count:%d remote[%s:%d] local[%s:%d] via %d]", conn->xids[1], pid, cid, conn->xids[4], qset->nqterms, qset->count, dconn->remote_ip, dconn->remote_port, dconn->local_ip, dconn->local_port, dconn->fd);
                dconn->xids[0] = conn->index;
                dconn->xids[1] = 0;
                dconn->xids[2] = pid;
                dconn->xids[4] = conn->xids[4];
                dconn->xids[5] = conn->xids[5];
                dconn->xids[6] = conn->xids[6];
                dconn->xids[9] = conn->xids[9];
                dconn->xids[10] = conn->xids[10];
                dconn->wait_evstate(dconn);
                dconn->set_timeout(dconn, summary_timeout);
                return dconn->push_chunk(dconn, buf, len);
            }
            res = &(qset->res);
            p = line;
            p += sprintf(p, "({\"io\":\"%d\", \"sort\":\"%d\", \"doctotal\":\"%d\","
                    "\"total\":\"%d\", \"count\":\"%d\",", res->io_time, res->sort_time,
                    res->doctotal, res->total, res->count);
            if(res->ncatgroups > 0)
            {
                p += sprintf(p, "\"catgroups\":{");
                for(i = 0; i < IB_CATEGORY_MAX; i++)
                {
                    if((x = res->catgroups[i]) > 0)
                        p += sprintf(p, "\"%d\":\"%d\",", i, x);
                }
                --p;
                p += sprintf(p, "},");
            }
            if(res->ngroups > 0)
            {
                p += sprintf(p, "\"groups\":{");
                for(i = 0; i < res->ngroups; i++)
                {
                    if(res->flag & IB_GROUPBY_DOUBLE)
                    {
                        p += sprintf(p, "\"%f\":\"%lld\",", 
                                IB_LONG2FLOAT(res->groups[i].val), LL64(res->groups[i].val));
                    }
                    else
                    {
                        p += sprintf(p, "\"%lld\":\"%lld\",", 
                                LL64(res->groups[i].key), LL64(res->groups[i].val));
                    }
                }
                --p;
                p += sprintf(p, "},");
            }
            p += sprintf(p, "\"records\":{");
            for(i = 0; i < qset->count; i++)
            {
                p += sprintf(p, "\"%d\":{\"id\":\"%lld\",\"score\":\"%lld\"},",
                        i, LL64(records[i].globalid), LL64(records[i].score));
            }
            --p;
            *p++ = '}';
            *p++ = '}';
            *p++ = ')';
            *p = '\0';
            return  httpd_push_summary(conn, line, p - line, qset->count);
        }
        else
        {
            return request_no_result(conn);
        }
    }
    return n;
}

/* packet reader for qservice */
int qservice_packet_reader(CONN *conn, CB_DATA *buffer)
{
    return -1;
}

/* qservice quick handler */
int qservice_quick_handler(CONN *conn, CB_DATA *packet)
{
    IHEAD *resp = NULL;
    int n = -1;

    if(conn)
    {
        if(packet && (resp = (IHEAD *)packet->data))
        {
            n = resp->length;
        }
    }
    return n; 
}

/* qservice packet handler */
int qservice_packet_handler(CONN *conn, CB_DATA *packet)
{
    IHEAD *resp = NULL;
    IRES res = {0};
    int ret = -1;

    if(conn)
    {
        if(packet && (resp = (IHEAD *)packet->data) && resp->length > 0)
        {
            conn->save_cache(conn, resp, sizeof(IHEAD));
            return conn->recv_chunk(conn, resp->length);
        }
        else
        {
            conn->over_timeout(conn);
            WARN_LOGGER(logger, "QUERY_NO_RESULT{qid:%d pid:%d rid:%d on remote[%s:%d] local[%s:%d] via %d}", conn->xids[1], conn->xids[2], conn->xids[4], conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
            res.qid = conn->xids[1];
            qres_merge(conn, &res, NULL);
            ret = qservice->freeconn(qservice, conn);
        }
    }
    return ret;
}

/* index  handler */
int qservice_index_handler(CONN *conn)
{
    return -1;
}

/* qservice data handler */
int qservice_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    IRECORD *records = NULL;
    IHEAD *head = NULL;
    IRES *res = NULL;
    int ret = -1;

    if(conn)
    {
        conn->over_timeout(conn);
        conn->over_evstate(conn);
        if(cache && (head = (IHEAD *)(packet->data)) && chunk && (res = (IRES *)(chunk->data)))
        {
            if(head->cmd == IB_RESP_QUERY)
            {
                if(chunk->ndata == (sizeof(IRES) + res->count * sizeof(IRECORD)))
                    records = (IRECORD *)(chunk->data+sizeof(IRES));
                LOG_ACCESS("RESP_QUERY(%d)[%d]{qid:%d pid:%d cid:%d rid:%d res[qid:%d count:%d total:%d] doctotal:%d records:%p remote[%s:%d] local[%s:%d] via %d}", head->status, head->nodeid, head->id, conn->xids[2], head->cid, conn->xids[4], res->qid, res->count, res->total, res->doctotal, records, conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
                //get summary 
                qres_merge(conn, res, records);
            }
        }
        ret =  qservice->freeconn(qservice, conn);
    }
    return ret;
}

/* qservice error handler */
int qservice_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    IRES res = {0};

    if(conn)
    {
        if(conn && conn->evstate == EVSTATE_WAIT)
        {
            FATAL_LOGGER(logger, "ERROR cid:%d qid:%d pid:%d nodeid:%d on locate[%s:%d] remote[%s:%d] via %d", conn->xids[0], conn->xids[1], conn->xids[2], conn->xids[3], conn->local_ip, conn->local_port, conn->remote_ip, conn->remote_port, conn->fd);
            res.qid = conn->xids[1];
            qres_merge(conn, &res, NULL);
            conn->over_evstate(conn);
            conn->over_cstate(conn);
            return conn->close(conn);
        }
    }
    return -1;
}

/* qservice timeout handler */
int qservice_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    IRES res = {0};

    if(conn && conn->evstate == EVSTATE_WAIT)
    {
        conn->over_timeout(conn);
        FATAL_LOGGER(logger, "TIMEOUT cid:%d qid:%d pid:%d nodeid:%d rid:%d on locate[%s:%d] remote[%s:%d] via %d", conn->xids[0], conn->xids[1], conn->xids[2], conn->xids[3], conn->xids[4], conn->local_ip, conn->local_port, conn->remote_ip, conn->remote_port, conn->fd);
        res.qid = conn->xids[1];
        qres_merge(conn, &res, NULL);
        conn->over_evstate(conn);
        conn->over_cstate(conn);
        return conn->close(conn);
    }
    return -1;
}

/* qservice transaction handler */
int qservice_trans_handler(CONN *conn, int tid)
{
    if(conn && tid >= 0)
    {
        return 0;
    }
    return -1;
}

/* packet reader for qservice */
int dservice_packet_reader(CONN *conn, CB_DATA *buffer)
{
    return -1;
}

/* dservice quick handler */
int dservice_quick_handler(CONN *conn, CB_DATA *packet)
{
    IHEAD *resp = NULL;
    int n = -1;

    if(conn && packet && (resp = (IHEAD *)(packet->data)))
    {
        n = resp->length;
    }
    return n;
}

/* dservice packet handler */
int dservice_packet_handler(CONN *conn, CB_DATA *packet)
{
    IHEAD *resp = NULL;

    if(conn && packet && (resp = (IHEAD *)(packet->data)))
    {
        if(resp->length > 0)
        {
            return conn->recv_chunk(conn, resp->length);
        }
        else  
        {
            conn->over_timeout(conn);
            LOG_ACCESS("QPARSER_NO_RESULT{qid:%d pid:%d}", conn->xids[1], conn->xids[2]);
            if(resp->cmd == IB_RESP_QPARSE) mmdb_over_qstatus(mmdb, conn->xids[1]);
            if(conn->xids[2] > 0) httpd_no_result(conn->xids[0], conn->xids[5], conn->xids[2]);
            return dservice->freeconn(dservice, conn);
        }
    }
    return -1;
}

/* dservice data handler */
int dservice_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    int ret = 0, i = 0, n = 0, new_query = 0, have_res_cache = 0, *res_cache = NULL;
    CONN *dconn = NULL, *xconn = NULL;
    IHEAD *head = NULL, *req = NULL;
    IQUERY *pquery = NULL;
    char buf[Q_LINE_SIZE];
    IRES res = {0};

    if(conn)
    {
        conn->over_timeout(conn);
        conn->over_evstate(conn);
        if(cache && (head = (IHEAD *)(packet->data)) && chunk && chunk->ndata > 0)
        {
            if(head->cmd == IB_RESP_QPARSE)
            {
                if((pquery = (IQUERY *)(chunk->data)) && chunk->ndata == sizeof(IQUERY))
                {
                    pquery->qid = head->id;
                    /* check search page id */
                    if(conn->xids[2] > 0) res_cache = &have_res_cache;
                    ret = mmdb_set_query(mmdb, head->id, pquery, nqnodes, 
                            &new_query, conn->xids[10], res_cache);
                    LOG_ACCESS("RESP_QPARSE(%d){qid:%d pid:%d cid:%d rid:%d nqterms:%d ntop:%d res_cache:%d new_query:%d remote[%s:%d] local[%s:%d] via %d}", head->status, pquery->qid, conn->xids[2], head->cid, conn->xids[4], pquery->nqterms, pquery->ntop, have_res_cache, new_query, conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
                    if(res_cache && have_res_cache > 0)
                    {
                        httpd_summary(conn->xids[2], conn->xids[0], conn->xids[5]);
                        conn->xids[2] = 0;
                    }
                    if(ret < 0)
                    {
                        mmdb_reset_qstatus(mmdb, conn->xids[1]);
                        LOG_ACCESS("SET_QUERY_FAIL(%d){qid:%d pid:%d cid:%d rid:%d remote[%s:%d] local[%s:%d] via %d}", head->status, head->id, conn->xids[2], head->cid, conn->xids[4], conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
                        if(conn->xids[2] > 0)
                            ret = httpd_server_error(conn->xids[0], conn->xids[5], conn->xids[2]);
                    }
                    else if(new_query)
                    {
                        LOG_ACCESS("READY_QUERY[%d]{qid:%d pid:%d cid:%d rid:%d nqterms:%d ntop:%d}", i, head->id, conn->xids[2], head->cid, conn->xids[4],pquery->nqterms, pquery->ntop);
                        req = (IHEAD *)buf;
                        req->cmd    = IB_REQ_QUERY;
                        req->id     = head->id;
                        req->cid    = head->cid;
                        req->length = chunk->ndata;
                        memcpy(buf + sizeof(IHEAD), chunk->data, chunk->ndata);
                        n = sizeof(IHEAD) + chunk->ndata;
                        for(i = 0; i < ngroups; i++)
                        {
                            if(groups[i].groupid >= 0)
                            {
                                if((dconn = qservice->getconn(qservice, groups[i].groupid)))
                                {
                                    LOG_ACCESS("REQ_QUERY[%d]{qid:%d pid:%d cid:%d rid:%d nqterms:%d ntop:%d remote[%s:%d] local[%s:%d] via %d}", i, head->id, conn->xids[2], head->cid, conn->xids[4],pquery->nqterms, pquery->ntop, dconn->remote_ip, dconn->remote_port, dconn->local_ip, dconn->local_port, dconn->fd);
                                    dconn->xids[0] = conn->xids[0];
                                    dconn->xids[1] = conn->xids[1];
                                    dconn->xids[2] = conn->xids[2];
                                    dconn->xids[3] = i;
                                    dconn->xids[4] = conn->xids[4];
                                    dconn->xids[5] = conn->xids[5];
                                    dconn->xids[6] = conn->xids[6];
                                    dconn->xids[9] = conn->xids[9];
                                    dconn->xids[10] = conn->xids[10];
                                    req->nodeid = i;
                                    dconn->wait_evstate(dconn);
                                    dconn->set_timeout(dconn, query_timeout);
                                    dconn->push_chunk(dconn, buf, n);
                                }
                                else 
                                {
                                    res.qid = head->id;
                                    FATAL_LOGGER(logger, "NO_QCONN{qid:%d pid:%d rid:%d to groups[%d/%d]}", conn->xids[1], conn->xids[2], conn->xids[4], i, qservice->groups[groups[i].groupid].nconns_free);
                                    qres_merge(conn, &res, NULL);
                                }
                            }
                            else
                            {
                                ERROR_LOGGER(logger, "get new conn to qservice[%d] failed", groups[i].nodeid);
                            }
                        }
                    }
                }
                else 
                {
                    LOG_ACCESS("BAD_QPARSE(%d){qid:%d pid:%d cid:%d rid:%d remote[%s:%d] local[%s:%d] via %d}", head->status, head->id, conn->xids[2], head->cid, conn->xids[4], conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
                    mmdb_reset_qstatus(mmdb, conn->xids[1]);
                    if(conn->xids[2]>0)httpd_no_result(conn->xids[0], conn->xids[5], conn->xids[2]);
                }
            }
            else if(head->cmd == IB_RESP_SUMMARY)
            {
                LOG_ACCESS("RESP_SUMMARY(%d){qid:%d pid:%d/%d cid:%d rid:%d nsummary:%d remote[%s:%d] local[%s:%d] via %d}", head->status, conn->xids[1], head->id, conn->xids[2], head->cid, conn->xids[4], chunk->ndata, conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
                if((xconn = httpd->findconn(httpd, conn->xids[0])) && xconn->fd == conn->xids[5]
                        && xconn->xids[2] > 0 && xconn->xids[2] == conn->xids[2])
                {
                    xconn->over_evstate(xconn);
                    xconn->over_cstate(xconn);
                    httpd_push_summary(xconn, chunk->data, chunk->ndata,  conn->xids[11]);
                }
                ret = mmdb_set_summary(mmdb, head->id, chunk->data, chunk->ndata);
            }
        }
        ret = dservice->freeconn(dservice, conn);
    }
    return ret;
}

/* dservice error handler */
int dservice_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    if(conn && conn->evstate == EVSTATE_WAIT)
    {
        FATAL_LOGGER(logger, "ERROR cid:%d qid:%d pid:%d nodeid:%d rid:%d on locate[%s:%d] remote[%s:%d] via %d", conn->xids[0], conn->xids[1], conn->xids[2], conn->xids[3], conn->xids[4], conn->local_ip, conn->local_port, conn->remote_ip, conn->remote_port, conn->fd);
        if(conn->xids[1] > 0) mmdb_reset_qstatus(mmdb, conn->xids[1]);
        conn->over_evstate(conn);
        conn->over_cstate(conn);
        if(conn->xids[2] > 0) httpd_server_error(conn->xids[0], conn->xids[5], conn->xids[2]);
        return conn->close(conn);
    }
    return -1;
}

/* dservice timeout handler */
int dservice_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    if(conn && conn->evstate == EVSTATE_WAIT)
    {
        conn->over_timeout(conn);
        FATAL_LOGGER(logger, "TIMEOUT cid:%d qid:%d pid:%d nodeid:%d rid:%d on locate[%s:%d] remote[%s:%d] via %d", conn->xids[0], conn->xids[1], conn->xids[2], conn->xids[3], conn->xids[4], conn->local_ip, conn->local_port, conn->remote_ip, conn->remote_port, conn->fd);
        mmdb_reset_qstatus(mmdb, conn->xids[1]);
        conn->over_evstate(conn);
        conn->over_cstate(conn);
        if(conn->xids[2] > 0)httpd_server_error(conn->xids[0], conn->xids[5], conn->xids[2]);
        return conn->close(conn);
    }
    return -1;
}

/* httpd packet reader */
int httpd_packet_reader(CONN *conn, CB_DATA *buffer)
{
    if(conn)
    {
        return 0;
    }
    return -1;
}

/* packet handler */
int httpd_packet_handler(CONN *conn, CB_DATA *packet)
{
    int ret = -1, n = 0, status = -1, new_query = 0, qid = 0, from = 0, count = 0, 
        pid = 0, nsummary = 0, lifetime = 0, res_cache = 0, keepalive = 0;
    char buf[Q_LINE_SIZE], file[HTTP_PATH_MAX], *s = NULL, *es = NULL, *p = NULL, 
         *end = NULL, *summary = NULL, *pp = NULL;
    IRECORD *records = NULL;
    struct timeval tv = {0};
    HTTP_REQ http_req = {0};
    struct stat st = {0};
    CONN *dconn = NULL;
    IHEAD *preq = NULL;
    IQSET *qset = NULL;

    if(conn && packet && (p = packet->data))
    {
        //TIMER_INIT(timer);
        //p = packet->data;
        end = packet->data + packet->ndata;
        //fprintf(stdout, "%s::%d argv:%s\n", __FILE__, __LINE__, p);
        if(http_request_parse(p, end, &http_req, http_headers_map) == -1) goto err_end;
        if(http_req.reqid == HTTP_GET)
        {
            if(running_status <= 0) goto err_end;
            if((n = http_req.headers[HEAD_GEN_CONNECTION]) > 0 
                    && strncasecmp(http_req.hlines + n, "Keep-Alive", 10) == 0)
                keepalive = 1;
            if(strstr(http_req.path, "/qstate")) return httpd_qstate(conn, keepalive);
            if(strstr(http_req.path, "/tstate")) return httpd_tstate(conn, keepalive);
            if(strstr(http_req.path, "/qtstate")) return httpd_qtstate(conn, keepalive);
            conn->xids[5] = conn->fd;
            START_QUERY(conn, tv);
            if((s = strstr(p, "&qid="))) conn->xids[4] = atoi(s+5);
            else conn->xids[4] = 0;
            if(http_req.argv_len > 0  && http_req.argv_off >= 0 
                    && (p = (packet->data + http_req.argv_off))
                    && (s = strstr(p, "&from=")) && (from = atoi(s+6)) >= 0 
                    && (es = strstr(s + 6, "&count=")) && (count = atoi(es+7)) > 0)
            {
                if((pp = strstr(s, "&lifetime="))) lifetime = atoi(pp+10);
                if(count > M_PAGE_NUM_MAX) count = M_PAGE_NUM_MAX;
                /* check query return unique qid /query_status/is_need_new_query */
                if(from < IB_TOPK_NUM && (qid = mmdb_qid(mmdb, p, s)) > 0 
                        && (pid = mmdb_pid(mmdb, qid, from, count)) > 0
                        && (status = mmdb_qstatus(mmdb, qid, lifetime, &new_query, &res_cache))>= 0)
                {
                    XLOG("REQUEST{%.*s lifetime:%d qid:%d pid:%d status:%d res_cache:%d new_query:%d rid:%d on remote[%s:%d] local[%s:%d] via %d}", http_req.argv_len, packet->data + http_req.argv_off, lifetime,qid, pid, status, res_cache, new_query, conn->xids[4], conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
                    if(strstr(p, "&debug=1")) 
                    {
                        conn->xids[6] = 1;
                    }
                    else  
                    {
                        conn->xids[6] = 0;
                        /* check current page summary cache */
                        nsummary = mmdb_check_summary_len(mmdb, pid);
                    }
                    preq = (IHEAD *)buf;
                    qset = (IQSET *)(buf + sizeof(IHEAD));
                    records = (IRECORD *)(buf + sizeof(IHEAD) + sizeof(IQSET));
                    conn->xids[0] = conn->index;
                    conn->xids[1] = qid;
                    conn->xids[2] = pid;
                    conn->xids[9] = keepalive;
                    conn->xids[10] = lifetime;
                    if(res_cache > 0) conn->xids[2] = 0;
                    /* new query */
                    if(new_query)
                    {
                        LOG_ACCESS("NEW_QUERY{qid:%d pid:%d cid:%d rid:%d on remote[%s:%d] via %d", qid, conn->xids[2], conn->index, conn->xids[4], conn->remote_ip, conn->remote_port, conn->fd); 
                        preq = (IHEAD *)buf;
                        preq->cmd = IB_REQ_QPARSE;
                        preq->id  = qid;
                        preq->cid  = conn->index;
                        preq->length = http_req.argv_len;
                        s = buf + sizeof(IHEAD);
                        strncpy(s, packet->data + http_req.argv_off, http_req.argv_len);
                        s += http_req.argv_len;
                        if((dconn = dservice->getconn(dservice, qparsergroup.groupid)))
                        {
                            LOG_ACCESS("REQ_QPARSE{qid:%d cid:%d rid:%d remote[%s:%d] local[%s:%d] via %d}", qid, conn->index, conn->xids[4], dconn->remote_ip, dconn->remote_port, dconn->local_ip, dconn->local_port, dconn->fd);
                            dconn->xids[0] = conn->index;
                            dconn->xids[1] = qid;
                            dconn->xids[2] = conn->xids[2];
                            dconn->xids[4] = conn->xids[4];
                            dconn->xids[5] = conn->xids[5];
                            dconn->xids[6] = conn->xids[6];
                            dconn->xids[9] = conn->xids[9];
                            dconn->xids[10] = conn->xids[10];
                            dconn->wait_evstate(dconn);
                            dconn->set_timeout(dconn, qparser_timeout);
                            dconn->push_chunk(dconn, buf, s - buf);
                        }
                        else 
                        {
                            FATAL_LOGGER(logger, "NO_QPCONN{nconns_free:%d qid:%d cid:%d rid:%d remote[%s:%d] via %d}",  dservice->groups[qparsergroup.groupid].nconns_free, qid, conn->index, conn->xids[4], conn->remote_ip, conn->remote_port, conn->fd);
                            mmdb_reset_qstatus(mmdb, qid);
                            if(nsummary == 0) return request_server_error(conn);
                        }
                    }
                    if(res_cache)
                    {
                        if(conn->xids[6] > 0) return request_page_cache(conn, pid);
                        else if(nsummary > 0)
                        {
                            if((summary = xmm_new(nsummary)))
                            {
                                if(mmdb_read_summary(mmdb, pid, summary) > 0)
                                    httpd_push_summary(conn, summary, nsummary, count);
                                else 
                                    request_server_error(conn);
                                xmm_free(summary, nsummary);
                            }
                            else 
                                request_server_error(conn);
                            return 0;
                        }
                        else
                        {
                            conn->xids[2] = pid;
                            return request_summary(conn, pid);
                        }
                    }
                    else
                    {
                        if(status != M_STATUS_FREE)
                        {
                            conn->s_id = pid;
                            conn->wait_evstate(conn);
                            return conn->set_timeout(conn, EVWAIT_TIMEOUT);
                        }
                    }
                }
                else 
                {
                    ERROR_LOGGER(logger, "ERROR{%.*s arg_len:%d from:%d count:%d qid:%d pid:%d status:%d new_query:%d", http_req.argv_len, packet->data + http_req.argv_off, (int)(s - p), from, count, qid, pid, status, new_query);
                    goto err_end;
                }
                return 0;
            }
            else
            {
                if(is_inside_html && httpd_index_html_code && nhttpd_index_html_code > 0)
                {
                    p = buf;
                    p += sprintf(p, "HTTP/1.0 200 OK\r\nContent-Length: %d\r\n"
                            "Content-Type: text/html;charset=%s\r\n",
                            nhttpd_index_html_code, http_default_charset);
                    if((n = http_req.headers[HEAD_GEN_CONNECTION]) > 0)
                        p += sprintf(p, "Connection: %s\r\n", (http_req.hlines + n));
                    else
                        p += sprintf(p, "Connection:Keep-Alive\r\n");
                    p += sprintf(p, "\r\n");
                    conn->push_chunk(conn, buf, (p - buf));
                    conn->push_chunk(conn, httpd_index_html_code, nhttpd_index_html_code);
                }
                else if(httpd_home)
                {
                    p = file;
                    if(http_req.path[0] != '/')
                        p += sprintf(p, "%s/%s", httpd_home, http_req.path);
                    else
                        p += sprintf(p, "%s%s", httpd_home, http_req.path);
                    if(http_req.path[1] == '\0')
                        p += sprintf(p, "%s", "index.html");
                    if((n = (p - file)) > 0 && stat(file, &st) == 0)
                    {
                        if(st.st_size == 0)
                        {
                            conn->push_chunk(conn, HTTP_NO_CONTENT, strlen(HTTP_NO_CONTENT));
                        }
                        else if((n = http_req.headers[HEAD_REQ_IF_MODIFIED_SINCE]) > 0
                                && str2time(http_req.hlines + n) == st.st_mtime)
                        {
                            conn->push_chunk(conn, HTTP_NOT_MODIFIED, strlen(HTTP_NOT_MODIFIED));
                        }
                        else
                        {
                            p = buf;
                            p += sprintf(p, "HTTP/1.0 200 OK\r\nContent-Length:%lld\r\n"
                                    "Content-Type: text/html;charset=%s\r\n",
                                    (long long int)(st.st_size), http_default_charset);
                            if((n = http_req.headers[HEAD_GEN_CONNECTION]) > 0)
                                p += sprintf(p, "Connection: %s\r\n", http_req.hlines + n);
                            p += sprintf(p, "Last-Modified:");
                            p += GMTstrdate(st.st_mtime, p);
                            p += sprintf(p, "%s", "\r\n");//date end
                            p += sprintf(p, "%s", "\r\n");
                            conn->push_chunk(conn, buf, (p - buf));
                            conn->push_file(conn, file, 0, st.st_size);
                        }
                    }
                    else
                    {
                        conn->push_chunk(conn, HTTP_NOT_FOUND, strlen(HTTP_NOT_FOUND));
                    }
                }
                else
                {
                    conn->push_chunk(conn, HTTP_BAD_REQUEST, strlen(HTTP_BAD_REQUEST));
                }
                if(keepalive == 0)return conn->over(conn);
                return 0;
            }
        }
        else if(http_req.reqid == HTTP_POST)
        {
            if((n = http_req.headers[HEAD_ENT_CONTENT_LENGTH]) > 0
                    && (n = atol(http_req.hlines + n)) > 0)
            {
                conn->save_cache(conn, &http_req, sizeof(HTTP_REQ));
                return conn->recv_chunk(conn, n);
            }
        }
err_end:
        ret = conn->push_chunk(conn, HTTP_BAD_REQUEST, strlen(HTTP_BAD_REQUEST));
    }
    return ret;
}

/* httpd request handler */
/* test 
//add node
POST / HTTP/1.1
Content-Length: 44

op=1&type=1&ip=10.0.6.82&port=4936&limit=256

//del node
POST / HTTP/1.1
Content-Length: 13

op=2&nodeid=0

//set node
POST / HTTP/1.1
Content-Length: 25

op=4&nodeid=0&limit=10000

//list node
POST / HTTP/1.1
Content-Length: 4

op=8

//clear cache
POST / HTTP/1.1
Content-Length: 5

op=16
*/
int httpd_request_handler(CONN *conn, HTTP_REQ *http_req)
{
    char buf[HTTP_LINE_MAX], *p = NULL, *end = NULL, *ip = NULL;
    int ret = -1, i = 0, id = 0, n = 0, type = -1, 
        nodeid = -1, port = -1, limit = -1, op = -1, len = 0;
    CB_DATA *chunk = NULL;

    if(conn && http_req)
    {
        for(i = 0; i < http_req->nargvs; i++)
        {
            if(http_req->argvs[i].nk > 0 && (n = http_req->argvs[i].k) > 0
                    && (p = (http_req->line + n)))
            {
                if((id = (mtrie_get(argvmap, p, http_req->argvs[i].nk) - 1)) >= 0
                        && http_req->argvs[i].nv > 0
                        && (n = http_req->argvs[i].v) > 0
                        && (p = (http_req->line + n)))
                {
                    switch(id)
                    {
                        case E_ARGV_OP :
                            op = atoi(p);
                            break;
                        case E_ARGV_IP :
                            ip = p;
                            break;
                        case E_ARGV_TYPE:
                            type = atoi(p);
                            break;
                        case E_ARGV_PORT :
                            port = atoi(p);
                            break;
                        case E_ARGV_NODEID:
                            nodeid = atoi(p);
                            break;
                        case E_ARGV_LIMIT:
                            limit = atoi(p);
                            break;
                        default:
                            break;
                    }
                }
            }
        }
        if(op == E_OP_CLEAR_CACHE)
        {
            running_status = 0;
            mmdb_clear_cache(mmdb);
            running_status = 1;
            return request_clear_cache_over(conn);
        }
        else if(op == E_OP_STOP)
        {
            running_status = 0;
            goto no_content;
        }
        else if(op == E_OP_RESTART)
        {
            running_status = 1;
            goto no_content;
        }
        else if(op == E_OP_ADD_NODE)
        {
            if(type >= 0 && ip && port > 0 && limit >= 0 
                    && mmdb->add_node(mmdb, type, ip, port, limit) > 0)
                goto nodelist;
            else goto err_end;
        }
        else if(op == E_OP_DEL_NODE)
        {
            if(nodeid >= 0 && mmdb->del_node(mmdb, nodeid) >= 0)
            {
                if(nodeid == qdocgroup.nodeid)
                {
                    //fprintf(stdout, "%s::%d del_node(%d) groupid:%d\n", __FILE__, __LINE__, nodeid, qdocgroup.groupid);
                    dservice->closegroup(dservice, qdocgroup.groupid);
                    qdocgroup.groupid = -1;
                    qdocgroup.nodeid = -1;
                }
                else if(nodeid == qparsergroup.nodeid)
                {
                    //fprintf(stdout, "%s::%d del_node(%d) groupid:%d\n", __FILE__, __LINE__, nodeid, qdocgroup.groupid);
                    dservice->closegroup(dservice, qparsergroup.groupid);
                    qparsergroup.groupid = -1;
                    qparsergroup.nodeid = -1;
                }
                else
                {
                    for(i = 0; i < ngroups; i++)
                    {
                        if(groups[i].nodeid == nodeid)
                        {
                            //fprintf(stdout, "%s::%d del_node(%d) groupid:%d\n", __FILE__, __LINE__, nodeid, groups[i].groupid);
                            qservice->closegroup(qservice, groups[i].groupid);
                            groups[i].groupid = -1;
                            groups[i].nodeid = -1;
                            --nqnodes;
                            break;
                        }
                    }
                }
                goto nodelist;
            }
            else goto err_end;
        }
        else if(op == E_OP_LIST_NODE) goto nodelist;
        else goto err_end;
nodelist:
        len = sizeof(SNODE) * M_NODES_MAX * 4;
        if((chunk = conn->newchunk(conn, len)))
        {
            p = chunk->data;
            end = p + len;
            if((len = mmdb->list_nodes(mmdb, p, end)) > 0)
            {
                p = buf;
                p += sprintf(p, "HTTP/1.0 200 OK\r\nContent-Type: text/html;charset=%s\r\n"
                        "Content-Length: %d\r\n", http_default_charset, len);
                if((n = http_req->headers[HEAD_GEN_CONNECTION]) > 0)
                    p += sprintf(p, "Connection: %s\r\n", (http_req->hlines + n));
                p += sprintf(p, "Connection:Keep-Alive\r\n");
                p += sprintf(p, "\r\n");
                conn->push_chunk(conn, buf, p - buf);
                return conn->send_chunk(conn, chunk, len);
            }
            else 
            {
                conn->freechunk(conn, chunk);
            }
        }
        return 0;
no_content:
        p = buf;
        p += sprintf(p, "HTTP/1.0 200 OK\r\nContent-Type: text/html;charset=%s\r\n"
                "Content-Length: 0\r\n\r\n", http_default_charset);
        conn->push_chunk(conn, buf, p - buf);
        return 0;
err_end:
        ret = conn->push_chunk(conn, HTTP_BAD_REQUEST, strlen(HTTP_BAD_REQUEST));
    }
    return ret;
}

/*  data handler */
int httpd_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    char *p = NULL, *end = NULL;
    HTTP_REQ *http_req = NULL;
    int ret = -1;

    if(conn && packet && cache && chunk && chunk->ndata > 0)
    {
        if((http_req = (HTTP_REQ *)cache->data))
        {
            if(http_req->reqid == HTTP_POST)
            {
                p = chunk->data;
                end = p + chunk->ndata;
                if(http_argv_parse(p, end, http_req) == -1)goto err_end;
                //fprintf(stdout, "%s::%d %s nargvs:%d\n", __FILE__, __LINE__, p, http_req->nargvs);
                return httpd_request_handler(conn, http_req);
            }
        }
err_end:
        ret = conn->push_chunk(conn, HTTP_BAD_REQUEST, strlen(HTTP_BAD_REQUEST));
    }
    return ret;
}

/* httpd error handler */
int httpd_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    if(conn)
    {
        conn->reset_xids(conn);
    }
    return -1;
}

/* httpd timeout handler */
int httpd_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    int ret = -1;

    if(conn)
    {
        conn->over_timeout(conn);
        if(conn->evstate == EVSTATE_WAIT)
        {
            ret = httpd_summary(conn->xids[2], conn->xids[0], conn->xids[5]);
            if(ret >= 0 )
            {
                LOG_ACCESS("OVER_SUMMARY{index:%d qid:%d pid:%d rid:%d fd:%d remote[%s:%d] via %d}", conn->xids[0], conn->xids[1], conn->xids[2], conn->xids[4], conn->xids[5], conn->remote_ip, conn->remote_port, conn->fd);
                conn->over_cstate(conn);
                return conn->over_evstate(conn);
            }
            else if(conn->timeout < http_timeout)
            {
                return conn->set_timeout(conn, conn->timeout+EVWAIT_TIMEOUT);
            }
            else
            {
                LOG_ACCESS("OVER_CONN{index:%d qid:%d pid:%d rid:%d fd:%d remote[%s:%d] via %d}", conn->xids[0], conn->xids[1], conn->xids[2], conn->xids[4], conn->xids[5], conn->remote_ip, conn->remote_port, conn->fd);
                conn->reset_xids(conn);
                conn->over_cstate(conn);
                return conn->close(conn);
            }
        }
        else
        {
            conn->reset_xids(conn);
            conn->over_cstate(conn);
            return conn->close(conn);
        }
    }
    return -1;
}

/* OOB data handler for httpd */
int httpd_oob_handler(CONN *conn, CB_DATA *oob)
{
    if(conn)
    {
        return 0;
    }
    return -1;
}

/* query heartbeat */
void q_heartbeat_handler(void *arg)
{
    return ;
}

/* document heartbeat */
void d_heartbeat_handler(void *arg)
{
    //SESSION session = {0}, qsession = {0};
    int nodeid = 0, groupid = 0, i = 0;
    SNODE node = {0};

    if(arg == (void *)dservice)
    {
        while((nodeid = mmdb->pop_node(mmdb, &node)) > 0)
        {
            if(node.type == M_NODE_INDEXD)
            {
                groupid = qservice->addgroup(qservice, node.ip, node.port, 
                        node.limit, &(qservice->session));
                i = ngroups++;
                nqnodes++;
                groups[i].groupid = groupid;
                groups[i].nodeid = nodeid;
            }
            else if(node.type == M_NODE_QPARSERD && qparsergroup.nodeid <= 0
                    && qparsergroup.groupid <= 0 )
            {
                groupid = dservice->addgroup(dservice, node.ip, node.port, 
                        node.limit, &(dservice->session));
                qparsergroup.groupid = groupid;
                qparsergroup.nodeid = nodeid;
            }
            else
            {
                groupid = dservice->addgroup(dservice, node.ip, node.port, 
                        node.limit, &(dservice->session));
                qdocgroup.groupid = groupid;
                qdocgroup.nodeid = nodeid;
            }
        }
    }
    return ;
}

/* Initialize from ini file */
int sbase_initialize(SBASE *sbase, char *conf)
{
    int i = 0, n = 0, interval = 0, pidfd = 0;
    char *s = NULL, *p = NULL, line[256];

    if((dict = iniparser_new(conf)) == NULL)
    {
        fprintf(stderr, "Initializing conf:%s failed, %s\n", conf, strerror(errno));
        _exit(-1);
    }
    /* http headers map */
    if((http_headers_map = http_headers_map_init()) == NULL)
    {
        fprintf(stderr, "Initialize http_headers_map failed,%s", strerror(errno));
        _exit(-1);
    }
    if((p = iniparser_getstr(dict, "SBASE:pidfile"))
                        && (pidfd = open(p, O_CREAT|O_TRUNC|O_WRONLY, 0644)) > 0)
    {
        n = sprintf(line, "%lu", (unsigned long int)getpid());
        n = write(pidfd, line, n);
        close(pidfd);
    }
    /* SBASE */
    sbase->nchilds = iniparser_getint(dict, "SBASE:nchilds", 0);
    sbase->connections_limit = iniparser_getint(dict, "SBASE:connections_limit", SB_CONN_MAX);
        setrlimiter("RLIMIT_NOFILE", RLIMIT_NOFILE, sbase->connections_limit);
    sbase->usec_sleep = iniparser_getint(dict, "SBASE:usec_sleep", SB_USEC_SLEEP);
    sbase->set_log(sbase, iniparser_getstr(dict, "SBASE:logfile"));
    sbase->set_log_level(sbase, iniparser_getint(dict, "SBASE:log_level", 0));
    sbase->set_evlog(sbase, iniparser_getstr(dict, "SBASE:evlogfile"));
    sbase->set_evlog_level(sbase, iniparser_getint(dict, "SBASE:evlog_level", 0));
    if((argvmap = mtrie_init()) == NULL )_exit(-1);
    else
    {
        for(i = 0; i < E_ARGV_NUM; i++)
        {
            mtrie_add(argvmap, e_argvs[i], strlen(e_argvs[i]), i+1);
        }
    }
    /* state */ 
    TSTATE_INIT();
    /* httpd */
    is_inside_html = iniparser_getint(dict, "HTTPD:is_inside_html", 1);
    httpd_home = iniparser_getstr(dict, "HTTPD:httpd_home");
    http_default_charset = iniparser_getstr(dict, "HTTPD:httpd_charset");
    /* decode html base64 */
    if(html_code_base64 && (n = strlen(html_code_base64)) > 0
            && (httpd_index_html_code = (unsigned char *)calloc(1, n + 1)))
    {
        nhttpd_index_html_code = base64_decode(httpd_index_html_code,
                (char *)html_code_base64, n);
    }
    if((httpd = service_init()) == NULL)
    {
        fprintf(stderr, "Initialize service failed, %s", strerror(errno));
        _exit(-1);
    }
    httpd->family = iniparser_getint(dict, "HTTPD:inet_family", AF_INET);
    httpd->sock_type = iniparser_getint(dict, "HTTPD:socket_type", SOCK_STREAM);
    httpd->ip = iniparser_getstr(dict, "HTTPD:service_ip");
    httpd->port = iniparser_getint(dict, "HTTPD:service_port", 80);
    httpd->working_mode = iniparser_getint(dict, "HTTPD:working_mode", WORKING_PROC);
    httpd->service_type = iniparser_getint(dict, "HTTPD:service_type", S_SERVICE);
    httpd->service_name = iniparser_getstr(dict, "HTTPD:service_name");
    httpd->nprocthreads = iniparser_getint(dict, "HTTPD:nprocthreads", 1);
    httpd->ndaemons = iniparser_getint(dict, "HTTPD:ndaemons", 0);
    httpd->niodaemons = iniparser_getint(dict, "HTTPD:niodaemons", 1);
    httpd->use_cond_wait = iniparser_getint(dict, "HTTPD:use_cond_wait", 0);
    if(iniparser_getint(dict, "HTTPD:use_cpu_set", 0) > 0) httpd->flag |= SB_CPU_SET;
    if(iniparser_getint(dict, "HTTPD:event_lock", 0) > 0) httpd->flag |= SB_EVENT_LOCK;
    if(iniparser_getint(dict, "HTTPD:newconn_delay", 0) > 0) httpd->flag |= SB_NEWCONN_DELAY;
    if(iniparser_getint(dict, "HTTPD:tcp_nodelay", 0) > 0) httpd->flag |= SB_TCP_NODELAY;
    if(iniparser_getint(dict, "HTTPD:socket_linger", 0) > 0) httpd->flag |= SB_SO_LINGER;
    if(iniparser_getint(dict, "HTTPD:while_send", 0) > 0) httpd->flag |= SB_WHILE_SEND;
    if(iniparser_getint(dict, "HTTPD:log_thread", 0) > 0) httpd->flag |= SB_LOG_THREAD;
    if(iniparser_getint(dict, "HTTPD:use_outdaemon", 0) > 0) httpd->flag |= SB_USE_OUTDAEMON;
    if(iniparser_getint(dict, "HTTPD:use_evsig", 0) > 0) httpd->flag |= SB_USE_EVSIG;
    if(iniparser_getint(dict, "HTTPD:use_cond", 0) > 0) httpd->flag |= SB_USE_COND;
    if((n = iniparser_getint(dict, "HTTPD:sched_realtime", 0)) > 0) httpd->flag |= (n & (SB_SCHED_RR|SB_SCHED_FIFO));
    if((n = iniparser_getint(dict, "HTTPD:io_sleep", 0)) > 0) httpd->flag |= ((SB_IO_NANOSLEEP|SB_IO_USLEEP|SB_IO_SELECT) & n);
    httpd->nworking_tosleep = iniparser_getint(dict, "HTTPD:nworking_tosleep", SB_NWORKING_TOSLEEP);
    httpd->set_log(httpd, iniparser_getstr(dict, "HTTPD:logfile"));
    httpd->set_log_level(httpd, iniparser_getint(dict, "HTTPD:log_level", 0));
    httpd->session.packet_type = iniparser_getint(dict, "HTTPD:packet_type",PACKET_DELIMITER);
    httpd->session.packet_delimiter = iniparser_getstr(dict, "HTTPD:packet_delimiter");
    p = s = httpd->session.packet_delimiter;
    while(*p != 0 )
    {
        if(*p == '\\' && *(p+1) == 'n')
        {
            *s++ = '\n';
            p += 2;
        }
        else if (*p == '\\' && *(p+1) == 'r')
        {
            *s++ = '\r';
            p += 2;
        }
        else
            *s++ = *p++;
    }
    *s++ = 0;
    httpd->session.packet_delimiter_length = strlen(httpd->session.packet_delimiter);
    httpd->session.buffer_size = iniparser_getint(dict, "HTTPD:buffer_size", SB_BUF_SIZE);
    httpd->session.packet_reader = &httpd_packet_reader;
    httpd->session.packet_handler = &httpd_packet_handler;
    httpd->session.data_handler = &httpd_data_handler;
    httpd->session.timeout_handler = &httpd_timeout_handler;
    httpd->session.error_handler = &httpd_error_handler;
    httpd->session.oob_handler = &httpd_oob_handler;
    log_access = iniparser_getint(dict,"HTTPD:log_access", 0);
    error_mode = iniparser_getint(dict,"HTTPD:error_mode", 0);
    /* MMDB */
    if((p = iniparser_getstr(dict, "HTTPD:mmdb_dir")) && (mmdb = mmdb_init()))
    {
        mmdb->set_basedir(mmdb, p); 
        cache_life_time = iniparser_getint(dict,"HTTPD:mmdb_cache_life_time", M_CACHE_LIFE_TIME);
        mmdb_set_cache_life_time(mmdb, cache_life_time);
        n = iniparser_getint(dict,"HTTPD:mmdb_log_level", 0);
        mmdb_set_log_level(mmdb, n);
    }
    else
    {
        fprintf(stderr, "configure file no include 'mmdb_dir' \n");
        _exit(-1);
    }
    if((n = iniparser_getint(dict,"HTTPD:http_timeout", 0)) >0) http_timeout = n;
    if((n = iniparser_getint(dict,"HTTPD:qparser_timeout", 0)) >0) qparser_timeout = n;
    if((n = iniparser_getint(dict,"HTTPD:query_timeout", 0)) >0) query_timeout = n;
    if((n = iniparser_getint(dict,"HTTPD:summary_timeout", 0)) >0) summary_timeout = n;
    /* logger */
    if((p = iniparser_getstr(dict, "HTTPD:access_log")))
    {
        LOGGER_ROTATE_INIT(logger, p, LOG_ROTATE_DAY);
    }
    /* QSERVICE */
    if((qservice = service_init()) == NULL)
    {
        fprintf(stderr, "Initialize service failed, %s", strerror(errno));
        _exit(-1);
    }
    qservice->family = iniparser_getint(dict, "QSERVICE:inet_family", AF_INET);
    qservice->sock_type = iniparser_getint(dict, "QSERVICE:socket_type", SOCK_STREAM);
    //qservice->ip = iniparser_getstr(dict, "QSERVICE:service_ip");
    //qservice->port = iniparser_getint(dict, "QSERVICE:service_port", 4936);
    qservice->working_mode = iniparser_getint(dict, "QSERVICE:working_mode", WORKING_PROC);
    qservice->service_type = iniparser_getint(dict, "QSERVICE:service_type", C_SERVICE);
    qservice->service_name = iniparser_getstr(dict, "QSERVICE:service_name");
    qservice->nprocthreads = iniparser_getint(dict, "QSERVICE:nprocthreads", 1);
    qservice->ndaemons = iniparser_getint(dict, "QSERVICE:ndaemons", 0);
    qservice->niodaemons = iniparser_getint(dict, "QSERVICE:niodaemons", 1);
    qservice->use_cond_wait = iniparser_getint(dict, "QSERVICE:use_cond_wait", 0);
    if(iniparser_getint(dict, "QSERVICE:use_cpu_set", 0) > 0) qservice->flag |= SB_CPU_SET;
    if(iniparser_getint(dict, "QSERVICE:event_lock", 0) > 0) qservice->flag |= SB_EVENT_LOCK;
    if(iniparser_getint(dict, "QSERVICE:newconn_delay", 0) > 0) qservice->flag |= SB_NEWCONN_DELAY;
    if(iniparser_getint(dict, "QSERVICE:tcp_nodelay", 0) > 0) qservice->flag |= SB_TCP_NODELAY;
    if(iniparser_getint(dict, "QSERVICE:socket_linger", 0) > 0) qservice->flag |= SB_SO_LINGER;
    if(iniparser_getint(dict, "QSERVICE:while_send", 0) > 0) qservice->flag |= SB_WHILE_SEND;
    if(iniparser_getint(dict, "QSERVICE:log_thread", 0) > 0) qservice->flag |= SB_LOG_THREAD;
    if(iniparser_getint(dict, "QSERVICE:use_outdaemon", 0) > 0) qservice->flag |= SB_USE_OUTDAEMON;
    if(iniparser_getint(dict, "QSERVICE:use_evsig", 0) > 0) qservice->flag |= SB_USE_EVSIG;
    if(iniparser_getint(dict, "QSERVICE:use_cond", 0) > 0) qservice->flag |= SB_USE_COND;
    if((n = iniparser_getint(dict, "QSERVICE:sched_realtime", 0)) > 0) qservice->flag |= (n & (SB_SCHED_RR|SB_SCHED_FIFO));
    if((n = iniparser_getint(dict, "QSERVICE:io_sleep", 0)) > 0) qservice->flag |= ((SB_IO_NANOSLEEP|SB_IO_USLEEP|SB_IO_SELECT) & n);

    qservice->nworking_tosleep = iniparser_getint(dict, "QSERVICE:nworking_tosleep", SB_NWORKING_TOSLEEP);
    qservice->set_log(qservice, iniparser_getstr(dict, "QSERVICE:logfile"));
    qservice->set_log_level(qservice, iniparser_getint(dict, "QSERVICE:log_level", 0));
    qservice->session.flags = SB_NONBLOCK;
    qservice->session.packet_type = PACKET_CERTAIN_LENGTH;
    qservice->session.packet_length = sizeof(IHEAD);
    qservice->session.buffer_size = iniparser_getint(dict, "QSERVICE:buffer_size", SB_BUF_SIZE);
    qservice->session.quick_handler = &qservice_quick_handler;
    qservice->session.packet_handler = &qservice_packet_handler;
    qservice->session.data_handler = &qservice_data_handler;
    qservice->session.timeout_handler = &qservice_timeout_handler;
    qservice->session.error_handler = &qservice_error_handler;
    interval = iniparser_getint(dict, "QSERVICE:heartbeat_interval", SB_HEARTBEAT_INTERVAL);
    qservice->set_heartbeat(qservice, interval, &q_heartbeat_handler, qservice);
    /* DSERVICE */
    if((dservice = service_init()) == NULL)
    {
        fprintf(stderr, "Initialize service failed, %s\n", strerror(errno));
        _exit(-1);
    }
    dservice->family = iniparser_getint(dict, "DSERVICE:inet_family", AF_INET);
    dservice->sock_type = iniparser_getint(dict, "DSERVICE:socket_type", SOCK_STREAM);
    //dservice->ip = iniparser_getstr(dict, "DSERVICE:service_ip");
    //dservice->port = iniparser_getint(dict, "DSERVICE:service_port", 4520);
    dservice->working_mode = iniparser_getint(dict, "DSERVICE:working_mode", WORKING_PROC);
    dservice->service_type = iniparser_getint(dict, "DSERVICE:service_type", C_SERVICE);
    dservice->service_name = iniparser_getstr(dict, "DSERVICE:service_name");
    dservice->nprocthreads = iniparser_getint(dict, "DSERVICE:nprocthreads", 1);
    dservice->ndaemons = iniparser_getint(dict, "DSERVICE:ndaemons", 0);
    dservice->niodaemons = iniparser_getint(dict, "DSERVICE:niodaemons", 1);
    dservice->use_cond_wait = iniparser_getint(dict, "DSERVICE:use_cond_wait", 0);
    if(iniparser_getint(dict, "DSERVICE:use_cpu_set", 0) > 0) dservice->flag |= SB_CPU_SET;
    if(iniparser_getint(dict, "DSERVICE:event_lock", 0) > 0) dservice->flag |= SB_EVENT_LOCK;
    if(iniparser_getint(dict, "DSERVICE:newconn_delay", 0) > 0) dservice->flag |= SB_NEWCONN_DELAY;
    if(iniparser_getint(dict, "DSERVICE:tcp_nodelay", 0) > 0) dservice->flag |= SB_TCP_NODELAY;
    if(iniparser_getint(dict, "DSERVICE:socket_linger", 0) > 0) dservice->flag |= SB_SO_LINGER;
    if(iniparser_getint(dict, "DSERVICE:while_send", 0) > 0) dservice->flag |= SB_WHILE_SEND;
    if(iniparser_getint(dict, "DSERVICE:log_thread", 0) > 0) dservice->flag |= SB_LOG_THREAD;
    if(iniparser_getint(dict, "DSERVICE:use_outdaemon", 0) > 0) dservice->flag |= SB_USE_OUTDAEMON;
    if(iniparser_getint(dict, "DSERVICE:use_evsig", 0) > 0) dservice->flag |= SB_USE_EVSIG;
    if(iniparser_getint(dict, "DSERVICE:use_cond", 0) > 0) dservice->flag |= SB_USE_COND;
    if((n = iniparser_getint(dict, "DSERVICE:sched_realtime", 0)) > 0) dservice->flag |= (n & (SB_SCHED_RR|SB_SCHED_FIFO));
    if((n = iniparser_getint(dict, "DSERVICE:io_sleep", 0)) > 0) dservice->flag |= ((SB_IO_NANOSLEEP|SB_IO_USLEEP|SB_IO_SELECT) & n);
    dservice->nworking_tosleep = iniparser_getint(dict, "DSERVICE:nworking_tosleep", SB_NWORKING_TOSLEEP);
    dservice->set_log(dservice, iniparser_getstr(dict, "DSERVICE:logfile"));
    dservice->set_log_level(dservice, iniparser_getint(dict, "DSERVICE:log_level", 0));
    dservice->session.flags = SB_NONBLOCK;
    dservice->session.packet_type = PACKET_CERTAIN_LENGTH;
    dservice->session.packet_length = sizeof(IHEAD);
    dservice->session.buffer_size = iniparser_getint(dict, "DSERVICE:buffer_size", SB_BUF_SIZE);
    dservice->session.quick_handler = &dservice_quick_handler;
    dservice->session.packet_handler = &dservice_packet_handler;
    dservice->session.data_handler = &dservice_data_handler;
    dservice->session.timeout_handler = &dservice_timeout_handler;
    dservice->session.error_handler = &dservice_error_handler;
    interval = iniparser_getint(dict, "DSERVICE:heartbeat_interval", SB_HEARTBEAT_INTERVAL);
    dservice->set_heartbeat(dservice, interval, &d_heartbeat_handler, dservice);
    //return sbase->add_service(sbase, dservice);
    return (sbase->add_service(sbase, httpd) | sbase->add_service(sbase, dservice)
            | sbase->add_service(sbase, qservice));
}

static void masterd_stop(int sig)
{
    switch (sig)
    {
        case SIGINT:
        case SIGTERM:
            fprintf(stderr, "master server is interrupted by user.\n");
            if(sbase)sbase->stop(sbase);
            break;
        default:
            break;
    }
    return ;
}

int main(int argc, char **argv)
{
    char *conf = NULL, ch = 0;
    int is_daemon = 0;
    pid_t pid;

    /* get configure file */
    while((ch = getopt(argc, argv, "c:d")) != (char)-1)
    {
        if(ch == 'c') conf = optarg;
        else if(ch == 'd') is_daemon = 1;
    }
    if(conf == NULL)
    {
        fprintf(stderr, "Usage:%s -d -c config_file\n", argv[0]);
        _exit(-1);
    }
    /* locale */
    setlocale(LC_ALL, "C");
    /* signal */
    signal(SIGTERM, &masterd_stop);
    signal(SIGINT,  &masterd_stop);
    signal(SIGHUP,  &masterd_stop);
    signal(SIGPIPE, SIG_IGN);
    signal(SIGCHLD, SIG_IGN);
    //signal(SIGALRM, SIG_IGN);
    //signal(SIGTTOU, SIG_IGN);
    //signal(SIGTTIN, SIG_IGN);
    //signal(SIGTSTP, SIG_IGN);
    if(is_daemon)
    {
        pid = fork();
        switch (pid) {
            case -1:
                perror("fork()");
                exit(EXIT_FAILURE);
                break;
            case 0: /* child process */
                if(setsid() == -1)
                    exit(EXIT_FAILURE);
                break;
            default:/* parent */
                _exit(EXIT_SUCCESS);
                break;
        }
    }
    //setpriority(PRIO_PROCESS, getpid(), 19);
    if((sbase = sbase_init()) == NULL)
    {
        exit(EXIT_FAILURE);
        return -1;
    }
    fprintf(stdout, "Initializing from configure file:%s\n", conf);
    /* Initialize sbase */
    if(sbase_initialize(sbase, conf) != 0 )
    {
        fprintf(stderr, "Initialize from configure file failed\n");
        return -1;
    }
    fprintf(stdout, "Initialized successed\n");
    //fprintf(stdout, "DEBUG:%d*%d/%d sizeof(IQUERY):%d sizeof(QTERM):%d sizeof(double):%d IB_QUERY_MAX:%d\n", sizeof(IDISPLAY), IB_FIELDS_MAX, sizeof(IDISPLAY) * IB_FIELDS_MAX, sizeof(IQUERY), sizeof(QTERM), sizeof(double), IB_QUERY_MAX);
    //fprintf(stdout, "sizeof(IRES):%u sizeof(IRECORD)*2000:%u sizeof(IHEAD):%u sizeof(ICHUNK):%u\n", sizeof(IRES), sizeof(IRECORD)*2000, sizeof(IHEAD), sizeof(ICHUNK));
    //fprintf(stdout, "sizeof(IRES):%u sizeof(QTERM)*IB_QUERY_MAX:%u sizeof(IDISPLAY)*IB_FIELDS_MAX:%u sizeof(IQSET):%u sizeof(CQRES):%u\n", sizeof(IRES), sizeof(QTERM)*IB_QUERY_MAX, sizeof(IDISPLAY)*IB_FIELDS_MAX, sizeof(IQSET), sizeof(CQRES));
    sbase->running(sbase, 0);
    //sbase->running(sbase, 3600);
    //sbase->running(sbase, 60000000);
    //sbase->stop(sbase);
    sbase->clean(sbase);
    if(http_headers_map) http_headers_map_clean(http_headers_map);
    if(argvmap) mtrie_clean(argvmap);
    if(dict)iniparser_free(dict);
    if(httpd_index_html_code) free(httpd_index_html_code);
    if(mmdb)mmdb_clean(mmdb);
    return 0;
}
