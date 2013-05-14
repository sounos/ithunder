#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <locale.h>
#include <sbase.h>
#include <ibase.h>
#include "mtrie.h"
#include "http.h"
#include "iniparser.h"
#include "qindex.h"
#include "mutex.h"
#include "iqueue.h"
#include "stime.h"
#include "base64.h"
#include "base64dispatchdhtml.h"
#include "timer.h"
#include "logger.h"
#include "xmm.h"
#ifndef HTTP_BUF_SIZE
#define HTTP_BUF_SIZE           262144
#endif
#ifndef HTTP_QUERY_MAX
#define HTTP_QUERY_MAX          1024
#endif
#define HTTP_LINE_MAX           65536
#define HTTP_RESP_OK            "HTTP/1.0 200 OK\r\n\r\n"
#define HTTP_BAD_REQUEST        "HTTP/1.0 400 Bad Request\r\n\r\n"
#define HTTP_NOT_FOUND          "HTTP/1.0 404 Not Found\r\n\r\n" 
#define HTTP_NOT_MODIFIED       "HTTP/1.0 304 Not Modified\r\n\r\n"
#define HTTP_NO_CONTENT         "HTTP/1.0 204 No Content\r\n\r\n"
#define HTTPD_NUM_PAGE          20
#define HTTPD_NPAGE             20
#define HTTPD_PAGE_MAX          500
#define QXTASK_MAX              256
typedef struct _QXTASK
{
    int status;
    int id;
    MINDEX *mindex;
}QXTASK;
static QXTASK tasks[QXTASK_MAX];
static void *taskqueue = NULL;
static int ntasks = 0;
static int task_wait_max        = 30000000;
static int task_conn_timeout    = 20000000;
static int task_wait_timeout    = 120000000;
static int task_trans_timeout   = 5000000;
static int task_chunk_size      = 1048576;
static QINDEX *qindex = NULL;
static SBASE *sbase = NULL;
static SERVICE *dispatchd = NULL;
static SERVICE *httpd = NULL;
static dictionary *dict = NULL;
static void *logger = NULL;
static void *http_headers_map = NULL;
static char *http_default_charset = "UTF-8";
static char *httpd_home = NULL;
static int is_inside_html = 1;
static unsigned char *httpd_index_html_code = NULL;
static int  nhttpd_index_html_code = 0;
static void *argvmap = NULL;
#define E_OP_RESYNC             0x00
#define E_OP_ADD_NODE           1
#define E_OP_DEL_NODE           2
#define E_OP_SET_NODE           3
#define E_OP_LIST_NODE          4
#define E_OP_ADD_TASK           5
#define E_OP_DEL_TASK           6
#define E_OP_LIST_TASK          7
#define E_OP_UPDATE_RANK        8
#define E_OP_UPDATE_CATEGORY    9
#define E_OP_UPDATE_SLEVEL      10
#define E_OP_UPDATE_BITXCAT     11
#define E_OP_UPDATE_FINT        12
#define E_OP_UPDATE_FDOUBLE     13
#define E_OP_SET_IDX_STATUS     14
#define E_OP_SET_BTERM          15
#define E_OP_UPDATE_BTERM       16
#define E_OP_XXX_SSS            17
#define E_OP_LIST_BTERMS        18
#define E_OP_DEL_BTERM          19
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
    "taskid",
#define E_ARGV_TASKID       5
    "limit",
#define E_ARGV_LIMIT        6
    "name",
#define E_ARGV_NAME         7
    "termlist",
#define E_ARGV_TERMLIST     8
    "doclist",
#define E_ARGV_DOCLIST      9
    "rank",
#define E_ARGV_RANK         10
    "category",
#define E_ARGV_CATEGORY     11
    "slevel",
#define E_ARGV_SLEVEL       12
    "bitxcat",
#define E_ARGV_BITXCAT      13
    "fields",
#define E_ARGV_FIELDS       14
    "idx_status",
#define E_ARGV_IDX_STATUS   15
    "bterms",
#define E_ARGV_BTERMS       16
    "status",
#define E_ARGV_STATUS       17
    "termid"
#define E_ARGV_TERMID       18
};
#define  E_ARGV_NUM         19
/* data handler */
int dispatchd_packet_reader(CONN *conn, CB_DATA *buffer);
int dispatchd_packet_handler(CONN *conn, CB_DATA *packet);
int dispatchd_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int dispatchd_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int dispatchd_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int dispatchd_ok_handler(CONN *conn);
int httpd_request_handler(CONN *conn, HTTP_REQ *http_req);
/* httpd packet reader */
int httpd_packet_reader(CONN *conn, CB_DATA *buffer)
{
    if(conn)
    {
        return 0;
    }
    return -1;
}

/* converts hex char (0-9, A-Z, a-z) to decimal */
char hex2int(unsigned char hex)
{
    hex = hex - '0';
    if (hex > 9) {
        hex = (hex + '0' - 1) | 0x20;
        hex = hex - 'a' + 11;
    }
    if (hex > 15)
        hex = 0xFF;

    return hex;
}
#define URLDECODE(p, end, ps, high, low)                                                \
do                                                                                      \
{                                                                                       \
    while(*p != '\0' && *p != '&')                                                      \
    {                                                                                   \
        if(ps >= end) break;                                                            \
        else if(*p == '+') {*ps++ = 0x20;++p;continue;}                                 \
        else if(*p == '%')                                                              \
        {                                                                               \
            high = hex2int(*(p + 1));                                                   \
            if (high != 0xFF)                                                           \
            {                                                                           \
                low = hex2int(*(p + 2));                                                \
                if (low != 0xFF)                                                        \
                {                                                                       \
                    high = (high << 4) | low;                                           \
                    if (high < 32 || high == 127) high = '_';                           \
                    *ps++ = high;                                                       \
                    p += 3;                                                             \
                    continue;                                                           \
                }                                                                       \
            }                                                                           \
        }                                                                               \
        *ps++ = *p++;                                                                   \
    }                                                                                   \
    *ps = '\0';                                                                         \
}while(0)

/* packet handler */
int httpd_packet_handler(CONN *conn, CB_DATA *packet)
{
    char buf[HTTP_BUF_SIZE], file[HTTP_PATH_MAX], *p = NULL, *end = NULL;
    HTTP_REQ http_req = {0};
    struct stat st = {0};
    int ret = -1, n = 0;

    if(conn && packet)
    {
        p = packet->data;
        end = packet->data + packet->ndata;
        if(http_request_parse(p, end, &http_req, http_headers_map) == -1) goto err_end;
        if(http_req.reqid == HTTP_GET)
        {
            if(http_req.nargvs > 0 && httpd_request_handler(conn, &http_req) != -1)
            {
                return 0;
            }
            else if(is_inside_html && httpd_index_html_code && nhttpd_index_html_code > 0)
            {
                p = buf;
                p += sprintf(p, "HTTP/1.0 200 OK\r\nContent-Length: %d\r\n"
                        "Content-Type: text/html;charset=%s\r\n",
                        nhttpd_index_html_code, http_default_charset);
                if((n = http_req.headers[HEAD_GEN_CONNECTION]) > 0)
                    p += sprintf(p, "Connection: %s\r\n", (http_req.hlines + n));
                p += sprintf(p, "Connection:Keep-Alive\r\n");
                p += sprintf(p, "\r\n");
                conn->push_chunk(conn, buf, (p - buf));
                conn->push_chunk(conn, httpd_index_html_code, nhttpd_index_html_code);
                return conn->over(conn);
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
                        return conn->over(conn);
                    }
                    else if((n = http_req.headers[HEAD_REQ_IF_MODIFIED_SINCE]) > 0
                            && str2time(http_req.hlines + n) == st.st_mtime)
                    {
                        conn->push_chunk(conn, HTTP_NOT_MODIFIED, strlen(HTTP_NOT_MODIFIED));
                        return conn->over(conn);
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
                        return conn->over(conn);
                    }
                }
                else
                {
                    conn->push_chunk(conn, HTTP_NOT_FOUND, strlen(HTTP_NOT_FOUND));
                    return conn->over(conn);
                }
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
        return conn->over(conn);
    }
    return ret;
}

/* httpd request handler */
/* test 
//resync index
POST / HTTP/1.1
Content-Length: 4

op=0

//add node
POST / HTTP/1.1
Content-Length: 28

op=1&type=0&name=DS0&limit=0

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

//add task
POST / HTTP/1.1
Content-Length: 37

op=16&nodeid=0&ip=10.0.1.73&port=4936

//del task
POST / HTTP/1.1
Content-Length: 14

op=32&taskid=0

//list task
POST / HTTP/1.1
Content-Length: 14

op=64&nodeid=0
//update rank
POST / HTTP/1.1
Content-Length: 27

op=128&rank=47247672:0.6543

POST / HTTP/1.1
Content-Length: 27

op=128&rank=47169004:0.6543

//update category
POST / HTTP/1.1
Content-Length: 51

op=256&category=47247672:68157555;47169004:68157623

//update slevel
POST / HTTP/1.1
Content-Length: 33

op=512&slevel=3591778:9;4488139:6

//update bitxcat
POST / HTTP/1.1
Content-Length: 35

op=1024&bitxcat=3591778:3;4488139:2

//update int fields
POST / HTTP/1.1
Content-Length: 52

op=2048&fields=3591778:0:1175011232;47247672:3:110211

//update double fields
POST / HTTP/1.1
Content-Length: 31

op=4096&fields=1:3:0.23;2:3:1.76

//set idx status
POST / HTTP/1.1
Content-Length: 31

op=8192&idx_status=1:0;3:-1;8:0

*/
int httpd_request_handler(CONN *conn, HTTP_REQ *http_req)
{
    char buf[HTTP_LINE_MAX], *p = NULL, *end = NULL, *name = NULL, *ip = NULL,
         *bterms = NULL, *term = NULL;
    int ret = -1, i = 0, id = 0,  n = 0, type = -1, taskid = -1, nodeid = -1, 
        port = -1, limit = -1, op = -1, len = 0, termid = -1, status = -1;
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
                        case E_ARGV_TASKID:
                            taskid = atoi(p);
                            break;
                        case E_ARGV_LIMIT:
                            limit = atoi(p);
                            break;
                        case E_ARGV_NAME:
                            name = p;
                            break;
                        case E_ARGV_BTERMS:
                            bterms = p;
                            break;
                        case E_ARGV_STATUS:
                            status = atoi(p);
                            break;
                        case E_ARGV_TERMID:
                            termid = atoi(p);
                            break;
                        default:
                            break;
                    }
                }
            }
        }
        switch(op)
        {
            case E_OP_ADD_NODE:
                {
                    if(type >= 0 && name && limit >= 0 
                            && qindex_add_node(qindex, type, name, limit) >= 0)
                        goto nodelist;
                    else goto err_end;
                }
                break;
            case E_OP_DEL_NODE:
                {
                    if(nodeid > 0 && qindex_del_node(qindex, nodeid) >= 0)
                        goto nodelist;
                    else goto err_end;
                }
                break;
            case E_OP_LIST_NODE:
                goto nodelist;
                break;
            case E_OP_ADD_TASK:
                {
                    if(nodeid > 0 && ip && port > 0 
                            && qindex_add_task(qindex, nodeid, ip, port) >= 0 )
                        goto tasklist;
                    else goto err_end;
                }
                break;
            case E_OP_DEL_TASK:
                {
                    if(taskid >= 0 && qindex_del_task(qindex, taskid) >= 0)
                    {
                        nodeid = taskid / Q_TASKS_MAX;
                        goto tasklist;
                    }
                    else goto err_end;
                }
                break;
            case E_OP_SET_NODE:
                {
                    if(nodeid > 0 && limit >= 0 
                            && qindex_set_node_limit(qindex, nodeid, limit) >= 0)
                    {
                        goto nodelist;
                    }
                    else goto err_end;
                }
                break;
            case E_OP_LIST_TASK:
                {
                    if(nodeid > 0)
                        goto tasklist;
                    else goto err_end;
                }
                break;
            case E_OP_SET_BTERM:
                {
                    if((p = bterms) && status > 0)
                    {
                        while(*p != '\0')
                        {
                            while(*p != '\0' && *p == 0x20)++p;
                            term = p;
                            while(*p != '\0' && *p != ',' && *p != ';')++p;
                            if((n = (p - term)))
                            {
                                qindex_set_bterm(qindex, term,  n, status);
                            }
                            if(*p == ',' || *p == ';')++p;
                            else break;
                        }
                        qindex_sync_bterms(qindex);
                        goto btermslist;
                    }
                    else goto err_end;
                }
                break;
            case E_OP_DEL_BTERM:
                {
                    if(termid > 0 && qindex_del_bterm(qindex, termid) == 0)
                    {
                        qindex_sync_bterms(qindex);
                        goto btermslist;
                    }
                    else
                        goto err_end;
                }
                break;
            case E_OP_UPDATE_BTERM:
                {
                    if(termid > 0 && status > 0 && qindex_update_bterm(qindex, termid, status) == 0)
                    {
                        qindex_sync_bterms(qindex);
                        goto btermslist;
                    }
                    else
                        goto err_end;
                }
                break;
            case E_OP_LIST_BTERMS:
                goto btermslist;
                break;
            default :
                goto err_end;
                break;
        }
        return 0;
btermslist:
        len = sizeof(BSTERM) * qindex->state->bterm_id_max;
        if((chunk = conn->newchunk(conn, len)))
        {
            p = chunk->data;
            if((len = qindex_list_bterms(qindex, p)) > 0)
            {
                p = buf;
                p += sprintf(p, "HTTP/1.0 200 OK\r\nContent-Type: text/html;charset=%s\r\n"
                        "Content-Length: %d\r\n", http_default_charset, len);
                if((n = http_req->headers[HEAD_GEN_CONNECTION]) > 0)
                    p += sprintf(p, "Connection: %s\r\n", (http_req->hlines + n));
                p += sprintf(p, "\r\n");
                conn->push_chunk(conn, buf, p - buf);
                if(conn->send_chunk(conn, chunk, len) != 0)
                    conn->freechunk(conn, chunk);
                return 0;
            }
            else
            {
                conn->freechunk(conn, chunk);
            }
        }
        return 0;
nodelist:
        len = sizeof(QTASK) * Q_TASKS_MAX;
        if((chunk = conn->newchunk(conn, len)))
        {
            p = chunk->data;
            end = p + len;
            if((len = qindex_list_nodes(qindex, p, end)) > 0)
            {
                p = buf;
                p += sprintf(p, "HTTP/1.0 200 OK\r\nContent-Type: text/html;charset=%s\r\n"
                        "Content-Length: %d\r\n", http_default_charset, len);
                if((n = http_req->headers[HEAD_GEN_CONNECTION]) > 0)
                    p += sprintf(p, "Connection: %s\r\n", (http_req->hlines + n));
                p += sprintf(p, "Connection:Keep-Alive\r\n");
                p += sprintf(p, "\r\n");
                conn->push_chunk(conn, buf, p - buf);
                if(conn->send_chunk(conn, chunk, len) != 0)
                    conn->freechunk(conn, chunk);
                return 0;
            }
            else 
            {
                conn->freechunk(conn, chunk);
            }
        }
        return 0;
tasklist:
        len = sizeof(QTASK) * Q_TASKS_MAX;
        if((chunk = conn->newchunk(conn, len)))
        {
            p = chunk->data;
            end = p + len;
            if((len = qindex_list_tasks(qindex, nodeid, p, end)) > 0)
            {
                p = buf;
                p += sprintf(p, "HTTP/1.0 200 OK\r\nContent-Type: text/html;charset=%s\r\n"
                        "Content-Length: %d\r\n", http_default_charset, len);
                if((n = http_req->headers[HEAD_GEN_CONNECTION]) > 0)
                    p += sprintf(p, "Connection: %s\r\n", (http_req->hlines + n));
                p += sprintf(p, "Connection:Keep-Alive\r\n");
                p += sprintf(p, "\r\n");
                conn->push_chunk(conn, buf, p - buf);
                if(conn->send_chunk(conn, chunk, len) != 0)
                    conn->freechunk(conn, chunk);
                return 0;
            }
            else 
            {
                conn->freechunk(conn, chunk);
            }
        }
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

/* httpd timeout handler */
int httpd_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    if(conn)
    {
        conn->over_timeout(conn);
        return conn->over(conn);
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


/* packet reader */
int dispatchd_packet_reader(CONN *conn, CB_DATA *buffer)
{
    if(conn) return 0;
    return -1;
}

/* packet handler */
int dispatchd_packet_handler(CONN *conn, CB_DATA *packet)
{
    //int id = 0, *px = NULL;
    IHEAD *resp = NULL;

    if(conn && packet)
    {
        DEBUG_LOGGER(logger, "packet_handler() on remote[%s:%d] local[%s:%d] via %d", conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
        if((resp = (IHEAD *)(packet->data)))
        {
            if(resp->status == IB_STATUS_ERR)
            {
                DEBUG_LOGGER(logger, "index packet:[%d] on remote[%s:%d] local[%s:%d] via %d",
                        resp->id, conn->remote_ip, conn->remote_port, conn->local_ip, 
                        conn->local_port, conn->fd);
            }
            if(resp->cmd == IB_RESP_UPDATE) 
            {
                qindex_over_upindex(qindex, conn->c_id, resp->id);
            }
            else if(resp->cmd == IB_RESP_INDEX) 
            {
                qindex_over_index(qindex, conn->c_id, resp->id);
            }
            else if(resp->cmd == IB_RESP_UPDATE_BTERM) 
            {
                qindex_over_bterms(qindex, conn->c_id);
            }
            return dispatchd_ok_handler(conn);
        }
        else
        {
            qindex_push_task(qindex, conn->c_id);
            conn->over_timeout(conn);
            conn->over_evstate(conn);
            conn->over(conn);
        }
    }
    return -1;
}


/* data handler */
int dispatchd_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    if(conn)
        return 0;
    return -1;
}

/* error handler */
int dispatchd_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    //int id = 0, *px = NULL;

    if(conn)
    {
        DEBUG_LOGGER(logger, "error on remote[%s:%d] local[%s:%d] via %d", conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
        qindex_push_task(qindex, conn->c_id);
        return conn->over(conn);
    }
    return -1;
}

/* timeout handler */
int dispatchd_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    //int id = 0, *px = NULL;

    if(conn)
    {
        //FATAL_LOGGER(logger, "timeout on remote[%s:%d] local[%s:%d] via %d", conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
        if(conn->evstate == EVSTATE_WAIT)
        {
            conn->over_timeout(conn);
            conn->over_evstate(conn);
            return dispatchd_ok_handler(conn);
        }
        else
        {
            qindex_push_task(qindex, conn->c_id);
            conn->over_timeout(conn);
            conn->over_evstate(conn);
            return conn->over(conn);
        }
    }
    return -1;
}

/* task handler */
void dispatchd_task_handler(void *arg)
{
    int id = 0;

    if((id = (((long)arg) - 1)) >= 0 && id < ntasks && tasks)
    {
        if(qindex_work(qindex, tasks[id].mindex) == 0)
        {
            dispatchd->newtask(dispatchd, &dispatchd_task_handler, (void *)(((long )id+1)));
        }
        else
        {
            tasks[id].status = 0;
            iqueue_push(taskqueue, id);
        }
    }
    return ;
}

/* OOB handler */
int dispatchd_oob_handler(CONN *conn, CB_DATA *oob)
{
    if(conn)
    {
        return 0;
    }
    return -1;
}

/* ok handler */
int dispatchd_ok_handler(CONN *conn)
{
    int n = 0, id = 0, len = 0, *pcount = 0, nup = 0;
    CB_DATA *chunk = NULL;
    IHEAD *req = NULL;
    char *p = NULL;

    if(conn)
    {
        if(conn->status == 0)
        {
            conn->over_timeout(conn);
            conn->over_evstate(conn);
            if((chunk = conn->newchunk(conn, task_chunk_size)) && (p = chunk->data))
            {
                req = (IHEAD *)p;
                p += sizeof(IHEAD);
                pcount = (int *)p;
                //memcpy(p, &(packet.count), sizeof(int));
                p += sizeof(int);
                nup = n = task_chunk_size - sizeof(IHEAD) - sizeof(int);
                if((len = qindex_read_bterms(qindex, conn->c_id, (chunk->data + sizeof(IHEAD)),
                                task_chunk_size - sizeof(IHEAD))) > 0)
                {
                    req->id = conn->c_id;
                    req->cmd = IB_REQ_UPDATE_BTERM;
                    req->nodeid = -1;
                    req->length = len;
                    len += sizeof(IHEAD);
                    if(conn->send_chunk(conn, chunk, len) != 0)
                        conn->freechunk(conn, chunk);
                    else
                    {
                        conn->set_timeout(conn, task_wait_timeout);
                    }
                    return 0;
                }
                else if((id = qindex_read_index(qindex, conn->c_id, p, &n, pcount)) > 0 && n > 0)
                {
                    req->id = id;
                    req->cmd = IB_REQ_INDEX;
                    req->nodeid = -1;
                    req->length = n + sizeof(int);
                    len = sizeof(IHEAD) + req->length;
                    if(conn->send_chunk(conn, chunk, len) != 0)
                        conn->freechunk(conn, chunk);
                    conn->set_timeout(conn, task_wait_timeout);
                    return 0;
                }
                else if((id = qindex_read_upindex(qindex, conn->c_id, p, &nup, pcount))>0&& nup > 0)
                {
                    req->id = id;
                    req->cmd = IB_REQ_UPDATE;
                    req->nodeid = -1;
                    req->length = nup + sizeof(int);
                    len = sizeof(IHEAD) + req->length;
                    if(conn->send_chunk(conn, chunk, len) != 0)
                        conn->freechunk(conn, chunk);
                    conn->set_timeout(conn, task_wait_timeout);
                    return 0;
                }
                else 
                {
                    conn->freechunk(conn, chunk);
                    if(id == -2) return conn->close(conn);
                    goto timeout;
                }
            }
        }
timeout:
        if(conn->timeout >= task_wait_max) conn->timeout = 0;
        conn->start_cstate(conn);
        conn->wait_evstate(conn);
        return conn->set_timeout(conn, conn->timeout + task_trans_timeout);
    }
    return -1;
}

/* heartbeat */
void cb_heartbeat_handler(void *arg)
{
    int i = 0, id = 0, total = 0, is_add_to_queue = 0;
    QTASK task = {0};
    CONN *conn = NULL;

    if(arg == (void *)dispatchd)
    {
        total = QTOTAL(taskqueue);
        for(i = 0; i < total; i++)
        {
            id = -1;
            is_add_to_queue = 0;
            iqueue_pop(taskqueue, &id);
            if(id >= 0 && id < ntasks)
            {
                if(tasks[id].status == 0)
                {
                    if(dispatchd->newtask(dispatchd, &dispatchd_task_handler, (void *)((long)(id+1))) == 0)
                    {
                        tasks[id].status = 1;
                    }
                    else
                    {
                        is_add_to_queue = 1;
                    }
                }
                else is_add_to_queue = 1;
                if(is_add_to_queue)
                {
                    tasks[id].status = 0;
                    iqueue_push(taskqueue, id);
                }
            }
        }
        while((id = qindex_pop_task(qindex, &task)) >= 0)
        {
            if((conn = dispatchd->newconn(dispatchd, -1, -1, task.ip, task.port, NULL)))
            {
                conn->c_id = id;
                conn->start_cstate(conn);
                conn->set_timeout(conn, task_conn_timeout);
            }
            else
            {
                qindex_push_task(qindex, id);
                break;
            }
        }
    }
    return ;
}

/* Initialize from ini file */
int sbase_initialize(SBASE *sbase, char *conf)
{
    char *s = NULL, *p = NULL, *host = NULL, *key_name = NULL, *block_name = NULL;
    int interval = 0, i = 0, n = 0, port = 0, commitid = 0, queueid = 0;

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
    /* SBASE */
    sbase->nchilds = iniparser_getint(dict, "SBASE:nchilds", 0);
    sbase->connections_limit = iniparser_getint(dict, "SBASE:connections_limit", SB_CONN_MAX);
    setrlimiter("RLIMIT_NOFILE", RLIMIT_NOFILE, sbase->connections_limit);
    sbase->usec_sleep = iniparser_getint(dict, "SBASE:usec_sleep", SB_USEC_SLEEP);
    sbase->set_log(sbase, iniparser_getstr(dict, "SBASE:logfile"));
    sbase->set_log_level(sbase, iniparser_getint(dict, "SBASE:log_level", 0));
    sbase->set_evlog(sbase, iniparser_getstr(dict, "SBASE:evlogfile"));
    sbase->set_evlog_level(sbase, iniparser_getint(dict, "SBASE:evlog_level", 0));
    /* httpd */
    if((argvmap = mtrie_init()) == NULL) _exit(-1);
    else
    {
        for(i = 0; i < E_ARGV_NUM; i++)
        {
            mtrie_add(argvmap, e_argvs[i], strlen(e_argvs[i]), i+1);
        }
    }
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
    httpd->session.oob_handler = &httpd_oob_handler;
    if((qindex = qindex_init()))
    {
        qindex->log_level = iniparser_getint(dict, "QINDEX:log_level", 0);
        qindex_set_basedir(qindex, iniparser_getstr(dict, "QINDEX:basedir"));
        host = iniparser_getstr(dict, "QINDEX:qtask_server_host");
        port = iniparser_getint(dict, "QINDEX:qtask_server_port", 0);
        commitid = iniparser_getint(dict, "QINDEX:qtask_commitid", 0);
        queueid = iniparser_getint(dict, "QINDEX:qtask_queueid", 0);
        if(qindex_set_qtask_server(qindex, host, port, commitid, queueid) != 0)
        {
            fprintf(stderr, "Invalid arguments for set_qtask_server(host:%s, port:%d, "
                    "commitid:%d, queueid:%d)\n", host, port, commitid, queueid);
            _exit(-1);
        }
        host = iniparser_getstr(dict, "QINDEX:db_host");
        port = iniparser_getint(dict, "QINDEX:db_port", 0);
        key_name = iniparser_getstr(dict, "QINDEX:db_key_name");
        block_name = iniparser_getstr(dict, "QINDEX:db_block_name");
        if(qindex_set_db(qindex, host, port, key_name, block_name) != 0)
        {
            fprintf(stderr, "Invalid arguments for set_db(host:%s, port:%d, key_name:%s, "
                    "block_name:%s)\n", host, port, key_name, block_name);
            _exit(-1);
        }
    }
    else
    {
        fprintf(stderr, "initialize QINDEX failed, %s\n", strerror(errno));
        _exit(-1);
    }
    /* DISPATCHD */
    if((dispatchd = service_init()) == NULL)
    {
        fprintf(stderr, "Initialize service failed, %s", strerror(errno));
        _exit(-1);
    }
    dispatchd->family = iniparser_getint(dict, "DISPATCHD:inet_family", AF_INET);
    dispatchd->sock_type = iniparser_getint(dict, "DISPATCHD:socket_type", SOCK_STREAM);
    //server_ip = dispatchd->ip = iniparser_getstr(dict, "DISPATCHD:service_ip");
    //server_port = dispatchd->port = iniparser_getint(dict, "DISPATCHD:service_port", 7749);
    dispatchd->working_mode = iniparser_getint(dict, "DISPATCHD:working_mode", WORKING_PROC);
    dispatchd->service_type = iniparser_getint(dict, "DISPATCHD:service_type", C_SERVICE);
    dispatchd->service_name = iniparser_getstr(dict, "DISPATCHD:service_name");
    dispatchd->nprocthreads = iniparser_getint(dict, "DISPATCHD:nprocthreads", 1);
    ntasks = dispatchd->ndaemons = iniparser_getint(dict, "DISPATCHD:ndaemons", 0);
    dispatchd->niodaemons = iniparser_getint(dict, "DISPATCHD:niodaemons", 1);
    dispatchd->use_cond_wait = iniparser_getint(dict, "DISPATCHD:use_cond_wait", 0);
    if(iniparser_getint(dict, "DISPATCHD:use_cpu_set", 0) > 0) dispatchd->flag |= SB_CPU_SET;
    if(iniparser_getint(dict, "DISPATCHD:event_lock", 0) > 0) dispatchd->flag |= SB_EVENT_LOCK;
    if(iniparser_getint(dict, "DISPATCHD:newconn_delay", 0) > 0) dispatchd->flag |= SB_NEWCONN_DELAY;
    if(iniparser_getint(dict, "DISPATCHD:tcp_nodelay", 0) > 0) dispatchd->flag |= SB_TCP_NODELAY;
    if(iniparser_getint(dict, "DISPATCHD:socket_linger", 0) > 0) dispatchd->flag |= SB_SO_LINGER;
    if(iniparser_getint(dict, "DISPATCHD:while_send", 0) > 0) dispatchd->flag |= SB_WHILE_SEND;
    if(iniparser_getint(dict, "DISPATCHD:log_thread", 0) > 0) dispatchd->flag |= SB_LOG_THREAD;
    if(iniparser_getint(dict, "DISPATCHD:use_outdaemon", 0) > 0) dispatchd->flag |= SB_USE_OUTDAEMON;
    if(iniparser_getint(dict, "DISPATCHD:use_evsig", 0) > 0) dispatchd->flag |= SB_USE_EVSIG;
    if(iniparser_getint(dict, "DISPATCHD:use_cond", 0) > 0) dispatchd->flag |= SB_USE_COND;
    if((n = iniparser_getint(dict, "DISPATCHD:sched_realtime", 0)) > 0) dispatchd->flag |= (n & (SB_SCHED_RR|SB_SCHED_FIFO));
    if((n = iniparser_getint(dict, "DISPATCHD:io_sleep", 0)) > 0) dispatchd->flag |= ((SB_IO_NANOSLEEP|SB_IO_USLEEP|SB_IO_SELECT) & n);
    dispatchd->nworking_tosleep = iniparser_getint(dict, "DISPATCHD:nworking_tosleep", SB_NWORKING_TOSLEEP);
    dispatchd->set_log(dispatchd, iniparser_getstr(dict, "DISPATCHD:logfile"));
    dispatchd->set_log_level(dispatchd, iniparser_getint(dict, "DISPATCHD:log_level", 0));
    dispatchd->session.packet_type = PACKET_CERTAIN_LENGTH;
    dispatchd->session.packet_length = sizeof(IHEAD);
    dispatchd->session.buffer_size = iniparser_getint(dict, "DISPATCHD:buffer_size", SB_BUF_SIZE);
    dispatchd->session.packet_reader = &dispatchd_packet_reader;
    dispatchd->session.packet_handler = &dispatchd_packet_handler;
    dispatchd->session.data_handler = &dispatchd_data_handler;
    dispatchd->session.error_handler = &dispatchd_error_handler;
    dispatchd->session.timeout_handler = &dispatchd_timeout_handler;
    dispatchd->session.ok_handler = &dispatchd_ok_handler;
    dispatchd->session.oob_handler = &dispatchd_oob_handler;
    dispatchd->session.flags = SB_NONBLOCK;
    interval = iniparser_getint(dict, "DISPATCHD:heartbeat_interval", SB_HEARTBEAT_INTERVAL);
    dispatchd->set_heartbeat(dispatchd, interval, &cb_heartbeat_handler, dispatchd);
    LOGGER_INIT(logger, iniparser_getstr(dict, "DISPATCHD:access_log"));
    task_wait_max = iniparser_getint(dict, "DISPATCHD:task_wait_max", 30000000);
    task_conn_timeout = iniparser_getint(dict, "DISPATCHD:task_conn_timeout", 20000000);
    task_wait_timeout = iniparser_getint(dict, "DISPATCHD:task_wait_timeout", 120000000);
    task_trans_timeout = iniparser_getint(dict, "DISPATCHD:task_trans_timeout", 5000000);
    task_chunk_size = iniparser_getint(dict, "DISPATCHD:task_chunk_size", task_chunk_size);
    //tasks = (KTASK *)xmm_new(ntasks * sizeof(KTASK));
    if((taskqueue = iqueue_init()))
    {
        for(i = 0; i < ntasks; i++)
        {
            tasks[i].status = 0;
            tasks[i].id = i;
            iqueue_push(taskqueue, i);
            tasks[i].mindex = mindex_new(qindex);
        }
    }
    fprintf(stdout, "Parsing for server...\n");
    return (sbase->add_service(sbase, httpd) | sbase->add_service(sbase, dispatchd));
}

/* stop dispatchd */
static void dispatchd_stop(int sig)
{
    switch (sig) 
    {
        case SIGINT:
        case SIGTERM:
            fprintf(stderr, "dispatchd is interrupted by user.\n");
            if(sbase){sbase->stop(sbase);}
            break;
        default:
            break;
    }
}

/* main */
int main(int argc, char **argv)
{
    char *conf = NULL, ch = 0;
    int is_daemon = 0, i = 0;
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
    signal(SIGTERM, &dispatchd_stop);
    signal(SIGINT,  &dispatchd_stop);
    signal(SIGHUP,  &dispatchd_stop);
    signal(SIGPIPE, SIG_IGN);
    signal(SIGCHLD, SIG_IGN);
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
    sbase->running(sbase, 0);
    //sbase->running(sbase, 3600);
    //sbase->running(sbase, 60000000);
    //sbase->stop(sbase);
    sbase->clean(sbase);
    if(http_headers_map) http_headers_map_clean(http_headers_map);
    if(dict)iniparser_free(dict);
    for(i = 0; i < ntasks; i++)
    {
        mindex_clean(tasks[i].mindex);
    }
    if(taskqueue) iqueue_clean(taskqueue);
    if(qindex) qindex_clean(qindex);
    if(argvmap){mtrie_clean(argvmap);}
    if(httpd_index_html_code) free(httpd_index_html_code);
    return 0;
}
