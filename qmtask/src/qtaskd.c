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
#include "qtask.h"
#include "mtrie.h"
#include "http.h"
#include "iniparser.h"
#include "mutex.h"
#include "stime.h"
#include "base64.h"
#include "base64qtaskdhtml.h"
#include "timer.h"
#include "logger.h"
#include "xmm.h"
#include "qtask.h"
#include "mtask.h"
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
static int task_wait_timeout    = 120000000;
static QTASK *qtask = NULL;
static SBASE *sbase = NULL;
static SERVICE *qtaskd = NULL;
static SERVICE *httpd = NULL;
static dictionary *dict = NULL;
//static CONN *dconn = NULL;
static void *logger = NULL;
//static int nconns = 1;
static void *http_headers_map = NULL;
static char *http_default_charset = "UTF-8";
static char *httpd_home = NULL;
static int is_inside_html = 1;
static unsigned char *httpd_index_html_code = NULL;
static int  nhttpd_index_html_code = 0;
static void *argvmap = NULL;
#define E_OP_ADD_TASK           1
#define E_OP_DEL_TASK           2
#define E_OP_RENAME_TASK        3
#define E_OP_LIST_TASK          4
static char *e_argvs[] =
{
    "op",
#define E_ARGV_OP           0
    "name",
#define E_ARGV_NAME         1
    "taskid"
#define E_ARGV_TASKID       2
};
#define  E_ARGV_NUM         3
/* data handler */
int qtaskd_packet_reader(CONN *conn, CB_DATA *buffer);
int qtaskd_packet_handler(CONN *conn, CB_DATA *packet);
int qtaskd_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int qtaskd_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int qtaskd_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
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
/*
//add task
POST / HTTP/1.1
Content-Length: 15

op=1&name=task1

//rename task
POST / HTTP/1.1
Content-Length: 25

op=2&taskid=1&name=task01

//delete task
POST / HTTP/1.1
Content-Length: 13

op=3&taskid=1

//list tasks
POST / HTTP/1.1
Content-Length: 4

op=4
*/
int httpd_request_handler(CONN *conn, HTTP_REQ *http_req)
{
    char buf[HTTP_LINE_MAX], *p = NULL, *end = NULL, *name = NULL;
    int ret = -1, taskid = -1, i = 0, op = -1, n = 0, id = -1, len;
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
                        case E_ARGV_TASKID :
                            taskid = atoi(p);
                            break;
                        case E_ARGV_NAME:
                            name = p;
                            break;
                        default:
                            break;
                    }
                }
            }
        }
        switch(op)
        {
            case E_OP_ADD_TASK:
                {
                    if(name && qtask_add_task(qtask, name) > 0)
                        goto tasklist;
                    else 
                        goto err_end;
                }
                break;
            case E_OP_RENAME_TASK:
                {
                    if(taskid > 0 && name && qtask_rename_task(qtask, taskid, name) > 0)
                        goto tasklist;
                    else 
                        goto err_end;
                }
                break;
            case E_OP_DEL_TASK:
                {
                    if(taskid > 0 && qtask_del_task(qtask, taskid) > 0)
                        goto tasklist;
                    else 
                        goto err_end;
                }
                break;
            case E_OP_LIST_TASK:
                goto tasklist;
                break;
            default :
                goto err_end;
                break;
        }
        return conn->over(conn);
tasklist:
        len = sizeof(XTASK) * QT_TASKS_MAX;
        if((chunk = conn->newchunk(conn, len)))
        {
            p = chunk->data;
            end = p + len;
            if((len = qtask_list_tasks(qtask, p, end)) > 0)
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
            }
            else 
            {
                conn->freechunk(conn, chunk);
            }
        }
        return conn->over(conn);
err_end:
        ret = conn->push_chunk(conn, HTTP_BAD_REQUEST, strlen(HTTP_BAD_REQUEST));
        return conn->over(conn);
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
        return conn->over(conn);
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
int qtaskd_packet_reader(CONN *conn, CB_DATA *buffer)
{
    if(conn) return 0;
    return -1;
}

/* request handler */
int qtaskd_request_handler(CONN *conn, MHEAD *mheader)
{
    MHEAD *xhead = NULL, mhead = {0};
    int len = 0, packetid = 0;
    CB_DATA *block = NULL;

    if(mheader->mtaskid > 0)
    {
        if(mheader->packetid > 0)
        {
            if(mheader->packetid > qtask->state->id_max)
            {
                FATAL_LOGGER(logger, "Invalid mid:%d qid:%d header->packetid:%d taskid:%d packetid:%d on remote[%s:%d] local[%s:%d] via %d", mheader->mtaskid, mheader->qtaskid, mheader->packetid, conn->xids[2], conn->xids[3], conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
                conn->xids[2] = conn->xids[3] = 0;
                return conn->push_chunk(conn, &mhead, sizeof(MHEAD)); 
                //_exit(-1);
            }
            //fprintf(stdout, "%s::%d cmd:%d mid:%d length:%d\n", __FILE__, __LINE__, mheader->cmd, mheader->mtaskid, mheader->length);
            if((mheader->cmd & MTASK_CMD_FINISH))
                qtask_over_packet(qtask, mheader->mtaskid, mheader->packetid);
            if(mheader->qtaskid > 0 && (mheader->cmd & MTASK_CMD_PUSH))
                qtask_push_packet(qtask, mheader->qtaskid, mheader->flag, mheader->packetid);
            if(mheader->cmd & MTASK_CMD_OVER)
                qtask_remove_packet(qtask, mheader->packetid);
        }
        else
        {
            if(conn->xids[2] > 0 && conn->xids[3] > 0)
                qtask_repacket(qtask, conn->xids[2], conn->xids[3]);
        }
        conn->xids[2] = conn->xids[3] = 0;
        conn->c_state = 0;
        if((mheader->cmd & MTASK_CMD_POP) 
                && (packetid = qtask_pop_packet(qtask, mheader->mtaskid, &len)) > 0 && len > 0)
        {
            if((block = conn->newchunk(conn, sizeof(MHEAD) + len))
                    && qtask_read_packet(qtask, mheader->mtaskid, packetid, 
                        ((char *)(block->data) + sizeof(MHEAD))) > 0)
            {
                conn->xids[2] = mheader->mtaskid;
                conn->xids[3] = packetid;
                conn->set_timeout(conn, task_wait_timeout);
                xhead = (MHEAD *)(block->data);
                xhead->length = len;
                xhead->packetid = packetid;
                xhead->mtaskid = mheader->mtaskid;
                xhead->qtaskid = mheader->qtaskid;
                xhead->cmd  = mheader->cmd;
                if(conn->send_chunk(conn, block, len + sizeof(MHEAD)) != 0)
                {
                    conn->freechunk(conn, block);
                    return conn->over(conn);
                }
            }
            else
            {
                qtask_repacket(qtask, mheader->mtaskid, packetid);
                conn->freechunk(conn, block);
                return 0;
            }
        }
        else
        {
            DEBUG_LOGGER(logger, "no_pop_packet() mid:%d qid:%d taskid:%d packetid:%d on remote[%s:%d] local[%s:%d] via %d", mheader->mtaskid, mheader->qtaskid, conn->xids[2], conn->xids[3], conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
            return conn->push_chunk(conn, &mhead, sizeof(MHEAD)); 
        }
    }
    return -1;
}

/* packet handler */
int qtaskd_packet_handler(CONN *conn, CB_DATA *packet)
{
    MHEAD *mheader = NULL, mhead = {0}; 

    if(conn && packet && (mheader = (MHEAD *)(packet->data)))
    {
        DEBUG_LOGGER(logger, "packet_handler() mid:%d qid:%d packetid:%d data->length:%d on remote[%s:%d] local[%s:%d] via %d", mheader->mtaskid, mheader->qtaskid, mheader->packetid, mheader->length, conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
        if(conn->xids[0] == 0 && mheader->mtaskid > 0 
                && qtask_update_workers(qtask, mheader->mtaskid, 1) > 0) 
        {
            conn->wait_estate(conn);
            conn->xids[0] = mheader->mtaskid;
            conn->xids[1] = mheader->qtaskid;
        }
        if(mheader->length > 0) 
        {
            conn->save_cache(conn, packet->data, packet->ndata);
            return conn->recv_chunk(conn, mheader->length);
        }
        else
        {
            conn->over_timeout(conn);
            //DEBUG_LOGGER(logger, "NO_DATA taskid:%d packetid:%d on remote[%s:%d] local[%s:%d] via %d", conn->xids[2], conn->xids[3], conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
            if((mheader->cmd & (MTASK_CMD_POP|MTASK_CMD_FINISH|MTASK_CMD_OVER)) 
                && mheader->mtaskid > 0)
            {
                DEBUG_LOGGER(logger, "pop_packet() mid:%d qid:%d taskid:%d packetid:%d on remote[%s:%d] local[%s:%d] via %d", mheader->mtaskid, mheader->qtaskid, conn->xids[2], conn->xids[3], conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
                return qtaskd_request_handler(conn, mheader);
            }
            else 
            {
                DEBUG_LOGGER(logger, "no_pop_packet() mid:%d qid:%d taskid:%d packetid:%d on remote[%s:%d] local[%s:%d] via %d", mheader->mtaskid, mheader->qtaskid, conn->xids[2], conn->xids[3], conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
                conn->xids[2] = conn->xids[3] = 0;
                return conn->push_chunk(conn, &mhead, sizeof(MHEAD));
            }
        }
    }
    return -1;
}

/* data handler */
int qtaskd_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    MHEAD *mheader = NULL, mhead = {0};
    //int ret = -1;

    if(conn && cache && cache->ndata > 0 && (mheader = (MHEAD *)(cache->data)) 
            && chunk && chunk->data && chunk->ndata)
    {
        conn->over_timeout(conn);
        if(mheader->qtaskid > 0 && (mheader->cmd & MTASK_CMD_PUSH))
        {
            if((mhead.packetid = qtask_gen_packet(qtask, (char *)chunk->data, chunk->ndata)) > 0)
            {
                //fprintf(stdout, "%s::%d packetid:%d\n", __FILE__, __LINE__, mhead.packetid);
                qtask_push_packet(qtask, mheader->qtaskid, mheader->flag, mhead.packetid);
            }
            else
            {
                DEBUG_LOGGER(logger, "GEN_PACKET() taskid:%d packetid:%d on remote[%s:%d] local[%s:%d] via %d", conn->xids[2], conn->xids[3], conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
                if(conn->xids[0] > 0) qtask_update_workers(qtask, conn->xids[0], -1);
                //conn->over_cstate(conn);
                return conn->close(conn);
            }
        }
        if((mheader->cmd & (MTASK_CMD_POP|MTASK_CMD_FINISH|MTASK_CMD_OVER)) 
                && mheader->mtaskid > 0)
        {
            DEBUG_LOGGER(logger, "pop_packet() mid:%d qid:%d taskid:%d packetid:%d on remote[%s:%d] local[%s:%d] via %d", mheader->mtaskid, mheader->qtaskid, conn->xids[2], conn->xids[3], conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
            return qtaskd_request_handler(conn, mheader);
        }
        else
        {
            conn->xids[2] = conn->xids[3] = 0;
            DEBUG_LOGGER(logger, "no_pop_packet() mid:%d qid:%d taskid:%d packetid:%d on remote[%s:%d] local[%s:%d] via %d", mheader->mtaskid, mheader->qtaskid, conn->xids[2], conn->xids[3], conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
            return conn->push_chunk(conn, &mhead, sizeof(MHEAD)); 
        }
        /*
        if(!(mheader->cmd & MTASK_CMD_POP)) 
            return conn->push_chunk(conn, &mhead, sizeof(MHEAD)); 
        */
    }
    return -1;
}

/* error handler */
int qtaskd_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    //int id = 0, *px = NULL;

    if(conn)
    {
        WARN_LOGGER(logger, "error taskid:%d packetid:%d on remote[%s:%d] local[%s:%d] via %d", conn->xids[2], conn->xids[3], conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
        if(conn->xids[2] > 0 && conn->xids[3] > 0)
        {
            qtask_repacket(qtask, conn->xids[2], conn->xids[3]);
        }
        if(conn->xids[0] > 0) qtask_update_workers(qtask, conn->xids[0], -1);
        return conn->over(conn);
    }
    return -1;
}

/* timeout handler */
int qtaskd_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    //int id = 0, *px = NULL;

    if(conn)
    {
        DEBUG_LOGGER(logger, "TIMEOUT taskid:%d packetid:%d on remote[%s:%d] local[%s:%d] via %d", conn->xids[2], conn->xids[3], conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
        conn->over_timeout(conn);
        if(conn->xids[0] > 0) qtask_update_workers(qtask, conn->xids[0], -1);
        if(conn->xids[2] > 0 && conn->xids[3] > 0)
        {
            qtask_repacket(qtask, conn->xids[2], conn->xids[3]);
        }
        conn->over_estate(conn);
        return conn->over(conn);
    }
    return -1;
}

/* OOB handler */
int qtaskd_oob_handler(CONN *conn, CB_DATA *oob)
{
    if(conn)
    {
        return 0;
    }
    return -1;
}

/* Initialize from ini file */
int sbase_initialize(SBASE *sbase, char *conf)
{
    char *s = NULL, *p = NULL;
    int i = 0, n = 0;

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
    if((argvmap = mtrie_init()) == NULL)_exit(-1);
    else
    {
        for(i = 0; i < E_ARGV_NUM; i++)
        {
            mtrie_add(argvmap, e_argvs[i], strlen(e_argvs[i]), i+1);
        }
    }
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
    httpd->port = iniparser_getint(dict, "HTTPD:service_port", 2080);
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
    if((p = iniparser_getstr(dict, "QTASKD:basedir")) && (qtask = qtask_init(p)))
    {
        qtask->log_access = iniparser_getint(dict, "QTASKD:log_access", 0);
    }
    else
    {
        fprintf(stderr, "initialize QTASK failed, %s\n", strerror(errno));
        _exit(-1);
    }
    /* QTASKD */
    if((qtaskd = service_init()) == NULL)
    {
        fprintf(stderr, "Initialize service failed, %s", strerror(errno));
        _exit(-1);
    }
    qtaskd->family = iniparser_getint(dict, "QTASKD:inet_family", AF_INET);
    qtaskd->sock_type = iniparser_getint(dict, "QTASKD:socket_type", SOCK_STREAM);
    qtaskd->ip = iniparser_getstr(dict, "QTASKD:service_ip");
    qtaskd->port = iniparser_getint(dict, "QTASKD:service_port", 2066);
    qtaskd->working_mode = iniparser_getint(dict, "QTASKD:working_mode", WORKING_PROC);
    qtaskd->service_type = iniparser_getint(dict, "QTASKD:service_type", S_SERVICE);
    qtaskd->service_name = iniparser_getstr(dict, "QTASKD:service_name");
    qtaskd->nprocthreads = iniparser_getint(dict, "QTASKD:nprocthreads", 8);
    qtaskd->ndaemons = iniparser_getint(dict, "QTASKD:ndaemons", 0);
    qtaskd->niodaemons = iniparser_getint(dict, "QTASKD:niodaemons", 1);
    qtaskd->use_cond_wait = iniparser_getint(dict, "QTASKD:use_cond_wait", 0);
    if(iniparser_getint(dict, "QTASKD:use_cpu_set", 0) > 0) qtaskd->flag |= SB_CPU_SET;
    if(iniparser_getint(dict, "QTASKD:event_lock", 0) > 0) qtaskd->flag |= SB_EVENT_LOCK;
    if(iniparser_getint(dict, "QTASKD:newconn_delay", 0) > 0) qtaskd->flag |= SB_NEWCONN_DELAY;
    if(iniparser_getint(dict, "QTASKD:tcp_nodelay", 0) > 0) qtaskd->flag |= SB_TCP_NODELAY;
    if(iniparser_getint(dict, "QTASKD:socket_linger", 0) > 0) qtaskd->flag |= SB_SO_LINGER;
    if(iniparser_getint(dict, "QTASKD:while_send", 0) > 0) qtaskd->flag |= SB_WHILE_SEND;
    if(iniparser_getint(dict, "QTASKD:log_thread", 0) > 0) qtaskd->flag |= SB_LOG_THREAD;
    if(iniparser_getint(dict, "QTASKD:use_outdaemon", 0) > 0) qtaskd->flag |= SB_USE_OUTDAEMON;
    if(iniparser_getint(dict, "QTASKD:use_evsig", 0) > 0) qtaskd->flag |= SB_USE_EVSIG;
    if(iniparser_getint(dict, "QTASKD:use_cond", 0) > 0) qtaskd->flag |= SB_USE_COND;
    if((n = iniparser_getint(dict, "QTASKD:sched_realtime", 0)) > 0) qtaskd->flag |= (n & (SB_SCHED_RR|SB_SCHED_FIFO));
    if((n = iniparser_getint(dict, "QTASKD:io_sleep", 0)) > 0) qtaskd->flag |= ((SB_IO_NANOSLEEP|SB_IO_USLEEP|SB_IO_SELECT) & n);
    qtaskd->nworking_tosleep = iniparser_getint(dict, "QTASKD:nworking_tosleep", SB_NWORKING_TOSLEEP);
    qtaskd->set_log(qtaskd, iniparser_getstr(dict, "QTASKD:logfile"));
    qtaskd->set_log_level(qtaskd, iniparser_getint(dict, "QTASKD:log_level", 0));
    qtaskd->session.packet_type = PACKET_CERTAIN_LENGTH;
    qtaskd->session.packet_length = sizeof(MHEAD);
    qtaskd->session.buffer_size = iniparser_getint(dict, "QTASKD:buffer_size", SB_BUF_SIZE);
    qtaskd->session.packet_reader = &qtaskd_packet_reader;
    qtaskd->session.packet_handler = &qtaskd_packet_handler;
    qtaskd->session.data_handler = &qtaskd_data_handler;
    qtaskd->session.error_handler = &qtaskd_error_handler;
    qtaskd->session.timeout_handler = &qtaskd_timeout_handler;
    qtaskd->session.oob_handler = &qtaskd_oob_handler;
    LOGGER_INIT(logger, iniparser_getstr(dict, "QTASKD:access_log"));
    task_wait_timeout = iniparser_getint(dict, "QTASKD:task_wait_timeout", 120000000);
    fprintf(stdout, "Parsing for server...\n");
    return (sbase->add_service(sbase, httpd) | sbase->add_service(sbase, qtaskd));
}

/* stop qtaskd */
static void qtaskd_stop(int sig)
{
    switch (sig) 
    {
        case SIGINT:
        case SIGTERM:
            fprintf(stderr, "qtaskd is interrupted by user.\n");
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
    signal(SIGTERM, &qtaskd_stop);
    signal(SIGINT,  &qtaskd_stop);
    signal(SIGHUP,  &qtaskd_stop);
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
    /*setrlimiter("RLIMIT_NOFILE", RLIMIT_NOFILE, 65536)*/
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
    if(http_headers_map) mtrie_clean(http_headers_map);
    if(dict) iniparser_free(dict);
    if(qtask) qtask_clean(qtask);
    if(argvmap){mtrie_clean(argvmap);}
    if(httpd_index_html_code) free(httpd_index_html_code);
    return 0;
}
