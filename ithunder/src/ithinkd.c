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
#include <mtask.h>
#include "mtrie.h"
#include "http.h"
#include "iniparser.h"
#include "kindex.h"
#include "mutex.h"
#include "iqueue.h"
#include "stime.h"
#include "base64.h"
#include "base64thinkdhtml.h"
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
typedef struct _KTASK
{
    int status;
    int id;
    XINDEX *xindex;
}KTASK;
static KINDEX *kindex = NULL;
static SBASE *sbase = NULL;
static SERVICE *thinkd = NULL;
static SERVICE *httpd = NULL;
static dictionary *dict = NULL;
static void *logger = NULL;
static KTASK *tasks = NULL;
static void *taskqueue = NULL;
static int ntasks = 0;
static int running_status = 1;
static void *http_headers_map = NULL;
static char *http_default_charset = "UTF-8";
static char *httpd_home = NULL;
static int is_inside_html = 1;
static unsigned char *httpd_index_html_code = NULL;
static int  nhttpd_index_html_code = 0;
static void *argvmap = NULL;
#define E_OP_RESYNC             0x00
#define E_OP_STATE              0x01
static char *e_argvs[] =
{
    "op"
#define E_ARGV_OP           0
};
#define  E_ARGV_NUM         1
/* data handler */
int thinkd_packet_reader(CONN *conn, CB_DATA *buffer);
int thinkd_packet_handler(CONN *conn, CB_DATA *packet);
int thinkd_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int thinkd_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int thinkd_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int httpd_request_handler(CONN *conn, HTTP_REQ *http_req);
int thinkd_ok_handler(CONN *conn);
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

/* httpd request handler */
int httpd_request_handler(CONN *conn, HTTP_REQ *http_req)
{
    char *p = NULL, buf[HTTP_BUF_SIZE], line[HTTP_LINE_MAX];
    int id = -1, n = -1, op = -1, i = 0;

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
                        default :
                            break;
                    }
                }
            }
        }
        if(op == E_OP_STATE)
        {
            if((n = kindex_state(kindex, line)) > 0)        
            {
                p = buf;
                p += sprintf(p, "HTTP/1.0 200 OK\r\nContent-Length: %d\r\n"
                        "Content-Type: text/html;charset=%s\r\n\r\n",n, http_default_charset);
                conn->push_chunk(conn, buf, (p - buf));
                conn->push_chunk(conn, line, n);
                return conn->over(conn);
            }
        }
    }
    return -1;
}

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
            if(http_req.nargvs > 0)
            {
                if(httpd_request_handler(conn, &http_req) < 0) goto err_end;
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

/*  data handler */
int httpd_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    int ret = -1;

    if(conn && packet && cache && chunk && chunk->ndata > 0)
    {
        ret = 0;
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
int thinkd_packet_reader(CONN *conn, CB_DATA *buffer)
{
    if(conn) return 0;
    return -1;
}

/* packet handler */
int thinkd_packet_handler(CONN *conn, CB_DATA *packet)
{
    if(conn && packet)
    {
        return 0;
    }
    return -1;
}


/* data handler */
int thinkd_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    if(conn)
        return 0;
    return -1;
}

/* error handler */
int thinkd_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    //int id = 0, *px = NULL;

    if(conn)
    {
        return conn->over(conn);
    }
    return -1;
}

/* timeout handler */
int thinkd_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    //int id = 0, *px = NULL;

    if(conn)
    {
        return 0;
    }
    return -1;
}

/* task handler */
void thinkd_task_handler(void *arg)
{
    int id = 0;

    if((id = (((long)arg) - 1)) >= 0 && id < ntasks && tasks)
    {
        if(kindex_work(kindex, tasks[id].xindex) == 0)
        {
            if(thinkd->newtask(thinkd, &thinkd_task_handler, (void *)(((long )id+1))) != 0)
            {
                tasks[id].status = 0;
                iqueue_push(taskqueue, id);
            }
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
int thinkd_oob_handler(CONN *conn, CB_DATA *oob)
{
    if(conn)
    {
        return 0;
    }
    return -1;
}

/* heartbeat */
void cb_heartbeat_handler(void *arg)
{
    int i = 0, id = 0, total = 0, is_add_to_queue = 0;

    if(arg == (void *)thinkd)
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
                    if(thinkd->newtask(thinkd, &thinkd_task_handler, (void *)((long)(id+1))) == 0)
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
    }
    return ;
}

/* Initialize from ini file */
int sbase_initialize(SBASE *sbase, char *conf)
{
    char *s = NULL, *p = NULL, *dictfile = NULL, *dictrules = NULL, *host = NULL, 
         *property_name = NULL, *text_index_name = NULL, *int_index_name = NULL,
         *long_index_name = NULL, *double_index_name = NULL, *display_name = NULL,
         *block_name = NULL, *key_name;
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
    if((argvmap = mtrie_init()) == NULL) _exit(-1);
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
    if((kindex = kindex_init()))
    {
        dictrules = iniparser_getstr(dict, "KINDEX:dictrules");
        dictfile = iniparser_getstr(dict, "KINDEX:dictfile");
        kindex_set_dict(kindex, dictfile, http_default_charset, dictrules);
        kindex->compress_status = iniparser_getint(dict, "KINDEX:compress_status", 0);
        kindex->phrase_status= iniparser_getint(dict, "KINDEX:phrase_status", 0);
        kindex->log_level = iniparser_getint(dict, "KINDEX:log_level", 0);
        kindex_set_basedir(kindex, iniparser_getstr(dict, "KINDEX:basedir"));
        host = iniparser_getstr(dict, "KINDEX:qtask_server_host");
        port = iniparser_getint(dict, "KINDEX:qtask_server_port", 0);
        commitid = iniparser_getint(dict, "KINDEX:qtask_commitid", 0);
        queueid = iniparser_getint(dict, "KINDEX:qtask_queueid", 0);
        if(kindex_set_qtask_server(kindex, host, port, commitid, queueid) != 0)
        {
            fprintf(stderr, "Invalid arguments for set_qtask_server(host:%s port:%d commitid:%d queueid:%d)\n", host, port, commitid, queueid);
            _exit(-1);
        }
        host = iniparser_getstr(dict, "KINDEX:source_db_host");
        port = iniparser_getint(dict, "KINDEX:source_db_port", 0);
        key_name = iniparser_getstr(dict, "KINDEX:source_db_key_name");
        property_name = iniparser_getstr(dict, "KINDEX:source_db_property_name");
        text_index_name = iniparser_getstr(dict, "KINDEX:source_db_text_index_name");
        int_index_name = iniparser_getstr(dict, "KINDEX:source_db_int_index_name");
        long_index_name = iniparser_getstr(dict, "KINDEX:source_db_long_index_name");
        double_index_name = iniparser_getstr(dict, "KINDEX:source_db_double_index_name");
        display_name = iniparser_getstr(dict, "KINDEX:source_db_display_name");
        if(kindex_set_source_db(kindex, host, port, key_name, property_name, text_index_name, 
                    int_index_name, long_index_name, double_index_name, display_name) != 0)
        {
            fprintf(stderr, "Invalid arguments for set_source_db(host:%s,port:%d, "
                    "key_name:%s, property_name:%s,text_index_name:%s, int_index_name:%s, "
                    "long_index_name:%s, double_index_name:%s, display_name:%s)\n", 
                    host, port, key_name, property_name,text_index_name, int_index_name, 
                    long_index_name, double_index_name, display_name);
            _exit(-1);
        }
        host = iniparser_getstr(dict, "KINDEX:res_db_host");
        port = iniparser_getint(dict, "KINDEX:res_db_port", 0);
        key_name = iniparser_getstr(dict, "KINDEX:res_db_key_name");
        block_name = iniparser_getstr(dict, "KINDEX:res_db_block_name");
        if(kindex_set_res_db(kindex, host, port, key_name, block_name) != 0)
        {
            fprintf(stderr, "Invalid arguments for set_res_db(host:%s, port:%d, "
                    "key_name:%s, block_name:%s)\n", host, port, key_name, block_name);
            _exit(-1);
        }
    }
    else
    {
        fprintf(stderr, "initialize KINDEX failed, %s\n", strerror(errno));
        _exit(-1);
    }
    /* THINKD */
    if((thinkd = service_init()) == NULL)
    {
        fprintf(stderr, "Initialize service failed, %s", strerror(errno));
        _exit(-1);
    }
    thinkd->family = iniparser_getint(dict, "THINKD:inet_family", AF_INET);
    thinkd->sock_type = iniparser_getint(dict, "THINKD:socket_type", SOCK_STREAM);
    //server_ip = thinkd->ip = iniparser_getstr(dict, "THINKD:service_ip");
    //server_port = thinkd->port = iniparser_getint(dict, "THINKD:service_port", 7749);
    thinkd->working_mode = iniparser_getint(dict, "THINKD:working_mode", WORKING_PROC);
    thinkd->service_type = iniparser_getint(dict, "THINKD:service_type", C_SERVICE);
    thinkd->service_name = iniparser_getstr(dict, "THINKD:service_name");
    thinkd->nprocthreads = iniparser_getint(dict, "THINKD:nprocthreads", 1);
    ntasks = thinkd->ndaemons = iniparser_getint(dict, "THINKD:ndaemons", 0);
    thinkd->niodaemons = iniparser_getint(dict, "THINKD:niodaemons", 1);
    thinkd->use_cond_wait = iniparser_getint(dict, "THINKD:use_cond_wait", 0);
    if(iniparser_getint(dict, "THINKD:use_cpu_set", 0) > 0) thinkd->flag |= SB_CPU_SET;
    if(iniparser_getint(dict, "THINKD:event_lock", 0) > 0) thinkd->flag |= SB_EVENT_LOCK;
    if(iniparser_getint(dict, "THINKD:newconn_delay", 0) > 0) thinkd->flag |= SB_NEWCONN_DELAY;
    if(iniparser_getint(dict, "THINKD:tcp_nodelay", 0) > 0) thinkd->flag |= SB_TCP_NODELAY;
    if(iniparser_getint(dict, "THINKD:socket_linger", 0) > 0) thinkd->flag |= SB_SO_LINGER;
    if(iniparser_getint(dict, "THINKD:while_send", 0) > 0) thinkd->flag |= SB_WHILE_SEND;
    if(iniparser_getint(dict, "THINKD:log_thread", 0) > 0) thinkd->flag |= SB_LOG_THREAD;
    if(iniparser_getint(dict, "THINKD:use_outdaemon", 0) > 0) thinkd->flag |= SB_USE_OUTDAEMON;
    if(iniparser_getint(dict, "THINKD:use_evsig", 0) > 0) thinkd->flag |= SB_USE_EVSIG;
    if(iniparser_getint(dict, "THINKD:use_cond", 0) > 0) thinkd->flag |= SB_USE_COND;
    if((n = iniparser_getint(dict, "THINKD:sched_realtime", 0)) > 0) thinkd->flag |= (n & (SB_SCHED_RR|SB_SCHED_FIFO));
    if((n = iniparser_getint(dict, "THINKD:io_sleep", 0)) > 0) thinkd->flag |= ((SB_IO_NANOSLEEP|SB_IO_USLEEP|SB_IO_SELECT) & n);
    thinkd->nworking_tosleep = iniparser_getint(dict, "THINKD:nworking_tosleep", SB_NWORKING_TOSLEEP);
    thinkd->set_log(thinkd, iniparser_getstr(dict, "THINKD:logfile"));
    thinkd->set_log_level(thinkd, iniparser_getint(dict, "THINKD:log_level", 0));
    thinkd->session.packet_type = PACKET_CERTAIN_LENGTH;
    thinkd->session.packet_length = sizeof(MHEAD);
    thinkd->session.buffer_size = iniparser_getint(dict, "THINKD:buffer_size", SB_BUF_SIZE);
    thinkd->session.packet_reader = &thinkd_packet_reader;
    thinkd->session.packet_handler = &thinkd_packet_handler;
    thinkd->session.data_handler = &thinkd_data_handler;
    thinkd->session.error_handler = &thinkd_error_handler;
    thinkd->session.timeout_handler = &thinkd_timeout_handler;
    thinkd->session.oob_handler = &thinkd_oob_handler;
    thinkd->session.flags = SB_NONBLOCK;
    interval = iniparser_getint(dict, "THINKD:heartbeat_interval", SB_HEARTBEAT_INTERVAL);
    thinkd->set_heartbeat(thinkd, interval, &cb_heartbeat_handler, thinkd);
    LOGGER_INIT(logger, iniparser_getstr(dict, "THINKD:access_log"));
    tasks = (KTASK *)xmm_mnew(ntasks * sizeof(KTASK));
    if((taskqueue = iqueue_init()))
    {
        for(i = 0; i < ntasks; i++)
        {
            iqueue_push(taskqueue, i);
            tasks[i].id = i;
            tasks[i].status = 0;
            tasks[i].xindex = xindex_new(kindex);
            //fprintf(stdout, "i:%d xindex:%p\n", i, tasks[i].xindex);
        }
    }
    return (sbase->add_service(sbase, httpd) | sbase->add_service(sbase, thinkd));
}

/* stop thinkd */
static void thinkd_stop(int sig)
{
    switch (sig) 
    {
        case SIGINT:
        case SIGTERM:
            fprintf(stderr, "thinkd is interrupted by user.\n");
            running_status = 0;
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
    signal(SIGTERM, &thinkd_stop);
    signal(SIGINT,  &thinkd_stop);
    signal(SIGHUP,  &thinkd_stop);
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
    //fprintf(stdout, "sizeof(XINDEX):%u sizeof(TERMNODE):%u\n", sizeof(XINDEX), sizeof(TERMNODE));
    //while(running_status)sleep(1);
    sbase->running(sbase, 0);
    //sbase->running(sbase, 3600);
    //sbase->running(sbase, 60000000);
    //sbase->stop(sbase);
    sbase->clean(sbase);
    if(http_headers_map) http_headers_map_clean(http_headers_map);
    if(dict)iniparser_free(dict);
    if(tasks)
    {
        for(i = 0; i < ntasks; i++)
        {
            if(tasks[i].xindex) xindex_clean(tasks[i].xindex);
        }
        xmm_free(tasks, ntasks * sizeof(KTASK));
    }
    if(taskqueue){iqueue_clean(taskqueue);}
    if(kindex) kindex_clean(kindex);
    if(argvmap)mtrie_clean(argvmap);
    if(httpd_index_html_code) free(httpd_index_html_code);
    return 0;
}
