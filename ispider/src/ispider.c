#define _GNU_SOURCE
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
#include <iconv.h>
#include <chardet.h>
#include "mtrie.h"
#include "http.h"
#include "mime.h"
#include "iniparser.h"
#include "mutex.h"
#include "iqueue.h"
#include "stime.h"
#include "base64.h"
#include "timer.h"
#include "logger.h"
#include "xmm.h"
#include "xtask.h"
#include "zstream.h"
#include "base64.h"
#include "qchardet.h"
#ifndef HTTP_BUF_SIZE
#define HTTP_BUF_SIZE           262144
#endif
#ifndef HTTP_QUERY_MAX
#define HTTP_QUERY_MAX          1024
#endif
#define HTTP_LINE_MAX           8192
#define HTTPD_NUM_PAGE          20
#define HTTPD_NPAGE             20
#define HTTPD_PAGE_MAX          500
#define HTTP_REQ_MAX            8192
#define HTTP_REDIRECT           0x01
#define LI(xxxx) ((unsigned int)xxxx)
typedef struct _HTTPTASK
{
    int status; 
    int flag;
    int taskid;
    int id;
    CONN *conn;
    XTMETA meta;
    char url[HTTP_URL_MAX];
    char headers[HTTP_LINE_MAX];
}HTTPTASK;
typedef struct _WTASK
{
    int status;
    int id;
    int total;
    int over;
    int nchunk;
    char *p;
    void *mutex;
    CB_DATA *chunk;
    CONN *conn;
    HTTPTASK conns[HTTP_TASK_MAX];
}WTASK;
static SBASE *sbase = NULL;
static SERVICE *spider = NULL;
static SERVICE *httpd = NULL;
static dictionary *dict = NULL;
static void *logger = NULL;
static WTASK *tasks = NULL;
static void *taskqueue = NULL;
static int ntasks = 0;
static int running_status = 1;
static MIME_MAP http_mime_map = {0};
static void *http_headers_map = NULL;
static char *http_default_charset = "UTF-8";
static char *monitord_ip = "127.0.0.1";
static int  monitord_port = 3082;
static void *argvmap = NULL;
static QCHARDET qchardet = {0};
static SESSION http_session = {0};
static int http_task_type = 0;
static int http_task_timeout = 120000000;
static int http_download_limit = 2097152;
static int task_wait_time = 10000000;
#define E_OP_RESYNC             0x00
#define E_OP_STATE              0x01
static char *e_argvs[] =
{
    "op"
#define E_ARGV_OP           0
};
#define  E_ARGV_NUM         1
/* data handler */
int spider_packet_reader(CONN *conn, CB_DATA *buffer);
int spider_packet_handler(CONN *conn, CB_DATA *packet);
int spider_quick_handler(CONN *conn, CB_DATA *packet);
int spider_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int spider_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int spider_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int spider_evtimeout_handler(CONN *conn);
int spider_ok_handler(CONN *conn);
void spider_over_task();

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
/* data handler */
int http_packet_reader(CONN *conn, CB_DATA *buffer);
int http_packet_handler(CONN *conn, CB_DATA *packet);
int http_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int http_ending_handler(CONN *conn);
int http_chunk_reader(CONN *conn, CB_DATA *buffer);
int http_chunk_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int http_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int http_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int http_trans_handler(CONN *conn, int tid);
int http_oob_handler(CONN *conn, CB_DATA *oob);
int http_download_redirect(int tid, int k, char *url);
int http_download_error(int tid, int k, int err);
int http_download_over(int tid, int k, int , HTTP_RESPONSE *resp, char *, size_t);

/* http download error */
int http_download_error(int tid, int k, int err)
{
    XTREC *rec = NULL;
    CONN *conn = NULL;
    char line[XT_LINE_SIZE];

    if((conn = tasks[tid].conns[k].conn))
    {
        line[0] = 0;
        if(err) xtask_err_msg(err, line);
        WARN_LOGGER(logger, "err:{%s} urlid:%d url:%s (recv:%lld sent:%lld)", line, tasks[tid].conns[k].id, tasks[tid].conns[k].url, LL(conn->recv_data_total), LL(conn->sent_data_total));
        MUTEX_LOCK(tasks[tid].mutex);
        memset(tasks[tid].p, 0, sizeof(XTREC));
        rec = (XTREC *)tasks[tid].p;
        rec->id = tasks[tid].conns[k].meta.id;
        rec->err = err;
        tasks[tid].p += sizeof(XTREC) ;
        memset(&(tasks[tid].conns[k]), 0, sizeof(HTTPTASK));
        //fprintf(stdout, "%s::%d total:%d over:%d\r\n", __FILE__, __LINE__, tasks[tid].over, tasks[tid].total);
        if(++(tasks[tid].over) == tasks[tid].total) spider_over_task(tid);
        MUTEX_UNLOCK(tasks[tid].mutex);
    }
    return -1;
}

int http_packet_reader(CONN *conn, CB_DATA *buffer)
{
    if(conn)
    {
        return 0;
    }
    return -1;
}

/* download */
int http_packet_handler(CONN *conn, CB_DATA *packet)
{
    int n = 0, tid = 0, k = 0, mime = -1;
    HTTP_RESPONSE http_resp = {0};
    char *p = NULL, *end = NULL;

    if(conn)
    {
        tid = conn->session.xids[0];
        k = conn->session.xids[1];
        p = packet->data;
        end = packet->data + packet->ndata;
        /* http handler */        
        if(conn == tasks[tid].conns[k].conn)
        {
            DEBUG_LOGGER(logger, "urlid:%d url:%s response:%s", tasks[tid].conns[k].id, tasks[tid].conns[k].url, packet->data);
            if(p == NULL || http_response_parse(p, end, &http_resp, http_headers_map) == -1)
            {
                conn->over_cstate(conn);
                conn->over(conn);
                ERROR_LOGGER(logger, "Invalid http response header on TASK[%d][%d] via url:%s", 
                        tid, k, tasks[tid].conns[k].url);
                return http_download_error(tid, k, ERR_PROGRAM);
            }
            if(http_resp.respid >= 0 && http_resp.respid < HTTP_RESPONSE_NUM)
                p = response_status[http_resp.respid].e;
            DEBUG_LOGGER(logger, "Ready for handling http response:[%d:%s] on url[%s] "
                    "remote[%s:%d] local[%s:%d] via %d", http_resp.respid, p, 
                    tasks[tid].conns[k].url, conn->remote_ip, conn->remote_port, 
                    conn->local_ip, conn->local_port, conn->fd);
            //location
            if(http_resp.respid == RESP_MOVEDPERMANENTLY
                && (n = http_resp.headers[HEAD_RESP_LOCATION]) > 0
                && (p = (http_resp.hlines + n)) )
            {
                return http_download_redirect(tid, k, p);
            }
            //check content-type
            if((n = http_resp.headers[HEAD_ENT_CONTENT_TYPE]) > 0 && (p = (http_resp.hlines + n)))
                mime = mime_id(&http_mime_map, p, strlen(p));
            if(http_resp.respid != RESP_OK
                || (http_mime_map.num > 0 && mime == -1))
            {
                conn->over_cstate(conn);
                conn->close(conn);
                ERROR_LOGGER(logger, "Invalid response [%s] on TASK[%d][%d].url:%s", 
                        http_resp.hlines, tid, k, tasks[tid].conns[k].url);
                return http_download_error(tid, k, ERR_CONTENT_TYPE);
            }
            else
            {
                conn->save_cache(conn, &http_resp, sizeof(HTTP_RESPONSE));
                conn->xids[6] = 0;
                if((n = http_resp.headers[HEAD_ENT_CONTENT_LENGTH]) > 0 
                        && (n = atol(http_resp.hlines + n)) > 0 
                        && n < http_download_limit)
                {
                    conn->xids[6] = n;
                    conn->recv_chunk(conn, n);
                }
                else if((n = http_resp.headers[HEAD_GEN_TRANSFER_ENCODING]) > 0
                        && (p = (http_resp.hlines + n))  && strcasestr(p, "chunked"))
                {
                    conn->read_chunk(conn);
                }
                else
                {
                    conn->set_timeout(conn, http_task_timeout);
                    conn->recv_chunk(conn, HTML_MAX_SIZE);
                }
            }
        }
        else
        {
            FATAL_LOGGER(logger, "Invalid TASK[%d][%d] connection[%p][%s:%d] local[%s:%d] via %d,"
                    "url:%s", tid, k , conn, conn->remote_ip, conn->remote_port, 
                    conn->local_ip, conn->local_port, conn->fd, tasks[tid].conns[k].url);
        }
    }
    return -1;
}

/* conn ending handler */
int http_ending_handler(CONN *conn)
{
    CB_DATA cache = {0}, *pcache = NULL, *pchunk = NULL;
    HTTP_RESPONSE http_resp = {0}, *resp = NULL;
    int tid = 0, k = 0, n = 0, ret = -1;
    char *p = NULL, *end = NULL;

    if(conn)
    {
        tid = conn->session.xids[0];
        k = conn->session.xids[1];
        /* http handler */        
        if(conn == tasks[tid].conns[k].conn)
        {
            if(conn->cache.ndata > 0)
            {
                resp = (HTTP_RESPONSE *)(conn->cache.data);
                pcache = (CB_DATA *)&(conn->cache);
            }
            else
            {
                p = conn->packet.data;
                end = conn->packet.data + conn->packet.ndata;
                if(end > p && http_response_parse(p, end, &http_resp, http_headers_map) != -1)
                {
                    cache.data = (char *)&http_resp;
                    cache.ndata = (int )sizeof(HTTP_RESPONSE);
                    resp = &http_resp;
                    pcache = &cache;
                }
            }
            if(conn->chunk.ndata > 0)
            {
                 pchunk = (CB_DATA *)&(conn->chunk);
            }
            else if(conn->buffer.ndata > 0 && conn->packet.ndata > 0)
            {
                pchunk = (CB_DATA *)&(conn->buffer);
            }
            if(resp && pchunk && pcache)
            {
                DEBUG_LOGGER(logger, "urlid:%d url:%s resp:%p pchunk:%p pcache:%p buffer_len:%d packet_len:%d cache_len:%d chunk_len:%d response:%s", tasks[tid].conns[k].id, tasks[tid].conns[k].url, resp, pchunk, pcache, conn->buffer.ndata, conn->packet.ndata, conn->cache.ndata, conn->chunk.ndata, conn->packet.data);
                if(((n = resp->headers[HEAD_ENT_CONTENT_LENGTH]) > 0
                    && (n = atol(resp->hlines + n)) > 0 && (pchunk->ndata >= n))
                    || ((n = resp->headers[HEAD_ENT_CONTENT_ENCODING]) > 0)
                    || ((n = resp->headers[HEAD_GEN_TRANSFER_ENCODING]) > 0
                        && (p = (resp->hlines + n))  && strcasestr(p, "chunked")
                        && http_chunk_reader(conn, pchunk) != -1))
                {
                    http_data_handler(conn, (CB_DATA *)&(conn->packet), pcache, pchunk);
                    ret = 0;
                }
            }
        }
    }
    return ret;
}

/* error handler */
int http_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    int tid = 0, k = 0, ret = -1;

    if(conn)
    {
        tid = conn->session.xids[0];
        k = conn->session.xids[1];
        if(tid >= 0 && k >= 0 && conn == tasks[tid].conns[k].conn && packet && cache && chunk) 
        {
            if(conn->xids[6] > 0)
            {
                if(chunk->ndata >= conn->xids[6])
                    return http_data_handler(conn, packet, cache, chunk);
            }
            ret = http_ending_handler(conn);
            if(ret < 0)
            {
                WARN_LOGGER(logger, "download urlid:%d url:%s error_handler(%p) "
                        " on remote[%s:%d] local[%s:%d] via %d buffer_len:%d "
                        " packet_len:%d cache_len:%d chunk_len:%d headers:%s", 
                        tasks[tid].conns[k].id, tasks[tid].conns[k].url, conn, conn->remote_ip, 
                        conn->remote_port, conn->local_ip, conn->local_port, conn->fd, 
                        conn->buffer.ndata, packet->ndata, cache->ndata, chunk->ndata, 
                        tasks[tid].conns[k].headers);
                conn->close(conn);
                return http_download_error(tid, k, ERR_TASK_CONN);
            }
        }
    }
    return -1;
}


int http_oob_handler(CONN *conn, CB_DATA *oob)
{
    return 0;
}

/* timeout handler */
int http_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    int tid = 0, k = 0, ret = -1;

    if(conn)
    {
        tid = conn->session.xids[0];
        k = conn->session.xids[1];
        if(conn == tasks[tid].conns[k].conn) 
        {
            if(conn->xids[6] > 0)
            {
                if(chunk->ndata >= conn->xids[6])
                    return http_data_handler(conn, packet, cache, chunk);
            }
            ret = http_ending_handler(conn);
            if(ret < 0)
            {
                ERROR_LOGGER(logger, "TIMEOUT on %s:%d via %d, chunk->size:%d via url:%s", 
                        conn->remote_ip, conn->remote_port, conn->fd, 
                        chunk->ndata, tasks[tid].conns[k].url);
                conn->over_cstate(conn);
                conn->close(conn);
                return http_download_error(tid, k,  ERR_TASK_TIMEOUT);
            }
        }
    }
    return -1;
}

int http_download_redirect(int tid, int k, char *url)
{
    char *p = NULL, *s = NULL, *end = NULL, *ip = NULL, host[HTTP_HOST_MAX], 
         oldhost[HTTP_HOST_MAX];
    int ret = -1, port = 0, oldport = 0, nurl = 0;
    struct hostent *hp = NULL;
    CB_DATA *chunk = NULL;
    SESSION sess = {0};
    CONN *conn = NULL;

    if(tid >= 0 && k >= 0 && (conn = tasks[tid].conns[k].conn) && (p = url) 
            && (nurl = strlen(url)) > 0 && (end = (url + nurl)) > url)
    {
        if((p = strstr(url, "http://")))
        {
            p += 7;
            s = host;
            while(p < end && *p != '/' && *p != ':')
            {
                if(*p >= 'A' && *p <= 'Z')
                    *s++ = *p + ('a' - 'A');
                else 
                    *s++ = *p++;
            }
            *s = '\0';
            if(*p == ':') port = atoi(++p);
        }
        if((p = strstr(tasks[tid].conns[k].url, "http://")))
        {
            p += 7;
            s = oldhost;
            while(p < end && *p != '/' && *p != ':')
            {
                if(*p >= 'A' && *p <= 'Z')
                    *s++ = *p + ('a' - 'A');
                else 
                    *s++ = *p++;
            }
            *s = '\0';
            if(*p == ':') oldport = atoi(++p);
        }
        strcpy(tasks[tid].conns[k].url, url);
        if(tasks[tid].conns[k].meta.proxy_ip == 0  
            && (strcmp(host, oldhost) != 0 || port != oldport))
        {
            conn->close(conn);
            tasks[tid].conns[k].conn = NULL;
            if((hp = gethostbyname(host)))
            {
                ip = host;
                sprintf(ip, "%s", inet_ntoa(*((struct in_addr *)(hp->h_addr))));
                tasks[tid].conns[k].meta.ip = inet_addr(ip);
                if(port == 0) port = 80;
                memcpy(&sess, &http_session, sizeof(SESSION)); 
                sess.xids[0] = tid;
                sess.xids[1] = k;
                if((tasks[tid].conns[k].conn = httpd->newconn(httpd, 
                                -1, -1, ip, port, &sess)) == NULL)
                {
                    return http_download_error(tid, k, ERR_NETWORK);
                }
            }
            else 
                return http_download_error(tid, k, ERR_NETWORK);
        }
        else
        {
            if((chunk = conn->newchunk(conn, HTTP_HEADER_MAX)))
            {
                p = chunk->data;
                p += sprintf(p, "GET %s HTTP/1.1\r\n%s\r\n", tasks[tid].conns[k].url, 
                        tasks[tid].conns[k].headers);
                if(conn->send_chunk(conn, chunk, (p - chunk->data)) != 0) 
                {
                    conn->freechunk(conn, chunk);
                    return http_download_error(tid, k, ERR_NETWORK);
                }
            }
        }
    }
    return ret;
}
/* download over */
int http_download_over(int tid, int k, int is_need_compress, 
        HTTP_RESPONSE *http_resp, char *rawdata, size_t nrawdata)
{
    char *p = NULL, *ps = NULL, *zdata = NULL;
    int ret = -1, n = 0;
    size_t nzdata = 0;
    XTREC *rec = NULL;

    if(http_resp && rawdata && nrawdata > 0)
    {
        MUTEX_LOCK(tasks[tid].mutex);
        p = tasks[tid].p;
        memset(p, 0, sizeof(XTREC));
        rec = (XTREC *)p;
        rec->id = tasks[tid].conns[k].meta.id;
        p += sizeof(XTREC);
        /* location */
        if(tasks[tid].conns[k].flag & HTTP_REDIRECT)
        {
            rec->nlocation = sprintf(p, "%s", tasks[tid].conns[k].url);
            p += rec->nlocation + 1;
        }
        /* date */
        if((n = http_resp->headers[HEAD_GEN_DATE]))
        {
            rec->date = (unsigned int)str2time(http_resp->hlines + n);
        }
        /* last modified */
        if((n = http_resp->headers[HEAD_ENT_LAST_MODIFIED]))
        {
            rec->last_modified = (unsigned int)str2time(http_resp->hlines + n);
        }
        /* cookie */
        if(http_resp->ncookies > 0)
        {
            ps = p;
            p += http_cookie_line(http_resp, p);
            rec->ncookie = p - ps;
        }
        /* compress with zlib::inflate() */
        ACCESS_LOGGER(logger, "url:%s is_need_compess:%d data:%p ndata:%u left:%d", tasks[tid].conns[k].url, is_need_compress, rawdata, LI(nrawdata), HTTP_CHUNK_MAX - (p -  tasks[tid].chunk->data));
        ps = p;
        if(is_need_compress)
        {
            zdata = p;
            nzdata = HTTP_CHUNK_MAX - (p -  tasks[tid].chunk->data);
            if(zcompress((Bytef *)rawdata, nrawdata, (Bytef *)zdata, (uLong * )&nzdata) == 0)
            {
                ACCESS_LOGGER(logger, "compressed %s data %u to %u via urlid:%d", tasks[tid].conns[k].url, LI(nrawdata), LI(nzdata), tasks[tid].conns[k].id);
                p += nzdata;
            }
            else
            {
                FATAL_LOGGER(logger, "compressed %s data %u to %u via urlid:%d failed, %s", tasks[tid].conns[k].url, LI(nrawdata), LI(nzdata), tasks[tid].conns[k].id, strerror(errno));
                rec->err |= ERR_DATA;
            }
        }
        else
        {
            memcpy(p, rawdata, nrawdata);
            p += nrawdata;
        }
        rec->length = p - ps; 
        ACCESS_LOGGER(logger, "url:%s id:%d nlocation:%d length:%d/%d ncookie:%d", tasks[tid].conns[k].url, tasks[tid].conns[k].id, rec->nlocation, rec->length, p - ps, rec->ncookie);
        tasks[tid].p = p;
        memset(&(tasks[tid].conns[k]), 0, sizeof(HTTPTASK));
        if(++(tasks[tid].over) == tasks[tid].total) spider_over_task(tid);
        MUTEX_UNLOCK(tasks[tid].mutex);
        ret = 0;
    }
    return ret;
}

/* decompress data */
int http_decompress(int tid, int k, char *encoding, char *zdata, int nzdata, 
        char *data, int ndata)
{
    int n = 0, ret = -1, nout = ndata;

    if(encoding && zdata && nzdata > 0 && data && ndata > 0)
    {
        if(strcasestr(encoding, "gzip")) 
        {
            if((n = httpgzdecompress((Bytef *)zdata, nzdata, 
                            (Bytef *)data, (uLong *)((void *)&nout))) != 0)
            {
                WARN_LOGGER(logger, "httpgzdecompress() => %d url:%s nzdata:%u left:%u failed, %s", 
                        n, tasks[tid].conns[k].url, LI(nzdata), LI(ndata), strerror(errno));
            }
            else ret = nout;
        }
        else if(strcasestr(encoding, "deflate"))
        {
            if(zdecompress((Bytef *)zdata, nzdata, (Bytef *)data, (uLong *)((void *)&nout)) != 0)
            {
                WARN_LOGGER(logger, "gzdecompress url:%s data nzdata %u left:%u failed, %s", 
                        tasks[tid].conns[k].url, LI(nzdata), LI(ndata), strerror(errno));
            }
            else ret = nout;
        }
        else
        {
            WARN_LOGGER(logger, "unspported url:%s data encoding:%s", encoding, 
                    tasks[tid].conns[k].url);
        }
        ACCESS_LOGGER(logger,"url:%s zdata:%p nzdata:%d data:%p ndata:%d ret:%d", tasks[tid].conns[k].url, zdata, nzdata, data, ndata, ret);
    }
    return ret;
}

/* data charset convert */
int http_convert_charset(char *data, int ndata, char *charset_from, 
        char *charset_to, char *out, int nout)
{
    char *p = NULL, *ps = NULL;
    size_t ninbuf = 0, n =0;
    iconv_t cd = NULL;
    int ret = -1;

    if(data && ndata > 0 && charset_from && charset_to && out && nout > 0) 
    {
        if((cd = iconv_open(charset_to, charset_from)) != (iconv_t)-1)
        {

            p = data;
            ninbuf = ndata;
            n = nout;
            ps = out;
            if(iconv(cd, &p, &ninbuf, &ps, &n) != (size_t)-1)
            {
                ret = nout - n; 
            }
            iconv_close(cd);
        }
        else ret = -2;
    }
    return ret;
}



int http_chardet(HTTP_RESPONSE *http_resp, int tid, int k, char *data, int ndata, char *charset)
{
    char *p = NULL, *s = NULL;
    chardet_t pdet = NULL;
    int ret = -1, n = 0;

    if(data && ndata > 0 && charset && (pdet = qchardet_pop(&qchardet)))
    {
        if(chardet_handle_data(pdet, data, ndata) == 0 
                && chardet_data_end(pdet) == 0 && chardet_get_charset(pdet, 
                    charset, CHARSET_MAX) == CHARDET_RESULT_OK)
        {
            if(strcasestr(charset, "gbk") || strcasestr(charset, "gb2312"))// || charset[0] == '\0')
                strcpy(charset, "gb18030");
            if(charset[0] == '\0' && http_resp 
                    && (n = http_resp->headers[HEAD_ENT_CONTENT_TYPE]) > 0
                    && (p = (http_resp->hlines + n))
                    && (p = strcasestr(p, "charset=")))
            {
                p += 8;
                while(*p == '\'' || *p == '\t' || *p == 0x20)++p;
                s = charset;
                while(*p != '\0' && *p != '\r' && *p != '\n' 
                        && *p != '\'' && *p != '\t' && *p != 0x20) *s++ = *p++;
                *s = '\0';
            }
            if(strcasecmp(charset, http_default_charset) == 0) ret = 0;
        }
        else
        {
            WARN_LOGGER(logger, "chardet url:%s failed, %s", tasks[tid].conns[k].url, strerror(errno));
        }
        qchardet_push(&qchardet, pdet);
    }
    return ret;
}

/* data handler */
int http_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    char charset[CHARSET_MAX], *p = NULL, *encoding = NULL,*s = NULL, *rawdata = NULL, 
         *mm = NULL, *block = NULL, *xmm = NULL, *ctxcharset = NULL;
    int  ret = -1, tid = 0, k = 0, i = 0, n = 0, is_need_compress = 0, 
         is_text = 0, is_chunked = 0;
    size_t  nrawdata = 0;
    HTTP_RESPONSE *http_resp = NULL;
    HTTP_CHUNK http_chunk;

    if(conn)
    {
        tid = conn->session.xids[0];
        k = conn->session.xids[1];
        if(conn == tasks[tid].conns[k].conn && chunk && chunk->data && chunk->ndata > 0
                && cache && cache->ndata > 0 && (http_resp = (HTTP_RESPONSE *)cache->data))
        {
            /* check content is text */
            if(((n = http_resp->headers[HEAD_ENT_CONTENT_TYPE]) > 0
                && (p = (http_resp->hlines + n))  && (strcasestr(p, "text") 
                    || strcasestr(p, "html") || strcasestr(p, "plain"))))
            {
                is_need_compress = 1;
                is_text = 1;
                if((s = strcasestr(p, "charset")))
                {
                    while(*s != '=' && *s != 0)s++;
                    if(*s == '=')
                    {
                        ++s;
                        while(*s == 0x20 || *s == '\t')++s;
                        ctxcharset = s;
                        while(*s != 0 && *s != 0x20 && *s != '\t' && *s != ';')++s;
                        *s = 0;
                    }
                }
            }
            memset(&http_chunk, 0, sizeof(HTTP_CHUNK));
            if((n = http_resp->headers[HEAD_GEN_TRANSFER_ENCODING]) > 0 
                    && (p = (http_resp->hlines + n))  && strcasestr(p, "chunked"))
            {
                if((n = http_chunked_parse(&http_chunk, chunk->data, chunk->ndata)) > 0)
                {
                    is_chunked = 1;
                    s = xmm = (char *)xmm_new(HTTP_BLOCK_MAX); 
                    for(i = 0; i < http_chunk.nchunks; i++)
                    {
                        memcpy(s, chunk->data + http_chunk.chunks[i].off, http_chunk.chunks[i].len);
                        s += http_chunk.chunks[i].len;
                    }
                    rawdata = xmm;
                    nrawdata = s - xmm;
                }
                else goto err_end;
            }
            else
            {
                rawdata = chunk->data;
                nrawdata = chunk->ndata;
            }
            /* check content encoding  */
            if(is_text && (mm = (char *)xmm_new(HTTP_BLOCK_MAX))) 
            {
                if((n = http_resp->headers[HEAD_ENT_CONTENT_ENCODING]) > 0 
                        && (encoding = (http_resp->hlines + n)))
                {
                    if((n = http_decompress(tid, k, encoding, rawdata, 
                                    nrawdata, mm, HTTP_BLOCK_MAX)) <= 0)
                    {
                        WARN_LOGGER(logger, "decomress url:%s rawdata:%p nrawdata:%d failed, %s", tasks[tid].conns[k].url, rawdata, nrawdata, strerror(errno));
                        goto err_end;
                    }
                    rawdata = mm;
                    nrawdata = (size_t)n;
                }
            }
            /* check text/plain/html/xml...  charset  */
            if(is_text && http_chardet(http_resp, tid, k, rawdata, nrawdata, charset) != 0)
            {
                if((block = xmm_new(HTTP_BLOCK_MAX)) == NULL) goto err_end;
                n = http_convert_charset(rawdata, nrawdata, charset,
                        http_default_charset, block, HTTP_BLOCK_MAX);
                if(n < 0 && ctxcharset == NULL)
                {
                    WARN_LOGGER(logger, "convert url:%s charset:%s to %s failed, %s", tasks[tid].conns[k].url, charset, http_default_charset, strerror(errno));
                    goto err_end;
                }
                if(n < 0 && (n = http_convert_charset(rawdata, nrawdata, ctxcharset,
                            http_default_charset, block, HTTP_BLOCK_MAX)) < 0)
                {
                    WARN_LOGGER(logger, "convert url:%s is_chunk:%d rawdata:%p nrawdata:%u charset:%s/%s to %s ret:%d failed, %s", tasks[tid].conns[k].url, is_chunked, rawdata, nrawdata, ctxcharset, charset, http_default_charset, n, strerror(errno));
                    goto err_end;
                }
                rawdata = block;
                nrawdata = n;
            }
            rawdata[nrawdata] = 0;
            ret = http_download_over(tid, k, is_need_compress, http_resp, rawdata, nrawdata);
        }
err_end:
        if(ret == -1) http_download_error(tid, k, ERR_DATA);
        conn->close(conn);
        if(block) xmm_free(block, HTTP_BLOCK_MAX);
        if(mm) xmm_free(mm, HTTP_BLOCK_MAX);
        if(xmm) xmm_free(xmm, HTTP_BLOCK_MAX);
    }
    return ret;
}

/* parse/check http  chunked */
int http_chunk_reader(CONN *conn, CB_DATA *buffer)
{
    HTTP_CHUNK http_chunk;
    int ret = -1;

    if(conn && buffer && buffer->data && buffer->ndata > 0)
    {
        memset(&http_chunk, 0, sizeof(HTTP_CHUNK)); 
        ret = http_chunked_parse(&http_chunk, buffer->data, buffer->ndata);
    }
    return ret;
}

/* chunk handler  */
int http_chunk_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    if(conn && packet && cache && chunk && chunk->ndata) 
        return http_data_handler(conn, packet, cache, chunk);
    return -1;
}

/* http ok handler */
int http_ok_handler(CONN *conn)
{
    char *p = NULL, *s = NULL, *path = "/";
    CB_DATA *chunk = NULL;
    int tid = 0, k = 0;

    if(conn)
    {
        tid = conn->session.xids[0];
        k = conn->session.xids[1];
        if((chunk = conn->newchunk(conn, HTTP_HEADER_MAX)))
        {
            p = chunk->data;
            if(tasks[tid].conns[k].meta.proxy_ip)
            {
                p += sprintf(p, "GET %s HTTP/1.1\r\n%s\r\n", tasks[tid].conns[k].url, 
                        tasks[tid].conns[k].headers);
            }
            else
            {
                s = tasks[tid].conns[k].url;
                while(*s){if(*s == '/' && *(s+1) != '/' && *(s-1) != '/')break;++s;}
                if(*s == '/') path = s;
                p += sprintf(p, "GET %s HTTP/1.1\r\n%s\r\n", path, 
                        tasks[tid].conns[k].headers);
            }
            DEBUG_LOGGER(logger, "urlid:%d %s", tasks[tid].conns[k].id, chunk->data);
            if(conn->send_chunk(conn, chunk, p - chunk->data) != 0) 
            {
                conn->freechunk(conn, chunk);
                return http_download_error(tid, k, ERR_NETWORK);
            }
        }
        return 0;
    }
    return -1;
}


/* packet reader */
int spider_packet_reader(CONN *conn, CB_DATA *buffer)
{
    if(conn) return 0;
    return -1;
}

/* packet handler */
int spider_packet_handler(CONN *conn, CB_DATA *packet)
{
    if(conn && packet)
    {
        return conn->wait_evtimeout(conn, task_wait_time);
    }
    return -1;
}

/* packet quick handler */
int spider_quick_handler(CONN *conn, CB_DATA *packet)
{
    XTHEAD *head = NULL;
    if(conn && (head = (XTHEAD *)(packet->data)))
    {
        return head->length;
    }
    return -1;
}

/* data handler */
int spider_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    char *p = NULL, *end = NULL, ip[HTTP_IP_MAX];
    int n = 0, tid = 0, k = 0, port = 0;
    unsigned char *s = NULL;
    XTMETA *meta = NULL;
    SESSION sess = {0};


    if(conn)
    {
        if((p = chunk->data) && (end = (p + chunk->ndata)) > p 
                && (tid = (conn->session.xids[0] - 1)) >= 0)
        {
            memcpy(&sess, &http_session, sizeof(SESSION));
            memset(&(tasks[tid].conns), 0, sizeof(HTTPTASK) * HTTP_TASK_MAX);
            tasks[tid].total = tasks[tid].over = 0;
            tasks[tid].conn = conn;
            tasks[tid].chunk = conn->newchunk(conn, HTTP_CHUNK_MAX);
            tasks[tid].p = tasks[tid].chunk->data + sizeof(XTHEAD);
            k = 0;
            while(p < end)
            {
                memcpy(&(tasks[tid].conns[k].meta), p, sizeof(XTMETA)); 
                meta = (XTMETA *)p; 
                tasks[tid].conns[k].id = meta->id;
                p += sizeof(XTMETA);
                strncpy(tasks[tid].conns[k].url, p, meta->nurl);
                p += meta->nurl;
                strncpy(tasks[tid].conns[k].headers, p, meta->nheaders);
                p += meta->nheaders;
                if(meta->proxy_ip > 0 && (port = meta->proxy_port) > 0)
                {
                    s = (unsigned char *)&(meta->proxy_ip);
                    n = sprintf(ip, "%d.%d.%d.%d", s[0], s[1], s[2], s[3]);
                }
                else
                {
                    s = (unsigned char *)&(meta->ip);
                    n = sprintf(ip, "%d.%d.%d.%d", s[0], s[1], s[2], s[3]);  
                    port = HTTP_DEFAULT_PORT;
                    if(meta->port > 0) port = meta->port;
                }
                sess.xids[0] = tid;
                sess.xids[1] = k;
                if((tasks[tid].conns[k].conn = httpd->newconn(httpd, 
                                -1, -1, ip, port, &sess)))
                {
                    ++k;
                }
            }
            MUTEX_LOCK(tasks[tid].mutex);
            tasks[tid].total = k;
            if(tasks[tid].over == k) spider_over_task(tid);
            MUTEX_UNLOCK(tasks[tid].mutex);
            return 0;
        }
    }
    return -1;
}

/* over task */
void spider_over_task(int tid)
{
    CB_DATA *chunk = NULL;
    XTHEAD *head = NULL;
    CONN *conn = NULL;
    int n = 0;

    if(tid >= 0 && (conn = tasks[tid].conn) && (chunk = tasks[tid].chunk) 
            && (head = (XTHEAD *)chunk->data))
    {
        //conn->start_cstate(conn);
        n = tasks[tid].p - tasks[tid].chunk->data;
        head->cmd = XT_REQ_DOWNLOAD;
        head->flag |= (http_task_type|XT_TASK_OVER);
        head->length = n - sizeof(XTHEAD);
        tasks[tid].chunk = NULL;
        if(conn->send_chunk(conn, chunk, n) != 0)
        {
            conn->freechunk(conn, chunk);
            conn->over(conn);
            tasks[tid].status = 0;
            iqueue_push(taskqueue, tid);
        }
    }
    return ;
}

/* ok handler */
int spider_ok_handler(CONN *conn)
{
    XTHEAD head = {0};
    if(conn)
    {
       head.cmd = XT_REQ_DOWNLOAD; 
       head.flag = http_task_type; 
       //conn->start_cstate(conn);
        return conn->push_chunk(conn, &head, sizeof(XTHEAD));
    }
    return -1;
}

/* error handler */
int spider_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    int tid = 0;

    if(conn && (tid = (conn->session.xids[0] - 1)) >= 0)
    {
        if(tasks[tid].status) 
        {
            tasks[tid].status = 0;
            iqueue_push(taskqueue, tid);
        }
        return 0;
    }
    return -1;
}

/* timeout handler */
int spider_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    int tid = 0;
    if(conn && (tid = (conn->session.xids[0] - 1)) >= 0)
    {
        conn->over(conn);
        if(tasks[tid].status) 
        {
            tasks[tid].status = 0;
            iqueue_push(taskqueue, tid);
        }
        return 0;
    }
    return -1;
}

/* evtimeout handler */
int spider_evtimeout_handler(CONN *conn)
{
    if(conn)
    {
        return spider_ok_handler(conn);
    }
    return -1;
}

/* OOB handler */
int spider_oob_handler(CONN *conn, CB_DATA *oob)
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
    CONN *conn = NULL;
    SESSION sess = {0};

    if(arg == (void *)spider)
    {
        memcpy(&sess, &(spider->session), sizeof(SESSION));
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
                    sess.xids[0] = id+1;
                    tasks[id].status = 1;
                    if((conn = spider->newconn(spider, -1,-1,
                                    monitord_ip,monitord_port, &sess)) == NULL)
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
    char *s = NULL, *p = NULL, *end = NULL;// *dictfile = NULL, *dictrules = NULL, *host = NULL; 
         //*property_name = NULL, *text_index_name = NULL, *int_index_name = NULL,
         //*long_index_name = NULL, *double_index_name = NULL, *display_name = NULL,
         //*block_name = NULL, *key_name;
    int interval = 0, i = 0, n = 0;// port = 0; //commitid = 0, queueid = 0;

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
    if((httpd = service_init()) == NULL)
    {
        fprintf(stderr, "Initialize service failed, %s", strerror(errno));
        _exit(-1);
    }
    httpd->family = iniparser_getint(dict, "HTTP:inet_family", AF_INET);
    httpd->sock_type = iniparser_getint(dict, "HTTP:socket_type", SOCK_STREAM);
    httpd->ip = iniparser_getstr(dict, "HTTP:service_ip");
    httpd->port = iniparser_getint(dict, "HTTP:service_port", 80);
    httpd->working_mode = iniparser_getint(dict, "HTTP:working_mode", WORKING_PROC);
    httpd->service_type = iniparser_getint(dict, "HTTP:service_type", C_SERVICE);
    httpd->service_name = iniparser_getstr(dict, "HTTP:service_name");
    httpd->nprocthreads = iniparser_getint(dict, "HTTP:nprocthreads", 1);
    httpd->ndaemons = iniparser_getint(dict, "HTTP:ndaemons", 0);
    httpd->niodaemons = iniparser_getint(dict, "HTTP:niodaemons", 1);
    httpd->use_cond_wait = iniparser_getint(dict, "HTTP:use_cond_wait", 0);
    if(iniparser_getint(dict, "HTTP:use_cpu_set", 0) > 0) httpd->flag |= SB_CPU_SET;
    if(iniparser_getint(dict, "HTTP:event_lock", 0) > 0) httpd->flag |= SB_EVENT_LOCK;
    if(iniparser_getint(dict, "HTTP:newconn_delay", 0) > 0) httpd->flag |= SB_NEWCONN_DELAY;
    if(iniparser_getint(dict, "HTTP:tcp_nodelay", 0) > 0) httpd->flag |= SB_TCP_NODELAY;
    if(iniparser_getint(dict, "HTTP:socket_linger", 0) > 0) httpd->flag |= SB_SO_LINGER;
    if(iniparser_getint(dict, "HTTP:while_send", 0) > 0) httpd->flag |= SB_WHILE_SEND;
    if(iniparser_getint(dict, "HTTP:log_thread", 0) > 0) httpd->flag |= SB_LOG_THREAD;
    if(iniparser_getint(dict, "HTTP:use_outdaemon", 0) > 0) httpd->flag |= SB_USE_OUTDAEMON;
    if(iniparser_getint(dict, "HTTP:use_evsig", 0) > 0) httpd->flag |= SB_USE_EVSIG;
    if(iniparser_getint(dict, "HTTP:use_cond", 0) > 0) httpd->flag |= SB_USE_COND;
    if((n = iniparser_getint(dict, "HTTP:sched_realtime", 0)) > 0) httpd->flag |= (n & (SB_SCHED_RR|SB_SCHED_FIFO));
    if((n = iniparser_getint(dict, "HTTP:io_sleep", 0)) > 0) httpd->flag |= ((SB_IO_NANOSLEEP|SB_IO_USLEEP|SB_IO_SELECT) & n);
    httpd->nworking_tosleep = iniparser_getint(dict, "HTTP:nworking_tosleep", SB_NWORKING_TOSLEEP);
    httpd->set_log(httpd, iniparser_getstr(dict, "HTTP:logfile"));
    httpd->set_log_level(httpd, iniparser_getint(dict, "HTTP:log_level", 0));
    httpd->session.packet_type = iniparser_getint(dict, "HTTP:packet_type",PACKET_DELIMITER);
    httpd->session.packet_delimiter = iniparser_getstr(dict, "HTTP:packet_delimiter");
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
    httpd->session.buffer_size = iniparser_getint(dict, "HTTP:buffer_size", SB_BUF_SIZE);
    httpd->session.packet_reader = &http_packet_reader;
    httpd->session.packet_handler = &http_packet_handler;
    httpd->session.chunk_reader = &http_chunk_reader;
    httpd->session.data_handler = &http_data_handler;
    httpd->session.chunk_handler = &http_chunk_handler;
    httpd->session.ok_handler = &http_ok_handler;
    httpd->session.timeout_handler = &http_timeout_handler;
    httpd->session.error_handler = &http_error_handler;
    httpd->session.oob_handler = &http_oob_handler;
    /* http request handler */
    memcpy(&http_session, &(httpd->session), sizeof(SESSION));
    http_session.timeout = http_task_timeout; 
    http_session.packet_reader = &http_packet_reader;
    http_session.packet_handler = &http_packet_handler;
    http_session.data_handler = &http_data_handler;
    http_session.chunk_reader = &http_chunk_reader;
    http_session.chunk_handler = &http_chunk_handler;
    http_session.ok_handler = &http_ok_handler;
    http_session.timeout_handler = &http_timeout_handler;
    http_session.error_handler = &http_error_handler;
    http_session.flags = SB_NONBLOCK;
    /* http mime map */
    if((p = iniparser_getstr(dict, "HTTP:mime")))
    {
        end = p + strlen(p);
        if((mime_map_init(&http_mime_map)) != 0)
        {
            fprintf(stderr, "Initialize http_mime_map failed,%s", strerror(errno));
            _exit(-1);
        }
        mime_add_line(&http_mime_map, p, end);
    }
    /* SPIDER */
    if((spider = service_init()) == NULL)
    {
        fprintf(stderr, "Initialize service failed, %s", strerror(errno));
        _exit(-1);
    }
    spider->family = iniparser_getint(dict, "SPIDER:inet_family", AF_INET);
    spider->sock_type = iniparser_getint(dict, "SPIDER:socket_type", SOCK_STREAM);
    monitord_ip = spider->ip = iniparser_getstr(dict, "SPIDER:service_ip");
    monitord_port = spider->port = iniparser_getint(dict, "SPIDER:service_port", 3082);
    spider->working_mode = iniparser_getint(dict, "SPIDER:working_mode", WORKING_PROC);
    spider->service_type = iniparser_getint(dict, "SPIDER:service_type", C_SERVICE);
    spider->service_name = iniparser_getstr(dict, "SPIDER:service_name");
    spider->nprocthreads = iniparser_getint(dict, "SPIDER:nprocthreads", 1);
    spider->niodaemons = iniparser_getint(dict, "SPIDER:niodaemons", 1);
    spider->use_cond_wait = iniparser_getint(dict, "SPIDER:use_cond_wait", 0);
    if(iniparser_getint(dict, "SPIDER:use_cpu_set", 0) > 0) spider->flag |= SB_CPU_SET;
    if(iniparser_getint(dict, "SPIDER:event_lock", 0) > 0) spider->flag |= SB_EVENT_LOCK;
    if(iniparser_getint(dict, "SPIDER:newconn_delay", 0) > 0) spider->flag |= SB_NEWCONN_DELAY;
    if(iniparser_getint(dict, "SPIDER:tcp_nodelay", 0) > 0) spider->flag |= SB_TCP_NODELAY;
    if(iniparser_getint(dict, "SPIDER:socket_linger", 0) > 0) spider->flag |= SB_SO_LINGER;
    if(iniparser_getint(dict, "SPIDER:while_send", 0) > 0) spider->flag |= SB_WHILE_SEND;
    if(iniparser_getint(dict, "SPIDER:log_thread", 0) > 0) spider->flag |= SB_LOG_THREAD;
    if(iniparser_getint(dict, "SPIDER:use_outdaemon", 0) > 0) spider->flag |= SB_USE_OUTDAEMON;
    if(iniparser_getint(dict, "SPIDER:use_evsig", 0) > 0) spider->flag |= SB_USE_EVSIG;
    if(iniparser_getint(dict, "SPIDER:use_cond", 0) > 0) spider->flag |= SB_USE_COND;
    if((n = iniparser_getint(dict, "SPIDER:sched_realtime", 0)) > 0) spider->flag |= (n & (SB_SCHED_RR|SB_SCHED_FIFO));
    if((n = iniparser_getint(dict, "SPIDER:io_sleep", 0)) > 0) spider->flag |= ((SB_IO_NANOSLEEP|SB_IO_USLEEP|SB_IO_SELECT) & n);
    spider->nworking_tosleep = iniparser_getint(dict, "SPIDER:nworking_tosleep", SB_NWORKING_TOSLEEP);
    spider->set_log(spider, iniparser_getstr(dict, "SPIDER:logfile"));
    spider->set_log_level(spider, iniparser_getint(dict, "SPIDER:log_level", 0));
    spider->session.packet_type = PACKET_CERTAIN_LENGTH;
    spider->session.packet_length = sizeof(XTHEAD);
    spider->session.buffer_size = iniparser_getint(dict, "SPIDER:buffer_size", SB_BUF_SIZE);
    spider->session.packet_reader = &spider_packet_reader;
    spider->session.packet_handler = &spider_packet_handler;
    spider->session.quick_handler = &spider_quick_handler;
    spider->session.ok_handler = &spider_ok_handler;
    spider->session.data_handler = &spider_data_handler;
    spider->session.error_handler = &spider_error_handler;
    spider->session.evtimeout_handler = &spider_evtimeout_handler;
    spider->session.oob_handler = &spider_oob_handler;
    spider->session.flags = SB_NONBLOCK;
    interval = iniparser_getint(dict, "SPIDER:heartbeat_interval", SB_HEARTBEAT_INTERVAL);
    spider->set_heartbeat(spider, interval, &cb_heartbeat_handler, spider);
    LOGGER_INIT(logger, iniparser_getstr(dict, "SPIDER:access_log"));
    LOGGER_SET_LEVEL(logger, iniparser_getint(dict, "SPIDER:access_log_level", 0));
    ntasks = iniparser_getint(dict, "SPIDER:ntasks", 64);
    http_task_type = iniparser_getint(dict, "SPIDER:task_type", 0);
    http_task_timeout = iniparser_getint(dict, "SPIDER:http_task_timeout", 120000000);
    task_wait_time = iniparser_getint(dict, "SPIDER:task_wait_time", 10000000);
    tasks = (WTASK *)xmm_mnew(ntasks * sizeof(WTASK));
    if((taskqueue = iqueue_init()))
    {
        for(i = 0; i < ntasks; i++)
        {
            iqueue_push(taskqueue, i);
            tasks[i].id = i;
            tasks[i].status = 0;
            MUTEX_INIT(tasks[i].mutex);
        }
    }
    qchardet_init(&qchardet);
    return (sbase->add_service(sbase, httpd) | sbase->add_service(sbase, spider));
}

/* stop spider */
static void spider_stop(int sig)
{
    switch (sig) 
    {
        case SIGINT:
        case SIGTERM:
            fprintf(stderr, "spider is interrupted by user.\n");
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
        //fprintf(stdout, "%s::%d ch:%d\r\n", __FILE__, __LINE__, ch);
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
    signal(SIGTERM, &spider_stop);
    signal(SIGINT,  &spider_stop);
    signal(SIGHUP,  &spider_stop);
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
    mime_map_clean(&http_mime_map);
    if(dict)iniparser_free(dict);
    if(tasks)
    {
        for(i = 0; i < ntasks; i++)
        {
            MUTEX_DESTROY(tasks[i].mutex);
        }
        xmm_free(tasks, ntasks * sizeof(WTASK));
    }
    if(taskqueue){iqueue_clean(taskqueue);}
    qchardet_close(&qchardet);
    if(argvmap)mtrie_clean(argvmap);
    return 0;
}
