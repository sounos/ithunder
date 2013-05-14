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
#include "xtask.h"
#include "evdns.h"
#include "mutex.h"
#include "iqueue.h"
#include "stime.h"
#include "base64.h"
#include "base64monitordhtml.h"
#include "timer.h"
#include "logger.h"
#include "xmm.h"
#include "mtree.h"
#ifndef HTTP_BUF_SIZE
#define HTTP_BUF_SIZE           262144
#endif
#ifndef HTTP_QUERY_MAX
#define HTTP_QUERY_MAX          1024
#endif
#define HTTP_LINE_MAX           65536
#define HTTP_CHUNK_SIZE         8388608
#define HTTPD_NUM_PAGE          20
#define HTTPD_NPAGE             20
#define HTTPD_PAGE_MAX          500
static XTASK *xtask = NULL;
static SBASE *sbase = NULL;
static SERVICE *adnsd = NULL;
static SERVICE *spiderd = NULL;
static SERVICE *extractord = NULL;
static SERVICE *httpd = NULL;
static void *taskqueue = NULL;
static int ntasks = 0;
static dictionary *dict = NULL;
static void *logger = NULL;
static void *adnsd_logger = NULL;
static void *http_headers_map = NULL;
static MTREE *ops_disallow_map = NULL;
static char *http_default_charset = "UTF-8";
static char *httpd_home = NULL;
static int is_inside_html = 1;
static unsigned char *httpd_index_html_code = NULL;
static int  nhttpd_index_html_code = 0;
static int adns_conn_timeout = 10000000;
static int adns_trans_timeout = 10000000;
static int adns_timeout_max = 60000000;
static int proxy_timeout = 2000000;
static int http_page_num = XT_PAGE_NUM;
static void *argvmap = NULL;
static char *e_argvs[] =
{
    "op",
#define E_ARGV_OP       0
    "host",
#define E_ARGV_HOST     1
    "url",
#define E_ARGV_URL      2
    "pattern",
#define E_ARGV_PATTERN  3
    "hostid",
#define E_ARGV_HOSTID   4
    "urlid",
#define E_ARGV_URLID    5
    "name",
#define E_ARGV_NAME     6
    "parentid",
#define E_ARGV_PARENTID 7
    "nodeid",
#define E_ARGV_NODEID   8
    "tableid",
#define E_ARGV_TABLEID  9
    "fieldid",
#define E_ARGV_FIELDID  10
    "type",
#define E_ARGV_TYPE     11
    "flag",
#define E_ARGV_FLAG     12
    "templateid",
#define E_ARGV_TEMPLATEID 13
    "map",
#define E_ARGV_MAP      14
    "link",
#define E_ARGV_LINK     15
    "linkmap",
#define E_ARGV_LINKMAP  16
    "urlnodeid",
#define E_ARGV_URLNODEID 17
    "level",
#define E_ARGV_LEVEL     18
    "speed",
#define E_ARGV_SPEED     19
    "page",
#define E_ARGV_PAGE      20
    "recordid",
#define E_ARGV_RECORDID  21
    "userid",
#define E_ARGV_USERID    22
    "user",
#define E_ARGV_USER      23
    "passwd",
#define E_ARGV_PASSWD    24
    "headers"
#define E_ARGV_HEADERS   25
};
#define E_ARGV_NUM       26
static char *e_ops[]=
{
    "host_up",
#define E_OP_HOST_UP        0
    "host_down",
#define E_OP_HOST_DOWN      1
    "node_add",
#define E_OP_NODE_ADD       2
    "node_update",
#define E_OP_NODE_UPDATE    3
    "node_delete",
#define E_OP_NODE_DELETE    4
    "node_childs",
#define E_OP_NODE_CHILDS    5
    "task_stop",
#define E_OP_TASK_STOP      6
    "task_running",
#define E_OP_TASK_RUNNING   7
    "task_view",
#define E_OP_TASK_VIEW      8
    "table_add",
#define E_OP_TABLE_ADD      9
    "table_view",
#define E_OP_TABLE_VIEW     10
    "table_list",
#define E_OP_TABLE_LIST     11
    "table_rename",
#define E_OP_TABLE_RENAME   12
    "table_delete",
#define E_OP_TABLE_DELETE   13
    "field_add",
#define E_OP_FIELD_ADD      14
    "field_rename",
#define E_OP_FIELD_RENAME   15
    "field_delete",
#define E_OP_FIELD_DELETE   16
    "template_add",
#define E_OP_TEMPLATE_ADD   17
    "template_update",
#define E_OP_TEMPLATE_UPDATE    18
    "template_delete",
#define E_OP_TEMPLATE_DELETE    19
    "template_list",
#define E_OP_TEMPLATE_LIST      20
    "database_view",
#define E_OP_DATABASE_VIEW      21
    "urlnode_add",
#define E_OP_URLNODE_ADD        22
    "urlnode_update",
#define E_OP_URLNODE_UPDATE     23
    "urlnode_delete",
#define E_OP_URLNODE_DELETE     24
    "urlnode_childs",
#define E_OP_URLNODE_CHILDS     25
    "urlnode_list",
#define E_OP_URLNODE_LIST       26
    "dns_add",
#define E_OP_DNS_ADD            27
    "dns_delete",
#define E_OP_DNS_DELETE         28
    "dns_list",
#define E_OP_DNS_LIST           29
    "proxy_add",
#define E_OP_PROXY_ADD          30
    "proxy_delete",
#define E_OP_PROXY_DELETE       31
    "proxy_list",
#define E_OP_PROXY_LIST         32
    "speed_limit",
#define E_OP_SPEED_LIMIT        33
    "node_brother",
#define E_OP_NODE_BROTHER       34
    "record_view",
#define E_OP_RECORD_VIEW        35
    "user_add",
#define E_OP_USER_ADD           36
    "user_del",
#define E_OP_USER_DEL           37
    "user_update",
#define E_OP_USER_UPDATE        38
    "user_list",
#define E_OP_USER_LIST          39
    "header_get",
#define E_OP_HEADER_GET         40
    "header_set",
#define E_OP_HEADER_SET         41
    "header_auto"
#define E_OP_HEADER_AUTO        42
};
#define E_OP_NUM                43
int adnsd_ok_handler(CONN *conn);
/* dns packet reader */
int adnsd_packet_reader(CONN *conn, CB_DATA *buffer)
{
    int tid = 0,  i = 0, n = 0, left = 0, ip  = 0;
    unsigned char *p = NULL, *s = NULL;
    EVHOSTENT hostent = {0};

    if(conn && buffer->ndata > 0 && buffer->data)
    {
        tid = conn->session.xids[0];
        s = (unsigned char *)buffer->data;
        left = buffer->ndata;
        do
        {
            if((n = evdns_parse_reply(s, left, &hostent)) > 0)
            {
                s += n;
                left -= n;
                DEBUG_LOGGER(adnsd_logger, "name:%s left:%d naddrs:%d ttl:%d", 
                        hostent.name, left, hostent.naddrs, hostent.ttl);
                if(hostent.naddrs > 0)
                {
                    xtask_set_host_ip(xtask, (char *)hostent.name, 
                            hostent.addrs, hostent.naddrs, hostent.ttl);
                    for(i = 0; i < hostent.nalias; i++)
                    {
                        xtask_set_host_ip(xtask, (char *)hostent.alias[i], 
                                hostent.addrs, hostent.naddrs, hostent.ttl);
                    }
                    ip = hostent.addrs[0];
                    p = (unsigned char *)&ip;
                    DEBUG_LOGGER(adnsd_logger, "Got host[%s]'s ip[%d.%d.%d.%d] ttl:%u from %s:%d", 
                            hostent.name, p[0], p[1], p[2], p[3], hostent.ttl,
                            conn->remote_ip, conn->remote_port);
                }
            }else break;
            memset(&hostent, 0, sizeof(EVHOSTENT));
        }while(left > 0);
        return (buffer->ndata - left);
    }
    return -1;
}

/* dns packet handler */
int adnsd_packet_handler(CONN *conn, CB_DATA *packet)
{
    int tid = 0;

    if(conn)
    {
        tid = conn->session.xids[0];
        if(tid > 0 && packet->ndata > 0 && packet->data)
        {
            return adnsd_ok_handler(conn);
        }
        DEBUG_LOGGER(adnsd_logger, "error_DNS()[%d] remote[%s:%d] local[%s:%d] via %d", tid, conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
        conn->over(conn);
        return xtask_reset_dns(xtask, tid);
    }
    return -1;
}

/* adns error handler */
int adnsd_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    int tid = 0;

    if(conn)
    {
        tid = conn->session.xids[0];
        DEBUG_LOGGER(adnsd_logger, "error_handler()[%d] remote[%s:%d] local[%s:%d] via %d", tid, conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
        if(conn->s_id > 0) xtask_reset_host(xtask, conn->s_id);
        conn->over(conn);
        return xtask_reset_dns(xtask, tid);
    }
    return -1;
}

/* adns timeout handler */
int adnsd_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    int tid = 0;

    if(conn)
    {
        tid = conn->session.xids[0];
        DEBUG_LOGGER(adnsd_logger, "timeout_handler()[%d] remote[%s:%d] local[%s:%d] via %d", tid, conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
        if(conn->s_id > 0) xtask_reset_host(xtask, conn->s_id);
        conn->over_timeout(conn);
        conn->over(conn);
        return xtask_reset_dns(xtask, tid);
    }
    return -1;
}

/* adns timeout handler */
int adnsd_evtimeout_handler(CONN *conn)
{
    if(conn)
    {
        return adnsd_ok_handler(conn);
    }
    return -1;
}

/* ok handler */
int adnsd_ok_handler(CONN *conn)
{
    unsigned char hostname[EVDNS_NAME_MAX], buf[HTTP_BUF_SIZE];
    int qid = 0, n = 0;

    if(conn)
    {
        memset(hostname, 0, EVDNS_NAME_MAX);
        DEBUG_LOGGER(adnsd_logger, "Ready for resolving dns on remote[%s:%d] local[%s:%d]", 
                conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port);
        if((qid = xtask_pop_host(xtask, (char *)hostname)) >= 0)
        {
            conn->set_timeout(conn, EVDNS_TIMEOUT);
            conn->s_id = qid;
            qid %= 65536;
            n = evdns_make_query((char *)hostname, 1, 1, (unsigned short)qid, 1, buf); 
            DEBUG_LOGGER(adnsd_logger, "Resolving %s from nameserver[%s]", 
                    hostname, conn->remote_ip);
            return conn->push_chunk(conn, buf, n);
        }
        else
        {
            conn->s_id = 0;
            if(conn->timeout >= adns_timeout_max) conn->timeout = 0;
            return conn->wait_evtimeout(conn, conn->timeout + adns_trans_timeout);
        }
    }
    return -1;
}

/* heartbeat handler */
void adnsd_heartbeat_handler(void *arg)
{
    char dns_ip[HTTP_IP_MAX];
    CONN *conn = NULL;
    SESSION sess = {0};
    int id = 0;

    if(adnsd == (SERVICE *)arg)
    {
        memcpy(&sess, &(adnsd->session), sizeof(SESSION));
        while((id = sess.xids[0] = xtask_pop_dns(xtask, dns_ip)) > 0 
            && (conn = adnsd->newconn(adnsd, -1, SOCK_DGRAM, dns_ip, DNS_DEFAULT_PORT, &sess)))
        {
            conn->c_id = id;
            conn->set_timeout(conn, adns_conn_timeout);
        }
    }
    return ;
}

/* packet reader */
int http_proxy_packet_reader(CONN *conn, CB_DATA *buffer)
{
    char *p = NULL, *end = NULL;
    int n = -1;

    if(conn && buffer && buffer->ndata > 0 && (p = buffer->data)
        && (end = (buffer->data + buffer->ndata)))
    {
        while(p < end)
        {
            if(p < (end - 3) && *p == '\r' && *(p+1) == '\n' && *(p+2) == '\r' && *(p+3) == '\n')
            {
                n = p + 4 - buffer->data;
                break;
            }
            else ++p;
        }
    }
    return n;
}

/* http proxy packet handler */
int http_proxy_handler(CONN *conn,  HTTP_REQ *http_req);


/* packet handler */
int http_proxy_packet_handler(CONN *conn, CB_DATA *packet)
{
    char *p = NULL, *end = NULL;
    HTTP_RESPONSE http_resp = {0};
    HTTP_REQ *http_req = NULL;
    CONN *parent = NULL;
    int n = 0, len = 0;

    if(conn && packet && packet->ndata > 0 && (p = packet->data) 
            && (end = packet->data + packet->ndata))
    {
        if(http_response_parse(p, end, &http_resp, http_headers_map) == -1) goto err_end;
        conn->save_cache(conn, &http_resp, sizeof(HTTP_RESPONSE));
        if(http_resp.respid == RESP_MOVEDPERMANENTLY 
                &&  (n = http_resp.headers[HEAD_RESP_LOCATION]) > 0
                && (p = http_resp.hlines + n))
        {
            if(conn->session.parent
                && (parent = httpd->findconn(httpd, conn->session.parentid))
                && conn->session.parent == parent
                && (http_req = (HTTP_REQ *)(parent->cache.data))
                && (n = strlen(p)) > 0 && n < HTTP_URL_PATH_MAX)
            {
                //fprintf(stdout, "Redirect %s to %s[%d]\n", http_req->path, p, n);
                memcpy(http_req->path, p, n);
                http_req->path[n] = '\0';
                return http_proxy_handler(parent, http_req); 
            }
            goto err_end;
        }
        if((n = http_resp.headers[HEAD_ENT_CONTENT_TYPE])
            && (p = (http_resp.hlines + n)) && strncasecmp(p, "text", 4) == 0)
        {
            if((n = http_resp.headers[HEAD_ENT_CONTENT_LENGTH]) > 0 
                && (p = (http_resp.hlines + n)))
            {
                len = atoi(p);
            }
            else
            {
                len = 1024 * 1024 * 16;
            }
            return conn->recv_chunk(conn, len);
        }
        else
        {
            if(conn->session.parent
                    && (parent = httpd->findconn(httpd, conn->session.parentid))
                    && conn->session.parent == parent)
            {
                conn->session.packet_type = PACKET_PROXY;
                parent->push_chunk(parent, packet->data, packet->ndata);
                return 0;
            }
        }
err_end: 
        if(conn)conn->over(conn);
    }
    return -1;
}

/* data handler */
int http_proxy_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    char buf[HTTP_BUF_SIZE], *content_type = NULL, *content_encoding = NULL, 
         *p = NULL, *out = NULL, *s = NULL;
    int n = 0, i = 0, nout = 0, is_need_compress = 0;
    HTTP_RESPONSE *http_resp = NULL;
    CONN *parent = NULL;

    if(conn && packet && packet->ndata > 0 && packet->data 
            && cache && cache->ndata > 0 && (http_resp = (HTTP_RESPONSE *)cache->data) 
            && chunk && chunk->ndata && chunk->data)
    {
        if((n = http_resp->headers[HEAD_ENT_CONTENT_ENCODING]))
            content_encoding = http_resp->hlines + n;
        else 
            content_encoding = "";
        if((n = http_resp->headers[HEAD_ENT_CONTENT_TYPE]) > 0 
                && (content_type = (http_resp->hlines + n))
                && (nout = http_charset_convert(content_type, content_encoding, 
                chunk->data, chunk->ndata, http_default_charset, is_need_compress, &out)) > 0)
        {
            p = buf;
            p += sprintf(p, "%s\r\n", http_resp->hlines);
            for(i = 0; i < HTTP_HEADER_NUM; i++)
            {
               if(HEAD_ENT_CONTENT_ENCODING == i
               || HEAD_ENT_CONTENT_LENGTH == i
               || HEAD_ENT_CONTENT_TYPE == i
               || HEAD_GEN_CONNECTION == i
               || HEAD_RESP_SET_COOKIE == i)
               {
                    continue;
               }
               else if((n = http_resp->headers[i]) > 0 && (s = (http_resp->hlines + n)))
               {
                    p += sprintf(p, "%s %s\r\n", http_headers[i].e, s);
               }
            }
            /*
            memcpy(p, packet->data, packet->ndata - 2);
            p += packet->ndata - 2;
            */
            if(is_need_compress)
                p += sprintf(p, "%s deflate\r\n", http_headers[HEAD_ENT_CONTENT_ENCODING].e);
            p += sprintf(p, "%s text/html; charset=%s\r\n", http_headers[HEAD_ENT_CONTENT_TYPE].e, 
                        http_default_charset);
            p += sprintf(p, "%s %d\r\n", http_headers[HEAD_ENT_CONTENT_LENGTH].e, nout);
            p += sprintf(p, "Connection: close\r\n");
            p += sprintf(p, "\r\n");
            //conn->push_exchange(conn, buf, (p - buf));
            //conn->push_exchange(conn, out, nout);
            if(conn->session.parent
                    && (parent = httpd->findconn(httpd, conn->session.parentid))
                    && conn->session.parent == parent)
            {
                //fprintf(stdout, "%s::%d out-length:%d %s", __FILE__, __LINE__, nout, buf);
                parent->push_chunk(parent, buf, (p - buf));
                parent->push_chunk(parent, out, nout);
            }
            if(out) http_charset_convert_free(out);
            return 0;
        }
        else
        {
            if(conn->session.parent
                    && (parent = httpd->findconn(httpd, conn->session.parentid))
                    && conn->session.parent == parent)
            {
                parent->push_chunk(parent, packet->data, packet->ndata);
                parent->push_chunk(parent, chunk->data, chunk->ndata);
            }
            return 0;
        }
    }
    if(conn) conn->over(conn);
    return -1;
}

/* error handler */
int http_proxy_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    if(conn)
    {
        return http_proxy_data_handler(conn, packet, cache, chunk);
    }
    return -1;
}

/* timeout handler */
int http_proxy_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    if(conn)
    {
        return http_proxy_data_handler(conn, packet, cache, chunk);
    }
    return -1;
}

/* bind proxy  */
int http_bind_proxy(CONN *conn, char *host, int port) 
{
    char *ip = NULL, cip[HTTP_IP_MAX];
    unsigned char *sip = NULL;
    struct hostent *hp = NULL;
    SESSION session = {0};
    CONN *new_conn = NULL;
    int bitip = 0;

    if(conn && host && port > 0)
    {
        DEBUG_LOGGER(adnsd_logger, "get_host_ip(%s)", host);
        if((bitip = xtask_get_host_ip(xtask, host)) > 0) 
        {
            sip = (unsigned char *)&bitip;
            ip = cip;
            sprintf(ip, "%d.%d.%d.%d", sip[0], sip[1], sip[2], sip[3]);
            //fprintf(stdout, "%s::%d %s %s:%d\r\n", __FILE__, __LINE__, host, ip, port);
        }
        else
        {
            if((hp = gethostbyname(host))
                && sprintf(cip, "%s", inet_ntoa(*((struct in_addr *)(hp->h_addr))))> 0)
            {
                ip = cip;
                DEBUG_LOGGER(adnsd_logger, "gethostbyname(%s):%s", host, cip);
                //bitip = inet_addr(ip);
                //xtask_set_host_ip(xtask, host, &bitip, 1);
            }
            //fprintf(stdout, "%s::%d %s:%d\r\n", __FILE__, __LINE__, ip, port);
        }
        if(ip)
        {
            session.packet_type = PACKET_PROXY;
            session.timeout = proxy_timeout;
#ifdef  _HTTP_CHARSET_CONVERT
            session.packet_type |= PACKET_CUSTOMIZED;
            session.packet_reader = &http_proxy_packet_reader;
            session.packet_handler = &http_proxy_packet_handler;
            session.data_handler = &http_proxy_data_handler;
            session.timeout_handler = &http_proxy_timeout_handler;
            session.error_handler = &http_proxy_error_handler;
#endif
            //fprintf(stdout, "%s::%d host:%s %s:%d\r\n", __FILE__, __LINE__, host, ip, port);
            if((new_conn = httpd->newproxy(httpd, conn, -1, -1, ip, port, &session)))
            {
                new_conn->start_cstate(new_conn);
                return 0;
            }
        }
    }
    return -1;
}

 
/* http proxy packet handler */
int http_proxy_handler(CONN *conn,  HTTP_REQ *http_req)
{
    char buf[HTTP_BUF_SIZE], *host = NULL, *path = NULL, 
         *s = NULL, *p = NULL;
    int n = 0, i = 0, port = 80;

    if(conn && http_req)
    {
        p = http_req->path;
        if(strncasecmp(p, "http://", 7) == 0)
        {
            p += 7;
            host = p;
            while(*p != '\0' && *p != ':' && *p != '/') ++p;
            if(*p == ':')
            {
                *p++ = '\0'; 
                port = atoi(p);
                while(*p >= '0' && *p <= '9') ++p;
                path = p;
            }
            else if(*p == '/') {*p++ = '\0'; path = p;}
            else if(*p == '\0') path = "";
            else path = p;
        }
        else
        {
            if((n = http_req->headers[HEAD_REQ_HOST]) > 0 )
            {
                path = p;
                host = (http_req->hlines + n);
            }
            else goto err_end;
        }
        if(path && *path == '/') ++path;
        if(path == NULL) path = "";
        //authorized 
        if(http_req->reqid == HTTP_GET)
        {
            p = buf;
            p += sprintf(p, "GET /%s HTTP/1.0\r\n", path);
            if(host) p += sprintf(p, "Host: %s\r\n", host);
            for(i = 0; i < HTTP_HEADER_NUM; i++)
            {
                if(HEAD_REQ_HOST == i && host) continue;
                if(HEAD_REQ_REFERER == i || HEAD_REQ_COOKIE == i) continue;
                if((n = http_req->headers[i]) > 0 && (s = (http_req->hlines + n)))
                {
                    p += sprintf(p, "%s %s\r\n", http_headers[i].e, s);
                }
            }
            p += sprintf(p, "%s", "\r\n");
            //fprintf(stdout, "%s", buf);
            conn->push_exchange(conn, buf, (p - buf));
        }
        else if(http_req->reqid == HTTP_POST)
        {
            p = buf;
            p += sprintf(p, "POST /%s HTTP/1.0\r\n", path);
            if(host) p += sprintf(p, "Host: %s\r\n", host);
            for(i = 0; i < HTTP_HEADER_NUM; i++)
            {
                if(HEAD_REQ_HOST == i && host) continue;
                if(HEAD_REQ_REFERER == i || HEAD_REQ_COOKIE == i) continue;
                if((n = http_req->headers[i]) > 0 && (s = http_req->hlines + n))
                {
                    p += sprintf(p, "%s %s\r\n", http_headers[i].e, s);
                }
            }
            p += sprintf(p, "%s", "\r\n");
            //fprintf(stdout, "%s", buf);
            conn->push_exchange(conn, buf, (p - buf));
            if((n = http_req->headers[HEAD_ENT_CONTENT_LENGTH]) > 0 
                    && (n = atol(http_req->hlines + n)) > 0)
            {
                conn->recv_chunk(conn, n);
            }
        }
        else goto err_end;
        if(http_bind_proxy(conn, host, port) == -1) goto err_end;
        return 0;
    }
err_end:
    conn->push_chunk(conn, HTTP_BAD_REQUEST, strlen(HTTP_BAD_REQUEST));
    return -1;
}

/* data handler */
int monitord_packet_reader(CONN *conn, CB_DATA *buffer);
int monitord_packet_handler(CONN *conn, CB_DATA *packet);
int monitord_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int monitord_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int monitord_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int monitord_ok_handler(CONN *conn);
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
        //fprintf(stdout, "%s::%d %s\r\n", __FILE__, __LINE__, p);
        if(http_request_parse(p, end, &http_req, http_headers_map) == -1) goto err_end;
        if(strncasecmp(http_req.path, "/proxy/", 7) == 0)
        {
            strcpy(http_req.path, http_req.path + 7);
            conn->save_cache(conn, &http_req, sizeof(HTTP_REQ));
            return http_proxy_handler(conn, &http_req);
        }
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

/* send chunk */
int httpd_send_chunk(CONN *conn, CB_DATA *chunk, int len)
{
    char buf[HTTP_LINE_MAX], *p = NULL;

    if(conn && chunk && len > 0)
    {
        p = buf;
        p += sprintf(p, "HTTP/1.0 200 OK\r\nContent-Type: text/html;charset=%s\r\n"
                "Content-Length: %d\r\n", http_default_charset, len);
        p += sprintf(p, "Connection:Keep-Alive\r\n");
        p += sprintf(p, "\r\n");
        conn->push_chunk(conn, buf, p - buf);
        if(conn->send_chunk(conn, chunk, len) != 0)
            conn->freechunk(conn, chunk);
        return 0;
    }
    return 0;
}

/* request handler */
int httpd_request_handler(CONN *conn, HTTP_REQ *http_req)
{
    int ret = -1, i = 0, id = 0, n = 0, op = -1, nodeid = -1, x = -1, fieldid = -1, parentid = -1,
        urlid = -1, hostid = -1, tableid = -1, type = -1,  flag = 0, templateid = -1,
        urlnodeid = -1, recordid = -1, level = -1, count = 0, page = 1, from = 0,
        total = 0, is_purl = 0, ppid = 0, userid = -1, len = 0, to = 0;
    char buf[HTTP_LINE_MAX], *p = NULL, *end = NULL, *name = NULL, *host = NULL, 
         *url = NULL, *link = NULL, *pattern = NULL, *map = NULL, *linkmap = NULL, 
         *pp = NULL, *user = NULL, *passwd = NULL, format[URL_LEN_MAX], *headers = NULL,
         block[HTTP_BUF_SIZE];
    XTNODE *nodes = NULL, node = {0};
    ITEMPLATE template = {0};
    //XTURLNODE *urlnodes = NULL;
    PURL purl = {0};
    double speed = 0.0;
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
                        case E_ARGV_OP:
                            op = mtrie_get(argvmap, p, http_req->argvs[i].nv) - 1;
                            break;
                        case E_ARGV_NAME :
                            name = p;
                            break;
                        case E_ARGV_NODEID :
                            nodeid = atoi(p);
                            break;
                        case E_ARGV_PARENTID:
                            parentid = atoi(p);
                            break;
                        case E_ARGV_PATTERN:
                            pattern = p;
                            break;
                        case E_ARGV_HOST :
                            host = p;
                            break;
                        case E_ARGV_HOSTID :
                            hostid = atoi(p);
                            break;
                        case  E_ARGV_URL :
                            url = p;
                            break;
                        case  E_ARGV_URLID :
                            urlid = atoi(p);
                            break;
                        case E_ARGV_TABLEID:
                            tableid = atoi(p);
                            break;
                        case E_ARGV_FIELDID:
                            fieldid = atoi(p);
                            break;
                        case E_ARGV_TYPE:
                            type = atoi(p);
                            break;
                        case E_ARGV_FLAG:
                            flag = atoi(p);
                            break;
                        case E_ARGV_TEMPLATEID:
                            templateid = atoi(p);
                            break;
                        case E_ARGV_MAP:
                            map = p;
                            break;
                        case E_ARGV_LINK:
                            link = p;
                            break;
                        case E_ARGV_LINKMAP:
                            linkmap = p;
                            break;
                        case E_ARGV_URLNODEID:
                            urlnodeid = atoi(p);
                            break;
                        case E_ARGV_RECORDID:
                            recordid = atoi(p);
                            break;
                        case E_ARGV_LEVEL:
                            level = atoi(p);
                            break;
                        case E_ARGV_SPEED:
                            speed = atof(p);
                            break;
                        case E_ARGV_PAGE:
                            page = atoi(p);
                            break;
                        case E_ARGV_HEADERS:
                            headers = p;
                            break;
                        default:
                            break;
                    }
                }
            }
        }
        if(mtree_get(ops_disallow_map, op, NULL) == 0) goto err_end;
        if(map)
        {
            p = map;
            while(*p != '{' && *p != '\0')++p;
            if(*p != '{') goto err_end;
            ++p;
            while(*p != '\0' && *p != '[')++p;
            if(*p != '[') goto err_end;
            for(i = 0; i < XT_FIELDS_MAX; i++)
            {
                if(*p == '[') ++p;
                else goto err_end;
                template.map[i].fieldid = atoi(p);
                while(*p != '\0' && ((*p >= '0' && *p <= '9') || *p == '-'))++p;
                while(*p != '\0' && *p != ',')++p;
                if(*p != ',') goto err_end;
                ++p;
                template.map[i].nodeid = atoi(p);
                while(*p != '\0' && ((*p >= '0' && *p <= '9') || *p == '-'))++p;
                while(*p != '\0' && *p != ',')++p;
                if(*p != ',') goto err_end;
                ++p;
                template.map[i].flag = atoi(p);
                while(*p != '\0' && ((*p >= '0' && *p <= '9') || *p == '-'))++p;
                while(*p != '\0' && *p != ']')++p;
                ++p;
                while(*p != '\0' && *p != ';')++p;
                if(*p != ';') break;
                ++p;
                while(*p != '\0' && *p != '[')++p;
                if(*p != '[') break;
            }
            template.nfields = ++i;
        }
        if(link && (x = strlen(link)) && linkmap)
        {
            memcpy(template.link, link, x);
            p = linkmap;
            while(*p != '\0' && *p != '[')++p;
            if(*p != '[') goto err_end;
            ++p;
            template.linkmap.fieldid = atoi(p);
            while(*p != '\0' && ((*p >= '0' && *p <= '9') || *p == '-'))++p;
            while(*p != '\0' && *p != ',')++p;
            if(*p != ',') goto err_end;
            ++p;
            template.linkmap.nodeid = atoi(p);
            while(*p != '\0' && ((*p >= '0' && *p <= '9') || *p == '-'))++p;
            while(*p != '\0' && *p != ',')++p;
            if(*p != ',') goto err_end;
            ++p;
            template.linkmap.flag = atoi(p);
            template.flags |= TMP_IS_LINK; 
        }
        if(op == E_OP_URLNODE_ADD && url)
        {
            is_purl = 0;
            p = url;
            while(*p != '\0')
            {
                if(*p == '[')
                {
                    memset(&purl, 0, sizeof(PURL));
                    purl.sfrom = p++;
                    if(*p >= '0' && *p <= '9')
                    {
                        purl.from = atoi(p++);
                        purl.type = PURL_TYPE_INT; 
                        while((*p >= '0' && *p <= '9') || *p == '-')
                        {
                            if(*p == '-') purl.to = atoi(++p);
                            else ++p;
                        }
                        if(*p == ']')
                        {
                            is_purl = 1;
                            purl.sto = ++p;
                            if(*p++ == '{')
                            {
                                if(*p > '0' && *p <= '9' && *(p+1) == '}')
                                {
                                    purl.length = atoi(p++);
                                    purl.sto = ++p;
                                }
                            }
                            break;
                        }
                    }
                    else if((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z'))
                    {
                        purl.from = (int)*p++;
                        purl.type = PURL_TYPE_CHAR; 
                        if(*p != '-')continue;
                        ++p;
                        if((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z'))
                        {
                            purl.to = (int)*p++;
                            if(*p != ']') continue;
                            else 
                            {
                                purl.sto = ++p;
                                is_purl = 1;
                                break;
                            }
                        }
                    }
                    else ++p;
                }
                else ++p;
            }
        }
        if(page <= 0) page = 1;
        from = (page - 1) * http_page_num;
        to = from + http_page_num;
        switch(op)
        {
            case E_OP_NODE_ADD :
                if(parentid >= 0 && name
                        && (id = xtask_add_node(xtask, parentid, name)) > 0)
                {
                    goto childs_view;
                }
                else goto err_end;
                break;
            case E_OP_NODE_UPDATE :
                if(parentid >= 0 && nodeid > 0 && name
                        && (id = xtask_update_node(xtask, parentid, nodeid, name)) > 0)
                {
                    goto node_op_ok;
                }else goto err_end;
                break;
            case E_OP_NODE_DELETE :
                if(parentid >= 0 && nodeid > 0
                        && (id = xtask_delete_node(xtask, nodeid)) > 0)
                {
                    goto node_op_ok;
                }else goto err_end;
                break;
            case E_OP_NODE_CHILDS :
                if(nodeid >= 0)
                {
                    parentid = nodeid;
                    goto childs_view;
                }else goto err_end;
                break;
            case E_OP_NODE_BROTHER :
                if(nodeid >= 0 && xtask_get_node(xtask, nodeid, &node) > 0)
                {
                    parentid = node.parent;
                    goto childs_view;
                }else goto err_end;
                break;
            case E_OP_TASK_VIEW:
                goto state_view;
                break;
                /*
                   case E_OP_TASK_STOP:
                   if(xtask_set_state_running(xtask, 0) == 0
                   && (n = xtask_get_stateinfo(xtask, block)) > 0)
                   {
                   conn->push_chunk(conn, block, n);
                   goto end;
                   }else goto err_end;
                   break;
                   case E_OP_TASK_RUNNING:
                   if(xtask_set_state_running(xtask, 1) == 0
                   && (n = xtask_get_stateinfo(xtask, block)) > 0)
                   {
                   conn->push_chunk(conn, block, n);
                   goto end;
                   }else goto err_end;
                   break;
                   */
            case E_OP_TABLE_ADD:
                if(name && (id = xtask_add_table(xtask, name)) > 0)
                {
                    goto table_list;
                }else goto err_end;
                break;
            case E_OP_TABLE_RENAME:
                if(tableid > 0 && name 
                        && (id = xtask_rename_table(xtask, tableid, name)) > 0)
                {
                    goto table_list;
                }else goto err_end;
                break;
            case E_OP_TABLE_DELETE:
                if(tableid > 0 && (id = xtask_delete_table(xtask, tableid)) > 0)
                {
                    goto table_list;
                }else goto err_end;
                break;
            case E_OP_TABLE_LIST:
                goto table_list;
                break;
            case E_OP_TABLE_VIEW:
                if((id = tableid) > 0)
                {
                    goto table_view;
                }else goto err_end;
                break;
            case E_OP_DATABASE_VIEW:
                goto database_view;
                break;
            case E_OP_FIELD_ADD:
                if(tableid > 0 && name && flag > 0 && (id = xtask_add_field(xtask, 
                                tableid, flag, name)) >= 0)
                {
                    id = tableid;
                    goto table_view;
                }else goto err_end;
                break;
            case E_OP_FIELD_RENAME:
                if(tableid > 0 && fieldid > 0 && name && (id = xtask_rename_field(xtask, 
                                tableid, fieldid, name)) >= 0)
                {
                    id = tableid;
                    goto table_view;
                }else goto err_end;
                break;
            case E_OP_FIELD_DELETE:
                if(tableid >= 0 && fieldid >= 0
                        && (id = xtask_delete_field(xtask, tableid, fieldid)) > 0)
                {
                    id = tableid;
                    goto table_view;
                }else goto err_end;
                break;
            case E_OP_TEMPLATE_ADD:
                template.tableid = tableid;
                if(nodeid > 0 && pattern && (map||linkmap) && url
                        && (template.flags = flag) >= 0
                        && (n = strlen(pattern)) > 0 && n < PATTERN_LEN_MAX
                        && (memcpy(template.pattern, pattern, n))
                        && (x = strlen(url)) > 0 && x < XT_URL_SIZE
                        && (memcpy(template.url, url, x))
                        && xtask_add_template(xtask, nodeid, &template) > 0)
                {
                    goto template_view;
                }else goto err_end;
                break;
            case E_OP_TEMPLATE_UPDATE:
                template.tableid = tableid;
                if(nodeid > 0 && templateid > 0 && pattern && (map||linkmap) && url
                        && (template.flags = flag) >= 0
                        && (n = strlen(pattern)) > 0 && n < PATTERN_LEN_MAX
                        && (memcpy(template.pattern, pattern, n))
                        && (x = strlen(url)) > 0 && x < XT_URL_SIZE
                        && (memcpy(template.url, url, x))
                        && xtask_update_template(xtask, templateid, &template) > 0)
                {
                    goto template_view;
                }else goto err_end;
                break;
            case E_OP_TEMPLATE_DELETE:
                if(nodeid > 0 && templateid > 0 
                        && xtask_delete_template(xtask, nodeid, templateid) > 0)
                {
                    goto template_view;
                }else goto err_end;
                break;
            case E_OP_TEMPLATE_LIST:
                if(nodeid > 0)
                {
                    goto template_view;
                }else goto err_end;
                break;
            case E_OP_URLNODE_ADD:
                if(level > 0) flag = REG_IS_LIST;
                if(parentid < 0) parentid = 0;
                if(is_purl && (n = (purl.sfrom - url)) > 0)
                {
                    memset(buf, 0, HTTP_BUF_SIZE);
                    memcpy(buf, url, n);
                    if(purl.type == PURL_TYPE_INT)
                    {
                        for(i = purl.from; i <= purl.to; i++)
                        {
                            p = buf + n;
                            if(purl.length > 1)
                            {
                                sprintf(format, "%%0%dd", purl.length);
                                p += sprintf(p, format, i);
                            }
                            else
                            {
                                p += sprintf(p, "%d", i);
                            }
                            p += sprintf(p, "%s", purl.sto);
                            urlid = xtask_add_url(xtask, parentid, nodeid, buf, flag);
                        }
                    }
                    else if(purl.type == PURL_TYPE_CHAR)
                    {
                        for(i = purl.from; i <= purl.to; i++)
                        {
                            p = buf + n;
                            p += sprintf(p, "%c", (char )i);
                            p += sprintf(p, "%s", purl.sto);
                            urlid = xtask_add_url(xtask, parentid, nodeid, buf, flag);
                        }
                    }
                }
                else
                {
                    urlid = xtask_add_url(xtask, parentid, nodeid, url, flag);
                }
                if(parentid > 0)
                    goto urlnodes_list;
                else
                    goto urlchilds_list;
                break;
            case E_OP_URLNODE_UPDATE:
                goto err_end;
                break;
            case E_OP_URLNODE_DELETE:
                if(urlnodeid > 0 && (parentid = xtask_delete_urlnode(xtask, urlnodeid)) > 0)
                {
                    goto urlnodes_list; 
                }else goto err_end;
                break;
            case E_OP_URLNODE_CHILDS:
                if(urlnodeid > 0)
                {
                    parentid = urlnodeid;
                    goto urlnodes_list;
                }else goto err_end;
                break;
            case E_OP_URLNODE_LIST:
                if(nodeid >= 0)
                {
                    goto urlchilds_list;
                }else goto err_end;
                break;
            case E_OP_RECORD_VIEW:
                if(urlnodeid > 0) goto record_view;
                else goto err_end;
                break;
            case E_OP_DNS_ADD:
                if(host && xtask_add_dns(xtask, host) > 0) 
                {
                    goto dns_list;
                }else goto err_end;
                break;
            case E_OP_DNS_DELETE:
                if(hostid > 0 && xtask_del_dns(xtask, hostid, NULL) > 0)
                {
                    goto dns_list;
                }else goto err_end;
                break;
            case E_OP_DNS_LIST:
                goto dns_list;
                break;
            case E_OP_PROXY_ADD:
                if(host && xtask_add_proxy(xtask, host) > 0)
                {
                    goto proxy_list;
                }else goto err_end;
                break;
            case E_OP_PROXY_DELETE:
                if(hostid > 0 && xtask_del_proxy(xtask, hostid, NULL) > 0) 
                {
                    goto proxy_list;
                }else goto err_end;
                break;
            case E_OP_PROXY_LIST:
                goto proxy_list;
                break;
            case E_OP_HEADER_GET:
                goto headers_list;
                break;
            case E_OP_HEADER_AUTO:
                if(xtask_auto_headers(xtask, http_req) == 0)
                {
                    goto headers_list;
                }
                else goto err_end;
                break;
            case E_OP_HEADER_SET:
                if(headers && xtask_set_headers(xtask, headers) == 0)
                {
                    goto headers_list;
                }
                else goto err_end;
                break;
                /*
                   case E_OP_SPEED_LIMIT:
                   xtask_set_speed_limit(xtask, speed);
                   if((n = xtask_get_stateinfo(xtask, block)) > 0)
                   {
                   conn->push_chunk(conn, block, n);
                   goto end;
                   }else goto err_end;
                   break;
                   case E_OP_USER_ADD:
                   if((xtask_add_user(xtask, user, passwd)) >= 0)
                   {
                   if((n = xtask_list_users(xtask, block)) > 0)
                   {
                   conn->push_chunk(conn, block, n);
                   goto end;
                   }
                   else goto err_end;
                   }else goto err_end;
                   break;
                   case E_OP_USER_DEL:
                   if((userid >= 0 || user) && (xtask_del_user(xtask, userid, user)) >= 0)
                   {
                   if((n = xtask_list_users(xtask, block)) > 0)
                   {
                   conn->push_chunk(conn, block, n);
                   goto end;
                   }
                   else goto err_end;
                   }else goto err_end;
                   break;
                   case E_OP_USER_UPDATE:
                   if(passwd && (userid >= 0 || user) 
                   && (xtask_update_passwd(xtask, userid, user, passwd)) >= 0)
                   {
                   if((n = xtask_list_users(xtask, block)) > 0)
                   {
                   conn->push_chunk(conn, block, n);
                   goto end;
                   }
                   else goto err_end;
                   }else goto err_end;
                   break;
                   case E_OP_USER_LIST:
                   if((n = xtask_list_users(xtask, block)) > 0)
                   {
                   conn->push_chunk(conn, block, n);
                   goto end;
                   }
                   else goto err_end;
                   break;
                   */
            default:
                goto err_end;
                break;
        }
        return 0;
childs_view:
        if((chunk = conn->newchunk(conn, HTTP_CHUNK_SIZE)))
        {
            if((len = xtask_view_node_childs(xtask, parentid, chunk->data, HTTP_CHUNK_SIZE)) > 0)
            {
                return httpd_send_chunk(conn, chunk, len);
            }
            else 
            {
                conn->freechunk(conn, chunk);
                conn->over(conn);
            }
        }
        return 0;
template_view:
        if((chunk = conn->newchunk(conn, HTTP_CHUNK_SIZE)))
        {
            if((len = xtask_view_templates(xtask, nodeid, chunk->data, HTTP_CHUNK_SIZE)) > 0)
            {
                return httpd_send_chunk(conn, chunk, len);
            }
            else 
            {
                conn->freechunk(conn, chunk);
                conn->over(conn);
            }
        }
        return 0;
node_op_ok:
        n = sprintf(buf, "%d\r\n", id);
        n = sprintf(buf, "HTTP/1.0 200\r\nContent-Type:text/html;charset=%s\r\n"
                "Content-Length:%d\r\nConnection:Keep-Alive\r\n\r\n%d\r\n", 
                http_default_charset, n, id);
        return conn->push_chunk(conn, buf, n);
headers_list:
        if((chunk = conn->newchunk(conn, HTTP_CHUNK_SIZE)))
        {
            if((len = xtask_get_headers(xtask, chunk->data)) > 0)
            {
                return httpd_send_chunk(conn, chunk, len);
            }
            else 
            {
                conn->freechunk(conn, chunk);
                conn->over(conn);
            }
        }
        return 0;

table_view:
        if((chunk = conn->newchunk(conn, HTTP_CHUNK_SIZE)))
        {
            if((len = xtask_view_table(xtask, id, chunk->data, HTTP_CHUNK_SIZE)) > 0)
            {
                return httpd_send_chunk(conn, chunk, len);
            }
            else 
            {
                conn->freechunk(conn, chunk);
                conn->over(conn);
            }
        }
        return 0;
table_list:
        if((chunk = conn->newchunk(conn, HTTP_CHUNK_SIZE)))
        {
            if((len = xtask_list_tables(xtask, chunk->data, HTTP_CHUNK_SIZE)) > 0)
            {
                return httpd_send_chunk(conn, chunk, len);
            }
            else 
            {
                conn->freechunk(conn, chunk);
                conn->over(conn);
            }
        }
        return 0;
database_view:
        if((chunk = conn->newchunk(conn, HTTP_CHUNK_SIZE)))
        {
            if((len = xtask_view_database(xtask, chunk->data, HTTP_CHUNK_SIZE)) > 0)
            {
                return httpd_send_chunk(conn, chunk, len);
            }
            else 
            {
                conn->freechunk(conn, chunk);
                conn->over(conn);
            }
        }
        return 0;
dns_list:
        if((chunk = conn->newchunk(conn, HTTP_CHUNK_SIZE)))
        {
            if((len = xtask_view_dnslist(xtask, chunk->data, HTTP_CHUNK_SIZE)) > 0)
            {
                return httpd_send_chunk(conn, chunk, len);
            }
            else 
            {
                conn->freechunk(conn, chunk);
                conn->over(conn);
            }
        }
        return 0;
proxy_list:
        if((chunk = conn->newchunk(conn, HTTP_CHUNK_SIZE)))
        {
            if((len = xtask_view_proxylist(xtask, chunk->data, HTTP_CHUNK_SIZE)) > 0)
            {
                return httpd_send_chunk(conn, chunk, len);
            }
            else 
            {
                conn->freechunk(conn, chunk);
                conn->over(conn);
            }
        }
        return 0;
urlnodes_list:
        if((chunk = conn->newchunk(conn, HTTP_CHUNK_SIZE)))
        {
            if((len = xtask_view_urlnodes(xtask, parentid, chunk->data, HTTP_CHUNK_SIZE, from, to)) > 0)
            {
                return httpd_send_chunk(conn, chunk, len);
            }
            else 
            {
                conn->freechunk(conn, chunk);
                conn->over(conn);
            }
        }
        return 0;
urlchilds_list:
        if((chunk = conn->newchunk(conn, HTTP_CHUNK_SIZE)))
        {
            if((len = xtask_view_urlchilds(xtask, nodeid, chunk->data, HTTP_CHUNK_SIZE, from, to)) > 0)
            {
                return httpd_send_chunk(conn, chunk, len);
            }
            else 
            {
                conn->freechunk(conn, chunk);
                conn->over(conn);
            }
        }
        return 0;
record_view:
        if((chunk = conn->newchunk(conn, HTTP_CHUNK_SIZE)))
        {
            if((len = xtask_view_record(xtask, urlnodeid, chunk->data, HTTP_CHUNK_SIZE)) > 0)
            {
                return httpd_send_chunk(conn, chunk, len);
            }
            else 
            {
                conn->freechunk(conn, chunk);
                conn->over(conn);
            }
        }
        return 0;
state_view:
        if((chunk = conn->newchunk(conn, HTTP_CHUNK_SIZE)))
        {
            if((len = xtask_get_stateinfo(xtask, chunk->data, HTTP_CHUNK_SIZE)) > 0)
            {
                return httpd_send_chunk(conn, chunk, len);
            }
            else 
            {
                conn->freechunk(conn, chunk);
                conn->over(conn);
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

/* new task */
int monitord_newtask(CONN *conn, XTHEAD *req)
{
    int ret = -1, n = 0, taskid = 0;
    CB_DATA *chunk = NULL;
    XTHEAD *head = NULL;
    char *block = 0;

    if(conn && req)
    {
        if((chunk = conn->newchunk(conn, XT_TASK_SIZE))) 
        {
            head = (XTHEAD *)chunk->data;
            if((req->cmd & XT_REQ_DOWNLOAD)) 
            {
                block = (char *)(chunk->data + sizeof(XTHEAD));
                n = XT_TASK_SIZE - sizeof(XTHEAD);
                if((n = xtask_new_download(xtask, req->flag, &taskid, block, n)) > 0)
                {
                    conn->xids[0] = taskid;
                    conn->xids[1] = req->flag;
                    conn->xids[2] = req->cmd;
                    head->cmd = XT_RESP_DOWNLOAD;
                    head->length = n;
                    if(conn->send_chunk(conn, chunk, n + sizeof(XTHEAD)) == 0)
                    {
                        DEBUG_LOGGER(logger, "NEW_DOWNLOAD_TASK:%d flag:%d ndata:%d", conn->xids[0], req->flag, n);
                        chunk = NULL;
                        return ret = 0;
                    }
                }
                else
                {

                }
            }
            else if((req->cmd & XT_REQ_EXTRACT)) 
            {
                block = (char *)(chunk->data + sizeof(XTHEAD));
                n = XT_TASK_SIZE - sizeof(XTHEAD);
                if((n = xtask_new_extract(xtask, &taskid, block, n)) > 0)
                {
                    conn->xids[0] = taskid;
                    conn->xids[2] = req->cmd;
                    head->cmd = XT_RESP_EXTRACT;
                    head->length = n;
                    if(conn->send_chunk(conn, chunk, n + sizeof(XTHEAD)) == 0)
                    {
                        ACCESS_LOGGER(logger, "NEW_EXTRACT_TASK:%d ndata:%d", conn->xids[0], n);
                        chunk = NULL;
                        return ret = 0;
                    }
                }
            }
            if(chunk) 
            {
                //fprintf(stdout, "%s::%d OK\r\n", __FILE__, __LINE__);
                if(req->cmd & XT_REQ_DOWNLOAD) head->cmd = XT_RESP_DOWNLOAD;
                if(req->cmd & XT_REQ_EXTRACT) head->cmd = XT_RESP_EXTRACT;
                if(conn->send_chunk(conn, chunk, sizeof(XTHEAD)) != 0)
                {
                    conn->freechunk(conn, chunk);
                    conn->close(conn);
                }
                else ret = 0;
            }
        }
    }
    return ret;
}

/* packet reader */
int monitord_packet_reader(CONN *conn, CB_DATA *buffer)
{
    return -1;
}

/* packet handler */
int monitord_packet_handler(CONN *conn, CB_DATA *packet)
{
    XTHEAD *head = NULL;

    if(conn && packet && (head = (XTHEAD *)(packet->data)))
    {
        if(head->length > 0)  
        {
            return conn->recv_chunk(conn, head->length);
        }
        else
        {
            return monitord_newtask(conn, head);
        }
    }
    return -1;
}

/* quick handler */
int monitord_quick_handler(CONN *conn, CB_DATA *packet)
{
    XTHEAD *head = NULL;

    if(conn && packet && (head = (XTHEAD *)(packet->data)))
    {
        return head->length;
    }
    return 0;
}

/* data handler */
int monitord_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    XTHEAD *head = NULL;
    int ret = -1;

    if(conn && packet && (head = (XTHEAD *)(packet->data)))
    {
        if(head->cmd & XT_REQ_DOWNLOAD)
        {
            ACCESS_LOGGER(logger, "OVER_DOWNLOAD_TASK:%d flag:%d ndata:%d", conn->xids[0], conn->xids[1], chunk->ndata);
            ret = xtask_over_download(xtask, conn->xids[1], conn->xids[0], chunk->data, chunk->ndata);
        }
        else if(head->cmd & XT_REQ_EXTRACT)
        {
            ACCESS_LOGGER(logger, "OVER_EXTRACT_TASK:%d ndata:%d", conn->xids[0], chunk->ndata);
            ret = xtask_over_extract(xtask, conn->xids[0], chunk->data, chunk->ndata);
        }
        conn->reset_xids(conn);
        return monitord_newtask(conn, head);
    }
    return -1;
}

/* error handler */
int monitord_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    int ret = -1;

    if(conn && conn->xids[0] > 0)
    {
        if(conn->xids[2] & XT_REQ_DOWNLOAD)
        {
            ret = xtask_reset_download(xtask, conn->xids[1], conn->xids[0]);
        }
        else if(conn->xids[2] & XT_REQ_EXTRACT)
        {
            ret = xtask_reset_extract(xtask, conn->xids[0]);
        }
    }
    return ret;
}

/* timeout handler */
int monitord_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{

    if(conn)
    {
        return 0;
        if(conn->evstate == EVSTATE_WAIT)
        {
            conn->over_timeout(conn);
            conn->over_evstate(conn);
        }
        else
        {
            conn->over_timeout(conn);
            conn->over_evstate(conn);
            return conn->over(conn);
        }
    }
    return -1;
}

/* OOB handler */
int monitord_oob_handler(CONN *conn, CB_DATA *oob)
{
    if(conn)
    {
        return 0;
    }
    return -1;
}

/* task handler */
void monitord_task_handler(void *arg)
{
    int id = 0;

    if((id = (((long)arg) - 1)) >= 0 && id < ntasks)
    {
        if(xtask_dump(xtask) > 0)
        {
            extractord->newtask(extractord, &monitord_task_handler, (void *)(((long )id+1)));
        }
        else
        {
            iqueue_push(taskqueue, id);
        }
    }
    return ;
}

/* heartbeat */
void cb_heartbeat_handler(void *arg)
{
    int i = 0, id = 0, total = 0;

    if(arg)
    {
        //return ;
        total = QTOTAL(taskqueue);
        for(i = 0; i < total; i++)
        {
            id = -1;
            iqueue_pop(taskqueue, &id);
            if(id >= 0 && id < ntasks)
            {
                if(extractord->newtask(extractord, &monitord_task_handler, 
                            (void *)((long)(id+1))) != 0)
                {
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
    int interval = 0, i = 0, n = 0, k = 0;
    char *s = NULL, *p = NULL;

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
        for(i = 0; i < E_OP_NUM; i++)
        {
            mtrie_add(argvmap, e_ops[i], strlen(e_ops[i]), i+1);
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
    p = iniparser_getstr(dict, "MONITORD:basedir");
    if((xtask = xtask_init(p)) == NULL)
    {
        fprintf(stderr, "initialize XTASK failed, %s\n", strerror(errno));
        _exit(-1);
    }
    if((n = iniparser_getint(dict, "MONITORD:task_log_level", 0)) > 0)
    {
        xtask_set_log_level(xtask, n);
    }
    p = iniparser_getstr(dict, "MONITORD:dumpdir");
    xtask_set_dumpdir(xtask, p);
    if((ops_disallow_map = mtree_init()) 
            && (p = iniparser_getstr(dict, "MONITORD:disallow_ops")))
    {
        while(*p != '\0')
        {
            while(*p == 0x20 || *p == '\t' || *p == ',' || *p == ';') ++p;
            s = p;
            while(*p != '\0' && *p != 0x20 && *p != '\t' && *p != ',' && *p != ';')++p;
            if((k = mtrie_get(argvmap, s, (p - s)) - 1) >= 0)
            {
                mtree_add(ops_disallow_map, k, k, NULL);
            }
        }
    }
    LOGGER_INIT(logger, iniparser_getstr(dict, "MONITORD:access_log"));
    LOGGER_SET_LEVEL(logger, iniparser_getint(dict, "MONITORD:access_log_level", 0));
    ntasks = iniparser_getint(dict, "MONITORD:dump_tasks", 64);
    if((taskqueue = iqueue_init()))
    {
        for(i = 0; i < ntasks; i++)
        {
            iqueue_push(taskqueue, i);
        }
    }
    /* ADNSD */
    if((adnsd = service_init()) == NULL)
    {
        fprintf(stderr, "Initialize service failed, %s", strerror(errno));
        _exit(-1);
    }
    adnsd->family = iniparser_getint(dict, "ADNSD:inet_family", AF_INET);
    adnsd->sock_type = iniparser_getint(dict, "ADNSD:socket_type", SOCK_DGRAM);
    adnsd->working_mode = iniparser_getint(dict, "ADNSD:working_mode", WORKING_PROC);
    adnsd->service_type = iniparser_getint(dict, "ADNSD:service_type", C_SERVICE);
    adnsd->service_name = iniparser_getstr(dict, "ADNSD:service_name");
    adnsd->nprocthreads = iniparser_getint(dict, "ADNSD:nprocthreads", 1);
    adnsd->niodaemons = iniparser_getint(dict, "ADNSD:niodaemons", 1);
    adnsd->use_cond_wait = iniparser_getint(dict, "ADNSD:use_cond_wait", 0);
    if(iniparser_getint(dict, "ADNSD:use_cpu_set", 0) > 0) adnsd->flag |= SB_CPU_SET;
    if(iniparser_getint(dict, "ADNSD:event_lock", 0) > 0) adnsd->flag |= SB_EVENT_LOCK;
    if(iniparser_getint(dict, "ADNSD:newconn_delay", 0) > 0) adnsd->flag |= SB_NEWCONN_DELAY;
    if(iniparser_getint(dict, "ADNSD:tcp_nodelay", 0) > 0) adnsd->flag |= SB_TCP_NODELAY;
    if(iniparser_getint(dict, "ADNSD:socket_linger", 0) > 0) adnsd->flag |= SB_SO_LINGER;
    if(iniparser_getint(dict, "ADNSD:while_send", 0) > 0) adnsd->flag |= SB_WHILE_SEND;
    if(iniparser_getint(dict, "ADNSD:log_thread", 0) > 0) adnsd->flag |= SB_LOG_THREAD;
    if(iniparser_getint(dict, "ADNSD:use_outdaemon", 0) > 0) adnsd->flag |= SB_USE_OUTDAEMON;
    if(iniparser_getint(dict, "ADNSD:use_evsig", 0) > 0) adnsd->flag |= SB_USE_EVSIG;
    if(iniparser_getint(dict, "ADNSD:use_cond", 0) > 0) adnsd->flag |= SB_USE_COND;
    if((n = iniparser_getint(dict, "ADNSD:sched_realtime", 0)) > 0) adnsd->flag |= (n & (SB_SCHED_RR|SB_SCHED_FIFO));
    if((n = iniparser_getint(dict, "ADNSD:io_sleep", 0)) > 0) adnsd->flag |= ((SB_IO_NANOSLEEP|SB_IO_USLEEP|SB_IO_SELECT) & n);
    adnsd->nworking_tosleep = iniparser_getint(dict, "ADNSD:nworking_tosleep", SB_NWORKING_TOSLEEP);
    adnsd->set_log(adnsd, iniparser_getstr(dict, "ADNSD:logfile"));
    adnsd->set_log_level(adnsd, iniparser_getint(dict, "ADNSD:log_level", 0));
    adnsd->session.packet_type = iniparser_getint(dict, "ADNS:packet_type", PACKET_CUSTOMIZED);
    adnsd->session.buffer_size = iniparser_getint(dict, "ADNSD:buffer_size", SB_BUF_SIZE);
    adnsd->session.packet_reader = &adnsd_packet_reader;
    adnsd->session.packet_handler = &adnsd_packet_handler;
    adnsd->session.error_handler = &adnsd_error_handler;
    adnsd->session.timeout_handler = &adnsd_timeout_handler;
    adnsd->session.evtimeout_handler = &adnsd_evtimeout_handler;
    adnsd->session.ok_handler = &adnsd_ok_handler;
    adnsd->session.flags = SB_NONBLOCK;
    interval = iniparser_getint(dict, "ADNSD:heartbeat_interval", SB_HEARTBEAT_INTERVAL);
    LOGGER_INIT(adnsd_logger, iniparser_getstr(dict, "ADNSD:access_log"));
    LOGGER_SET_LEVEL(adnsd_logger, iniparser_getint(dict, "ADNSD:access_log_level", 0));
    adnsd->set_heartbeat(adnsd, interval, &adnsd_heartbeat_handler, adnsd);
    /* SPIDERD */
    if((spiderd = service_init()) == NULL)
    {
        fprintf(stderr, "Initialize service failed, %s", strerror(errno));
        _exit(-1);
    }
    spiderd->family = iniparser_getint(dict, "SPIDERD:inet_family", AF_INET);
    spiderd->sock_type = iniparser_getint(dict, "SPIDERD:socket_type", SOCK_STREAM);
    spiderd->ip = iniparser_getstr(dict, "SPIDERD:service_ip");
    spiderd->port = iniparser_getint(dict, "SPIDERD:service_port", 3082);
    spiderd->working_mode = iniparser_getint(dict, "SPIDERD:working_mode", WORKING_PROC);
    spiderd->service_type = iniparser_getint(dict, "SPIDERD:service_type", S_SERVICE);
    spiderd->service_name = iniparser_getstr(dict, "SPIDERD:service_name");
    spiderd->nprocthreads = iniparser_getint(dict, "SPIDERD:nprocthreads", 1);
    spiderd->niodaemons = iniparser_getint(dict, "SPIDERD:niodaemons", 1);
    spiderd->use_cond_wait = iniparser_getint(dict, "SPIDERD:use_cond_wait", 0);
    if(iniparser_getint(dict, "SPIDERD:use_cpu_set", 0) > 0) spiderd->flag |= SB_CPU_SET;
    if(iniparser_getint(dict, "SPIDERD:event_lock", 0) > 0) spiderd->flag |= SB_EVENT_LOCK;
    if(iniparser_getint(dict, "SPIDERD:newconn_delay", 0) > 0) spiderd->flag |= SB_NEWCONN_DELAY;
    if(iniparser_getint(dict, "SPIDERD:tcp_nodelay", 0) > 0) spiderd->flag |= SB_TCP_NODELAY;
    if(iniparser_getint(dict, "SPIDERD:socket_linger", 0) > 0) spiderd->flag |= SB_SO_LINGER;
    if(iniparser_getint(dict, "SPIDERD:while_send", 0) > 0) spiderd->flag |= SB_WHILE_SEND;
    if(iniparser_getint(dict, "SPIDERD:log_thread", 0) > 0) spiderd->flag |= SB_LOG_THREAD;
    if(iniparser_getint(dict, "SPIDERD:use_outdaemon", 0) > 0) spiderd->flag |= SB_USE_OUTDAEMON;
    if(iniparser_getint(dict, "SPIDERD:use_evsig", 0) > 0) spiderd->flag |= SB_USE_EVSIG;
    if(iniparser_getint(dict, "SPIDERD:use_cond", 0) > 0) spiderd->flag |= SB_USE_COND;
    if((n = iniparser_getint(dict, "SPIDERD:sched_realtime", 0)) > 0) spiderd->flag |= (n & (SB_SCHED_RR|SB_SCHED_FIFO));
    if((n = iniparser_getint(dict, "SPIDERD:io_sleep", 0)) > 0) spiderd->flag |= ((SB_IO_NANOSLEEP|SB_IO_USLEEP|SB_IO_SELECT) & n);
    spiderd->nworking_tosleep = iniparser_getint(dict, "SPIDERD:nworking_tosleep", SB_NWORKING_TOSLEEP);
    spiderd->set_log(spiderd, iniparser_getstr(dict, "SPIDERD:logfile"));
    spiderd->set_log_level(spiderd, iniparser_getint(dict, "SPIDERD:log_level", 0));
    spiderd->session.packet_type = PACKET_CERTAIN_LENGTH;
    spiderd->session.packet_length = sizeof(XTHEAD);
    spiderd->session.buffer_size = iniparser_getint(dict, "SPIDERD:buffer_size", SB_BUF_SIZE);
    spiderd->session.packet_reader = &monitord_packet_reader;
    spiderd->session.packet_handler = &monitord_packet_handler;
    spiderd->session.quick_handler = &monitord_quick_handler;
    spiderd->session.data_handler = &monitord_data_handler;
    spiderd->session.error_handler = &monitord_error_handler;
    spiderd->session.timeout_handler = &monitord_timeout_handler;
    spiderd->session.flags = SB_NONBLOCK;
    //interval = iniparser_getint(dict, "SPIDERD:heartbeat_interval", SB_HEARTBEAT_INTERVAL);
    //spiderd->set_heartbeat(spiderd, interval, &cb_heartbeat_handler, spiderd);
    /* EXTRACTOR */
    if((extractord = service_init()) == NULL)
    {
        fprintf(stderr, "Initialize service failed, %s", strerror(errno));
        _exit(-1);
    }
    extractord->family = iniparser_getint(dict, "EXTRACTORD:inet_family", AF_INET);
    extractord->sock_type = iniparser_getint(dict, "EXTRACTORD:socket_type", SOCK_STREAM);
    extractord->ip = iniparser_getstr(dict, "EXTRACTORD:service_ip");
    extractord->port = iniparser_getint(dict, "EXTRACTORD:service_port", 3086);
    extractord->working_mode = iniparser_getint(dict, "EXTRACTORD:working_mode", WORKING_PROC);
    extractord->service_type = iniparser_getint(dict, "EXTRACTORD:service_type", S_SERVICE);
    extractord->service_name = iniparser_getstr(dict, "EXTRACTORD:service_name");
    extractord->nprocthreads = iniparser_getint(dict, "EXTRACTORD:nprocthreads", 1);
    extractord->niodaemons = iniparser_getint(dict, "EXTRACTORD:niodaemons", 1);
    extractord->use_cond_wait = iniparser_getint(dict, "EXTRACTORD:use_cond_wait", 0);
    extractord->ndaemons = ntasks;
    if(iniparser_getint(dict, "EXTRACTORD:use_cpu_set", 0) > 0) extractord->flag |= SB_CPU_SET;
    if(iniparser_getint(dict, "EXTRACTORD:event_lock", 0) > 0) extractord->flag |= SB_EVENT_LOCK;
    if(iniparser_getint(dict, "EXTRACTORD:newconn_delay", 0) > 0)extractord->flag |= SB_NEWCONN_DELAY;
    if(iniparser_getint(dict, "EXTRACTORD:tcp_nodelay", 0) > 0) extractord->flag |= SB_TCP_NODELAY;
    if(iniparser_getint(dict, "EXTRACTORD:socket_linger", 0) > 0) extractord->flag |= SB_SO_LINGER;
    if(iniparser_getint(dict, "EXTRACTORD:while_send", 0) > 0) extractord->flag |= SB_WHILE_SEND;
    if(iniparser_getint(dict, "EXTRACTORD:log_thread", 0) > 0) extractord->flag |= SB_LOG_THREAD;
    if(iniparser_getint(dict, "EXTRACTORD:use_outdaemon", 0) > 0) extractord->flag |= SB_USE_OUTDAEMON;
    if(iniparser_getint(dict, "EXTRACTORD:use_evsig", 0) > 0) extractord->flag |= SB_USE_EVSIG;
    if(iniparser_getint(dict, "EXTRACTORD:use_cond", 0) > 0) extractord->flag |= SB_USE_COND;
    if((n = iniparser_getint(dict, "EXTRACTORD:sched_realtime", 0)) > 0) extractord->flag |= (n & (SB_SCHED_RR|SB_SCHED_FIFO));
    if((n = iniparser_getint(dict, "EXTRACTORD:io_sleep", 0)) > 0) extractord->flag |= ((SB_IO_NANOSLEEP|SB_IO_USLEEP|SB_IO_SELECT) & n);
    extractord->nworking_tosleep = iniparser_getint(dict, "EXTRACTORD:nworking_tosleep", SB_NWORKING_TOSLEEP);
    extractord->set_log(extractord, iniparser_getstr(dict, "EXTRACTORD:logfile"));
    extractord->set_log_level(extractord, iniparser_getint(dict, "EXTRACTORD:log_level", 0));
    extractord->session.packet_type = PACKET_CERTAIN_LENGTH;
    extractord->session.packet_length = sizeof(XTHEAD);
    extractord->session.buffer_size = iniparser_getint(dict, "EXTRACTORD:buffer_size", SB_BUF_SIZE);
    extractord->session.packet_reader = &monitord_packet_reader;
    extractord->session.packet_handler = &monitord_packet_handler;
    extractord->session.quick_handler = &monitord_quick_handler;
    extractord->session.data_handler = &monitord_data_handler;
    extractord->session.error_handler = &monitord_error_handler;
    extractord->session.timeout_handler = &monitord_timeout_handler;
    extractord->session.flags = SB_NONBLOCK;
    interval = iniparser_getint(dict, "EXTRACTORD:heartbeat_interval", SB_HEARTBEAT_INTERVAL);
    extractord->set_heartbeat(extractord, interval, &cb_heartbeat_handler, extractord);
    return (sbase->add_service(sbase, httpd) 
            | sbase->add_service(sbase, adnsd)
            | sbase->add_service(sbase, spiderd)
            | sbase->add_service(sbase, extractord));
}

/* stop spiderd */
static void monitord_stop(int sig)
{
    switch (sig) 
    {
        case SIGINT:
        case SIGTERM:
            fprintf(stderr, "monitord is interrupted by user.\n");
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
    signal(SIGTERM, &monitord_stop);
    signal(SIGINT,  &monitord_stop);
    signal(SIGHUP,  &monitord_stop);
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
    //fprintf(stdout, "Initialized successed sizeof(XTTABLE):%d sizeof(ITEMPLATE):%d\r\n", sizeof(XTTABLE), sizeof(ITEMPLATE));
    fprintf(stdout, "Initialized successed\r\n");
    sbase->running(sbase, 0);
    //sbase->running(sbase, 3600);
    //sbase->running(sbase, 60000000);
    //sbase->stop(sbase);
    sbase->clean(sbase);
    if(http_headers_map) http_headers_map_clean(http_headers_map);
    if(dict)iniparser_free(dict);
    if(argvmap){mtrie_clean(argvmap);}
    if(taskqueue){iqueue_clean(taskqueue);}
    if(ops_disallow_map){mtree_clean(ops_disallow_map);}
    if(httpd_index_html_code) free(httpd_index_html_code);
    if(xtask) xtask_clean(xtask);
    //fprintf(stdout, "%s::%d\r\n", __FILE__, __LINE__);
    return 0;
}
