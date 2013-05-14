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
#include <pcre.h>
#include <sbase.h>
#include <ibase.h>
#include <iconv.h>
#include <chardet.h>
#include <mtask.h>
#include "mtrie.h"
#include "http.h"
#include "mime.h"
#include "html.h"
#include "iniparser.h"
#include "mutex.h"
#include "iqueue.h"
#include "stime.h"
#include "base64.h"
#include "url.h"
//#include "base64extractorhtml.h"
#include "timer.h"
#include "logger.h"
#include "xmm.h"
#include "xtask.h"
#include "zstream.h"
#include "base64.h"
#ifndef HTTP_BUF_SIZE
#define HTTP_BUF_SIZE           262144
#endif
#ifndef HTTP_QUERY_MAX
#define HTTP_QUERY_MAX          1024
#endif
#define HTTP_LINE_MAX           65536
#define HTTPD_NUM_PAGE          20
#define HTTPD_NPAGE             20
#define HTTPD_PAGE_MAX          500
#define HTTP_REQ_MAX            8192
#define HTTP_REDIRECT           0x01
#define TASK_BLOCK_SIZE         1048576
#define TASK_WAIT_TIME          1000000
#define LI(xxxx) ((unsigned int)xxxx)
typedef struct _ETASK
{
    int id;
    int bits;
    int status;
    int nblock;
    char block[TASK_BLOCK_SIZE];
    HTML *html;
    char *p;
    void *mutex;
    CB_DATA *chunk;
    CONN *conn;
}ETASK;
static SBASE *sbase = NULL;
static SERVICE *extractor = NULL;
static SERVICE *httpd = NULL;
static dictionary *dict = NULL;
static void *logger = NULL;
static ETASK *tasks = NULL;
static void *taskqueue = NULL;
static int ntasks = 0;
static MIME_MAP http_mime_map = {0};
static void *http_headers_map = NULL;
static char *http_default_charset = "UTF-8";
static char *httpd_home = NULL;
static int is_inside_html = 1;
static unsigned char *httpd_index_html_code = NULL;
static int  nhttpd_index_html_code = 0;
static char *extractord_ip = "127.0.0.1";
static int  extractord_port = 3086;
static void *argvmap = NULL;
static int http_task_type = 0;
#define E_OP_RESYNC             0x00
#define E_OP_STATE              0x01
static char *e_argvs[] =
{
    "op"
#define E_ARGV_OP           0
};
#define  E_ARGV_NUM         1
/* data handler */
int extractor_packet_reader(CONN *conn, CB_DATA *buffer);
int extractor_packet_handler(CONN *conn, CB_DATA *packet);
int extractor_quick_handler(CONN *conn, CB_DATA *packet);
int extractor_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int extractor_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int extractor_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int httpd_request_handler(CONN *conn, HTTP_REQ *http_req);
int extractor_ok_handler(CONN *conn);
void extractor_over_task();
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
            //if((n = kindex_state(kindex, line)) > 0)        
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

/* match data */
int extractor_data_match(int tid, XTROW *row, char *url, ITEMPLATE *templates, XTTABLE *table)
{
    int ret = -1, i = 0, flag = 0, erroffset = 0, nres = 0, x = 0,j = 0, id = 0,
        res[XT_FIELDS_MAX * 3], start_offset = 0, start = 0, over = 0, length = 0, 
        nodeid = 0, count = 0, filter = 0, antispam = 0;
    char *newurl, *p = NULL, *pp = NULL, *epp = NULL, *e = NULL, *s = NULL, *es = NULL, 
         *content = NULL, *host = NULL, *path = NULL, *last = NULL, *end = NULL;
    const char *error = NULL;
    XTRES *record = NULL;
    XTITEM *item = NULL;
    pcre *regx = NULL;

    if(tid >= 0 && row && url && templates && table)
    {
        record = (XTRES *)(tasks[tid].p);
        tasks[tid].p += sizeof(XTRES);
        record->id  = row->id;
        for(i = 0; i < row->ntemplates; i++)
        {
            record->nodeid = templates[i].linkmap.nodeid;
            flag = PCRE_DOTALL|PCRE_MULTILINE|PCRE_UTF8;
            if(templates[i].flags & TMP_IS_IGNORECASE) flag |= PCRE_CASELESS;
            if((regx = pcre_compile(templates[i].pattern, flag, &error, &erroffset, NULL)) == NULL)
            {
                FATAL_LOGGER(logger, "pcre_compile() error:%s", error);
                continue;
            }
            //WARN_LOGGER(logger, "content:%s", tasks[tid].block);
            start_offset = 0;
            nres = XT_FIELDS_MAX * 3;
            //k = 0;
            //pres = (PRES *)((void *)res);
            while(start_offset >= 0 && (count = pcre_exec(regx, NULL, tasks[tid].block, 
                            tasks[tid].nblock, start_offset, 0, res, nres)) > 0)
            {
                //out = tasks[tid].p; 
                item = (XTITEM *)tasks[tid].p;
                tasks[tid].p += sizeof(XTITEM);
                item->tableid = templates[i].tableid;
                if(count > XT_REGX_MAX) count = XT_REGX_MAX;
                item->count = count - 1;
                content = tasks[tid].block;
                if((templates[i].flags & TMP_IS_GLOBAL)) start_offset = res[(2 * (count - 1)) + 1];
                else start_offset = -1;
                if(templates[i].flags & TMP_IS_LINK)
                {
                    //if(templates[i].linkmap.flag & REG_IS_LIST) urlflag |=  URL_IS_LIST;
                    //if(templates[i].linkmap.flag & REG_IS_POST) urlflag |=  URL_IS_POST;
                    //if(templates[i].linkmap.flag & REG_IS_FILE) urlflag |=  URL_IS_FILE;
                    p = templates[i].link;
                    pp = newurl = tasks[tid].p;
                    epp = newurl + HTTP_URL_MAX;
                    MATCHEURL(count, p, pp, epp, s, es, x, res, content);
                    if((id = templates[i].map[x].fieldid) >= 0 && id < XT_FIELDS_MAX)
                    {
                        //records[id].start = eblock - block;
                        //records[id].end = eblock - block;
                    }
                }
                else
                {
                    for(j = 1; j < count; j++)
                    {
                        x = j - 1;
                        start = res[2*j];
                        over = res[2*j+1];
                        length = over - start;
                        nodeid = item->regs[x].nodeid = templates[i].map[x].nodeid;
                        item->regs[x].flag = templates[i].map[x].flag;
                        item->regs[x].fieldid = templates[i].map[x].fieldid;
                        //fprintf(stdout, "%s::%d j:%.*s\r\n", __FILE__, __LINE__, length, content + start);
                        //if(templates[i].map[x].flag & REG_IS_LIST) urlflag |= URL_IS_LIST;
                        ///if(templates[i].map[x].flag & REG_IS_FILE) urlflag |= URL_IS_FILE;
                        if((templates[i].map[x].flag & (REG_IS_LINK|REG_IS_URL)) 
                                && length > 0 && length < HTTP_URL_MAX && x < templates[i].nfields)
                        {
                            p = content + start;
                            e = content + over;
                            newurl = pp = tasks[tid].p;
                            epp = pp + HTTP_URL_MAX - 1;
                            s = url;
                            es = url + row->nurl;
                            //left = XT_CHUNK_SIZE - (tasks[tid].p - tasks[tid].chunk->data);
                            //WARN_LOGGER(logger, "i:%d j:%d url:%s path:%.*s left:%d", i, j, url, length, p, left);
                            CPURL(s, es, p, e, pp, epp, end, host, path, last);
                            tasks[tid].p = pp + 1;
                            item->regs[x].length = (pp - newurl);
                            DEBUG_LOGGER(logger, "urlid:%d start_offset:%d count:%d x:%d field:%d flag:%d url:%s", record->id, start_offset, count, x, item->regs[x].fieldid, (item->regs[x].flag&REG_IS_LINK), newurl);
                            //left = XT_CHUNK_SIZE - (tasks[tid].p - tasks[tid].chunk->data);
                            //WARN_LOGGER(logger, "i:%d j:%d newurl:%s left:%d length:%d", i, j, newurl, left, item->regs[x].length);
                            //if(templates[i].map[x].flag & REG_IS_LINK) urlflag |= URL_IS_LINK;
                            //item->regs[x].flag = urlflag;
                            //fprintf(stdout, "%s::%d newurl:%s length:%d/%d\r\n", __FILE__, __LINE__, newurl, pp - tasks[tid].p, pp - newurl);
                        }
                        else //if(templates[i].map[x].flag & (REG_IS_NEED_CLEARHTML| REG_IS_NEED_ANTISPAM)) 
                        {
                            filter = 0;
                            antispam = 0;
                            if(templates[i].map[x].flag & REG_IS_NEED_ANTISPAM)
                            {
                                antispam = 1;
                                filter = HTML_LINK_FILTER;
                            }
                            html_reset(tasks[tid].html);
                            html_get_content(tasks[tid].html,content+start,length,filter,antispam);
                            pp = tasks[tid].p;
                            memcpy(tasks[tid].p, tasks[tid].html->content, tasks[tid].html->ncontent);
                            tasks[tid].p += tasks[tid].html->ncontent;
                            *(tasks[tid].p)++ = '\0';
                            item->regs[x].length = tasks[tid].html->ncontent;
                            //left = XT_CHUNK_SIZE - (tasks[tid].p - tasks[tid].chunk->data);
                            //fprintf(stdout, "%s::%d newurl:%s length:%d/%d\r\n", __FILE__, __LINE__, newurl, pp - tasks[tid].p, pp - newurl);
                            //fprintf(stdout, "%s::%d ncontent:%d left:%d\r\n", __FILE__, __LINE__, item->regs[x].length, left);
                        }
                    }
                }
                item->length = tasks[tid].p - (char *)item;
                //WARN_LOGGER(logger, "i:%d start_offset:%d ntemplates:%d", i, start_offset, row->ntemplates);
            }
        }
        ret = record->length = tasks[tid].p - (char *)record;
    }
    return ret;
}

/* work for extractor */
int extractor_work(CONN *conn, char *data, int ndata)
{
    char *p = NULL, *end = NULL, *zdata = NULL, *url = NULL;
    int ret = -1, tid = 0;
    ITEMPLATE *templates = NULL;
    XTTABLE *table = NULL;
    CB_DATA *chunk = NULL;
    XTROW *row = NULL;

    if((p = data) && (end = (data + ndata)) > p && (tid = conn->session.xids[0] - 1) >= 0 
            && (chunk = tasks[tid].chunk = conn->newchunk(conn, XT_CHUNK_SIZE)))
    {
        tasks[tid].p = chunk->data;
        tasks[tid].p += sizeof(XTHEAD); 
        while(p < end) 
        {
            templates = NULL;
            table = NULL;
            row = (XTROW *)p;
            DEBUG_LOGGER(logger, "id:%d length:%d", row->id, row->length);
            p += sizeof(XTROW);
            if(row->ntemplates > 0) 
            {
                templates = (ITEMPLATE *)p;
                p += sizeof(ITEMPLATE) * row->ntemplates;
                table = (XTTABLE *)p;
                p += sizeof(XTTABLE);
            }
            url = p;
            p += row->nurl + 1;
            zdata = p;
            tasks[tid].nblock = TASK_BLOCK_SIZE;
            if(zdecompress((Bytef *)zdata, (uLong )(row->length),
                (Bytef *)(tasks[tid].block), (uLong *)((void *)&(tasks[tid].nblock))) == 0)
            {
                tasks[tid].block[tasks[tid].nblock] = 0;
                DEBUG_LOGGER(logger, "url:%s content:%s", url, tasks[tid].block);
                if(templates && table)
                {
                    extractor_data_match(tid, row, url, templates, table);
                }
                else
                {
                    html_reset(tasks[tid].html);
                    html_get_content(tasks[tid].html, tasks[tid].block, tasks[tid].nblock, 0, 0);
                }
            }
            p += row->length; 
        }
        extractor_over_task(tid);
        ret = 0;
    }
    return ret;
}


/* packet reader */
int extractor_packet_reader(CONN *conn, CB_DATA *buffer)
{
    if(conn) return 0;
    return -1;
}

/* packet handler */
int extractor_packet_handler(CONN *conn, CB_DATA *packet)
{
    if(conn && packet)
    {
        conn->wait_estate(conn);
        conn->set_timeout(conn, TASK_WAIT_TIME);
        return 0;
    }
    return -1;
}

/* packet quick handler */
int extractor_quick_handler(CONN *conn, CB_DATA *packet)
{
    XTHEAD *head = NULL;
    if(conn && (head = (XTHEAD *)(packet->data)))
    {
        return head->length;
    }
    return -1;
}

/* data handler */
int extractor_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    int tid = 0;

    if(conn)
    {
        if(chunk && chunk->data && (tid = conn->session.xids[0] - 1) >= 0)
        {
            tasks[tid].conn = conn;
            tasks[tid].status = HTTP_TASK_INIT;
            //tasks[tid].chunk = conn->newchunk(conn, XT_CHUNK_MAX);
            //tasks[tid].p = tasks[tid].chunk->data + sizeof(XTHEAD);
            return extractor_work(conn, chunk->data, chunk->ndata);
        }
    }
    return -1;
}

/* over task */
void extractor_over_task(int tid)
{
    CB_DATA *chunk = NULL;
    XTHEAD *head = NULL;
    CONN *conn = NULL;
    int n = 0;

    if(tid >= 0 && (conn = tasks[tid].conn) 
            && (chunk = tasks[tid].chunk) 
            && (head = (XTHEAD *)chunk->data))
    {
        n = tasks[tid].p - tasks[tid].chunk->data;
        head->cmd = XT_REQ_EXTRACT;
        head->flag |= (http_task_type|XT_TASK_OVER);
        head->length = n - sizeof(XTHEAD);
        tasks[tid].chunk = NULL;
        if(conn->send_chunk(conn, chunk, n) != 0)
        {
            conn->freechunk(conn, chunk);
            conn->close(conn);
            iqueue_push(taskqueue, tid);
        }
        //fprintf(stdout, "%s::%d OK\r\n", __FILE__, __LINE__);
    }
    return ;
}

/* ok handler */
int extractor_ok_handler(CONN *conn)
{
    XTHEAD head = {0};
    if(conn)
    {
       head.cmd = XT_REQ_EXTRACT; 
       head.flag = http_task_type; 
        return conn->push_chunk(conn, &head, sizeof(XTHEAD));
    }
    return -1;
}

/* error handler */
int extractor_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    int tid = 0;

    if(conn && (tid = conn->session.xids[0] - 1) >= 0)
    {
        iqueue_push(taskqueue, tid);
        return 0;
    }
    return -1;
}



/* timeout handler */
int extractor_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    if(conn)
    {
        conn->over_estate(conn);
        conn->over_timeout(conn);
        return extractor_ok_handler(conn);
    }
    return -1;
}

/* OOB handler */
int extractor_oob_handler(CONN *conn, CB_DATA *oob)
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
    SESSION sess = {0};
    CONN *conn = NULL;

    if(arg == (void *)extractor)
    {
        memcpy(&sess, &(extractor->session), sizeof(SESSION));
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
                    sess.xids[0] = id + 1;
                    if((conn = extractor->newconn(extractor, -1,-1, extractord_ip, extractord_port, &sess)))
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
    char *s = NULL, *p = NULL, *end = NULL;// *dictfile = NULL, *dictrules = NULL, *host = NULL; 
         //*property_name = NULL, *text_index_name = NULL, *int_index_name = NULL,
         //*long_index_name = NULL, *double_index_name = NULL, *display_name = NULL,
         //*block_name = NULL, *key_name;
    int interval = 0, i = 0, n = 0;// port = 0;// commitid = 0, queueid = 0;

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
    /*
    if(html_code_base64 && (n = strlen(html_code_base64)) > 0
            && (httpd_index_html_code = (unsigned char *)calloc(1, n + 1)))
    {
        nhttpd_index_html_code = base64_decode(httpd_index_html_code,
                (char *)html_code_base64, n);
    }
    */
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
    /* http mime map */
    if((p = iniparser_getstr(dict, "HTTPD:mime")))
    {
        end = p + strlen(p);
        if((mime_map_init(&http_mime_map)) != 0)
        {
            fprintf(stderr, "Initialize http_mime_map failed,%s", strerror(errno));
            _exit(-1);
        }
        mime_add_line(&http_mime_map, p, end);
    }
    /* EXTRACTOR */
    if((extractor = service_init()) == NULL)
    {
        fprintf(stderr, "Initialize service failed, %s", strerror(errno));
        _exit(-1);
    }
    extractor->family = iniparser_getint(dict, "EXTRACTOR:inet_family", AF_INET);
    extractor->sock_type = iniparser_getint(dict, "EXTRACTOR:socket_type", SOCK_STREAM);
    extractord_ip = extractor->ip = iniparser_getstr(dict, "EXTRACTOR:service_ip");
    extractord_port = extractor->port = iniparser_getint(dict, "EXTRACTOR:service_port", 3086);
    extractor->working_mode = iniparser_getint(dict, "EXTRACTOR:working_mode", WORKING_PROC);
    extractor->service_type = iniparser_getint(dict, "EXTRACTOR:service_type", C_SERVICE);
    extractor->service_name = iniparser_getstr(dict, "EXTRACTOR:service_name");
    extractor->nprocthreads = iniparser_getint(dict, "EXTRACTOR:nprocthreads", 1);
    extractor->niodaemons = iniparser_getint(dict, "EXTRACTOR:niodaemons", 1);
    extractor->use_cond_wait = iniparser_getint(dict, "EXTRACTOR:use_cond_wait", 0);
    if(iniparser_getint(dict, "EXTRACTOR:use_cpu_set", 0) > 0) extractor->flag |= SB_CPU_SET;
    if(iniparser_getint(dict, "EXTRACTOR:event_lock", 0) > 0) extractor->flag |= SB_EVENT_LOCK;
    if(iniparser_getint(dict, "EXTRACTOR:newconn_delay", 0) > 0) extractor->flag |= SB_NEWCONN_DELAY;
    if(iniparser_getint(dict, "EXTRACTOR:tcp_nodelay", 0) > 0) extractor->flag |= SB_TCP_NODELAY;
    if(iniparser_getint(dict, "EXTRACTOR:socket_linger", 0) > 0) extractor->flag |= SB_SO_LINGER;
    if(iniparser_getint(dict, "EXTRACTOR:while_send", 0) > 0) extractor->flag |= SB_WHILE_SEND;
    if(iniparser_getint(dict, "EXTRACTOR:log_thread", 0) > 0) extractor->flag |= SB_LOG_THREAD;
    if(iniparser_getint(dict, "EXTRACTOR:use_outdaemon", 0) > 0) extractor->flag |= SB_USE_OUTDAEMON;
    if(iniparser_getint(dict, "EXTRACTOR:use_evsig", 0) > 0) extractor->flag |= SB_USE_EVSIG;
    if(iniparser_getint(dict, "EXTRACTOR:use_cond", 0) > 0) extractor->flag |= SB_USE_COND;
    if((n = iniparser_getint(dict, "EXTRACTOR:sched_realtime", 0)) > 0) extractor->flag |= (n & (SB_SCHED_RR|SB_SCHED_FIFO));
    if((n = iniparser_getint(dict, "EXTRACTOR:io_sleep", 0)) > 0) extractor->flag |= ((SB_IO_NANOSLEEP|SB_IO_USLEEP|SB_IO_SELECT) & n);
    extractor->nworking_tosleep = iniparser_getint(dict, "EXTRACTOR:nworking_tosleep", SB_NWORKING_TOSLEEP);
    extractor->set_log(extractor, iniparser_getstr(dict, "EXTRACTOR:logfile"));
    extractor->set_log_level(extractor, iniparser_getint(dict, "EXTRACTOR:log_level", 0));
    extractor->session.packet_type = PACKET_CERTAIN_LENGTH;
    extractor->session.packet_length = sizeof(XTHEAD);
    extractor->session.buffer_size = iniparser_getint(dict, "EXTRACTOR:buffer_size", SB_BUF_SIZE);
    extractor->session.packet_reader = &extractor_packet_reader;
    extractor->session.packet_handler = &extractor_packet_handler;
    extractor->session.ok_handler = &extractor_ok_handler;
    extractor->session.quick_handler = &extractor_quick_handler;
    extractor->session.data_handler = &extractor_data_handler;
    extractor->session.error_handler = &extractor_error_handler;
    extractor->session.timeout_handler = &extractor_timeout_handler;
    extractor->session.oob_handler = &extractor_oob_handler;
    extractor->session.flags = SB_NONBLOCK;
    interval = iniparser_getint(dict, "EXTRACTOR:heartbeat_interval", SB_HEARTBEAT_INTERVAL);
    extractor->set_heartbeat(extractor, interval, &cb_heartbeat_handler, extractor);
    LOGGER_INIT(logger, iniparser_getstr(dict, "EXTRACTOR:access_log"));
    ntasks = iniparser_getint(dict, "EXTRACTOR:ntasks", 64);
    tasks = (ETASK *)xmm_mnew(ntasks * sizeof(ETASK));
    if((taskqueue = iqueue_init()))
    {
        for(i = 0; i < ntasks; i++)
        {
            iqueue_push(taskqueue, i);
            tasks[i].id = i;
            tasks[i].status = 0;
            tasks[i].html = html_init();
            MUTEX_INIT(tasks[i].mutex);
        }
    }
    return (sbase->add_service(sbase, httpd) | sbase->add_service(sbase, extractor));
}

/* stop extractor */
static void extractor_stop(int sig)
{
    switch (sig) 
    {
        case SIGINT:
        case SIGTERM:
            fprintf(stderr, "extractor is interrupted by user.\n");
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
    while((ch = getopt(argc, argv, "c:d")) != -1)
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
    signal(SIGTERM, &extractor_stop);
    signal(SIGINT,  &extractor_stop);
    signal(SIGHUP,  &extractor_stop);
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
            html_clean(tasks[i].html);
            MUTEX_DESTROY(tasks[i].mutex);
        }
        xmm_free(tasks, ntasks * sizeof(ETASK));
    }
    if(taskqueue){iqueue_clean(taskqueue);}
    if(argvmap)mtrie_clean(argvmap);
    if(httpd_index_html_code) free(httpd_index_html_code);
    return 0;
}
