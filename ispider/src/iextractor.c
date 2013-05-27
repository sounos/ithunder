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
//#include <mtask.h>
#include "mtrie.h"
#include "http.h"
#include "iniparser.h"
#include "iqueue.h"
#include "stime.h"
#include "base64.h"
#include "url.h"
#include "timer.h"
#include "logger.h"
#include "xmm.h"
#include "html.h"
#include "xtask.h"
#include "zstream.h"
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
#define LI(xxxx) ((unsigned int)xxxx)
typedef struct _ETASK
{
    CONN *conn;
    CB_DATA *chunk;
    int id;
    int bits;
    int status;
    int nblock;
    char *p;
    char *pp;
    HTML *html;
    char block[TASK_BLOCK_SIZE];
}ETASK;
static SBASE *sbase = NULL;
static SERVICE *extractor = NULL;
static dictionary *dict = NULL;
static void *logger = NULL;
static ETASK *tasks = NULL;
static void *taskqueue = NULL;
static int ntasks = 0;
static int running_status = 1;
static char *extractord_ip = "127.0.0.1";
static int  extractord_port = 3086;
static void *argvmap = NULL;
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
int extractor_packet_reader(CONN *conn, CB_DATA *buffer);
int extractor_packet_handler(CONN *conn, CB_DATA *packet);
int extractor_quick_handler(CONN *conn, CB_DATA *packet);
int extractor_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int extractor_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int extractor_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk);
int extractor_evtimeout_handler(CONN *conn);
int extractor_ok_handler(CONN *conn);
void extractor_over_task();

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


/* match data */
int extractor_data_match(int tid, XTROW *row, char *url, int *ids, 
        ITEMPLATE *templates, XTTABLE *tables)
{
    int ret = -1, i = 0, flag = 0, erroffset = 0, nmatches = 0, x = 0,j = 0, id = 0,
        matches[XT_FIELDS_MAX * 3], start_offset = 0, start = 0, over = 0, length = 0, 
        nodeid = 0, count = 0, filter = 0, antispam = 0, left = 0, k = 0, ntxt = 0;
    char *newurl, *p = NULL, *pp = NULL, *epp = NULL, *e = NULL, *s = NULL, *es = NULL, 
         *content = NULL, *host = NULL, *path = NULL, *last = NULL, *end = NULL, 
         *txt = NULL, *ps = NULL, *pattern = NULL, *top = NULL;
    const char *error = NULL;
    ITEMPLATE *temp = NULL;
    XTTABLE *table = NULL;
    XTRES *res = NULL;
    XTITEM *item = NULL;
    pcre *regx = NULL;

    if(tid >= 0 && row && url && ids && templates && tables)
    {
        if((left = (XT_CHUNK_SIZE - (tasks[tid].p - tasks[tid].pp))) < row->length * 2)
        {
            WARN_LOGGER(logger, "chunk length:%d [%p - %p]", tasks[tid].p - tasks[tid].pp);
            return ret;
        }
        res = (XTRES *)(tasks[tid].p);
        tasks[tid].p += sizeof(XTRES);
        res->id  = row->id;
        for(i = 0; i < row->ntemplates; i++)
        {
            k = ids[i];
            temp = &(templates[k]);
            flag = PCRE_DOTALL|PCRE_MULTILINE|PCRE_UTF8;
            txt = tasks[tid].block;
            ntxt = tasks[tid].nblock;
            start_offset = 0;
            nmatches = XT_FIELDS_MAX * 3;
            pattern = temp->pattern;
            if((temp->flags & TMP_IS_SUB))
            {
                if((ps = strchr(temp->pattern, '\n')) == NULL) continue;
                *ps++ = '\0';
                top = temp->pattern;
                if((regx = pcre_compile(top, flag, &error, &erroffset, NULL)) == NULL)
                {
                    FATAL_LOGGER(logger, "pcre_compile(%d,%d,%d,%s) url:%s error:%s", k, row->ntemplates, temp->flags, top, url, error);
                    continue;

                }
                if((count = pcre_exec(regx, NULL, txt, ntxt, 0, 0, matches, nmatches)) > 1)
                {
                    start = matches[2];
                    over = matches[3];
                    txt = txt + start;
                    ntxt = over - start;
                }
                else
                {
                    FATAL_LOGGER(logger, "pcre_exec(%d,%d,%d,%s) url:%s error:%s", k, row->ntemplates, temp->flags, top, url, error);
                    continue;
                }
                pattern = ps;
                /*replace \0 with \n */
                *(ps-1) = '\n';
            }
            //fprintf(stdout, "top:%s pattern:%s block:%.*s\n", top, pattern, ntxt, txt);
            table = &(tables[temp->tableid]);
            res->nodeid = temp->linkmap.nodeid;
            if(temp->flags & TMP_IS_IGNORECASE) flag |= PCRE_CASELESS;
            if((regx = pcre_compile(pattern, flag, &error, &erroffset, NULL)) == NULL)
            {
                FATAL_LOGGER(logger, "pcre_compile(%d,%d,%d,%s) url:%s error:%s", k, row->ntemplates, temp->flags, pattern, url, error);
                continue;
            }
            DEBUG_LOGGER(logger, "k:%d url:%s chunk->used:%d ntxt:%d nblock:%d", k, url, tasks[tid].p - tasks[tid].pp, ntxt, tasks[tid].nblock);
            while(start_offset >= 0 && (count = pcre_exec(regx, NULL, txt, ntxt, start_offset, 0, matches, nmatches)) > 1)
            {
                //out = tasks[tid].p; 
                item = (XTITEM *)tasks[tid].p;
                memset(item, 0, sizeof(XTITEM));
                tasks[tid].p += sizeof(XTITEM);
                item->tableid = temp->tableid;
                if(count > XT_REGX_MAX) count = XT_REGX_MAX;
                item->count = count - 1;
                content = txt;
                if((temp->flags & TMP_IS_GLOBAL)) start_offset = matches[(2 * (count - 1)) + 1];
                else start_offset = -1;
                if(temp->flags & TMP_IS_LINK)
                {
                    p = temp->link;
                    pp = newurl = tasks[tid].p;
                    epp = newurl + HTTP_URL_MAX;
                    MATCHEURL(count, p, pp, epp, s, es, x, matches, content);
                    if((id = temp->map[x].fieldid) >= 0 && id < XT_FIELDS_MAX)
                    {
                        //ress[id].start = eblock - block;
                        //ress[id].end = eblock - block;
                    }
                }
                else
                {
                    for(j = 1; j < count; j++)
                    {
                        x = j - 1;
                        start = matches[2*j];
                        over = matches[2*j+1];
                        length = over - start;
                        nodeid = item->regs[x].nodeid = temp->map[x].nodeid;
                        item->regs[x].flag = temp->map[x].flag;
                        id = item->regs[x].fieldid = temp->map[x].fieldid;
                        if((temp->map[x].flag & (REG_IS_LINK|REG_IS_URL)) 
                                && length > 0 && length < HTTP_URL_MAX && x < temp->nfields)
                        {
                            p = content + start;
                            e = content + over;
                            newurl = pp = tasks[tid].p;
                            epp = pp + HTTP_URL_MAX - 1;
                            s = url;
                            es = url + row->nurl;
                            CPURL(s, es, p, e, pp, epp, end, host, path, last);
                            *pp = '\0';
                            tasks[tid].p = pp + 1;
                            item->regs[x].length = (pp - newurl);
                            DEBUG_LOGGER(logger, "x:%d length:%d/%d chunk:%d pattern:%s offset:%d count:%d base:%s newurl:%s", x, item->regs[x].length, length, tasks[tid].p - tasks[tid].pp, pattern, txt - tasks[tid].block, count, url, newurl);
                        }
                        else if(temp->map[x].flag & (REG_IS_NEED_CLEARHTML|REG_IS_NEED_ANTISPAM)) 
                        {
                            filter = 0;
                            antispam = 0;
                            if(temp->map[x].flag & REG_IS_NEED_ANTISPAM)
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
                            DEBUG_LOGGER(logger, "res->id:%d count:%d x:%d length:%d chunk:%d ncontent:%d", res->id, count, x, item->regs[x].length, tasks[tid].p - tasks[tid].pp, tasks[tid].html->ncontent);
                        }
                        else
                        {
                            memcpy(tasks[tid].p, content+start,length);
                            tasks[tid].p += length;
                            *(tasks[tid].p)++ = '\0';
                            item->regs[x].length = length;
                        }
                    }
                }
                item->length = tasks[tid].p - (char *)item;
                //WARN_LOGGER(logger, "k:%d url:%s chunk->used:%d count:%d offset:%d flag:%d temps[%d] item{length:%d,count:%d,%s}", k, url, tasks[tid].p - tasks[tid].pp, count, start_offset, temp->flags, i, item->length, item->count, (char *)item + sizeof(XTITEM));
            }
        }
        ret = res->length = tasks[tid].p - (char *)res;
    }
    return ret;
}

/* work for extractor */
int extractor_work(CONN *conn, char *data, int ndata)
{
    char *p = NULL, *end = NULL, *zdata = NULL, *url = NULL, *out = NULL;
    int ret = -1, tid = 0, x = 0, *ids = NULL;
    size_t nzdata = 0, nout = 0;
    ITEMPLATE *templates = NULL;
    XTTABLE *tables = NULL;
    CB_DATA *chunk = NULL;
    XTROW *row = NULL;
    XTMORE *more = NULL;

    if((p = data) && (end = (data + ndata)) > p && (tid = (conn->session.xids[0] - 1)) >= 0 
            && (chunk = tasks[tid].chunk = conn->newchunk(conn, XT_CHUNK_SIZE)))
    {
        tasks[tid].pp = tasks[tid].p = chunk->data;
        tasks[tid].p += sizeof(XTHEAD); 
        more = (XTMORE *)p;
        p += sizeof(XTMORE);
        tables = (XTTABLE *)p; 
        p += sizeof(XTTABLE) * more->ntables;
        templates = (ITEMPLATE *)p;
        p += sizeof(ITEMPLATE) * more->ntemplates;
        DEBUG_LOGGER(logger, "p:%p pp:%p ndata:%d ntable:%d ntemplates:%d", tasks[tid].p, tasks[tid].pp, ndata, more->ntables, more->ntemplates);
        while(p < end && running_status) 
        {
            row = (XTROW *)p;
            p += sizeof(XTROW);
            ids = (int *)p;
            p += sizeof(int) * row->ntemplates;
            url = p;
            DEBUG_LOGGER(logger, "p:%p pp:%p", tasks[tid].p, tasks[tid].pp);
            p += row->nurl + 1;
            zdata = p;
            nzdata = row->length;
            out = tasks[tid].block;
            nout = tasks[tid].nblock = TASK_BLOCK_SIZE;
            DEBUG_LOGGER(logger, "id:%d url:%s length:%d", row->id, url, row->length);
            if(nzdata < TASK_BLOCK_SIZE && zdecompress((Bytef *)zdata, (uLong )nzdata, 
                        (Bytef *)out, (uLong *)&nout) == 0)
            {
                tasks[tid].block[nout] = 0;
                tasks[tid].nblock = nout;
                DEBUG_LOGGER(logger, "p:%p pp:%p nout:%d ", tasks[tid].p, tasks[tid].pp, nout);
                if(row->ntemplates > 0)
                {
                    ACCESS_LOGGER(logger, "x:%d match id:%d url:%s length:%d nblock:%d chunk->left:%d[%p - %p]", x, row->id, url, row->length, tasks[tid].nblock, (tasks[tid].p - tasks[tid].pp), tasks[tid].p, tasks[tid].pp);
                    extractor_data_match(tid, row, url, ids, templates, tables);
                    ACCESS_LOGGER(logger, "x:%d match id:%d url:%s length:%d nblock:%d chunk->left:%d[%p - %p]", x, row->id, url, row->length, tasks[tid].nblock, (tasks[tid].p - tasks[tid].pp), tasks[tid].p, tasks[tid].pp);
                }
                else
                {
                    DEBUG_LOGGER(logger, "x:%d get_content id:%d url:%s length:%d nblock:%d chunk->left:%d", x, row->id, url, row->length, tasks[tid].nblock, tasks[tid].p - tasks[tid].pp);
                    html_reset(tasks[tid].html);
                    html_get_content(tasks[tid].html, tasks[tid].block, tasks[tid].nblock, 0, 0);
                    DEBUG_LOGGER(logger, "get_content id:%d url:%s length:%d", row->id, url, row->length);
                }
            }
            DEBUG_LOGGER(logger, "over id:%d url:%s length:%d", row->id, url, row->length);
            p += row->length; 
            ++x;
        }
        if(running_status) extractor_over_task(tid);
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
        conn->wait_evtimeout(conn, task_wait_time);
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
        if(chunk && chunk->data && (tid = (conn->session.xids[0] - 1)) >= 0)
        {
            tasks[tid].conn = conn;
            //tasks[tid].status = HTTP_TASK_INIT;
            //tasks[tid].chunk = conn->newchunk(conn, XT_CHUNK_MAX);
            //tasks[tid].p = tasks[tid].chunk->data + sizeof(XTHEAD);
            ACCESS_LOGGER(logger, "working tid:%d ndata:%d on remote[%s:%d] local[%s:%d]", tid, chunk->ndata, conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port);
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
       //conn->start_cstate(conn);
        n = tasks[tid].p - tasks[tid].chunk->data;
        head->cmd = XT_REQ_EXTRACT;
        head->flag |= XT_TASK_OVER;
        head->length = n - sizeof(XTHEAD);
        tasks[tid].chunk = NULL;
        ACCESS_LOGGER(logger, "over-working tid:%d ndata:%d on remote[%s:%d] local[%s:%d]", tid, head->length, conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port);
        if(conn->send_chunk(conn, chunk, n) != 0)
        {
            conn->freechunk(conn, chunk);
            conn->close(conn);
            tasks[tid].status = 0;
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
    int tid = -1;
    if(conn && (tid = (conn->session.xids[0] - 1)) >= 0)
    {
       head.cmd = XT_REQ_EXTRACT; 
       ACCESS_LOGGER(logger, "ready-extractor tid:%d on remote[%s:%d] local[%s:%d]", tid, conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port);
       //conn->start_cstate(conn);
        return conn->push_chunk(conn, &head, sizeof(XTHEAD));
    }
    return -1;
}

/* error handler */
int extractor_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    int tid = 0;

    if(conn && (tid = (conn->session.xids[0] - 1)) >= 0)
    {
        if(tasks[tid].status) 
        {
            tasks[tid].status = 0;
            iqueue_push(taskqueue, tid);
            WARN_LOGGER(logger, "tid:%d conn broken remote[%s:%d] local[%s:%d] via %d", tid, conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
        }
        return 0;
    }
    return -1;
}



/* timeout handler */
int extractor_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
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

/* timeout handler */
int extractor_evtimeout_handler(CONN *conn)
{
    if(conn)
    {
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
                    tasks[id].status = 1;
                    if((conn = extractor->newconn(extractor, -1,-1, extractord_ip, 
                                    extractord_port, &sess)) == NULL)
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
    int interval = 0, i = 0, n = 0;
    // port = 0;// commitid = 0, queueid = 0;

    if((dict = iniparser_new(conf)) == NULL)
    {
        fprintf(stderr, "Initializing conf:%s failed, %s\n", conf, strerror(errno));
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
    extractor->session.evtimeout_handler = &extractor_evtimeout_handler;
    extractor->session.oob_handler = &extractor_oob_handler;
    extractor->session.flags = SB_NONBLOCK;
    interval = iniparser_getint(dict, "EXTRACTOR:heartbeat_interval", SB_HEARTBEAT_INTERVAL);
    extractor->set_heartbeat(extractor, interval, &cb_heartbeat_handler, extractor);
    LOGGER_INIT(logger, iniparser_getstr(dict, "EXTRACTOR:access_log"));
    LOGGER_SET_LEVEL(logger, iniparser_getint(dict, "EXTRACTOR:access_log_level", 0));
    ntasks = iniparser_getint(dict, "EXTRACTOR:ntasks", 64);
    task_wait_time = iniparser_getint(dict, "EXTRACTOR:task_wait_time", 10000000);
    tasks = (ETASK *)xmm_mnew(ntasks * sizeof(ETASK));
    if((taskqueue = iqueue_init()))
    {
        for(i = 0; i < ntasks; i++)
        {
            iqueue_push(taskqueue, i);
            tasks[i].id = i;
            tasks[i].status = 0;
            tasks[i].html = html_init();
        }
    }
    return sbase->add_service(sbase, extractor);
}

/* stop extractor */
static void extractor_stop(int sig)
{
    switch (sig) 
    {
        case SIGINT:
        case SIGTERM:
            fprintf(stderr, "extractor is interrupted by user.\n");
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
    if(dict)iniparser_free(dict);
    if(tasks)
    {
        for(i = 0; i < ntasks; i++)
        {
            html_clean(tasks[i].html);
        }
        xmm_free(tasks, ntasks * sizeof(ETASK));
    }
    if(taskqueue){iqueue_clean(taskqueue);}
    if(argvmap)mtrie_clean(argvmap);
    return 0;
}
