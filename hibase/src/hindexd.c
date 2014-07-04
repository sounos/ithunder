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
#include "stime.h"
#include "base64.h"
#include "base64indexdhtml.h"
#include "http.h"
#include "iniparser.h"
#include "logger.h"
#include "timer.h"
#include "mtrie.h"
#ifndef HTTP_BUF_SIZE
#define HTTP_BUF_SIZE           131072
#endif
#ifndef HTTP_LINE_SIZE
#define HTTP_LINE_SIZE          4096
#endif
#ifndef HTTP_QUERY_MAX
#define HTTP_QUERY_MAX          1024
#endif
#define HTTP_LINE_MAX           65536
#define HTTP_RESP_OK            "HTTP/1.0 200 OK\r\nContent-Length:0\r\n\r\n"
#define HTTP_BAD_REQUEST        "HTTP/1.0 400 Bad Request\r\nContent-Length:0\r\n\r\n"
#define HTTP_NOT_FOUND          "HTTP/1.0 404 Not Found\r\nContent-Length:0\r\n\r\n" 
#define HTTP_NOT_MODIFIED       "HTTP/1.0 304 Not Modified\r\nContent-Length:0\r\n\r\n"
#define HTTP_NO_CONTENT         "HTTP/1.0 204 No Content\r\nContent-Length:0\r\n\r\n"
#ifndef LL64
#define LL64(xxxx) ((long long int)xxxx)
#endif
static char *http_default_charset = "UTF-8";
static char *httpd_home = NULL;
static int is_inside_html = 1;
static unsigned char *httpd_index_html_code = NULL;
static int  nhttpd_index_html_code = 0;
static SBASE *sbase = NULL;
static IBASE *ibase = NULL;
static SERVICE *httpd = NULL;
static SERVICE *indexd = NULL;
static SERVICE *queryd = NULL;
static dictionary *dict = NULL;
static void *http_headers_map = NULL;
static void *argvmap = NULL;
static void *logger = NULL;
//static int httpd_page_num  = 20;
//static int httpd_page_max  = 50;
static char *highlight_start = "<font color=red>";
static char *highlight_end = "</font>";
#define OP_CLEAR_CACHE      0x01
#define OP_READ_LIST        0x02
#define X_KEY_MAX           1024
static char *e_argvs[] =
{
    "query",
#define E_ARGV_QUERY        0
    "fieldsfilter",
#define E_ARGV_FIELDFILTER  1
    "orderby",
#define E_ARGV_ORDERBY      2
    "from",
#define E_ARGV_FROM         3
    "display",
#define E_ARGV_DISPLAY      4
    "order",
#define E_ARGV_ORDER        5
    "catgroup",
#define E_ARGV_CATGROUP     6
    "catfilter",
#define E_ARGV_CATFILTER    7
    "catblock",             
#define E_ARGV_CATBLOCK     8 
    "rangefilter",
#define E_ARGV_RANGEFILTER  9
    "phrase",
#define E_ARGV_PHRASE       10
    "count",
#define E_ARGV_COUNT        11
    "not",
#define E_ARGV_NOT          12
    "rank",
#define E_ARGV_RANK         13
    "slevel",
#define E_ARGV_SLEVEL       14
    "xup",
#define E_ARGV_XUP          15
    "xdown",
#define E_ARGV_XDOWN        16
    "qfunc",
#define E_ARGV_QFUNC        17
    "qfhits",
#define E_ARGV_QFHITS       18
    "rel",
#define E_ARGV_REL          19
    "hitscale",
#define E_ARGV_HITSCALE     20
    "op",            
#define E_ARGV_OP           21
    "qid",
#define E_ARGV_QID          22
    "booland",
#define E_ARGV_BOOLAND      23
    "bhits",
#define E_ARGV_BHITS        24
    "bfhits",
#define E_ARGV_BFHITS       25
    "bphrase",
#define E_ARGV_BPHRASE      26
    "bnterm",
#define E_ARGV_BNTERM       27
    "bxcatup",
#define E_ARGV_BXCATUP      28
    "bxcatdown",
#define E_ARGV_BXCATDOWN    29
    "brank",
#define E_ARGV_BRANK        30
    "ntop",
#define E_ARGV_NTOP         31
    "multicat",
#define E_ARGV_MULTICAT     32
    "key",
#define E_ARGV_KEY          33
    "bitfields",
#define E_ARGV_BITFIELDS    34
    "qweight",
#define E_ARGV_QWEIGHT      35
    "in"
#define E_ARGV_IN           36
};
#define  E_ARGV_NUM         37
int httpd_request_handler(CONN *conn, HTTP_REQ *httpRQ, IQUERY *query);
/* packet reader for indexd */
int indexd_packet_reader(CONN *conn, CB_DATA *buffer)
{
    return -1;
}

/* indexd quick handler */
int indexd_quick_handler(CONN *conn, CB_DATA *packet)
{
    IHEAD *req = NULL;
    int n = 0;
    
    if(conn && packet && (req = (IHEAD *)packet->data)) 
    {
        n = req->length;
    }
    return n;
}

/* indexd packet handler */
int indexd_packet_handler(CONN *conn, CB_DATA *packet)
{
    IHEAD *req = NULL;

    if(conn && packet && (req = (IHEAD *)packet->data)) 
    {
        if(req->length > 1024 * 1024 * 32)
        {
            FATAL_LOGGER(logger, "Invalid DATA{cmd:%d length:%d}", req->cmd, req->length);
        }
        if(req->length > 0)
        {
            //conn->save_cache(conn, req, packet->ndata);
            return conn->recv_chunk(conn, req->length);
        }
        else 
        {
            if(req->cmd == IB_REQ_CLEAR_CACHE)
            {
                req->cmd = IB_RESP_CLEAR_CACHE;
                return conn->push_chunk(conn, packet->data, packet->ndata);
            }
            else
                goto err_end;
        }
    }
err_end:
    return conn->close(conn);
}

/* index  handler */
int indexd_index_handler(CONN *conn)
{
    CB_DATA *chunk = NULL, *packet = NULL;
    char *p = NULL, *end = NULL;
    IBDATA block = {0};
    IHEAD resp = {0};
    IHEAD *req = NULL;
    DOCHEADER *docheader = NULL;
    int i = 0, count = 0;

    if(conn)
    {
        chunk = PCB(conn->chunk);
        packet = PCB(conn->packet);
        if(packet && (req = (IHEAD *)(packet->data)) && chunk 
                && (p = chunk->data) && chunk->ndata > 0)
        {
            if(chunk->ndata < sizeof(int)) goto err_end; 
            end = p + chunk->ndata;
            memcpy(&count, p, sizeof(int));
            p += sizeof(int);
            while(p < end && i < count)
            {
                memcpy(&(block.ndata), p, sizeof(int));
                p += sizeof(int);
                block.data = p;
                docheader = (DOCHEADER *)p;
                p += block.ndata;
                if(block.ndata <= sizeof(DOCHEADER) || p  > end 
                        || docheader->size != block.ndata)
                {
                    FATAL_LOGGER(logger, "Invalid packet:%d cmd:%d at documents[%d] count:%d chunk[%d] ndata:%d size:%d p:%p end:%p", req->id, req->cmd, i, count, chunk->ndata, block.ndata, docheader->size, p, end); 
                    goto err_end;
                }
                else
                {
                    if((ibase_add_document(ibase, &block)) != 0)
                    {
                        FATAL_LOGGER(logger, "Add documents[%d][%d] failed, %s", i, docheader->globalid, strerror(errno));
                        goto err_end;
                    }
                }
                ++i;
            }
            resp.id = req->id;
            resp.cid = req->cid;
            resp.nodeid = req->nodeid;
            resp.cmd = req->cmd + 1;
            resp.status = IB_STATUS_OK;
            resp.length = 0;
            conn->push_chunk(conn, &resp, sizeof(IHEAD));
            return conn->over_session(conn);
        }
err_end:
        resp.status = IB_STATUS_ERR;
        resp.length = 0;
        conn->push_chunk(conn, &resp, sizeof(IHEAD));
        return conn->over_session(conn);
    }
    return -1;
}

/* indexd data handler */
int indexd_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    char *p = NULL, *end = NULL, *summary = NULL, *term = NULL;
    int ret = -1, n = 0, len = 0;
    IHEAD resp = {0}, *presp = NULL;
    IRES *res = NULL, xres = {0};
    IRECORD *records = NULL;
    struct timeval tv = {0};
    IQUERY *pquery = NULL;
    HTTP_REQ httpRQ = {0};
    CB_DATA *block = NULL;
    ICHUNK *ichunk = NULL;
    BTERM *bterm = NULL;
    IQSET *qset = NULL;
    IHEAD *req = NULL;

    if(conn && packet && (req = (IHEAD *)packet->data))
    {
        if(chunk && chunk->data && chunk->ndata > 0)
        {
            //fprintf(stdout, "%s::%d req->id:%lld req->cmd:%d req->length:%d\n", __FILE__, __LINE__, LL64(req->id), req->cmd, req->length);
            switch(req->cmd)
            {
                case IB_REQ_INDEX:
                    return indexd_index_handler(conn);
                    break;
                case IB_REQ_UPDATE:
                    return indexd_index_handler(conn);
                    break;
                case IB_REQ_QPARSE:
                    {
                        gettimeofday(&tv, NULL); 
                        conn->xids[8] = tv.tv_sec; conn->xids[9] = tv.tv_usec;
                        p = chunk->data;
                        end = p + chunk->ndata;
                        if(http_argv_parse(p, end, &httpRQ) == -1)goto err_end;
                        len = sizeof(IHEAD) + sizeof(IQUERY);
                        if((block = conn->mnewchunk(conn, len)))
                        {
                            pquery = (IQUERY *)(block->data + sizeof(IHEAD));
                            if(httpd_request_handler(conn, &httpRQ, pquery) < 0)
                            {
                                conn->freechunk(conn, block);
                                goto end;
                            }
                            else
                            {
                                presp = (IHEAD *)block->data;
                                presp->id = req->id;
                                presp->cid = req->cid;
                                presp->nodeid = req->nodeid;
                                presp->cmd = IB_RESP_QPARSE;
                                presp->status = IB_STATUS_OK;
                                presp->length = sizeof(IQUERY);
                                if(conn->send_chunk(conn, block, len) != 0)
                                    conn->freechunk(conn, block);
                            }
                            return 0;
                        }
                        else goto err_end;
                    }
                    break;
                case IB_REQ_QUERY:
                    {
                        if(chunk->ndata == sizeof(IQUERY))
                        {
                            pquery = (IQUERY *)chunk->data; 
                            /*
                            if(pquery->qfunc == IB_QFUNC_XQUERY) 
                                ichunk = ibase_xquery(ibase, pquery);
                            else 
                                ichunk = ibase_bquery(ibase, pquery);
                            */
                            if((ichunk = ibase_bquery(ibase, pquery)))
                            {
                                presp = &(ichunk->resp);
                                res = &(ichunk->res);
                                presp->id = req->id;
                                presp->cid = req->cid;
                                presp->nodeid = req->nodeid;
                                presp->cmd = IB_RESP_QUERY;
                                presp->status = IB_STATUS_OK;
                                presp->length = sizeof(IRES) + res->count * sizeof(IRECORD);
                                if(pquery->qid != req->id)
                                {
                                    FATAL_LOGGER(logger, "Invalid qid:%d to head->id:%d on remote[%s:%d] via %d", pquery->qid, req->id, conn->remote_ip, conn->remote_port, conn->fd);
                                    ret = -1;
                                }
                                else if(pquery->qid != res->qid)
                                {
                                    FATAL_LOGGER(logger, "Invalid qid:%d to res->qid:%d on remote[%s:%d] via %d", pquery->qid, res->qid, conn->remote_ip, conn->remote_port, conn->fd);
                                    ret = -1;
                                }
                                else ret = 0;
                                if(ret == 0)
                                {
                                    n = sizeof(IHEAD) + presp->length;
                                    conn->push_chunk(conn, (char *)ichunk, n);
                                    ibase_push_chunk(ibase, ichunk);
                                    return 0;
                                }
                                else resp.status = IB_STATUS_ERR;
                            }
                            else resp.status = IB_STATUS_OK;
                        }
                        else resp.status = IB_STATUS_ERR;
                        resp.id = req->id;
                        resp.cid = req->cid;
                        resp.nodeid = req->nodeid;
                        resp.cmd = req->cmd + 1;
                        resp.length = sizeof(IRES);
                        xres.qid = pquery->qid;
                        xres.doctotal = ibase->state->dtotal;
                        conn->push_chunk(conn, &resp, sizeof(IHEAD));
                        conn->push_chunk(conn, &xres, sizeof(IRES));
                        return conn->over_session(conn);
                    }
                    break;
                case IB_REQ_SUMMARY:
                    {
                        if(chunk->ndata >= sizeof(IQSET) && (qset = (IQSET *)chunk->data) 
                                && (chunk->ndata >= (qset->count * sizeof(IRECORD) + sizeof(IQSET))))
                        {
                            records = (IRECORD *)(chunk->data + sizeof(IQSET));
                            res = &(qset->res);
                            len = sizeof(IHEAD) + IB_SUMMARY_MAX * qset->count;
                            if((block = conn->newchunk(conn, len)))
                            {
                                summary = block->data + sizeof(IHEAD);
                                if((n = ibase_read_summary(ibase, qset, records, summary, 
                                                highlight_start, highlight_end)) > 0)
                                {
                                    presp = (IHEAD *)block->data;
                                    presp->id = req->id;
                                    presp->cid = req->cid;
                                    presp->nodeid = req->nodeid;
                                    presp->cmd = IB_RESP_SUMMARY;
                                    presp->status = IB_STATUS_OK;
                                    presp->length = n;
                                    if(conn->send_chunk(conn, block, n+sizeof(IHEAD)) != 0)
                                        conn->freechunk(conn, block);
                                }
                                else
                                {
                                    conn->freechunk(conn, block);
                                    goto err_end;
                                }
                                return 0;
                            }
                        }
                    }
                    break;
                case IB_REQ_UPDATE_BTERM:
                    {
                        p = chunk->data;
                        end = (chunk->data + chunk->ndata);
                        while(p < end)
                        {
                            bterm = (BTERM *)p;
                            p += sizeof(BTERM);
                            term = p;
                            p += bterm->len;
                            ibase_update_bterm(ibase, bterm, term);
                        }
                        goto end;
                    }
                    break;
                default:
                    break;
            }

end:
            resp.id = req->id;
            resp.cid = req->cid;
            resp.nodeid = req->nodeid;
            resp.cmd = req->cmd + 1;
            resp.status = IB_STATUS_OK;
            resp.length = 0;
            conn->push_chunk(conn, &resp, sizeof(IHEAD));
            return conn->over_session(conn);
        }
err_end:
        resp.id = req->id;
        resp.cid = req->cid;
        resp.nodeid = req->nodeid;
        resp.status = IB_STATUS_ERR;
        resp.cmd = req->cmd + 1;
        resp.length = 0;
        return conn->push_chunk(conn, &resp, sizeof(IHEAD));
    }
    return -1;
}

/* indexd error handler */
int indexd_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    if(conn)
    {
	    //fprintf(stdout, "%s::%d ERROR\n", __FILE__, __LINE__);
        conn->over_cstate(conn);
        return conn->over(conn);
    }
    return -1;
}

/* indexd timeout handler */
int indexd_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    if(conn)
    {
	    //fprintf(stdout, "%s::%d TIMEOUT\n", __FILE__, __LINE__);
        conn->over_cstate(conn);
        return conn->over(conn);
    }
    return -1;
}

/* indexd transaction handler */
int indexd_trans_handler(CONN *conn, int tid)
{
    if(conn && tid >= 0)
    {
        return 0;
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

/* request handler */
int httpd_request_handler(CONN *conn, HTTP_REQ *httpRQ, IQUERY *query)
{
    char *p = NULL, *query_str = NULL, *not_str = "", *display = NULL, *range_filter = NULL,
         *hitscale = NULL, *slevel_filter = NULL, *catfilter = NULL, *catgroup = NULL, 
         *multicat = NULL, *catblock = NULL, *xup = NULL, *xdown = NULL, *range_from = NULL, 
         *range_to = NULL, *bitfields = NULL, *last = NULL, *in = NULL, *in_ptr = 0;
    int ret = -1, n = 0, i = 0, k = 0, id = 0, phrase = 0, booland = 0, fieldsfilter = -1, 
        orderby = 0, order = 0, field_id = 0, int_index_from = 0, int_index_to = 0, 
        long_index_from = 0, long_index_to = 0, double_index_from = 0, double_index_to = 0, 
        xint = 0, op = 0, need_rank = 0, usecs = 0, flag =0;
    int64_t xlong = 0;
    struct timeval tv = {0};
    double xdouble = 0.0;

    if(httpRQ && query)
    {
        for(i = 0; i < httpRQ->nargvs; i++)
        {
            if(httpRQ->argvs[i].nk > 0 && (n = httpRQ->argvs[i].k) > 0
                    && (p = (httpRQ->line + n)))
            {
                if((id = (mtrie_get(argvmap, p, httpRQ->argvs[i].nk) - 1)) >= 0 
                        && httpRQ->argvs[i].nv > 0
                        && (n = httpRQ->argvs[i].v) > 0
                        && (p = (httpRQ->line + n)))
                {
                    switch(id)
                    {
                        case E_ARGV_QUERY :
                            query_str = p;
                            break;
                        case E_ARGV_FIELDFILTER :
                            fieldsfilter = atoi(p);
                            break;
                        case E_ARGV_CATFILTER:
                            catfilter = p;
                            break;
                        case E_ARGV_MULTICAT:
                            multicat = p;
                            break;
                        case E_ARGV_CATBLOCK:
                            catblock = p;
                            break;
                        case E_ARGV_ORDERBY:
                            orderby = atoi(p);
                            break;
                        case E_ARGV_FROM :
                            query->from = atoi(p);
                            break;
                        case E_ARGV_COUNT:
                            query->count = atoi(p);
                            break;
                        case E_ARGV_ORDER:
                            order = atoi(p);
                            break;
                        case E_ARGV_CATGROUP:
                            catgroup = p;
                            break;
                        case E_ARGV_DISPLAY:
                            display = p;
                            break;
                        case E_ARGV_RANGEFILTER:
                            range_filter = p;
                            break;
                        case E_ARGV_BITFIELDS:
                            bitfields = p;
                            break;
                        case E_ARGV_QWEIGHT:
                            query->qweight = atoi(p);
                            break;
                        case E_ARGV_PHRASE:
                            phrase = atoi(p);
                            break;
                        case E_ARGV_NOT:
                            not_str = p;
                            break;
                        case E_ARGV_RANK:
                            need_rank = atoi(p);
                            break;
                        case E_ARGV_SLEVEL:
                            slevel_filter = p;
                            break;
                        case E_ARGV_XUP:
                            xup = p;
                            break;
                        case E_ARGV_XDOWN:
                            xdown = p;
                            break;
                        case E_ARGV_QFUNC:
                            query->qfunc = atoi(p);
                            break;
                        case E_ARGV_QFHITS:
                            query->qfhits = atoi(p);
                            break;
                        case E_ARGV_REL:
                            if(atoi(p)) query->flag |= IB_QUERY_RELEVANCE;
                            break;
                        case E_ARGV_HITSCALE:
                            hitscale = p;
                            break;
                        case E_ARGV_OP:
                            op = atoi(p);
                            break;
                        case E_ARGV_BOOLAND:
                            booland = atoi(p);
                            break;
                        case E_ARGV_BHITS:
                            query->base_hits = atoi(p);
                            break;
                        case E_ARGV_BFHITS:
                            query->base_fhits = atoi(p);
                            break;
                        case E_ARGV_BPHRASE:
                            query->base_phrase = atoi(p);
                            break;
                        case E_ARGV_BNTERM:
                            query->base_nterm = atoi(p);
                            break;
                        case E_ARGV_BXCATUP:
                            query->base_xcatup = atoi(p);
                            break;
                        case E_ARGV_BXCATDOWN:
                            query->base_xcatdown = atoi(p);
                            break;
                        case E_ARGV_BRANK:
                            query->base_rank = atoi(p);
                            break;
                        case E_ARGV_NTOP:
                            query->ntop = atoi(p);
                            break;
                        case E_ARGV_IN:
                            in = p;
                            break;
                        default :
                            break;
                    }
                }
            }
        }
        /* clear cache */
        if(op == OP_CLEAR_CACHE) 
        {
            query->flag |= IB_CLEAR_CACHE;
            if(query_str == NULL) return op;
        }
        /* check query_str */
        if(query_str == NULL) return -1;
        gettimeofday(&tv, NULL);
        if(conn->xids[8] > 0 && conn->xids[9] > 0)
        {
            usecs = (off_t)tv.tv_sec * (off_t)1000000 + (off_t)tv.tv_usec 
                - ((off_t) conn->xids[8] * (off_t)1000000 + (off_t)conn->xids[9]);
            conn->xids[8] = 0;
            conn->xids[9] = 0;
        }
        //query->ntop = query->from + query->count;
        if(query->ntop <= 0 || query->ntop > IB_TOPK_NUM) query->ntop = IB_TOPK_NUM;
        //httpd_page_max * httpd_page_num;
        /* filter */
        if(fieldsfilter > 0) query->fields_filter = fieldsfilter;
        /* slevel */
        if((p = slevel_filter))
        {
            i = 0;
            while(*p != '\0') 
            {
                last = p;
                while(*p == 0x20 || *p == '\t' || *p == ',' || *p == ';')++p;
                if(*p != '\0' && (i=atoi(p)) >= 0 && i < IB_SLEVEL_MAX) 
                {
                    query->slevel_filter[i] = 1;
                }
                while(*p >= '0' && *p <= '9')++p;
                ++i;
                if(p == last)break;
            }
        }
        /*catfilter */
        if((p = catfilter))
        {
            i = 0;
            while(*p != '\0') 
            {
                last = p;
                while(*p == 0x20 || *p == '\t' || *p == ',' || *p == ';')++p;
                if(*p >= '0' && *p <= '9' && (i = atoi(p)) >= 0 && i < IB_CATEGORY_MAX) 
                {
                    query->category_filter |= (int64_t)1 << i;
                }
                while(*p >= '0' && *p <= '9')++p;
                ++i;
                if(p == last)break;
            }
        }
        /* multicat */
        if((p = multicat))
        {
            i = 0;
            while(*p != '\0') 
            {
                last = p;
                while(*p == 0x20 || *p == '\t' || *p == ',' || *p == ';')++p;
                if(*p >= '0' && *p <= '9' && (i = atoi(p)) >= 0 && i < IB_CATEGORY_MAX) 
                {
                    query->multicat_filter |= (int64_t)1 << i;
                }
                while(*p >= '0' && *p <= '9')++p;
                ++i;
                if(p == last)break;
            }
        }
        /* catgroup */
        if((p = catgroup))
        {
            i = 0;
            while(*p != '\0') 
            {
                last = p;
                while(*p == 0x20 || *p == '\t' || *p == ',' || *p == ';')++p;
                if(*p >= '0' && *p <= '9' && (i = atoi(p)) >= 0 && i < IB_CATEGORY_MAX) 
                {
                    query->catgroup_filter |= (int64_t)1 << i;
                    //fprintf(stdout, "catgroup:%d\n", i);
                }
                while(*p >= '0' && *p <= '9')++p;
                ++i;
                if(p == last)break;
            }
        }
        /*catblock */
        if((p = catblock))
        {
            i = 0;
            while(*p != '\0') 
            {
                last = p;
                while(*p == 0x20 || *p == '\t' || *p == ',' || *p == ';')++p;
                if(*p >= '0' && *p <= '9' && (i = atoi(p)) >= 0 && i < IB_CATEGORY_MAX) 
                {
                    query->catblock_filter |= (int64_t)1 << i;
                    //fprintf(stdout, "catblock:%d\n", i);
                }
                while(*p >= '0' && *p <= '9')++p;
                ++i;
                if(p == last)break;
            }
        }
        /* bitxcat up */
        if((p = xup))
        {
            i = 0;
            while(*p != '\0') 
            {
                last = p;
                while(*p == 0x20 || *p == '\t' || *p == ',' || *p == ';')++p;
                if(*p >= '0' && *p <= '9' && (i = atoi(p)) >= 0 && i < IB_CATEGORY_MAX) 
                {
                    query->bitxcat_up |= (int64_t)1 << i;
                    //fprintf(stdout, "xup:%d\n", i);
                }
                while(*p >= '0' && *p <= '9')++p;
                ++i;
                if(p == last)break;
            }
        }
        /* bitxcat down */
        if((p = xdown))

        {
            i = 0;
            while(*p != '\0') 
            {
                last = p;
                while(*p == 0x20 || *p == '\t' || *p == ',' || *p == ';')++p;
                if(*p >= '0' && *p <= '9' && (i = atoi(p)) >= 0 && i < IB_CATEGORY_MAX) 
                {
                    query->bitxcat_down |= (int64_t)1 << i;
                    //fprintf(stdout, "xdown:%d\n", i);
                }
                while(*p >= '0' && *p <= '9')++p;
                ++i;
                if(p == last)break;
            }
        }
        int_index_from = ibase->state->int_index_from;
        int_index_to = int_index_from + ibase->state->int_index_fields_num;
        long_index_from = ibase->state->long_index_from;
        long_index_to = long_index_from + ibase->state->long_index_fields_num;
        double_index_from = ibase->state->double_index_from;
        double_index_to = double_index_from + ibase->state->double_index_fields_num;
        if((p = in))
        {
            while(*p != '\0')
            {
                last = p;
                while(*p == 0x20)++p;
                if(*p != '\0')field_id = atoi(p);
                while(*p != '[' && *p != '\0')++p;
                if(*p != '\0')++p;
                while(*p != '\0' && *p != ']')
                {
                    while(*p == 0x20)++p;
                    if(*p >= '0' && *p <= '9') in_ptr = p;
                    while((*p >= '0' && *p <= '9') || *p == '.')++p;
                    while(*p == 0x20)++p;
                    if(*p == ',' || *p == ';')++p;
                    if(field_id >= int_index_from && field_id < int_index_to) 
                    {
                        k = query->in_int_num++;
                        query->in_int_fieldid = field_id;
                        query->in_int_list[k] = atoi(in_ptr);
                        while(k > 0 && query->in_int_list[k] < query->in_int_list[k-1])
                        {
                            xint = query->in_int_list[k-1];
                            query->in_int_list[k-1] = query->in_int_list[k];
                            query->in_int_list[k] = xint;
                            --k;
                        }
                    }
                    else if(field_id >= long_index_from && field_id < long_index_to) 
                    {
                        k = query->in_long_num++;
                        query->in_long_fieldid = field_id;
                        query->in_long_list[k] = atoll(in_ptr);
                        while(k > 0 && query->in_long_list[k] < query->in_long_list[k-1])
                        {
                            xlong = query->in_long_list[k-1];
                            query->in_long_list[k-1] = query->in_long_list[k];
                            query->in_long_list[k] = xlong;
                            --k;
                        }
                    }
                    else if(field_id >= double_index_from && field_id < double_index_to)
                    {
                        k = query->in_double_num++;
                        query->in_double_fieldid = field_id;
                        query->in_double_list[k] = atof(in_ptr);
                        while(k > 0 && query->in_double_list[k] < query->in_double_list[k-1])
                        {
                            xdouble = query->in_double_list[k-1];
                            query->in_double_list[k-1] = query->in_double_list[k];
                            query->in_double_list[k] = xdouble;
                            --k;
                        }
                    }
                }
                if(p == last)break;
            }
            /*
            for(i = 0; i < query->in_int_num; i++)
            {
                fprintf(stdout, "%d:%d\n", i, query->in_int_list[i]);
            }
            fprintf(stdout, "%s::%d in:%s in_int_field:%d/%d in_long_field:%d/%d in_double_feild:%d/%d\n", __FILE__, __LINE__, in, query->in_int_fieldid, query->in_int_num, query->in_long_fieldid, query->in_long_num, query->in_double_fieldid, query->in_double_num);
            */
        }
        /* display */
        if((p = display))
        {
            while(*p != '\0')
            {
                last = p;
                if(*p == 0x20 || *p == ',' || *p == ';')++p;
                if(*p != '\0') i = atoi(p);
                else break;
                while(*p >= '0' && *p <= '9')++p;
                if(i >= 0 && i  < IB_FIELDS_MAX)
                {
                    query->display[i].flag |= IB_IS_DISPLAY;
                    if(*p == '#' || *p =='|' || *p == '$' || *p == '@' || *p == '}')
                    {
                        query->display[i].flag |= IB_IS_HIGHLIGHT;
                        ++p;
                    }
                }
                if(p == last)break;
            }
        }
        /* rank */
        if(need_rank) query->flag |= IB_QUERY_RANK;
        /* order/order by/range */
        if(order < 0) query->flag |= IB_QUERY_RSORT;
        else query->flag |= IB_QUERY_SORT;
                if(orderby >= int_index_from && orderby < int_index_to)
        {
            query->int_order_field = orderby;
        }
        else if(orderby >= long_index_from && orderby < long_index_to)
        {
            query->long_order_field = orderby;
        }
        else if(orderby >= double_index_from && orderby < double_index_to)
        {
            query->double_order_field = orderby;
        }
        //DEBUG_LOGGER(logger, "int_from:%d int_to:%d long_from:%d long_to:%d  double_from:%d double_to:%d orderby:%d/%d", int_index_from, int_index_to, long_index_from, long_index_to, double_index_from, double_index_to, orderby, query->double_order_field);
        /* range */
        if((p = range_filter))
        {
            while(*p != '\0')
            {
                last = p;
                range_from = NULL;
                range_to = NULL;
                while(*p == 0x20)++p;
                if(*p != '\0')field_id = atoi(p);
                while(*p != '[' && *p != '\0')++p;
                if(*p != '\0')++p;
                while(*p == 0x20)++p;
                if(*p >= '0' && *p <= '9') range_from = p;
                while((*p >= '0' && *p <= '9') || *p == '.')++p;
                while(*p == 0x20)++p;
                if(*p == ',' || *p == ';')++p;
                while(*p == 0x20)++p;
                if(*p >= '0' && *p <= '9') range_to = p;
                while(*p != ']' && *p != '\0')++p;
                while(*p != ',' && *p != ';' && *p != '\0')++p;
                if(*p != '\0')++p;
                if(field_id >= int_index_from && field_id < int_index_to 
                        && query->int_range_count < IB_INT_INDEX_MAX)
                {
                    k = query->int_range_count++;
                    query->int_range_list[k].field_id = field_id;
                    if(range_from) 
                    {
                        query->int_range_list[k].flag |= IB_RANGE_FROM;
                        query->int_range_list[k].from = atoi(range_from);
                    }
                    if(range_to) 
                    {
                        query->int_range_list[k].flag |= IB_RANGE_TO;
                        query->int_range_list[k].to = atoi(range_to);
                    }
                    if(range_from && range_to && query->int_range_list[k].to 
                            < query->int_range_list[k].from)
                    {
                        xint = query->int_range_list[k].to;
                        query->int_range_list[k].to = query->int_range_list[k].from;
                        query->int_range_list[k].from = xint;
                    }
                    //fprintf(stdout, "from:%s to:%s\n", range_from, range_to);
                }
                else if(field_id >= long_index_from && field_id < long_index_to 
                        && query->long_range_count < IB_LONG_INDEX_MAX)
                {
                    k = query->long_range_count++;
                    query->long_range_list[k].field_id = field_id;
                    if(range_from) 
                    {
                        query->long_range_list[k].flag |= IB_RANGE_FROM;
                        query->long_range_list[k].from = atoll(range_from);
                    }
                    if(range_to) 
                    {
                        query->long_range_list[k].flag |= IB_RANGE_TO;
                        query->long_range_list[k].to = atoll(range_to);
                    }
                    if(range_from && range_to && query->long_range_list[k].to 
                            < query->long_range_list[k].from)
                    {
                        xlong = query->long_range_list[k].to;
                        query->long_range_list[k].to = query->long_range_list[k].from;
                        query->long_range_list[k].from = xlong;
                    }
                    //fprlongf(stdout, "from:%s to:%s\n", range_from, range_to);
                }
                else if(field_id >= double_index_from && field_id < double_index_to)
                {
                    k = query->double_range_count++;
                    query->double_range_list[k].field_id = field_id;
                    if(range_from) 
                    {
                        query->double_range_list[k].flag |= IB_RANGE_FROM;
                        query->double_range_list[k].from = atof(range_from);
                    }
                    if(range_to) 
                    {
                        query->double_range_list[k].flag |= IB_RANGE_TO;
                        query->double_range_list[k].to = atof(range_to);
                    }
                    if(range_from && range_to && query->double_range_list[k].to 
                            < query->double_range_list[k].from)
                    {
                        xdouble = query->double_range_list[k].to;
                        query->double_range_list[k].to = query->double_range_list[k].from;
                        query->double_range_list[k].from = xdouble;
                    }
                }
                if(p == last)break;
            }
            /*
            for(i = 0; i < query->int_range_count; i++)
            {
                fprintf(stdout, "%d:[from:%d,to:%d][%d]\n", i, query->int_range_list[i].from, query->int_range_list[i].to, query->int_range_list[i].flag);
            }
            */
        }
        if((p = bitfields))
        {
            while(*p != '\0')
            {
                last = p;
                while(*p == 0x20)++p;
                if(*p != '\0')field_id = atoi(p);
                while(*p != '[' && *p != '{' && *p != '\0')++p;
                if(*p == '[') flag = IB_BITFIELDS_FILTER;
                else flag = IB_BITFIELDS_BLOCK;
                ++p;
                while(*p == 0x20)++p;
                xint  = 0;
                xlong = 0;
                //fprintf(stdout, "%s::%d fieldid:%d  int_index_from:%d/%d\n", __FILE__, __LINE__, field_id, int_index_from, int_index_to);
                if(field_id >= int_index_from && field_id < int_index_to 
                        && query->int_bits_count < IB_INT_INDEX_MAX)
                {
                    k = query->int_bits_count++;
                    query->int_bits_list[k].field_id = field_id;
                    while(*p != ']' && *p != '}' && *p != '\0')
                    {
                        if(*p >= '0' && *p <= '9' && (i = atoi(p)) >= 0 && i < IB_INT_BITS_MAX) 
                        {
                            xint |= 1 << i;
                        }
                        while(*p >= '0' && *p <= '9')++p;
                        while(*p == 0x20 || *p == ',' || *p == ';')++p;
                    }
                    query->int_bits_list[k].bits = xint;
                    query->int_bits_list[k].flag = flag;
                    DEBUG_LOGGER(logger, "field:%d int_index:%d/%d bits:%d", field_id, int_index_from, int_index_to, xint);
                }
                else if(field_id >= long_index_from && field_id < long_index_to 
                        && query->long_bits_count < IB_LONG_INDEX_MAX)
                {
                    k = query->long_bits_count++;
                    query->long_bits_list[k].field_id = field_id;
                    while(*p != ']' && *p != '}' && *p != '\0')
                    {
                        if(*p >= '0' && *p <= '9' && (i = atoi(p)) >= 0 && i < IB_LONG_BITS_MAX) 
                        {
                            xlong |= (int64_t)1 << i;
                        }
                        while(*p >= '0' && *p <= '9')++p;
                        while(*p == 0x20 || *p == ',' || *p == ';')++p;
                    }
                    query->long_bits_list[k].bits = xlong;
                    query->long_bits_list[k].flag = flag;
                    DEBUG_LOGGER(logger, "field:%d long_index:%d/%d long_bits:%lld", field_id, long_index_from, long_index_to, IBLL(xlong));
                }
                else
                {
                    while(*p != ']' && *p != '}' && *p != '\0')++p;
                }
                while(*p == ']' || *p == '}' || *p == ',' || *p == ';' || *p == 0x20)++p;
                if(p == last)break;
            }
        }
        /* hitscale */
        if((p = hitscale))
        {
           i = 0;
           while(*p != '\0') 
           {
               last = p;
               while(*p == 0x20 || *p == '\t' || *p == ',' || *p == ';')++p;
               query->hitscale[i] = atoi(p);
               while(*p >= '0' && *p <= '9')++p;
                ++i;
                if(p == last)break;
           }
        }
        if(phrase > 0) query->flag |= IB_QUERY_PHRASE;
        if(booland > 0) query->flag |= IB_QUERY_BOOLAND;
        ACCESS_LOGGER(logger, "ready for query:%s not:%s from:%d count:%d fieldsfilter:%d catfilter:%lld multicat:%lld catgroup:%lld catblock:%lld orderby:%d order:%d qfhits:%d base_hits:%d base_fhits:%d base_phrase:%d base_nterm:%d base_xcatup:%lld base_xcatdown:%lld base_rank:%d int_range_count:%d long_range_count:%d double_range_count:%d usec_used:%d remote[%s:%d -> %d]", query_str, not_str, query->from, query->count, fieldsfilter, LL64(query->category_filter), LL64(query->multicat_filter), LL64(query->catgroup_filter), LL64(query->catblock_filter), orderby, order, query->qfhits, query->base_hits, query->base_fhits, query->base_phrase, query->base_nterm, LL64(query->base_xcatup), LL64(query->base_xcatdown), query->base_rank, query->int_range_count, query->long_range_count, query->double_range_count, usecs, conn->remote_ip, conn->remote_port, conn->fd);
        //fprintf(stdout, "%s::%d query:%s OK\n", __FILE__, __LINE__, query_str);
        ret = ibase_qparser(ibase, query_str, not_str, query);
    }
    return ret;
}

/* httpd query handler */
int httpd_query_handler(CONN *conn, IQUERY *query)
{
    char buf[HTTP_BUF_SIZE], *summary = NULL, *p = NULL;
    int ret = -1, x = 0, n = 0;
    IRECORD *records = NULL;
    CB_DATA *block = NULL;
    ICHUNK *ichunk = NULL;
    IQSET qset = {0};
    IRES *res = NULL;

    if(conn)
    {
        //TIMER_INIT(timer);
        if(query && query->from < query->ntop && query->count > 0)
        {
            //fprintf(stdout, "%s::%d from:%d count:%d ntop:%d\n", __FILE__, __LINE__, query->from, query->count, query->ntop);
            //if(query->qfunc == IB_QFUNC_QUERY) ichunk = ibase_query(ibase, query);
            /*
            if(query->qfunc == IB_QFUNC_XQUERY) ichunk = ibase_xquery(ibase, query);
            else ichunk = ibase_bquery(ibase, query);
            */
            if((ichunk = ibase_bquery(ibase, query)))
            {
                res     = &(ichunk->res);
                records  = ichunk->records;
                qset.count = query->count;
                if((query->from + query->count) > res->count)
                    qset.count = res->count - query->from;
                x = query->from;
                /*
                   n = sprintf(line, "count:%d total:%d io_time:%u sort_time:%d time:%lld\n", 
                   res->count, res->total, res->io_time, res->sort_time, PT_LU_USEC(timer));
                   p = buf;
                   p += sprintf(p, "HTTP/1.0 200 OK\r\nContent-Type:text/html;charset=%s\r\n"
                   "Content-Length: %d\r\nConnection:Keep-Alive\r\n\r\n%s",
                   http_default_charset, n, line);
                   conn->push_chunk(conn, buf, (p - buf));
                   ret = 0;
                   */
                if(qset.count > 0 && (n = (IB_SUMMARY_MAX * qset.count)) > 0 
                        && (block = conn->newchunk(conn, n)))
                {
                    summary = block->data; 
                    qset.nqterms = query->nqterms;
                    memcpy(qset.qterms, query->qterms, query->nqterms * sizeof(QTERM));
                    memcpy(qset.displaylist, query->display, sizeof(IDISPLAY) * IB_FIELDS_MAX); 
                    memcpy(&(qset.res), res, sizeof(IRES));
                    records = &(ichunk->records[x]);
                    if((n = ibase_read_summary(ibase, &qset, records, summary, 
                                    highlight_start, highlight_end)) > 0)
                    {
                        //fprintf(stdout, "%s::%d ntop:%d from:%d count:%d n:%d summary:%s\n", __FILE__, __LINE__, query->ntop, query->from, query->count, n, summary);
                        p = buf;
                        p += sprintf(p, "HTTP/1.0 200 OK\r\nContent-Type:text/html;charset=%s\r\n"
                                "Content-Length: %d\r\nConnection:Keep-Alive\r\n\r\n", 
                                http_default_charset, n);
                        conn->push_chunk(conn, buf, (p - buf));
                        if((ret = conn->send_chunk(conn, block, n)) != 0)
                            conn->freechunk(conn,block);
                    }
                    else
                    {
                        conn->freechunk(conn,block);
                        ret = -1;
                    }
                }
                ibase_push_chunk(ibase, ichunk);
                if(ret == -1) goto err_end;
                return 0;
            }
        }
err_end:
        ret = conn->push_chunk(conn, HTTP_BAD_REQUEST, strlen(HTTP_BAD_REQUEST));
    }
    return ret;
}

/* packet handler */
int httpd_packet_handler(CONN *conn, CB_DATA *packet)
{
    char buf[HTTP_BUF_SIZE], file[HTTP_PATH_MAX], *p = NULL, *end = NULL;
    HTTP_REQ http_req = {0};
    struct timeval tv = {0};
    int ret = -1, n = 0;
    struct stat st = {0};
    IQUERY query;
    
    if(conn)
    {
        //TIMER_INIT(timer);
        p = packet->data;
        end = packet->data + packet->ndata;
        if(http_request_parse(p, end, &http_req, http_headers_map) == -1) goto err_end;
        if(http_req.reqid == HTTP_GET)
        {
            gettimeofday(&tv, NULL); conn->xids[8] = tv.tv_sec; conn->xids[9] = tv.tv_usec;
            memset(&query, 0, sizeof(IQUERY));
            if(http_req.nargvs > 0 && httpd_request_handler(conn, &http_req, &query) >= 0) 
            {
                return httpd_query_handler(conn, &query);
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
                    p += sprintf(p, "Connection:Keep-Alive\r\n");
                    p += sprintf(p, "\r\n");
                    conn->push_chunk(conn, buf, (p - buf));
                    return conn->push_chunk(conn, httpd_index_html_code, nhttpd_index_html_code);
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
                            return conn->push_chunk(conn, HTTP_NO_CONTENT, strlen(HTTP_NO_CONTENT));
                        }
                        else if((n = http_req.headers[HEAD_REQ_IF_MODIFIED_SINCE]) > 0
                                && str2time(http_req.hlines + n) == st.st_mtime)
                        {
                            return conn->push_chunk(conn, HTTP_NOT_MODIFIED, strlen(HTTP_NOT_MODIFIED));
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
                            return conn->push_file(conn, file, 0, st.st_size);
                        }
                    }
                    else
                    {
                        return conn->push_chunk(conn, HTTP_NOT_FOUND, strlen(HTTP_NOT_FOUND));
                    }
                }
                else
                {
                    return conn->push_chunk(conn, HTTP_BAD_REQUEST, strlen(HTTP_BAD_REQUEST));
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
    }
    return ret;
}

/*  data handler */
int httpd_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    char *p = NULL, *s = NULL, *end = NULL, line[HTTP_LINE_SIZE]; 
    HTTP_REQ httpRQ = {0}, *http_req = NULL;
    int ret = -1, n = 0, i = 0, len = 0;
    int64_t list[X_KEY_MAX]; 
    struct timeval tv = {0};
    CB_DATA *block = NULL;
    IQUERY query;

    if(conn && packet && cache && chunk && chunk->ndata > 0)
    {
        if((http_req = (HTTP_REQ *)cache->data))
        {
            if(http_req->reqid == HTTP_POST)
            {
                gettimeofday(&tv, NULL); conn->xids[8] = tv.tv_sec; conn->xids[9] = tv.tv_usec;
                p = chunk->data;
                end = chunk->data + chunk->ndata;
                if(chunk->ndata > 9 && strncmp(p, "op=2", 4) == 0 
                        && strncmp(p+4, "&key=", 5) == 0)
                {
                    s = p + 9;  
                    i = 0;
                    while(s < end && i < X_KEY_MAX)
                    {
                        while(s < end && *s == 0x20)++s;
                        if((list[i] = (int64_t)atoll(s)))
                        {
                            ++i;
                        }
                        while(*s != ',' && *s != ';' && s < end)++s;
                        ++s;
                    }
                    if((n = i) > 0 && (len = ibase_bound_items(ibase, n)) > 0)
                    {
                        if((block = conn->newchunk(conn, len)))
                        {
                            p = block->data;
                            if((len = ibase_read_items(ibase, list, n, p)) > 0)
                            {
                                n = sprintf(line, "HTTP/1.0 200 OK\r\nContent-Type:text/html;"
                                        "charset=%s\r\nContent-Length: %d\r\n\r\n", 
                                        http_default_charset, len);
                                conn->push_chunk(conn, line, n);
                                if(conn->send_chunk(conn, block, len) == 0)
                                    return 0;
                            }
                            conn->freechunk(conn,block);
                        }
                    }
                    return conn->push_chunk(conn, HTTP_NOT_FOUND, strlen(HTTP_NOT_FOUND));
                }
                //fprintf(stdout, "%s::%d request:%s\n", __FILE__, __LINE__, p);
                if(http_argv_parse(p, end, &httpRQ) == -1)goto err_end;
                //fprintf(stdout, "%s::%d OK\n", __FILE__, __LINE__);
                memset(&query, 0, sizeof(IQUERY));
                if(httpd_request_handler(conn, &httpRQ, &query) < 0) goto err_end;
                //fprintf(stdout, "%s::%d OK\n", __FILE__, __LINE__);
                if(query.flag & IB_CLEAR_CACHE)
                {
                    n = sprintf(line, "HTTP/1.0 200 OK\r\nContent-Type:text/html;charset=%s\r\n"
                                "Content-Length: 0\r\n\r\n", 
                                http_default_charset);
                    return conn->push_chunk(conn, line, n);
                }
                else
                {
                    /* query parser */
                    return httpd_query_handler(conn, &query);
                }
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
        conn->over_cstate(conn);
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

/* Initialize from ini file */
int sbase_initialize(SBASE *sbase, char *conf)
{
    char *s = NULL, *p = NULL, *dir = NULL, *charset = NULL, *rules = NULL, line[256];
    int i = 0, n = 0, used_for = -1, mmsource_status = 0, pidfd = 0;

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
    /* argvmap */
    if((argvmap = mtrie_init()) == NULL)_exit(-1);
    else
    {
        for(i = 0; i < E_ARGV_NUM; i++)
        {
            mtrie_add(argvmap, e_argvs[i], strlen(e_argvs[i]), i+1);
        }
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
    /* IBASE */
    used_for = iniparser_getint(dict, "IBASE:used_for", 0);
    mmsource_status = iniparser_getint(dict, "IBASE:mmsource_status", 1);
    if((dir = iniparser_getstr(dict, "IBASE:basedir")) == NULL || (ibase = ibase_init()) == NULL 
            || ibase->set_basedir(ibase, dir, used_for, mmsource_status) != 0)
    {
        fprintf(stderr, "Initialize ibase failed, %s", strerror(errno));
        _exit(-1);
    }
    if((p = iniparser_getstr(dict, "IBASE:dict_file")) 
            && (charset = iniparser_getstr(dict, "IBASE:dict_charset")))
    {
        rules = iniparser_getstr(dict, "IBASE:dictrules");
        ibase_set_dict(ibase, charset, p, rules);
    }
    LOGGER_ROTATE_INIT(logger, iniparser_getstr(dict, "IBASE:query_log"), LOG_ROTATE_DAY);
    LOGGER_SET_LEVEL(logger, iniparser_getint(dict, "IBASE:query_log_level", 0));
    ibase->set_index_status(ibase, iniparser_getint(dict, "IBASE:index_status", 0));
    ibase->set_phrase_status(ibase, iniparser_getint(dict, "IBASE:phrase_status", 0));
    ibase->set_compression_status(ibase, iniparser_getint(dict, "IBASE:compression_status", 0));
    ibase->set_log_level(ibase, iniparser_getint(dict, "IBASE:log_level", 0));
    if((p = iniparser_getstr(dict, "IBASE:highlight_start"))) highlight_start = p;
    if((p = iniparser_getstr(dict, "IBASE:highlight_end"))) highlight_end = p;
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
    /* QUERYD */
    if((queryd = service_init()) == NULL)
    {
        fprintf(stderr, "Initialize service failed, %s", strerror(errno));
        _exit(-1);
    }
    queryd->family = iniparser_getint(dict, "QUERYD:inet_family", AF_INET);
    queryd->sock_type = iniparser_getint(dict, "QUERYD:socket_type", SOCK_STREAM);
    queryd->ip = iniparser_getstr(dict, "QUERYD:service_ip");
    queryd->port = iniparser_getint(dict, "QUERYD:service_port", 3927);
    queryd->working_mode = iniparser_getint(dict, "QUERYD:working_mode", WORKING_PROC);
    queryd->service_type = iniparser_getint(dict, "QUERYD:service_type", S_SERVICE);
    queryd->service_name = iniparser_getstr(dict, "QUERYD:service_name");
    queryd->nprocthreads = iniparser_getint(dict, "QUERYD:nprocthreads", 1);
    queryd->ndaemons = iniparser_getint(dict, "QUERYD:ndaemons", 0);
    queryd->niodaemons = iniparser_getint(dict, "QUERYD:niodaemons", 1);
    queryd->use_cond_wait = iniparser_getint(dict, "QUERYD:use_cond_wait", 0);
    if(iniparser_getint(dict, "QUERYD:use_cpu_set", 0) > 0) queryd->flag |= SB_CPU_SET;
    if(iniparser_getint(dict, "QUERYD:event_lock", 0) > 0) queryd->flag |= SB_EVENT_LOCK;
    if(iniparser_getint(dict, "QUERYD:newconn_delay", 0) > 0) queryd->flag |= SB_NEWCONN_DELAY;
    if(iniparser_getint(dict, "QUERYD:tcp_nodelay", 0) > 0) queryd->flag |= SB_TCP_NODELAY;
    if(iniparser_getint(dict, "QUERYD:socket_linger", 0) > 0) queryd->flag |= SB_SO_LINGER;
    if(iniparser_getint(dict, "QUERYD:while_send", 0) > 0) queryd->flag |= SB_WHILE_SEND;
    if(iniparser_getint(dict, "QUERYD:log_thread", 0) > 0) queryd->flag |= SB_LOG_THREAD;
    if(iniparser_getint(dict, "QUERYD:use_outdaemon", 0) > 0) queryd->flag |= SB_USE_OUTDAEMON;
    if(iniparser_getint(dict, "QUERYD:use_evsig", 0) > 0) queryd->flag |= SB_USE_EVSIG;
    if(iniparser_getint(dict, "QUERYD:use_cond", 0) > 0) queryd->flag |= SB_USE_COND;
    if((n = iniparser_getint(dict, "QUERYD:sched_realtime", 0)) > 0) queryd->flag |= (n & (SB_SCHED_RR|SB_SCHED_FIFO));
    if((n = iniparser_getint(dict, "QUERYD:io_sleep", 0)) > 0) queryd->flag |= ((SB_IO_NANOSLEEP|SB_IO_USLEEP|SB_IO_SELECT) & n);
    queryd->nworking_tosleep = iniparser_getint(dict, "QUERYD:nworking_tosleep", SB_NWORKING_TOSLEEP);
    queryd->set_log(queryd, iniparser_getstr(dict, "QUERYD:logfile"));
    queryd->set_log_level(queryd, iniparser_getint(dict, "QUERYD:log_level", 0));
    queryd->session.packet_type = PACKET_CERTAIN_LENGTH;
    queryd->session.packet_length = sizeof(IHEAD);
    queryd->session.buffer_size = iniparser_getint(dict, "QUERYD:buffer_size", SB_BUF_SIZE);
    queryd->session.quick_handler = &indexd_quick_handler;
    queryd->session.packet_handler = &indexd_packet_handler;
    queryd->session.data_handler = &indexd_data_handler;
    queryd->session.timeout_handler = &indexd_timeout_handler;

    /* INDEXD */
    if((indexd = service_init()) == NULL)
    {
        fprintf(stderr, "Initialize service failed, %s", strerror(errno));
        _exit(-1);
    }
    indexd->family = iniparser_getint(dict, "INDEXD:inet_family", AF_INET);
    indexd->sock_type = iniparser_getint(dict, "INDEXD:socket_type", SOCK_STREAM);
    indexd->ip = iniparser_getstr(dict, "INDEXD:service_ip");
    indexd->port = iniparser_getint(dict, "INDEXD:service_port", 4936);
    indexd->working_mode = iniparser_getint(dict, "INDEXD:working_mode", WORKING_PROC);
    indexd->service_type = iniparser_getint(dict, "INDEXD:service_type", S_SERVICE);
    indexd->service_name = iniparser_getstr(dict, "INDEXD:service_name");
    indexd->nprocthreads = iniparser_getint(dict, "INDEXD:nprocthreads", 1);
    indexd->ndaemons = iniparser_getint(dict, "INDEXD:ndaemons", 0);
    indexd->niodaemons = iniparser_getint(dict, "INDEXD:niodaemons", 1);
    indexd->use_cond_wait = iniparser_getint(dict, "INDEXD:use_cond_wait", 0);
    if(iniparser_getint(dict, "INDEXD:use_cpu_set", 0) > 0) indexd->flag |= SB_CPU_SET;
    if(iniparser_getint(dict, "INDEXD:event_lock", 0) > 0) indexd->flag |= SB_EVENT_LOCK;
    if(iniparser_getint(dict, "INDEXD:newconn_delay", 0) > 0) indexd->flag |= SB_NEWCONN_DELAY;
    if(iniparser_getint(dict, "INDEXD:tcp_nodelay", 0) > 0) indexd->flag |= SB_TCP_NODELAY;
    if(iniparser_getint(dict, "INDEXD:socket_linger", 0) > 0) indexd->flag |= SB_SO_LINGER;
    if(iniparser_getint(dict, "INDEXD:while_send", 0) > 0) indexd->flag |= SB_WHILE_SEND;
    if(iniparser_getint(dict, "INDEXD:log_thread", 0) > 0) indexd->flag |= SB_LOG_THREAD;
    if(iniparser_getint(dict, "INDEXD:use_outdaemon", 0) > 0) indexd->flag |= SB_USE_OUTDAEMON;
    if(iniparser_getint(dict, "INDEXD:use_evsig", 0) > 0) indexd->flag |= SB_USE_EVSIG;
    if(iniparser_getint(dict, "INDEXD:use_cond", 0) > 0) indexd->flag |= SB_USE_COND;
    if((n = iniparser_getint(dict, "INDEXD:sched_realtime", 0)) > 0) indexd->flag |= (n & (SB_SCHED_RR|SB_SCHED_FIFO));
    if((n = iniparser_getint(dict, "INDEXD:io_sleep", 0)) > 0) indexd->flag |= ((SB_IO_NANOSLEEP|SB_IO_USLEEP|SB_IO_SELECT) & n);
    indexd->nworking_tosleep = iniparser_getint(dict, "INDEXD:nworking_tosleep", SB_NWORKING_TOSLEEP);
    indexd->set_log(indexd, iniparser_getstr(dict, "INDEXD:logfile"));
    indexd->set_log_level(indexd, iniparser_getint(dict, "INDEXD:log_level", 0));
    indexd->session.packet_type = PACKET_CERTAIN_LENGTH;
    indexd->session.packet_length = sizeof(IHEAD);
    indexd->session.buffer_size = iniparser_getint(dict, "INDEXD:buffer_size", SB_BUF_SIZE);
    indexd->session.quick_handler = &indexd_quick_handler;
    indexd->session.packet_handler = &indexd_packet_handler;
    indexd->session.data_handler = &indexd_data_handler;
    indexd->session.timeout_handler = &indexd_timeout_handler;
        //return sbase->add_service(sbase, indexd);
    return (sbase->add_service(sbase, httpd) | sbase->add_service(sbase, queryd)
            | sbase->add_service(sbase, indexd));
}

static void indexd_stop(int sig)
{
    switch (sig)
    {
        case SIGINT:
        case SIGTERM:
            fprintf(stderr, "indexd server is interrupted by user.\n");
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
    while((ch = getopt(argc, argv, "c:d")) != (char )-1)
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
    signal(SIGTERM, &indexd_stop);
    signal(SIGINT,  &indexd_stop);
    signal(SIGHUP,  &indexd_stop);
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
    sbase->running(sbase, 0);
    //sbase->running(sbase, 3600);
    //sbase->running(sbase, 60000000);
    //sbase->stop(sbase);
    ibase->clean(ibase);
    sbase->clean(sbase);
    if(http_headers_map) http_headers_map_clean(http_headers_map);
    if(argvmap)mtrie_clean(argvmap);
    if(dict)iniparser_free(dict);
    if(logger){LOGGER_CLEAN(logger);}
    if(httpd_index_html_code) free(httpd_index_html_code);
    return 0;
}
