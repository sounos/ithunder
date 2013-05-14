#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <zlib.h>
#include <ibase.h>
#include <dbase.h>
#ifdef  HAVE_SCWS
#include <scws/scws.h>
#endif
#include "xmm.h"
#include "mmtrie.h"
#include "kindex.h"
#include "mutex.h"
#include "timer.h"
#include "logger.h"
#include "mtree.h"
#include "zvbcode.h"
#ifndef LLI
#define LLI(x) ((long long int ) x)
#define LL64(x) ((long long int ) x)
#endif
#define UCHR(p) ((unsigned char *)p)
#define ISSIGN(p) (*p == '@' || *p == '.' || *p == '-' || *p == '_')
#define ISNUM(p) ((*p >= '0' && *p <= '9'))
#define ISCHAR(p) ((*p >= 'A' && *p <= 'Z')||(*p >= 'a' && *p <= 'z'))
#define CHECK_MEM(oldmm, nold, nlen)                                                    \
do                                                                                      \
{                                                                                       \
    if(nlen > nold)                                                                     \
    {                                                                                   \
        if(oldmm && nold > 0)xmm_free(oldmm, nold);                                     \
        oldmm = NULL;                                                                   \
        nold = ((nlen / K_BLOCK_SIZE)+1) * K_BLOCK_SIZE;                                \
        if((oldmm = (char *)xmm_new(nold)) == NULL) nold = 0;                           \
    }                                                                                   \
}while(0)

#define KINDEX_LOG_NAME         "kindex.log"
#define KINDEX_STATE_NAME       "kindex.state"
#define KINDEX_MDICT_NAME       "kindex.mdict"
#define KINDEX_XDICT_NAME       "kindex.xdict"
/* mkdir force */
int kindex_pmkdir(char *path)
{
    char fullpath[K_PATH_MAX];
    int level = -1, ret = -1;
    struct stat st = {0};
    char *p = NULL;

    if(path)
    {
        strcpy(fullpath, path);
        p = fullpath;
        while(*p != '\0')
        {
            if(*p == '/' )
            {
                while(*p != '\0' && *p == '/' && *(p+1) == '/')++p;
                if(level > 0)
                {
                    *p = '\0';
                    memset(&st, 0, sizeof(struct stat));
                    ret = stat(fullpath, &st);
                    if(ret == 0 && !S_ISDIR(st.st_mode)) return -1;
                    if(ret != 0 && mkdir(fullpath, 0755) != 0) return -1;
                    *p = '/';
                }
                level++;
            }
            ++p;
        }
        return 0;
    }
    return -1;
}

/* set document basedir  */
int kindex_set_basedir(KINDEX *kindex, char *basedir)
{
    char path[K_PATH_MAX];
    struct stat st = {0};

    if(kindex)
    {
        strcpy(kindex->basedir, basedir);
        sprintf(path, "%s/%s", basedir, KINDEX_LOG_NAME);
        kindex_pmkdir(path);
        LOGGER_INIT(kindex->logger, path);
        LOGGER_SET_LEVEL(kindex->logger, kindex->log_level);
        /* mdict */
        sprintf(path, "%s/%s", basedir, KINDEX_MDICT_NAME);
        if((kindex->mdict = mmtrie_init(path)) == NULL)
        {
            fprintf(stderr, "Initialize dict(%s) failed, %s\n", path, strerror(errno));
            _exit(-1);
            return -1;
        }
        /* xdict */
        sprintf(path, "%s/%s", basedir, KINDEX_XDICT_NAME);
        if((kindex->xdict = mmtrie_init(path)) == NULL)
        {
            fprintf(stderr, "Initialize xdict(%s) failed, %s\n", path, strerror(errno));
            _exit(-1);
            return -1;
        }
        /* state file */
        sprintf(path, "%s/%s", basedir, KINDEX_STATE_NAME);
        if((kindex->statefd = open(path, O_CREAT|O_RDWR, 0644)) > 0)
        {
            if(fstat(kindex->statefd, &st) != 0)
            {
                fprintf(stderr, "stat(%s) failed, %s\n", path, strerror(errno));
                _exit(-1);
            }
            if(st.st_size < sizeof(KSTATE) && ftruncate(kindex->statefd, sizeof(KSTATE)) != 0)
            {
                fprintf(stderr, "ftruncate(%s, %ld) failed, %s\n", path, sizeof(KSTATE), strerror(errno));
                _exit(-1);
            }
            if((kindex->state = (KSTATE *)mmap(NULL, sizeof(KSTATE), PROT_READ|PROT_WRITE,
                            MAP_SHARED, kindex->statefd, 0)) == NULL || kindex->state == (void *)-1)
            {
                fprintf(stderr, "mmap state failed, %s\n", strerror(errno));
                _exit(-1);
            }
            if(st.st_size < sizeof(KSTATE)) memset(kindex->state, 0, sizeof(KSTATE));
            kindex->state->start_time = (off_t)time(NULL);
        }
        else
        {
            fprintf(stderr, "open state(%s) failed, %s\n", path, strerror(errno));
            _exit(-1);
            return -1;
        }
        return 0;
    }
    return -1;
}

/* segment */
int kindex_segment(KINDEX *kindex, XINDEX *xindex, char *base, char *start, char *end, int bit_fields)
{
    int termid = 0, nterm = 0, i = 0, x = 0, n = 0, old = 0, *pold = &old;
    char *ps = NULL, *s = NULL, *es = NULL;
    void *map = NULL;

    if(kindex && xindex && (map = xindex->map))
    {
        s = start;
        es = end;
        while(s < es)
        {
            while(s < es && (*(UCHR(s))) < 0x80
                    && !ISCHAR(UCHR(s)) && !ISNUM(UCHR(s))) ++s;
            if((*(UCHR(s))) < 0x80)
            {
                ps = s;
                while(ps < es && (*(UCHR(ps))) < 0x80
                    && (ISSIGN(ps) || ISCHAR(UCHR(ps)) || ISNUM(UCHR(ps)))) ++ps;
                nterm = (ps - s);
                //termid = mmtrie_xadd((MMTRIE *)(kindex->mdict), s, nterm);
            }
            else
            {
                n = es - s;
                termid = 0;nterm = 0;
                if(mmtrie_maxfind((MMTRIE *)(kindex->mdict), s, n, &nterm) > 0)
                {
                    ps = s;
                }
            }
            if(ps && nterm > 0 && xindex->nterms < IB_TERMS_MAX
                    && (termid = mmtrie_xadd((MMTRIE *)(kindex->xdict), ps, nterm)) > 0)
            //if(termid > 0 && nterm > 0 && xindex->nterms < IB_TERMS_MAX)
            {
                if((MTREE_ADD(map, termid, xindex->nterms, pold)) == 0)
                {
                    i = xindex->nterms++;
                    memset(&(xindex->terms[i]), 0, sizeof(STERM));
                    memset(&(xindex->nodes[i]), 0, sizeof(TERMNODE));
                    xindex->nodes[i].termid = termid;
                    xindex->nodes[i].term_offset = s - base;
                    xindex->terms[i].termid = termid;
                    xindex->terms[i].term_len = nterm;
                    xindex->term_text_total += nterm;
                }
                else 
                {
                    i = old;
                }
                x = xindex->nodes[i].noffsets++;
                xindex->nodes[i].offsets[x] = s - base;
                xindex->terms[i].bit_fields |= bit_fields;
                xindex->terms[i].term_count++;
                xindex->term_offsets_total++;
                s += nterm;
            }
            else
            {
                if(((unsigned char )(*s)) >= 252) n = 6;
                else if(((unsigned char )(*s)) >= 248) n = 5;
                else if(((unsigned char )(*s)) >= 240) n = 4;
                else if(((unsigned char )(*s)) >= 224) n = 3;
                else if(((unsigned char )(*s)) >= 192) n = 2;
                else n = 1;
                s += n;
            }
        }
        return 0;
    }
    return -1;
}

/* rsegment */
int kindex_rsegment(KINDEX *kindex, XINDEX *xindex, 
        char *base, char *start, char *end, int bit_fields)
{
    char *p = NULL, *ps = NULL, *s = NULL, *es = NULL, *ep = NULL;
    int termid = 0, nterm = 0, i = 0, x = 0, n = 0, last = -1, old = 0, *pold = &old;
    void *map = NULL;

    if(kindex && xindex && (map = xindex->map) && start && end && start < end)
    {
        s = start;
        es = end;
        --es;
        while(es >= s)
        {
            while(es >= s && (*(UCHR(es))) < 0x80
                    && !ISCHAR(UCHR(es)) && !ISNUM(UCHR(es))) --es;
            if(es < s)break;
            ps = NULL, termid = 0;nterm = 0;
            if(*(UCHR(es)) < 0x80)
            {
                ps = es;
                while(ps >= s && (*(UCHR(ps))) < 0x80
                    && (ISSIGN(ps) || ISCHAR(UCHR(ps)) || ISNUM(UCHR(ps)))) --ps;
                ++ps;
                nterm = (es - ps + 1);
            }
            else
            {
                n = es - s  + 1;
                if((mmtrie_rmaxfind((MMTRIE *)(kindex->mdict), s, n, &nterm)) > 0)
                {
                    ps = s;
                }
            }
            if(ps && nterm > 0 && xindex->nterms < IB_TERMS_MAX
                    && (termid = mmtrie_xadd((MMTRIE *)(kindex->xdict), ps, nterm)) > 0)
            {
                es -= nterm;
                if((MTREE_ADD(map, termid, xindex->nterms, pold)) == 0)
                {
                    i = xindex->nterms++;
                    memset(&(xindex->terms[i]), 0, sizeof(STERM));
                    memset(&(xindex->nodes[i]), 0, sizeof(TERMNODE));
                    xindex->nodes[i].termid = termid;
                    xindex->nodes[i].term_offset = es + 1 - base;
                    xindex->terms[i].termid = termid;
                    xindex->terms[i].term_len = nterm;
                    xindex->term_text_total += nterm;
                }
                else 
                {
                    i = old;
                }
                x = xindex->nodes[i].nroffsets++;
                xindex->nodes[i].roffsets[x] = es + 1 - base;
                xindex->terms[i].bit_fields |= bit_fields;
                xindex->terms[i].term_count++;
                xindex->term_offsets_total++;
                if(kindex->phrase_status != K_PHRASE_DISABLED)
                {
                    if(ep && last >= 0 && es == ep)
                    {
                        xindex->nodes[i].nexts[last] = 1; 
                        xindex->nodes[last].prevs[i] = 1; 
                    }
                    ep = es + nterm;
                    last = i;
                }
            }
            else
            {
                if((p = (es - 5)) >= s && ((unsigned char )(*p)) >= 252) n = 6;
                else if((p = (es - 4)) >= s && ((unsigned char )(*p)) >= 248) n = 5;
                else if((p = (es - 3)) >= s && ((unsigned char )(*p)) >= 240) n = 4;
                else if((p = (es - 2)) >= s && ((unsigned char )(*p)) >= 224) n = 3;
                else if((p = (es - 1)) >= s && ((unsigned char )(*p)) >= 192) n = 2;
                else n = 1;
                es -= n;
            }
        }
        return 0;
    }
    return -1;
}

/* import new dict */
int kindex_set_dict(KINDEX *kindex, char *dict_file, char *dict_charset, char *dict_rules)
{
    int i = 0;
    if(kindex)
    {
#ifdef HAVE_SCWS
        scws_t segmentor = NULL;
        if(dict_charset)strcpy(kindex->dict_charset, dict_charset);
        if(dict_rules)strcpy(kindex->dict_rules, dict_rules);
        if(dict_file)strcpy(kindex->dict_file, dict_file);
        if((kindex->segmentor = scws_new()))
        {
            scws_set_charset((scws_t)(kindex->segmentor), dict_charset);
            scws_set_rule((scws_t)(kindex->segmentor), dict_rules);
            scws_set_dict((scws_t)(kindex->segmentor), dict_file, SCWS_XDICT_XDB);
            for(i = 0; i < IB_SEGMENTORS_MIN; i++)
            {
                if((segmentor = scws_new()))
                {
                    kindex->qsegmentors[i] = segmentor;
                    ((scws_t)(segmentor))->r = ((scws_t)kindex->segmentor)->r;
                    ((scws_t)(segmentor))->d = ((scws_t)kindex->segmentor)->d;
                    ((scws_t)(segmentor))->mblen = ((scws_t)kindex->segmentor)->mblen;
                    //scws_set_charset((scws_t)(segmentor), kindex->dict_charset);
                    //scws_set_rule((scws_t)(segmentor), kindex->dict_rules);
                    //scws_set_dict((scws_t)(segmentor), kindex->dict_file, SCWS_XDICT_XDB);
                    kindex->nqsegmentors++;
                }
            }
        }
        else
        {
            _exit(-1);
        }
#else
        mmtrie_import(kindex->mdict, dict_file, -1);
#endif
        return 0;
    }
    return -1;
}

/* set task server */
int kindex_set_qtask_server(KINDEX *kindex, char *ip, int port, int commitid, int queueid)
{
    int ret = -1;

    if(kindex && ip && port > 0 && strlen(ip) < K_IP_MAX && (commitid > 0 || queueid > 0))
    {
        kindex->qtask_commitid = commitid;
        kindex->qtask_queueid = queueid;
        kindex->qtask_server_port = port;
        strcpy(kindex->qtask_server_host, ip);
        ret = 0;
    }
    return ret;
}

/* set data-source  db */
int kindex_set_source_db(KINDEX *kindex, char *ip, int port, char *key_name, char *property_name, 
        char *text_index_name, char *int_index_name, char *long_index_name, 
        char *double_index_name, char *display_fields_name)
{
    int ret = -1;

    if(kindex && ip && port > 0 && key_name && property_name && text_index_name 
            && int_index_name && long_index_name && double_index_name 
            && display_fields_name && strlen(ip) < K_IP_MAX 
            && strlen(key_name) < K_FIELDNAME_MAX
            && strlen(property_name) < K_FIELDNAME_MAX
            && strlen(text_index_name) < K_FIELDNAME_MAX  
            && strlen(int_index_name) < K_FIELDNAME_MAX 
            && strlen(long_index_name) < K_FIELDNAME_MAX 
            && strlen(double_index_name) < K_FIELDNAME_MAX 
            && strlen(display_fields_name) < K_FIELDNAME_MAX) 
    {
        kindex->s_port = port;
        strcpy(kindex->s_host, ip);    
        strcpy(kindex->s_key_name, key_name);    
        strcpy(kindex->s_property_name, property_name);    
        strcpy(kindex->s_text_index_name, text_index_name);    
        strcpy(kindex->s_int_index_name, int_index_name);    
        strcpy(kindex->s_long_index_name, long_index_name);    
        strcpy(kindex->s_double_index_name, double_index_name);    
        strcpy(kindex->s_display_fields_name, display_fields_name);    
        ret = 0;
    }
    return ret;
}

/* set res-data  db */
int kindex_set_res_db(KINDEX *kindex, char *ip, int port, char *key_name, char *index_block_name)
{
    int ret = -1;

    if(kindex && ip && port > 0 && key_name && index_block_name 
            && strlen(ip) < K_IP_MAX  && strlen(key_name) < K_FIELDNAME_MAX 
            && strlen(index_block_name) < K_FIELDNAME_MAX)
    {
        kindex->r_port = port;
        strcpy(kindex->r_host, ip);
        strcpy(kindex->r_key_name, key_name);    
        strcpy(kindex->r_index_block_name, index_block_name);    
        ret = 0;
    }
    return ret;
}

/* new xindex */
XINDEX *xindex_new(KINDEX *kindex)
{
    XINDEX *xindex = NULL;
    if(strlen(kindex->qtask_server_host) > 0 && kindex->qtask_server_port > 0
            && (kindex->qtask_commitid > 0 || kindex->qtask_queueid > 0)
            && (xindex = xmm_mnew(sizeof(XINDEX))))
    {
        /* map */
        xindex->map = mtree_init();
        /* set mtask */
        mtask_set(&(xindex->mtask), kindex->qtask_server_host, kindex->qtask_server_port, 
                kindex->qtask_commitid, kindex->qtask_queueid);
        /* set source/res db connection*/
        if(dbase_set(&(xindex->sdb), kindex->s_host, kindex->s_port) != 0
            || dbase_connect(&(xindex->sdb)) != 0)
        {
            FATAL_LOGGER(kindex->logger, "connect_source_db(%s:%d) failed, %s", kindex->s_host, kindex->s_port, strerror(errno));
        }
        if(dbase_set(&(xindex->rdb), kindex->r_host, kindex->r_port) != 0
            || dbase_connect(&(xindex->rdb)) != 0)
        {
            FATAL_LOGGER(kindex->logger, "connect_res_db(%s:%d) failed, %s", kindex->s_host, kindex->s_port, strerror(errno));
        }
        TIMER_INIT(xindex->timer);
    }
    return xindex;
}

/* xinde check db */
int xindex_check(XINDEX *xindex)
{
    int ret = -1;
    if(xindex)
    {
        if(xindex->mtask.fd <= 0 && (ret = mtask_connect(&(xindex->mtask))) < 0)
        {
            WARN_LOGGER(xindex->logger, "connect to qtaskd failed, %s", strerror(errno))
            return -2;
        }
        return 0;
    }
    return -1;
}

/* clean hindex */
void xindex_clean(XINDEX *xindex)
{
    if(xindex)
    {
        mtask_close(&(xindex->mtask));
        dbase_close(&(xindex->sdb));
        dbase_close(&(xindex->rdb));
        bjson_clean(&(xindex->record));
        /* clean block */
        if(xindex->data && xindex->ndata) xmm_free(xindex->data, xindex->ndata);
        if(xindex->block && xindex->nblock) xmm_free(xindex->block, xindex->nblock);
        TIMER_CLEAN(xindex->timer);
        xmm_free(xindex, sizeof(XINDEX));
    }
    return ;
}

/* push segmentor */
void kindex_push_segmentor(KINDEX *kindex, void *segmentor)
{
    int x = 0;

    if(kindex && segmentor)
    {
        MUTEX_LOCK(kindex->mutex_segmentor);
        if(kindex->nqsegmentors < IB_SEGMENTORS_MAX)
        {
            x = kindex->nqsegmentors++;
            kindex->qsegmentors[x] = segmentor;
        }
        else
        {
#ifdef HAVE_SCWS
            ((scws_t)(segmentor))->r = NULL;
            ((scws_t)(segmentor))->d = NULL;
            scws_free((scws_t)segmentor);
#endif
        }
        MUTEX_UNLOCK(kindex->mutex_segmentor);
    }
    return ;
}

/* pop segmentor */
void *kindex_pop_segmentor(KINDEX *kindex)
{
    void *segmentor = NULL;
    int x = 0;

    if(kindex && kindex->segmentor)
    {
        MUTEX_LOCK(kindex->mutex_segmentor);
        if(kindex->nqsegmentors > 0)
        {
            x = --(kindex->nqsegmentors);
            segmentor = kindex->qsegmentors[x];
        }
        else
        {
#ifdef HAVE_SCWS
            if((segmentor = scws_new()))
            {
                ((scws_t)(segmentor))->r = ((scws_t)kindex->segmentor)->r;
                ((scws_t)(segmentor))->d = ((scws_t)kindex->segmentor)->d;
                ((scws_t)(segmentor))->mblen = ((scws_t)kindex->segmentor)->mblen;
                //scws_set_charset((scws_t)(segmentor), kindex->dict_charset);
                //scws_set_rule((scws_t)(segmentor), kindex->dict_rules);
                //scws_set_dict((scws_t)(segmentor), kindex->dict_file, SCWS_XDICT_XDB);
            }
#endif
        }
        MUTEX_UNLOCK(kindex->mutex_segmentor);
    }
    return segmentor;
}


#ifdef HAVE_SCWS
int kindex_scws_segment(KINDEX *kindex, XINDEX *xindex, char *base, 
        char *start, char *end, int bit_fields)
{
    int termid = 0, nterm = 0, i = 0, x = 0, last = -1, old = 0, *pold = &old;
    char line[K_LINE_MAX], *p = NULL, *pp = NULL, *epp = NULL, *s = NULL, *es = NULL, *ep = NULL;
    scws_res_t res = NULL, cur = NULL;
    scws_t segmentor = NULL;
    void *map = NULL;

    if(kindex && xindex && (map = xindex->map) && start && end && start < end
            && (segmentor=(scws_t)kindex_pop_segmentor(kindex)))
    {
        s = start;
        es = end;
        scws_send_text(segmentor, s, es - s);
        while ((res = cur = scws_get_result(segmentor)))
        {
            while (cur != NULL)
            {
                pp = s = start + cur->off;
                nterm = cur->len;
                epp = s + nterm;
                p = line;
                while(pp < epp)
                {
                    if(*pp >= 'A' && *pp <= 'Z')
                        *p++ = *pp + 'a' - 'A';
                    else
                        *p++ = *pp;
                    ++pp;
                }
                if(nterm > 0 && (termid=mmtrie_xadd((MMTRIE *)(kindex->xdict), line, nterm)) > 0)
                {
                    if((MTREE_ADD(map, termid, xindex->nterms, pold)) == 0)
                    {
                        i = xindex->nterms++;
                        memset(&(xindex->terms[i]), 0, sizeof(STERM));
                        memset(&(xindex->nodes[i]), 0, sizeof(TERMNODE));
                        xindex->nodes[i].termid = termid;
                        xindex->nodes[i].term_offset = s - base;
                        xindex->terms[i].termid = termid;
                        xindex->terms[i].term_len = nterm;
                        xindex->term_text_total += nterm;
                    }                       else 
                    {
                        i = old;
                    }
                    x = xindex->nodes[i].noffsets++;
                    xindex->nodes[i].offsets[x] = s - base;
                    xindex->terms[i].bit_fields |= bit_fields;
                    xindex->terms[i].term_count++;
                    xindex->term_offsets_total++;
                    if(kindex->phrase_status != K_PHRASE_DISABLED)
                    {
                        if(ep && last >= 0 && ep  == s)
                        {
                            xindex->nodes[i].prevs[last] = 1;
                            xindex->nodes[last].nexts[i] = 1;
                        }
                        ep = s + nterm;
                        last = i;
                    }
                }
                cur = cur->next;
            }
            scws_free_result(res);
        }
        kindex_push_segmentor(kindex, segmentor);
    }
    return 0;
}
#endif
#define XINDEX_RESET(x)                                                                 \
do                                                                                      \
{                                                                                       \
    x->nterms = 0;                                                                      \
    x->term_text_total = 0;                                                             \
    x->term_offsets_total = 0;                                                          \
}while(0)

/* genindex block */
int kindex_genindex(KINDEX *kindex, XINDEX *xindex, FHEADER *fheader, IFIELD *fields, int nfields,
        char *content, int ncontent, IBDATA *block)
{
    int ret = -1,  i = 0, j = 0, x = 0, last = 0, to = 0, n = 0, mm = 0, 
        *np = NULL, index_int_from = -1, index_long_from = -1, index_double_from = -1, 
        index_int_num = 0, index_long_num = 0, index_double_num = 0;
    char *s = NULL, *es = NULL, *p = NULL, *pp = NULL, *ps = NULL;
    STERM *termlist = NULL, *sterm = NULL;
    int64_t *npl = NULL, nl = 0;
    DOCHEADER *docheader = NULL;
    XFIELD *xfields = NULL;
    size_t nzcontent = 0;
    unsigned int un = 0;
    void *timer = NULL;
    double *npf = NULL;

    if(kindex && xindex && fheader && fields && nfields  > 0 && content && ncontent > 0 
            && block && xindex->map)
    {
        TIMER_INIT(timer);
        XINDEX_RESET(xindex);
        MTREE_RESET(xindex->map);
        TIMER_SAMPLE(timer);
        DEBUG_LOGGER(kindex->logger, "ready for segment content(%d) time used:%lld", ncontent, PT_LU_USEC(timer));
        to = nfields;
        if(nfields > IB_INDEX_MAX) to = IB_INDEX_MAX;
        i = 0;
        while(i < to)
        {
            s = content + fields[i].offset;
            es = s + fields[i].length;
            if((fields[i].flag & IB_IS_NEED_INDEX))
            {
                if(fields[i].flag & IB_DATATYPE_TEXT)
                {
                    n = 1 << i;
#ifdef HAVE_SCWS
                    kindex_scws_segment(kindex, xindex, content, s, es, n);
#endif
                }
                else if(fields[i].flag & IB_DATATYPE_INT)
                {
                    if(index_int_from < 0) index_int_from = i;
                    index_int_num++;
                }
                else if(fields[i].flag & IB_DATATYPE_LONG)
                {
                    if(index_long_from < 0) index_long_from = i;
                    index_long_num++;
                }
                else if(fields[i].flag & IB_DATATYPE_DOUBLE)
                {
                    if(index_double_from < 0) index_double_from = i;
                    index_double_num++;
                }
            }
            ++i;
        }
        //rsegment 
#ifndef HAVE_SCWS
        i = to - 1;
        while(i >= 0)
        {
            s = content + fields[i].offset;
            es = s + fields[i].length;
            if((fields[i].flag & IB_IS_NEED_INDEX))
            {
                if(fields[i].flag & IB_DATATYPE_TEXT)
                {
                    n = 1 << i;
                    kindex_rsegment(kindex, xindex, content, s, es, n);
                }
            }
            --i;
        }
#endif
        TIMER_SAMPLE(timer);
        DEBUG_LOGGER(kindex->logger, "segment content(%d) time used:%lld", ncontent, PT_LU_USEC(timer));
        if(xindex->nterms == 0) 
        {
            FATAL_LOGGER(kindex->logger, "NO terms globalid:%lld content:%s", LL(fheader->globalid), content);
            goto end;
        }
        block->ndata = sizeof(DOCHEADER) 
                + sizeof(XFIELD) * nfields 
                + xindex->nterms * sizeof(STERM)
                + xindex->term_offsets_total * sizeof(int) 
                + xindex->term_text_total 
                + sizeof(int) * index_int_num 
                + sizeof(int64_t) * index_long_num 
                + sizeof(double) * index_double_num;
        if(kindex->compress_status != K_COMPRESS_DISABLED)
        {
            nzcontent = compressBound(ncontent);
            block->ndata += nzcontent;
        }
        else
        {
            block->ndata += ncontent;
        }
        CHECK_MEM(xindex->block, xindex->nblock, block->ndata);
        if((block->data = xindex->block) == NULL)
        {
            FATAL_LOGGER(kindex->logger, "xmm_new(%d) failed, %s", block->ndata, strerror(errno));
            _exit(-1);
        }
        TIMER_SAMPLE(timer);
        DEBUG_LOGGER(kindex->logger, "malloc block[%d] time used:%lld", xindex->nblock, PT_LU_USEC(timer));
        docheader = (DOCHEADER *)block->data;
        memset(block->data, 0, sizeof(DOCHEADER));
        docheader->crc = fheader->crc;
        docheader->category = fheader->category;
        docheader->slevel = fheader->slevel;
        docheader->rank = fheader->rank;
        docheader->globalid = fheader->globalid;
        if(docheader->globalid == 0ll)
        {
            FATAL_LOGGER(kindex->logger, "invalid document globalid:%lld content:%.*s", LL(fheader->globalid), ncontent, content);
            goto end;
        }
        //copy fields
        docheader->nfields = nfields;
        docheader->nterms = xindex->nterms;
        p = block->data + sizeof(DOCHEADER);
        xfields = (XFIELD *)p;
        for(i = 0; i < nfields; i++) xfields[i].from = fields[i].offset;
        p += sizeof(XFIELD) * nfields;
        //compress && dump copy terms_map
        termlist = sterm = (STERM *)p;
        p += sizeof(STERM) * xindex->nterms;
#ifdef HAVE_SCWS
        //x = xindex->nterms - 1;
        x = 0;
        do
        {
            memcpy(sterm, &(xindex->terms[x]), sizeof(STERM));
            last = 0;
            pp = p;
            sterm->posting_offset = p - (char *)sterm; 
            j = 0;
            do
            {
                n = (xindex->nodes[x].offsets[j] - last);
                np = &n;
                ZVBCODE(np, p);
                docheader->terms_total++;
                last = xindex->nodes[x].offsets[j];
            }while(++j < xindex->nodes[x].noffsets);
            /*
             //test uncomress posting
            last = 0;
            s = pp;
            while(s < p)
            {
                to = 0;
                np = &to;
                UZVBCODE(s, n, np);
                last += to;
                fprintf(stdout, "x:%d termid:%d %.*s\n", x, xindex->nodes[x].termid, sterm->term_len, content + last);
            }
            */
            sterm->posting_size = p - pp;
            ++sterm;
        }while(++x  < xindex->nterms);
#else
        x = xindex->nterms - 1;
        do
        {
            memcpy(sterm, &(xindex->terms[x]), sizeof(STERM));
            last = 0;
            pp = p;
            sterm->posting_offset = p - (char *)sterm; 
            j = xindex->nodes[x].nroffsets - 1;
            do
            {
                n = (xindex->nodes[x].roffsets[j] - last);
                np = &n;
                ZVBCODE(np, p);
                docheader->terms_total++;
                last = xindex->nodes[x].roffsets[j];
            }while(--j >= 0);
            /*
             //test uncomress posting
            last = 0;
            s = pp;
            while(s < p)
            {
                to = 0;
                np = &to;
                UZVBCODE(s, n, np);
                last += to;
                fprintf(stdout, "x:%d termid:%d %.*s\n", x, xindex->nodes[x].termid, sterm->term_len, content + last);
            }
            */
            sterm->posting_size = p - pp;
            ++sterm;
        }while(--x  >= 0);
#endif
        TIMER_SAMPLE(timer);
        DEBUG_LOGGER(kindex->logger, "compress posting[%d] time used:%lld", xindex->nterms, PT_LU_USEC(timer));
        //compress content 
        docheader->content_off = p - (char *)block->data;
        //fprintf(stdout, "%s::%d off:%d p:%p\n", __FILE__, __LINE__, docheader->content_off, p);
        if(kindex->compress_status != K_COMPRESS_DISABLED)
        {
            if((ret = compress((Bytef *)p, (uLongf *)&nzcontent,
                            (Bytef *)(content), (uLong)(ncontent))) == Z_OK)
            {
                TIMER_SAMPLE(timer);
                DEBUG_LOGGER(kindex->logger, "compress content(%d) time used:%lld", ncontent, PT_LU_USEC(timer));
                docheader->content_size = ncontent;
                docheader->content_zsize = nzcontent;
                p += nzcontent;
            }
            else goto end; 
        }
        else
        {
            memcpy(p, content, ncontent);
            docheader->content_size = ncontent;
            p += ncontent;
        }
        //copy prevnext block
        docheader->prevnext_off = p - (char *)block->data; 
        //fprintf(stdout, "%s::%d off:%d p:%p ncontent:%d x:%d\n", __FILE__, __LINE__, docheader->content_off, p, ncontent, docheader->prevnext_off - docheader->content_off);
        if(kindex->phrase_status != K_PHRASE_DISABLED)
        {
            termlist = (STERM *)((char *)block->data + sizeof(DOCHEADER) + sizeof(XFIELD) * nfields);
            pp = p;
#ifdef HAVE_SCWS
            for(i = 0; i < xindex->nterms; i++)
            {
                last = 0;
                ps = p;
                for(j = 0; j < xindex->nterms; j++)
                {
                    if(xindex->nodes[i].prevs[j])
                    {
                        mm = j << 1;
                        n = mm - last;
                        last = mm;
                        np = &n;
                        ZVBCODE(np, p);
                    }
                    if(xindex->nodes[i].nexts[j])
                    {
                        mm = ((j << 1) | 1);
                        n = mm - last;
                        last = mm;
                        np = &n;
                        ZVBCODE(np, p);
                    }
                }
                termlist[i].prevnext_size = p - ps;
            }
#else
            int k = 0;
            for(i = (xindex->nterms - 1); i >= 0; i--)
            {
                last = 0;
                x = 0;
                ps = p;
                for(j = (xindex->nterms - 1); j >= 0; j--)
                {
                    if(xindex->nodes[i].prevs[j])
                    {
                        mm = j << 1;
                        n = mm - last;
                        last = mm;
                        np = &n;
                        ZVBCODE(np, p);
                    }
                    if(xindex->nodes[i].nexts[j])
                    {
                        mm = ((j << 1) | 1);
                        n = mm - last;
                        last = mm;
                        np = &n;
                        ZVBCODE(np, p);
                    }
                    ++x;
                }
                termlist[k].prevnext_size = p - ps;
                ++k;
            }
#endif
            /*
            //test prevnext
            s = pp;
            for(i = 0; i < xindex->nterms; i++)
            {
                last = 0;
                es = s + termlist[i].prevnext_size;
                while(s < es)
                {
                    x = 0;
                    np = &x;
                    UZVBCODE(s, n, np);
                    last += x;
                    if(last > xindex->nterms)
                    {
                        fprintf(stdout, "%s::%d nterm:%d i:%d last:%d x:%d\n", __FILE__, __LINE__, xindex->nterms, i, last, x);
                        _exit(-1);
                    }
                }
            }
            */
            docheader->prevnext_size = p - pp;
        }
        /*
        char *content = block->data + docheader->content_off;
        int j = 0;
        IFIELD *fieldslist = block->data + sizeof(DOCHEADER);
        for(j = 0; j < docheader->nfields; j++)
        {
            if((fieldslist[j].flag & IB_DATATYPE_TEXT) && (fieldslist[j].flag & IB_IS_NEED_INDEX))
                fprintf(stdout, "%d:%.*s\n", j, fieldslist[j].length, content + fieldslist[j].offset);
        }
        */
        //copy term block 
        docheader->textblock_off = p - (char *)block->data;

        pp = p;
#ifdef HAVE_SCWS
        x = 0 ;
        do
        {
            s = content + xindex->nodes[x].term_offset;
            es = s + xindex->terms[x].term_len;
            while(s < es)
            {
                if(*s >= 'A' && *s <= 'Z') *p++ = *s + 'a' - 'A';
                else *p++ = *s;
                ++s;
            }
            //memcpy(p, (content + xindex->nodes[x].term_offset), xindex->terms[x].term_len);
            //p += xindex->terms[x].term_len;
            //fprintf(stdout, "%s::%d x:%d off:%d %d:%.*s\n", __FILE__, __LINE__, x, xindex->nodes[x].term_offset, xindex->nodes[x].termid, xindex->terms[x].term_len, content + xindex->nodes[x].term_offset);
        }while(++x < xindex->nterms);
#else
        x = xindex->nterms - 1;
        do
        {
            memcpy(p, (content + xindex->nodes[x].term_offset), xindex->terms[x].term_len);
            //fprintf(stdout, "%s::%d x:%d off:%d %d:%.*s\n", __FILE__, __LINE__, x, xindex->nodes[x].term_offset, xindex->nodes[x].termid, xindex->terms[x].term_len, content + xindex->nodes[x].term_offset);
            p += xindex->terms[x].term_len;
        }while(--x >= 0);
#endif
        docheader->textblock_size = p - pp;
        //fprintf(stdout, "pp:%s\n", pp);
        //_exit(-1);
        //copy int/double index
        if(index_int_num > 0)
        {
            docheader->intblock_off = p - (char *)block->data;
            pp = p;
            ACCESS_LOGGER(kindex->logger, "int index from:%d num:%d", index_int_from, index_int_num);
            np = (int *)p;
            i = index_int_from;
            to = i + index_int_num;
            do{
                s = content + fields[i].offset;
                un = 0;
                if(*s >= '0' && *s <= '9') un = (unsigned int)atoi(s);
                //fprintf(stdout, "%d:{n:%d,s:%.*s}\n", i, n, fields[i].length, s);
                *np++ = un;
            }while(++i < to);
            p = (char *)np;
            docheader->intindex_from = index_int_from;
            docheader->intblock_size = p - pp;
        }
        if(index_long_num > 0)
        {
            docheader->longblock_off = p - (char *)block->data;
            pp = p;
            //fprintf(stdout, "int index num:%d\n", index_int_num);
            npl = (int64_t *)p;
            i = index_long_from;
            to = i + index_long_num;
            do{
                s = content + fields[i].offset;
                nl = 0;
                if(*s >= '0' && *s <= '9') nl = (int64_t)atoll(s);
                //fprintf(stdout, "%d:{n:%d,s:%.*s}\n", i, n, fields[i].length, s);
                *npl++ = nl;
            }while(++i < to);
            p = (char *)npl;
            docheader->longindex_from = index_long_from;
            docheader->longblock_size = p - pp;
        }
        if(index_double_num > 0)
        {
            docheader->doubleblock_off = p - (char *)block->data;
            pp = p;
            //fprintf(stdout, "double index num:%d\n", index_double_num);
            npf = (double *)p;
            i = index_double_from;
            to = i + index_double_num;
            do{*npf++ = atof(content + fields[i].offset);}while(++i < to);
            p = (char *)npf;
            docheader->doubleindex_from = index_double_from;
            docheader->doubleblock_size = p - pp;
        }
        if(docheader->intblock_size == 0
                && docheader->longblock_size  == 0
                && docheader->doubleblock_size == 0)
        {
            WARN_LOGGER(kindex->logger, "global:%lld no int/long/double index", LL64(docheader->globalid));
        }
        ret = docheader->size = block->ndata = p - (char *)block->data;
        if(ret <= 0) {FATAL_LOGGER(kindex->logger, "bad block globalid:%lld", LL(fheader->globalid));}
end:
        //if(ret == -1 && block->data){free(block->data); block->data = NULL;}
        TIMER_CLEAN(timer);
    }
    else
    {
        FATAL_LOGGER(kindex->logger, "Invalid Document:%lld fields:%p nfields:%d content:%p ncontent:%d block:%p map:%p", LL(fheader->globalid), fields, nfields, content, ncontent, block, xindex->map);

    }
    return ret;
}

/* make index */
int kindex_work(KINDEX *kindex, XINDEX *xindex)
{
    int ret = -1, i = 0, x = 0, n = 0, packetid = -1, nfields = 0, vint = 0, npackets = 0;
    off_t task_rio_time = 0, task_wio_time = 0, packet_rio_time = 0, packet_wio_time = 0;
    BELEMENT *e = NULL, *sub = NULL, *root = NULL;
    char *content = NULL, *p = NULL, *s = NULL;
    int64_t *xid = 0, key = 0, vlong = 0;
    IFIELD fields[IB_FIELDS_MAX];
    FHEADER fheader = {0};
    double vdouble = 0.0;
    IBDATA block = {0};
    BRES *res = NULL;

    if(kindex && xindex && xindex_check(xindex) == 0)
    {
        //WARN_LOGGER(kindex->logger, "xindex:%p->work()", xindex);
        TIMER_RESET(xindex->timer);
        if((packetid = mtask_pop(&(xindex->mtask))) > 0 && (xid = (int64_t *)(xindex->mtask.packet))
                && xindex->mtask.length > sizeof(int64_t))
        {
            TIMER_SAMPLE(xindex->timer);
            task_rio_time = PT_LU_USEC(xindex->timer);
            //WARN_LOGGER(kindex->logger, "xindex:%p xid:%p end:%p mtask_pop:%d", xindex, xid, end, (end - xid)/sizeof(int64_t));
            n = xindex->mtask.length/sizeof(int64_t);
            brequest_reset(&(xindex->request));
            brequest_append_keys(&(xindex->request), xid, n);
            brequest_finish(&(xindex->request));
            TIMER_SAMPLE(xindex->timer);
            if((res = dbase_get_records(&(xindex->sdb), &(xindex->request))))
            {
                TIMER_SAMPLE(xindex->timer);
                x = 0;
                packet_rio_time += PT_LU_USEC(xindex->timer);
                while((root = dbase_next_record(res, root, &key))) 
                {
                    ACCESS_LOGGER(kindex->logger, "index_record[%d/%d] packetid:%d key:%lld", x, n, xindex->mtask.packetid, LLI(key));
                    CHECK_MEM(xindex->data, xindex->ndata, K_DOCUMENT_MAX);
                    if((p = content = xindex->data) == NULL)
                    {
                        FATAL_LOGGER(kindex->logger, "MALLOC[%d/%d] packetid:%d key:%lld failed, %s", x, n, xindex->mtask.packetid, LLI(key), strerror(errno));
                        return ret;
                    }
                    vlong = 0;
                    if((e = belement_find(root, kindex->s_key_name)))
                    {
                        belement_v_long(e, &vlong);
                    }
                    if(vlong != key)
                    {
                        FATAL_LOGGER(kindex->logger, "Invalid record[%d/%d] packetid:%d key:%lld/%lld", x, n, xindex->mtask.packetid, LLI(vlong), LLI(key));
                        return ret;
                    }
                    memset(&fheader, 0, sizeof(FHEADER));
                    //bson_print(&out);
                    if((sub = belement_find(root, kindex->s_property_name)))
                    {
                        if((e = belement_find(sub, "status")) && belement_v_int(e, &vint) == 0)
                        {
                            fheader.status = vint;
                        }
                        else
                        {
                            WARN_LOGGER(kindex->logger, "status NoFound record[%d/%d] packetid:%d key:%lld/%lld", x, n, xindex->mtask.packetid, LLI(vlong), LLI(key));
                        }
                        if((e = belement_find(sub, "crc")) && belement_v_int(e, &vint) == 0)
                        {
                            fheader.crc = vint;
                        }
                        else
                        {
                            WARN_LOGGER(kindex->logger, "crc NoFound record[%d/%d] packetid:%d key:%lld/%lld", x, n, xindex->mtask.packetid, LLI(vlong), LLI(key));
                        }
                        if((e = belement_find(sub, "category")) && belement_v_long(e, &vlong) == 0)
                        {
                            fheader.category = vlong;
                        }
                        else
                        {
                            WARN_LOGGER(kindex->logger, "category NoFound record[%d/%d] packetid:%d key:%lld/%lld", x, n, xindex->mtask.packetid, LLI(vlong), LLI(key));
                        }
                        if((e = belement_find(sub, "slevel")) && belement_v_int(e, &vint) == 0)
                        {
                            fheader.slevel = vint;
                        }
                        else
                        {
                            WARN_LOGGER(kindex->logger, "slevel NoFound record[%d/%d] packetid:%d key:%lld/%lld", x, n, xindex->mtask.packetid, LLI(vlong), LLI(key));
                        }
                        if((e = belement_find(sub, "rank")) && belement_v_double(e, &vdouble) == 0)
                        {
                            fheader.rank = vdouble;
                        }
                        else
                        {
                            WARN_LOGGER(kindex->logger, "rank NoFound record[%d/%d] packetid:%d key:%lld/%lld", x, n, xindex->mtask.packetid, LLI(vlong), LLI(key));
                        }

                        //fprintf(stdout, "%s::%d status:%d crc:%d slevel:%d category:%lld rank:%f\n",__FILE__, __LINE__, fheader.status, fheader.crc, fheader.slevel, LL64(fheader.category), fheader.rank);
                        if(fheader.crc == 0)
                        {
                            FATAL_LOGGER(kindex->logger, "Invalid fields[%s]", kindex->s_property_name);
                            return ret;
                        }
                    }
                    memset(fields, 0, sizeof(IFIELD) * IB_FIELDS_MAX);
                    i = 0;
                    /* text index */
                    if((sub = belement_find(root, kindex->s_text_index_name))
                            && (e = belement_childs(sub)))
                    {
                        s = p;
                        do
                        {
                            fields[i].offset = p - content;
                            p += sprintf(p, "%s\n", belement_v_string(e));
                            fields[i].length = p - content - fields[i].offset;
                            fields[i].flag = IB_IS_NEED_INDEX|IB_DATATYPE_TEXT;
                            ++i;
                        }while((e = belement_next(sub, e)));
                        ACCESS_LOGGER(kindex->logger, "%s:%s", kindex->s_text_index_name, s);
                    }
                    /* int index */
                    if((sub = belement_find(root, kindex->s_int_index_name))
                            && (e = belement_childs(sub)))
                    {
                        s = p;
                        do
                        {
                            fields[i].offset = p - content;
                            vint = 0;belement_v_int(e, &vint);
                            p += sprintf(p, "%d\n", vint);
                            fields[i].length = p - content - fields[i].offset;
                            fields[i].flag = IB_IS_NEED_INDEX|IB_DATATYPE_INT;
                            ++i;
                        }while((e = belement_next(sub, e)));
                        ACCESS_LOGGER(kindex->logger, "%s:%s", kindex->s_int_index_name, s);
                    }
                    /* long index */
                    if((sub = belement_find(root, kindex->s_long_index_name))
                            && (e = belement_childs(sub)))
                    {
                        s = p;
                        do
                        {
                            fields[i].offset = p - content;
                            vlong = 0;belement_v_long(e, &vlong);
                            p += sprintf(p, "%lld\n", LL64(vlong));
                            fields[i].length = p - content - fields[i].offset;
                            fields[i].flag = IB_IS_NEED_INDEX|IB_DATATYPE_LONG;
                            ++i;
                        }while((e = belement_next(sub, e)));
                        ACCESS_LOGGER(kindex->logger, "%s:%s", kindex->s_long_index_name, s);
                    }
                    /* double index */
                    if((sub = belement_find(root, kindex->s_double_index_name))
                            && (e = belement_childs(sub)))
                    {
                        s = p;
                        do
                        {
                            fields[i].offset = p - content;
                            vdouble = 0.0;belement_v_double(e, &vdouble);
                            p += sprintf(p, "%f\n", vdouble);
                            fields[i].length = p - content - fields[i].offset;
                            fields[i].flag = IB_IS_NEED_INDEX|IB_DATATYPE_DOUBLE;
                            ++i;
                        }while((e = belement_next(sub, e)));
                        ACCESS_LOGGER(kindex->logger, "%s:%s", kindex->s_double_index_name, s);
                    }
                    /* display fields */
                    if((sub = belement_find(root, kindex->s_display_fields_name))
                            && (e = belement_childs(sub)))
                    {
                        s = p;
                        do
                        {
                            fields[i].offset = p - content;
                            p += sprintf(p, "%s\n", belement_v_string(e));
                            fields[i].length = p - content - fields[i].offset;
                            fields[i].flag = IB_DATATYPE_TEXT;
                            ++i;
                        }while((e = belement_next(sub, e)));
                        ACCESS_LOGGER(kindex->logger, "%s:%s", kindex->s_display_fields_name, s);
                    }
                    *p = '\0';
                    nfields = i;
                    fheader.globalid = key;
                    //WARN_LOGGER(kindex->logger, "xindex:%p->genindex():%d", xindex, p - content);
                    if(kindex_genindex(kindex, xindex, &fheader, fields, nfields, content, 
                                p - content, &block) > 0 && block.ndata > 0)
                    {
                        brequest_reset(&(xindex->record)); 
                        bjson_start(&(xindex->record));
                        bjson_append_long(&(xindex->record), kindex->r_key_name, key);
                        bjson_append_blob(&(xindex->record), kindex->r_index_block_name, 
                                block.data, block.ndata);
                        bjson_finish(&(xindex->record));
                        //WARN_LOGGER(kindex->logger, "xindex:%p->set_record():%lld", xindex, key);
                        TIMER_SAMPLE(xindex->timer);
                        if((ret = dbase_set_record(&(xindex->rdb), key, &(xindex->record))) != 0)
                        {
                            FATAL_LOGGER(kindex->logger, "set_record[%d/%d] packetid:%d key:%lld failed, ret:%d", x, n, xindex->mtask.packetid, LLI(key), ret);
                            return ret;
                        }
                        ACCESS_LOGGER(kindex->logger, "over_record[%d/%d] packetid:%d key:%lld", x, n, xindex->mtask.packetid, LLI(key));
                        TIMER_SAMPLE(xindex->timer);
                        packet_wio_time += PT_LU_USEC(xindex->timer);
                        //WARN_LOGGER(kindex->logger, "xindex:%p->over_set_record():%lld", xindex, key);
                    }
                    else
                    {
                        FATAL_LOGGER(kindex->logger, "genindex(%lld) failed", LL64(key));
                        return ret;
                    }
                    npackets++;
                    x++;
                }
                TIMER_SAMPLE(xindex->timer);
                if((ret = mtask_finish(&(xindex->mtask), 0)) == 0)
                {
                    TIMER_SAMPLE(xindex->timer);
                    task_wio_time = PT_LU_USEC(xindex->timer);
                    MUTEX_LOCK(kindex->mutex);
                    kindex->state->ntasks++;
                    kindex->state->npackets += npackets;
                    kindex->state->task_rio_time += task_rio_time;
                    kindex->state->task_wio_time += task_wio_time;
                    kindex->state->packet_rio_time += packet_rio_time;
                    kindex->state->packet_wio_time += packet_wio_time;
                    kindex->state->usecs += PT_USEC_U(xindex->timer);
                    MUTEX_UNLOCK(kindex->mutex);
                }
            }
            else
            {
                FATAL_LOGGER(kindex->logger, "get_records(%d) failed, %s", xindex->mtask.packetid, strerror(errno));
            }
        }
    }
    return ret;
}

/* state */
int kindex_state(KINDEX *kindex, char *out)
{
    off_t task_avg = 0, packet_avg = 0, task_ravg = 0, task_wavg = 0,
          packet_ravg = 0, packet_wavg = 0;
    char *p = NULL;
    int ret = -1;

    if(kindex && kindex->state && (p = out))
    {
        MUTEX_LOCK(kindex->mutex);
        if(kindex->state->ntasks > 0) 
        {
            task_avg = kindex->state->usecs / kindex->state->ntasks;
            task_ravg = kindex->state->task_rio_time / kindex->state->ntasks;
            task_wavg = kindex->state->task_wio_time / kindex->state->ntasks;
            packet_ravg = kindex->state->packet_rio_time / kindex->state->ntasks;
        }
        if(kindex->state->npackets > 0) 
        {
            packet_avg = kindex->state->usecs / kindex->state->npackets;
            packet_wavg = kindex->state->packet_wio_time / kindex->state->npackets;
        }
        p += sprintf(p, "({\"time\":\"%lld\", \"usecs\":\"%lld\", "
                "\"ntasks\":\"%lld\", \"task_avg\":\"%lld\", "
                "\"npackets\":\"%lld\", \"packet_avg\":\"%lld\","
                "\"task_rio\":\"%lld\", \"task_ravg\":\"%lld\", "
                "\"task_wio\":\"%lld\", \"task_wavg\":\"%lld\","
                "\"packet_rio\":\"%lld\", \"packet_ravg\":\"%lld\", "
                "\"packet_wio\":\"%lld\", \"packet_wavg\":\"%lld\"})",
                LL64(time(NULL) - kindex->state->start_time), LL64(kindex->state->usecs), 
                LL64(kindex->state->ntasks), LL64(task_avg),
                LL64(kindex->state->npackets), LL64(packet_avg), 
                LL64(kindex->state->task_rio_time), LL64(task_ravg),
                LL64(kindex->state->task_wio_time), LL64(task_wavg),
                LL64(kindex->state->packet_rio_time), LL64(packet_ravg),
                LL64(kindex->state->packet_wio_time), LL64(packet_wavg));
        ret = p - out;
        MUTEX_UNLOCK(kindex->mutex);
    }
    return ret;
}

/* initialize kindex */
KINDEX *kindex_init()
{
    KINDEX *kindex = NULL;

    if((kindex = (KINDEX *)xmm_mnew(sizeof(KINDEX))))
    {
        MUTEX_INIT(kindex->mutex);
        MUTEX_INIT(kindex->mutex_segmentor);
    }
    return kindex;
}

void kindex_clean(KINDEX *kindex)
{
    int i = 0;

    if(kindex)
    {
        if(kindex->mdict){mmtrie_clean(kindex->mdict);}
        if(kindex->xdict){mmtrie_clean(kindex->xdict);}
#ifdef HAVE_SCWS
        for(i = 0; i < kindex->nqsegmentors; i++)
        {
            ((scws_t)(kindex->qsegmentors[i]))->r = NULL;
            ((scws_t)(kindex->qsegmentors[i]))->d = NULL;
            scws_free((scws_t)(kindex->qsegmentors[i]));
        }
        if(kindex->segmentor) scws_free((scws_t)kindex->segmentor);
#endif
        if(kindex->state) munmap(kindex->state, sizeof(KSTATE));
        if(kindex->statefd > 0) close(kindex->statefd);
        MUTEX_DESTROY(kindex->mutex_segmentor);
        MUTEX_DESTROY(kindex->mutex);
        LOGGER_CLEAN(kindex->logger);
        xmm_free(kindex, sizeof(KINDEX));
    }
    return ;
}

#ifdef _DEBUG_KINDEX
int main()
{
    KINDEX *kindex = NULL;
    XINDEX *xindex = NULL;

    if((kindex = (KINDEX *)kindex_init()))
    {
        kindex_set_basedir(kindex, "/data/qtask");
        kindex_set_dict(kindex, "/var/dict/dict.utf8.xdb", "UTF-8", "/etc/rules.utf8.ini");
        if(kindex_set_qtask_server(kindex, "127.0.0.1", 2066, 1, 2) != 0)
        {
            fprintf(stderr, "set_qtask_server() failed\n");
            _exit(-1);
        }
        if(kindex_set_source_db(kindex, "127.0.0.1", 27017, "search", "items", "id", "property", 
                    "text_index", "int_index", "long_index", "double_index", "display") != 0)
        {
            fprintf(stderr, "set_source_db() failed\n");
            _exit(-1);
        }
        if(kindex_set_res_db(kindex, "127.0.0.1", 27017, "search", "index", "id", "block") != 0)
        {
            fprintf(stderr, "set_res_db() failed\n");
            _exit(-1);
        }
        if((xindex = xindex_new(kindex)))
        {
            while(kindex_work(kindex, xindex) == 0);
            xindex_clean(xindex);
        }
        kindex_clean(kindex);
    }
    return -1;
}
//gcc -o kindex kindex.c utils/*.c -Iutils/ -D_DEBUG_KINDEX -DHAVE_MMAP -DHAVE_SCWS -DHAVE_PTHREAD -ldbase -lmtask -lscws -lz
#endif
