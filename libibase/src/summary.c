#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <sys/mman.h>
#include "ibase.h"
#include "logger.h"
#include "timer.h"
#include "mtree64.h"
#include "zvbcode.h"
#include "db.h"
#include "xmm.h"
void ibase_push_iblock(IBASE *ibase, IBLOCK *iblock);
IBLOCK *ibase_pop_iblock(IBASE *ibase);
#ifndef LLI
#define LLI(x) ((long long int) x)
#endif
#define UZPOS(map, wblocks, x, k, m, np)                                                    \
do                                                                                          \
{                                                                                           \
    if(wblocks[x].p < wblocks[x].end)                                                       \
    {                                                                                       \
        m = 0;                                                                              \
        np = &m;                                                                            \
        UZVBCODE(wblocks[x].p, k, np);                                                      \
        wblocks[x].last += m;                                                               \
        if(m >= 0){MTREE64_ADD(map, wblocks[x].last, x, NULL);}                             \
    }                                                                                       \
}while(0)
#define ADDPOS(block, x, xpos, nterm, nscore)                                               \
do                                                                                          \
{                                                                                           \
    if(block->count < IB_POS_MAX)                                                           \
    {                                                                                       \
        if(block->last_size > 0 && (block->last_pos + block->last_size) == xpos)            \
        {                                                                                   \
            block->score += IB_INT_SCORE(IB_SUMMARY_SCORE);                                 \
        }                                                                                   \
        block->last_pos   = xpos;                                                           \
        block->last_size  = nterm;                                                          \
        block->poslist[block->count].pos = xpos - block->from;                              \
        block->poslist[block->count].len = nterm;                                           \
        block->count++;                                                                     \
        if(block->list[x] == 0){block->list[x] = 1;block->uniq++;}                          \
        block->score += IB_INT_SCORE(nscore);                                               \
    }                                                                                       \
}while(0)
/* new block */
#define NEWIBLOCK(xbase, block, wb, x, xpos, ps, p, start, end)                             \
do                                                                                          \
{                                                                                           \
    ps = start + xpos;                                                                      \
    p = ps + wb.nterm;                                                                      \
    while(ps > start && *(ps-1) != '\n')--ps;                                               \
    while(p < end && *p != '\n')++p;                                                        \
    if((p - ps) > 0 && (block = ibase_pop_iblock(xbase)))                                   \
    {                                                                                       \
        block->from = (ps - start);                                                         \
        block->len  = p - ps;                                                               \
        ADDPOS(block, x, xpos, wb.nterm, wb.score);                                         \
    }                                                                                       \
}while(0)
/* push block to rmap */
#define PUSHIBLOCK(rmap, block)                                                             \
do                                                                                          \
{                                                                                           \
    block->score += IB_INT_SCORE(block->uniq);                                              \
    MTREE64_PUSH(rmap, block->score, block);                                                \
    block = NULL;                                                                           \
}while(0)
#define ESCAPECP(s, es, out)                                                                \
do                                                                                          \
{                                                                                           \
    while(s < es)                                                                           \
    {                                                                                       \
        if(*s == '\r' || *s == '\n' || *s == '\0' || *s == '\t')++s;                        \
        else if(*s == '\''){out += sprintf(out, "&#39;");++s;}                              \
        else if(*s == '"'){out += sprintf(out, "&#34;");++s;}                               \
        else if(*s == '\\'){*out++ = '\\';*out++ = '\\';++s;}                               \
        else *out++ = *s++;                                                                 \
    }                                                                                       \
}while(0)
#define ESCAPECP_PHRASE(s, es, p, out)                                                      \
do                                                                                          \
{                                                                                           \
    p = out;                                                                                \
    while(s < es)                                                                           \
    {                                                                                       \
        if(*s == '\n' && (p - out) >= IB_PHRASE_LIMIT)break;                                \
        if(*s == '\r' || *s == '\n' || *s == '\0' || *s == '\t')++s;                        \
        else if(*s == '\''){p += sprintf(p, "&#39;");++s;}                                  \
        else if(*s == '"'){p += sprintf(p, "&#34;");++s;}                                   \
        else if(*s == '\\'){*p++ = '\\';*p++ = '\\';++s;}                                   \
        else *p++ = *s++;                                                                   \
    }                                                                                       \
}while(0)
#define EHIGHLIGHT(block, i, base, p, pp, epp, out, start_tag, end_tag)                     \
do                                                                                          \
{                                                                                           \
    pp = base + block->from;                                                                \
    epp = pp + block->len;                                                                  \
    i = 0;                                                                                  \
    do                                                                                      \
    {                                                                                       \
        p = base + block->from + block->poslist[i].pos;                                     \
        ESCAPECP(pp, p, out);                                                               \
        out += sprintf(out, "%s%.*s%s", start_tag, block->poslist[i].len, pp, end_tag);     \
        pp += block->poslist[i].len;                                                        \
    }while(++i < block->count);                                                             \
    ESCAPECP(pp, epp, out);                                                                 \
}while(0)

/* push block */
void ibase_push_block(IBASE *ibase, char *block)
{
    int x = 0;

    if(ibase && block)
    {
        MUTEX_LOCK(ibase->mutex_block);
        if(ibase->nqblocks < IB_BLOCKS_MAX)
        {
            x = ibase->nqblocks++;
            ibase->qblocks[x] = block;
        }
        else
        {
            xmm_free(block, IB_DOCUMENT_MAX);
        }
        MUTEX_UNLOCK(ibase->mutex_block);
    }
    return ;
}

/* ibase pop block */
char *ibase_pop_block(IBASE *ibase)
{
    char *block = NULL;
    int x = 0;

    if(ibase)
    {
        MUTEX_LOCK(ibase->mutex_block);
        if(ibase->nqblocks > 0)
        {
            x = --(ibase->nqblocks);
            block = ibase->qblocks[x];
            //memset(block, 0, );
            ibase->qblocks[x] = NULL;
        }
        else
        {
            block = (char *)xmm_new(IB_DOCUMENT_MAX);
        }
        MUTEX_UNLOCK(ibase->mutex_block);
    }
    return block;
}

/* push iblock */
void ibase_push_iblock(IBASE *ibase, IBLOCK *iblock)
{
    int x = 0;

    if(ibase && iblock)
    {
        MUTEX_LOCK(ibase->mutex_iblock);
        if(ibase->nqiblocks < IB_IBLOCKS_MAX)
        {
            x = ibase->nqiblocks++;
            ibase->qiblocks[x] = iblock;
        }
        else
        {
            xmm_free(iblock, sizeof(IBLOCK));
        }
        MUTEX_UNLOCK(ibase->mutex_iblock);
    }
    return ;
}

/* ibase pop block */
IBLOCK *ibase_pop_iblock(IBASE *ibase)
{
    IBLOCK *iblock = NULL;
    int x = 0;

    if(ibase)
    {
        MUTEX_LOCK(ibase->mutex_iblock);
        if(ibase->nqiblocks > 0)
        {
            x = --(ibase->nqiblocks);
            iblock = ibase->qiblocks[x];
            memset(iblock, 0, sizeof(IBLOCK));
            ibase->qiblocks[x] = NULL;
        }
        else
        {
            iblock = (IBLOCK *)xmm_mnew(sizeof(IBLOCK));
        }
        MUTEX_UNLOCK(ibase->mutex_iblock);
    }
    return iblock;
}

/* summary */
char *ibase_summary(IBASE *ibase, int docid, IDISPLAY *displaylist, IHITS *hits, void *map, 
        void *mtop, DOCHEADER *docheader, XFIELD *fields, STERM *termlist, 
        char *content, int content_end, char *start_tag, char *end_tag, char *out)
{
    int i = 0, j = 0, no = 0, x = 0, k = 0, n = 0, *np = NULL, fid = 0, xpos = 0, pos = 0, 
    field_start = 0, field_end = 0;
    char *start = NULL, *end = NULL, *p = NULL, *ps = NULL, *pp = NULL, 
         *epp = NULL, *base =  NULL, *pps = NULL;
    int64_t key = 0, data = 0, *pkey = &key, *pdata = &data;
    WBLOCK  wblocks[IB_FIELDS_MAX];
    IBLOCK  *iblock =  NULL;

    if(ibase && displaylist && map && mtop && docheader && fields && termlist && content)
    {
        //memset(wblocks, 0, sizeof(WBLOCK) * IB_FIELDS_MAX);
        MTREE64_RESET(map);
        MTREE64_RESET(mtop);
        /* push first postion to map */
        for(x = 0; x < hits->nhits; x++)
        {
            no = hits->weights[x].no;
            wblocks[x].nterm =  termlist[no].term_len;
            wblocks[x].score =  IB_INT_SCORE(hits->weights[x].score);
            wblocks[x].p = (char *)(&(termlist[no])) + termlist[no].posting_offset;
            wblocks[x].end =  wblocks[x].p + termlist[no].posting_size;
            wblocks[x].last = 0;
            UZPOS(map, wblocks, x, k, n, np);
        }
        /* move position window */
        content_end = strlen(content);
        fid = 0;
        pos = -1;
        x = 0;
        field_start = 0;
        field_end = 0;
        if(MTREE64_TOTAL(map) > 0)
        {
            if(MTREE64_POP_MIN(map, pkey, pdata) == 0)
            {
                pos = (int)key;
                x = (int)data;
                UZPOS(map, wblocks, x, k, n, np);
            }
        }
        while(fid < docheader->nfields)
        {
            field_start = fields[fid].from;
            if((fid +1) < docheader->nfields)
            {
                field_end = fields[fid+1].from;
            }
            else
            {
                field_end = content_end; 
            }
            start = content + field_start;
            end = content + field_end;
            iblock = NULL;
            while(pos >= field_start && pos < field_end)
            {
                if(x >= 0 && (displaylist[fid].flag & IB_IS_HIGHLIGHT))
                {
                    xpos = pos - field_start;
                    if(iblock && xpos >= (iblock->from + iblock->len))
                    {
                        PUSHIBLOCK(mtop, iblock);
                        iblock = NULL;
                    }
                    // find new block 
                    if(iblock == NULL)
                    {
                        NEWIBLOCK(ibase, iblock, wblocks[x], x, xpos, ps, p, start, end);
                    }
                    else
                    {
                        ADDPOS(iblock, x, xpos, wblocks[x].nterm, wblocks[x].score);
                    }
                }
                if(MTREE64_TOTAL(map) > 0)
                {
                    if(MTREE64_POP_MIN(map, pkey, pdata) == 0)
                    {
                        pos = (int)key;
                        x = (int)data;
                        UZPOS(map, wblocks, x, k, n, np);
                    }else break;
                }else break;

            }
            //check last iblock
            if(iblock){PUSHIBLOCK(mtop, iblock);}
            //out field data
            if((displaylist[fid].flag & IB_IS_DISPLAY))
            {
                //int_field_from = ibase->state->int_index_from;
                //int_field_from = ibase->state->int_index_from;
                //long_field_from = ibase->state->long_index_from;
                //long_field_to = long_field_from + ibase->state->long_index_fields_num;
                //double_field_from = ibase->state->double_index_from;
                //double_field_to = double_field_from + ibase->state->double_index_fields_num;
                out += sprintf(out, "\"%d\":\"", fid);
                if((displaylist[fid].flag & IB_IS_HIGHLIGHT) && MTREE64_TOTAL(mtop) > 0)
                {
                    // generate summary 
                    j = 0;
                    do
                    {
                        if((MTREE64_POP_MAX(mtop, pkey, pdata)) == 0 && j < IB_PHRASE_LIMIT)
                        {
                            iblock = (IBLOCK *)((long)data);
                            // mark highlight 
                            pps = pp;
                            base = content + field_start;
                            EHIGHLIGHT(iblock, i, base, p, pp, epp, out, start_tag, end_tag);
                            j += pp - pps;
                        }
                        ibase_push_iblock(ibase, iblock);
                    }while(MTREE64_TOTAL(mtop) > 0);
                }
                else
                {
                    pp = start;
                    epp = end;
                    if((displaylist[fid].flag & IB_IS_HIGHLIGHT))
                    {
                        ESCAPECP_PHRASE(pp, epp, pps, out);
                        out = pps;
                    }
                    else
                    {
                        ESCAPECP(pp, epp, out);
                    }
                }
                *out++ = '"';
                *out++ = ',';
            }
            fid++;
        }
    }
    return out;
}

/* read summary */
int ibase_read_summary(IBASE *ibase, IQSET *qset, IRECORD *records, char *summary, 
        char *highlight_start, char *highlight_end)
{
    void *hitsmap = NULL, *map = NULL, *mtop = NULL, *timer = NULL;
    char *pp = NULL, *p = NULL, *zblock = NULL, *block = NULL, *content = NULL, 
         *zdata = NULL, *out = NULL, *source = NULL;
    int64_t data = 0, *pdata = &data;
    int ret = -1, i = 0, j = 0, x = 0, z = 0, docid = 0;
    int id_time = 0, io_time = 0, p_time = 0, map_time = 0, sum_time = 0;
    size_t nzdata = 0, ndata = 0;
    DOCHEADER *docheader = NULL;
    //IDISPLAY *displaylist = NULL;
    STERM *termlist = NULL;
    XFIELD *fields = NULL;
    IRES *res = NULL;
    IHITS hits = {0};

    if(ibase && qset && qset->count > 0 && records && summary)
    {
        res = &(qset->res);
        //displaylist = qset->displaylist;
        hitsmap = ibase_pop_stree(ibase);
        map = ibase_pop_stree(ibase);
        mtop = ibase_pop_stree(ibase);
        block = ibase_pop_block(ibase);
        zblock = ibase_pop_block(ibase);
        if(map == NULL || mtop == NULL || hitsmap == NULL 
                || block == NULL || zblock == NULL) goto end;
        if(highlight_start == NULL) highlight_start = "";
        if(highlight_end == NULL) highlight_end = "";
        TIMER_INIT(timer);
        if((p = summary))
        {
            p += sprintf(p, "({\"qid\":\"%d\", \"io\":\"%d\", \"sort\":\"%d\", "
                    "\"doctotal\":\"%d\",\"total\":\"%d\", \"count\":\"%d\",", 
                    res->qid, res->io_time, res->sort_time, 
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
                                IB_LONG2FLOAT(res->groups[i].val), IBLL(res->groups[i].val));
                    }
                    else
                    {
                        p += sprintf(p, "\"%lld\":\"%lld\",",
                                IBLL(res->groups[i].key), IBLL(res->groups[i].val));
                    }
                }
                --p;
                p += sprintf(p, "},");
            }
            p += sprintf(p, "\"records\":{");
            pp = p;
            MTREE64_RESET(hitsmap);
            for(j = 0; j < qset->nqterms; j++)
            {
                MTREE64_ADD(hitsmap, qset->qterms[j].id, j, NULL);
            }
            for(i = 0; i < qset->count; i++)
            {
                TIMER_SAMPLE(timer);
                if((docid = ibase_localid(ibase, (int64_t)records[i].globalid)) > 0 
                        && docid <= ibase->state->docid && (source = zblock))

                {
                    TIMER_SAMPLE(timer);
                    id_time += PT_LU_USEC(timer);
                    memset(zblock, 0, sizeof(DOCHEADER));
                    if(db_read_data(PDB(ibase->source), docid, zblock) <= 0) 
                    {
                        FATAL_LOGGER(ibase->logger, "read source docid:%d globalid:%lld failed,%s", docid, IBLL(records[i].globalid), strerror(errno));
                        continue;
                    }
                    TIMER_SAMPLE(timer);
                    io_time += PT_LU_USEC(timer);
                    docheader = (DOCHEADER *)source;
                    zdata = source + docheader->content_off;
                    ndata = IB_DOCUMENT_MAX;
                    nzdata = docheader->content_zsize;
                    if((nzdata = docheader->content_zsize) > 0)
                    {
                        DEBUG_LOGGER(ibase->logger, "Ready for uncompress data %u to %d", nzdata, docheader->content_size);
                        block[docheader->content_size] = '\0';
                        if(uncompress((Bytef *)block, (uLongf *)&ndata, 
                                    (Bytef *)zdata, (uLong)nzdata) == -1) 
                        {
                            DEBUG_LOGGER(ibase->logger, "uncompress %d failed, %s", 
                                    i, strerror(errno));
                            continue;
                        }
                        content = block;
                        DEBUG_LOGGER(ibase->logger, "Over uncompress data %u to %d", nzdata, docheader->content_size);
                    }
                    else
                    {
                        //ACCESS_LOGGER(ibase->logger, "No uncompress data %u to %d", nzdata, docheader->content_size);
                        content = zdata;
                    }
                    content[docheader->content_size] = '\0';
                    fields = (XFIELD *)(source + sizeof(DOCHEADER));
                    termlist = (STERM *)((char *)fields + sizeof(XFIELD) * docheader->nfields);
                    //posting = (char *)termlist + sizeof(STERM) * docheader->nterms; 
                    p += sprintf(p, "\"%d\":{\"id\":\"%lld\", \"rank\":\"%f\", "
                            "\"dbid\":\"%d\",\"category\":\"%lld\", \"slevel\":\"%d\","
                            "\"score\":\"%lld\", \"summary\":{", i, IBLL(docheader->globalid), 
                            docheader->rank, docheader->dbid, LLI(docheader->category), 
                            docheader->slevel, IBLL(records[i].score));
                    memset(&hits, 0, sizeof(IHITS));
                    TIMER_SAMPLE(timer);
                    p_time += PT_LU_USEC(timer);
                    for(j = 0; j < docheader->nterms; j++)
                    {
                        if(MTREE64_GET(hitsmap, termlist[j].termid, pdata) == 0)
                        {
                            x = (int)data;
                            z = hits.nhits++;
                            hits.weights[z].no = j;
                            hits.weights[z].score = (double)(termlist[j].term_count)/
                                (double)docheader->terms_total;
                            if(qset->qterms[x].idf > 0.0f)
                                hits.weights[z].score *= qset->qterms[x].idf;
                        }
                    }
                    TIMER_SAMPLE(timer);
                    map_time += PT_LU_USEC(timer);
                    out = ibase_summary(ibase, docid, qset->displaylist, &hits, map, mtop, 
                            docheader, fields, termlist, content, docheader->content_size, 
                            highlight_start, highlight_end, p);
                    if(out > p) p = --out;
                    p += sprintf(p, "}},");
                    TIMER_SAMPLE(timer);
                    sum_time += PT_LU_USEC(timer);
                    /* check last position */
                }
                else
                {
                    ERROR_LOGGER(ibase->logger, "read records[%lld][%u/%d] last:%d failed, %s", i, IBLL(records[i].globalid), docid, ibase->state->docid, strerror(errno));
                }
            }
            if(pp != p) --p; 
            p += sprintf(p, "}})");
            ret = p - summary;
        }
        TIMER_SAMPLE(timer);
        ACCESS_LOGGER(ibase->logger, "read_summary(qid:%d count:%d, id_time:%d io_time:%d p_time:%d map_time:%d sum_time:%d) time used:%lld", res->qid, qset->count, id_time, io_time, p_time, map_time, sum_time, PT_USEC_U(timer));
end:
        if(block){ibase_push_block(ibase, block);}
        if(zblock){ibase_push_block(ibase, zblock);}
        if(hitsmap){ibase_push_stree(ibase, hitsmap);}
        if(map){ibase_push_stree(ibase, map);}
        if(mtop){ibase_push_stree(ibase, mtop);}
        TIMER_CLEAN(timer);
    }
    return ret;
}

/* bound items */
int ibase_bound_items(IBASE *ibase, int count)
{
    int ret = -1;
    if(ibase && count > 0 && ibase->source && PDB(ibase->source)->state)
    {
        ret = count * PDB(ibase->source)->state->data_len_max;
    }
    return ret;
}

/* read items */
int ibase_read_items(IBASE *ibase, int64_t *list, int count, char *out)
{
    char *pp = NULL, *epp = NULL, *p = NULL, *zblock = NULL, *block = NULL, 
         *content = NULL, *zdata = NULL,*source = NULL;
    int ret = -1, i = 0, j = 0, docid = 0;
    size_t nzdata = 0, ndata = 0;
    DOCHEADER *docheader = NULL;
    XFIELD *fields = NULL;

    if(ibase && list && count > 0 && (p = out) && (block = ibase_pop_block(ibase)))
    {
        if((zblock = ibase_pop_block(ibase)))
        {
            p += sprintf(p, "({");
            for(i = 0; i < count; i++)
            {
                if((docid = ibase_localid(ibase, list[i])) > 0
                        && docid <= ibase->state->docid && (source = zblock))
                {
                    if(db_read_data(PDB(ibase->source), docid, zblock) <= 0) 
                    {
                        FATAL_LOGGER(ibase->logger, "read source docid:%d globalid:%lld failed,%s", docid, IBLL(list[i]), strerror(errno));
                        continue;
                    }
                    docheader = (DOCHEADER *)source;
                    zdata = source + docheader->content_off;
                    ndata = IB_DOCUMENT_MAX;
                    nzdata = docheader->content_zsize;
                    if((nzdata = docheader->content_zsize) > 0)
                    {
                        block[docheader->content_size] = '\0';
                        if(uncompress((Bytef *)block, (uLongf *)&ndata, 
                                    (Bytef *)zdata, (uLong)nzdata) == -1) 
                        {
                            DEBUG_LOGGER(ibase->logger, "uncompress %d failed, %s", 
                                    i, strerror(errno));
                            continue;
                        }
                        content = block;
                    }
                    else
                    {
                        content = zdata;
                    }
                    content[docheader->content_size] = '\0';
                    fields = (XFIELD *)(source + sizeof(DOCHEADER));
                    p += sprintf(p, "\"%lld\":{\"status\":\"%d\", \"crc\":\"%d\", \"rank\":\"%f\", "
                            "\"category\":\"%lld\", \"slevel\":\"%d\", \"nfields\":\"%d\", "
                            "\"dbid\":\"%d\",\"fields\":{", 
                            IBLL(docheader->globalid), docheader->status, 
                            docheader->crc, docheader->rank, LLI(docheader->category), 
                            docheader->slevel, docheader->nfields, docheader->dbid);
                    for(j = 0; j < docheader->nfields; j++)
                    {
                        pp = content + fields[j].from;
                        p += sprintf(p, "\"%d\":\"", j);
                        if((j+1) == docheader->nfields)
                        {
                            epp = content + docheader->content_size;
                            ESCAPECP(pp, epp, p);
                            *p++ = '"';
                        }
                        else
                        {
                            epp = content + fields[j + 1].from;
                            ESCAPECP(pp, epp, p);
                            *p++ = '"';
                            *p++ = ',';
                        }
                    }
                    if((i+1) == count)
                        p += sprintf(p, "}}");
                    else
                        p += sprintf(p, "}},");
                }
            }
            if(*(p-1) == ',') --p;
            *p++ = '}';
            *p++ = ')';
            *p = '\0';
            ret = p - out;
            ibase_push_block(ibase, zblock);
        }
        ibase_push_block(ibase, block);
    }
    return ret;
}
