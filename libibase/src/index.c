#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <string.h>
#include <math.h>
#include <errno.h>
#include <zlib.h>
#include "ibase.h"
#include "zvbcode.h"
#include "timer.h"
#include "mmtrie.h"
#include "logger.h"
#include "xmm.h"
#include "db.h"
#define PIHEADER(ibase, docid) &(((IHEADER *)(ibase->headersio.map))[docid])
#ifndef LLI
#define LLI(x) ((long long int) x)
#endif
#ifndef SIZET
#define SIZET(x) ((size_t)x)
#endif
#define SET_INT_INDEX(ibase, docid, list, zzz)                                              \
do                                                                                          \
{                                                                                           \
    zzz = ibase->state->int_index_fields_num * docid;                                       \
    if(((off_t)zzz * (off_t)sizeof(int)) >= ibase->intidxio.end)                            \
    {                                                                                       \
        ibase->intidxio.old = ibase->intidxio.end;                                          \
        ibase->intidxio.end = (off_t)((off_t)sizeof(int) * (off_t)IB_NUMERIC_BASE           \
                * (off_t)ibase->state->int_index_fields_num                                 \
                * (off_t)((docid/IB_NUMERIC_BASE)+1));                                      \
        if(ftruncate(ibase->intidxio.fd, ibase->intidxio.end) != 0)break;                   \
        memset(ibase->intidxio.map + ibase->intidxio.old, 0,                                \
                ibase->intidxio.end - ibase->intidxio.old);                                 \
    }                                                                                       \
    memcpy(&(((int *)ibase->intidxio.map)[zzz]), list, sizeof(int) *                        \
                ibase->state->int_index_fields_num);                                        \
}while(0);
#define SET_LONG_INDEX(ibase, docid, list, zzz)                                             \
do                                                                                          \
{                                                                                           \
    zzz = ibase->state->long_index_fields_num * docid;                                      \
    if(((off_t)zzz * (off_t)sizeof(int64_t)) >= ibase->longidxio.end)                       \
    {                                                                                       \
        ibase->longidxio.old = ibase->longidxio.end;                                        \
        ibase->longidxio.end = (off_t)((off_t)sizeof(int64_t) * (off_t)IB_NUMERIC_BASE      \
                * (off_t)ibase->state->long_index_fields_num                                \
                * (off_t)((docid/IB_NUMERIC_BASE)+1));                                      \
        if(ftruncate(ibase->longidxio.fd, ibase->longidxio.end) != 0)break;                 \
        memset(ibase->longidxio.map + ibase->longidxio.old, 0,                              \
                ibase->longidxio.end - ibase->longidxio.old);                               \
    }                                                                                       \
    memcpy(&(((int64_t *)ibase->longidxio.map)[zzz]), list, sizeof(int64_t) *               \
                ibase->state->long_index_fields_num);                                       \
}while(0);
#define SET_DOUBLE_INDEX(ibase, docid, list, zzz)                                           \
do                                                                                          \
{                                                                                           \
    zzz = ibase->state->double_index_fields_num * docid;                                    \
    if(((off_t)zzz * (off_t)sizeof(double)) >= ibase->doubleidxio.end)                      \
    {                                                                                       \
        ibase->doubleidxio.old = ibase->doubleidxio.end;                                    \
        ibase->doubleidxio.end = (off_t)((off_t)sizeof(double)*(off_t)IB_NUMERIC_BASE       \
                * (off_t)ibase->state->double_index_fields_num                              \
                * (off_t)((docid/IB_NUMERIC_BASE)+1));                                      \
        if(ftruncate(ibase->doubleidxio.fd, ibase->doubleidxio.end) != 0)break;             \
        memset(ibase->doubleidxio.map + ibase->doubleidxio.old, 0,                          \
                ibase->doubleidxio.end - ibase->doubleidxio.old);                           \
    }                                                                                       \
    memcpy(&(((double *)ibase->doubleidxio.map)[zzz]), list, sizeof(double) *               \
                ibase->state->double_index_fields_num);                                     \
}while(0);

#define ADD_TERMSTATE(ibase, xtermid)                                                       \
do                                                                                          \
{                                                                                           \
    if(ibase->state && ibase->termstateio.fd > 0)                                           \
    {                                                                                       \
        if(((off_t)xtermid * (off_t)sizeof(TERMSTATE)) >= ibase->termstateio.end)           \
        {                                                                                   \
            ibase->termstateio.old = ibase->termstateio.end;                                \
            ibase->termstateio.end = ((off_t)((xtermid / IB_TERMSTATE_BASE) + 1)            \
                * (off_t)IB_TERMSTATE_BASE * (off_t)sizeof(TERMSTATE));                     \
            if(ftruncate(ibase->termstateio.fd, ibase->termstateio.end) != 0)break;         \
            memset(ibase->termstateio.map+ibase->termstateio.old, 0,                        \
                    ibase->termstateio.end - ibase->termstateio.old);                       \
        }                                                                                   \
        if(xtermid > ibase->state->termid) ibase->state->termid = xtermid;                  \
    }                                                                                       \
}while(0)
#define UPDATE_TERMSTATE(ibase, termid)                                                     \
do                                                                                          \
{                                                                                           \
    if(ibase->termstateio.map)                                                              \
    {                                                                                       \
        ((TERMSTATE *)(ibase->termstateio.map))[termid].total++;                            \
    }                                                                                       \
}while(0)
/*
int ibase_set_int_index(IBASE *ibase, DOCHEADER *docheader)
{
    int ret = -1, n = 0, *idx = NULL, *list = NULL;

    if(ibase && docheader && (n = (int)(docheader->intblock_size/sizeof(int))) > 0)
    {
        list =  
        docheader->intindex_from, n
    }
    return ret;
}
*/
/* check index state */
int ibase_check_index_state(IBASE *ibase, DOCHEADER *docheader)
{
    int ret = -1, n = 0;
    if(ibase && docheader)
    {
        if(ibase->state->int_index_fields_num == 0 
                && (n = (int)(docheader->intblock_size/sizeof(int))) > 0)
        {
            WARN_LOGGER(ibase->logger, "set_int_index(from:%d,num:%d) globalid:%lld", docheader->intindex_from, n, IBLL(docheader->globalid));
            ibase_set_int_index(ibase, docheader->intindex_from, n);
        }
        if(ibase->state->long_index_fields_num == 0 
                && (n = (int)(docheader->longblock_size/sizeof(int64_t))) > 0)
        {
            ibase_set_long_index(ibase, docheader->longindex_from, n);
            WARN_LOGGER(ibase->logger, "set_long_index(from:%d,num:%d) globalid:%lld", docheader->longindex_from, n, IBLL(docheader->globalid));
        }
        if(ibase->state->double_index_fields_num == 0 
                && (n = (int)(docheader->doubleblock_size/sizeof(double))) > 0)
        {
            WARN_LOGGER(ibase->logger, "set_double_index(from:%d,num:%d) docid:%lld", docheader->doubleindex_from, n, IBLL(docheader->globalid));
            ibase_set_double_index(ibase, docheader->doubleindex_from, n);
        }
        if((docheader->intblock_size/sizeof(int)) != ibase->state->int_index_fields_num
                || (docheader->longblock_size/sizeof(int64_t)) != ibase->state->long_index_fields_num
                || (docheader->doubleblock_size/sizeof(double)) != ibase->state->double_index_fields_num)
        {
            FATAL_LOGGER(ibase->logger, "Invalid document globalid:%lld int/long/double index num:%d/%d/%d old_index_int/long/double:%d/%d/%d", IBLL(docheader->globalid), (int)(docheader->intblock_size/sizeof(int)), (int)(docheader->longblock_size/sizeof(int64_t)), (int)(docheader->doubleblock_size/sizeof(double)), ibase->state->int_index_fields_num, ibase->state->long_index_fields_num, ibase->state->double_index_fields_num);
        }
        else ret = 0;
    }
    return ret;
}

/* add index */
int ibase_index(IBASE *ibase, int docid, IBDATA *block)
{
    char *term = NULL, buf[IB_BUF_SIZE], *data = NULL, *p = NULL, 
         *pp = NULL, *end = NULL, *prevnext = NULL;
    int i = 0, termid = 0, n = 0, ndocid = 0, ret = -1, ndata = 0, 
        x = 0, *intlist = NULL, *np = NULL;
    DOCHEADER *docheader = NULL;
    int64_t *longlist = NULL;
    double *doublelist = NULL;
    IHEADER *iheader = NULL;
    STERM *termlist = NULL;
    off_t size = 0;

    if((docheader = (DOCHEADER *)block->data) && ibase_check_index_state(ibase, docheader) == 0)
    {
        ibase->state->dtotal++;
        ibase->state->ttotal += (off_t)docheader->terms_total;
        if(ibase->state->used_for == IB_USED_FOR_INDEXD)
        {
            /* add to source */
            if(((off_t)docid * (off_t)sizeof(IHEADER)) >= ibase->headersio.end)
            {
                ibase->headersio.old = ibase->headersio.end;
                size = (off_t)((docid / IB_HEADERS_BASE) + 1) 
                    * (off_t)IB_HEADERS_BASE * (off_t)sizeof(IHEADER);
                ret = ftruncate(ibase->headersio.fd, size);
                ibase->headersio.end = size;
                memset(ibase->headersio.map + ibase->headersio.old, 0, 
                        ibase->headersio.end -  ibase->headersio.old);
            }
            if((iheader = PIHEADER(ibase, docid)))
            {
                iheader->status      = docheader->status;
                iheader->terms_total = docheader->terms_total;
                iheader->crc         = docheader->crc;
                iheader->category    = docheader->category;
                iheader->slevel      = docheader->slevel;
                iheader->rank        = docheader->rank;
                iheader->globalid    = docheader->globalid;
                //WARN_LOGGER(ibase->logger, "iheader->category:%p docheader->category:%p", (void *)iheader->category, (void *)docheader->category);
            }
        }
        /* index */
        end = block->data + block->ndata;
        termlist = (STERM *)(block->data + sizeof(DOCHEADER) 
                + docheader->nfields * sizeof(XFIELD));
        term = block->data + docheader->textblock_off;
        prevnext = block->data + docheader->prevnext_off;
        for(i = 0; i < docheader->nterms; i++)
        {
            termid = termlist[i].termid;
            //find/add termid
            if(termid > 0 && termlist[i].term_len > 0 && termlist[i].prevnext_size >= 0 
                && (ret=mmtrie_add((MMTRIE *)ibase->mmtrie,term,termlist[i].term_len,termid))>0)
            {
                MUTEX_LOCK(ibase->mutex_termstate);
                if(termid > ibase->state->termid){ADD_TERMSTATE(ibase, termid);}
                ((TERMSTATE *)(ibase->termstateio.map))[termid].len = termlist[i].term_len;
                ndocid = docid - ((TERMSTATE *)(ibase->termstateio.map))[termid].last_docid;
                ((TERMSTATE *)(ibase->termstateio.map))[termid].last_docid = docid;
                ((TERMSTATE *)(ibase->termstateio.map))[termid].total++;
                MUTEX_UNLOCK(ibase->mutex_termstate);
                if(ibase->state->index_status != IB_INDEX_DISABLED)
                {
                    p = pp = buf;
                    if((ndata = ((sizeof(int) * 5 + termlist[i].prevnext_size))) > IB_BUF_SIZE)
                        p = pp = data = (char *)xmm_new(ndata);
                    /* compress index */
                    if(ibase->state->compression_status != IB_COMPRESSION_DISABLED)
                    {
                        n = ndocid; np = &n; ZVBCODE(np, p);
                        n = termlist[i].term_count; np = &n;  ZVBCODE(np, p);
                        n = i; np = &n; ZVBCODE(np, p);
                        n = termlist[i].bit_fields; np = &n; ZVBCODE(np, p);
                        n = termlist[i].prevnext_size; np = &n; ZVBCODE(np, p);
                    }
                    else
                    {
                        memcpy(p, &docid, sizeof(int));p += sizeof(int);
                        memcpy(p, &(termlist[i].term_count), sizeof(int));p += sizeof(int);
                        memcpy(p, &i, sizeof(int));p += sizeof(int);
                        memcpy(p, &(termlist[i].bit_fields), sizeof(int));p += sizeof(int);
                        memcpy(p, &(termlist[i].prevnext_size), sizeof(int));p += sizeof(int);
                    }
                    if(termlist[i].prevnext_size > 0) 
                    {
                        memcpy(p, prevnext, termlist[i].prevnext_size);
                        p += termlist[i].prevnext_size;
                    }
                    if(db_add_data(PDB(ibase->index), termid, pp, (p - pp)) <= 0)
                    {
                        FATAL_LOGGER(ibase->logger, "index term[%d] failed, %s", 
                                termid, strerror(errno));
                        if(data){free(data); data = NULL;}
                        _exit(-1);
                    }
                    if(data){xmm_free(data, ndata); data = NULL;}
                }
            }
            else 
            {
                FATAL_LOGGER(ibase->logger, "Invalid ret:%d term[%d]{%.*s} termid:%d in doc:%d", ret, i, termlist[i].term_len, term, termid, docid);
                _exit(-1);
            }
            term += termlist[i].term_len;
            prevnext += termlist[i].prevnext_size;
        }
        if(ibase->state->used_for == IB_USED_FOR_INDEXD)
        {
            /* index int */
            if(ibase->state->int_index_fields_num > 0 
                    && (intlist = (int *)(block->data + docheader->intblock_off)))
            {
                SET_INT_INDEX(ibase, docid, intlist, x);
            }
            /* index long */
            if(ibase->state->long_index_fields_num > 0 
                    && (longlist = (int64_t *)(block->data + docheader->longblock_off)))
            {
                SET_LONG_INDEX(ibase, docid, longlist, x);
            }
            /* index double */
            if(ibase->state->double_index_fields_num > 0 
                    && (doublelist = (double *)(block->data + docheader->doubleblock_off)))
            {
                SET_DOUBLE_INDEX(ibase, docid, doublelist, x);
            }
        }
        if(ibase->state->used_for != IB_USED_FOR_QPARSERD)
        {
            docheader->size = docheader->prevnext_off;
            ret = db_set_data(PDB(ibase->source), docid, block->data, docheader->size);
        }
        ret = 0;
    }
    return ret;
}

/* updated index */
int ibase_update_index(IBASE *ibase, int docid, IBDATA *block)
{
    int ret = -1, x = 0, *intlist = NULL;
    DOCHEADER *docheader = NULL;
    //TERMSTATE *termstate = NULL;
    double *doublelist = NULL;
    int64_t *longlist = NULL;
    IHEADER *iheader = NULL;
    //STERM *termlist = NULL;
    off_t size = 0;

    if(ibase && block && block->data && block->ndata > 0 
            && (docheader =  (DOCHEADER *)(block->data))
            && ibase_check_index_state(ibase, docheader) == 0)
    {
        if(ibase->state->used_for == IB_USED_FOR_INDEXD)
        {
            if(((off_t)docid * (off_t)sizeof(IHEADER)) >= ibase->headersio.end)
            {
                ibase->headersio.old = ibase->headersio.end;
                size = (off_t)((docid / IB_HEADERS_BASE) + 1) 
                    * (off_t)IB_HEADERS_BASE * (off_t)sizeof(IHEADER);
                ret = ftruncate(ibase->headersio.fd, size);
                ibase->headersio.end = size;
                memset(ibase->headersio.map + ibase->headersio.old, 0, 
                        ibase->headersio.end -  ibase->headersio.old);
            }
            if((iheader = PIHEADER(ibase, docid)))
            {
                iheader->status      = docheader->status;
                iheader->terms_total = docheader->terms_total;
                iheader->category    = docheader->category;
                iheader->slevel      = docheader->slevel;
                iheader->rank        = docheader->rank;
                iheader->globalid    = docheader->globalid;
                iheader->crc         = docheader->crc;
                if(ibase->state->used_for == IB_USED_FOR_INDEXD)
                {
                    /* index int */
                    if(ibase->state->int_index_fields_num > 0 
                            && (intlist = (int *)(block->data + docheader->intblock_off)))
                    {
                        SET_INT_INDEX(ibase, docid, intlist, x);
                    }
                    /* index long */
                    if(ibase->state->long_index_fields_num > 0 
                            && (longlist = (int64_t *)(block->data + docheader->longblock_off)))
                    {
                        SET_LONG_INDEX(ibase, docid, longlist, x);
                    }
                    /* index double */
                    if(ibase->state->double_index_fields_num > 0 
                            && (doublelist = (double *)(block->data + docheader->doubleblock_off)))
                    {
                        SET_DOUBLE_INDEX(ibase, docid, doublelist, x);
                    }
                }
            }
        }   
        if(ibase->state->used_for != IB_USED_FOR_QPARSERD)
        {
            docheader->size = docheader->prevnext_off;
            ret = db_set_data(PDB(ibase->source), docid, block->data, docheader->size);
        }
        ret = 0;
    }
    return ret;
}

/* del index */
int ibase_del_index(IBASE *ibase, int docid)
{
    IHEADER *iheader = NULL;
    int ret = -1;

    if((iheader = PIHEADER(ibase, docid))) 
    {
        ibase->state->dtotal--;
        ibase->state->ttotal -= iheader->terms_total;
        iheader->status = -1;
        if(ibase->state->used_for == IB_USED_FOR_INDEXD)
        {
            ret = db_del_data(PDB(ibase->source), docid);
        }
        ret = 0;        
    }
    return ret;
}

/* add document */
int ibase_add_document(IBASE *ibase, IBDATA *block)
{
    int ret = -1, docid = 0, n = 0, newid = 0;
    DOCHEADER *docheader = NULL;
    IHEADER *iheader = NULL;
    char line[IB_LINE_MAX];

    if(ibase && block && block->ndata > 0 && (docheader = (DOCHEADER *)(block->data)) 
            && docheader->globalid && (n = sprintf(line, "%lld", IBLL(docheader->globalid))))
    {
        MUTEX_LOCK(ibase->mutex);
        if((docid = mmtrie_xadd(ibase->docmap, line, n)) > 0)
        {
            if(docid <= ibase->state->docid) 
            {
                if(ibase->state->used_for == IB_USED_FOR_INDEXD)
                {
                    if((iheader = PIHEADER(ibase, docid)))
                    {
                        /* disabled */
                        if(docheader->status < 0)
                        {
                            iheader->status = -1;
                            DEBUG_LOGGER(ibase->logger, "Update docid:%d globalid:%lld status:%d", docid, IBLL(docheader->globalid), docheader->status);
                            ret = 0;
                        }
                        else if(docheader->crc != iheader->crc) 
                        {
                            ibase_del_index(ibase, docid);
                            mmtrie_del(ibase->docmap, line, n);
                            newid = mmtrie_xadd(ibase->docmap, line, n);
                            ibase->state->docid = newid;
                            DEBUG_LOGGER(ibase->logger, "over_reindex[%lld/%d]{crc:%d rank:%f slevel:%d category:%lld}", IBLL(docheader->globalid), docid, docheader->crc, docheader->rank, docheader->slevel, LLI(docheader->category));
                            ret = ibase_index(ibase, newid, block);
                            DEBUG_LOGGER(ibase->logger, "over_reindex[%lld/%d]{crc:%d rank:%f slevel:%d category:%lld}", IBLL(docheader->globalid), docid, docheader->crc, docheader->rank, docheader->slevel, LLI(docheader->category));
                        }
                        else
                        {
                            DEBUG_LOGGER(ibase->logger, "update_index[%lld/%d]{crc:%d rank:%f slevel:%d category:%lld}", IBLL(docheader->globalid), docid, docheader->crc, docheader->rank, docheader->slevel, LLI(docheader->category));
                            ret = ibase_update_index(ibase, docid, block);
                            iheader->status = 0;
                            DEBUG_LOGGER(ibase->logger, "over_update_index[%lld/%d]{crc:%d rank:%f slevel:%d category:%lld}", IBLL(docheader->globalid), docid, docheader->crc, docheader->rank, docheader->slevel, LLI(docheader->category));
                        }
                    }
                }
                else
                {
                    ret = ibase_update_index(ibase, docid, block);
                    DEBUG_LOGGER(ibase->logger, "over_update_index[%lld/%d]{crc:%d rank:%f slevel:%d category:%lld}", IBLL(docheader->globalid), docid, docheader->crc, docheader->rank, docheader->slevel, LLI(docheader->category));
                }
            }
            else
            {
                ibase->state->docid = docid;
                DEBUG_LOGGER(ibase->logger, "new_index[%lld/%d]{crc:%d rank:%f slevel:%d category:%lld}", IBLL(docheader->globalid), docid, docheader->crc, docheader->rank, docheader->slevel, LLI(docheader->category));
                ret = ibase_index(ibase, docid, block);
            }
        }
        else
        {
            FATAL_LOGGER(ibase->logger, "new_index[%lld]{crc:%d rank:%f slevel:%d category:%lld} failed, %s", IBLL(docheader->globalid), docheader->crc, docheader->rank, docheader->slevel, LLI(docheader->category), strerror(errno));

        }
        MUTEX_UNLOCK(ibase->mutex);
    }
    return ret;
}

/* update document */
int ibase_update_document(IBASE *ibase, IBDATA *block)
{
    int ret = -1, docid = 0, n = 0, newid = 0;
    DOCHEADER *docheader = NULL;
    IHEADER *iheader = NULL;
    char line[IB_LINE_MAX];

    if(ibase && block && block->ndata > 0 && (docheader = (DOCHEADER *)(block->data)) 
            && docheader->globalid && (n = sprintf(line, "%lld", IBLL(docheader->globalid))))
    {
        MUTEX_LOCK(ibase->mutex);
        if((docid = mmtrie_xadd(ibase->docmap, line, n)) > 0)
        {
            if(docid <= ibase->state->docid) 
            {
                if(ibase->state->used_for == IB_USED_FOR_INDEXD)
                {
                    if((iheader = PIHEADER(ibase, docid)))
                    {
                        /* disabled */
                        if(docheader->status < 0)
                        {
                            iheader->status = -1;
                            DEBUG_LOGGER(ibase->logger, "Update docid:%d globalid:%lld status:%d", docid, IBLL(docheader->globalid), docheader->status);
                            ret = 0;
                        }
                        else if(docheader->crc != iheader->crc) 
                        {
                            ibase_del_index(ibase, docid);
                            mmtrie_del(ibase->docmap, line, n);
                            newid = mmtrie_xadd(ibase->docmap, line, n);
                            ibase->state->docid = newid;
                            DEBUG_LOGGER(ibase->logger, "over_reindex[%lld/%d]{crc:%d rank:%f slevel:%d category:%lld}", IBLL(docheader->globalid), docid, docheader->crc, docheader->rank, docheader->slevel, LLI(docheader->category));
                            ret = ibase_index(ibase, newid, block);
                            DEBUG_LOGGER(ibase->logger, "over_reindex[%lld/%d]{crc:%d rank:%f slevel:%d category:%lld}", IBLL(docheader->globalid), docid, docheader->crc, docheader->rank, docheader->slevel, LLI(docheader->category));
                        }
                        else
                        {
                            DEBUG_LOGGER(ibase->logger, "update_index[%lld/%d]{crc:%d rank:%f slevel:%d category:%lld}", IBLL(docheader->globalid), docid, docheader->crc, docheader->rank, docheader->slevel, LLI(docheader->category));
                            ret = ibase_update_index(ibase, docid, block);
                            iheader->status = 0;
                            DEBUG_LOGGER(ibase->logger, "over_update_index[%lld/%d]{crc:%d rank:%f slevel:%d category:%lld}", IBLL(docheader->globalid), docid, docheader->crc, docheader->rank, docheader->slevel, LLI(docheader->category));
                        }
                    }
                }
                else
                {
                    ret = ibase_update_index(ibase, docid, block);
                    DEBUG_LOGGER(ibase->logger, "over_update_index[%lld/%d]{crc:%d rank:%f slevel:%d category:%lld}", IBLL(docheader->globalid), docid, docheader->crc, docheader->rank, docheader->slevel, LLI(docheader->category));
                }
            }
            else
            {
                ibase->state->docid = docid;
                DEBUG_LOGGER(ibase->logger, "new_index[%lld/%d]{crc:%d rank:%f slevel:%d category:%lld}", IBLL(docheader->globalid), docid, docheader->crc, docheader->rank, docheader->slevel, LLI(docheader->category));
                ret = ibase_index(ibase, docid, block);
            }
        }
        else
        {
            FATAL_LOGGER(ibase->logger, "new_index[%lld]{crc:%d rank:%f slevel:%d category:%lld} failed, %s", IBLL(docheader->globalid), docheader->crc, docheader->rank, docheader->slevel, LLI(docheader->category), strerror(errno));

        }
        MUTEX_UNLOCK(ibase->mutex);
    }
    return ret;
}
/* delete document */
int ibase_del_document(IBASE *ibase, int docid)
{    
    int ret = -1;

    if(ibase)
    {

    }
    return ret;
}
