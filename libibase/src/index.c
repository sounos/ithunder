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
#include "mdb.h"
#include "imap.h"
#include "lmap.h"
#include "dmap.h"
#include "bmap.h"
#define PIHEADER(ibase, secid, docid) &(((IHEADER *)(ibase->state->headers[secid].map))[docid])
#define PMHEADER(ibase, docid) &(((MHEADER *)(ibase->mheadersio.map))[docid])
#ifndef LLI
#define LLI(x) ((long long int) x)
#endif
#ifndef SIZET
#define SIZET(x) ((size_t)x)
#endif
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
/* check index state */
int ibase_check_index_state(IBASE *ibase, DOCHEADER *docheader)
{
    int ret = -1, nint = 0, nlong = 0, ndouble = 0, secid = 0;
    if(ibase && docheader)
    {
        nint =  docheader->intblock_size / sizeof(int);
        nlong =  docheader->longblock_size / sizeof(int64_t);
        ndouble = docheader->doubleblock_size / sizeof(double);
        if(nint > 0 && !ibase->state->int_index_fields_num)
        {
            ibase->state->int_index_from = docheader->intindex_from;
            ibase->state->int_index_fields_num = nint;
        }
        if(nlong > 0 && !ibase->state->long_index_fields_num)
        {
            ibase->state->long_index_from = docheader->longindex_from;
            ibase->state->long_index_fields_num = nlong;
        }
        if(ndouble > 0 && !ibase->state->double_index_fields_num)
        {
            ibase->state->double_index_from = docheader->doubleindex_from;
            ibase->state->double_index_fields_num = ndouble;
        }
        if(ibase->state->used_for == IB_USED_FOR_INDEXD)
        {
            if((secid = docheader->secid) >= 0 && secid < IB_SEC_MAX && !ibase->mindex[secid])
            {
                ibase_set_int_index(ibase, docheader->secid, docheader->intindex_from, nint);
                WARN_LOGGER(ibase->logger, "set_int_index(from:%d,num:%d) globalid:%lld secid:%d", docheader->intindex_from, nint, IBLL(docheader->globalid), docheader->secid);
                ibase_set_long_index(ibase, docheader->secid, docheader->longindex_from, nlong);
                WARN_LOGGER(ibase->logger, "set_long_index(from:%d,num:%d) globalid:%lld secid:%d", docheader->longindex_from, nlong, IBLL(docheader->globalid), docheader->secid);
                ibase_set_double_index(ibase, docheader->secid, docheader->doubleindex_from, ndouble);
                WARN_LOGGER(ibase->logger, "set_double_index(from:%d,num:%d) docid:%lld secid:%d", docheader->doubleindex_from, ndouble, IBLL(docheader->globalid), docheader->secid);
                ibase_check_mindex(ibase, docheader->secid);
            }
        }
        ret = 0;
    }
    return ret;
}

/* add index */
int ibase_index(IBASE *ibase, int docid, IBDATA *block)
{
    char *term = NULL, buf[IB_BUF_SIZE], *data = NULL, *p = NULL, 
         *pp = NULL, *prevnext = NULL;
    int i = 0, termid = 0, n = 0, k = 0, ndocid = 0, ret = -1, ndata = 0, secid = 0, 
        *intlist = NULL, *np = NULL, last_docid = 0;
    DOCHEADER *docheader = NULL;
    int64_t *longlist = NULL;
    double *doublelist = NULL;
    IHEADER *iheader = NULL;
    STERM *termlist = NULL;
    MDB *index = NULL, *posting = NULL;
    off_t size = 0;

    if((docheader = (DOCHEADER *)block->data) 
            && (secid = docheader->secid) >= 0 && docheader->secid < IB_SEC_MAX)
    {
        index = (MDB *)(ibase->mindex[docheader->secid]);
        posting = (MDB *)(ibase->mposting[docheader->secid]);
        if(docheader->dbid != -1)
        {
            ibase->state->dtotal++;
            ibase->state->ttotal += (int64_t)docheader->terms_total;
        }
        if(ibase->state->used_for == IB_USED_FOR_INDEXD)
        {
            if(((off_t)docid * (off_t)sizeof(IHEADER)) >= ibase->state->headers[secid].end)
            {
                ibase->state->headers[secid].old = ibase->state->headers[secid].end;
                size = (off_t)((docid / IB_HEADERS_BASE) + 1) 
                    * (off_t)IB_HEADERS_BASE * (off_t)sizeof(IHEADER);
                ret = ftruncate(ibase->state->headers[secid].fd, size);
                ibase->state->headers[secid].end = size;
                memset(ibase->state->headers[secid].map 
                        + ibase->state->headers[secid].old, 0, 
                    ibase->state->headers[secid].end 
                        -  ibase->state->headers[secid].old);
            }
        }
        if(ibase->state->used_for == IB_USED_FOR_INDEXD)
        {
#ifdef IB_USE_BMAP
            /* bmap */ 
            if(docheader->status < 0) bmap_unset(ibase->bmaps[secid], docid);
            else bmap_set(ibase->bmaps[secid], docid);
#endif
            /* index int */
            if((n = ibase->state->int_index_fields_num) > 0 
                    && (intlist = (int *)(block->data + docheader->intblock_off)))
            {
                n += IB_INT_OFF;
                k = 0;
                for(i = IB_INT_OFF; i < n; i++)
                {
                    IMAP_SET(ibase->state->mfields[secid][i], docid, intlist[k]);
                    k++;
                }
            }
            /* index long */
            if((n = ibase->state->long_index_fields_num) > 0 
                    && (longlist = (int64_t *)(block->data + docheader->longblock_off)))
            {
                n += IB_LONG_OFF;
                k = 0;
                for(i = IB_LONG_OFF; i < n; i++)
                {
                    LMAP_SET(ibase->state->mfields[secid][i], docid, longlist[k]);
                    k++;
                }
            }
            /* index double */
            if((n = ibase->state->double_index_fields_num) > 0 
                    && (doublelist = (double *)(block->data + docheader->doubleblock_off)))
            {
                n += IB_DOUBLE_OFF;
                k = 0;
                for(i = IB_DOUBLE_OFF; i < n; i++)
                {
                    DMAP_SET(ibase->state->mfields[secid][i], docid, doublelist[k]);
                    k++;
                }
            }
            if((iheader = PIHEADER(ibase, docheader->secid, docid)))
            {
                iheader->status      = docheader->status;
                iheader->terms_total = docheader->terms_total;
                //iheader->crc         = docheader->crc;
                iheader->category    = docheader->category;
                iheader->slevel      = docheader->slevel;
                iheader->rank        = docheader->rank;
                iheader->globalid    = docheader->globalid;
                //WARN_LOGGER(ibase->logger, "iheader->category:%p docheader->category:%p", (void *)iheader->category, (void *)docheader->category);
            }
        }
        /* index */
        //end = block->data + block->ndata;
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
                if(ibase->state->index_status != IB_INDEX_DISABLED)
                {
                    last_docid = 0;
                    ndocid = -1;
                    if(mdb_get_tag(index, termid, &last_docid) == 0)
                    {
                        ndocid = docid - last_docid;
                        if(ndocid <= 0) goto term_state_update;
                    }
                    else
                    {
                        ndocid = docid;
                    }
                    p = pp = buf;
                    if((ndata = ((sizeof(int) * 5))) > IB_BUF_SIZE)
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
                    last_docid = docid;
                    mdb_get_tag(posting, termid, &last_docid);
                    if(termlist[i].prevnext_size > 0 && last_docid < docid)
                    {
                        if(mdb_add_data(posting, termid, prevnext, termlist[i].prevnext_size) <= 0) 
                        {
                            FATAL_LOGGER(ibase->logger, "index posting term[%d] failed, %s", 
                                    termid, strerror(errno));
                            _exit(-1);
                        }
                        mdb_set_tag(posting, termid, docid);
                        //WARN_LOGGER(ibase->logger, "docid:%lld termid:%d prevnext_size:%d", IBLL(docheader->globalid), termid, termlist[i].prevnext_size);
                    }
                    if(mdb_add_data(index, termid, pp, (p - pp)) <= 0)
                    {
                        FATAL_LOGGER(ibase->logger, "index term[%d] failed, %s", 
                                termid, strerror(errno));
                        if(data){free(data); data = NULL;}
                        _exit(-1);
                    }
                    mdb_set_tag(index, termid, docid);
                    if(data){xmm_free(data, ndata); data = NULL;}
                }
term_state_update:
                MUTEX_LOCK(ibase->mutex_termstate);
                if(termid > ibase->state->termid){ADD_TERMSTATE(ibase, termid);}
                if(ndocid > 0 )
                {
                    ((TERMSTATE *)(ibase->termstateio.map))[termid].len=termlist[i].term_len;
                    ((TERMSTATE *)(ibase->termstateio.map))[termid].total++;
                }
                MUTEX_UNLOCK(ibase->mutex_termstate);
            }
            else 
            {
                FATAL_LOGGER(ibase->logger, "Invalid ret:%d term[%d]{%.*s} termid:%d in doc:%d", ret, i, termlist[i].term_len, term, termid, docid);
                _exit(-1);
            }
            term += termlist[i].term_len;
            prevnext += termlist[i].prevnext_size;
        }
        ret = 0;
    }
    return ret;
}

/* updated index */
int ibase_update_index(IBASE *ibase, int docid, IBDATA *block)
{
    int ret = -1,  i = 0, k = 0, secid = 0, n = 0, *intlist = NULL;
    DOCHEADER *docheader = NULL;
    //TERMSTATE *termstate = NULL;
    double *doublelist = NULL;
    int64_t *longlist = NULL;
    IHEADER *iheader = NULL;
    //STERM *termlist = NULL;
    off_t size = 0;

    if(ibase && block && block->data && block->ndata > 0 
            && (docheader =  (DOCHEADER *)(block->data))
            && (secid = docheader->secid) >= 0 && docheader->secid < IB_SEC_MAX)
    {
        if(ibase->state->used_for == IB_USED_FOR_INDEXD)
        {
            if(((off_t)docid * (off_t)sizeof(IHEADER)) >= ibase->state->headers[secid].end)
            {
                ibase->state->headers[secid].old = ibase->state->headers[secid].end;
                size = (off_t)((docid / IB_HEADERS_BASE) + 1) 
                    * (off_t)IB_HEADERS_BASE * (off_t)sizeof(IHEADER);
                ret = ftruncate(ibase->state->headers[secid].fd, size);
                ibase->state->headers[secid].end = size;
                memset(ibase->state->headers[secid].map + ibase->state->headers[secid].old, 0, 
                        ibase->state->headers[secid].end -  ibase->state->headers[secid].old);
            }
            if((iheader = PIHEADER(ibase, docheader->secid, docid)))
            {
                iheader->status      = docheader->status;
                iheader->terms_total = docheader->terms_total;
                iheader->category    = docheader->category;
                iheader->slevel      = docheader->slevel;
                iheader->rank        = docheader->rank;
                iheader->globalid    = docheader->globalid;
                //iheader->crc         = docheader->crc;
                if(ibase->state->used_for == IB_USED_FOR_INDEXD)
                {
                    secid = docheader->secid;
#ifdef IB_USE_BMAP
                    /* bmap */ 
                    if(iheader->status < 0) bmap_unset(ibase->bmaps[secid], docid);
                    else bmap_set(ibase->bmaps[secid], docid);
#endif
                    /* index int */
                    if((n = ibase->state->int_index_fields_num) > 0 
                            && (intlist = (int *)(block->data + docheader->intblock_off)))
                    {
                        n += IB_INT_OFF;
                        k = 0;
                        for(i = IB_INT_OFF; i < n; i++)
                        {
                            IMAP_SET(ibase->state->mfields[secid][i], docid, intlist[k]);
                            k++;
                        }
                    }
                    /* index long */
                    if((n = ibase->state->long_index_fields_num) > 0 
                            && (longlist = (int64_t *)(block->data + docheader->longblock_off)))
                    {
                        n += IB_LONG_OFF;
                        k = 0;
                        for(i = IB_LONG_OFF; i < n; i++)
                        {
                            LMAP_SET(ibase->state->mfields[secid][i], docid, longlist[k]);
                            k++;
                        }
                    }
                    /* index double */
                    if((n = ibase->state->double_index_fields_num) > 0 
                            && (doublelist = (double *)(block->data + docheader->doubleblock_off)))
                    {
                        n += IB_DOUBLE_OFF;
                        k = 0;
                        for(i = IB_DOUBLE_OFF; i < n; i++)
                        {
                            DMAP_SET(ibase->state->mfields[secid][i], docid, doublelist[k]);
                            k++;
                        }
                    }
                }   
            }
        }
        ret = 0;
    }
    return ret;
}

/* del index */
int ibase_del_index(IBASE *ibase, int secid, int localid)
{
    int ret = -1, i = 0, k = 0, n = 0, docid = 0;
    MHEADER *mheader = NULL;
    IHEADER *iheader = NULL;

    if((mheader = PMHEADER(ibase, localid)) && (docid = mheader->docid) >= 0) 
    {
        ibase->state->dtotal--;
        if(mheader->docid > 0 && (iheader = PIHEADER(ibase, mheader->secid, mheader->docid)))
        {
            ibase->state->ttotal -= iheader->terms_total;
            iheader->status = -1;
            if(ibase->state->used_for == IB_USED_FOR_INDEXD)
            {
#ifdef IB_USE_BMAP
                /* unset bmap */
                bmap_unset(ibase->bmaps[secid], docid);
#endif
                /* del int index */
                if((n = ibase->state->int_index_fields_num) > 0) 
                {
                    n += IB_INT_OFF;
                    k = 0;
                    for(i = IB_INT_OFF; i < n; i++)
                    {
                        IMAP_DEL(ibase->state->mfields[secid][i], docid);
                        k++;
                    }
                }
                /* del long index */
                if((n = ibase->state->long_index_fields_num) > 0) 
                {
                    n += IB_LONG_OFF;
                    k = 0;
                    for(i = IB_LONG_OFF; i < n; i++)
                    {
                        LMAP_DEL(ibase->state->mfields[secid][i], docid);
                        k++;
                    }
                }
                /* del double index */
                if((n = ibase->state->double_index_fields_num) > 0) 
                {
                    n += IB_DOUBLE_OFF;
                    k = 0;
                    for(i = IB_DOUBLE_OFF; i < n; i++)
                    {
                        DMAP_DEL(ibase->state->mfields[secid][i], docid);
                        k++;
                    }
                }
            }
        }
        if(ibase->state->used_for == IB_USED_FOR_INDEXD 
                && ibase->state->mmsource_status != IB_MMSOURCE_NULL)
        {
            /* add to source */
            WARN_LOGGER(ibase->logger, "delete source[%d] %d bytes", localid, db_get_data_len(PDB(ibase->source), localid));
            ret = db_del_data(PDB(ibase->source), localid);
        }
        ret = 0;        
    }
    return ret;
}

/* add document */
int ibase_add_document(IBASE *ibase, IBDATA *block)
{
    int ret = -1, localid = 0, secid = 0, n = 0, newid = 0;
    DOCHEADER *docheader = NULL;
    MHEADER *mheader = NULL;
    IHEADER *iheader = NULL;
    char line[IB_LINE_MAX];
    off_t size = 0;

    if(ibase && block && block->ndata > 0 && (docheader = (DOCHEADER *)(block->data)) 
            && (secid = docheader->secid) >= 0 && secid < IB_SEC_MAX
            && ibase_check_index_state(ibase, docheader) == 0
            && docheader->globalid && (n = sprintf(line, "%lld", IBLL(docheader->globalid))))
    {
        if((localid = mmtrie_xadd(ibase->docmap, line, n)) > 0)
        {
            MUTEX_LOCK(ibase->mutex);
            if(localid <= ibase->state->docid) 
            {
                if(ibase->state->used_for == IB_USED_FOR_INDEXD)
                {
                    if((mheader = PMHEADER(ibase, localid))
                        && (iheader = PIHEADER(ibase, secid, mheader->docid)))
                    {
                        /* disabled */
                        if(docheader->status < 0)
                        {
                            mheader->status = -1;
                            iheader->status = -1;
#ifdef IB_USE_BMAP
                            bmap_unset(ibase->bmaps[secid], mheader->docid); 
#endif
                            DEBUG_LOGGER(ibase->logger, "Update docid:%d globalid:%lld status:%d", localid, IBLL(docheader->globalid), docheader->status);
                            ret = 0;
                        }
                        else if(docheader->crc != mheader->crc) 
                        {
                            ibase_del_index(ibase, secid, localid);
                            newid = ++(ibase->state->ids[secid]);
                            DEBUG_LOGGER(ibase->logger, "ready_reindex[%lld/%d]{crc:%d rank:%f slevel:%d category:%lld}", IBLL(docheader->globalid), localid, docheader->crc, docheader->rank, docheader->slevel, LLI(docheader->category));
                            mheader->docid = newid;
                            mheader->crc = docheader->crc;
                            ret = ibase_index(ibase, newid, block);
                            DEBUG_LOGGER(ibase->logger, "over_reindex[%lld/%d]{crc:%d rank:%f slevel:%d category:%lld}", IBLL(docheader->globalid), localid, docheader->crc, docheader->rank, docheader->slevel, LLI(docheader->category));
                        }
                        else
                        {
                            DEBUG_LOGGER(ibase->logger, "update_index[%lld/%d]{crc:%d rank:%f slevel:%d category:%lld}", IBLL(docheader->globalid), localid, docheader->crc, docheader->rank, docheader->slevel, LLI(docheader->category));
                            ret = ibase_update_index(ibase, mheader->docid, block);
                            DEBUG_LOGGER(ibase->logger, "over_update_index[%lld/%d]{crc:%d rank:%f slevel:%d category:%lld}", IBLL(docheader->globalid), localid, docheader->crc, docheader->rank, docheader->slevel, LLI(docheader->category));
                        }
                    }
                }
                else
                {
                    docheader->dbid = -1;
                    ret = ibase_index(ibase, localid, block);
                }
            }
            else
            {
                if(ibase->state->used_for == IB_USED_FOR_INDEXD)
                {
                    if(((off_t)localid * (off_t)sizeof(MHEADER)) >= ibase->mheadersio.end)
                    {
                        ibase->mheadersio.old = ibase->mheadersio.end;
                        size = (off_t)((localid / IB_HEADERS_BASE) + 1) 
                            * (off_t)IB_HEADERS_BASE * (off_t)sizeof(MHEADER);
                        ret = ftruncate(ibase->mheadersio.fd, size);
                        ibase->mheadersio.end = size;
                        memset(ibase->mheadersio.map + ibase->mheadersio.old, 0, 
                                ibase->mheadersio.end -  ibase->mheadersio.old);
                    }
                    if((mheader = PMHEADER(ibase, localid)))
                    {
                        mheader->status = 0;
                        mheader->docid = ++(ibase->state->ids[secid]);
                        mheader->secid = docheader->secid;
                        mheader->crc = docheader->crc;
                        DEBUG_LOGGER(ibase->logger, "new_index[%lld/%d]{crc:%d rank:%f slevel:%d category:%lld}", IBLL(docheader->globalid), localid, docheader->crc, docheader->rank, docheader->slevel, LLI(docheader->category));
                        ret = ibase_index(ibase, mheader->docid, block);
                    }
                }
                else
                {
                    ret = ibase_index(ibase, localid, block);
                }
                ibase->state->docid = localid;
            }
            MUTEX_UNLOCK(ibase->mutex);
            if(ibase->state->used_for != IB_USED_FOR_QPARSERD 
                    && ibase->state->mmsource_status != IB_MMSOURCE_NULL && localid > 0)
            {
                ACCESS_LOGGER(ibase->logger, "docid:%lld/%d c_size:%d c_zsize:%d size:%d", docheader->globalid, localid, docheader->content_size, docheader->content_zsize, docheader->prevnext_off);
                docheader->size = docheader->prevnext_off;
                ret = db_set_data(PDB(ibase->source), localid, block->data, docheader->size);
            }
            ret = 0;
        }
        else
        {
            FATAL_LOGGER(ibase->logger, "new_index[%lld]{crc:%d rank:%f slevel:%d category:%lld} failed, %s", IBLL(docheader->globalid), docheader->crc, docheader->rank, docheader->slevel, LLI(docheader->category), strerror(errno));

        }
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
