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
#include "mmtree.h"
#include "logger.h"
//#include "kvmap.h"
//#include "qsmap.h"
#include "db.h"
#define PIHEADER(ibase, docid) &(((IHEADER *)(ibase->headersio.map))[docid])
#ifndef LLI
#define LLI(x) ((long long int) x)
#endif
#define ZMERGE(ibase, is_query_phrase, tab, wmap, x, n, np, dp, olddp)                          \
do                                                                                              \
{                                                                                               \
    if(tab[x].p < tab[x].end)                                                                   \
    {                                                                                           \
        tab[x].ndocid = 0;                                                                      \
        tab[x].term_count = 0;                                                                  \
        tab[x].no = 0;                                                                          \
        tab[x].fields = 0;                                                                      \
        tab[x].sprevnext = NULL;                                                                \
        tab[x].eprevnext = NULL;                                                                \
        tab[x].prevnext_size = 0;                                                               \
        np = &(tab[x].ndocid);                                                                  \
        UZVBCODE(tab[x].p, n, np);                                                              \
        tab[x].docid +=  tab[x].ndocid;                                                         \
        np = &(tab[x].term_count);                                                              \
        UZVBCODE(tab[x].p, n, np);                                                              \
        np = &(tab[x].no);                                                                      \
        UZVBCODE(tab[x].p, n, np);                                                              \
        np = &(tab[x].fields);                                                                  \
        UZVBCODE(tab[x].p, n, np);                                                              \
        np = &(tab[x].prevnext_size);                                                           \
        UZVBCODE(tab[x].p, n, np);                                                              \
        tab[x].who.fields = tab[x].fields;                                                      \
        tab[x].who.count = 1;                                                                   \
        tab[x].who.whoes[0]  = x;                                                               \
        tab[x].who.hits[0]   = tab[x].no;                                                       \
        tab[x].who.bithits   = 1 << x;                                                          \
        if(ibase->state->phrase_status != IB_PHRASE_DISABLED && is_query_phrase)                \
        {                                                                                       \
            memset(tab[x].who.bitphrase, 0, sizeof(int) * IB_BITSPHRASE_MAX);                   \
            n = (tab[x].no/32);tab[x].who.bitphrase[n] |= (1 << (tab[x].no % 32));              \
        }                                                                                       \
        if(tab[x].prevnext_size > 0)                                                            \
        {                                                                                       \
            tab[x].sprevnext = tab[x].p;                                                        \
            tab[x].p += tab[x].prevnext_size;                                                   \
            tab[x].eprevnext = tab[x].p;                                                        \
        }                                                                                       \
        dp = (void *)&(tab[x].who);                                                             \
        if(tab[x].docid > ibase->state->docid)                                                  \
        {                                                                                       \
            FATAL_LOGGER(ibase->logger, "invalid record[term:%d doc:%d ndocid:%d "              \
                    "docmax:%d prevnext_size:%d]", tab[x].termid, tab[x].docid,                 \
                    tab[x].ndocid, ibase->state->docid, tab[x].prevnext_size);                  \
        }                                                                                       \
        else                                                                                    \
        {                                                                                       \
            olddp = NULL;                                                                       \
            KVMAP_ADD(wmap, tab[x].docid, dp, olddp);                                           \
            if(olddp)                                                                           \
            {                                                                                   \
                tab[x].who.fields |= tab[x].fields;                                             \
                n = ((IWHO *)olddp)->count++;                                                   \
                ((IWHO *)olddp)->whoes[n]   = x;                                                \
                ((IWHO *)olddp)->hits[n]    = tab[x].no;                                        \
                if(ibase->state->phrase_status != IB_PHRASE_DISABLED && is_query_phrase)        \
                {                                                                               \
                    n = (tab[x].no/32);                                                         \
                    ((IWHO *)olddp)->bitphrase[n] |= (1 << (tab[x].no % 32));                   \
                }                                                                               \
                ((IWHO *)olddp)->bithits    |= (1 << x);                                        \
            }                                                                                   \
        }                                                                                       \
    }                                                                                           \
}while(0)
#define XMERGE(ibase, is_query_phrase, tab, wmap, x, n, np, dp, olddp)                          \
do                                                                                              \
{                                                                                               \
    if(tab[x].p < tab[x].end)                                                                   \
    {                                                                                           \
        tab[x].docid = *((int*)tab[x].p);tab[x].p += sizeof(int);                               \
        tab[x].term_count = *((int*)tab[x].p);tab[x].p += sizeof(int);                          \
        tab[x].no = *((int*)tab[x].p);tab[x].p += sizeof(int);                                  \
        tab[x].fields = *((int*)tab[x].p);tab[x].p += sizeof(int);                              \
        tab[x].prevnext_size = *((int*)tab[x].p);tab[x].p += sizeof(int);                       \
        tab[x].sprevnext = NULL;                                                                \
        tab[x].eprevnext = NULL;                                                                \
        tab[x].who.fields = tab[x].fields;                                                      \
        tab[x].who.count = 1;                                                                   \
        tab[x].who.whoes[0]  = x;                                                               \
        tab[x].who.hits[0]   = tab[x].no;                                                       \
        if(ibase->state->phrase_status != IB_PHRASE_DISABLED && is_query_phrase)                \
        {                                                                                       \
            memset(tab[x].who.bitphrase, 0, sizeof(int) * IB_BITSPHRASE_MAX);                   \
            n = (tab[x].no/32);tab[x].who.bitphrase[n] |= (1 << (tab[x].no % 32));              \
        }                                                                                       \
        tab[x].who.bithits   = 1 << x;                                                          \
        if(tab[x].prevnext_size > 0)                                                            \
        {                                                                                       \
            tab[x].sprevnext = tab[x].p;                                                        \
            tab[x].p += tab[x].prevnext_size;                                                   \
            tab[x].eprevnext = tab[x].p;                                                        \
        }                                                                                       \
        dp = (void *)&(tab[x].who);                                                             \
        if(tab[x].docid > ibase->state->docid)                                                  \
        {                                                                                       \
            FATAL_LOGGER(ibase->logger, "invalid record[term:%d doc:%d ndocid:%d "              \
                    "docmax:%d prevnext_size:%d]", tab[x].termid, tab[x].docid,                 \
                    tab[x].ndocid, ibase->state->docid, tab[x].prevnext_size);                  \
        }                                                                                       \
        else                                                                                    \
        {                                                                                       \
            olddp = NULL;                                                                       \
            KVMAP_ADD(wmap, tab[x].docid, dp, olddp);                                           \
            if(olddp)                                                                           \
            {                                                                                   \
                tab[x].who.fields |= tab[x].fields;                                             \
                n = ((IWHO *)olddp)->count++;                                                   \
                ((IWHO *)olddp)->whoes[n]   = x;                                                \
                ((IWHO *)olddp)->hits[n]    = tab[x].no;                                        \
                if(ibase->state->phrase_status != IB_PHRASE_DISABLED && is_query_phrase)        \
                {                                                                               \
                    n = (tab[x].no/32);                                                         \
                    ((IWHO *)olddp)->bitphrase[n] |= (1 << (tab[x].no % 32));                   \
                }                                                                               \
                ((IWHO *)olddp)->bithits    |= (1 << x);                                        \
            }                                                                                   \
        }                                                                                       \
    }                                                                                           \
}while(0)
#define MERGE(ibase, is_query_phrase, tab, wmap, x, n, np, dp, olddp)                           \
do                                                                                              \
{                                                                                               \
    if(ibase->state->compression_status != IB_COMPRESSION_DISABLED)                             \
    {                                                                                           \
        ZMERGE(ibase, is_query_phrase, tab, wmap, x, n, np, dp, olddp);                         \
    }                                                                                           \
    else                                                                                        \
    {                                                                                           \
        XMERGE(ibase, is_query_phrase, tab, wmap, x, n, np, dp, olddp);                         \
    }                                                                                           \
}while(0)   

/* sort */
int ibase_sort(IBASE *ibase, IQUERY *query, IRES *res, 
        ITERM *hitstable, void *merge_map, void *topmap)
{
    int i = 0, x = 0, n = 0, z = 0, nhits = 0, last = 0, is_sort_reverse = 0, is_field_sort = 0, 
        k = 0, docid = 0, total = 0, *np = NULL, *intidx = NULL, ignore_score = 0, fid = -1, 
        is_query_phrase = 0, int_index_from = 0, int_index_to = 0, ignore_rank = 0, ifrom = 0, 
        double_index_from = 0, double_index_to = 0, bits = 0, ito = 0, nxrecords = 0;
    double *doubleidx = NULL, score = 0.0f, p1 = 0.0, p2 = 0.0, tf = 1.0, 
           Py = 0.0, Px = 0.0, ffrom = 0.0, fto = 0.0;
    IRECORD *record = NULL, xrecords[IB_NTOP_MAX];
    void *dp = NULL, *olddp = NULL, *timer = NULL;
    unsigned int doc_score = 0, old_score = 0;
    IHEADER *headers = NULL;
    IWHO *pwho = NULL;

    if(ibase && merge_map && topmap && hitstable)
    {
        //TIMER_INIT(timer);
        /* merge hits list and sort */
        headers = (IHEADER *)(ibase->headersio.map);
        intidx = (int *)(ibase->intidxio.map);
        doubleidx = (double *)(ibase->doubleidxio.map);
        if((p1 = query->ravgdl) <= 0.0) p1 = 1.0;
        if((query->flag & IB_QUERY_PHRASE)) is_query_phrase = 1;
        if((query->flag & IB_QUERY_RSORT)) is_sort_reverse = 1;
        else if((query->flag & IB_QUERY_SORT)) is_sort_reverse = 0;
        else is_sort_reverse = 1;
        int_index_from = ibase->state->int_index_from;
        int_index_to = int_index_from + ibase->state->int_index_fields_num;
        double_index_from = ibase->state->double_index_from;
        double_index_to = double_index_from + ibase->state->double_index_fields_num;
        if((fid = query->int_order_field) >= int_index_from && fid < int_index_to)
        {
            is_field_sort = 1;
        }
        else if((fid = query->double_order_field) >= double_index_from && fid < double_index_to)
        {
            is_field_sort = 2;
        }
        do
        {
            dp = NULL;
            KVMAP_POP_RMIN(merge_map, docid, dp);
            /* get min docid */
            if((pwho = (IWHO *)dp))
            {
                //fprintf(stdout, "%s::%d docid:%d filter:%d pwho:%p\n", __FILE__, __LINE__, docid, query->fields_filter, pwho);
                if(headers[docid].status < 0 || headers[docid].globalid < 0) goto next;
                /* check fobidden terms in query string */
                if((query->flag & IB_QUERY_FORBIDDEN) && headers[docid].status<IB_SECURITY_OK)goto next;
                /* slevel filter */
                if((k = headers[docid].slevel) < 0 || headers[docid].slevel > IB_SLEVEL_MAX || query->slevel_filter[k]  == 1) goto next;
                /* catetory block filter */
                if(query->catblock_filter != 0 && (query->catblock_filter & headers[docid].category)) goto next;
                /* fields filter */
                if(query->fields_filter != 0 && query->fields_filter != -1 
                        && !(query->fields_filter & pwho->fields))
                    goto next;
                /* int range  filter */
                if(query->int_range_count > 0 && intidx)
                {
                    for(i = 0; i < query->int_range_count; i++)
                    {
                        if((k = query->int_range_list[i].field_id) >= int_index_from 
                                && k < int_index_to)
                        {
                            ifrom = query->int_range_list[i].from;
                            ito = query->int_range_list[i].to;
                            k -= int_index_from;
                            k += ibase->state->int_index_fields_num * docid;
                            if(intidx[k] < ifrom || (ito >= ifrom && intidx[k] > ito)) goto next;
                        }
                    }
                }
                /* double range filter */
                if(query->double_range_count > 0 && doubleidx)
                {
                    for(i = 0; i < query->double_range_count; i++)
                    {
                        if((k = query->double_range_list[i].field_id) >= double_index_from 
                                && k < double_index_to)
                        {
                            ffrom = query->double_range_list[i].from;
                            fto = query->double_range_list[i].to;
                            k -= double_index_from;
                            k += ibase->state->double_index_fields_num * docid;
                            if(doubleidx[k] < ffrom || (fto >= ffrom && doubleidx[k] > fto)) goto next;
                        }
                    }
                }
                /* boolen check */
                if(query->operators.bitsnot  && (query->operators.bitsnot & pwho->bithits)) 
                    goto next;
                if((query->flag & IB_QUERY_BOOLAND) && query->operators.bitsand 
                && (query->operators.bitsand & pwho->bithits) != query->operators.bitsand) 
                    goto next;
                ignore_score = 0;
                ignore_rank = 0;
                doc_score = 0;
                score = 0;
                nhits = pwho->count;
                /* group */
                if((bits = (query->catgroup_filter & headers[docid].category)))
                {
                    x = 0;
                    do
                    {
                        if((bits & 0x01))
                        {
                            if(res->catgroups[x] == 0) res->ncatgroups++;
                            res->catgroups[x]++;
                        }
                        bits >>= 1;
                        ++x;
                    }while(bits);
                }
                /* category filter */
                if(query->category_filter != 0 && query->category_filter != -1 
                        && (query->category_filter & headers[docid].category) 
                        != query->category_filter)
                    goto next;
                /* caculate score */
                //TIMER_SAMPLE(timer);
                i = pwho->count - 1;
                do
                {
                    x = pwho->whoes[i];
                    /* score = hitstable[x].idf * tf; */
                    if(ignore_score == 0)
                    {
                        tf = (double)(hitstable[x].term_count)/(double)(headers[docid].terms_total);
                        /* caculate phrase */
                        if(ibase->state->phrase_status != IB_PHRASE_DISABLED
                                && pwho->count > 1 && is_query_phrase 
                                && hitstable[x].prevnext_size > 0)
                        {
                            DEBUG_LOGGER(ibase->logger, "phrase is_query_phrase:%d prevnext_size:%d docid:%d count:%d", is_query_phrase, hitstable[x].prevnext_size, docid, pwho->count);
                            last = 0;
                            do
                            {
                                z = 0;
                                np = &z;
                                UZVBCODE(hitstable[x].sprevnext, n, np);
                                last += z;
                                n = (1 << (last%32));
                                z = (last/32);
                                if(z >= IB_BITSPHRASE_MAX)
                                {
                                    FATAL_LOGGER(ibase->logger, "z:%d n:%d last:%d\n", z, n, last);
                                    break;
                                }
                                if(z < IB_BITSPHRASE_MAX && (pwho->bitphrase[z] & n)) 
                                    doc_score += IB_INT_SCORE(query->base_phrase);
                            }while(hitstable[x].sprevnext < hitstable[x].eprevnext); 
                        }
                        p2 = (double)(headers[docid].terms_total) * p1;
                        Py = hitstable[x].idf * tf * IB_BM25_P1;
                        Px = tf+IB_BM25_K1-IB_BM25_P2+IB_BM25_P2*p2;
                        score = Py/Px;
                        doc_score += IB_INT_SCORE(score);
                    }
                    //record->weights[i].no = pwho->hits[i];
                    //record->weights[i].score = score;
                    MERGE(ibase, is_query_phrase, hitstable, merge_map, x, n, np, dp, olddp);
                }while(--i >= 0);
                if(ignore_score == 0) 
                {
                    doc_score += IB_INT_SCORE(nhits * query->base_hits);
                    /* bitxcat */
                    if(headers[docid].category != 0)
                    {
                        if(query->bitxcat_up != 0 && (headers[docid].category & query->bitxcat_up))
                            doc_score += IB_INT_SCORE(query->base_xcatup);
                        else if(query->bitxcat_down != 0
                                && (headers[docid].category & query->bitxcat_down))
                        {
                            if(doc_score > IB_INT_SCORE(query->base_xcatdown))
                                doc_score -= IB_INT_SCORE(query->base_xcatdown);
                            else
                                doc_score = 0;
                        }
                    }
                    if(ignore_rank == 0 && (query->flag & IB_QUERY_RANK)) 
                        doc_score += (int)(headers[docid].rank * query->base_rank);
                }
                //TIMER_SAMPLE(timer);
                //DEBUG_LOGGER(ibase->logger, "caculate docid:%ld score:%f time used:%lld",docid, doc_score, PT_LU_USEC(timer));
                //fprintf(stdout, "%s::%d docid:%d score:%f\r\n", __FILE__, __LINE__, docid, doc_score);
                /* sort */
                total++;
                /* topmap is over flow */
                if(PQS(topmap)->count >= query->ntop)
                {
                    dp = NULL;
                    if(is_sort_reverse || is_field_sort)
                    {
                        if(doc_score > PQS_MINK(topmap))
                        {
                            QSMAP_POP_RMIN(topmap, old_score, dp);
                            if((record = (IRECORD *)dp))
                            {
                                record->globalid    = docid;
                                QSMAP_ADD(topmap, doc_score, dp, olddp);
                            }
                        }
                    }
                    else
                    {
                        if(doc_score < PQS_MAXK(topmap))
                        {
                            QSMAP_POP_RMAX(topmap, old_score, dp);
                            if((record = (IRECORD *)dp))
                            {
                                record->globalid    = docid;
                                QSMAP_ADD(topmap, doc_score, dp, olddp);
                            }
                        }
                    }
                }
                else
                {
                    if(nxrecords < IB_NTOP_MAX && (record = &(xrecords[nxrecords++])))
                    {
                        record->globalid    = docid;
                        dp = (void *)record;
                        QSMAP_ADD(topmap, doc_score, dp, olddp);
                    }
                }
                //fprintf(stdout, "%s::%d docid:%d score:%f\r\n", __FILE__, __LINE__, docid, doc_score);
                continue;
next:
                i = pwho->count - 1;
                do
                {
                    x = pwho->whoes[i];
                    MERGE(ibase, is_query_phrase, hitstable, merge_map, x, n, np, dp, olddp);
                }while(--i >= 0);
            }else break;
        }while(KV_ROOT(merge_map));
        TIMER_CLEAN(timer);
    }
    return total;
}

/* query */
ICHUNK *ibase_query(IBASE *ibase, IQUERY *query)
{
    void *merge_map = NULL, *topmap = NULL, *fmap = NULL, *dp = NULL, *olddp = NULL;
    int i = 0, n = 0, nbytes = 0, total = 0,  *np = NULL, nres = 0, io_time = 0, 
        doc_score = 0,sort_time = 0, is_sort_reverse = 0, is_query_phrase = 0, 
        nqterms = 0, *intidx = NULL, int_index_from = 0, int_index_to = 0, docid = 0,
        double_index_from = 0, double_index_to = 0, fid = -1, is_field_sort = 0;
    IRECORD *records = NULL, *record = NULL;
    double *doubleidx = NULL;
    ITERM *hitstable = NULL;
    IHEADER *headers = NULL;
    ICHUNK *chunk = NULL;
    void *timer = NULL;
    IRES *res = NULL;


    if(ibase && query && query->nqterms > 0)
    {
        /* chunks */
        if((chunk = ibase_pop_chunk(ibase)))
        {
            res = &(chunk->res);
            memset(res, 0, sizeof(IRES));
            records = chunk->records;
        }
        else 
        {
            FATAL_LOGGER(ibase->logger, "pop chunk failed, %s", strerror(errno));
            goto err;
        }
        TIMER_INIT(timer);
        /* initialize hits table */
        hitstable = ibase_pop_itermlist(ibase);
        /* merge map */
        merge_map = ibase_pop_kmap(ibase);
        /* sort map */
        topmap = ibase_pop_smap(ibase);
        /* fmap */
        fmap = ibase_pop_smap(ibase);
        if(merge_map == NULL || fmap == NULL || topmap == NULL || hitstable == NULL) 
        {
            FATAL_LOGGER(ibase->logger, "merge_map:%p topmap:%p histable:%p, %s", merge_map, topmap, hitstable, strerror(errno));
            goto err;
        }
        headers = (IHEADER *)(ibase->headersio.map);
        intidx = (int *)(ibase->intidxio.map);
        doubleidx = (double *)(ibase->doubleidxio.map);
        if((query->flag & IB_QUERY_PHRASE)) is_query_phrase = 1;
        if((query->flag & IB_QUERY_RSORT)) is_sort_reverse = 1;
        else if((query->flag & IB_QUERY_SORT)) is_sort_reverse = 0;
        else is_sort_reverse = 1;
        int_index_from = ibase->state->int_index_from;
        int_index_to = int_index_from + ibase->state->int_index_fields_num;
        double_index_from = ibase->state->double_index_from;
        double_index_to = double_index_from + ibase->state->double_index_fields_num;
        if((fid = query->int_order_field) >= int_index_from && fid < int_index_to)
        {
            is_field_sort = 1;
            fid -= int_index_from;
        }
        else if((fid = query->double_order_field) >= double_index_from && fid < double_index_to)
        {
            is_field_sort = 2;
            fid -= double_index_from;
        }
        nqterms = query->nqterms;
        if(query->nqterms > IB_QUERY_MAX) nqterms = IB_QUERY_MAX;
        //fprintf(stdout, "%s::%d nqterms:%d chunk:%p\n", __FILE__, __LINE__, query->nqterms, chunk);
        /* ready for merging hits list */
        for(i = 0; i < nqterms; i++)
        {
            hitstable[i].who.total = nqterms;
            hitstable[i].termid = query->qterms[i].id;
            hitstable[i].idf = query->qterms[i].idf;
            if((n = hitstable[i].mm.ndata = db_get_data(PDB(ibase->index), hitstable[i].termid,
                            &(hitstable[i].mm.data))) > 0)
            {
                nbytes += n;
                hitstable[i].mm.ndata = n;
                hitstable[i].p = hitstable[i].mm.data + hitstable[i].mm.ndata;
                hitstable[i].end = hitstable[i].mm.data + n;
                MERGE(ibase, is_query_phrase, hitstable, merge_map, i, n, np, dp, olddp);
            }
        }
        DEBUG_LOGGER(ibase->logger, "sort flag:%d total:%d time used :%lld", query->flag, total, PT_LU_USEC(timer));
        //fprintf(stdout, "%s::%d nqterms:%d chunk:%p\n", __FILE__, __LINE__, query->nqterms, chunk);
        TIMER_SAMPLE(timer);
        DEBUG_LOGGER(ibase->logger, "reading index data  %d nytes time used :%lld", nbytes, PT_LU_USEC(timer));
        io_time = (int)(PT_LU_USEC(timer));
        //sort
        total = ibase_sort(ibase, query, res, hitstable, merge_map, topmap);
        TIMER_SAMPLE(timer);
        sort_time = (int)PT_LU_USEC(timer);
        DEBUG_LOGGER(ibase->logger, "sort flag:%d total:%d time used :%lld", query->flag, total, PT_LU_USEC(timer));
        //fprintf(stdout, "%s::%d io_time:%d sort_time:%d\n", __FILE__, __LINE__, io_time, sort_time);
        //fprintf(stdout, "%s::%d nqterms:%d chunk:%p\n", __FILE__, __LINE__, query->nqterms, chunk);
        if(total > 0 && (nres = PQS(topmap)->count) > 0)
        {
            res->io_time = io_time;
            res->sort_time = sort_time;
            res->total = total;
            res->count = PQS(topmap)->count;
            if(is_field_sort)
            {
                do
                {
                    dp = NULL;
                    QSMAP_POP_RMAX(topmap, doc_score, dp);
                    if((record = (IRECORD *)dp))
                    {
                        docid = record->globalid;
                        if(is_field_sort == 1)
                        {
                            i = fid + ibase->state->int_index_fields_num * docid;
                            doc_score = intidx[i];
                        }
                        else
                        {
                            i = fid + ibase->state->double_index_fields_num * docid;
                            doc_score = IB_INT_SCORE(doubleidx[i]);
                        }
                        QSMAP_ADD(fmap, doc_score, dp, olddp);
                    }
                }while(QS_ROOT(topmap));
                i = 0;
                do
                {
                    dp = NULL;
                    if(is_sort_reverse){QSMAP_POP_RMAX(fmap, doc_score, dp);}
                    else{QSMAP_POP_RMIN(fmap, doc_score, dp);}
                    if((record = (IRECORD *)dp))
                    {
                        docid = record->globalid;
                        records[i].score = doc_score; 
                        records[i].globalid = headers[docid].globalid;
                    }
                    i++;
                }while(QS_ROOT(fmap));
            }
            else
            {
                i = 0;
                do
                {
                    dp = NULL;
                    if(is_sort_reverse){QSMAP_POP_RMAX(topmap, doc_score, dp);}
                    else{QSMAP_POP_RMIN(topmap, doc_score, dp);}
                    if((record = (IRECORD *)dp))
                    {
                        docid = record->globalid;
                        records[i].score = record->score; 
                        records[i].globalid = headers[docid].globalid;
                    }
                    i++;
                }while(QS_ROOT(topmap));
            }
        }
        //fprintf(stdout, "%s::%d nqterms:%d chunk:%p\n", __FILE__, __LINE__, query->nqterms, chunk);
        /* clean */
        for(i = 0; i < nqterms; i++)
        {
            db_free_data(PDB(ibase->index), hitstable[i].mm.data, hitstable[i].mm.ndata);
        }
err:
        if(res) res->doctotal = ibase->state->dtotal;
        if(merge_map){ibase_push_kmap(ibase, merge_map);}
        if(topmap){ibase_push_smap(ibase, topmap);}
        if(fmap){ibase_push_smap(ibase, fmap);}
        if(hitstable){ibase_push_itermlist(ibase, hitstable);}
        if(nres == 0){ibase_push_chunk(ibase, chunk); chunk = NULL;}
        //if(hitstable)free(hitstable);
        TIMER_CLEAN(timer);
    }
    return chunk;
}
