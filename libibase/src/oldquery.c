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
#include "kvmap.h"
#include "qsmap.h"
#include "idb.h"
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

/* push iterm */
void ibase_push_itermlist(IBASE *ibase, ITERM *iterm)
{
    int x = 0;

    if(ibase && iterm)
    {
        MUTEX_LOCK(ibase->mutex);
        if(ibase->nqiterms < IB_QITERMS_MAX)
        {
            x = ibase->nqiterms++; 
            ibase->qiterms[x] = iterm;
        }
        else
        {
            free(iterm);
        }
        MUTEX_UNLOCK(ibase->mutex);
    }
    return ;
}

/* ibase pop iterm */
ITERM *ibase_pop_itermlist(IBASE *ibase)
{
    ITERM *iterm = NULL;
    int x = 0;

    if(ibase)
    {
        MUTEX_LOCK(ibase->mutex);
        if(ibase->nqiterms > 0)
        {
            x = --(ibase->nqiterms);
            iterm = ibase->qiterms[x];
            memset(iterm, 0, sizeof(ITERM) * IB_QUERY_MAX);
            ibase->qiterms[x] = NULL;
        }
        else 
        {
            iterm = (ITERM *)calloc(IB_QUERY_MAX, sizeof(ITERM));
        }
        MUTEX_UNLOCK(ibase->mutex);
    }
    return iterm;
}

/* push chunk */
void ibase_push_chunk(IBASE *ibase, ICHUNK *chunk)
{
    int x = 0;

    if(ibase && chunk)
    {
        MUTEX_LOCK(ibase->mutex);
        if(ibase->nqchunks < IB_MAPS_MAX)
        {
            x = ibase->nqchunks++; 
            ibase->qchunks[x] = chunk;
        }
        else
        {
            free(chunk);
        }
        MUTEX_UNLOCK(ibase->mutex);
    }
    return ;
}

/* ibase pop chunk */
ICHUNK *ibase_pop_chunk(IBASE *ibase)
{
    ICHUNK *chunk = NULL;
    int x = 0;

    if(ibase)
    {
        MUTEX_LOCK(ibase->mutex);
        if(ibase->nqchunks > 0)
        {
            x = --(ibase->nqchunks);
            chunk = ibase->qchunks[x];
            ibase->qchunks[x] = NULL;
        }
        else 
        {
            chunk = (ICHUNK *)calloc(1, sizeof(ICHUNK));
        }
        MUTEX_UNLOCK(ibase->mutex);
    }
    return chunk;
}

/* push record */
void ibase_push_record(IBASE *ibase, IRECORD *record)
{
    int x = 0;

    if(ibase && record)
    {
        MUTEX_LOCK(ibase->mutex);
        if(ibase->nqrecords < IB_RECS_MAX)
        {
            x = ibase->nqrecords++; 
            ibase->qrecords[x] = record;
        }
        else
        {
            free(record);
        }
        MUTEX_UNLOCK(ibase->mutex);
    }
    return ;
}

/* ibase pop record */
IRECORD *ibase_pop_record(IBASE *ibase)
{
    IRECORD *record = NULL;
    int x = 0;

    if(ibase)
    {
        MUTEX_LOCK(ibase->mutex);
        if(ibase->nqrecords > 0)
        {
            x = --(ibase->nqrecords);
            record = ibase->qrecords[x];
            ibase->qrecords[x] = NULL;
        }
        else 
        {
            record = (IRECORD *)calloc(1, sizeof(IRECORD));
        }
        MUTEX_UNLOCK(ibase->mutex);
    }
    return record;
}



/* sort */
int ibase_sort(IBASE *ibase, IQUERY *query, ICATGROUP *catgroup, 
        ITERM *hitstable, void *merge_map, void *topmap)
{
    int i = 0, x = 0, n = 0, z = 0, nhits = 0, last = 0, is_sort_reverse = 0, 
        docid = 0, total = 0, *np = NULL, *intidx = NULL, ignore_score = 0, 
        is_query_phrase = 0, int_index_from = 0, int_index_to = 0, 
        double_index_from = 0, double_index_to = 0, bits = 0;
    unsigned int doc_score = 0, old_score = 0;
    double *doubleidx = NULL, score = 0.0f, p1 = 0.0, p2 = 0.0, tf = 1.0, Py = 0.0, Px = 0.0;
    void *dp = NULL, *olddp = NULL, *timer = NULL;
    IRECORD *record = NULL, *oldrecord = NULL;
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
        do
        {
            dp = NULL;
            KVMAP_POP_RMIN(merge_map, docid, dp);
            /* get min docid */
            if((pwho = (IWHO *)dp))
            {
                //fprintf(stdout, "%s::%d docid:%d filter:%d pwho:%p\n", __FILE__, __LINE__, docid, query->fields_filter, pwho);
                if(headers[docid].status < 0 || headers[docid].globalid < 0) goto next;
                /* slevel filter */
                if(query->slevel_filter != 0 && headers[docid].slevel != -0 
                    && (query->slevel_filter & headers[docid].slevel) == 0)
                    goto next;
                /* fields filter */
                if(query->fields_filter != 0 && query->fields_filter != -1 
                        && !(query->fields_filter & pwho->fields))
                    goto next;
                /* category filter */
                if(query->category_filter != 0 && query->category_filter != -1 
                        && (query->category_filter & headers[docid].category) 
                        != query->category_filter)
                    goto next;
                /* int/double range  filter */
                if(intidx && (i = query->int_range_field) >= int_index_from && i < int_index_to)
                {
                    i -= int_index_from;
                    i += ibase->state->int_index_fields_num * docid;
                    if(intidx[i] < query->int_range_from || intidx[i] > query->int_range_to) 
                        goto next;
                }
                if(doubleidx && (i = query->double_range_field) >= double_index_from 
                        && i < double_index_to)
                {
                    i -= double_index_from;
                    i += ibase->state->double_index_fields_num * docid;
                    if(doubleidx[i] < query->double_range_from 
                            || doubleidx[i] > query->double_range_to) 
                        goto next;
                }
                /* boolen check */
                if(query->operators.bitsnot  && (query->operators.bitsnot & pwho->bithits)) 
                    goto next;
                //if((query->operators.bitsand & pwho->bithits) != query->operators.bitsand) 
                //    goto next;
                ignore_score = 0;
                doc_score = 0;
                score = 0;
                nhits = pwho->count;
                //if(nhits == query->nqterms) doc_score += IB_INT_SCORE(IB_BOOL_SCORE);
                //logic AND
                /*
                if(PQS(topmap)->count >= query->ntop && query->operators.bitsand)
                {
                    if((query->operators.bitsand & pwho->bithits) != query->operators.bitsand) 
                        goto next;
                    //fprintf(stdout, "bitsand:%d bithits:%d\n", query->operators.bitsand, pwho->bithits);
                }
                */
                /* field sort */
                if((i = query->int_order_field) >= int_index_from && i < int_index_to)
                {
                    i -= int_index_from;
                    i += ibase->state->int_index_fields_num * docid;
                    doc_score = intidx[i];
                    ignore_score = 1;
                }
                else if((i = query->double_order_field) >= double_index_from && i < double_index_to)
                {
                    i -= double_index_from;
                    i += ibase->state->double_index_fields_num * docid;
                    doc_score = IB_INT_SCORE(doubleidx[i]);
                    ignore_score = 1;
                }
                /* group */
                if((query->flag & IB_QUERY_CATGROUP) && (bits = headers[docid].category))
                {
                    x = 0;
                    do
                    {
                        if((bits & 0x01))
                        {
                            if(catgroup->catgroups[x] == 0)
                                catgroup->ncatgroups++;
                            catgroup->catgroups[x]++;
                        }
                        bits >>= 1;
                        ++x;
                    }while(bits);
                    DEBUG_LOGGER(ibase->logger, "catgroup docid:%d category:%lld", docid, IBLL(headers[docid].category));
                }
                /* caculate score */
                //TIMER_SAMPLE(timer);
                if(ignore_score == 0) 
                {
                    doc_score += IB_INT_SCORE(nhits * IB_HITS_SCORE);
                    if(query->flag & IB_QUERY_RANK) 
                        doc_score += IB_INT_SCORE(headers[docid].rank);
                }
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
                                    doc_score += IB_INT_SCORE(IB_PHRASE_SCORE);
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
                //TIMER_SAMPLE(timer);
                //DEBUG_LOGGER(ibase->logger, "caculate docid:%ld score:%f time used:%lld",docid, doc_score, PT_LU_USEC(timer));
                //fprintf(stdout, "%s::%d docid:%d score:%f\r\n", __FILE__, __LINE__, docid, doc_score);
                /* sort */
                total++;
                /* topmap is over flow */
                if(PQS(topmap)->count >= query->ntop)
                {
                    dp = NULL;
                    if(is_sort_reverse)
                    {
                        if(doc_score > PQS_MINK(topmap))
                        {
                            QSMAP_POP_RMIN(topmap, old_score, dp);
                            if((record = (IRECORD *)dp))
                            {
                                record->globalid    = headers[docid].globalid;
                                record->score = doc_score;
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
                                record->globalid    = headers[docid].globalid;
                                record->score = doc_score;
                                QSMAP_ADD(topmap, doc_score, dp, olddp);
                            }
                        }
                    }
                }
                else
                {
                    if((record = ibase_pop_record(ibase)))
                    {
                        record->globalid    = headers[docid].globalid;
                        record->score = doc_score;
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
        if(oldrecord) ibase_push_record(ibase, oldrecord);
        TIMER_CLEAN(timer);
    }
    return total;
}

/* query */
ICHUNK *ibase_query(IBASE *ibase, IQUERY *query)
{
    int i = 0, n = 0, nbytes = 0, total = 0,  *np = NULL, nres = 0, io_time = 0, doc_score = 0,
        sort_time = 0, is_sort_reverse = 0, is_query_phrase = 0, nqterms = 0;
    void *merge_map = NULL, *topmap = NULL, *dp = NULL, *olddp = NULL;
    ICATGROUP *catgroup = NULL;
    ITERM *hitstable = NULL;
    IRECORD *records = NULL;
    ICHUNK *chunk = NULL;
    void *timer = NULL;
    IRES *res = NULL;


    if(ibase && query && query->nqterms > 0)
    {
        TIMER_INIT(timer);
        /* initialize hits table */
        //memset(hitstable, 0, sizeof(ITERM) * IB_QUERY_MAX);
        //hitstable = (ITERM *)calloc(1, sizeof(ITERM) * IB_QUERY_MAX);
        hitstable = ibase_pop_itermlist(ibase);
        /* merge map */
        merge_map = ibase_pop_kmap(ibase);
        /* chunks */
        if((chunk = ibase_pop_chunk(ibase)))
        {
            res = &(chunk->res);
            memset(res, 0, sizeof(IRES));
            records = chunk->records;
            catgroup = &(res->catgroup);
            memset(catgroup, 0, sizeof(ICATGROUP));
        }
        else 
        {
            FATAL_LOGGER(ibase->logger, "pop chunk failed, %s", strerror(errno));
            goto err;
        }
        /* sort map */
        topmap = ibase_pop_smap(ibase);
        if(merge_map == NULL || topmap == NULL || hitstable == NULL) 
        {
            FATAL_LOGGER(ibase->logger, "merge_map:%p topmap:%p histable:%p, %s", merge_map, topmap, hitstable, strerror(errno));
            goto err;
        }
        if((query->flag & IB_QUERY_PHRASE)) is_query_phrase = 1;
        nqterms = query->nqterms;
        if(query->nqterms > IB_QUERY_MAX) nqterms = IB_QUERY_MAX;
        //fprintf(stdout, "%s::%d nqterms:%d chunk:%p\n", __FILE__, __LINE__, query->nqterms, chunk);
        /* ready for merging hits list */
        for(i = 0; i < nqterms; i++)
        {
            hitstable[i].who.total = nqterms;
            hitstable[i].termid = query->qterms[i].id;
            hitstable[i].idf = query->qterms[i].idf;
            if((n = idb_get(PIDB(ibase->db), hitstable[i].termid,
                            &(hitstable[i].block))) > 0)
            {
                nbytes += n;
                hitstable[i].nblock = n;
                hitstable[i].p = hitstable[i].block;
                hitstable[i].end = hitstable[i].block + n;
                MERGE(ibase, is_query_phrase, hitstable, merge_map, i, n, np, dp, olddp);
            }
        }
        DEBUG_LOGGER(ibase->logger, "sort flag:%d total:%d time used :%lld", query->flag, total, PT_LU_USEC(timer));
        //fprintf(stdout, "%s::%d nqterms:%d chunk:%p\n", __FILE__, __LINE__, query->nqterms, chunk);
        TIMER_SAMPLE(timer);
        DEBUG_LOGGER(ibase->logger, "reading index data  %d nytes time used :%lld", nbytes, PT_LU_USEC(timer));
        io_time = (int)(PT_LU_USEC(timer));
        //sort
        total = ibase_sort(ibase, query, catgroup, hitstable, merge_map, topmap);
        TIMER_SAMPLE(timer);
        sort_time = (int)PT_LU_USEC(timer);
        DEBUG_LOGGER(ibase->logger, "sort flag:%d total:%d time used :%lld", query->flag, total, PT_LU_USEC(timer));
        fprintf(stdout, "%s::%d io_time:%d sort_time:%d\n", __FILE__, __LINE__, io_time, sort_time);
        //fprintf(stdout, "%s::%d nqterms:%d chunk:%p\n", __FILE__, __LINE__, query->nqterms, chunk);
        if(total > 0 && (nres = PQS(topmap)->count) > 0)
        {
            res->io_time = io_time;
            res->sort_time = sort_time;
            res->total = total;
            res->count = PQS(topmap)->count;
            if((query->flag & IB_QUERY_RSORT)) is_sort_reverse = 1;
            else if((query->flag & IB_QUERY_SORT)) is_sort_reverse = 0;
            else is_sort_reverse = 1;
            i = 0;
            do
            {
                dp = NULL;
                if(is_sort_reverse){QSMAP_POP_RMAX(topmap, doc_score, dp);}
                else{QSMAP_POP_RMIN(topmap, doc_score, dp);}
                if(dp)
                {
                    memcpy(&(records[i]), dp, sizeof(IRECORD));
                    ibase_push_record(ibase, (IRECORD *)dp);
                }
                i++;
            }while(QS_ROOT(topmap));
        }
        //fprintf(stdout, "%s::%d nqterms:%d chunk:%p\n", __FILE__, __LINE__, query->nqterms, chunk);
        /* clean */
        for(i = 0; i < nqterms; i++)
        {
            idb_free(PIDB(ibase->db), hitstable[i].block);
        }
err:
        if(merge_map){ibase_push_kmap(ibase, merge_map);}
        if(topmap){ibase_push_smap(ibase, topmap);}
        if(hitstable){ibase_push_itermlist(ibase, hitstable);}
        if(nres == 0){ibase_push_chunk(ibase, chunk); chunk = NULL;}
        //if(hitstable)free(hitstable);
        TIMER_CLEAN(timer);
    }
    return chunk;
}
