#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "ibase.h"
#include "timer.h"
#include "zvbcode.h"
#include "logger.h"
#include "db.h"
#include "mtree64.h"
#define XBITXHIT(ibase, is_query_phrase, qfhits, xnode, newnode, _n_)                    \
do                                                                                      \
{                                                                                       \
    _n_ = xnode->nhits++;                                                               \
    xnode->hits[_n_] = newnode->which;                                                  \
    xnode->bithits |= newnode->bithits;                                                 \
    xnode->bitfields |= newnode->bitfields;                                             \
    if(pxnode->bitfields & qfhits) xnode->nhitfields++;                                 \
    if(ibase->state->phrase_status != IB_PHRASE_DISABLED && is_query_phrase)            \
    {                                                                                   \
        if(newnode->no < xnode->xmin) xnode->xmin = newnode->no;                        \
        else if(newnode->no > xnode->xmax) xnode->xmax = newnode->no;                   \
        if(xnode->bitphrase[_n_ - 1] <= newnode->no)                                    \
        xnode->bitphrase[_n_] = newnode->no;                                            \
        else                                                                            \
        {                                                                               \
            while(xnode->bitphrase[_n_ - 1] > newnode->no)                              \
            {                                                                           \
                xnode->bitphrase[_n_] = xnode->bitphrase[_n_ - 1];                      \
                --_n_;                                                                  \
            }                                                                           \
            xnode->bitphrase[_n_ - 1] = newnode->no;                                    \
        }                                                                               \
    }                                                                                   \
}while(0)
#define BITXHIT(ibase, is_query_phrase, qfhits, old, pxnode, _m_)               \
do                                                                              \
{                                                                               \
    _m_ = old->nhits++;                                                         \
    old->hits[_m_] = pxnode->which;                                             \
    old->bithits |= pxnode->bithits;                                            \
    old->bitfields |= pxnode->bitfields;                                        \
    if(pxnode->bitfields & qfhits) old->nhitfields++;                           \
    if(ibase->state->phrase_status != IB_PHRASE_DISABLED && is_query_phrase)    \
    {                                                                           \
        if(pxnode->no < old->xmax)                                              \
        {                                                                       \
            while(_m_ > 0 && pxnode->no < old->bitphrase[_m_-1])                \
            {                                                                   \
                old->bitphrase[_m_] = old->bitphrase[_m_-1];                    \
                old->bitquery[_m_] = old->bitquery[_m_-1];                      \
                --_m_;                                                          \
            }                                                                   \
        }                                                                       \
        old->bitphrase[_m_] = pxnode->no;                                       \
        old->bitquery[_m_] = pxnode->which;                                     \
        if(pxnode->no < old->xmin) old->xmin = pxnode->no;                      \
        else if(pxnode->no > old->xmax) old->xmax = pxnode->no;                 \
    }                                                                           \
}while(0)
void ibase_unxindex(IBASE *ibase, ITERM *itermlist, void *_stree_, 
        int is_query_phrase, int qfhits, int _x_)
{
    XNODE *pxnode = NULL, *pold = NULL;
    int _n_ = 0 , *_np_ = NULL;
    int64_t data = 0, *pdata = &data;

    if(itermlist[_x_].p < itermlist[_x_].end)
    {
        if(ibase->state->compression_status != IB_COMPRESSION_DISABLED)
        {
            itermlist[_x_].ndocid = 0;
            itermlist[_x_].term_count = 0;
            itermlist[_x_].no = 0;
            itermlist[_x_].fields = 0;
            itermlist[_x_].sprevnext = NULL;
            itermlist[_x_].eprevnext = NULL;
            itermlist[_x_].prevnext_size = 0;
            _np_ = &(itermlist[_x_].ndocid);
            UZVBCODE(itermlist[_x_].p, _n_, _np_);
            itermlist[_x_].docid +=  itermlist[_x_].ndocid;
            _np_ = &(itermlist[_x_].term_count);
            UZVBCODE(itermlist[_x_].p, _n_, _np_);
            _np_ = &(itermlist[_x_].no);
            UZVBCODE(itermlist[_x_].p, _n_, _np_);
            _np_ = &(itermlist[_x_].fields);
            UZVBCODE(itermlist[_x_].p, _n_, _np_);
            _np_ = &(itermlist[_x_].prevnext_size);
            UZVBCODE(itermlist[_x_].p, _n_, _np_);
        }
        else
        {
            itermlist[_x_].docid = *((int*)itermlist[_x_].p);
            itermlist[_x_].p += sizeof(int);
            itermlist[_x_].term_count = *((int*)itermlist[_x_].p);
            itermlist[_x_].p += sizeof(int);
            itermlist[_x_].no = *((int*)itermlist[_x_].p);
            itermlist[_x_].p += sizeof(int);
            itermlist[_x_].fields = *((int*)itermlist[_x_].p);
            itermlist[_x_].p += sizeof(int);
            itermlist[_x_].prevnext_size = *((int*)itermlist[_x_].p);
            itermlist[_x_].p += sizeof(int);
        }
        if(itermlist[_x_].prevnext_size > 0)
        {
            itermlist[_x_].sprevnext = itermlist[_x_].p;
            itermlist[_x_].eprevnext = itermlist[_x_].p + itermlist[_x_].prevnext_size;
            itermlist[_x_].p += itermlist[_x_].prevnext_size;
        }
        else
        {
            itermlist[_x_].sprevnext = NULL;
            itermlist[_x_].eprevnext = NULL;
        }
        itermlist[_x_].xnode.which = _x_;
        itermlist[_x_].xnode.docid = itermlist[_x_].docid;
        itermlist[_x_].xnode.bithits = 1 << _x_;
        itermlist[_x_].xnode.bitfields = itermlist[_x_].fields;
        if(itermlist[_x_].fields & qfhits)
            itermlist[_x_].xnode.nhitfields = 1;
        else
            itermlist[_x_].xnode.nhitfields = 0;
        itermlist[_x_].xnode.hits[0] = _x_;
        _n_ = itermlist[_x_].no;
        itermlist[_x_].xnode.bitphrase[0] = _n_;
        itermlist[_x_].xnode.bitquery[0] = _x_;
        itermlist[_x_].xnode.nhits = 1;
        itermlist[_x_].xnode.no = _n_;
        itermlist[_x_].xnode.xmin = _n_;
        itermlist[_x_].xnode.xmax = _n_;
        pxnode = &(itermlist[_x_].xnode);
        if(MTREE64_ADD(_stree_, itermlist[_x_].docid, pxnode, pdata) == 1)
        {
            pold = (XNODE *)((long)data);
            BITXHIT(ibase, is_query_phrase, qfhits, pold, pxnode, _n_);
        }
    }
    return ;
}
 
ICHUNK *ibase_xquery(IBASE *ibase, IQUERY *query)
{
    int i = 0, x = 0, n = 0, k = 0, z = 0, *np = NULL, nqterms = 0, nquerys = 0, next = 0,
        is_query_phrase =  0, docid = 0, *intidx = NULL, ifrom = 0, mm = 0, nn = 0,
        is_sort_reverse = 0, max = 0, int_index_from = 0, int_index_to = 0, prev = 0,
        double_index_from = 0, nres = 0, min = 0, double_index_to = 0, ito = 0,
        ignore_score = 0, last = 0, no = 0, fid = 0, range_flag = 0, xno = 0,
        is_field_sort = 0, scale = 0, total = 0, ignore_rank = 0, nxrecords = 0,
        long_index_from = 0, long_index_to = 0, nx = 0, kk = 0, prevnext = 0;
    double *doubleidx = NULL, score = 0.0, p1 = 0.0, p2 = 0.0, ffrom = 0.0,
           tf = 1.0, Py = 0.0, Px = 0.0, fto = 0.0;
    void *timer = NULL, *topmap = NULL, *fmap = NULL;//, *dp = NULL, *olddp = NULL;
    int64_t bits = 0, *longidx = NULL, lfrom = 0, lto = 0, old_score = 0, 
            base_score = 0, doc_score = 0, xdata = 0, key = 0, val = 0, 
            *pkey = &key, *pval = &val;
    IRECORD *record = NULL, *records = NULL, xrecords[IB_NTOP_MAX];
    IHEADER *headers = NULL; ICHUNK *chunk = NULL;
    void *stree = NULL; XNODE *xnode = NULL;
    TERMSTATE *termstates = NULL;
    ITERM *itermlist = NULL;
    IRES *res = NULL;

    if(ibase && query)
    {
        if((chunk = ibase_pop_chunk(ibase)))
        {
            res = &(chunk->res);
            memset(res, 0, sizeof(IRES));
            res->qid = query->qid;
            records = chunk->records;
        }
        else
        {
            FATAL_LOGGER(ibase->logger, "pop chunk failed, %s", strerror(errno));
            goto end;
        }
        stree = ibase_pop_stree(ibase);
        itermlist = ibase_pop_itermlist(ibase);
        topmap = ibase_pop_stree(ibase);
        fmap = ibase_pop_stree(ibase);
        headers = (IHEADER *)(ibase->headersio.map);
        intidx = (int *)(ibase->intidxio.map);
        longidx = (int64_t *)(ibase->longidxio.map);
        doubleidx = (double *)(ibase->doubleidxio.map);
        if((p1 = query->ravgdl) <= 0.0) p1 = 1.0;
        if((query->flag & IB_QUERY_PHRASE)) is_query_phrase = 1;
        if((query->flag & IB_QUERY_RSORT)) is_sort_reverse = 1;        
        else if((query->flag & IB_QUERY_SORT)) is_sort_reverse = 0;
        else is_sort_reverse = 1;
        int_index_from = ibase->state->int_index_from;
        int_index_to = int_index_from + ibase->state->int_index_fields_num;
        long_index_from = ibase->state->long_index_from;
        long_index_to = long_index_from + ibase->state->long_index_fields_num;
        double_index_from = ibase->state->double_index_from;
        double_index_to = double_index_from + ibase->state->double_index_fields_num;
        if((query->flag & IB_QUERY_PHRASE)) is_query_phrase = 1;
        nqterms = query->nqterms;
        if(query->nqterms > IB_QUERY_MAX) nqterms = IB_QUERY_MAX;
        nquerys = query->nquerys;
        if(query->nquerys > IB_QUERY_MAX) nquerys = IB_QUERY_MAX;
        if(topmap == NULL || fmap == NULL || stree == NULL 
                || itermlist == NULL || headers == NULL) goto end;
        scale = query->hitscale[nquerys-1];
        if((fid = query->int_order_field) >= int_index_from && fid < int_index_to)
        {
            is_field_sort = IB_SORT_BY_INT;
            fid -= int_index_from;
        }
        else if((fid = query->long_order_field) >= long_index_from && fid < long_index_to)
        {
            is_field_sort = IB_SORT_BY_LONG;
            fid -= long_index_from;
        }
        else if((fid = query->double_order_field) >= double_index_from && fid < double_index_to)
        {
            is_field_sort = IB_SORT_BY_DOUBLE;
            fid -= double_index_from;
        }
        TIMER_INIT(timer);
        //read index 
        for(i = 0; i < nqterms; i++)
        {
            itermlist[i].idf = query->qterms[i].idf;
            itermlist[i].termid = query->qterms[i].id;
            if((n = itermlist[i].mm.ndata = db_get_data(PDB(ibase->index), itermlist[i].termid, &(itermlist[i].mm.data))) > 0)
            {
                itermlist[i].p = itermlist[i].mm.data;
                itermlist[i].end = itermlist[i].mm.data + n;
                itermlist[i].docid = 0;
                itermlist[i].last = -1;
                MUTEX_LOCK(ibase->mutex_termstate);
                termstates = (TERMSTATE *)(ibase->termstateio.map);
                itermlist[i].term_len = termstates[itermlist[i].termid].len;
                MUTEX_UNLOCK(ibase->mutex_termstate);
                x = i;
                ibase_unxindex(ibase, itermlist, stree, is_query_phrase, query->qfhits, x);
            }
        }
        TIMER_SAMPLE(timer);
        res->io_time = (int)PT_LU_USEC(timer);
        ACCESS_LOGGER(ibase->logger, "reading index data qid:%d terms:%d querys:%d bytes:%d time used :%lld", query->qid, query->nqterms, query->nquerys, total, PT_LU_USEC(timer));
        res->total = 0;
        while(MTREE64_POP_MIN(stree, pkey, pval) == 0)
        {
            docid = (int)key;
            xnode = (XNODE *)((long)val);
            if(last != -1 && docid < last)
            {
                FATAL_LOGGER(ibase->logger, "docid:%d last:%d", docid, last);
                _exit(-1);
            }
            last = docid;
            ignore_score = 0;
            ignore_rank = 0;
            doc_score = 0;
            base_score = 0;
            score = 0.0;
            if(headers[docid].status < 0 || headers[docid].globalid == 0)
            {
                goto next;
            }
            /* check fobidden terms in query string */
            if((query->flag & IB_QUERY_FORBIDDEN) && headers[docid].status<IB_SECURITY_OK)goto next;
            /* secure level */
            if((k = headers[docid].slevel) < 0 || headers[docid].slevel > IB_SLEVEL_MAX || query->slevel_filter[k]  == 1) goto next;
            /* catetory block filter */
            if(query->catblock_filter != 0 && (query->catblock_filter & headers[docid].category)) 
            {
                //WARN_LOGGER(ibase->logger, "catblock_filter:%lld category:%lld", IBLL(query->catblock_filter), IBLL(headers[docid].category));
                goto next;
            }
            /* multicat filter */
            if(query->multicat_filter != 0 && (query->multicat_filter & headers[docid].category) != query->multicat_filter)
                goto next;
            /* fields filter */
            if(query->fields_filter != 0 && query->fields_filter != -1
                    && !(query->fields_filter & xnode->bitfields))
                goto next;
            // boolen check 
            if(query->operators.bitsnot && (query->operators.bitsnot & xnode->bithits))
                goto next;
            if((query->flag & IB_QUERY_BOOLAND) && query->operators.bitsand 
                    && (query->nqterms != query->nquerys || (query->operators.bitsand & xnode->bithits) != query->operators.bitsand))
                goto next;
            if(!(query->flag & IB_QUERY_BOOLAND) && ((xnode->nhits * 100) / nquerys) < scale) goto next;
            /* int range  filter */
            if(intidx)
            {
                for(i = 0; i < query->int_range_count; i++)
                {
                    if((k = query->int_range_list[i].field_id) >= int_index_from 
                            && k < int_index_to && (range_flag = query->int_range_list[i].flag))
                    {
                        ifrom = query->int_range_list[i].from;
                        ito = query->int_range_list[i].to;
                        k -= int_index_from;
                        k += ibase->state->int_index_fields_num * docid;
                        if((range_flag & IB_RANGE_FROM) && intidx[k] < ifrom) goto next;
                        if((range_flag & IB_RANGE_TO) && intidx[k] > ito) goto next;
                        ACCESS_LOGGER(ibase->logger, "from:%d to:%d k:%d",  ifrom, ito, intidx[k]);
                    }
                }
                for(i = 0; i < query->int_bits_count; i++)
                {
                    if((k = query->int_bits_list[i].field_id) >= int_index_from 
                            && k < int_index_to && query->int_bits_list[i].bits != 0) 
                    {
                        k -= int_index_from;
                        k += ibase->state->int_index_fields_num * docid;
                        if((query->int_bits_list[i].flag & IB_BITFIELDS_FILTER))
                        {
                            if((query->int_bits_list[i].bits & intidx[k]) == 0) goto next;
                        }
                        else
                        {
                            if((query->int_bits_list[i].bits & intidx[k])) goto next;
                        }
                    }
                }
            }
            /* long range  filter */
            if(longidx)
            {
                for(i = 0; i < query->long_range_count; i++)
                {
                    if((k = query->long_range_list[i].field_id) >= long_index_from 
                            && k < long_index_to && (range_flag = query->long_range_list[i].flag))
                    {
                        lfrom = query->long_range_list[i].from;
                        lto = query->long_range_list[i].to;
                        k -= long_index_from;
                        k += ibase->state->long_index_fields_num * docid;
                        if((range_flag & IB_RANGE_FROM) && longidx[k] < lfrom) goto next;
                        if((range_flag & IB_RANGE_TO) && longidx[k] > lto) goto next;
                    }
                }
                for(i = 0; i < query->long_bits_count; i++)
                {
                    if((k = query->long_bits_list[i].field_id) >= long_index_from 
                            && k < long_index_to && query->long_bits_list[i].bits != 0)
                    {
                        k -= long_index_from;
                        k += ibase->state->long_index_fields_num * docid;
                        if((query->long_bits_list[i].flag & IB_BITFIELDS_FILTER))
                        {
                            if((query->long_bits_list[i].bits & longidx[k]) == 0) goto next;
                        }
                        else
                        {
                            if((query->long_bits_list[i].bits & longidx[k])) goto next;
                        }
                    }
                }

            }
            /* double range filter */
            if(query->double_range_count > 0 && doubleidx)
            {
                for(i = 0; i < query->double_range_count; i++)
                {
                    if((k = query->double_range_list[i].field_id) >= double_index_from 
                            && k < double_index_to 
                            && (range_flag = query->double_range_list[i].flag))
                    {
                        ffrom = query->double_range_list[i].from;
                        fto = query->double_range_list[i].to;
                        k -= double_index_from;
                        k += ibase->state->double_index_fields_num * docid;
                        if((range_flag & IB_RANGE_FROM) && doubleidx[k] < ffrom) goto next;
                        if((range_flag & IB_RANGE_TO) && doubleidx[k] > fto) goto next;
                    }
                }
            }
            /* category grouping  */
            if((bits = (query->catgroup_filter & headers[docid].category)))
            {
                k = 0;
                do
                {
                    if((bits & (int64_t)0x01))
                    {
                        if(res->catgroups[k] == 0) res->ncatgroups++;
                        res->catgroups[k]++;
                    }
                    bits >>= 1;
                    ++k;
                }while(bits);
            }

            /* cat filter */
            if(query->category_filter != 0 && (query->category_filter & headers[docid].category) == 0)
                goto next;

             ACCESS_LOGGER(ibase->logger, "catgroup docid:%d category:%lld", docid, IBLL(headers[docid].category));
            /* hits/fields hits */
            base_score += IBLONG(xnode->nhits * query->base_hits);
            base_score += IBLONG(xnode->nhitfields * query->base_fhits);
            mm = xnode->nhits - 1;
            nn = 0;
            /* scoring */
            i = --(xnode->nhits);
            do
            {
                x = xnode->hits[i];
                /* phrase  */
                if(ibase->state->phrase_status != IB_PHRASE_DISABLED
                        && is_query_phrase && itermlist[x].sprevnext)
                {
                    no = 0;
                    nx = 0;
                    xno = -1;
                    do
                    {
                        prev = -1;
                        next = -1;
                        z = 0;
                        np = &z;
                        UZVBCODE(itermlist[x].sprevnext, n, np);
                        nx += z;
                        no = nx >> 1;
                        if(nx & 0x01) next = no;
                        else prev = no;
                        if(no < xnode->xmin)continue;
                        if(no > xnode->xmax)break;
                        if(no != xno)
                        {
                            min = nn;max = mm;
                            k = -1;
                            do
                            {
                                if(no == xnode->bitphrase[min]){k = min;break;}
                                if(no == xnode->bitphrase[max]){k = max;break;}
                                z = (max + min)/2;
                                if(z == min) break;
                                if(no == xnode->bitphrase[z]){k = z;break;}
                                else if(no > xnode->bitphrase[z]) min = z;
                                else max = z;
                            }while(max > min);
                        }
                        if(k >= 0 && no == xnode->bitphrase[k]) 
                        {
                            kk = xnode->bitquery[k];
                            prevnext = 0;
                            if(prev >= 0 && (query->qterms[x].prev & (1 << kk)))
                            {
                                ACCESS_LOGGER(ibase->logger, "docid:%d phrase_prev:%d x:%d kk:%d", docid, prev, x, kk);
                                prevnext = 2;
                            }
                            else if(next >= 0 && (query->qterms[x].next & (1 << kk)))
                            {
                                ACCESS_LOGGER(ibase->logger, "docid:%d phrase_next:%d x:%d kk:%d", docid, next, x, kk);
                                prevnext = 2;
                            }
                            if(prevnext)
                            {
                                if((itermlist[x].fields & query->qfhits) 
                                        && (itermlist[kk].fields & query->qfhits))
                                {
                                    prevnext += 4;
                                }
                                base_score += IBLONG(query->base_phrase * prevnext);
                            }
                        }
                        xno = no;
                    }while(itermlist[x].sprevnext < itermlist[x].eprevnext);
                }
                if((query->flag & IB_QUERY_RELEVANCE) && is_field_sort == 0)
                {
                    tf = (double)(itermlist[x].term_count)
                        /(double)(headers[docid].terms_total);
                    p2 = (double)(headers[docid].terms_total) / p1;
                    Py = itermlist[x].idf * tf * IB_BM25_P1;
                    Px = tf + IB_BM25_K1 - IB_BM25_P2 + IB_BM25_P2 * p2;
                    score += (Py/Px);
                    ACCESS_LOGGER(ibase->logger, "term:%d docid:%d/%lld p1:%f p2:%f ttotal:%d tf:%f idf:%f score:%f", itermlist[x].termid, docid, IBLL(headers[docid].globalid), p1, p2, headers[docid].terms_total, tf, itermlist[x].idf, score);
                }
                base_score += IBLONG(itermlist[x].term_len * query->base_nterm);
                //xnode->bitphrase[i] = 0;
                //xnode->bitquery[i] = 0;
                ibase_unxindex(ibase, itermlist, stree, is_query_phrase, query->qfhits, x);
            }while(--i >= 0);
            /* bitxcat */
            if(headers[docid].category != 0)
            {
                if(query->bitxcat_up != 0 && (headers[docid].category & query->bitxcat_up))
                    base_score += IBLONG(query->base_xcatup);
                else if(query->bitxcat_down != 0 && (headers[docid].category & query->bitxcat_down))
                {
                    ignore_rank = 1;
                    if(base_score > IBLONG(query->base_xcatdown))
                        base_score -= IBLONG(query->base_xcatdown);
                    else
                        base_score = 0ll;
                }
            }
            doc_score = IB_LONG_SCORE(((double)base_score+(double)score));
            ACCESS_LOGGER(ibase->logger, "docid:%d/%lld base_score:%lld score:%f doc_score:%lld", docid, IBLL(headers[docid].globalid), IBLL(base_score), score, IBLL(doc_score));
            /* rank */
            if(ignore_rank == 0 && (query->flag & IB_QUERY_RANK)) 
                doc_score += IBLONG((double)headers[docid].rank*(double)query->base_rank);
            ACCESS_LOGGER(ibase->logger, "docid:%d/%lld base_score:%lld rank:%f base_rank:%lld doc_score:%lld", docid, IBLL(headers[docid].globalid), IBLL(base_score), headers[docid].rank, IBLL(query->base_rank), IBLL(doc_score));
            /* sorting */
            if(MTREE64_TOTAL(topmap) >= query->ntop)
            {
                xdata = 0ll;
                if(is_sort_reverse || is_field_sort)
                {
                    if(doc_score >= MTREE64_MINK(topmap))
                    {
                        mtree64_pop_min(MTR64(topmap), &old_score, &xdata);
                        if((record = (IRECORD *)((long)xdata)))
                        {
                            record->globalid    = (int64_t)docid;
                            mtree64_push(MTR64(topmap), doc_score, (int64_t)record);
                        }
                    }
                }
                else
                {
                    if(doc_score <= MTREE64_MAXK(topmap))
                    {
                        mtree64_pop_max(MTR64(topmap), &old_score, &xdata);
                        if((record = (IRECORD *)((long)xdata)))
                        {
                            record->globalid    = (int64_t)docid;
                            mtree64_push(MTR64(topmap), doc_score, (int64_t)record);
                        }
                    }
                }
            }
            else
            {
                if(nxrecords < IB_NTOP_MAX && (record = &(xrecords[nxrecords++])))
                {
                    record->globalid = docid;
                    mtree64_push(MTR64(topmap), doc_score, (int64_t)record);
                }
            }
            res->total++;
            continue;

next:
            i = --(xnode->nhits);
            do
            {
                x = xnode->hits[i];
                xnode->bitphrase[i] = 0;
                xnode->bitquery[i] = 0;
                ibase_unxindex(ibase, itermlist, stree, is_query_phrase, query->qfhits, x);
            }while(--i >= 0);
        }
        TIMER_SAMPLE(timer);
        res->sort_time = (int)PT_LU_USEC(timer);
        ACCESS_LOGGER(ibase->logger, "xsort(%d) %d documents res:%d time used:%lld", query->qid, res->total, MTREE64_TOTAL(topmap), PT_LU_USEC(timer));
        //out result 
        if((res->count = nres = MTREE64_TOTAL(topmap)) > 0)
        {
            if(is_field_sort)
            {
                do
                {
                    xdata = 0;
                    mtree64_pop_max(MTR64(topmap), &doc_score, &xdata);
                    if((record = (IRECORD *)((long)xdata)))
                    {
                        docid = (int)record->globalid;
                        if(is_field_sort == IB_SORT_BY_INT && intidx)
                        {
                            i = fid + ibase->state->int_index_fields_num * docid;
                            doc_score = IB_INT2LONG_SCORE(intidx[i]) + (int64_t)(doc_score >> 16);
                            //doc_score = IBLONG(intidx[i]);
                        }
                        else if(is_field_sort == IB_SORT_BY_LONG && longidx)
                        {
                            i = fid + ibase->state->long_index_fields_num * docid;
                            doc_score = longidx[i];
                        }
                        else if(is_field_sort == IB_SORT_BY_DOUBLE && doubleidx)
                        {
                            i = fid + ibase->state->double_index_fields_num * docid;
                            doc_score = IB_LONG_SCORE(doubleidx[i]);
                        }
                        mtree64_push(MTR64(fmap), doc_score, (int64_t)record);
                    }
                }while(MTREE64_TOTAL(topmap) > 0);
                i = 0;
                do
                {
                    xdata = 0;
                    if(is_sort_reverse){mtree64_pop_max(MTR64(fmap), &doc_score, &xdata);}
                    else{mtree64_pop_min(MTR64(fmap), &doc_score, &xdata);}
                    if((record = (IRECORD *)((long )xdata)))
                    {
                        docid = (int)record->globalid;
                        records[i].score = doc_score;
                        records[i].globalid = (int64_t)headers[docid].globalid;
                        ACCESS_LOGGER(ibase->logger, "top[%d/%d] docid:%d/%lld score:%lld", i, MTREE64_TOTAL(fmap), docid, IBLL(headers[docid].globalid), IBLL(doc_score));
                    }
                    i++;
                }while(MTREE64_TOTAL(fmap) > 0);
            }
            else
            {
                i = 0;
                do
                {
                    xdata = 0;
                    if(is_sort_reverse){mtree64_pop_max(MTR64(topmap), &doc_score, &xdata);}
                    else{mtree64_pop_min(MTR64(topmap), &doc_score, &xdata);}
                    if((record = (IRECORD *)((long)xdata)))
                    {
                        docid = (int)record->globalid;
                        records[i].score = doc_score;
                        records[i].globalid = (int64_t)headers[docid].globalid;
                        ACCESS_LOGGER(ibase->logger, "top[%d/%d] docid:%d/%lld score:%lld", i, MTREE64_TOTAL(topmap), docid, IBLL(headers[docid].globalid), IBLL(doc_score));
                    }
                    ++i;
                }while(MTREE64_TOTAL(topmap) > 0);
            }
        }

end:
        //free db blocks
        if(itermlist)
        {
            for(i = 0; i < nqterms; i++)
            {
                db_free_data(PDB(ibase->index), itermlist[i].mm.data, itermlist[i].mm.ndata);
            }
            ibase_push_itermlist(ibase, itermlist);
        }
        if(res) res->doctotal = ibase->state->dtotal;
        if(stree) ibase_push_stree(ibase, stree);
        if(topmap) ibase_push_stree(ibase, topmap);
        if(fmap) ibase_push_stree(ibase, fmap);
        TIMER_CLEAN(timer);
    }
    return chunk;
}
