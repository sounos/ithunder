#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "ibase.h"
#include "db.h"
#include "timer.h"
#include "zvbcode.h"
//#include "qsmap.h"
#include "logger.h"
#include "xmm.h"
/* push MBITMAP */
void ibase_push_bitmap(IBASE *ibase, IBITMAP *bitmap)
{
    int x = 0;

    if(ibase && bitmap)
    {
        MUTEX_LOCK(ibase->mutex_bitmap);
        if(ibase->nqbitmaps < IB_BITMAPS_MAX)
        {
            x = ibase->nqbitmaps++;
            ibase->qbitmaps[x] = bitmap;
        }
        else
        {
            xmm_free(bitmap, IB_BITMAP_COUNT * sizeof(IBITMAP));
        }
        MUTEX_UNLOCK(ibase->mutex_bitmap);
    }
    return ;
}

/* ibase pop bitmap */
IBITMAP *ibase_pop_bitmap(IBASE *ibase)
{
    IBITMAP *bitmap = NULL;
    int x = 0;

    if(ibase)
    {
        MUTEX_LOCK(ibase->mutex_bitmap);
        if(ibase->nqbitmaps > 0)
        {
            x = --(ibase->nqbitmaps);
            bitmap = ibase->qbitmaps[x];
            ibase->qbitmaps[x] = NULL;
        }
        else
        {
            bitmap = (IBITMAP *)xmm_new(IB_BITMAP_COUNT * sizeof(IBITMAP));
        }
        MUTEX_UNLOCK(ibase->mutex_bitmap);
    }
    return bitmap;
}

/* query with bit map merging */
ICHUNK *ibase_mquery(IBASE *ibase, IQUERY *query)
{
    int i = 0, x = 0, n = 0, *np = NULL, from = 0, to = 0, left = 0, nqterms = 0, bits = 0,
        is_query_phrase = 0, k = 0, min = -1, max = -1, docid = 0, doc_score = 0, ifrom = 0, 
        *intidx = NULL, is_sort_reverse = 0, int_index_from = 0, int_index_to = 0, 
        double_index_from = 0, double_index_to = 0, ignore_score = 0, ito = 0,
        old_score = 0, nres = 0, nxrecords = 0;
    double *doubleidx = NULL, score = 0.0, p1 = 0.0, p2 = 0.0, ffrom = 0.0,
           tf = 1.0, Py = 0.0, Px = 0.0, fto = 0.0;
    void *timer = NULL, *topmap = NULL, *dp = NULL, *olddp = NULL;
    IRECORD *record = NULL, *records = NULL, xrecords[IB_NTOP_MAX];
    IBITMAP *bitmaps = NULL;  ITERM *itermlist = NULL;
    IHEADER *headers = NULL; ICHUNK *chunk = NULL;
    IRES *res = NULL;

    if(ibase && query)
    {
        if((chunk = ibase_pop_chunk(ibase)))
        {
            res = &(chunk->res);
            memset(res, 0, sizeof(IRES));
            records = chunk->records;
        }
        else
        {
            FATAL_LOGGER(ibase->logger, "pop chunk failed, %s", strerror(errno));
            goto end;
        }
        bitmaps = ibase_pop_bitmap(ibase);
        itermlist = ibase_pop_itermlist(ibase);
        topmap = ibase_pop_smap(ibase);
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
        TIMER_INIT(timer);
        if((query->flag & IB_QUERY_PHRASE)) is_query_phrase = 1;
        nqterms = query->nqterms;
        if(query->nqterms > IB_QUERY_MAX) nqterms = IB_QUERY_MAX;
        if(topmap == NULL || bitmaps == NULL || itermlist == NULL || headers == NULL) goto end;
        //read index 
        for(i = 0; i < nqterms; i++)
        {
            itermlist[i].idf = query->qterms[i].idf;
            itermlist[i].termid = query->qterms[i].id;
            if((n = itermlist[i].mm.ndata = db_get_data(PDB(ibase->index), itermlist[i].termid, &(itermlist[i].mm.data))) > 0)
            {
                itermlist[i].p = itermlist[i].mm.data;
                itermlist[i].end = itermlist[i].mm.data + itermlist[i].mm.ndata;
                itermlist[i].docid = 0;
                itermlist[i].last = -1;
                //merge 
            }
        }
        TIMER_SAMPLE(timer);
        res->io_time = (int)PT_LU_USEC(timer);
        //merge
        left = query->nqterms;
        from = 0;
        to = IB_BITMAP_COUNT;
        int total = 0;
        do
        {
            //uncompress index 
            i = 0; min = -1; max = -1;
            do
            {
                while(itermlist[i].docid >= from && itermlist[i].docid < to)
                {
                    //fprintf(stdout, "%s::%d i:%d docid:%d left:%d from:%d to:%d min:%d max:%d p:%p end:%p\n", __FILE__, __LINE__, i, itermlist[i].docid, left, from ,to, min, max, itermlist[i].p, itermlist[i].end);
                    if(itermlist[i].last == itermlist[i].docid)
                    {
                        x = itermlist[i].docid - from;
                        if(min == -1) min = x; 
                        else if(x < min) min = x;
                        if(max == -1) max = x; 
                        else if(x > max) max = x;
                        bitmaps[x].xterms[i].term_count = itermlist[i].term_count;
                        bitmaps[x].xterms[i].sprevnext = itermlist[i].sprevnext - itermlist[i].mm.data;
                        bitmaps[x].xterms[i].eprevnext = itermlist[i].eprevnext - itermlist[i].mm.data;
                        bitmaps[x].bithits |= 1 << i;
                        bitmaps[x].bitfields |= itermlist[i].fields;
                        bitmaps[x].nhits++;
                        itermlist[i].last = -1;
                    }
                    if(itermlist[i].p == NULL)
                    {
                        itermlist[i].docid = -1;
                        break;
                    }
                    if(itermlist[i].p && itermlist[i].p < itermlist[i].end)
                    {
                        if(ibase->state->compression_status != IB_COMPRESSION_DISABLED)
                        {
                            itermlist[i].ndocid = 0;
                            itermlist[i].term_count = 0;
                            itermlist[i].no = 0;
                            itermlist[i].fields = 0;
                            itermlist[i].sprevnext = NULL;
                            itermlist[i].eprevnext = NULL;
                            itermlist[i].prevnext_size = 0;
                            np = &(itermlist[i].ndocid);
                            UZVBCODE(itermlist[i].p, n, np);
                            itermlist[i].docid +=  itermlist[i].ndocid;
                            np = &(itermlist[i].term_count);
                            UZVBCODE(itermlist[i].p, n, np);
                            np = &(itermlist[i].no);
                            UZVBCODE(itermlist[i].p, n, np);
                            np = &(itermlist[i].fields);
                            UZVBCODE(itermlist[i].p, n, np);
                            np = &(itermlist[i].prevnext_size);
                            UZVBCODE(itermlist[i].p, n, np);
                        }
                        else
                        {
                            itermlist[i].docid = *((int*)itermlist[i].p);
                            itermlist[i].p += sizeof(int);
                            itermlist[i].term_count = *((int*)itermlist[i].p);
                            itermlist[i].p += sizeof(int);
                            itermlist[i].no = *((int*)itermlist[i].p);
                            itermlist[i].p += sizeof(int);
                            itermlist[i].fields = *((int*)itermlist[i].p);
                            itermlist[i].p += sizeof(int);
                            itermlist[i].prevnext_size = *((int*)itermlist[i].p);
                            itermlist[i].p += sizeof(int);
                        }
                        if(itermlist[i].prevnext_size > 0)
                        {
                            itermlist[i].sprevnext = itermlist[i].p;
                            itermlist[i].eprevnext = itermlist[i].p + itermlist[i].prevnext_size;
                            itermlist[i].p += itermlist[i].prevnext_size;
                        }
                        else
                        {
                            itermlist[i].sprevnext = NULL;
                            itermlist[i].eprevnext = NULL;
                        }
                        if(itermlist[i].p == itermlist[i].end) 
                        {
                            itermlist[i].p = NULL;
                            --left;
                        }
                    }
                    //bithits bitfields bitphrase 
                    if(itermlist[i].docid  >= from && itermlist[i].docid < to)
                    {
                        x = itermlist[i].docid - from;
                        if(min == -1) min = x; 
                        else if(x < min) min = x;
                        if(max == -1) max = x; 
                        else if(x > max) max = x;
                        bitmaps[x].xterms[i].term_count = itermlist[i].term_count;
                        bitmaps[x].xterms[i].sprevnext = itermlist[i].sprevnext - itermlist[i].mm.data;
                        bitmaps[x].xterms[i].eprevnext = itermlist[i].eprevnext - itermlist[i].mm.data;
                        bitmaps[x].bithits |= 1 << i;
                        bitmaps[x].bitfields |= itermlist[i].fields;
                        bitmaps[x].nhits++;
                        itermlist[i].last = -1;
                    }
                    else
                    {
                        itermlist[i].last = itermlist[i].docid;
                    }
                }
            }while(++i <= nqterms);
            //scoring 
            x = min;
            if(min == -1 || max == -1) goto next_index;
            do
            {
                if(bitmaps[x].nhits > 0)
                {
                    bitmaps[x].bithits = 0;
                    bitmaps[x].bitfields = 0;
                    bitmaps[x].nhits = 0;
                    total++;
                }
            }while(++x <= max);
            goto next_index;
            /*
            */
            do
            {
                //scoring
                if(bitmaps[x].nhits > 0)
                {
                    docid = from + x;
                    ignore_score = 0;
                    doc_score = 0;
                    score = 0;
                    if(headers[docid].status < 0 || headers[docid].globalid < 0) goto next;
                    /* check fobidden terms in query string */
                    if((query->flag & IB_QUERY_FORBIDDEN) && headers[docid].status<IB_SECURITY_OK)goto next;
                    // slevel filter 
                    if((k = headers[docid].slevel) < 0 || headers[docid].slevel > IB_SLEVEL_MAX || query->slevel_filter[k]  == 1) goto next;
                    /* catetory block filter */
                    if(query->catblock_filter != 0 && (query->catblock_filter & headers[docid].category)) goto next;
                    // fields filter 
                    if(query->fields_filter != 0 && query->fields_filter != -1
                            && !(query->fields_filter & bitmaps[x].bitfields))
                        goto next;
                    // category filter
                    if(query->category_filter != 0 && query->category_filter != -1
                            && (query->category_filter & headers[docid].category)
                            != query->category_filter)
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
                    // boolen check 
                    if(query->operators.bitsnot && (query->operators.bitsnot & bitmaps[x].bithits))
                        goto next;
                    //if(query->operators.bitsand && (query->operators.bitsand & bitmaps[x].bithits) 
                    //        != query->operators.bitsand)
                    //    goto next;
                    //if(PQS(topmap)->count >= query->ntop && query->operators.bitsand != 0
                    //   && (query->operators.bitsand & bitmaps[x].bithits) 
                    //   != query->operators.bitsand)
                    //   goto next;
                    if((k = query->int_order_field) >= int_index_from && k < int_index_to)
                    {
                        k -= int_index_from;
                        k += ibase->state->int_index_fields_num * docid;
                        doc_score = intidx[k];
                        ignore_score = 1;
                    }
                    else if((k = query->double_order_field) >= double_index_from 
                            && k < double_index_to)
                    {
                        k -= double_index_from;
                        k += ibase->state->double_index_fields_num * docid;
                        doc_score = IB_INT_SCORE(doubleidx[k]);
                        ignore_score = 1;
                    }
                    //fprintf(stdout, "%s::%d docid:%d\n", __FILE__, __LINE__, docid);
                    // group 
                    if((bits = (query->catgroup_filter & headers[docid].category)))
                    {
                        k = 0;
                        do
                        {
                            if((bits & 0x01))
                            {
                                if(res->catgroups[k] == 0) res->ncatgroups++;
                                res->catgroups[k]++;
                            }
                            bits >>= 1;
                            ++k;
                        }while(bits);
                    }
                    if(ignore_score == 0)
                    {
                        doc_score += IB_INT_SCORE(bitmaps[x].nhits * IB_HITS_SCORE);
                        if(query->flag & IB_QUERY_RANK)
                            doc_score += IB_INT_SCORE(headers[docid].rank);
                    }
                    for(i = 0; i < nqterms; i++)
                    {
                        if(ignore_score == 0)
                        {
                            tf = (double)(bitmaps[x].xterms[i].term_count)
                                /(double)(headers[docid].terms_total);
                            // caculate phrase 
                            if(ibase->state->phrase_status != IB_PHRASE_DISABLED
                                    && is_query_phrase && bitmaps[x].xterms[i].sprevnext > 0)
                            {
                            }
                            p2 = (double)(headers[docid].terms_total) * p1;
                            Py = itermlist[i].idf * tf * IB_BM25_P1;
                            Px = tf + IB_BM25_K1 - IB_BM25_P2 + IB_BM25_P2 * p2;
                            score = Py/Px;
                            doc_score += IB_INT_SCORE(score);
                        }
                        bitmaps[x].xterms[i].bitphrase = 0;
                        bitmaps[x].xterms[i].sprevnext = 0;
                        bitmaps[x].xterms[i].eprevnext = 0;
                        bitmaps[x].xterms[i].term_count = 0;
                    }
                    res->total++;
                    //fprintf(stdout, "%s::%d x:%d docid:%d score:%d\n", __FILE__, __LINE__, x, docid, doc_score);
                    if(PQS(topmap)->count >= query->ntop)
                    {
                        dp = olddp = NULL;
                        if(is_sort_reverse)
                        {
                            if(doc_score > PQS_MINK(topmap))
                            {
                                QSMAP_POP_RMIN(topmap, old_score, dp);
                                if((record = (IRECORD *)dp))
                                {
                                    record->globalid    = headers[docid].globalid;
                                    record->score       = doc_score;
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
                                    record->score       = doc_score;
                                    QSMAP_ADD(topmap, doc_score, dp, olddp);
                                }
                            }
                        }
                    }
                    else
                    {
                        if(nxrecords < IB_NTOP_MAX && (record = &(xrecords[nxrecords++])))
                        {
                            record->globalid    = headers[docid].globalid;
                            record->score       = doc_score;
                            dp = (void *)record;
                            QSMAP_ADD(topmap, doc_score, dp, olddp);
                        }
                    }
                    bitmaps[x].nhits = 0;
                    bitmaps[x].bithits = 0;
                    bitmaps[x].bitfields = 0;
                    continue;
next:
                    for(i = 0; i < nqterms; i++)
                    {
                        bitmaps[x].xterms[i].bitphrase = 0;
                        bitmaps[x].xterms[i].sprevnext = 0;
                        bitmaps[x].xterms[i].eprevnext = 0;
                        bitmaps[x].xterms[i].term_count = 0;
                    }
                    bitmaps[x].nhits = 0;
                    bitmaps[x].bithits = 0;
                    bitmaps[x].bitfields = 0;
                }
            }while(++x <= max);
    next_index:
            to += IB_BITMAP_COUNT;
            from += IB_BITMAP_COUNT;
        }while(left > 0);
        TIMER_SAMPLE(timer);
        res->sort_time = (int)PT_LU_USEC(timer);
        fprintf(stdout, "%s::%d io_time:%d hits:%d sort[%d](%d)-[%d] time_used:%d\n", __FILE__, __LINE__, res->io_time, nqterms, total, res->total, PQS(topmap)->count, res->sort_time);
        //out result 
        if((res->count = nres = PQS(topmap)->count) > 0)
        {
            i = 0;
            do
            {
                dp = NULL;
                if(is_sort_reverse){QSMAP_POP_RMAX(topmap, doc_score, dp);}
                else{QSMAP_POP_RMIN(topmap, doc_score, dp);}
                if(dp)
                {
                    memcpy(&(records[i]), dp, sizeof(IRECORD));
                    //fprintf(stdout, "%s::%d docid:%d score:%d\n", __FILE__, __LINE__, records[i].globalid, doc_score);
                }
                ++i;
            }while(QS_ROOT(topmap));
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
        if(bitmaps) ibase_push_bitmap(ibase, bitmaps);
        if(topmap) ibase_push_smap(ibase, topmap);
        if(itermlist) ibase_push_itermlist(ibase, itermlist);
        if(nres == 0){ibase_push_chunk(ibase, chunk); chunk = NULL;}
        TIMER_CLEAN(timer);
    }
    return chunk;
}
