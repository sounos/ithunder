#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "ibase.h"
#include "db.h"
#include "timer.h"
#include "zvbcode.h"
#include "logger.h"
#include "xmm.h"
#include "mtree64.h"
#include "imap.h"
#include "lmap.h"
#include "dmap.h"
 
/* binary list merging */
ICHUNK *ibase_query(IBASE *ibase, IQUERY *query)
{
    int i = 0, n = 0, k = 0,min_set_num = -1, 
        docid = 0, ifrom = -1, is_sort_reverse = 0, min_set_fid = 0, 
        int_index_from = 0, int_index_to = 0, ito = -1, double_index_from = 0, 
        double_index_to = 0, range_flag = 0,
        fid = 0, nxrecords = 0, is_field_sort = 0, 
        total = 0, ignore_rank = 0, long_index_from = 0, long_index_to = 0,
        ii = 0, jj = 0, imax = 0, imin = 0, xint = 0, off = 0,
        irangefrom = 0, irangeto = 0, query_range = 0;
    uint32_t *docs = NULL, docs_size = 0, ndocs = 0;
    double score = 0.0, dfrom = 0.0, dto = 0.0, drangefrom = 0.0, drangeto = 0.0,xdouble = 0.0;
    int64_t bits = 0, lfrom = 0, lto = 0, base_score = 0, lrangefrom = 0, lrangeto = 0, 
            doc_score = 0, old_score = 0, xdata = 0, xlong = 0;
    void *timer = NULL, *topmap = NULL;
    IRECORD *record = NULL, *records = NULL, xrecords[IB_NTOP_MAX];
    IHEADER *headers = NULL; ICHUNK *chunk = NULL;
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
            //ACCESS_LOGGER(ibase->logger, "pop chunk failed, %s", strerror(errno));
            goto end;
        }
        topmap = ibase_pop_stree(ibase);
        headers = (IHEADER *)(ibase->headersio.map);
        if((query->flag & IB_QUERY_RSORT)) is_sort_reverse = 1;        
        else if((query->flag & IB_QUERY_SORT)) is_sort_reverse = 0;
        else is_sort_reverse = 1;
        int_index_from = ibase->state->int_index_from;
        int_index_to = int_index_from + ibase->state->int_index_fields_num;
        long_index_from = ibase->state->long_index_from;
        long_index_to = long_index_from + ibase->state->long_index_fields_num;
        double_index_from = ibase->state->double_index_from;
        double_index_to = double_index_from + ibase->state->double_index_fields_num;
        if(topmap == NULL || headers == NULL) goto end;
        if((fid = query->int_order_field) >= int_index_from && fid < int_index_to)
        {
            fid -= int_index_from;
            fid += IB_INT_OFF;
            if(ibase->state->mfields[fid]) is_field_sort = IB_SORT_BY_INT;
        }
        else if((fid = query->long_order_field) >= long_index_from && fid < long_index_to)
        {
            fid -= long_index_from;
            fid += IB_LONG_OFF;
            if(ibase->state->mfields[fid]) is_field_sort = IB_SORT_BY_LONG;
        }
        else if((fid = query->double_order_field) >= double_index_from && fid < double_index_to)
        {
            fid -= double_index_from;
            fid += IB_DOUBLE_OFF;
            if(ibase->state->mfields[fid]) is_field_sort = IB_SORT_BY_DOUBLE;
        }
        TIMER_INIT(timer);
        min_set_fid = 0;
        if((k = query->in_int_fieldid) > 0 && query->in_int_num > 0)
        {
            k += IB_INT_OFF - int_index_from ;
            n = imap_ins(ibase->state->mfields[k], query->in_int_list, query->in_int_num, NULL);
            if(min_set_num == -1 || n < min_set_num)
            {min_set_fid = k;min_set_num = n;query_range = IB_RANGE_IN;}
        }
        if((k = query->in_long_fieldid) > 0 && query->in_long_num > 0)
        {
            k += IB_LONG_OFF - long_index_from ;
            n = lmap_ins(ibase->state->mfields[k], query->in_long_list, query->in_long_num, NULL);
            if(min_set_num == -1 || n < min_set_num)
            {min_set_fid = k;min_set_num = n;query_range = IB_RANGE_IN;}
        }
        if((k = query->in_double_fieldid) > 0 && query->in_double_num > 0)
        {
            k += IB_DOUBLE_OFF - double_index_from ;
            n = dmap_ins(ibase->state->mfields[k], query->in_double_list, query->in_double_num, NULL);
            if(min_set_num == -1 || n < min_set_num)
            {min_set_fid = k;min_set_num = n;query_range = IB_RANGE_IN;}
        }
        if(query->int_range_count > 0)
        {
            for(i = 0; i < query->int_range_count; i++)
            {
                if((k = query->int_range_list[i].field_id) >= int_index_from 
                        && k < int_index_to && (range_flag = query->int_range_list[i].flag))
                {
                    ifrom = query->int_range_list[i].from;
                    ito = query->int_range_list[i].to;
                    k -= int_index_from;
                    k += IB_INT_OFF;
                    if(!ibase->state->mfields[k]) continue;
                    if((range_flag & IB_RANGE_FROM))
                    {
                        n = imap_rangefrom(ibase->state->mfields[k], ifrom, NULL);
                        if(min_set_num == -1 || n < min_set_num)
                        {min_set_fid = k;min_set_num = n;irangefrom = ifrom;query_range=range_flag;}
                    }
                    else if((range_flag & IB_RANGE_TO))
                    {
                        n = imap_rangeto(ibase->state->mfields[k], ito, NULL);
                        if(min_set_num == -1 || n < min_set_num)
                        {min_set_fid = k;min_set_num = n;irangeto = ito;query_range=range_flag;}
                    }
                    else
                    {
                        n = imap_range(ibase->state->mfields[k], ifrom, ito, NULL);
                        if(min_set_num == -1 || n < min_set_num)
                        {min_set_fid = k;min_set_num = n;irangefrom = ifrom;irangeto = ito;}
                    }
                }
            }
        }
        if(query->long_range_count > 0)
        {
            for(i = 0; i < query->long_range_count; i++)
            {
                if((k = query->long_range_list[i].field_id) >= long_index_from 
                        && k < long_index_to && (range_flag = query->long_range_list[i].flag))
                {
                    lfrom = query->long_range_list[i].from;
                    lto = query->long_range_list[i].to;
                    k -= long_index_from;
                    k += IB_LONG_OFF;
                    if(!ibase->state->mfields[k]) continue;
                    if((range_flag & IB_RANGE_FROM))
                    {
                        n = lmap_rangefrom(ibase->state->mfields[k], lfrom, NULL);
                        if(min_set_num == -1 || n < min_set_num)
                        {min_set_fid = k;min_set_num = n;lrangefrom=lfrom;query_range=range_flag;}
                    }
                    else if((range_flag & IB_RANGE_TO))
                    {
                        n = lmap_rangeto(ibase->state->mfields[k], lto, NULL);
                        if(min_set_num == -1 || n < min_set_num)
                        {min_set_fid = k;min_set_num = n;lrangeto = lto;query_range = range_flag;}
                    }
                    else
                    {
                        n = lmap_range(ibase->state->mfields[k], lfrom, lto, NULL);
                        if(min_set_num == -1 || n < min_set_num)
                        {min_set_fid = k;min_set_num = n;lrangefrom = lfrom;lrangeto = lto;}
                    }
                }
            }
        }
        if(query->double_range_count > 0)
        {
            for(i = 0; i < query->double_range_count; i++)
            {
                if((k = query->double_range_list[i].field_id) >= double_index_from 
                        && k < double_index_to && (range_flag = query->double_range_list[i].flag))
                {
                    dfrom = query->double_range_list[i].from;
                    dto = query->double_range_list[i].to;
                    k -= double_index_from;
                    k += IB_DOUBLE_OFF;
                    if(!ibase->state->mfields[k]) continue;
                    if((range_flag & IB_RANGE_FROM))
                    {
                        n = dmap_rangefrom(ibase->state->mfields[k], dfrom, NULL);
                        if(min_set_num == -1 || n < min_set_num)
                        {min_set_fid = k;min_set_num = n;drangefrom = dfrom;query_range=range_flag;}
                    }
                    else if((range_flag & IB_RANGE_TO))
                    {
                        n = dmap_rangeto(ibase->state->mfields[k], dto, NULL);
                        if(min_set_num == -1 || n < min_set_num)
                        {min_set_fid = k;min_set_num = n;drangeto = dto;query_range=range_flag;}
                    }
                    else
                    {
                        n = dmap_range(ibase->state->mfields[k], dfrom, dto, NULL);
                        if(min_set_num == -1 || n < min_set_num)
                        {min_set_fid = k;min_set_num = n;drangefrom = dfrom;drangeto = dto;}
                    }
                }
            }
        }
        if(min_set_num > 0)
        {
            docs_size = (min_set_num + 100000) * sizeof(uint32_t);
            docs = (uint32_t *)db_new_data(PDB(ibase->index), docs_size);
            if(min_set_fid >= IB_INT_OFF && min_set_fid < IB_INT_TO 
                    && ibase->state->mfields[min_set_fid])
            {
                if(query_range == IB_RANGE_FROM)
                    ndocs = imap_rangefrom(ibase->state->mfields[min_set_fid], irangefrom, docs);
                else if(query_range == IB_RANGE_TO)
                    ndocs = imap_rangeto(ibase->state->mfields[min_set_fid], irangeto, docs);
                else if(query_range == IB_RANGE_IN)
                    ndocs = imap_ins(ibase->state->mfields[min_set_fid], query->in_int_list, query->in_int_num, docs);
                else
                    ndocs = imap_range(ibase->state->mfields[min_set_fid], irangefrom, irangeto, docs);
            }
            else if(min_set_fid >= IB_LONG_OFF && min_set_fid < IB_LONG_TO
                    && ibase->state->mfields[min_set_fid])
            {
                if(query_range == IB_RANGE_FROM)
                    ndocs = lmap_rangefrom(ibase->state->mfields[min_set_fid], lrangefrom, docs);
                else if(query_range == IB_RANGE_TO)
                    ndocs = lmap_rangeto(ibase->state->mfields[min_set_fid], lrangeto, docs);
                else if(query_range == IB_RANGE_IN)
                    ndocs = lmap_ins(ibase->state->mfields[min_set_fid], query->in_long_list, query->in_long_num, docs);
                else
                    ndocs = lmap_range(ibase->state->mfields[min_set_fid], lrangefrom, lrangeto, docs);
            }
            else if(min_set_fid >= IB_DOUBLE_OFF && min_set_fid < IB_DOUBLE_TO
                    && ibase->state->mfields[min_set_fid])
            {
                if(query_range == IB_RANGE_FROM)
                    ndocs = dmap_rangefrom(ibase->state->mfields[min_set_fid], drangefrom, docs);
                else if(query_range == IB_RANGE_TO)
                    ndocs = dmap_rangeto(ibase->state->mfields[min_set_fid], drangeto, docs);
                else if(query_range == IB_RANGE_IN)
                    ndocs = dmap_ins(ibase->state->mfields[min_set_fid], query->in_double_list, query->in_double_num, docs);
                else
                    ndocs = dmap_range(ibase->state->mfields[min_set_fid], drangefrom, drangeto, docs);
            }
        }
        TIMER_SAMPLE(timer);
        res->io_time = (int)PT_LU_USEC(timer);
        ACCESS_LOGGER(ibase->logger, "reading range index data qid:%d terms:%d vqterms:%d querys:%d bytes:%d time used :%lld", query->qid, query->nqterms, query->nvqterms, 0, total, PT_LU_USEC(timer));
        res->total = 0;
        while(off < ndocs)
        {
            docid = docs[off];
            doc_score = 0.0;
            base_score = 0.0;
            score = 0.0;
            if(headers[docid].status < 0 || headers[docid].globalid == 0) goto next;
            /* check fobidden terms in query string */
            if((query->flag & IB_QUERY_FORBIDDEN) && headers[docid].status<IB_SECURITY_OK)goto next;
            /* secure level */
            if((k = headers[docid].slevel) < 0 || headers[docid].slevel > IB_SLEVEL_MAX || query->slevel_filter[k]  == 1) goto next;
            /* catetory block filter */
            if(query->catblock_filter && (query->catblock_filter & headers[docid].category)) 
            {
                goto next;
            }
            /* multicat filter */
            if(query->multicat_filter != 0 && (query->multicat_filter & headers[docid].category) != query->multicat_filter)
                goto next;
            ACCESS_LOGGER(ibase->logger, "docid:%d/%lld nquerys:%d/%d int[%d/%d] catgroup:%d", docid, LL(headers[docid].globalid), query->nqterms, query->int_range_count, query->int_bits_count, query->catgroup_filter);
            /* in filter */
            if(query->in_int_fieldid > 0 && query->in_int_num > 0)
            {
                imax = query->in_int_num - 1;imin = 0;
                if((jj = query->in_int_fieldid) >= int_index_from && jj < int_index_to
                    && (jj += (IB_INT_OFF - int_index_from)) > 0 && ibase->state->mfields[jj])
                {
                    if(jj == min_set_fid) goto next;
                    xint = IMAP_GET(ibase->state->mfields[jj], docid);
                    if(xint < query->in_int_list[imin] || xint > query->in_int_list[imax]) goto next;
                    if(xint != query->in_int_list[imin] && xint != query->in_int_list[imax])
                    {
                        while(imax > imin)
                        {
                            ii = (imax + imin) / 2; 
                            if(ii == imin)break;
                            if(xint == query->in_int_list[ii]) break;
                            else if(xint > query->in_int_list[ii]) imin = ii;
                            else imax = ii;
                        }
                        if(xint != query->in_int_list[ii]) goto next;
                    }
                }
            }
            if(query->in_long_fieldid > 0 && query->in_long_num > 0)
            {
                imax = query->in_long_num - 1;imin = 0;
                if((jj = query->in_long_fieldid) >= long_index_from && jj < long_index_to
                    && (jj += (IB_LONG_OFF - long_index_from)) > 0 && ibase->state->mfields[jj])
                {
                    if(jj == min_set_fid) goto next;
                    xlong = LMAP_GET(ibase->state->mfields[jj], docid);
                    if(xlong < query->in_long_list[imin] || xlong > query->in_long_list[imax]) goto next;
                    if(xlong != query->in_long_list[imin] && xlong != query->in_long_list[imax])
                    {
                        while(imax > imin)
                        {
                            ii = (imax + imin) / 2; 
                            if(ii == imin)break;
                            if(xlong == query->in_long_list[ii]) break;
                            else if(xlong > query->in_long_list[ii]) imin = ii;
                            else imax = ii;
                        }
                        if(xlong != query->in_long_list[ii]) goto next;
                    }
                }
            }
            if(query->in_double_fieldid > 0 && query->in_double_num > 0)
            {
                imax = query->in_double_num - 1;imin = 0;
                if((jj = query->in_double_fieldid) >= double_index_from && jj < double_index_to
                    && (jj += (IB_DOUBLE_OFF - double_index_from)) > 0 && ibase->state->mfields[jj])
                {
                    if(jj == min_set_fid) goto next;
                    xdouble = DMAP_GET(ibase->state->mfields[jj], docid);
                    if(xdouble < query->in_double_list[imin] || xdouble > query->in_double_list[imax]) goto next;
                    if(xdouble != query->in_double_list[imin] && xdouble != query->in_double_list[imax])
                    {
                        while(imax > imin)
                        {
                            ii = (imax + imin) / 2; 
                            if(ii == imin)break;
                            if(xdouble == query->in_double_list[ii]) break;
                            else if(xdouble > query->in_double_list[ii]) imin = ii;
                            else imax = ii;
                        }
                        if(xdouble != query->in_double_list[ii]) goto next;
                    }
                }
            }
            /* long range  filter */
            if((query->int_range_count > 0 || query->int_bits_count > 0))
            {
                for(i = 0; i < query->int_range_count; i++)
                {
                    if((k = query->int_range_list[i].field_id) >= int_index_from 
                            && k < int_index_to && (range_flag = query->int_range_list[i].flag))
                    {
                        ifrom = query->int_range_list[i].from;
                        ito = query->int_range_list[i].to;
                        k -= int_index_from;
                        k += IB_INT_OFF;
                        if(k == min_set_fid) goto next;
                        xint = IMAP_GET(ibase->state->mfields[k], docid);
                        if((range_flag & IB_RANGE_FROM) && xint < ifrom) goto next;
                        if((range_flag & IB_RANGE_TO) && xint > ito) goto next;
                        //ACCESS_LOGGER(ibase->logger, "from:%d to:%d k:%d",  ifrom, ito, intidx[k]);
                    }
                }
                for(i = 0; i < query->int_bits_count; i++)
                {
                    if((k = query->int_bits_list[i].field_id) >= int_index_from 
                            && k < int_index_to && query->int_bits_list[i].bits != 0) 
                    {
                        k -= int_index_from;
                        k += IB_INT_OFF;
                        if(k == min_set_fid) goto next;
                        xint = IMAP_GET(ibase->state->mfields[k], docid);
                        if((query->int_bits_list[i].flag & IB_BITFIELDS_FILTER))
                        {
                            if((query->int_bits_list[i].bits & xint) == 0) goto next;
                        }
                        else
                        {
                            if((query->int_bits_list[i].bits & xint)) goto next;
                        }
                    }
                }
            }
            /* long range  filter */
            if((query->long_range_count > 0 || query->long_bits_count > 0))
            {
                for(i = 0; i < query->long_range_count; i++)
                {
                    if((k = query->long_range_list[i].field_id) >= long_index_from 
                            && k < long_index_to && (range_flag = query->long_range_list[i].flag))
                    {
                        lfrom = query->long_range_list[i].from;
                        lto = query->long_range_list[i].to;
                        k -= long_index_from;
                        k += IB_LONG_OFF;
                        if(k == min_set_fid) goto next;
                        xlong = LMAP_GET(ibase->state->mfields[k], docid);
                        //k += ibase->state->long_index_fields_num * docid;
                        if((range_flag & IB_RANGE_FROM) && xlong < lfrom) goto next;
                        if((range_flag & IB_RANGE_TO) && xlong > lto) goto next;
                    }
                }
                for(i = 0; i < query->long_bits_count; i++)
                {
                    if((k = query->long_bits_list[i].field_id) >= long_index_from 
                            && k < long_index_to && query->long_bits_list[i].bits != 0)
                    {
                        k -= long_index_from;
                        k += IB_LONG_OFF;
                        if(k == min_set_fid) goto next;
                        xlong = LMAP_GET(ibase->state->mfields[k], docid);
                        //k += ibase->state->long_index_fields_num * docid;
                        if((query->long_bits_list[i].flag & IB_BITFIELDS_FILTER))
                        {
                            if((query->long_bits_list[i].bits & xlong) == 0) goto next;
                        }
                        else
                        {
                            if((query->long_bits_list[i].bits & xlong)) goto next;
                        }
                    }
                }
            }
            /* double range filter */
            if(query->double_range_count > 0)
            {
                for(i = 0; i < query->double_range_count; i++)
                {
                    if((k = query->double_range_list[i].field_id) >= double_index_from 
                            && k < double_index_to 
                            && (range_flag = query->double_range_list[i].flag))
                    {
                        dfrom = query->double_range_list[i].from;
                        dto = query->double_range_list[i].to;
                        k -= double_index_from;
                        k += IB_DOUBLE_OFF;
                        if(k == min_set_fid) goto next;
                        xdouble = DMAP_GET(ibase->state->mfields[k], docid);
                        //k += ibase->state->double_index_fields_num * docid;
                        if((range_flag & IB_RANGE_FROM) && xdouble < dfrom) goto next;
                        if((range_flag & IB_RANGE_TO) && xdouble > dto) goto next;
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

            ACCESS_LOGGER(ibase->logger, "catgroup docid:%d/%lld category:%lld ", docid, IBLL(headers[docid].globalid), IBLL(headers[docid].category));
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
                doc_score += IBLONG((headers[docid].rank*(double)(query->base_rank)));
            ACCESS_LOGGER(ibase->logger, "docid:%d/%lld base_score:%lld rank:%f base_rank:%lld doc_score:%lld", docid, IBLL(headers[docid].globalid), IBLL(base_score), headers[docid].rank, IBLL(query->base_rank), IBLL(doc_score));
            if(is_field_sort)
            {
                //WARN_LOGGER(ibase->logger, "docid:%d/%lld base_score:%lld rank:%f base_rank:%lld doc_score:%lld fid:%d", docid, IBLL(headers[docid].globalid), IBLL(base_score), headers[docid].rank, IBLL(query->base_rank), IBLL(doc_score), fid);
                if(is_field_sort == IB_SORT_BY_INT)
                {
                    //i = fid + ibase->state->int_index_fields_num * docid;
                    doc_score = IB_INT2LONG_SCORE(IMAP_GET(ibase->state->mfields[fid], docid)) + (int64_t)(doc_score >> 16);
                }
                else if(is_field_sort == IB_SORT_BY_LONG)
                {
                    //i = fid + ibase->state->long_index_fields_num * docid;
                    doc_score = LMAP_GET(ibase->state->mfields[fid], docid);
                }
                else if(is_field_sort == IB_SORT_BY_DOUBLE)
                {
                    //i = fid + ibase->state->double_index_fields_num * docid;
                    doc_score = IB_LONG_SCORE(DMAP_GET(ibase->state->mfields[fid], docid));
                }
            }
            /* sorting */
            if(MTREE64_TOTAL(topmap) >= query->ntop)
            {
                xdata = 0ll;
                if(is_sort_reverse)
                {
                    if(doc_score >= MTREE64_MINK(topmap))
                    {
                        mtree64_pop_min(MTR64(topmap), &old_score, &xdata);
                        if((record = (IRECORD *)((long )xdata)))
                        {
                            record->globalid    = (int64_t)docid;
                            mtree64_push(MTR64(topmap), doc_score, xdata);
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
                            mtree64_push(MTR64(topmap), doc_score, xdata);
                        }
                    }
                }
            }
            else
            {
                if(nxrecords < IB_NTOP_MAX && (record = &(xrecords[nxrecords++])))
                {
                    record->globalid    = (int64_t)docid;
                    mtree64_push(MTR64(topmap), doc_score, (int64_t)record);
                }
            }
            res->total++;
next:
            ++off;
        }
        TIMER_SAMPLE(timer);
        res->sort_time = (int)PT_LU_USEC(timer);
        ACCESS_LOGGER(ibase->logger, "bsort(%d) %d documents res:%d time used:%lld", query->qid, res->total, MTREE64_TOTAL(topmap), PT_LU_USEC(timer));
        if((res->count = MTREE64_TOTAL(topmap)) > 0)
        {
            i = 0;
            do
            {
                xdata = 0;
                doc_score = 0;
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

end:
        if(docs) db_free_data(PDB(ibase->index), (char *)docs, docs_size);
        if(res) res->doctotal = ibase->state->dtotal;
        if(topmap) ibase_push_stree(ibase, topmap);
        TIMER_CLEAN(timer);
    }
    return chunk;
}
