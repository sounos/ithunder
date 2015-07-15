#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "ibase.h"
#include "db.h"
#include "mdb.h"
#include "timer.h"
#include "zvbcode.h"
#include "logger.h"
#include "xmm.h"
#include "mtree64.h"
#include "imap.h"
#include "lmap.h"
#include "dmap.h"
#include "immx.h"
#include "bmap.h"
 
/* binary list merging */
ICHUNK *ibase_query(IBASE *ibase, IQUERY *query, int secid)
{
    int i = 0, n = 0, k = 0,min_set_num = -1, docid = 0, ifrom = -1, is_sort_reverse = 0, 
        min_set_fid = 0, int_index_from = 0, int_index_to = 0, ito = -1, *vint = NULL, 
        double_index_from = 0, double_index_to = 0, range_flag = 0, is_groupby = 0,
        fid = 0, nxrecords = 0, is_field_sort = 0, ignore_rank = 0, gid = 0, j = 0, 
        long_index_from = 0, long_index_to = 0, ii = 0, jj = 0, imax = 0, imin = 0, 
        xint = 0, off = 0, irangefrom = 0, irangeto = 0, query_range = 0, m = 0, min_set_no = 0;
    uint32_t *docs = NULL, docs_size = 0, ndocs = 0;
    double dfrom = 0.0, dto = 0.0, drangefrom = 0.0, drangeto = 0.0,xdouble = 0.0,*vdouble = NULL;
    int64_t bits = 0, lfrom = 0, lto = 0, base_score = 0, lrangefrom = 0, lrangeto = 0, 
            doc_score = 0, old_score = 0, xdata = 0, xlong = 0, *vlong = NULL;
    void *timer = NULL, *topmap = NULL, *groupby = NULL;
    IRECORD *record = NULL, *records = NULL, xrecords[IB_NTOP_MAX];
    IHEADER *headers = NULL; ICHUNK *chunk = NULL;
    IRES *res = NULL;

    if(ibase && query && secid >= 0 && secid < IB_SEC_MAX 
            && ibase->index && ibase->mindex[secid])
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
        headers = (IHEADER *)(ibase->state->headers[secid].map);
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
        fid = query->orderby;
        if(fid >= int_index_from && fid < int_index_to)
        {
            fid -= int_index_from;
            fid += IB_INT_OFF;
            if(ibase->state->mfields[secid][fid]) is_field_sort = IB_SORT_BY_INT;
        }
        else if(fid >= long_index_from && fid < long_index_to)
        {
            fid -= long_index_from;
            fid += IB_LONG_OFF;
            if(ibase->state->mfields[secid][fid]) is_field_sort = IB_SORT_BY_LONG;
        }
        else if(fid >= double_index_from && fid < double_index_to)
        {
            fid -= double_index_from;
            fid += IB_DOUBLE_OFF;
            if(ibase->state->mfields[secid][fid]) is_field_sort = IB_SORT_BY_DOUBLE;
        }
        gid = query->groupby;
        if(gid >= int_index_from && gid < int_index_to)
        {
            gid -= int_index_from;
            gid += IB_INT_OFF;
            if(ibase->state->mfields[secid][gid]) is_groupby  = IB_GROUPBY_INT;
        }
        else if(gid >= long_index_from && gid < long_index_to)
        {
            gid -= long_index_from;
            gid += IB_LONG_OFF;
            if(ibase->state->mfields[secid][gid]) is_groupby  = IB_GROUPBY_LONG;
        }
        else if(gid >= double_index_from && gid < double_index_to)
        {
            gid -= double_index_from;
            gid += IB_DOUBLE_OFF;
            if(ibase->state->mfields[secid][gid]) is_groupby  = IB_GROUPBY_DOUBLE;
        }
        if(gid > 0){groupby = ibase_pop_mmx(ibase);};
        TIMER_INIT(timer);
        min_set_fid = 0;
        for(j = 0; j <  query->int_in_num; j++)
        {
            if((k = query->int_in_list[j].fieldid) > 0 
                    && (m = query->int_in_list[j].num) > 0
                    && (vint = query->int_in_list[j].vals))
            {
                k += IB_INT_OFF - int_index_from ;
                n = imap_ins(ibase->state->mfields[secid][k], vint, m, NULL);
                if(min_set_num == -1 || n < min_set_num)
                {min_set_fid = k;min_set_num = n;min_set_no=j;query_range = IB_RANGE_IN;}
            }
        }
        for(j = 0; j <  query->long_in_num; j++)
        {
            if((k = query->long_in_list[j].fieldid) > 0 
                    && (m = query->long_in_list[j].num) > 0
                    && (vlong = query->long_in_list[j].vals))
            {
                k += IB_LONG_OFF - long_index_from ;
                n = lmap_ins(ibase->state->mfields[secid][k], vlong, m, NULL);
                if(min_set_num == -1 || n < min_set_num)
                {min_set_fid = k;min_set_num = n;min_set_no=j;query_range = IB_RANGE_IN;}
            }
        }
        for(j = 0; j <  query->double_in_num; j++)
        {
            if((k = query->double_in_list[j].fieldid) > 0 
                    && (m = query->double_in_list[j].num) > 0
                    && (vdouble = query->double_in_list[j].vals))
            {
                k += IB_DOUBLE_OFF - double_index_from ;
                n = dmap_ins(ibase->state->mfields[secid][k], vdouble, m, NULL);
                if(min_set_num == -1 || n < min_set_num)
                {min_set_fid = k;min_set_num = n;min_set_no = j;query_range = IB_RANGE_IN;}
            }
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
                    if(!ibase->state->mfields[secid][k]) continue;
                    if((range_flag & (IB_RANGE_FROM|IB_RANGE_TO)) == (IB_RANGE_FROM|IB_RANGE_TO))
                    {
                        n = imap_range(ibase->state->mfields[secid][k], ifrom, ito, NULL);
                        if(min_set_num == -1 || n < min_set_num)
                        {min_set_fid=k;min_set_num = n;min_set_no=i;irangefrom=ifrom;irangeto=ito;query_range=0;}
                    }
                    else if((range_flag & IB_RANGE_TO))
                    {
                        n = imap_rangeto(ibase->state->mfields[secid][k],ito, NULL);
                        if(min_set_num == -1 || n < min_set_num)
                        {min_set_fid = k;min_set_num = n;min_set_no=i;irangeto=ito;query_range=IB_RANGE_TO;}
                    }
                    else
                    {
                        n = imap_rangefrom(ibase->state->mfields[secid][k],ifrom, NULL);
                        if(min_set_num == -1 || n < min_set_num)
                        {min_set_fid = k;min_set_num=n;min_set_no=i;irangefrom=ifrom;query_range=IB_RANGE_FROM;}
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
                    if(!ibase->state->mfields[secid][k]) continue;
                    if((range_flag & (IB_RANGE_FROM|IB_RANGE_TO)) == (IB_RANGE_FROM|IB_RANGE_TO))
                    {
                        n = lmap_range(ibase->state->mfields[secid][k], lfrom, lto, NULL);
                        if(min_set_num == -1 || n < min_set_num)
                        {min_set_fid=k;min_set_num = n;min_set_no=i;lrangefrom=lfrom;lrangeto=lto;query_range=0;}
                    }
                    else if((range_flag & IB_RANGE_TO))
                    {
                        n = lmap_rangeto(ibase->state->mfields[secid][k],lto, NULL);
                        if(min_set_num == -1 || n < min_set_num)
                        {min_set_fid = k;min_set_num = n;min_set_no=i;lrangeto=lto;query_range=IB_RANGE_TO;}
                    }
                    else
                    {
                        n = lmap_rangefrom(ibase->state->mfields[secid][k],lfrom, NULL);
                        if(min_set_num == -1 || n < min_set_num)
                        {min_set_fid = k;min_set_num=n;min_set_no=i;lrangefrom=lfrom;query_range=IB_RANGE_FROM;}
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
                    if(!ibase->state->mfields[secid][k]) continue;
                    if((range_flag & (IB_RANGE_FROM|IB_RANGE_TO)) == (IB_RANGE_FROM|IB_RANGE_TO))
                    {
                        n = dmap_range(ibase->state->mfields[secid][k], dfrom, dto, NULL);
                        if(min_set_num == -1 || n < min_set_num)
                        {min_set_fid=k;min_set_num = n;min_set_no=i;drangefrom=dfrom;drangeto=dto;query_range=0;}
                    }
                    else if((range_flag & IB_RANGE_TO))
                    {
                        n = dmap_rangeto(ibase->state->mfields[secid][k], dto, NULL);
                        if(min_set_num == -1 || n < min_set_num)
                        {min_set_fid = k;min_set_num = n;min_set_no=i;drangeto = dto;query_range=IB_RANGE_TO;}
                    }
                    else
                    {
                        n = dmap_rangefrom(ibase->state->mfields[secid][k], dfrom, NULL);
                        if(min_set_num == -1 || n < min_set_num)
                        {min_set_fid = k;min_set_num=n;min_set_no=i;drangefrom=dfrom;query_range=IB_RANGE_FROM;}
                    }
                }
            }
        }
        if(min_set_num >= 0)
        {
            docs_size = (min_set_num + 100000) * sizeof(uint32_t);
            docs = (uint32_t *)mdb_new_data(PMDB(ibase->index), docs_size);
            if(min_set_fid >= IB_INT_OFF && min_set_fid < IB_INT_TO 
                    && ibase->state->mfields[secid][min_set_fid])
            {
                if(query_range == IB_RANGE_FROM)
                    ndocs = imap_rangefrom(ibase->state->mfields[secid][min_set_fid], irangefrom, docs);
                else if(query_range == IB_RANGE_TO)
                    ndocs = imap_rangeto(ibase->state->mfields[secid][min_set_fid], irangeto, docs);
                else if(query_range == IB_RANGE_IN)
                    ndocs = imap_ins(ibase->state->mfields[secid][min_set_fid], query->int_in_list[min_set_no].vals, query->int_in_list[min_set_no].num, docs);
                else
                    ndocs = imap_range(ibase->state->mfields[secid][min_set_fid], irangefrom, irangeto, docs);
            }
            else if(min_set_fid >= IB_LONG_OFF && min_set_fid < IB_LONG_TO
                    && ibase->state->mfields[secid][min_set_fid])
            {
                if(query_range == IB_RANGE_FROM)
                    ndocs = lmap_rangefrom(ibase->state->mfields[secid][min_set_fid], lrangefrom, docs);
                else if(query_range == IB_RANGE_TO)
                    ndocs = lmap_rangeto(ibase->state->mfields[secid][min_set_fid], lrangeto, docs);
                else if(query_range == IB_RANGE_IN)
                    ndocs = lmap_ins(ibase->state->mfields[secid][min_set_fid], query->long_in_list[min_set_no].vals, query->long_in_list[min_set_no].num, docs);
                else
                    ndocs = lmap_range(ibase->state->mfields[secid][min_set_fid], lrangefrom, lrangeto, docs);
            }
            else if(min_set_fid >= IB_DOUBLE_OFF && min_set_fid < IB_DOUBLE_TO
                    && ibase->state->mfields[secid][min_set_fid])
            {
                if(query_range == IB_RANGE_FROM)
                    ndocs = dmap_rangefrom(ibase->state->mfields[secid][min_set_fid], drangefrom, docs);
                else if(query_range == IB_RANGE_TO)
                    ndocs = dmap_rangeto(ibase->state->mfields[secid][min_set_fid], drangeto, docs);
                else if(query_range == IB_RANGE_IN)
                    ndocs = dmap_ins(ibase->state->mfields[secid][min_set_fid], query->double_in_list[min_set_no].vals, query->double_in_list[min_set_no].num, docs);
                else
                    ndocs = dmap_range(ibase->state->mfields[secid][min_set_fid], drangefrom, drangeto, docs);
            }
        }
        TIMER_SAMPLE(timer);
        res->io_time = (int)PT_LU_USEC(timer);
        DEBUG_LOGGER(ibase->logger, "reading range fields[%d](%d){%d} data:%p ndata:%d qid:%d time used :%lld", min_set_fid, min_set_num, min_set_fid%32, docs, ndocs, query->qid, PT_LU_USEC(timer));
        res->total = 0;
        while(docs && off < ndocs)
        {
            docid = docs[off];
            doc_score = 0.0;
            base_score = 0.0;
#ifdef IB_USE_BMAP
            if(!(query->flag & IB_QUERY_IGNSTATUS))
            {
                if(query->flag & IB_QUERY_BMAP)
                {
                    if(bmap_check(ibase->bmaps[secid], docid) == 0) goto next;
                }
                else
                {
                    if(headers[docid].status < 0 || headers[docid].globalid == 0) goto next;
                }
            }
#else
            if(headers[docid].status < 0 || headers[docid].globalid == 0) goto next;
#endif
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
            //ACCESS_LOGGER(ibase->logger, "docid:%d/%lld nquerys:%d/%d int[%d/%d] catgroup:%d", docid, LL(headers[docid].globalid), query->nqterms, query->int_range_count, query->int_bits_count, query->catgroup_filter);
            /* ins filter */
            for(j = 0; j < query->int_in_num; j++)
            {
                if((jj = query->int_in_list[j].fieldid) > 0 && jj != min_set_fid
                        && (imax = (query->int_in_list[j].num - 1)) >= 0
                        && (jj += (IB_INT_OFF - int_index_from)) > 0 
                        && ibase->state->mfields[secid][jj]
                        && (vint = query->int_in_list[j].vals))
                {
                    imin = 0;
                    xint = IMAP_GET(ibase->state->mfields[secid][jj], docid);
                    if(xint < vint[imin] || xint > vint[imax]) goto next;
                    if(xint != vint[imin] && xint != vint[imax])
                    {
                        while(imax > imin)
                        {
                            ii = (imax + imin) / 2; 
                            if(ii == imin)break;
                            if(xint == vint[ii]) break;
                            else if(xint > vint[ii]) imin = ii;
                            else imax = ii;
                        }
                        if(xint != vint[ii]) goto next;
                    }
                }
            }
            for(j = 0; j < query->long_in_num; j++)
            {
                if((jj = query->long_in_list[j].fieldid) > 0 && jj != min_set_fid
                        && (imax = (query->long_in_list[j].num - 1)) >= 0
                        && (jj += (IB_LONG_OFF - long_index_from)) > 0 
                        && ibase->state->mfields[secid][jj]
                        && (vlong = query->long_in_list[j].vals))
                {
                    imin = 0;
                    xlong = LMAP_GET(ibase->state->mfields[secid][jj], docid);
                    if(xlong < vlong[imin] || xlong > vlong[imax]) goto next;
                    if(xlong != vlong[imin] && xlong != vlong[imax])
                    {
                        while(imax > imin)
                        {
                            ii = (imax + imin) / 2; 
                            if(ii == imin)break;
                            if(xlong == vlong[ii]) break;
                            else if(xlong > vlong[ii]) imin = ii;
                            else imax = ii;
                        }
                        if(xlong != vlong[ii]) goto next;
                    }
                }
            }
            for(j = 0; j < query->double_in_num; j++)
            {
                if((jj = query->double_in_list[j].fieldid) > 0 && jj != min_set_fid
                        && (imax = (query->double_in_list[j].num - 1)) >= 0
                        && (jj += (IB_DOUBLE_OFF - double_index_from)) > 0 
                        && ibase->state->mfields[secid][jj]
                        && (vdouble = query->double_in_list[j].vals))
                {
                    imin = 0;
                    xdouble = DMAP_GET(ibase->state->mfields[secid][jj], docid);
                    if(xdouble < vdouble[imin] || xdouble > vdouble[imax]) goto next;
                    if(xdouble != vdouble[imin] && xdouble != vdouble[imax])
                    {
                        while(imax > imin)
                        {
                            ii = (imax + imin) / 2; 
                            if(ii == imin)break;
                            if(xdouble == vdouble[ii]) break;
                            else if(xdouble > vdouble[ii]) imin = ii;
                            else imax = ii;
                        }
                        if(xdouble != vdouble[ii]) goto next;
                    }
                }
            }
            /* range filter */
            if((query->int_range_count > 0 || query->int_bits_count > 0))
            {
                for(i = 0; i < query->int_range_count; i++)
                {
                    k = query->int_range_list[i].field_id;
                    range_flag = query->int_range_list[i].flag;
                    ifrom = query->int_range_list[i].from;
                    ito = query->int_range_list[i].to;
                    k -= int_index_from;
                    k += IB_INT_OFF;
                    if(k == min_set_fid) continue;
                    if(!ibase->state->mfields[secid][k]) goto next;
                    xint = IMAP_GET(ibase->state->mfields[secid][k], docid);
                    if((range_flag & IB_RANGE_FROM) && xint < ifrom) goto next;
                    if((range_flag & IB_RANGE_TO) && xint > ito) goto next;
                }
                for(i = 0; i < query->int_bits_count; i++)
                {
                    if(query->int_bits_list[i].bits == 0) continue;
                    k = query->int_bits_list[i].field_id;
                    k -= int_index_from;
                    k += IB_INT_OFF;
                    if(k == min_set_fid) continue;
                    if(!ibase->state->mfields[secid][k]) goto next;
                    xint = IMAP_GET(ibase->state->mfields[secid][k], docid);
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
            if((query->long_range_count > 0 || query->long_bits_count > 0))
            {
                for(i = 0; i < query->long_range_count; i++)
                {
                    k = query->long_range_list[i].field_id;
                    range_flag = query->long_range_list[i].flag;
                    lfrom = query->long_range_list[i].from;
                    lto = query->long_range_list[i].to;
                    k -= long_index_from;
                    k += IB_LONG_OFF;
                    if(k == min_set_fid) continue;
                    if(!ibase->state->mfields[secid][k]) goto next;
                    xlong = LMAP_GET(ibase->state->mfields[secid][k], docid);
                    if((range_flag & IB_RANGE_FROM) && xlong < lfrom) goto next;
                    if((range_flag & IB_RANGE_TO) && xlong > lto) goto next;
                }
                for(i = 0; i < query->long_bits_count; i++)
                {
                    if(query->int_bits_list[i].bits == 0) continue;
                    k = query->long_bits_list[i].field_id;
                    k -= long_index_from;
                    k += IB_LONG_OFF;
                    if(k == min_set_fid) continue;
                    if(!ibase->state->mfields[secid][k]) goto next;
                    xlong = LMAP_GET(ibase->state->mfields[secid][k], docid);
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
            if(query->double_range_count > 0)
            {
                for(i = 0; i < query->double_range_count; i++)
                {
                    k = query->double_range_list[i].field_id;
                    range_flag = query->double_range_list[i].flag;
                    dfrom = query->double_range_list[i].from;
                    dto = query->double_range_list[i].to;
                    k -= double_index_from;
                    k += IB_DOUBLE_OFF;
                    if(k == min_set_fid) continue;
                    if(!ibase->state->mfields[secid][k]) goto next;
                    xdouble = DMAP_GET(ibase->state->mfields[secid][k], docid);
                    if((range_flag & IB_RANGE_FROM) && xdouble < dfrom) goto next;
                    if((range_flag & IB_RANGE_TO) && xdouble > dto) goto next;
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

            DEBUG_LOGGER(ibase->logger, "catgroup docid:%d/%lld category:%lld ", docid, IBLL(headers[docid].globalid), IBLL(headers[docid].category));
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
            /* rank */
            doc_score = IB_LONG_SCORE(base_score);
            //ACCESS_LOGGER(ibase->logger, "docid:%d/%lld base_score:%lld score:%f doc_score:%lld", docid, IBLL(headers[docid].globalid), IBLL(base_score), score, IBLL(doc_score));
            if(ignore_rank == 0 && is_sort_reverse && (query->flag & IB_QUERY_RANK)) 
               doc_score += IBLONG((headers[docid].rank*(double)(query->base_rank)));
            //ACCESS_LOGGER(ibase->logger, "docid:%d/%lld base_score:%lld rank:%f base_rank:%lld doc_score:%lld", docid, IBLL(headers[docid].globalid), IBLL(base_score), headers[docid].rank, IBLL(query->base_rank), IBLL(doc_score));
            if(is_field_sort && min_set_fid != fid)
            {
                if(is_field_sort == IB_SORT_BY_INT)
                {
                    doc_score = IB_INT2LONG_SCORE(IMAP_GET(ibase->state->mfields[secid][fid], docid)) + (int64_t)(doc_score >> 16);
                }
                else if(is_field_sort == IB_SORT_BY_LONG)
                {
                    doc_score = LMAP_GET(ibase->state->mfields[secid][fid], docid);
                }
                else if(is_field_sort == IB_SORT_BY_DOUBLE)
                {
                    xdouble = DMAP_GET(ibase->state->mfields[secid][fid], docid);
                    doc_score = IB_LONG_SCORE(xdouble);
                }
            }
            /* group by */
            if(groupby && gid > 0)
            {
                if(is_groupby == IB_GROUPBY_INT)
                {
                    xlong = (int64_t)IMAP_GET(ibase->state->mfields[secid][gid], docid);
                }
                else if(is_groupby == IB_GROUPBY_LONG)
                {
                    xlong = LMAP_GET(ibase->state->mfields[secid][gid], docid);
                }
                else if(is_groupby == IB_GROUPBY_DOUBLE)
                {
                    xlong = IB_LONG_SCORE(DMAP_GET(ibase->state->mfields[secid][gid], docid));
                }
                IMMX_ADD(groupby, xlong);
                DEBUG_LOGGER(ibase->logger, "docid:%d/%lld gid:%d key:%lld count:%d", docid, IBLL(headers[docid].globalid), gid, IBLL(xlong), PIMX(groupby)->count);
            }
            /* sorting */
            if(min_set_fid != fid)
            {
                //ACCESS_LOGGER(ibase->logger, "docid:%d/%lld base_score:%lld rank:%f base_rank:%lld doc_score:%lld fid:%d", docid, IBLL(headers[docid].globalid), IBLL(base_score), headers[docid].rank, IBLL(query->base_rank), IBLL(doc_score), fid);
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
            }
            res->total++;
            ++off;
            continue;
next:
            docs[off++] = 0;
        }
        TIMER_SAMPLE(timer);
        res->sort_time = (int)PT_LU_USEC(timer);
        if(min_set_fid != fid)
        {
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
                        DEBUG_LOGGER(ibase->logger, "top[%d/%d] docid:%d/%lld score:%lld", i, MTREE64_TOTAL(topmap), docid, IBLL(headers[docid].globalid), IBLL(doc_score));
                    }
                    ++i;
                }while(MTREE64_TOTAL(topmap) > 0);
            }
        }
        else
        {
            if(is_sort_reverse)
            {
                i = ndocs - 1;
                k = 0;
                while(i >= 0 && k < query->ntop)
                {
                    if((docid = docs[i]))
                    {
                        doc_score = IBLONG((headers[docid].rank*(double)(query->base_rank)));
                        if(is_field_sort == IB_SORT_BY_INT)
                        {
                            doc_score = IB_INT2LONG_SCORE(IMAP_GET(ibase->state->mfields[secid][fid], docid)) + (int64_t)(doc_score >> 16);
                        }
                        else if(is_field_sort == IB_SORT_BY_LONG)
                        {
                            doc_score = LMAP_GET(ibase->state->mfields[secid][fid], docid);
                        }
                        else if(is_field_sort == IB_SORT_BY_DOUBLE)
                        {
                            xdouble = DMAP_GET(ibase->state->mfields[secid][fid], docid);
                            doc_score = IB_LONG_SCORE(xdouble);
                        }
                        records[k].score = doc_score;
                        records[k].globalid = (int64_t)headers[docid].globalid;
                        res->count++;
                        ++k;
                    }
                    --i;
                }
            }
            else
            {
                i = 0;
                k = 0;
                while(i < ndocs && k < query->ntop)
                {
                    if((docid = docs[i]))
                    {
                        doc_score = IBLONG((headers[docid].rank*(double)(query->base_rank)));
                        if(is_field_sort == IB_SORT_BY_INT)
                        {
                            doc_score = IB_INT2LONG_SCORE(IMAP_GET(ibase->state->mfields[secid][fid], docid)) + (int64_t)(doc_score >> 16);
                        }
                        else if(is_field_sort == IB_SORT_BY_LONG)
                        {
                            doc_score = LMAP_GET(ibase->state->mfields[secid][fid], docid);
                        }
                        else if(is_field_sort == IB_SORT_BY_DOUBLE)
                        {
                            xdouble = DMAP_GET(ibase->state->mfields[secid][fid], docid);
                            doc_score = IB_LONG_SCORE(xdouble);
                        }
                        records[k].score = doc_score;
                        records[k].globalid = (int64_t)headers[docid].globalid;
                        res->count++;
                        ++k;
                    }
                    ++i;
                }
            }
        }
        if(groupby && (res->ngroups = (PIMX(groupby)->count)) > 0)
        {
            i = 0;
            do
            {
                IMMX_POP_MIN(groupby, xlong, xdata);
                if(i < IB_GROUP_MAX)
                {
                    res->groups[i].key = xlong;
                    res->groups[i].val = xdata;
                }
                ++i;
            }while(PIMX(groupby)->count > 0);
            res->flag |= is_groupby;
            if(res->ngroups > IB_GROUP_MAX)
            {
                WARN_LOGGER(ibase->logger, "large groups[%d] qid:%d", res->groups, query->qid);
                res->ngroups = IB_GROUP_MAX;
            }
        }
        ACCESS_LOGGER(ibase->logger, "bsort(%d) %d/%d documents res:%d time used:%lld ioTime:%lld sortTime:%lld ncatgroups:%d ngroups:%d", query->qid, ndocs, res->total, res->count, PT_USEC_U(timer), IBLL(res->io_time), IBLL(res->sort_time),res->ncatgroups, res->ngroups);
end:
        if(docs) mdb_free_data(PMDB(ibase->index), (char *)docs, docs_size);
        if(res) res->doctotal = ibase->state->dtotal;
        if(topmap) ibase_push_stree(ibase, topmap);
        if(groupby) ibase_push_mmx(ibase, groupby);
        TIMER_CLEAN(timer);
    }
    return chunk;
}
