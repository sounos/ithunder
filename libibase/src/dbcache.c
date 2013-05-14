#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <sys/time.h>
#include "ibase.h"
#include "db.h"
#include "logger.h"
/* check and get cache */
int ibase_get_cache(IBASE *ibase, IQUERY *query, ICHUNK *chunk)
{
    int i = 0, x = 0, n = 0, ret = -1;
    TERMSTATE *termstatelist = NULL;
    char line[IB_LINE_MAX];

    if(ibase && ibase->state->cache_status != IB_CACHE_DISABLED 
            && query && query->qid > 0 && chunk && ibase->cdb)
    {
        n = sprintf(line, "q:%d", query->qid);
        if((ret = db_xread_data(PDB(ibase->cdb), line, n, (char *)chunk)) > 0)
        {
            ibase->state->nquerys_cache++;
            MUTEX_LOCK(ibase->mutex_termstate);
            if((chunk->mod_time + ibase->state->cache_life_time) < time(NULL)
                && (termstatelist = (TERMSTATE *)(ibase->termstateio.map)))
            {
                for(i = 0; i < query->nqterms; i++)
                {
                    if((x = query->qterms[i].id) > 0 && x <= ibase->state->termid
                            && chunk->qmtime[i] != termstatelist[x].mod_time)
                    {
                        ret = -1;
                        break;
                    }
                }
            }
            MUTEX_UNLOCK(ibase->mutex_termstate);
            if(ret != -1)
            {
                ibase->state->nhits_cache++;
                ACCESS_LOGGER(ibase->logger, "Hit cache[%d/%d] qid:%d count:%d total:%d io:%d sort:%d ncatgroups:%d res->qid:%d", ibase->state->nhits_cache, ibase->state->nquerys_cache, query->qid, chunk->res.count, chunk->res.total, chunk->res.io_time, chunk->res.sort_time, chunk->res.ncatgroups, chunk->res.qid);
            }
        }
    }
    return ret;
}

/* set cache */
int ibase_set_cache(IBASE *ibase, IQUERY *query, ICHUNK *chunk)
{
    int n = 0, id = -1, i = 0, x = 0;
    TERMSTATE *termstatelist = NULL;
    char line[IB_LINE_MAX];

    if(ibase && ibase->state->cache_status != IB_CACHE_DISABLED
        && ibase->cdb && query && query->qid > 0 && chunk)
    {
        MUTEX_LOCK(ibase->mutex_termstate);
        if((termstatelist = (TERMSTATE *)(ibase->termstateio.map)))
        {
            for(i = 0; i < query->nqterms; i++)
            {
                if((x = query->qterms[i].id) > 0 && x <= ibase->state->termid)
                {
                    chunk->qmtime[i] = termstatelist[x].mod_time;
                }
            }
        }
        MUTEX_UNLOCK(ibase->mutex_termstate);
        chunk->mod_time = time(NULL);
        n = sprintf(line, "q:%d", query->qid);
        id = db_xset_data(PDB(ibase->cdb), line, n, (char *)chunk, sizeof(ICHUNK));
        ACCESS_LOGGER(ibase->logger, "Update cache[%d] size:%d qid:%d count:%d total:%d io:%d sort:%d ncatgroups:%d", id, (int)sizeof(ICHUNK), query->qid, chunk->res.count, chunk->res.total, chunk->res.io_time, chunk->res.sort_time, chunk->res.ncatgroups);
    }
    return id;
}

/* clean cache */
void ibase_clean_cache(IBASE *ibase)
{
    if(ibase)
    {
        return db_destroy(ibase->cdb);
    }
    return ;
}
