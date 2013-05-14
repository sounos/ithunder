#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "qchardet.h"
#include "mutex.h"
void qchardet_init(QCHARDET *q)
{
    if(q)
    {
        memset(q, 0, sizeof(QCHARDET));
        MUTEX_INIT(q->mutex);
    }
    return ;
}

chardet_t qchardet_pop(QCHARDET *q)
{
    chardet_t pdet = NULL;

    if(q)   
    {
        MUTEX_LOCK(q->mutex);
        if(q->nqpool > 0)
        {
            pdet = q->qpool[--(q->nqpool)];
            //fprintf(stdout, "%s::%d pdet:%p npool:%d\r\n", __FILE__, __LINE__, pdet, q->nqpool);
        }
        else
        {
            chardet_create(&pdet);
            //fprintf(stdout, "%s::%d pdet:%p\r\n", __FILE__, __LINE__, pdet);
        }
        MUTEX_UNLOCK(q->mutex);
    }
    return pdet;
}

void qchardet_push(QCHARDET *q, chardet_t pdet)
{
    if(q)   
    {
        MUTEX_LOCK(q->mutex);
        if(q->nqpool < Q_CHARDET_MAX)
        {
            chardet_reset(pdet);
            q->qpool[(q->nqpool)++] = pdet;
        }
        else
        {
            chardet_destroy(pdet);
        }
        MUTEX_UNLOCK(q->mutex);
    }
    return ;
}

void qchardet_close(QCHARDET *q)
{
    int i = 0;

    if(q)
    {
        MUTEX_LOCK(q->mutex);
        for(i = 0; i < q->nqpool; i++)
        {
            chardet_destroy(q->qpool[i]);
        }
        MUTEX_UNLOCK(q->mutex);
        MUTEX_DESTROY(q->mutex);
    }
    return ;
}
