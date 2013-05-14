#include "mutex.h"
#include "xmm.h"
#include "iqueue.h"
void *iqueue_init()
{
    QUEUE *q = NULL;

    if((q = (QUEUE *)xmm_mnew(sizeof(QUEUE))))
    {
        MUTEX_INIT(q->mutex);
    }
    return q;
}

void iqueue_push(void *queue, int data)
{
    QNODE *node = NULL, *tmp = NULL;
    QUEUE *q = (QUEUE *)queue;
    int i = 0;

    if(q)
    {
        MUTEX_LOCK(q->mutex);
        if((node = q->left))
        {
            q->left = node->next;
        }
        else 
        {
            if((i = q->nlist) < QNODE_LINE_MAX 
                && (node = (QNODE *)xmm_mnew(QNODE_LINE_NUM * sizeof(QNODE))))
            {
                q->list[i] = node;
                q->nlist++;
                i = 1;
                while(i  < QNODE_LINE_NUM)
                {
                    tmp = &(node[i]); 
                    tmp->next = q->left;
                    q->left = tmp;
                    ++i;
                }
            }
        }
        if(node)
        {
            node->data = data;
            if(q->last)
            {
                q->last->next = node;
                q->last = node;
            }
            else
            {
                q->first = q->last = node;
            }
            node->next = NULL;
            q->total++;
        }
        MUTEX_UNLOCK(q->mutex);
    }
    return ;
}

void iqueue_head(void *queue, int *data)
{
    QUEUE *q = (QUEUE *)queue;
    QNODE *node = NULL;

    if(q)
    {
        if((node = q->first))
        {
            *data = node->data;
        }
    }
    return ;
}

void iqueue_pop(void *queue, int *data)
{
    QUEUE *q = (QUEUE *)queue;
    QNODE *node = NULL;

    if(q)
    {
        MUTEX_LOCK(q->mutex);
        if((node = q->first))
        {
            *data = node->data;
            if((q->first = q->first->next) == NULL)
            {
                q->last = NULL;
            }
            node->next = q->left;
            q->left = node;
            --(q->total);
        }
        MUTEX_UNLOCK(q->mutex);
    } 
    return ;
}

void iqueue_clean(void *queue)
{
    QUEUE *q = (QUEUE *)queue;
    int i = 0;

    if(q)
    {
        for(i = 0; i < q->nlist; i++);
        {
            if(q->list[i])xmm_free(q->list[i], QNODE_LINE_NUM * sizeof(QNODE));
        }
        MUTEX_DESTROY(q->mutex);
        xmm_free(q, sizeof(QUEUE));
    }
    return ;
}
