#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <errno.h>
#include "mutex.h"
#include "mmqueue.h"
#include "xmm.h"
MMQUEUE *mmqueue_init(char *qfile)
{
    struct stat st = {0};
    MMQUEUE *mmq = NULL;

    if(qfile && (mmq = (MMQUEUE *)xmm_mnew(sizeof(MMQUEUE))))
    {
        MUTEX_INIT(mmq->mutex);
        if((mmq->fd = open(qfile, O_CREAT|O_RDWR, 0644)) > 0
            && fstat(mmq->fd, &st) == 0)
        {
            if(st.st_size == 0) 
            {
                mmq->end = sizeof(MMQSTATE);
                if(ftruncate(mmq->fd, mmq->end) != 0) _exit(-1);
            }
            else 
                mmq->end = st.st_size;
            mmq->size = sizeof(MMQNODE) * MMQ_NODE_MAX + sizeof(MMQSTATE);
            if(mmq->size < st.st_size) mmq->size = st.st_size;
            if((mmq->map = mmap(NULL, mmq->size, PROT_READ|PROT_WRITE, MAP_SHARED,
                        mmq->fd, 0)) == NULL || mmq->map == (void *)-1)
            {
                fprintf(stderr, "mmap qfile:%s failed, %s\n", qfile, strerror(errno));
                _exit(-1);
            }
            mmq->state = (MMQSTATE *)mmq->map;
            if(st.st_size < sizeof(MMQSTATE)) memset(mmq->state, 0, sizeof(MMQSTATE));
            mmq->nodes = (MMQNODE *)((char *)mmq->map + sizeof(MMQSTATE));
        }
        else
        {
            fprintf(stderr, "open qfile:%s failed, %s\n", qfile, strerror(errno));
            _exit(-1);
        }
    }
    return mmq;
}

/* to qleft */
int mmqueue_incre(MMQUEUE *mmq)
{
    int x = 0;

    if(mmq && mmq->fd > 0)
    {
        if(mmq->end > 0 && mmq->end == mmq->size)
        {
            if(mmq->map)
            {
                munmap(mmq->map, mmq->end);
                mmq->map = NULL;
                mmq->state = NULL;
                mmq->nodes = NULL;
            }
            mmq->size += sizeof(MMQNODE) * MMQ_INCRE_NUM; 
            if((mmq->map = mmap(NULL, mmq->size, PROT_READ|PROT_WRITE, MAP_SHARED,
                            mmq->fd, 0)) == NULL || mmq->map == (void *)-1)
            {
                fprintf(stderr, "mmap qfile:%d failed, %s\n", mmq->fd, strerror(errno));
                _exit(-1);
            }
            mmq->state = (MMQSTATE *)mmq->map;
            mmq->nodes = (MMQNODE *)((char *)mmq->map + sizeof(MMQSTATE));
        }
        //fprintf(stdout, "%s::%d end:%lld qleft:%d qtotal:%d\n", __FILE__, __LINE__, (long long int)mmq->end, mmq->state->qleft, mmq->state->qtotal);
        x = (mmq->end - sizeof(MMQSTATE)) / sizeof(MMQNODE); 
        mmq->end += sizeof(MMQNODE) * MMQ_INCRE_NUM;
        if(ftruncate(mmq->fd, mmq->end) != 0)
        {
            fprintf(stderr, "truncate qfile failed, %s\n", strerror(errno));
            _exit(-1);
        }
        mmq->state->qtotal += MMQ_INCRE_NUM;
        if(x == 0) ++x;
        while(x < mmq->state->qtotal)
        {
            mmq->nodes[x].data = 0;
            mmq->nodes[x].next = mmq->state->qleft;
            mmq->state->qleft = x;
            ++x;
        }
    }
    return 0;
}

/* new queue */
int mmqueue_new(MMQUEUE *mmq)
{
    int rootid = -1, i = 0;

    if(mmq)
    {
        MUTEX_LOCK(mmq->mutex);
        if(mmq->state && mmq->map && mmq->state->nroots < MMQ_ROOT_MAX)
        {
            i = 1;
            while(mmq->state->roots[i].status && i < MMQ_ROOT_MAX) ++i;
            if(i < MMQ_ROOT_MAX && mmq->state->roots[i].status == 0)
            {
                mmq->state->roots[i].status = 1;
                mmq->state->nroots++;
                mmq->state->roots[i].total = 0;
                mmq->state->roots[i].first = mmq->state->roots[i].last = 0;
                rootid = i;
            }
        }
        MUTEX_UNLOCK(mmq->mutex);
    }
    return rootid;
}

/* total */
int mmqueue_total(MMQUEUE *mmq, int rootid)
{
    int ret = 0;

    if(mmq)
    {
        if(mmq->state && mmq->map && rootid < MMQ_ROOT_MAX)
        {
            ret = mmq->state->roots[rootid].total;
        }
    }
    return ret;
}

/* close queue */
int mmqueue_close(MMQUEUE *mmq, int rootid)
{
    int i = 0;

    if(mmq)
    {
        MUTEX_LOCK(mmq->mutex);
        if(mmq->state && mmq->map && rootid > 0 && rootid < MMQ_ROOT_MAX)
        {
            if((i = mmq->state->roots[rootid].last) > 0)
            {
                mmq->nodes[i].next = mmq->state->qleft;
                mmq->state->qleft = mmq->state->roots[rootid].first;
            }
            memset(&(mmq->state->roots[rootid]), 0, sizeof(MMQROOT));
            mmq->state->nroots--;
        }
        MUTEX_UNLOCK(mmq->mutex);
    }
    return rootid;
}

/* push */
int mmqueue_push(MMQUEUE *mmq, int rootid, int data)
{
    int id = -1, x = 0;

    if(mmq && rootid > 0 && rootid < MMQ_ROOT_MAX)
    {
        MUTEX_LOCK(mmq->mutex);
        if(mmq->state && mmq->state->roots[rootid].status > 0)
        {
            if(mmq->state->qleft == 0) mmqueue_incre(mmq);
            if((id = mmq->state->qleft) > 0 && mmq->nodes) 
            {
                mmq->state->qleft = mmq->nodes[id].next;
                if(mmq->state->roots[rootid].total == 0)  
                {
                    mmq->state->roots[rootid].first = mmq->state->roots[rootid].last = id;
                }
                else
                {
                    x = mmq->state->roots[rootid].last;
                    mmq->nodes[x].next = id;
                    mmq->state->roots[rootid].last = id;
                }
                mmq->state->roots[rootid].total++;
                mmq->nodes[id].next = 0;
                mmq->nodes[id].data = data;
            }
        }
        MUTEX_UNLOCK(mmq->mutex);
    }
    return id;
}
/* head */
int mmqueue_head(MMQUEUE *mmq, int rootid, int *data)
{
    int id = -1;

    if(mmq && rootid > 0 && rootid < MMQ_ROOT_MAX)
    {
        MUTEX_LOCK(mmq->mutex);
        if(mmq->state && mmq->nodes && mmq->state->roots[rootid].status > 0 
                && mmq->state->roots[rootid].total > 0 
                && (id = mmq->state->roots[rootid].first) > 0)
        {
            if(data) *data = mmq->nodes[id].data;    
        }
        MUTEX_UNLOCK(mmq->mutex);
    }
    return id;
}

/* pop */
int mmqueue_pop(MMQUEUE *mmq, int rootid, int *data)
{
    int id = -1;

    if(mmq && rootid > 0 && rootid < MMQ_ROOT_MAX)
    {
        MUTEX_LOCK(mmq->mutex);
        if(mmq->state && mmq->nodes && mmq->state->roots[rootid].status > 0 
                && mmq->state->roots[rootid].total > 0 
                && (id = mmq->state->roots[rootid].first) > 0)
        {
            if(data) *data = mmq->nodes[id].data;    
            mmq->state->roots[rootid].first = mmq->nodes[id].next;
            mmq->state->roots[rootid].total--;
            if(mmq->state->roots[rootid].total == 0)
                mmq->state->roots[rootid].last = 0;
            mmq->nodes[id].next = mmq->state->qleft;
            mmq->state->qleft = id;
        }
        MUTEX_UNLOCK(mmq->mutex);
    }
    return id;
}

/* clean */
void mmqueue_clean(MMQUEUE *mmq)
{
    if(mmq)
    {
        MUTEX_DESTROY(mmq->mutex);
        if(mmq->map) munmap(mmq->map, mmq->end);
        if(mmq->fd) close(mmq->fd);
        xmm_free(mmq, sizeof(MMQUEUE));
    }
    return ;
}

#ifdef _DEBUG_MMQUEUE
int main()
{
    int qroots[64], i = 0, j = 0, id = 0,k = 0;
    MMQUEUE *mmq = NULL;

    if((mmq = mmqueue_init("/tmp/mmq")))
    {
        //push
        for(i = 0; i < 64; i++)
        {
            if((qroots[i] = mmqueue_new(mmq)))
            {
                mmqueue_push(mmq, qroots[i], i);
                for(j = 0; j < 500000; j++)
                {
                    mmqueue_push(mmq, qroots[i], j);
                }
            }
        }
        //pop
        for(i = 0; i < 64; i++)
        {
            if((id = mmqueue_head(mmq, qroots[i], &k)) > 0)
            {
                fprintf(stdout, "%d:", k);
                j = 0;
                while(mmqueue_pop(mmq, qroots[i], &k) > 0)
                {
                    fprintf(stdout, "%d|", k);
                    ++j;
                }
                fprintf(stdout, "[%d]\r\n", j);
            }
        }
        mmqueue_clean(mmq);
    }
}
//gcc -o mmq mmqueue.c xmm.c -D_DEBUG_MMQUEUE && ./mmq
#endif
