#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <string.h>
#include <errno.h>
#include "rwlock.h"
#include "bmap.h"
#define USE_BMAP_BITS 1
void *bmap_init(char *file)
{
    BMAP *bmap = NULL;
    struct stat st = {0};

    if(file && (bmap = (BMAP *)calloc(1, sizeof(BMAP))))
    {
        if((bmap->fd = open(file, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(bmap->fd, &st) == 0)
        {
            bmap->bytes = st.st_size;
            bmap->id_max = bmap->bytes * 8;
            if((bmap->mbits = (char *)mmap(NULL, BMAP_ID_MAX/8, PROT_READ|PROT_WRITE, 
                    MAP_SHARED, bmap->fd, 0)) == NULL || bmap->mbits == (void *)MAP_FAILED)
            {
                fprintf(stderr, "mmap file(%s) failed, %s", file, strerror(errno));
                _exit(-1);
            }
#ifdef USE_BMAP_BITS
            if((bmap->bits = (char *)mmap(NULL, BMAP_ID_MAX/8, PROT_READ|PROT_WRITE, 
                    MAP_ANON|MAP_PRIVATE, -1, 0)) == NULL 
                    || bmap->bits == (void *)MAP_FAILED)
            //if((bmap->bits = (char *)malloc(BMAP_ID_MAX/8)) == NULL)
            {
                fprintf(stderr, "new mmap failed, %s", strerror(errno));
                _exit(-1);
            }
            if(bmap->bytes > 0)
            {
                memcpy(bmap->bits, bmap->mbits, bmap->bytes);
            }
#endif
            RWLOCK_INIT(bmap->mutex);
        }
        else
        {
            fprintf(stderr, "create file(%s) failed, %s", file, strerror(errno));
            _exit(-1);
        }
    }
    return bmap;
}

int bmap_resize(BMAP *bmap, int id)
{
    int ret = -1, bytes = 0;

    if(bmap && id >= 0 && id < BMAP_ID_MAX)
    {
       bytes = ((id / (8 *  BMAP_BASE_NUM)) + 1) * BMAP_BASE_NUM;
       ret =  ftruncate(bmap->fd, bytes);
       memset(bmap->mbits+bmap->bytes, 0, bytes - bmap->bytes);
#ifdef USE_BMAP_BITS
       memset(bmap->bits+bmap->bytes, 0, bytes - bmap->bytes);
#endif
       bmap->bytes = bytes;
       bmap->id_max = bmap->bytes * 8;
    }
    return ret;
}

int bmap_set(void *p, int id)
{
    BMAP *bmap = NULL;
    int ret = -1, no = 0, off = 0;

    if((bmap = (BMAP *)p) && id < BMAP_ID_MAX)
    {
        RWLOCK_WRLOCK(bmap->mutex);
        if(id >= bmap->id_max && (ret = bmap_resize(bmap, id)) != 0) return ret;
        no = id / 8;
        off = id % 8;
        bmap->mbits[no] |= 1 << off;
#ifdef USE_BMAP_BITS
        bmap->bits[no] |= 1 << off;
#endif
        RWLOCK_UNLOCK(bmap->mutex);
        ret = 0;
    }
    return ret;
}

int bmap_unset(void *p, int id)
{
    BMAP *bmap = NULL;
    int ret = -1, no = 0, off = 0;

    if((bmap = (BMAP *)p) && id < BMAP_ID_MAX)
    {
        RWLOCK_WRLOCK(bmap->mutex);
        if(id >= bmap->id_max && (ret = bmap_resize(bmap, id)) != 0) return ret;
        no = id / 8;
        off = id % 8;
        bmap->mbits[no] &= ~(1 << off);
#ifdef USE_BMAP_BITS
        bmap->bits[no] &= ~(1 << off);
#endif
        RWLOCK_UNLOCK(bmap->mutex);
        ret = 0;
    }
    return ret;
}

int bmap_check(void *p, int id)
{
    BMAP *bmap = NULL;

    if((bmap = (BMAP *)p) && id < bmap->id_max && bmap->bits)
    {
#ifdef USE_BMAP_BITS
        return (bmap->bits[(id/8)] & (1 << id % 8));
#endif
    }
    return 0;
}

int bmap_mcheck(void *p, int id)
{
    BMAP *bmap = NULL;

    if((bmap = (BMAP *)p) && id < bmap->id_max && bmap->mbits)
    {
        return (bmap->mbits[(id/8)] & (1 << id % 8));
    }
    return 0;
}

void bmap_clean(void *p)
{
    BMAP *bmap = NULL;

    if((bmap = (BMAP *)p))
    {

#ifdef USE_BMAP_BITS
        munmap(bmap->bits, BMAP_ID_MAX/8);
#endif
        munmap(bmap->mbits, BMAP_ID_MAX/8);
        close(bmap->fd);
        RWLOCK_DESTROY(bmap->mutex);
        free(bmap);
    }
    return ;
}

#ifdef TEST_BMAP
#define TEST_MAX   	2000000
#define TEST_ID_MAX   	200000000
#include "timer.h"
int main()
{
    int i = 0, no = 0, *list = NULL;
    char *file = "/tmp/xxx.bmap";
    void *bmap = NULL, *timer = NULL;

    if((bmap = bmap_init(file)))
    {
        for(i = 0; i < TEST_ID_MAX; i++)
        {
            bmap_set(bmap, i);
        }
        list = (int *)calloc(1, sizeof(int) * TEST_MAX);
        for(i = 0; i < TEST_MAX; i++)
        {
            list[i] = random()%TEST_ID_MAX;
        }
        TIMER_INIT(timer);
#ifdef USE_BMAP_BITS
        for(i = 0; i < TEST_MAX; i++)
        {
            no = list[i];
            if(bmap_check(bmap, no) == 0)
            {
                fprintf(stderr, "bits[%d] invalid\n", no);
            }
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "bmap_check[%d] time used:%lld\n", TEST_MAX, PT_LU_USEC(timer));
#endif
        TIMER_RESET(timer);
        for(i = 0; i < TEST_MAX; i++)
        {
            no = list[i];
            if(bmap_mcheck(bmap, no) == 0)
            {
                fprintf(stderr, "bits[%d] invalid\n", no);
            }
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "bmap_mcheck[%d] time used:%lld\n", TEST_MAX, PT_LU_USEC(timer));
#ifdef TEST_BMAP_UNSET
        for(i = 0; i < TEST_ID_MAX; i++)
        {
            if((i % 33) == 0) bmap_unset(bmap, i);
        }
        TIMER_RESET(timer);
        for(i = 0; i < TEST_MAX; i++)
        {
            no = list[i];
            if(bmap_check(bmap, no) == 0)
            {
                fprintf(stderr, "bits[%d] unseted\n", no);
            }
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "bmap_unset_check[%d] time used:%lld\n", TEST_MAX, PT_LU_USEC(timer));
#endif
        TIMER_CLEAN(timer);
        bmap_clean(bmap);
        free(list);
        sleep(10);
    }
    return 0; 
}
#endif
//rm -f /tmp/xxx.bmap ;gcc -o scheck bmap.c -O2 -DTEST_BMAP && ./scheck
//rm -f /tmp/xxx.bmap ;gcc -o scheck bmap.c -O2 -DTEST_BMAP -DTEST_BMAP_UNSET && ./scheck
