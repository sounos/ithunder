#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/mman.h>
#include "xmm.h"
//#define M_PAGE_SIZE getpagesize()
#define M_PAGE_SIZE 4096
#define MMSIZE(xxx) (((xxx/M_PAGE_SIZE) + ((xxx%M_PAGE_SIZE) != 0)) * M_PAGE_SIZE)
/* new memory */
void *xmm_mnew(size_t size)
{
    void *m = NULL;
    if(size > 0)
    {
        if(size < M_PAGE_SIZE)
            m = calloc(1, size);
        else
        {
            m = mmap(NULL, MMSIZE(size), PROT_READ|PROT_WRITE, MAP_ANON|MAP_PRIVATE, -1, 0);
            if(m == NULL || m == (void *)-1) m = NULL;
            else memset(m, 0, MMSIZE(size));
        }
    }
    return m;
}

void *xmm_new(size_t size)
{
    void *m = NULL;
    if(size > 0)
    {
        if(size < M_PAGE_SIZE)
            m = calloc(1, size);
        else
        {
            m = mmap(NULL, MMSIZE(size), PROT_READ|PROT_WRITE, MAP_ANON|MAP_PRIVATE, -1, 0);
            if(m == NULL || m == (void *)-1) m = NULL;
            //else memset(m, 0, MMSIZE(size));
        }
    }
    return m;
}
/* resize */
void *xmm_mresize(void *old, size_t old_size, size_t new_size)
{
    void *m = NULL;
    if(new_size > 0 && new_size > old_size)
    {
        if(new_size < M_PAGE_SIZE)
        {
            m = calloc(1, new_size);
        }
        else
        {
            m = mmap(NULL, MMSIZE(new_size), PROT_READ|PROT_WRITE, MAP_ANON|MAP_PRIVATE, -1, 0);
            if(m == (void *)-1) m = NULL;
            else memset(m, 0, MMSIZE(new_size));
        }
        if(old)
        {
            if(old_size > 0)
            {
                if(m) memcpy(m, old, old_size);
                if(old_size < M_PAGE_SIZE) free(old);
                else 
                    munmap(old, MMSIZE(old_size));
            }
        }
    }
    return m;
}
/* resize */
void *xmm_resize(void *old, size_t old_size, size_t new_size)
{
    void *m = NULL;
    if(new_size > 0 && new_size > old_size)
    {
        if(new_size < M_PAGE_SIZE)
        {
            m = calloc(1, new_size);
        }
        else
        {
            m = mmap(NULL, MMSIZE(new_size), PROT_READ|PROT_WRITE, MAP_ANON|MAP_PRIVATE, -1, 0);
            if(m == (void *)-1) m = NULL;
            //else memset(m, 0, MMSIZE(new_size));
        }
        if(old)
        {
            if(old_size > 0)
            {
                if(m) memcpy(m, old, old_size);
                if(old_size < M_PAGE_SIZE) free(old);
                else
                    munmap(old, MMSIZE(old_size));
            }
        }
    }
    return m;
}

/* remalloc */
void *xmm_mrenew(void *old, size_t old_size, size_t new_size)
{
    xmm_free(old, old_size);
    return xmm_mnew(new_size);
}

/* remalloc */
void *xmm_renew(void *old, size_t old_size, size_t new_size)
{
    xmm_free(old, old_size);
    return xmm_new(new_size);
}

/* free memory */
void xmm_free(void *m, size_t size)
{
    if(m && size > 0)
    {
        if(size < M_PAGE_SIZE) free(m);
        else 
            munmap(m, MMSIZE(size));
    }
    return ;
}
