#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include "mmlist.h"
MMLIST *mmlist_init(char *file)
{
    MMLIST *mlist = NULL;
    char path[1024];
    struct stat st = {0};
    off_t size = 0;
    int n = 0, i = 0;

    if(file && (mlist = (MMLIST *)calloc(1, sizeof(MMLIST))))
    {
       if((mlist->fd = open(file, O_CREAT|O_RDWR, 0644)) > 0) 
       {
           size = mlist->msize = (off_t)sizeof(MMSTATE) + (off_t)sizeof(MMKV) * (off_t)MM_NODES_MAX;
           mlist->state = (MMSTATE*)mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, mlist->fd, 0);
           mlist->map = (MMKV *)((char *)mlist->state + sizeof(MMSTATE));
           fstat(mlist->fd, &st);
           mlist->size = st.st_size;
           if(st.st_size < sizeof(MMSTATE))
           {
               ftruncate(mlist->fd, sizeof(MMSTATE));
               memset(mlist->state, 0, sizeof(MMSTATE));
               mlist->size += sizeof(MMSTATE);
               for(i = 0; i < MM_SLOT_MAX; i++)
               {
                   mlist->state->slots[i].nodeid = -1;
               }
           }
           mlist->slots = mlist->state->slots;
           //fprintf(stdout, "size:%lld/%d\n", mlist->size, sizeof(MMSTATE));
       }
       else
       {
           fprintf(stderr, "open %s failed, %s\n", file, strerror(errno));
       }
       n = sprintf(path, "%s.v", file);
       if((mlist->vfd = open(path, O_CREAT|O_RDWR, 0644)) > 0) 
       {
            size = mlist->vmsize = (off_t)sizeof(VNODE) *  (off_t)MM_NODES_MAX;
            mlist->vmap = (VNODE *)mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, mlist->vfd, 0);
            fstat(mlist->vfd, &st);
            mlist->vsize = st.st_size;
            //fprintf(stdout, "vsize:%lld\n", mlist->vsize);
       }
       else
       {
           fprintf(stderr, "open %s failed, %s\n", file, strerror(errno));
       }

    }
    return mlist;
}

/* mmlist set val */
int mmlist_vset(MMLIST *mlist, int no, int32_t val)
{
    off_t size = (off_t)((no / MM_VNODE_INC) + ((no%MM_VNODE_INC) != 0)) 
            * (off_t)MM_VNODE_INC * (off_t) sizeof(VNODE);
    int ret = -1;

    if(mlist && mlist->state && no > 0 && no < MM_NODES_MAX)
    {
        if(size > mlist->vsize)
        {
            ftruncate(mlist->vfd, size);
            memset(((char *)mlist->vmap+mlist->vsize), 0, size - mlist->vsize);
            mlist->vsize = size;
        }
        mlist->vmap[no].val = val; 
        ret = 0;
    }
    return ret;
}

/* mmlist get val */
int mmlist_vget(MMLIST *mlist, int no, int32_t *val)
{
    int ret = -1;

    if(mlist && mlist->state && no > 0 && no < mlist->vsize)
    {
        if(val) *val = mlist->vmap[no].val; 
        ret = 0;
    }
    return ret;
}

/* new bolt  */
int mmlist_slot_new(MMLIST *mlist)
{
    off_t size = 0;
    int ret = -1;

    if(mlist && mlist->state)
    {
        size = mlist->size + (off_t)sizeof(MMKV) * (off_t)MM_SLOT_NUM; 
        ftruncate(mlist->fd, size);
        memset(((char *)mlist->state+mlist->size), 0, (size - mlist->size));
        ret = (mlist->size - (off_t)sizeof(MMSTATE)) / (off_t)sizeof(MMKV);
        mlist->size = size;
    }
    return ret;
}

int mmlist_insert(MMLIST *mlist, int no, int32_t key)
{
    int ret = -1, i = 0, k = -1, nodeid = 0, pos = 0, num = 0, 
        n = 0, x = 0, min = 0, max = 0;
    MMKV *kvs = NULL, *kv = NULL;
    VNODE *vnodes = NULL;

    if(mlist && mlist->state && (vnodes = mlist->vmap))
    {
        if((n = mlist->state->count) > 0)
        {
            max = n - 1;
            min = 0;
            if(key <= mlist->slots[min].max) k = min;
            else if(key >= mlist->slots[max].min) k = max;
            else
            {
                while(max > min)
                {
                    x = (min + max) / 2;
                    if(x == min)
                    {
                        //fprintf(stdout, "%s::%d x:%d min:%d max:%d key:%d count:%d\n", __FILE__, __LINE__, x, mlist->slots[x].min, mlist->slots[x].max, key, n);
                        k = x;
                        break;
                    }
                    if(key >=  mlist->slots[x].min && key <= mlist->slots[x+1].min)
                    {
                        //fprintf(stdout, "%s::%d x:%d min:%d max:%d num:%d key:%d total:%d\n", __FILE__, __LINE__, x, mlist->slots[x].min, mlist->slots[x].max, mlist->slots[x].count, key, n);
                        k = x;
                        break;
                    }
                    else if(key > mlist->slots[x].max) min = x;
                    else max = x;
                }
            }
        }
        /* 未满的slot 直接插入 */
        if(k >= 0 && k < n && mlist->slots[k].count < MM_SLOT_NUM)
        {
            x = mlist->slots[k].count++;
            //fprintf(stdout, "%s::%d k:%d key:%d min:%d max:%d count:%d\n", __FILE__, __LINE__, k, key, mlist->slots[k].min, mlist->slots[k].max, x);
            kvs = mlist->map + mlist->slots[k].nodeid;
            while(x > 0 && key < kvs[x-1].key)
            {
                kvs[x].key = kvs[x-1].key;
                kvs[x].val = kvs[x-1].val;
                --x;
            }
            kvs[x].key = key;
            kvs[x].val = no;
            if(key < mlist->slots[k].min) mlist->slots[k].min = key;
            if(key > mlist->slots[k].max) mlist->slots[k].max = key;
        }
        else
        {
            //fprintf(stdout, "%s::%d k:%d key:%d\n", __FILE__, __LINE__, k, key);
            nodeid = mmlist_slot_new(mlist);
            /* slot已满 转移元素到新的slot */
            if(k >= 0 && k < n && mlist->slots[k].count == MM_SLOT_NUM)
                    //&& key >= mlist->slots[k].min && key < mlist->slots[k].max)
            {
                //fprintf(stdout, "%s::%d k:%d no:%d count:%d min:%d max:%d key:%d OK\n", __FILE__, __LINE__, k, no, mlist->slots[k].count, mlist->slots[k].min, mlist->slots[k].max, key);
                kvs = mlist->map + mlist->slots[k].nodeid + MM_SLOT2_NUM;
                kv = mlist->map + nodeid;
                num = MM_SLOT2_NUM;
                for(i = 0; i < MM_SLOT2_NUM; i++) 
                {
                    if(key >= kvs->key && key < kvs[i].key && num == MM_SLOT2_NUM) 
                    {
                        //fprintf(stdout, "%s::%d k:%d no:%d count:%d min:%d max:%d key:%d OK\n", __FILE__, __LINE__, k, no, mlist->slots[k].count, mlist->slots[k].min, mlist->slots[k].max, key);
                        kv->key = key;
                        kv->val = no;
                        kv++;
                        num = MM_SLOT2_NUM+1;
                    }
                    kv->key = kvs[i].key;
                    kv->val = kvs[i].val;
                    kv++;
                }
                if(num == MM_SLOT2_NUM && key >= kvs[i-1].key)
                {
                    //fprintf(stdout, "%s::%d k:%d no:%d count:%d min:%d max:%d key:%d OK\n", __FILE__, __LINE__, k, no, mlist->slots[k].count, mlist->slots[k].min, mlist->slots[k].max, key);
                    kv->key = key;
                    kv->val = no;
                    kv++;
                    num = MM_SLOT2_NUM+1;
                }
                if(num == MM_SLOT2_NUM)
                {
                    kvs = mlist->map + mlist->slots[k].nodeid;
                    i = MM_SLOT2_NUM;
                    while(i > 0 && key < kvs[i-1].key)
                    {
                        kvs[i].key = kvs[i-1].key;
                        kvs[i].val = kvs[i-1].val;
                        --i;
                    }
                    kvs[i].key = key;
                    kvs[i].val = no;
                    mlist->slots[k].min = kvs[0].key;
                    mlist->slots[k].max = kvs[MM_SLOT2_NUM].key;
                    mlist->slots[k].count = MM_SLOT2_NUM + 1;
                    //fprintf(stdout, "%s::%d k:%d no:%d count:%d min:%d max:%d key:%d OK\n", __FILE__, __LINE__, k, no, mlist->slots[k].count, mlist->slots[k].min, mlist->slots[k].max, key);
                    //pos = k;
                    //fprintf(stdout, "%s:%d no:%d new-slot:%d k:%d count:%d OK\n", __FILE__, __LINE__, no, nodeid, k, mlist->slots[k].count);
                }
                else
                {
                    //pos = k+1;
                    kvs = mlist->map + mlist->slots[k].nodeid;
                    mlist->slots[k].min = kvs[0].key;
                    mlist->slots[k].max = kvs[MM_SLOT2_NUM-1].key;
                    mlist->slots[k].count = MM_SLOT2_NUM;
                    //fprintf(stdout, "%s:%d k:%d no:%d count:%d min:%d max:%d key:%d OK\n", __FILE__, __LINE__, k, no, mlist->slots[k].count, mlist->slots[k].min, mlist->slots[k].max, key);
                }
                pos = k+1;
                k = mlist->state->count++; 
                while(k > pos)
                {
                    mlist->slots[k].min = mlist->slots[k-1].min;
                    mlist->slots[k].max = mlist->slots[k-1].max;
                    mlist->slots[k].nodeid = mlist->slots[k-1].nodeid;
                    mlist->slots[k].count = mlist->slots[k-1].count;
                    --k;
                }
                //fprintf(stdout, "%s:%d k:%d no:%d count:%d min:%d max:%d key:%d OK\n", __FILE__, __LINE__, k, no, mlist->slots[k].count, mlist->slots[k].min, mlist->slots[k].max, key);
                //fprintf(stdout, "%s:%d no:%d new-slot:%d k:%d count:%d OK\n", __FILE__, __LINE__, no, nodeid, k, mlist->slots[k].count);
            }
            else
            {
                /* 创建新的slot */
                kv = mlist->map + nodeid;
                kv->key = key;
                kv->val = no;
                num = 1;
                k = mlist->state->count++; 
                while(key <= mlist->slots[k].min)
                {
                    mlist->slots[k].min = mlist->slots[k-1].min;
                    mlist->slots[k].max = mlist->slots[k-1].max;
                    mlist->slots[k].nodeid = mlist->slots[k-1].nodeid;
                    mlist->slots[k].count = mlist->slots[k-1].count;
                    --k;
                }
            }
            kv = mlist->map + nodeid;
            mlist->slots[k].min = kv[0].key;
            mlist->slots[k].max = kv[num-1].key;
            mlist->slots[k].nodeid = nodeid;
            mlist->slots[k].count = num;
            //fprintf(stdout, "%s::%d k:%d min:%d max:%d num:%d/%d\n", __FILE__, __LINE__, k, mlist->slots[k].min, mlist->slots[k].max, num, mlist->state->count);
        }
    }
    return ret;
}

int mmlist_remove(MMLIST *mlist, u_int32_t nodeid)
{
    MMKV *kv = NULL;

    if(mlist && mlist->state && mlist->map)
    {
        kv = mlist->map + nodeid;
    }
}

int mmlist_update(MMLIST *mlist, int no, int32_t nodeid);
int mmlist_range(MMLIST *mlist, int32_t from, int32_t to);
int mmlist_range1(MMLIST *mlist, int32_t key) /* from */
{
    int n = 0, min = 0, max = 0, k = 0, x = 0, ret = -1;

    if(mlist && (n = mlist->state->count) > 0)
    {
        max = n - 1;
        min = 0;
        if(key <= mlist->slots[min].max) k = min;
        else if(key >= mlist->slots[max].min) k = max;
        else
        {
            while(max > min)
            {
                x = (min + max) / 2;
                if(x == min)
                {
                    k = x;
                    break;
                }
                if(key >=  mlist->slots[x].min && key <= mlist->slots[x+1].min)
                {
                    k = x;
                    break;
                }
                else if(key > mlist->slots[x].max) min = x;
                else max = x;
            }
        }
        fprintf(stdout, "%s::%d k:%d\n", __FILE__, __LINE__, k);
    }
    return ret;
}
int mmlist_range2(MMLIST *mlist, int32_t key) /* to */
{
    int n = 0, min = 0, max = 0, k = 0, x = 0, ret = -1;

    if(mlist && (n = mlist->state->count) > 0)
    {
        max = n - 1;
        min = 0;
        if(key <= mlist->slots[min].max) k = min;
        else if(key >= mlist->slots[max].min) k = max;
        else
        {
            while(max > min)
            {
                x = (min + max) / 2;
                if(x == min)
                {
                    k = x;
                    break;
                }
                if(key >=  mlist->slots[x].min && key <= mlist->slots[x+1].min)
                {
                    k = x;
                    break;
                }
                else if(key > mlist->slots[x].max) min = x;
                else max = x;
            }
        }
        if(k > 0 && key >= mlist->slots[k].min && key <= mlist->slots[k].max)
        {
        }
        fprintf(stdout, "%s::%d k:%d\n", __FILE__, __LINE__, k);
    }
    return ret;
}
void mmlist_close(MMLIST *mlist)
{
    off_t size = 0;
    if(mlist)
    {
        if(mlist->state) munmap(mlist->state, mlist->msize);
        if(mlist->fd) close(mlist->fd);
        if(mlist->vmap) munmap(mlist->vmap, mlist->vmsize);
        if(mlist->vfd) close(mlist->vfd);
        free(mlist);
    }
    return ;
}

#ifdef MMLIST_TEST
//gcc -o imap mmlist.c -DMMLIST_TEST && ./imap
int main()
{
    MMLIST *mlist = NULL;
    int i = 0;
    int32_t val = 0;

    if((mlist = mmlist_init("/tmp/1.idx")))
    {
        /*
        for(i = 0; i < 10000000; i++)
            mmlist_vset(mlist, i, random());
        */
        srand(time(NULL));
        for(i = 0; i < 100000000; i++)
        {
            val = rand();
            mmlist_insert(mlist, i, val);
            //fprintf(stdout, "%s::%d val(%d:%d)\n", __FILE__, __LINE__, i, val);
        }
        for(i = 0; i < mlist->state->count; i++)
        {
            fprintf(stdout, "%d:{min:%d max:%d}(%d)\n", i, mlist->slots[i].min, mlist->slots[i].max, mlist->slots[i].count);
        }
        mmlist_close(mlist);
    }
}
#endif
