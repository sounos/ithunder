#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include "mmlist.h"
#include "rwlock.h"

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
                i = ftruncate(mlist->fd, sizeof(MMSTATE));
                memset(mlist->state, 0, sizeof(MMSTATE));
                mlist->size += sizeof(MMSTATE);
                for(i = 0; i < MM_SLOT_MAX; i++)
                {
                   mlist->state->slots[i].nodeid = -1;
                }
           }
           mlist->roots = mlist->state->roots;
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
       RWLOCK_INIT(&(mlist->rwlock));
    }
    return mlist;
}

/* mmlist set val */
int mmlist_vset(MMLIST *mlist, int no, int32_t val)
{
    off_t size = (off_t)((no / MM_VNODE_INC) + 1) 
            * (off_t)MM_VNODE_INC * (off_t) sizeof(VNODE);
    int ret = -1, n = 0, i = 0;

    if(mlist && mlist->state && no >= 0 && no < MM_NODES_MAX)
    {
        if(size > mlist->vsize)
        {
            n = ftruncate(mlist->vfd, size);
            //memset(((char *)mlist->vmap+mlist->vsize), 0, size - mlist->vsize);
            i = mlist->vsize / sizeof(VNODE);
            n = size / sizeof(VNODE);
            while(i < n) mlist->vmap[i++].off = -1;
            mlist->vsize = size;
        }
        //mlist->vmap[no].val = val; 
        ret = 0;
    }
    return ret;
}

/* mmlist get val */
int mmlist_vget(MMLIST *mlist, int no, int32_t *val)
{
    int ret = -1, n = 0;

    if((n = (mlist->vsize/sizeof(VNODE))) > 0 && no < n)
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
    int ret = -1, n = 0;

    if(mlist && mlist->state)
    {
        if(mlist->state->nleft > 0)
        {
            n = --(mlist->state->nleft);
            ret = mlist->state->qleft[n];
        }
        else
        {
            size = mlist->size + (off_t)sizeof(MMKV) * (off_t)MM_SLOT_NUM; 
            n = ftruncate(mlist->fd, size);
            memset(((char *)mlist->state+mlist->size), 0, (size - mlist->size));
            ret = (mlist->size - (off_t)sizeof(MMSTATE)) / (off_t)sizeof(MMKV);
            mlist->size = size;
        }
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
        }
        /* 未满的slot 直接插入 */
        if(k >= 0 && k < n && mlist->slots[k].count < MM_SLOT_NUM)
        {
            x = mlist->slots[k].count++;
            kvs = mlist->map + mlist->slots[k].nodeid;
            while(x > 0 && key < kvs[x-1].key)
            {
                kvs[x].key = kvs[x-1].key;
                kvs[x].val = kvs[x-1].val;
                mlist->vmap[(kvs[x].val)].off = (mlist->slots[k].nodeid + x);
                --x;
            }
            kvs[x].key = key;
            kvs[x].val = no;
            mlist->vmap[no].off = (mlist->slots[k].nodeid + x);
            if(key < mlist->slots[k].min) mlist->slots[k].min = key;
            if(key > mlist->slots[k].max) mlist->slots[k].max = key;
        }
        else
        {
            nodeid = mmlist_slot_new(mlist);
            /* slot已满 转移元素到新的slot */
            if(k >= 0 && k < n && mlist->slots[k].count == MM_SLOT_NUM)
            {
                kvs = mlist->map + mlist->slots[k].nodeid + MM_SLOT2_NUM;
                kv = mlist->map + nodeid;
                num = MM_SLOT2_NUM;
                x = 0;
                for(i = 0; i < MM_SLOT2_NUM; i++) 
                {
                    if(key >= kvs->key && key < kvs[i].key && num == MM_SLOT2_NUM) 
                    {
                        kv->key = key;
                        kv->val = no;
                        ++kv;
                        ++x;
                        mlist->vmap[no].off = (nodeid + x);
                        num = MM_SLOT2_NUM+1;
                    }
                    kv->key = kvs[i].key;
                    kv->val = kvs[i].val;
                    ++kv;
                    ++x;
                    mlist->vmap[(kv->val)].off = (nodeid + x);
                }
                if(num == MM_SLOT2_NUM && key >= kvs[i-1].key)
                {
                    kv->key = key;
                    kv->val = no;
                    ++kv;
                    ++x;
                    num = MM_SLOT2_NUM+1;
                    mlist->vmap[(kv->val)].off = (nodeid + x);
                }
                if(num == MM_SLOT2_NUM)
                {
                    kvs = mlist->map + mlist->slots[k].nodeid;
                    i = MM_SLOT2_NUM;
                    while(i > 0 && key < kvs[i-1].key)
                    {
                        kvs[i].key = kvs[i-1].key;
                        kvs[i].val = kvs[i-1].val;
                        mlist->vmap[(kvs[i].val)].off = (mlist->slots[k].nodeid + i);
                        --i;
                    }
                    kvs[i].key = key;
                    kvs[i].val = no;
                    mlist->vmap[no].off = (mlist->slots[k].nodeid + i);
                    mlist->slots[k].min = kvs[0].key;
                    mlist->slots[k].max = kvs[MM_SLOT2_NUM].key;
                    mlist->slots[k].count = MM_SLOT2_NUM + 1;
                }
                else
                {
                    kvs = mlist->map + mlist->slots[k].nodeid;
                    mlist->slots[k].min = kvs[0].key;
                    mlist->slots[k].max = kvs[MM_SLOT2_NUM-1].key;
                    mlist->slots[k].count = MM_SLOT2_NUM;
                }
                pos = k+1;
                k = mlist->state->count++; 
                while(k > pos)
                {
                    memcpy(&(mlist->slots[k]), &(mlist->slots[k-1]), sizeof(MMSLOT));
                    x = (mlist->slots[k].nodeid / MM_SLOT_NUM);
                    mlist->roots[x] = k;
                    --k;
                }
            }
            else
            {
                /* 创建新的slot */
                kv = mlist->map + nodeid;
                kv->key = key;
                kv->val = no;
                mlist->vmap[no].off = nodeid;
                num = 1;
                k = mlist->state->count++; 
                while(k > 0 && key <= mlist->slots[k].min)
                {
                    memcpy(&(mlist->slots[k]), &(mlist->slots[k-1]), sizeof(MMSLOT));
                    x = (mlist->slots[k].nodeid / MM_SLOT_NUM);
                    mlist->roots[x] = k;
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

int mmlist_remove(MMLIST *mlist, int32_t no)
{
    uint32_t nodeid = 0, rootid = 0, slotid = 0;
    int ret = -1, i = 0, x = 0, n = 0;
    VNODE *vmap = NULL;
    MMKV *kvs = NULL;

    if(mlist && mlist->state && mlist->vmap 
            && (n = mlist->vsize/sizeof(VNODE)) > 0 && no >= 0 && no < n
            && (nodeid = mlist->vmap[no].off) >= 0)
    {
        rootid = (nodeid / MM_SLOT_NUM);
        slotid = mlist->roots[rootid];
        i = nodeid % MM_SLOT_NUM;
        kvs = mlist->map + mlist->slots[slotid].nodeid;    
        while(i < mlist->slots[slotid].count)
        {
            memcpy(&(kvs[i]), &(kvs[i+1]), sizeof(MMKV));
            x = kvs[i].val;
            mlist->vmap[x].off = i;
            ++i;
        }
        if(mlist->slots[slotid].count == 1)
        {
            i = slotid;
            mlist->state->qleft[(mlist->state->nleft++)] = nodeid;
            mlist->roots[rootid] = -1;
            while(i < mlist->state->count)
            {
                memcpy(&(mlist->slots[i]), &(mlist->slots[i+1]), sizeof(MMSLOT));
                rootid = (mlist->slots[i].nodeid / MM_SLOT_NUM);
                mlist->roots[rootid] = i;
                ++i;
            }
            --mlist->state->count;
        }
        else
        {
            n = --(mlist->slots[slotid].count);
            mlist->slots[slotid].min = kvs[0].key;
            mlist->slots[slotid].max = kvs[n - 1].key;
        }
    }
    return ret;
}

int mmlist_find_slot(MMLIST *mlist, int32_t key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    if(mlist && (n = mlist->state->count) > 0)
    {
        max = n - 1;
        min = 0;
        if(key <= mlist->slots[min].max) ret = min;
        else if(key >= mlist->slots[max].min) ret = max;
        else
        {
            while(max > min)
            {
                x = (min + max) / 2;
                if(x == min)
                {
                    ret = x;
                    break;
                }
                if(key >=  mlist->slots[x].min && key <= mlist->slots[x+1].min)
                {
                    ret = x;
                    break;
                }
                else if(key > mlist->slots[x].max) min = x;
                else max = x;
            }
        }
        if((x = ret)>= 0 && x < n)
        {
            while(x >= 0 && key <= mlist->slots[x].max)
            {
                ret = x--;
            }
        }
    }
    return ret;
}

int mmlist_find_slot2(MMLIST *mlist, int32_t key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    if(mlist && (n = mlist->state->count) > 0)
    {
        max = n - 1;
        min = 0;
        if(key <= mlist->slots[min].max) ret = min;
        else if(key >= mlist->slots[max].min) ret = max;
        else
        {
            while(max > min)
            {
                x = (min + max) / 2;
                if(x == min)
                {
                    ret = x;
                    break;
                }
                if(key >=  mlist->slots[x].min && key <= mlist->slots[x+1].min)
                {
                    ret = x;
                    break;
                }
                else if(key > mlist->slots[x].max) min = x;
                else max = x;
            }
        }
        if((x = ret)>= 0 && x < n)
        {
            while(x < n && key >= mlist->slots[x].min) 
            {
                ret = x++;
            }
        }
    }
    return ret;
}

int mmlist_find_kv(MMLIST *mlist, int k, int32_t key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    MMKV *kvs = NULL;

    if(mlist && k >= 0 && k < mlist->state->count 
            && (n = mlist->slots[k].count) > 0)
    {
        kvs = mlist->map + mlist->slots[k].nodeid;
        min = 0;
        max = n - 1; 
        if(key <= kvs[min].key) ret = min;
        else
        {
            while(max > min)
            {
                x = (min + max) / 2;
                if(x == min)
                {
                    ret = x;
                    break;
                }
                if(key ==  kvs[x].key)
                {
                    ret = x;
                    break;
                }
                else if(key > kvs[x].key) min = x;
                else max = x;
            }
            if((x = ret) >= 0 && x < n)
            {
                while(x >= 0 && key == kvs[x].key)
                {
                    ret = x--;
                }
            }

        }
        //fprintf(stdout, "find_kv(%d) min:%d max:%d count:%d ret:%d\n", key, mlist->slots[k].min, mlist->slots[k].max, mlist->slots[k].count, ret);
    }
    return ret;
}

int mmlist_find_kv2(MMLIST *mlist, int k, int32_t key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    MMKV *kvs = NULL;

    if(mlist && k >= 0 && k < mlist->state->count 
            && (n = mlist->slots[k].count) > 0)
    {
        kvs = mlist->map + mlist->slots[k].nodeid;
        min = 0;
        max = n - 1; 
        if(key <= kvs[max].key) ret = max;
        else
        {
            while(max > min)
            {
                x = (min + max) / 2;
                if(x == min)
                {
                    ret = x;
                    break;
                }
                if(key ==  kvs[x].key)
                {
                    ret = x;
                    break;
                }
                else if(key > kvs[x].key) min = x;
                else max = x;
            }
            if((x = ret) >= 0 && x < n)
            {
                while(x < n && key == kvs[x].key)
                {
                    ret = x++;
                }
            }
        }
    }
    return ret;
}

int mmlist_in(MMLIST *mlist, int32_t key, int32_t *list)
{
    int i = 0, k = 0, z = 0, ret = 0, n = 0, ii = 0;
    MMKV *kvs = NULL;

    if(mlist && mlist->state && (n = mlist->state->count) > 0)
    {
        k = mmlist_find_slot(mlist, key);
        i = mmlist_find_kv(mlist, k, key);
        ii = mmlist_find_kv2(mlist, k, key);
        do
        {
            kvs = mlist->map + mlist->slots[k].nodeid;
            if(key == kvs[i].key && i < mlist->slots[k].count)
            {
                if(key == mlist->slots[k].max)
                {
                    ret += mlist->slots[k].count - i;
                    if(list)
                    {
                        while(i < mlist->slots[k].count) list[z++] = kvs[i++].key;
                    }
                }
                else
                {
                    while(i < mlist->slots[k].count && key == kvs[i].key)
                    {
                        if(list)list[z++] = kvs[i].key;
                        ++ret;
                        ++i;
                    }
                }
            }
            i=0;
        }while(++k < n && mlist->slots[k].min == key);
    }
    return ret;
}

int mmlist_range(MMLIST *mlist, int32_t from, int32_t to, int32_t *list)
{
    int i = 0, ii = 0, k = 0, kk = 0, j = 0, x = 0, z = 0, ret = 0, n = 0;
    MMKV *kvs = NULL;

    if(mlist && mlist->state)
    {
        k = mmlist_find_slot(mlist, from);
        kk = mmlist_find_slot2(mlist, to);
        i = mmlist_find_kv(mlist, k, from);
        ii = mmlist_find_kv2(mlist, kk, to);
        if(k == kk)
        {
            ret = ii + 1 - i;
            if(list)
            {
                kvs = mlist->map + mlist->slots[k].nodeid;
                for(x = i; x <= ii; x++) list[z++] = kvs[x].key;
            }
        }
        else
        {
            n =  mlist->slots[k].count;
            ret = n - i;
            if(list)
            {
                kvs = mlist->map + mlist->slots[k].nodeid;
                for(x = i; x < n; x++) list[z++] = kvs[x].key;
            }
            for(j = i+1; j < kk; j++)
            {
                ret += mlist->slots[j].count;
                if(list)
                {
                    kvs = mlist->map + mlist->slots[j].nodeid;
                    for(x = 0; x < mlist->slots[j].count; x++) list[z++] = kvs[x].key;
                }
            }
            ret += ii;
            if(list)
            {
                kvs = mlist->map + mlist->slots[kk].nodeid;
                for(x = 0; x < ii; x++) list[z++] = kvs[x].key;
            }
        }
    }
    return ret;
}

int mmlist_rangefrom(MMLIST *mlist, int32_t key, int32_t *list) /* key = from */
{
    int i = 0, k = 0, x = 0, z = 0, ret = 0, n = 0;
    MMKV *kvs = NULL;

    if(mlist && mlist->state)
    {
        if((k = mmlist_find_slot(mlist, key)) >= 0 && (i = mmlist_find_kv(mlist, k, key)) >= 0)
        {
            kvs = mlist->map + mlist->slots[k].nodeid;
            n =  mlist->slots[k].count;
            if(list)
            {
                for(x = i; x < n; x++) list[z++] = kvs[x].key;
            }
            ret = n - i;
            for(i = k + 1; i <  mlist->state->count; i++)
            {
                ret += mlist->slots[i].count;
                if(list)
                {
                    kvs = mlist->map + mlist->slots[i].nodeid;
                    n = mlist->slots[i].count;
                    for(x = 0; x < n; x++) list[z++] = kvs[x].key;
                }
            }
        }
    }
    //fprintf(stdout, "%s::%d k:%d ret:%d/%d\n", __FILE__, __LINE__, k, ret, z);
    return ret;
}

int mmlist_rangeto(MMLIST *mlist, int32_t key, int32_t *list) /* key = to */
{
    int i = 0, k = 0, x = 0, j = 0, z = 0, ret = 0, n = 0;
    MMKV *kvs = NULL;

    if(mlist && mlist->state && (n = (mlist->state->count)) > 0)
    {
        if((k = mmlist_find_slot2(mlist, key)) >= 0 && k < n 
                && (i = mmlist_find_kv2(mlist, k, key)) >= 0)
        {
            for(j = 0; j < k; j++)
            {
                ret += mlist->slots[j].count;
                if(list)
                {
                    kvs = mlist->map + mlist->slots[j].nodeid;
                    for(x = 0; x < mlist->slots[j].count; x++)
                    {
                        list[z++] = kvs[x].key;
                    }
                }
            }
            ret += i + 1;
            if(list)
            {
                kvs = mlist->map + mlist->slots[k].nodeid;
                for(x = 0; x <= i; x++)
                {
                    list[z++] = kvs[x].key;
                }
            }
        }
    }
    //fprintf(stdout, "%s::%d k:%d ret:%d/%d\n", __FILE__, __LINE__, k, ret, z);
    return ret;
}

int mmlist_ins(MMLIST *mlist, int32_t *keys, int nkeys, int32_t *list)
{
    int ret = 0, i = 0, n = 0;
    int32_t *plist = list;

    if(mlist && mlist->state && keys && nkeys > 0)
    {
       for(i = 0; i < nkeys; i++) 
       {
            n = mmlist_in(mlist, keys[i], plist);
            if(list) plist += n;
            ret += n;
       }
    }
    return ret;
}

int mmlist_get(MMLIST *mlist, int no, int32_t *key)
{
    int ret = -1, n = 0;

    if(mlist)
    {
        RWLOCK_RDLOCK(&(mlist->rwlock));
        if((n = (mlist->vsize/sizeof(VNODE))) > 0 && no < n)
        {
            if(key) *key = mlist->vmap[no].val;
        }
        RWLOCK_UNLOCK(&(mlist->rwlock));
    }
    return ret;
}

int mmlist_set(MMLIST *mlist, int no, int32_t key)
{
    int ret = -1;

    if(mlist)
    {
       RWLOCK_WRLOCK(&(mlist->rwlock));
       mmlist_vset(mlist, no, key);
       if(mlist->vmap[no].off  < 0)
       {
           mmlist_insert(mlist, no, key);
       }
       else
       {
           if(key != mlist->vmap[no].val)
           {
                mmlist_remove(mlist, no);
                mmlist_insert(mlist, no, key);
           }
       }
       mlist->vmap[no].val = key;
       RWLOCK_UNLOCK(&(mlist->rwlock));
    }
    return ret;
}

int mmlist_del(MMLIST *mlist, int no)
{
    int ret = -1, n = 0;

    if(mlist)
    {
        RWLOCK_WRLOCK(&(mlist->rwlock));
        if((n = (mlist->vsize/sizeof(VNODE))) > 0 && no < n)
        {
            mmlist_remove(mlist, no);
            mlist->vmap[no].off = -1;
        }
        RWLOCK_UNLOCK(&(mlist->rwlock));
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
        RWLOCK_DESTROY(&(mlist->rwlock));
        free(mlist);
    }
    return ;
}

#ifdef MMLIST_TEST
#include "timer.h"
#define MASK  120000
//gcc -o imap mmlist.c -DMMLIST_TEST -DTEST_INS -DHAVE_PTHREAD -lpthread && ./imap
int main()
{
    MMLIST *mlist = NULL;
    int i = 0, j = 0, n = 0, total = 0, stat[MASK], stat2[MASK];
    int32_t val = 0, from = 0, to = 0, *res = NULL;
    int32_t inputs[256], last[256];
    int64_t all = 0;
    time_t stime = 0, etime = 0;
    void *timer = NULL;

    if((mlist = mmlist_init("/tmp/1.idx")))
    {
        res = (int32_t *)calloc(60000000, sizeof(int32_t));
        TIMER_INIT(timer);
#ifdef TEST_INS
        //fprintf(stdout, "sizeof(stat):%d\n", sizeof(stat));
        memset(stat, 0, sizeof(stat));
        memset(stat2, 0, sizeof(stat2));
        srand(time(NULL));
        n = 256;
        for(i = 0; i < n; i++)
        {
            inputs[i] = rand()%MASK;
        }
        TIMER_RESET(timer);
        for(i = 1; i < 40000000; i++)
        {
           j = (rand()%n);
           val = inputs[j];
           stat[val]++;
           mmlist_set(mlist, i, val);
           last[j] = i;
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "set() 40000000 data, time used:%lld\n", PT_LU_USEC(timer));
        TIMER_RESET(timer);
        for(i = 0; i < n; i++)
        {
            mmlist_del(mlist, last[i]);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "del() time used:%lld\n", PT_LU_USEC(timer));
        TIMER_RESET(timer);
        for(i = 0; i < n; i++)
        {
            val = inputs[i];
            stat2[val] = mmlist_in(mlist, val, res);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "in() time used:%lld\n", PT_LU_USEC(timer));
        TIMER_RESET(timer);
        total = mmlist_ins(mlist, inputs, n, NULL);
        TIMER_SAMPLE(timer);
        fprintf(stdout, "ins(keys, NULL) total:%d time used:%lld\n", total, PT_LU_USEC(timer));
        TIMER_RESET(timer);
        total = mmlist_ins(mlist, inputs, n, res);
        TIMER_SAMPLE(timer);
        fprintf(stdout, "ins(keys, res:%p) total:%d time used:%lld\n", res, total, PT_LU_USEC(timer));
        for(i = 0; i < n; i++)
        {
            j = inputs[i];
            if(stat[j] != stat2[j])
                fprintf(stdout, "%d:%d/%d\n", j, stat[j], stat2[j]);
        }
#ifdef OUT_ALL
        for(i = 0; i < total; i++)
        {
            fprintf(stdout, "%d:%d\n", i, res[i]);
        }
#endif
#endif
        /*
        for(i = 0; i < mlist->state->count; i++)
        {
            fprintf(stdout, "%d:{min:%d max:%d}(%d)\n", i, mlist->slots[i].min, mlist->slots[i].max, mlist->slots[i].count);
        }
        */
#ifdef TEST_RANGE
        srand(time(NULL));
        TIMER_RESET(timer);
        for(i = 1; i < 40000000; i++)
        {
            val = 1356969600 + (rand()%31536000);
            mmlist_set(mlist, i, val);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "set() 40000000 timestamps,  time used:%lld\n", PT_LU_USEC(timer));
        srand(time(NULL));
        TIMER_RESET(timer);
        all = 0;
        for(i = 0; i < 1000; i++)
        {
            val = 1356969600 + (rand()%31536000);
            all += mmlist_rangefrom(mlist, val, res);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "rangefrom() 1000 times total:%lld, time used:%lld\n", (long long int)all, PT_LU_USEC(timer));
        srand(time(NULL));
        TIMER_RESET(timer);
        all = 0;
        for(i = 0; i < 1000; i++)
        {
            val = 1356969600 + (rand()%31536000);
            all += mmlist_rangeto(mlist, val, res);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "rangeto() 1000 times total:%lld, time used:%lld\n", (long long int)all, PT_LU_USEC(timer));
        srand(time(NULL));
        TIMER_RESET(timer);
        all = 0;
        for(i = 0; i < 1000; i++)
        {
            from = 1356969600 + (rand()%31536000);
            to = from + rand()%31536000;
            all += mmlist_range(mlist, from, to, res);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "range(%p) 1000 times total:%lld, time used:%lld\n", res, (long long int)all, PT_LU_USEC(timer));
        srand(time(NULL));
        TIMER_RESET(timer);
        all = 0;
        for(i = 0; i < 1000; i++)
        {
            from = 1356969600 + (rand()%31536000);
            to = from + rand()%31536000;
            all += mmlist_range(mlist, from, to, NULL);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "range(null) 1000 times total:%lld, time used:%lld\n", (long long int)all, PT_LU_USEC(timer));

#endif
        mmlist_close(mlist);
        TIMER_CLEAN(timer);
        free(res);
    }
}
#endif
