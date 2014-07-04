#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include "imap.h"
#include "rwlock.h"

IMAP *imap_init(char *file)
{
    IMAP *imap = NULL;
    char path[1024];
    struct stat st = {0};
    off_t size = 0;
    int i = 0;

    if(file && (imap = (IMAP *)calloc(1, sizeof(IMAP))))
    {
       if((imap->fd = open(file, O_CREAT|O_RDWR, 0644)) > 0) 
       {
           size = imap->msize = (off_t)sizeof(IMMSTATE) + (off_t)sizeof(IMMKV) * (off_t)IMM_NODES_MAX;
           imap->state = (IMMSTATE*)mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, imap->fd, 0);
           imap->map = (IMMKV *)((char *)imap->state + sizeof(IMMSTATE));
           fstat(imap->fd, &st);
           imap->size = st.st_size;
           if(st.st_size < sizeof(IMMSTATE))
           {
                i = ftruncate(imap->fd, sizeof(IMMSTATE));
                memset(imap->state, 0, sizeof(IMMSTATE));
                imap->size += sizeof(IMMSTATE);
                for(i = 0; i < IMM_SLOT_MAX; i++)
                {
                   imap->state->slots[i].nodeid = -1;
                }
           }
           imap->roots = imap->state->roots;
           imap->slots = imap->state->slots;
           //fprintf(stdout, "size:%lld/%d\n", imap->size, sizeof(IMMSTATE));
       }
       else
       {
           fprintf(stderr, "open %s failed, %s\n", file, strerror(errno));
       }
       sprintf(path, "%s.v", file);
       if((imap->vfd = open(path, O_CREAT|O_RDWR, 0644)) > 0) 
       {
            size = imap->vmsize = (off_t)sizeof(IMMV) *  (off_t)IMM_NODES_MAX;
            imap->vmap = (IMMV *)mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, imap->vfd, 0);
            fstat(imap->vfd, &st);
            imap->vsize = st.st_size;
            //fprintf(stdout, "vsize:%lld\n", imap->vsize);
       }
       else
       {
           fprintf(stderr, "open %s failed, %s\n", file, strerror(errno));
       }
       RWLOCK_INIT(imap->rwlock);
    }
    return imap;
}

/* imap set val */
int imap_vset(IMAP *imap, u32_t no, int32_t val)
{
    off_t size = (off_t)((no / IMM_IMMV_INC) + 1) 
            * (off_t)IMM_IMMV_INC * (off_t) sizeof(IMMV);
    int ret = -1, n = 0, i = 0;

    if(imap && imap->state && no >= 0 && no < IMM_NODES_MAX)
    {
        if(size > imap->vsize)
        {
            n = ftruncate(imap->vfd, size);
            //memset(((char *)imap->vmap+imap->vsize), 0, size - imap->vsize);
            i = imap->vsize / sizeof(IMMV);
            n = size / sizeof(IMMV);
            while(i < n) imap->vmap[i++].off = -1;
            imap->vsize = size;
        }
        //imap->vmap[no].val = val; 
        ret = 0;
    }
    return ret;
}

/* imap get val */
int imap_vget(IMAP *imap, u32_t no, int32_t *val)
{
    int ret = -1, n = 0;

    if((n = (imap->vsize/sizeof(IMMV))) > 0 && no >= 0 && no < n)
    {
        if(val) *val = imap->vmap[no].val; 
        ret = 0;
    }
    return ret;
}

/* new bolt  */
int imap_slot_new(IMAP *imap)
{
    off_t size = 0;
    int ret = -1, n = 0;

    if(imap && imap->state)
    {
        if(imap->state->nleft > 0)
        {
            n = --(imap->state->nleft);
            ret = imap->state->qleft[n];
        }
        else
        {
            size = imap->size + (off_t)sizeof(IMMKV) * (off_t)IMM_SLOT_NUM; 
            n = ftruncate(imap->fd, size);
            memset(((char *)imap->state+imap->size), 0, (size - imap->size));
            ret = (imap->size - (off_t)sizeof(IMMSTATE)) / (off_t)sizeof(IMMKV);
            imap->size = size;
        }
    }
    return ret;
}

int imap_insert(IMAP *imap, u32_t no, int32_t key)
{
    int ret = -1, i = 0, k = -1, nodeid = 0, pos = 0, num = 0, 
        n = 0, x = 0, min = 0, max = 0;
    IMMKV *kvs = NULL, *kv = NULL;
    IMMV *vnodes = NULL;

    if(imap && imap->state && (vnodes = imap->vmap))
    {
        if((n = imap->state->count) > 0)
        {
            max = n - 1;
            min = 0;
            if(key <= imap->slots[min].max) k = min;
            else if(key >= imap->slots[max].min) k = max;
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
                    if(key >=  imap->slots[x].min && key <= imap->slots[x+1].min)
                    {
                        k = x;
                        break;
                    }
                    else if(key > imap->slots[x].max) min = x;
                    else max = x;
                }
            }
        }
        /* 未满的slot 直接插入 */
        if(k >= 0 && k < n && imap->slots[k].count < IMM_SLOT_NUM)
        {
            x = imap->slots[k].count++;
            kvs = imap->map + imap->slots[k].nodeid;
            while(x > 0 && key < kvs[x-1].key)
            {
                kvs[x].key = kvs[x-1].key;
                kvs[x].val = kvs[x-1].val;
                imap->vmap[(kvs[x].val)].off = (imap->slots[k].nodeid + x);
                --x;
            }
            kvs[x].key = key;
            kvs[x].val = no;
            imap->vmap[no].off = (imap->slots[k].nodeid + x);
            if(key < imap->slots[k].min) imap->slots[k].min = key;
            if(key > imap->slots[k].max) imap->slots[k].max = key;
        }
        else
        {
            nodeid = imap_slot_new(imap);
            /* slot已满 转移元素到新的slot */
            if(k >= 0 && k < n && imap->slots[k].count == IMM_SLOT_NUM)
            {
                kvs = imap->map + imap->slots[k].nodeid + IMM_SLOT2_NUM;
                kv = imap->map + nodeid;
                num = IMM_SLOT2_NUM;
                x = 0;
                for(i = 0; i < IMM_SLOT2_NUM; i++) 
                {
                    if(key >= kvs->key && key < kvs[i].key && num == IMM_SLOT2_NUM) 
                    {
                        kv->key = key;
                        kv->val = no;
                        ++kv;
                        ++x;
                        imap->vmap[no].off = (nodeid + x);
                        num = IMM_SLOT2_NUM+1;
                    }
                    kv->key = kvs[i].key;
                    kv->val = kvs[i].val;
                    ++kv;
                    ++x;
                    imap->vmap[(kv->val)].off = (nodeid + x);
                }
                if(num == IMM_SLOT2_NUM && key >= kvs[i-1].key)
                {
                    kv->key = key;
                    kv->val = no;
                    ++kv;
                    ++x;
                    num = IMM_SLOT2_NUM+1;
                    imap->vmap[(kv->val)].off = (nodeid + x);
                }
                if(num == IMM_SLOT2_NUM)
                {
                    kvs = imap->map + imap->slots[k].nodeid;
                    i = IMM_SLOT2_NUM;
                    while(i > 0 && key < kvs[i-1].key)
                    {
                        kvs[i].key = kvs[i-1].key;
                        kvs[i].val = kvs[i-1].val;
                        imap->vmap[(kvs[i].val)].off = (imap->slots[k].nodeid + i);
                        --i;
                    }
                    kvs[i].key = key;
                    kvs[i].val = no;
                    imap->vmap[no].off = (imap->slots[k].nodeid + i);
                    imap->slots[k].min = kvs[0].key;
                    imap->slots[k].max = kvs[IMM_SLOT2_NUM].key;
                    imap->slots[k].count = IMM_SLOT2_NUM + 1;
                }
                else
                {
                    kvs = imap->map + imap->slots[k].nodeid;
                    imap->slots[k].min = kvs[0].key;
                    imap->slots[k].max = kvs[IMM_SLOT2_NUM-1].key;
                    imap->slots[k].count = IMM_SLOT2_NUM;
                }
                pos = k+1;
                k = imap->state->count++; 
                while(k > pos)
                {
                    memcpy(&(imap->slots[k]), &(imap->slots[k-1]), sizeof(IMMSLOT));
                    x = (imap->slots[k].nodeid / IMM_SLOT_NUM);
                    imap->roots[x] = k;
                    --k;
                }
            }
            else
            {
                /* 创建新的slot */
                kv = imap->map + nodeid;
                kv->key = key;
                kv->val = no;
                imap->vmap[no].off = nodeid;
                num = 1;
                k = imap->state->count++; 
                while(k > 0 && key <= imap->slots[k].min)
                {
                    memcpy(&(imap->slots[k]), &(imap->slots[k-1]), sizeof(IMMSLOT));
                    x = (imap->slots[k].nodeid / IMM_SLOT_NUM);
                    imap->roots[x] = k;
                    --k;
                }
            }
            kv = imap->map + nodeid;
            imap->slots[k].min = kv[0].key;
            imap->slots[k].max = kv[num-1].key;
            imap->slots[k].nodeid = nodeid;
            imap->slots[k].count = num;
            //fprintf(stdout, "%s::%d k:%d min:%d max:%d num:%d/%d\n", __FILE__, __LINE__, k, imap->slots[k].min, imap->slots[k].max, num, imap->state->count);
        }
    }
    return ret;
}

int imap_remove(IMAP *imap, u32_t no)
{
    u32_t nodeid = 0, rootid = 0, slotid = 0;
    int ret = -1, i = 0, x = 0, n = 0;
    IMMKV *kvs = NULL;

    if(imap && imap->state && imap->vmap 
            && (n = imap->vsize/sizeof(IMMV)) > 0 && no >= 0 && no < n
            && (nodeid = imap->vmap[no].off) >= 0)
    {
        rootid = (nodeid / IMM_SLOT_NUM);
        slotid = imap->roots[rootid];
        i = nodeid % IMM_SLOT_NUM;
        kvs = imap->map + imap->slots[slotid].nodeid;    
        while(i < imap->slots[slotid].count)
        {
            memcpy(&(kvs[i]), &(kvs[i+1]), sizeof(IMMKV));
            x = kvs[i].val;
            imap->vmap[x].off = i;
            ++i;
        }
        if(imap->slots[slotid].count == 1)
        {
            i = slotid;
            imap->state->qleft[(imap->state->nleft++)] = nodeid;
            imap->roots[rootid] = -1;
            while(i < imap->state->count)
            {
                memcpy(&(imap->slots[i]), &(imap->slots[i+1]), sizeof(IMMSLOT));
                rootid = (imap->slots[i].nodeid / IMM_SLOT_NUM);
                imap->roots[rootid] = i;
                ++i;
            }
            --imap->state->count;
        }
        else
        {
            n = --(imap->slots[slotid].count);
            imap->slots[slotid].min = kvs[0].key;
            imap->slots[slotid].max = kvs[n - 1].key;
        }
    }
    return ret;
}

int imap_find_slot(IMAP *imap, int32_t key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    if(imap && (n = imap->state->count) > 0)
    {
        max = n - 1;
        min = 0;
        if(key <= imap->slots[min].max) ret = min;
        else if(key >= imap->slots[max].min) ret = max;
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
                if(key >=  imap->slots[x].min && key <= imap->slots[x+1].min)
                {
                    ret = x;
                    break;
                }
                else if(key > imap->slots[x].max) min = x;
                else max = x;
            }
        }
        if((x = ret)>= 0 && x < n)
        {
            while(x >= 0 && key <= imap->slots[x].max)
            {
                ret = x--;
            }
        }
    }
    return ret;
}

int imap_find_slot2(IMAP *imap, int32_t key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    if(imap && (n = imap->state->count) > 0)
    {
        max = n - 1;
        min = 0;
        if(key <= imap->slots[min].max) ret = min;
        else if(key >= imap->slots[max].min) ret = max;
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
                if(key >=  imap->slots[x].min && key <= imap->slots[x+1].min)
                {
                    ret = x;
                    break;
                }
                else if(key > imap->slots[x].max) min = x;
                else max = x;
            }
        }
        if((x = ret)>= 0 && x < n)
        {
            while(x < n && key >= imap->slots[x].min) 
            {
                ret = x++;
            }
        }
    }
    return ret;
}

int imap_find_kv(IMAP *imap, int k, int32_t key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    IMMKV *kvs = NULL;

    if(imap && k >= 0 && k < imap->state->count 
            && (n = imap->slots[k].count) > 0)
    {
        kvs = imap->map + imap->slots[k].nodeid;
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
        //fprintf(stdout, "find_kv(%d) min:%d max:%d count:%d ret:%d\n", key, imap->slots[k].min, imap->slots[k].max, imap->slots[k].count, ret);
    }
    return ret;
}

int imap_find_kv2(IMAP *imap, int k, int32_t key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    IMMKV *kvs = NULL;

    if(imap && k >= 0 && k < imap->state->count 
            && (n = imap->slots[k].count) > 0)
    {
        kvs = imap->map + imap->slots[k].nodeid;
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

int imap_in(IMAP *imap, int32_t key, u32_t *list)
{
    int i = 0, k = 0, z = 0, ret = 0, n = 0;
    IMMKV *kvs = NULL;

    if(imap && imap->state && (n = imap->state->count) > 0)
    {
        RWLOCK_RDLOCK(imap->rwlock);
        k = imap_find_slot(imap, key);
        i = imap_find_kv(imap, k, key);
        do
        {
            kvs = imap->map + imap->slots[k].nodeid;
            if(key == kvs[i].key && i < imap->slots[k].count)
            {
                if(key == imap->slots[k].max)
                {
                    ret += imap->slots[k].count - i;
                    if(list)
                    {
                        while(i < imap->slots[k].count) list[z++] = kvs[i++].val;
                    }
                }
                else
                {
                    while(i < imap->slots[k].count && key == kvs[i].key)
                    {
                        if(list)list[z++] = kvs[i].val;
                        ++ret;
                        ++i;
                    }
                }
            }
            i=0;
        }while(++k < n && imap->slots[k].min == key);
        RWLOCK_UNLOCK(imap->rwlock);
    }
    return ret;
}

int imap_range(IMAP *imap, int32_t from, int32_t to, u32_t *list)
{
    int i = 0, ii = 0, k = 0, kk = 0, j = 0, x = 0, z = 0, ret = 0, n = 0;
    IMMKV *kvs = NULL;

    if(imap && imap->state)
    {
        RWLOCK_RDLOCK(imap->rwlock);
        k = imap_find_slot(imap, from);
        kk = imap_find_slot2(imap, to);
        i = imap_find_kv(imap, k, from);
        ii = imap_find_kv2(imap, kk, to);
        if(k == kk)
        {
            ret = ii + 1 - i;
            if(list)
            {
                kvs = imap->map + imap->slots[k].nodeid;
                for(x = i; x <= ii; x++) list[z++] = kvs[x].val;
            }
        }
        else
        {
            n =  imap->slots[k].count;
            ret = n - i;
            if(list)
            {
                kvs = imap->map + imap->slots[k].nodeid;
                for(x = i; x < n; x++) list[z++] = kvs[x].val;
            }
            for(j = i+1; j < kk; j++)
            {
                ret += imap->slots[j].count;
                if(list)
                {
                    kvs = imap->map + imap->slots[j].nodeid;
                    for(x = 0; x < imap->slots[j].count; x++) list[z++] = kvs[x].val;
                }
            }
            ret += ii;
            if(list)
            {
                kvs = imap->map + imap->slots[kk].nodeid;
                for(x = 0; x < ii; x++) list[z++] = kvs[x].val;
            }
        }
        RWLOCK_UNLOCK(imap->rwlock);
    }
    return ret;
}

int imap_rangefrom(IMAP *imap, int32_t key, u32_t *list) /* key = from */
{
    int i = 0, k = 0, x = 0, z = 0, ret = 0, n = 0;
    IMMKV *kvs = NULL;

    if(imap && imap->state)
    {
        RWLOCK_RDLOCK(imap->rwlock);
        if((k = imap_find_slot(imap, key)) >= 0 && (i = imap_find_kv(imap, k, key)) >= 0)
        {
            kvs = imap->map + imap->slots[k].nodeid;
            n =  imap->slots[k].count;
            if(list)
            {
                for(x = i; x < n; x++) list[z++] = kvs[x].val;
            }
            ret = n - i;
            for(i = k + 1; i <  imap->state->count; i++)
            {
                ret += imap->slots[i].count;
                if(list)
                {
                    kvs = imap->map + imap->slots[i].nodeid;
                    n = imap->slots[i].count;
                    for(x = 0; x < n; x++) list[z++] = kvs[x].val;
                }
            }
        }
        RWLOCK_UNLOCK(imap->rwlock);
    }
    //fprintf(stdout, "%s::%d k:%d ret:%d/%d\n", __FILE__, __LINE__, k, ret, z);
    return ret;
}

int imap_rangeto(IMAP *imap, int32_t key, u32_t *list) /* key = to */
{
    int i = 0, k = 0, x = 0, j = 0, z = 0, ret = 0, n = 0;
    IMMKV *kvs = NULL;

    if(imap && imap->state && (n = (imap->state->count)) > 0)
    {
        RWLOCK_RDLOCK(imap->rwlock);
        if((k = imap_find_slot2(imap, key)) >= 0 && k < n 
                && (i = imap_find_kv2(imap, k, key)) >= 0)
        {
            for(j = 0; j < k; j++)
            {
                ret += imap->slots[j].count;
                if(list)
                {
                    kvs = imap->map + imap->slots[j].nodeid;
                    for(x = 0; x < imap->slots[j].count; x++)
                    {
                        list[z++] = kvs[x].val;
                    }
                }
            }
            ret += i + 1;
            if(list)
            {
                kvs = imap->map + imap->slots[k].nodeid;
                for(x = 0; x <= i; x++)
                {
                    list[z++] = kvs[x].val;
                }
            }
        }
        RWLOCK_UNLOCK(imap->rwlock);
    }
    //fprintf(stdout, "%s::%d k:%d ret:%d/%d\n", __FILE__, __LINE__, k, ret, z);
    return ret;
}

int imap_ins(IMAP *imap, int32_t *keys, int nkeys, u32_t *list)
{
    int ret = 0, i = 0, n = 0;
    u32_t *plist = list;

    if(imap && imap->state && keys && nkeys > 0)
    {
       for(i = 0; i < nkeys; i++) 
       {
            n = imap_in(imap, keys[i], plist);
            if(list) plist += n;
            ret += n;
       }
    }
    return ret;
}

int imap_get(IMAP *imap, u32_t no, u32_t *val)
{
    int ret = -1, n = 0;

    if(imap)
    {
        RWLOCK_RDLOCK(imap->rwlock);
        if((n = (imap->vsize/sizeof(IMMV))) > 0 && no < n)
        {
            if(val) *val = imap->vmap[no].val;
        }
        RWLOCK_UNLOCK(imap->rwlock);
    }
    return ret;
}

int imap_set(IMAP *imap, u32_t no, int32_t key)
{
    int ret = -1;

    if(imap)
    {
       RWLOCK_WRLOCK(imap->rwlock);
       imap_vset(imap, no, key);
       if(imap->vmap[no].off  < 0)
       {
           imap_insert(imap, no, key);
       }
       else
       {
           if(key != imap->vmap[no].val)
           {
                imap_remove(imap, no);
                imap_insert(imap, no, key);
           }
       }
       ret = 0;
       imap->vmap[no].val = key;
       RWLOCK_UNLOCK(imap->rwlock);
    }
    return ret;
}

int imap_del(IMAP *imap, u32_t no)
{
    int ret = -1, n = 0;

    if(imap)
    {
        RWLOCK_WRLOCK(imap->rwlock);
        if((n = (imap->vsize/sizeof(IMMV))) > 0 && no < n)
        {
            imap_remove(imap, no);
            imap->vmap[no].off = -1;
            ret = 0;
        }
        RWLOCK_UNLOCK(imap->rwlock);
    }
    return ret;
}

void imap_close(IMAP *imap)
{
    if(imap)
    {
        if(imap->state) munmap(imap->state, imap->msize);
        if(imap->fd) close(imap->fd);
        if(imap->vmap) munmap(imap->vmap, imap->vmsize);
        if(imap->vfd) close(imap->vfd);
        RWLOCK_DESTROY(imap->rwlock);
        free(imap);
    }
    return ;
}

#ifdef IMAP_TEST
#include "timer.h"
#define MASK  120000
//gcc -o imap imap.c -DIMAP_TEST -DTEST_INS -DHAVE_PTHREAD -lpthread && ./imap
int main()
{
    IMAP *imap = NULL;
    int i = 0, j = 0, n = 0, total = 0, stat[MASK], stat2[MASK];
    int32_t val = 0, from = 0, to = 0, *res = NULL;
    int32_t inputs[256], last[256];
    int32_t all = 0;
    time_t stime = 0, etime = 0;
    void *timer = NULL;

    if((imap = imap_init("/tmp/1.idx")))
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
           imap_set(imap, i, val);
           last[j] = i;
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "set() 40000000 data, time used:%lld\n", PT_LU_USEC(timer));
        TIMER_RESET(timer);
        for(i = 0; i < n; i++)
        {
            imap_del(imap, last[i]);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "del() time used:%lld\n", PT_LU_USEC(timer));
        TIMER_RESET(timer);
        for(i = 0; i < n; i++)
        {
            val = inputs[i];
            stat2[val] = imap_in(imap, val, res);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "in() time used:%lld\n", PT_LU_USEC(timer));
        TIMER_RESET(timer);
        total = imap_ins(imap, inputs, n, NULL);
        TIMER_SAMPLE(timer);
        fprintf(stdout, "ins(keys, NULL) total:%d time used:%lld\n", total, PT_LU_USEC(timer));
        TIMER_RESET(timer);
        total = imap_ins(imap, inputs, n, res);
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
        for(i = 0; i < imap->state->count; i++)
        {
            fprintf(stdout, "%d:{min:%d max:%d}(%d)\n", i, imap->slots[i].min, imap->slots[i].max, imap->slots[i].count);
        }
        */
#ifdef TEST_RANGE
        srand(time(NULL));
        TIMER_RESET(timer);
        for(i = 1; i < 40000000; i++)
        {
            val = 1356969600 + (rand()%31536000);
            imap_set(imap, i, val);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "set() 40000000 timestamps,  time used:%lld\n", PT_LU_USEC(timer));
        srand(time(NULL));
        TIMER_RESET(timer);
        all = 0;
        for(i = 0; i < 1000; i++)
        {
            val = 1356969600 + (rand()%31536000);
            all += imap_rangefrom(imap, val, res);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "rangefrom() 1000 times total:%lld, time used:%lld\n", (long long int)all, PT_LU_USEC(timer));
        srand(time(NULL));
        TIMER_RESET(timer);
        all = 0;
        for(i = 0; i < 1000; i++)
        {
            val = 1356969600 + (rand()%31536000);
            all += imap_rangeto(imap, val, res);
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
            all += imap_range(imap, from, to, res);
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
            all += imap_range(imap, from, to, NULL);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "range(null) 1000 times total:%lld, time used:%lld\n", (long long int)all, PT_LU_USEC(timer));

#endif
        imap_close(imap);
        TIMER_CLEAN(timer);
        free(res);
    }
}
#endif
