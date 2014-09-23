#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include "dmap.h"
#include "rwlock.h"

DMAP *dmap_init(char *file)
{
    DMAP *dmap = NULL;
    char path[1024];
    struct stat st = {0};
    off_t size = 0;
    int i = 0;

    if(file && (dmap = (DMAP *)calloc(1, sizeof(DMAP))))
    {
       if((dmap->fd = open(file, O_CREAT|O_RDWR, 0644)) > 0) 
       {
           size = dmap->msize = (off_t)sizeof(DMMSTATE) + (off_t)sizeof(DMMKV) * (off_t)DMM_NODES_MAX;
           dmap->state = (DMMSTATE*)mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, dmap->fd, 0);
           dmap->map = (DMMKV *)((char *)dmap->state + sizeof(DMMSTATE));
           fstat(dmap->fd, &st);
           dmap->size = st.st_size;
           if(st.st_size < sizeof(DMMSTATE))
           {
                i = ftruncate(dmap->fd, sizeof(DMMSTATE));
                memset(dmap->state, 0, sizeof(DMMSTATE));
                dmap->size += sizeof(DMMSTATE);
                for(i = 0; i < DMM_SLOT_MAX; i++)
                {
                   dmap->state->slots[i].nodeid = -1;
                }
           }
           dmap->roots = dmap->state->roots;
           dmap->slots = dmap->state->slots;
           //fprintf(stdout, "size:%lld/%d\n", dmap->size, sizeof(DMMSTATE));
       }
       else
       {
           fprintf(stderr, "open %s failed, %s\n", file, strerror(errno));
       }
       sprintf(path, "%s.v", file);
       if((dmap->vfd = open(path, O_CREAT|O_RDWR, 0644)) > 0) 
       {
            size = dmap->vmsize = (off_t)sizeof(DMMV) *  (off_t)DMM_NODES_MAX;
            dmap->vmap = (DMMV *)mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, dmap->vfd, 0);
            fstat(dmap->vfd, &st);
            dmap->vsize = st.st_size;
            //fprintf(stdout, "vsize:%lld\n", dmap->vsize);
       }
       else
       {
           fprintf(stderr, "open %s failed, %s\n", file, strerror(errno));
       }
       RWLOCK_INIT(dmap->rwlock);
    }
    return dmap;
}

/* dmap set val */
int dmap_vset(DMAP *dmap, u32_t no, double val)
{
    off_t size = (off_t)((no / DMM_DMMV_INC) + 1) 
            * (off_t)DMM_DMMV_INC * (off_t) sizeof(DMMV);
    int ret = -1, n = 0, i = 0;

    if(dmap && dmap->state && no >= 0 && no < DMM_NODES_MAX)
    {
        if(size > dmap->vsize)
        {
            n = ftruncate(dmap->vfd, size);
            //memset(((char *)dmap->vmap+dmap->vsize), 0, size - dmap->vsize);
            i = dmap->vsize / sizeof(DMMV);
            n = size / sizeof(DMMV);
            while(i < n) dmap->vmap[i++].off = -1;
            dmap->vsize = size;
        }
        //dmap->vmap[no].val = val; 
        ret = 0;
    }
    return ret;
}

/* dmap get val */
int dmap_vget(DMAP *dmap, u32_t no, double *val)
{
    int ret = -1, n = 0;

    if((n = (dmap->vsize/sizeof(DMMV))) > 0 && no >= 0 && no < n)
    {
        if(val) *val = dmap->vmap[no].val; 
        ret = 0;
    }
    return ret;
}

/* new bolt  */
int dmap_slot_new(DMAP *dmap)
{
    off_t size = 0;
    int ret = -1, n = 0;

    if(dmap && dmap->state)
    {
        if(dmap->state->nleft > 0)
        {
            n = --(dmap->state->nleft);
            ret = dmap->state->qleft[n];
        }
        else
        {
            size = dmap->size + (off_t)sizeof(DMMKV) * (off_t)DMM_SLOT_NUM; 
            n = ftruncate(dmap->fd, size);
            memset(((char *)dmap->state+dmap->size), 0, (size - dmap->size));
            ret = (dmap->size - (off_t)sizeof(DMMSTATE)) / (off_t)sizeof(DMMKV);
            dmap->size = size;
        }
    }
    return ret;
}

int dmap_insert(DMAP *dmap, u32_t no, double key)
{
    int ret = -1, i = 0, k = -1, nodeid = 0, pos = 0, num = 0, 
        n = 0, x = 0, min = 0, max = 0;
    DMMKV *kvs = NULL, *kv = NULL;
    DMMV *vnodes = NULL;

    if(dmap && dmap->state && (vnodes = dmap->vmap))
    {
        if((n = dmap->state->count) > 0)
        {
            max = n - 1;
            min = 0;
            if(key <= dmap->slots[min].max) k = min;
            else if(key >= dmap->slots[max].min) k = max;
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
                    if(key >=  dmap->slots[x].min && key <= dmap->slots[x+1].min)
                    {
                        k = x;
                        break;
                    }
                    else if(key > dmap->slots[x].max) min = x;
                    else max = x;
                }
            }
        }
        /* 未满的slot 直接插入 */
        if(k >= 0 && k < n && dmap->slots[k].count < DMM_SLOT_NUM)
        {
            x = dmap->slots[k].count++;
            kvs = dmap->map + dmap->slots[k].nodeid;
            while(x > 0 && key < kvs[x-1].key)
            {
                kvs[x].key = kvs[x-1].key;
                kvs[x].val = kvs[x-1].val;
                dmap->vmap[(kvs[x].val)].off = (dmap->slots[k].nodeid + x);
                --x;
            }
            kvs[x].key = key;
            kvs[x].val = no;
            dmap->vmap[no].off = (dmap->slots[k].nodeid + x);
            if(key < dmap->slots[k].min) dmap->slots[k].min = key;
            if(key > dmap->slots[k].max) dmap->slots[k].max = key;
        }
        else
        {
            nodeid = dmap_slot_new(dmap);
            /* slot已满 转移元素到新的slot */
            if(k >= 0 && k < n && dmap->slots[k].count == DMM_SLOT_NUM)
            {
                kvs = dmap->map + dmap->slots[k].nodeid + DMM_SLOT2_NUM;
                kv = dmap->map + nodeid;
                num = DMM_SLOT2_NUM;
                x = 0;
                for(i = 0; i < DMM_SLOT2_NUM; i++) 
                {
                    if(key >= kvs->key && key < kvs[i].key && num == DMM_SLOT2_NUM) 
                    {
                        kv->key = key;
                        kv->val = no;
                        ++kv;
                        ++x;
                        dmap->vmap[no].off = (nodeid + x);
                        num = DMM_SLOT2_NUM+1;
                    }
                    kv->key = kvs[i].key;
                    kv->val = kvs[i].val;
                    ++kv;
                    ++x;
                    dmap->vmap[(kv->val)].off = (nodeid + x);
                }
                if(num == DMM_SLOT2_NUM && key >= kvs[i-1].key)
                {
                    kv->key = key;
                    kv->val = no;
                    ++kv;
                    ++x;
                    num = DMM_SLOT2_NUM+1;
                    dmap->vmap[(kv->val)].off = (nodeid + x);
                }
                if(num == DMM_SLOT2_NUM)
                {
                    kvs = dmap->map + dmap->slots[k].nodeid;
                    i = DMM_SLOT2_NUM;
                    while(i > 0 && key < kvs[i-1].key)
                    {
                        kvs[i].key = kvs[i-1].key;
                        kvs[i].val = kvs[i-1].val;
                        dmap->vmap[(kvs[i].val)].off = (dmap->slots[k].nodeid + i);
                        --i;
                    }
                    kvs[i].key = key;
                    kvs[i].val = no;
                    dmap->vmap[no].off = (dmap->slots[k].nodeid + i);
                    dmap->slots[k].min = kvs[0].key;
                    dmap->slots[k].max = kvs[DMM_SLOT2_NUM].key;
                    dmap->slots[k].count = DMM_SLOT2_NUM + 1;
                }
                else
                {
                    kvs = dmap->map + dmap->slots[k].nodeid;
                    dmap->slots[k].min = kvs[0].key;
                    dmap->slots[k].max = kvs[DMM_SLOT2_NUM-1].key;
                    dmap->slots[k].count = DMM_SLOT2_NUM;
                }
                pos = k+1;
                k = dmap->state->count++; 
                while(k > pos)
                {
                    memcpy(&(dmap->slots[k]), &(dmap->slots[k-1]), sizeof(DMMSLOT));
                    x = (dmap->slots[k].nodeid / DMM_SLOT_NUM);
                    dmap->roots[x] = k;
                    --k;
                }
                dmap->roots[x] = k;
            }
            else
            {
                /* 创建新的slot */
                kv = dmap->map + nodeid;
                kv->key = key;
                kv->val = no;
                dmap->vmap[no].off = nodeid;
                num = 1;
                k = dmap->state->count++; 
                while(k > 0 && key <= dmap->slots[k].min)
                {
                    memcpy(&(dmap->slots[k]), &(dmap->slots[k-1]), sizeof(DMMSLOT));
                    x = (dmap->slots[k].nodeid / DMM_SLOT_NUM);
                    dmap->roots[x] = k;
                    --k;
                }
            }
            kv = dmap->map + nodeid;
            dmap->slots[k].min = kv[0].key;
            dmap->slots[k].max = kv[num-1].key;
            dmap->slots[k].nodeid = nodeid;
            dmap->slots[k].count = num;
            x = (dmap->slots[k].nodeid / DMM_SLOT_NUM);
            dmap->roots[x] = k;
            //fprintf(stdout, "%s::%d k:%d min:%d max:%d num:%d/%d\n", __FILE__, __LINE__, k, dmap->slots[k].min, dmap->slots[k].max, num, dmap->state->count);
        }
    }
    return ret;
}

int dmap_remove(DMAP *dmap, u32_t no)
{
    int ret = -1, i = 0, x = 0, n = 0, slotid = 0;
    u32_t nodeid = 0, rootid = 0;
    DMMKV *kvs = NULL;

    if(dmap && dmap->state && dmap->vmap 
            && (n = dmap->vsize/sizeof(DMMV)) > 0 && no >= 0 && no < n
            && (nodeid = dmap->vmap[no].off) >= 0)
    {
        rootid = (nodeid / DMM_SLOT_NUM);
        slotid = (int)dmap->roots[rootid];
        if(slotid < 0) return ret;
        i = nodeid % DMM_SLOT_NUM;
        kvs = dmap->map + dmap->slots[slotid].nodeid;    
        while(i < (dmap->slots[slotid].count-1))
        {
            memcpy(&(kvs[i]), &(kvs[i+1]), sizeof(DMMKV));
            x = kvs[i].val;
            dmap->vmap[x].off = (i + dmap->slots[slotid].nodeid);
            ++i;
        }
        if(dmap->slots[slotid].count == 1)
        {
            i = slotid;
            dmap->state->qleft[(dmap->state->nleft++)] = dmap->slots[slotid].nodeid;
            dmap->roots[rootid] = -1;
            while(i < (dmap->state->count-1))
            {
                memcpy(&(dmap->slots[i]), &(dmap->slots[i+1]), sizeof(DMMSLOT));
                rootid = (dmap->slots[i].nodeid / DMM_SLOT_NUM);
                dmap->roots[rootid] = i;
                ++i;
            }
            --(dmap->state->count);
        }
        else
        {
            n = --(dmap->slots[slotid].count);
            dmap->slots[slotid].min = kvs[0].key;
            dmap->slots[slotid].max = kvs[n - 1].key;
        }
        dmap->vmap[no].off = -1;
    }
    return ret;
}

int dmap_find_slot(DMAP *dmap, double key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    if(dmap && (n = dmap->state->count) > 0)
    {
        max = n - 1;
        min = 0;
        if(key <= dmap->slots[min].max) ret = min;
        else if(key > dmap->slots[max].min) ret = max;
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
                if(key >=  dmap->slots[x].min && key <= dmap->slots[x+1].min)
                {
                    ret = x;
                    break;
                }
                else if(key > dmap->slots[x].max) min = x;
                else max = x;
            }
        }
        if((x = ret)>= 0 && x < n)
        {
            while(x >= 0 && key <= dmap->slots[x].max)
            {
                ret = x--;
            }
        }
    }
    return ret;
}

int dmap_find_slot2(DMAP *dmap, double key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    if(dmap && (n = dmap->state->count) > 0)
    {
        max = n - 1;
        min = 0;
        if(key >= dmap->slots[max].min) ret = max;
        else if(key < dmap->slots[min].max) ret = min;
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
                if(key >=  dmap->slots[x].min && key <= dmap->slots[x+1].min)
                {
                    ret = x;
                    break;
                }
                else if(key > dmap->slots[x].max) min = x;
                else max = x;
            }
        }
        if((x = ret)>= 0 && x < n)
        {
            while(x < n && key >= dmap->slots[x].min) 
            {
                ret = x++;
            }
        }
    }
    return ret;
}

int dmap_find_kv(DMAP *dmap, int k, double key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    DMMKV *kvs = NULL;

    if(dmap && k >= 0 && k < dmap->state->count 
            && (n = dmap->slots[k].count) > 0)
    {
        kvs = dmap->map + dmap->slots[k].nodeid;
        min = 0;
        max = n - 1; 
        if(kvs[min].key >= key) ret = min;
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
                if(kvs[x].key < key && (x+1) < n) ret = ++x;
                else
                {
                    while(x >= 0 && key == kvs[x].key)
                    {
                        ret = x--;
                    }
                }
            }

        }
        //fprintf(stdout, "find_kv(%d) min:%d max:%d count:%d ret:%d\n", key, dmap->slots[k].min, dmap->slots[k].max, dmap->slots[k].count, ret);
    }
    return ret;
}

int dmap_find_kv2(DMAP *dmap, int k, double key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    DMMKV *kvs = NULL;

    if(dmap && k >= 0 && k < dmap->state->count 
            && (n = dmap->slots[k].count) > 0)
    {
        kvs = dmap->map + dmap->slots[k].nodeid;
        min = 0;
        max = n - 1; 
        if(kvs[max].key <= key) ret = max;
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
                if(kvs[x].key > key && x > 0) ret = --x;
                else
                {
                    while(x < n && key == kvs[x].key)
                    {
                        ret = x++;
                    }
                }
            }
        }
    }
    return ret;
}

int dmap_in(DMAP *dmap, double key, u32_t *list)
{
    int i = 0, k = 0, z = 0, ret = 0, n = 0;
    DMMKV *kvs = NULL;

    if(dmap && dmap->state && (n = dmap->state->count) > 0)
    {
        RWLOCK_RDLOCK(dmap->rwlock);
        k = dmap_find_slot(dmap, key);
        i = dmap_find_kv(dmap, k, key);
        do
        {
            kvs = dmap->map + dmap->slots[k].nodeid;
            if(key == kvs[i].key && i < dmap->slots[k].count)
            {
                if(key == dmap->slots[k].max)
                {
                    ret += dmap->slots[k].count - i;
                    if(list)
                    {
                        while(i < dmap->slots[k].count) list[z++] = kvs[i++].val;
                    }
                }
                else
                {
                    while(i < dmap->slots[k].count && key == kvs[i].key)
                    {
                        if(list)list[z++] = kvs[i].val;
                        ++ret;
                        ++i;
                    }
                }
            }
            i=0;
        }while(++k < n && dmap->slots[k].min == key);
        RWLOCK_UNLOCK(dmap->rwlock);
    }
    return ret;
}

int dmap_range(DMAP *dmap, double from, double to, u32_t *list)
{
    int i = 0, ii = 0, k = 0, kk = 0, j = 0, x = 0, z = 0, ret = 0, n = 0;
    DMMKV *kvs = NULL;

    if(dmap && dmap->state)
    {
        if(from == to) return dmap_in(dmap, from, list);
        RWLOCK_RDLOCK(dmap->rwlock);
        k = dmap_find_slot(dmap, from);
        kk = dmap_find_slot2(dmap, to);
        i = dmap_find_kv(dmap, k, from);
        ii = dmap_find_kv2(dmap, kk, to);
        if(k == kk)
        {
            ret = ii + 1 - i;
            if(list)
            {
                kvs = dmap->map + dmap->slots[k].nodeid;
                for(x = i; x <= ii; x++) list[z++] = kvs[x].val;
            }
        }
        else
        {
            n =  dmap->slots[k].count;
            ret = n - i;
            if(list)
            {
                kvs = dmap->map + dmap->slots[k].nodeid;
                for(x = i; x < n; x++) list[z++] = kvs[x].val;
            }
            for(j = i+1; j < kk; j++)
            {
                ret += dmap->slots[j].count;
                if(list)
                {
                    kvs = dmap->map + dmap->slots[j].nodeid;
                    for(x = 0; x < dmap->slots[j].count; x++) list[z++] = kvs[x].val;
                }
            }
            ret += ii;
            if(list)
            {
                kvs = dmap->map + dmap->slots[kk].nodeid;
                for(x = 0; x < ii; x++) list[z++] = kvs[x].val;
            }
        }
        RWLOCK_UNLOCK(dmap->rwlock);
    }
    return ret;
}

int dmap_rangefrom(DMAP *dmap, double key, u32_t *list) /* key = from */
{
    int i = 0, k = 0, x = 0, z = 0, ret = 0, n = 0;
    DMMKV *kvs = NULL;

    if(dmap && dmap->state)
    {
        RWLOCK_RDLOCK(dmap->rwlock);
        if((k = dmap_find_slot(dmap, key)) >= 0 && (i = dmap_find_kv(dmap, k, key)) >= 0)
        {
            kvs = dmap->map + dmap->slots[k].nodeid;
            n =  dmap->slots[k].count;
            if(list)
            {
                for(x = i; x < n; x++) list[z++] = kvs[x].val;
            }
            ret = n - i;
            for(i = k + 1; i <  dmap->state->count; i++)
            {
                ret += dmap->slots[i].count;
                if(list)
                {
                    kvs = dmap->map + dmap->slots[i].nodeid;
                    n = dmap->slots[i].count;
                    for(x = 0; x < n; x++) list[z++] = kvs[x].val;
                }
            }
        }
        RWLOCK_UNLOCK(dmap->rwlock);
    }
    //fprintf(stdout, "%s::%d k:%d ret:%d/%d\n", __FILE__, __LINE__, k, ret, z);
    return ret;
}

int dmap_rangeto(DMAP *dmap, double key, u32_t *list) /* key = to */
{
    int i = 0, k = 0, x = 0, j = 0, z = 0, ret = 0, n = 0;
    DMMKV *kvs = NULL;

    if(dmap && dmap->state && (n = (dmap->state->count)) > 0)
    {
        RWLOCK_RDLOCK(dmap->rwlock);
        if((k = dmap_find_slot2(dmap, key)) >= 0 && k < n 
                && (i = dmap_find_kv2(dmap, k, key)) >= 0)
        {
            for(j = 0; j < k; j++)
            {
                ret += dmap->slots[j].count;
                if(list)
                {
                    kvs = dmap->map + dmap->slots[j].nodeid;
                    for(x = 0; x < dmap->slots[j].count; x++)
                    {
                        list[z++] = kvs[x].val;
                    }
                }
            }
            ret += i + 1;
            if(list)
            {
                kvs = dmap->map + dmap->slots[k].nodeid;
                for(x = 0; x <= i; x++)
                {
                    list[z++] = kvs[x].val;
                }
            }
        }
        RWLOCK_UNLOCK(dmap->rwlock);
    }
    //fprintf(stdout, "%s::%d k:%d ret:%d/%d\n", __FILE__, __LINE__, k, ret, z);
    return ret;
}

int dmap_ins(DMAP *dmap, double *keys, int nkeys, u32_t *list)
{
    int ret = 0, i = 0, n = 0;
    u32_t *plist = list;

    if(dmap && dmap->state && keys && nkeys > 0)
    {
       for(i = 0; i < nkeys; i++) 
       {
            n = dmap_in(dmap, keys[i], plist);
            if(list) plist += n;
            ret += n;
       }
    }
    return ret;
}

int dmap_get(DMAP *dmap, u32_t no, u32_t *val)
{
    int ret = -1, n = 0;

    if(dmap)
    {
        RWLOCK_RDLOCK(dmap->rwlock);
        if((n = (dmap->vsize/sizeof(DMMV))) > 0 && no < n)
        {
            if(val) *val = dmap->vmap[no].val;
            ret = 0;
        }
        RWLOCK_UNLOCK(dmap->rwlock);
    }
    return ret;
}

int dmap_set(DMAP *dmap, u32_t no, double key)
{
    int ret = -1;

    if(dmap)
    {
       RWLOCK_WRLOCK(dmap->rwlock);
       dmap_vset(dmap, no, key);
       if(dmap->vmap[no].off  < 0)
       {
           dmap_insert(dmap, no, key);
       }
       else
       {
           if(key != dmap->vmap[no].val)
           {
                dmap_remove(dmap, no);
                dmap_insert(dmap, no, key);
           }
       }
       ret = 0;
       dmap->vmap[no].val = key;
       RWLOCK_UNLOCK(dmap->rwlock);
    }
    return ret;
}

int dmap_del(DMAP *dmap, u32_t no)
{
    int ret = -1, n = 0;

    if(dmap)
    {
        RWLOCK_WRLOCK(dmap->rwlock);
        if((n = (dmap->vsize/sizeof(DMMV))) > 0 && no < n)
        {
            dmap_remove(dmap, no);
            dmap->vmap[no].off = -1;
            ret = 0;
        }
        RWLOCK_UNLOCK(dmap->rwlock);
    }
    return ret;
}

void dmap_close(DMAP *dmap)
{
    if(dmap)
    {
        if(dmap->state) munmap(dmap->state, dmap->msize);
        if(dmap->fd) close(dmap->fd);
        if(dmap->vmap) munmap(dmap->vmap, dmap->vmsize);
        if(dmap->vfd) close(dmap->vfd);
        RWLOCK_DESTROY(dmap->rwlock);
        free(dmap);
    }
    return ;
}

#ifdef DMAP_TEST
#include "timer.h"
#define MASK  120000
//gcc -o dmap dmap.c -DDMAP_TEST -DTEST_INS -DHAVE_PTHREAD -lpthread && ./dmap
int main()
{
    DMAP *dmap = NULL;
    int i = 0, j = 0, n = 0, total = 0, no = 0, stat[MASK], stat2[MASK];
    double val = 0, from = 0, to = 0, *res = NULL;
    double inputs[256], nos[256], last[256];
    double all = 0;
    time_t stime = 0, etime = 0;
    void *timer = NULL;

    if((dmap = dmap_init("/tmp/1.idx")))
    {
        res = (double *)calloc(60000000, sizeof(double));
        TIMER_INIT(timer);
#ifdef TEST_INS
        //fprintf(stdout, "sizeof(stat):%d\n", sizeof(stat));
        memset(stat, 0, sizeof(stat));
        memset(stat2, 0, sizeof(stat2));
        srand(time(NULL));
        n = 256;
        for(i = 0; i < n; i++)
        {
            no = (rand()%MASK);
            nos[i] = no;
            if((i % 3) == 0)
                inputs[i] = no * -1;
            else
                inputs[i] = no;
        }
        TIMER_RESET(timer);
        for(i = 1; i < 20000000; i++)
        {
           j = (rand()%n);
           val = inputs[j];
           no = nos[j];
           stat[no]++;
           dmap_set(dmap, i, val);
           last[j] = i;
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "set() 40000000 data, time used:%lld\n", PT_LU_USEC(timer));
        TIMER_RESET(timer);
        for(i = 0; i < n; i++)
        {
            dmap_del(dmap, last[i]);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "del() time used:%lld\n", PT_LU_USEC(timer));
        TIMER_RESET(timer);
        for(i = 0; i < n; i++)
        {
            val = inputs[i];
            no = nos[i];
            stat2[no] = dmap_in(dmap, val, res);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "in() time used:%lld\n", PT_LU_USEC(timer));
        TIMER_RESET(timer);
        total = dmap_ins(dmap, inputs, n, NULL);
        TIMER_SAMPLE(timer);
        fprintf(stdout, "ins(keys, NULL) total:%d time used:%lld\n", total, PT_LU_USEC(timer));
        TIMER_RESET(timer);
        total = dmap_ins(dmap, inputs, n, res);
        TIMER_SAMPLE(timer);
        fprintf(stdout, "ins(keys, res:%p) total:%d time used:%lld\n", res, total, PT_LU_USEC(timer));
        for(i = 0; i < n; i++)
        {
            j = nos[i];
            if(stat[j] != stat2[j])
                fprintf(stdout, "%d:%d/%d::%d\n", j, stat[j], stat2[j], inputs[i]);
        }
#ifdef OUT_ALL
        for(i = 0; i < total; i++)
        {
            fprintf(stdout, "%d:%d\n", i, res[i]);
        }
#endif
#endif
        /*
        for(i = 0; i < dmap->state->count; i++)
        {
            fprintf(stdout, "%d:{min:%d max:%d}(%d)\n", i, dmap->slots[i].min, dmap->slots[i].max, dmap->slots[i].count);
        }
        */
#ifdef TEST_RANGE
        srand(time(NULL));
        TIMER_RESET(timer);
        for(i = 1; i < 40000000; i++)
        {
            val = 1356969600 + (rand()%31536000);
            dmap_set(dmap, i, val);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "set() 40000000 timestamps,  time used:%lld\n", PT_LU_USEC(timer));
        srand(time(NULL));
        TIMER_RESET(timer);
        all = 0;
        for(i = 0; i < 1000; i++)
        {
            val = 1356969600 + (rand()%31536000);
            all += dmap_rangefrom(dmap, val, res);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "rangefrom() 1000 times total:%lld, time used:%lld\n", (long long int)all, PT_LU_USEC(timer));
        srand(time(NULL));
        TIMER_RESET(timer);
        all = 0;
        for(i = 0; i < 1000; i++)
        {
            val = 1356969600 + (rand()%31536000);
            all += dmap_rangeto(dmap, val, res);
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
            all += dmap_range(dmap, from, to, res);
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
            all += dmap_range(dmap, from, to, NULL);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "range(null) 1000 times total:%lld, time used:%lld\n", (long long int)all, PT_LU_USEC(timer));

#endif
        dmap_close(dmap);
        TIMER_CLEAN(timer);
        free(res);
    }
}
#endif
