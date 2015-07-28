#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include "dkv.h"
#include "rwlock.h"
DKV *dkv_init(char *file)
{
    DKV *dkv = NULL;
    char path[1024];
    struct stat st = {0};
    off_t size = 0;
    int i = 0;

    if(file && (dkv = (DKV *)calloc(1, sizeof(DKV))))
    {
#ifdef __DKV_USE_IDX__
       if((dkv->fd = open(file, O_CREAT|O_RDWR, 0644)) > 0) 
       {
           size = dkv->msize = (off_t)sizeof(DVVSTATE) + (off_t)sizeof(DVVKV) * (off_t)DVV_NODES_MAX;

           dkv->state = (DVVSTATE*)mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, dkv->fd, 0);
           dkv->map = (DVVKV *)((char *)dkv->state + sizeof(DVVSTATE));
           fstat(dkv->fd, &st);
           dkv->size = st.st_size;
           if(st.st_size < sizeof(DVVSTATE))
           {
                i = ftruncate(dkv->fd, sizeof(DVVSTATE));
                memset(dkv->state, 0, sizeof(DVVSTATE));
                dkv->size = sizeof(DVVSTATE);
                for(i = 0; i < DVV_SLOT_MAX; i++)
                {
                   dkv->state->slots[i].nodeid = -1;
                }
           }
           dkv->roots = dkv->state->roots;
           dkv->slots = dkv->state->slots;
           //fprintf(stdout, "size:%lld/%d\n", dkv->size, sizeof(DVVSTATE));
       }
       else
       {
           fprintf(stderr, "open %s failed, %s\n", file, strerror(errno));
       }
#endif
       sprintf(path, "%s.v", file);
       if((dkv->vfd = open(path, O_CREAT|O_RDWR, 0644)) > 0) 
       {
            size = dkv->vmsize = (off_t)sizeof(DVVV) *  (off_t)DVV_NODES_MAX;
            dkv->vmap = (DVVV *)mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, dkv->vfd, 0);
            fstat(dkv->vfd, &st);
            dkv->vsize = st.st_size;
       }
       else
       {
           fprintf(stderr, "open %s failed, %s\n", file, strerror(errno));
       }
       RWLOCK_INIT(dkv->rwlock);
    }
    return dkv;
}

/* dkv set val */
int dkv_vset(DKV *dkv, u32_t no, double val)
{
    off_t size = (off_t)((no / DVV_DVVV_INC) + 1) 
            * (off_t)DVV_DVVV_INC * (off_t) sizeof(DVVV);
    int ret = -1, n = 0, i = 0;

    if(dkv && no >= 0 && no < DVV_NODES_MAX)
    {
        if(size > dkv->vsize)
        {
            n = ftruncate(dkv->vfd, size);
            //memset(((char *)dkv->vmap+dkv->vsize), 0, size - dkv->vsize);
            i = dkv->vsize / sizeof(DVVV);
            n = size / sizeof(DVVV);
#ifdef __DKV_USE_IDX__
            while(i < n) {dkv->vmap[i].off = -1;dkv->vmap[i].val=0;++i;}
#endif
            dkv->vsize = size;
        }
        ret = 0;
    }
    return ret;
}

/* dkv get val */
int dkv_vget(DKV *dkv, u32_t no, double *val)
{
    int ret = -1, n = 0;

    if((n = (dkv->vsize/sizeof(DVVV))) > 0 && no >= 0 && no < n)
    {
        if(val) *val = dkv->vmap[no].val; 
        ret = 0;
    }
    return ret;
}

#ifdef __DKV_USE_IDX__
/* new bolt  */
int dkv_slot_new(DKV *dkv)
{
    int ret = -1, i = 0, n = 0, id = 0;
    off_t size = 0;

    if(dkv && dkv->state)
    {
        if(dkv->state->nleft > 0)
        {
            n = --(dkv->state->nleft);
            ret = dkv->state->qleft[n];
        }
        else
        {
            size = (off_t)sizeof(DVVKV) * (off_t)DVV_SLOT_NUM * DVV_SLOT_INC + dkv->size; 
            n = ftruncate(dkv->fd, size);
            memset(((char *)dkv->state+dkv->size), 0, (size - dkv->size));
            ret = (dkv->size - (off_t)sizeof(DVVSTATE)) / (off_t)sizeof(DVVKV);
            id = ret + DVV_SLOT_NUM;
            i = 1;
            while(i < DVV_SLOT_INC)
            {
                dkv->state->qleft[(dkv->state->nleft++)] = id;
                id += DVV_SLOT_NUM;
                ++i;
            }
            dkv->size = size;
        }
    }
    return ret;
}

int dkv_insert(DKV *dkv, u32_t no, double key)
{
    int ret = -1, i = 0, k = -1, nodeid = 0, pos = 0, num = 0, 
        n = 0, x = 0, min = 0, max = 0, m = 0, num2 = 0;
    DVVKV *kvs = NULL, *kv = NULL, *kv1 = NULL, *kv2 = NULL;
    DVVV *vnodes = NULL;

    if(dkv && dkv->state && (vnodes = dkv->vmap))
    {
        if((n = dkv->state->count) > 0) 
        {
            max = n - 1;
            min = 0;
            if(key <= dkv->slots[min].max) k = min;
            else if(key >= dkv->slots[max].min) k = max;
            else
            {
                while(max > min)
                {
                    x = (min + max) / 2;
                    if(x == min)
                    {
                        if(key >= dkv->slots[max].min) k = max;
                        else k = x;
                        break;
                    }
                    if(key >=  dkv->slots[x].min && (key <= dkv->slots[x].max
                                || (x < (n - 1) && key <= dkv->slots[x+1].min)))
                    {
                        k = x;
                        break;
                    }
                    else if(key > dkv->slots[x].max) min = x;
                    else max = x;
                }
            }
        }
        if(k >= 0 && k < n && dkv->slots[k].count ==  DVV_SLOT_NUM
                && dkv->slots[k].max == key)
        {
            while(k < (n - 1) && dkv->slots[k].count ==  DVV_SLOT_NUM
                    && key == dkv->slots[k+1].min)++k;
        }
        /* 未满的slot 直接插入 */
        if(k >= 0 && k < n && dkv->slots[k].count < DVV_SLOT_NUM)
        {
            m = x = dkv->slots[k].count++;
            kvs = dkv->map + dkv->slots[k].nodeid;
            while(x > 0 && key < kvs[x-1].key)
            {
                kvs[x].key = kvs[x-1].key;
                kvs[x].val = kvs[x-1].val;
                dkv->vmap[(kvs[x].val)].off = (dkv->slots[k].nodeid + x);
                --x;
            }
            kvs[x].key = key;
            kvs[x].val = no;
            dkv->vmap[no].off = (dkv->slots[k].nodeid + x);
            dkv->slots[k].min = kvs[0].key;
            dkv->slots[k].max = kvs[m].key;
        }
        else
        {
            nodeid = dkv_slot_new(dkv);
            /* slot已满转移元素到新的slot */
            if(k >= 0 && k < n && dkv->slots[k].count == DVV_SLOT_NUM) 
            {
                kv1 = dkv->map + dkv->slots[k].nodeid + DVV_SLOT2_NUM;
                kv2 = dkv->map + nodeid;
                num = DVV_SLOT2_NUM;
                if(key < kv1[0].key)
                {
                    /* 转移尾部一半元素到新slot */
                    for(i = 0; i < DVV_SLOT2_NUM; i++)
                    {
                        kv2[i].key = kv1[i].key;
                        kv2[i].val = kv1[i].val;
                        dkv->vmap[(kv2[i].val)].off = (nodeid + i);
                    }
                    /* 插入到本slot */
                    kvs = dkv->map + dkv->slots[k].nodeid;
                    i = DVV_SLOT2_NUM;
                    while(i > 0 && key < kvs[i-1].key)
                    {
                        kvs[i].key = kvs[i-1].key;
                        kvs[i].val = kvs[i-1].val;
                        dkv->vmap[(kvs[i].val)].off = (dkv->slots[k].nodeid + i);
                        --i;
                    }
                    kvs[i].key = key;
                    kvs[i].val = no;
                    dkv->vmap[(kvs[i].val)].off = (dkv->slots[k].nodeid + i);
                    num = DVV_SLOT2_NUM;
                    num2 = DVV_SLOT2_NUM + 1;
                }
                else
                {
                    i = 0;
                    x = 0;
                    while(i < DVV_SLOT2_NUM)
                    {
                        if(key <= kv1[i].key && x == i)
                        {
                            kv2->key = key;
                            kv2->val = no;
                            dkv->vmap[(kv2->val)].off = (nodeid + x);
                            ++x;
                            ++kv2;
                        }
                        kv2->key = kv1[i].key;
                        kv2->val = kv1[i].val;
                        dkv->vmap[(kv2->val)].off = (nodeid + x);
                        ++kv2;
                        ++x;
                        ++i;
                    }
                    if(x == i)
                    {
                        kv2->key = key;
                        kv2->val = no;
                        dkv->vmap[(kv2->val)].off = (nodeid + x);
                    }
                    num = DVV_SLOT2_NUM + 1;
                    num2 = DVV_SLOT2_NUM;
                }
                /* rebuild slot */
                kvs = dkv->map + dkv->slots[k].nodeid;
                dkv->slots[k].count = num2;
                dkv->slots[k].min = kvs[0].key; 
                dkv->slots[k].max = kvs[num2-1].key; 
                pos = k+1;
                k = dkv->state->count++; 
                while(k > pos)
                {
                    memcpy(&(dkv->slots[k]), &(dkv->slots[k-1]), sizeof(DVVSLOT));
                    x = (dkv->slots[k].nodeid / DVV_SLOT_NUM);
                    dkv->roots[x] = k;
                    --k;
                }
            }
            else
            {
                /* 插入新的slot */
                kv = dkv->map + nodeid;
                kv->key = key;
                kv->val = no;
                dkv->vmap[no].off = nodeid;
                num = 1;
                m = k = dkv->state->count++; 
                while(k > 0 && key < dkv->slots[k-1].min)
                {
                    memcpy(&(dkv->slots[k]), &(dkv->slots[k-1]), sizeof(DVVSLOT));
                    x = (dkv->slots[k].nodeid / DVV_SLOT_NUM);
                    dkv->roots[x] = k;
                    --k;
                }
            }
            kv = dkv->map + nodeid;
            dkv->slots[k].min = kv[0].key;
            dkv->slots[k].max = kv[num-1].key;
            dkv->slots[k].nodeid = nodeid;
            dkv->slots[k].count = num;
            x = (dkv->slots[k].nodeid / DVV_SLOT_NUM);
            dkv->roots[x] = k;
        }
    }
    return ret;
}

int dkv_remove(DKV *dkv, u32_t no)
{
    int ret = -1, i = 0, x = 0, n = 0, slotid = 0;
    u32_t nodeid = 0, rootid = 0;
    DVVKV *kvs = NULL;

    if(dkv && dkv->state && dkv->vmap 
            && (n = dkv->vsize/sizeof(DVVV)) > 0 && no >= 0 && no < n
            && (nodeid = dkv->vmap[no].off) >= 0)
    {
        rootid = (nodeid / DVV_SLOT_NUM);
        slotid = (int)dkv->roots[rootid];
        if(slotid < 0) return ret;
        i = nodeid % DVV_SLOT_NUM;
        kvs = dkv->map + dkv->slots[slotid].nodeid;    
        while(i < (dkv->slots[slotid].count-1))
        {
            memcpy(&(kvs[i]), &(kvs[i+1]), sizeof(DVVKV));
            x = kvs[i].val;
            dkv->vmap[x].off = (i + dkv->slots[slotid].nodeid);
            ++i;
        }
        if(dkv->slots[slotid].count == 1)
        {
            i = slotid;
            dkv->state->qleft[(dkv->state->nleft++)] = dkv->slots[slotid].nodeid;
            dkv->roots[rootid] = -1;
            while(i < (dkv->state->count-1))
            {
                memcpy(&(dkv->slots[i]), &(dkv->slots[i+1]), sizeof(DVVSLOT));
                rootid = (dkv->slots[i].nodeid / DVV_SLOT_NUM);
                dkv->roots[rootid] = i;
                ++i;
            }
            --(dkv->state->count);
        }
        else
        {
            n = --(dkv->slots[slotid].count);
            dkv->slots[slotid].min = kvs[0].key;
            dkv->slots[slotid].max = kvs[n - 1].key;
        }
        dkv->vmap[no].off = -1;
    }
    return ret;
}

int dkv_find_slot(DKV *dkv, double key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    if(dkv && (n = dkv->state->count) > 0)
    {
        max = n - 1;
        min = 0;
        if(key <= dkv->slots[min].max) ret = min;
        else if(key > dkv->slots[max].min) ret = max;
        else
        {
            while(max > min)
            {
                x = (min + max) / 2;
                if(x == min)
                {
                    if(key <= dkv->slots[x].max) ret = x;
                    else if(key <= dkv->slots[max].max) ret = max;
                    break;
                }
                if(key >=  dkv->slots[x].min && key <= dkv->slots[x].max)
                            //|| (x < (n - 1) && key <= dkv->slots[x+1].min)))
                {
                    ret = x;
                    break;
                }
                else if(key > dkv->slots[x].max) min = x;
                else max = x;
            }
        }
        if((x = ret)>= 0 && x < n)
        {
            while(x >= 0 && key <= dkv->slots[x].max)
            {
                ret = x--;
            }
        }
    }
    return ret;
}

int dkv_find_slot2(DKV *dkv, double key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    if(dkv && (n = dkv->state->count) > 0)
    {
        max = n - 1;
        min = 0;
        if(key >= dkv->slots[max].min) ret = max;
        else if(key < dkv->slots[min].max) ret = min;
        else
        {
            while(max > min)
            {
                x = (min + max) / 2;
                if(x == min)
                {
                    if(key >= dkv->slots[max].min) ret = max;
                    else if(key >= dkv->slots[x].min) ret = x;
                    break;
                }
                if(key >=  dkv->slots[x].min && key <= dkv->slots[x].max) 
                            //|| (x < (n - 1) && key <= dkv->slots[x+1].min)))
                {
                    ret = x;
                    break;
                }
                else if(key > dkv->slots[x].max) min = x;
                else max = x;
            }
        }
        if((x = ret)>= 0 && x < n)
        {
            while(x < n && key >= dkv->slots[x].min) 
            {
                ret = x++;
            }
        }
    }
    return ret;
}

int dkv_find_kv(DKV *dkv, int k, double key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    DVVKV *kvs = NULL;

    if(dkv && k >= 0 && k < dkv->state->count 
            && (n = dkv->slots[k].count) > 0)
    {
        kvs = dkv->map + dkv->slots[k].nodeid;
        min = 0;
        max = n - 1; 
        if(max > 0)
        {
            if(kvs[min].key >= key) ret = min;
            else
            {
                while(max > min)
                {
                    x = (min + max) / 2;
                    if(x == min)
                    {
                        if(kvs[x].key >= key) ret = x;
                        else if(kvs[max].key >= key) ret = max;
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
        }
        else
        {
            if(kvs[min].key >= key) ret = min;
        }
        //fprintf(stdout, "find_kv(%d) min:%d max:%d count:%d ret:%d\n", key, dkv->slots[k].min, dkv->slots[k].max, dkv->slots[k].count, ret);
    }
    return ret;
}

int dkv_find_kv2(DKV *dkv, int k, double key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    DVVKV *kvs = NULL;

    if(dkv && k >= 0 && k < dkv->state->count 
            && (n = dkv->slots[k].count) > 0)
    {
        kvs = dkv->map + dkv->slots[k].nodeid;
        min = 0;
        max = n - 1; 
        if(max > 0)
        {
            if(kvs[max].key <= key) ret = max;
            else
            {
                while(max > min)
                {
                    x = (min + max) / 2;
                    if(x == min)
                    {
                        if(kvs[max].key <= key) ret = max;
                        else if(kvs[x].key <= key) ret = x;
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
        else
        {
            if(kvs[min].key <= key) ret = min;
        }
    }
    return ret;
}

int dkv_in(DKV *dkv, double key, u32_t *list)
{
    int i = 0, k = 0, z = 0, ret = 0, n = 0;
    DVVKV *kvs = NULL;

    if(dkv && dkv->state && (n = dkv->state->count) > 0)
    {
        RWLOCK_RDLOCK(dkv->rwlock);
        k = dkv_find_slot(dkv, key);
        i = dkv_find_kv(dkv, k, key);
        do
        {
            kvs = dkv->map + dkv->slots[k].nodeid;
            if(i >= 0 && key == kvs[i].key && i < dkv->slots[k].count)
            {
                if(key == dkv->slots[k].max)
                {
                    ret += dkv->slots[k].count - i;
                    if(list)
                    {
                        while(i < dkv->slots[k].count) list[z++] = kvs[i++].val;
                    }
                }
                else
                {
                    while(i < dkv->slots[k].count && key == kvs[i].key)
                    {
                        if(list)list[z++] = kvs[i].val;
                        ++ret;
                        ++i;
                    }
                }
            }
            i=0;
        }while(++k < n && dkv->slots[k].min == key);
        RWLOCK_UNLOCK(dkv->rwlock);
    }
    return ret;
}

int dkv_range(DKV *dkv, double from, double to, u32_t *list)
{
    int i = 0, ii = 0, k = 0, kk = 0, j = 0, x = 0, z = 0, ret = 0, n = 0;
    DVVKV *kvs = NULL;

    if(dkv && dkv->state)
    {
        if(from == to) return dkv_in(dkv, from, list);
        RWLOCK_RDLOCK(dkv->rwlock);
        k = dkv_find_slot(dkv, from);
        kk = dkv_find_slot2(dkv, to);
        i = dkv_find_kv(dkv, k, from);
        ii = dkv_find_kv2(dkv, kk, to);
        if(k == -1 || kk == -1 || i == -1 || ii == -1) goto end;
        if(k == kk)
        {
            ret = ii + 1 - i;
            if(list)
            {
                kvs = dkv->map + dkv->slots[k].nodeid;
                for(x = i; x <= ii; x++) list[z++] = kvs[x].val;
            }
        }
        else
        {
            n =  dkv->slots[k].count;
            ret = n - i;
            if(list)
            {
                kvs = dkv->map + dkv->slots[k].nodeid;
                for(x = i; x < n; x++) list[z++] = kvs[x].val;
            }
            for(j = k+1; j < kk; j++)
            {
                ret += dkv->slots[j].count;
                if(list)
                {
                    kvs = dkv->map + dkv->slots[j].nodeid;
                    for(x = 0; x < dkv->slots[j].count; x++) list[z++] = kvs[x].val;
                }
            }
            ret += ii + 1;
            if(list)
            {
                kvs = dkv->map + dkv->slots[kk].nodeid;
                for(x = 0; x <= ii; x++) list[z++] = kvs[x].val;
            }
        }
end:
        RWLOCK_UNLOCK(dkv->rwlock);
    }
    return ret;
}

int dkv_rangefrom(DKV *dkv, double key, u32_t *list) /* key = from */
{
    int i = 0, k = 0, x = 0, z = 0, ret = 0, n = 0;
    DVVKV *kvs = NULL;

    if(dkv && dkv->state)
    {
        RWLOCK_RDLOCK(dkv->rwlock);
        if((k = dkv_find_slot(dkv, key)) >= 0 && (i = dkv_find_kv(dkv, k, key)) >= 0)
        {
            //fprintf(stdout, "k:%d i:%d\n", k, i);
            kvs = dkv->map + dkv->slots[k].nodeid;
            n =  dkv->slots[k].count;
            if(list)
            {
                for(x = i; x < n; x++) list[z++] = kvs[x].val;
            }
            ret = n - i;
            for(i = k + 1; i <  dkv->state->count; i++)
            {
                ret += dkv->slots[i].count;
                if(list)
                {
                    kvs = dkv->map + dkv->slots[i].nodeid;
                    n = dkv->slots[i].count;
                    for(x = 0; x < n; x++) list[z++] = kvs[x].val;
                }
            }
        }
        RWLOCK_UNLOCK(dkv->rwlock);
    }
    //fprintf(stdout, "%s::%d k:%d ret:%d/%d\n", __FILE__, __LINE__, k, ret, z);
    return ret;
}

int dkv_rangeto(DKV *dkv, double key, u32_t *list) /* key = to */
{
    int i = 0, k = 0, x = 0, j = 0, z = 0, ret = 0, n = 0;
    DVVKV *kvs = NULL;

    if(dkv && dkv->state && (n = (dkv->state->count)) > 0)
    {
        RWLOCK_RDLOCK(dkv->rwlock);
        if((k = dkv_find_slot2(dkv, key)) >= 0 && k < n 
                && (i = dkv_find_kv2(dkv, k, key)) >= 0)
        {
            for(j = 0; j < k; j++)
            {
                ret += dkv->slots[j].count;
                if(list)
                {
                    kvs = dkv->map + dkv->slots[j].nodeid;
                    for(x = 0; x < dkv->slots[j].count; x++)
                    {
                        list[z++] = kvs[x].val;
                    }
                }
            }
            ret += i + 1;
            if(list)
            {
                kvs = dkv->map + dkv->slots[k].nodeid;
                for(x = 0; x <= i; x++)
                {
                    list[z++] = kvs[x].val;
                }
            }
        }
        RWLOCK_UNLOCK(dkv->rwlock);
    }
    //fprintf(stdout, "%s::%d k:%d ret:%d/%d\n", __FILE__, __LINE__, k, ret, z);
    return ret;
}

int dkv_ins(DKV *dkv, double *keys, int nkeys, u32_t *list)
{
    int ret = 0, i = 0, n = 0;
    u32_t *plist = list;

    if(dkv && dkv->state && keys && nkeys > 0)
    {
       for(i = 0; i < nkeys; i++) 
       {
            n = dkv_in(dkv, keys[i], plist);
            if(list) plist += n;
            ret += n;
       }
    }
    return ret;
}
int dkv_del(DKV *dkv, u32_t no)
{
    int ret = -1, n = 0;

    if(dkv)
    {
        RWLOCK_WRLOCK(dkv->rwlock);
        if((n = (dkv->vsize/sizeof(DVVV))) > 0 && no < n)
        {
            dkv_remove(dkv, no);
            dkv->vmap[no].off = -1;
            ret = 0;
        }
        RWLOCK_UNLOCK(dkv->rwlock);
    }
    return ret;
}
#endif
int dkv_get(DKV *dkv, u32_t no, u32_t *val)
{
    int ret = -1, n = 0;

    if(dkv)
    {
        RWLOCK_RDLOCK(dkv->rwlock);
        if((n = (dkv->vsize/sizeof(DVVV))) > 0 && no < n)
        {
            if(val) *val = dkv->vmap[no].val;
            ret = 0;
        }
        RWLOCK_UNLOCK(dkv->rwlock);
    }
    return ret;
}

int dkv_set(DKV *dkv, u32_t no, double key)
{
    int ret = -1;

    if(dkv)
    {
       RWLOCK_WRLOCK(dkv->rwlock);
       dkv_vset(dkv, no, key);
#ifdef __DKV_USE_IDX__
       if(dkv->vmap[no].off  < 0)
       {
           dkv_insert(dkv, no, key);
       }
       else
       {
           if(key != dkv->vmap[no].val)
           {
                dkv_remove(dkv, no);
                dkv_insert(dkv, no, key);
           }
       }
#endif
       dkv->vmap[no].val = key;
       ret = 0;
       RWLOCK_UNLOCK(dkv->rwlock);
    }
    return ret;
}


void dkv_close(DKV *dkv)
{
    if(dkv)
    {
        if(dkv->state) munmap(dkv->state, dkv->msize);
        if(dkv->fd) close(dkv->fd);
        if(dkv->vmap) munmap(dkv->vmap, dkv->vmsize);
        if(dkv->vfd) close(dkv->vfd);
        RWLOCK_DESTROY(dkv->rwlock);
        free(dkv);
    }
    return ;
}

#ifdef DKV_TEST
#include "timer.h"
#define MASK  120000
//rm -rf /tmp/1.idx* && gcc -O2 -o dkv dkv.c -DDKV_TEST -DTEST_KV -DHAVE_PTHREAD -lpthread && ./dkv
int main()
{
    DKV *dkv = NULL;
    int i = 0, j = 0, n = 0, total = 0, no = 0, stat[MASK], stat2[MASK],
        v = 0, num = 1000000, base = 60000000;
    double val = 0, from = 0, to = 0, *res = NULL, all_mask = 200000;
    double inputs[256], nos[256], last[256], tall[200000];
    double all = 0;
    time_t stime = 0, etime = 0;
    void *timer = NULL;

    if((dkv = dkv_init("/tmp/1.idx")))
    {
        res = (double *)calloc(base, sizeof(double));
        TIMER_INIT(timer);
#ifdef TEST_KV
        TIMER_RESET(timer);
        for(i = 0; i < num; i++)
        {
            no = random()%base;
            DKV_SET(dkv, no, i);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "set(%d) time used:%lld\n", num, PT_LU_USEC(timer));
        TIMER_RESET(timer);
        for(i = 0; i < num; i++)
        {
            no = random()%base;
            v = DKV_GET(dkv, no);
        }
        fprintf(stdout, "get(%d) time used:%lld\n", num, PT_LU_USEC(timer));
#endif

#ifdef TEST_RFROM
            dkv_set(dkv, 1, 22);
            dkv_set(dkv, 2, 25);
            dkv_set(dkv, 3, 20);
            dkv_set(dkv, 4, 13);
            dkv_set(dkv, 6, 22);
            n = dkv_rangefrom(dkv, 21, NULL);
            fprintf(stdout, "rangefrom(21) => %d\n", n);
#endif
#ifdef TEST_DEB
        /*
            n = dkv_in(dkv, 16615, NULL);
            fprintf(stdout, "16615:%d\n", n);
        */
        n = 0;
        for(i = 0; i < dkv->state->count; i++)
        {
            /*
            if(dkv->slots[i].min <= 26650 && dkv->slots[i].max >= 26650)
            {
                fprintf(stdout, "%d:[min:%d max:%d]\n", i, dkv->slots[i].min, dkv->slots[i].max);
            }
            */
            n+= dkv->slots[i].count;
        }
        fprintf(stdout, "total:%d\n", n);
#endif
#ifdef TEST_IN
        for(i = 0; i < all_mask; i++)
        {
            tall[i] = 0;
        }
        for(i = 80000000; i > 0; i--)
        {
            no = (rand()%all_mask);
            dkv_set(dkv, i, no);
            tall[no]++;
        }
        for(i = 0; i < all_mask; i++)
        {
            n = dkv_in(dkv, i, NULL);
            if(n != tall[i])
                fprintf(stdout, "%d:[%d/%d]\n", i, n, tall[i]);
        }
#endif
#ifdef TEST_ALL
        for(i = 0; i < all_mask; i++)
        {
            tall[i] = 0;
        }
        for(i = 80000000; i > 0; i--)
        {
            no = (rand()%all_mask);
            dkv_set(dkv, i, no);
            tall[no]++;
        }
        no = (rand()%all_mask);
        total = 0;for(i = no; i < all_mask; i++) total += tall[i];
        fprintf(stdout, "rangefrom(%d):%d/%d\n", no, dkv_rangefrom(dkv, no, NULL), total);
        total = 0;for(i = 0; i <= no; i++) total += tall[i];
        fprintf(stdout, "rangeto(%d):%d/%d\n", no, dkv_rangeto(dkv, no, NULL), total);
        from = (rand()%all_mask);
        to = (rand()%all_mask);
        if(from > to){from += to; to = from - to; from = from - to;}
        total = 0;for(i = from; i <= to; i++) total += tall[i];
        fprintf(stdout, "range(%d,%d):%d/%d\n", from, no, dkv_range(dkv, from, to, NULL), total);
        for(i = 0; i < all_mask; i++)
        {
            n = dkv_in(dkv, i, NULL);
            if(n != tall[i])
                fprintf(stdout, "%d:[%d/%d]\n", i, n, tall[i]);
        }
#endif
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
           dkv_set(dkv, i, val);
           last[j] = i;
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "set() 40000000 data, time used:%lld\n", PT_LU_USEC(timer));
        TIMER_RESET(timer);
        for(i = 0; i < n; i++)
        {
            dkv_del(dkv, last[i]);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "del() time used:%lld\n", PT_LU_USEC(timer));
        TIMER_RESET(timer);
        for(i = 0; i < n; i++)
        {
            val = inputs[i];
            no = nos[i];
            stat2[no] = dkv_in(dkv, val, res);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "in() time used:%lld\n", PT_LU_USEC(timer));
        TIMER_RESET(timer);
        total = dkv_ins(dkv, inputs, n, NULL);
        TIMER_SAMPLE(timer);
        fprintf(stdout, "ins(keys, NULL) total:%d time used:%lld\n", total, PT_LU_USEC(timer));
        TIMER_RESET(timer);
        total = dkv_ins(dkv, inputs, n, res);
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
        for(i = 0; i < dkv->state->count; i++)
        {
            fprintf(stdout, "%d:{min:%d max:%d}(%d)\n", i, dkv->slots[i].min, dkv->slots[i].max, dkv->slots[i].count);
        }
        */
#ifdef TEST_RANGEFILTER
            dkv_set(dkv, 1, 1234567);
            dkv_set(dkv, 2, 1567890);
            fprintf(stdout, "rangefrom():%d\n", dkv_rangefrom(dkv, 1569000, NULL));
            fprintf(stdout, "rangeto():%d\n", dkv_rangeto(dkv, 1111111, NULL));
            fprintf(stdout, "range():%d\n", dkv_range(dkv, 1111111, 1400000, NULL));
#endif
#ifdef TEST_RANGE
        srand(time(NULL));
        TIMER_RESET(timer);
        for(i = 1; i < 40000000; i++)
        {
            val = 1356969600 + (rand()%31536000);
            dkv_set(dkv, i, val);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "set() 40000000 timestamps,  time used:%lld\n", PT_LU_USEC(timer));
        srand(time(NULL));
        TIMER_RESET(timer);
        all = 0;
        for(i = 0; i < 1000; i++)
        {
            val = 1356969600 + (rand()%31536000);
            all += dkv_rangefrom(dkv, val, res);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "rangefrom() 1000 times total:%lld, time used:%lld\n", (long long int)all, PT_LU_USEC(timer));
        srand(time(NULL));
        TIMER_RESET(timer);
        all = 0;
        for(i = 0; i < 1000; i++)
        {
            val = 1356969600 + (rand()%31536000);
            all += dkv_rangeto(dkv, val, res);
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
            all += dkv_range(dkv, from, to, res);
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
            all += dkv_range(dkv, from, to, NULL);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "range(null) 1000 times total:%lld, time used:%lld\n", (long long int)all, PT_LU_USEC(timer));

#endif
        dkv_close(dkv);
        TIMER_CLEAN(timer);
        free(res);
    }
}
#endif
