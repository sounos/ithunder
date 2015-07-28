#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include "lkv.h"
#include "rwlock.h"
LKV *lkv_init(char *file)
{
    LKV *lkv = NULL;
    char path[1024];
    struct stat st = {0};
    off_t size = 0;
    int i = 0;

    if(file && (lkv = (LKV *)calloc(1, sizeof(LKV))))
    {
#ifdef __LKV_USE_IDX__
       if((lkv->fd = open(file, O_CREAT|O_RDWR, 0644)) > 0) 
       {
           size = lkv->msize = (off_t)sizeof(LVVSTATE) + (off_t)sizeof(LVVKV) * (off_t)LVV_NODES_MAX;

           lkv->state = (LVVSTATE*)mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, lkv->fd, 0);
           lkv->map = (LVVKV *)((char *)lkv->state + sizeof(LVVSTATE));
           fstat(lkv->fd, &st);
           lkv->size = st.st_size;
           if(st.st_size < sizeof(LVVSTATE))
           {
                i = ftruncate(lkv->fd, sizeof(LVVSTATE));
                memset(lkv->state, 0, sizeof(LVVSTATE));
                lkv->size = sizeof(LVVSTATE);
                for(i = 0; i < LVV_SLOT_MAX; i++)
                {
                   lkv->state->slots[i].nodeid = -1;
                }
           }
           lkv->roots = lkv->state->roots;
           lkv->slots = lkv->state->slots;
           //fprintf(stdout, "size:%lld/%d\n", lkv->size, sizeof(LVVSTATE));
       }
       else
       {
           fprintf(stderr, "open %s failed, %s\n", file, strerror(errno));
       }
#endif
       sprintf(path, "%s.v", file);
       if((lkv->vfd = open(path, O_CREAT|O_RDWR, 0644)) > 0) 
       {
            size = lkv->vmsize = (off_t)sizeof(LVVV) *  (off_t)LVV_NODES_MAX;
            lkv->vmap = (LVVV *)mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, lkv->vfd, 0);
            fstat(lkv->vfd, &st);
            lkv->vsize = st.st_size;
       }
       else
       {
           fprintf(stderr, "open %s failed, %s\n", file, strerror(errno));
       }
       RWLOCK_INIT(lkv->rwlock);
    }
    return lkv;
}

/* lkv set val */
int lkv_vset(LKV *lkv, u32_t no, int64_t val)
{
    off_t size = (off_t)((no / LVV_LVVV_INC) + 1) 
            * (off_t)LVV_LVVV_INC * (off_t) sizeof(LVVV);
    int ret = -1, n = 0, i = 0;

    if(lkv && no >= 0 && no < LVV_NODES_MAX)
    {
        if(size > lkv->vsize)
        {
            n = ftruncate(lkv->vfd, size);
            //memset(((char *)lkv->vmap+lkv->vsize), 0, size - lkv->vsize);
            i = lkv->vsize / sizeof(LVVV);
            n = size / sizeof(LVVV);
#ifdef __LKV_USE_IDX__
            while(i < n) {lkv->vmap[i].off = -1;lkv->vmap[i].val=0;++i;}
#endif
            lkv->vsize = size;
        }
        ret = 0;
    }
    return ret;
}

/* lkv get val */
int lkv_vget(LKV *lkv, u32_t no, int64_t *val)
{
    int ret = -1, n = 0;

    if((n = (lkv->vsize/sizeof(LVVV))) > 0 && no >= 0 && no < n)
    {
        if(val) *val = lkv->vmap[no].val; 
        ret = 0;
    }
    return ret;
}

#ifdef __LKV_USE_IDX__
/* new bolt  */
int lkv_slot_new(LKV *lkv)
{
    int ret = -1, i = 0, n = 0, id = 0;
    off_t size = 0;

    if(lkv && lkv->state)
    {
        if(lkv->state->nleft > 0)
        {
            n = --(lkv->state->nleft);
            ret = lkv->state->qleft[n];
        }
        else
        {
            size = (off_t)sizeof(LVVKV) * (off_t)LVV_SLOT_NUM * LVV_SLOT_INC + lkv->size; 
            n = ftruncate(lkv->fd, size);
            memset(((char *)lkv->state+lkv->size), 0, (size - lkv->size));
            ret = (lkv->size - (off_t)sizeof(LVVSTATE)) / (off_t)sizeof(LVVKV);
            id = ret + LVV_SLOT_NUM;
            i = 1;
            while(i < LVV_SLOT_INC)
            {
                lkv->state->qleft[(lkv->state->nleft++)] = id;
                id += LVV_SLOT_NUM;
                ++i;
            }
            lkv->size = size;
        }
    }
    return ret;
}

int lkv_insert(LKV *lkv, u32_t no, int64_t key)
{
    int ret = -1, i = 0, k = -1, nodeid = 0, pos = 0, num = 0, 
        n = 0, x = 0, min = 0, max = 0, m = 0, num2 = 0;
    LVVKV *kvs = NULL, *kv = NULL, *kv1 = NULL, *kv2 = NULL;
    LVVV *vnodes = NULL;

    if(lkv && lkv->state && (vnodes = lkv->vmap))
    {
        if((n = lkv->state->count) > 0) 
        {
            max = n - 1;
            min = 0;
            if(key <= lkv->slots[min].max) k = min;
            else if(key >= lkv->slots[max].min) k = max;
            else
            {
                while(max > min)
                {
                    x = (min + max) / 2;
                    if(x == min)
                    {
                        if(key >= lkv->slots[max].min) k = max;
                        else k = x;
                        break;
                    }
                    if(key >=  lkv->slots[x].min && (key <= lkv->slots[x].max
                                || (x < (n - 1) && key <= lkv->slots[x+1].min)))
                    {
                        k = x;
                        break;
                    }
                    else if(key > lkv->slots[x].max) min = x;
                    else max = x;
                }
            }
        }
        if(k >= 0 && k < n && lkv->slots[k].count ==  LVV_SLOT_NUM
                && lkv->slots[k].max == key)
        {
            while(k < (n - 1) && lkv->slots[k].count ==  LVV_SLOT_NUM
                    && key == lkv->slots[k+1].min)++k;
        }
        /* 未满的slot 直接插入 */
        if(k >= 0 && k < n && lkv->slots[k].count < LVV_SLOT_NUM)
        {
            m = x = lkv->slots[k].count++;
            kvs = lkv->map + lkv->slots[k].nodeid;
            while(x > 0 && key < kvs[x-1].key)
            {
                kvs[x].key = kvs[x-1].key;
                kvs[x].val = kvs[x-1].val;
                lkv->vmap[(kvs[x].val)].off = (lkv->slots[k].nodeid + x);
                --x;
            }
            kvs[x].key = key;
            kvs[x].val = no;
            lkv->vmap[no].off = (lkv->slots[k].nodeid + x);
            lkv->slots[k].min = kvs[0].key;
            lkv->slots[k].max = kvs[m].key;
        }
        else
        {
            nodeid = lkv_slot_new(lkv);
            /* slot已满转移元素到新的slot */
            if(k >= 0 && k < n && lkv->slots[k].count == LVV_SLOT_NUM) 
            {
                kv1 = lkv->map + lkv->slots[k].nodeid + LVV_SLOT2_NUM;
                kv2 = lkv->map + nodeid;
                num = LVV_SLOT2_NUM;
                if(key < kv1[0].key)
                {
                    /* 转移尾部一半元素到新slot */
                    for(i = 0; i < LVV_SLOT2_NUM; i++)
                    {
                        kv2[i].key = kv1[i].key;
                        kv2[i].val = kv1[i].val;
                        lkv->vmap[(kv2[i].val)].off = (nodeid + i);
                    }
                    /* 插入到本slot */
                    kvs = lkv->map + lkv->slots[k].nodeid;
                    i = LVV_SLOT2_NUM;
                    while(i > 0 && key < kvs[i-1].key)
                    {
                        kvs[i].key = kvs[i-1].key;
                        kvs[i].val = kvs[i-1].val;
                        lkv->vmap[(kvs[i].val)].off = (lkv->slots[k].nodeid + i);
                        --i;
                    }
                    kvs[i].key = key;
                    kvs[i].val = no;
                    lkv->vmap[(kvs[i].val)].off = (lkv->slots[k].nodeid + i);
                    num = LVV_SLOT2_NUM;
                    num2 = LVV_SLOT2_NUM + 1;
                }
                else
                {
                    i = 0;
                    x = 0;
                    while(i < LVV_SLOT2_NUM)
                    {
                        if(key <= kv1[i].key && x == i)
                        {
                            kv2->key = key;
                            kv2->val = no;
                            lkv->vmap[(kv2->val)].off = (nodeid + x);
                            ++x;
                            ++kv2;
                        }
                        kv2->key = kv1[i].key;
                        kv2->val = kv1[i].val;
                        lkv->vmap[(kv2->val)].off = (nodeid + x);
                        ++kv2;
                        ++x;
                        ++i;
                    }
                    if(x == i)
                    {
                        kv2->key = key;
                        kv2->val = no;
                        lkv->vmap[(kv2->val)].off = (nodeid + x);
                    }
                    num = LVV_SLOT2_NUM + 1;
                    num2 = LVV_SLOT2_NUM;
                }
                /* rebuild slot */
                kvs = lkv->map + lkv->slots[k].nodeid;
                lkv->slots[k].count = num2;
                lkv->slots[k].min = kvs[0].key; 
                lkv->slots[k].max = kvs[num2-1].key; 
                pos = k+1;
                k = lkv->state->count++; 
                while(k > pos)
                {
                    memcpy(&(lkv->slots[k]), &(lkv->slots[k-1]), sizeof(LVVSLOT));
                    x = (lkv->slots[k].nodeid / LVV_SLOT_NUM);
                    lkv->roots[x] = k;
                    --k;
                }
            }
            else
            {
                /* 插入新的slot */
                kv = lkv->map + nodeid;
                kv->key = key;
                kv->val = no;
                lkv->vmap[no].off = nodeid;
                num = 1;
                m = k = lkv->state->count++; 
                while(k > 0 && key < lkv->slots[k-1].min)
                {
                    memcpy(&(lkv->slots[k]), &(lkv->slots[k-1]), sizeof(LVVSLOT));
                    x = (lkv->slots[k].nodeid / LVV_SLOT_NUM);
                    lkv->roots[x] = k;
                    --k;
                }
            }
            kv = lkv->map + nodeid;
            lkv->slots[k].min = kv[0].key;
            lkv->slots[k].max = kv[num-1].key;
            lkv->slots[k].nodeid = nodeid;
            lkv->slots[k].count = num;
            x = (lkv->slots[k].nodeid / LVV_SLOT_NUM);
            lkv->roots[x] = k;
        }
    }
    return ret;
}

int lkv_remove(LKV *lkv, u32_t no)
{
    int ret = -1, i = 0, x = 0, n = 0, slotid = 0;
    u32_t nodeid = 0, rootid = 0;
    LVVKV *kvs = NULL;

    if(lkv && lkv->state && lkv->vmap 
            && (n = lkv->vsize/sizeof(LVVV)) > 0 && no >= 0 && no < n
            && (nodeid = lkv->vmap[no].off) >= 0)
    {
        rootid = (nodeid / LVV_SLOT_NUM);
        slotid = (int)lkv->roots[rootid];
        if(slotid < 0) return ret;
        i = nodeid % LVV_SLOT_NUM;
        kvs = lkv->map + lkv->slots[slotid].nodeid;    
        while(i < (lkv->slots[slotid].count-1))
        {
            memcpy(&(kvs[i]), &(kvs[i+1]), sizeof(LVVKV));
            x = kvs[i].val;
            lkv->vmap[x].off = (i + lkv->slots[slotid].nodeid);
            ++i;
        }
        if(lkv->slots[slotid].count == 1)
        {
            i = slotid;
            lkv->state->qleft[(lkv->state->nleft++)] = lkv->slots[slotid].nodeid;
            lkv->roots[rootid] = -1;
            while(i < (lkv->state->count-1))
            {
                memcpy(&(lkv->slots[i]), &(lkv->slots[i+1]), sizeof(LVVSLOT));
                rootid = (lkv->slots[i].nodeid / LVV_SLOT_NUM);
                lkv->roots[rootid] = i;
                ++i;
            }
            --(lkv->state->count);
        }
        else
        {
            n = --(lkv->slots[slotid].count);
            lkv->slots[slotid].min = kvs[0].key;
            lkv->slots[slotid].max = kvs[n - 1].key;
        }
        lkv->vmap[no].off = -1;
    }
    return ret;
}

int lkv_find_slot(LKV *lkv, int64_t key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    if(lkv && (n = lkv->state->count) > 0)
    {
        max = n - 1;
        min = 0;
        if(key <= lkv->slots[min].max) ret = min;
        else if(key > lkv->slots[max].min) ret = max;
        else
        {
            while(max > min)
            {
                x = (min + max) / 2;
                if(x == min)
                {
                    if(key <= lkv->slots[x].max) ret = x;
                    else if(key <= lkv->slots[max].max) ret = max;
                    break;
                }
                if(key >=  lkv->slots[x].min && key <= lkv->slots[x].max)
                            //|| (x < (n - 1) && key <= lkv->slots[x+1].min)))
                {
                    ret = x;
                    break;
                }
                else if(key > lkv->slots[x].max) min = x;
                else max = x;
            }
        }
        if((x = ret)>= 0 && x < n)
        {
            while(x >= 0 && key <= lkv->slots[x].max)
            {
                ret = x--;
            }
        }
    }
    return ret;
}

int lkv_find_slot2(LKV *lkv, int64_t key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    if(lkv && (n = lkv->state->count) > 0)
    {
        max = n - 1;
        min = 0;
        if(key >= lkv->slots[max].min) ret = max;
        else if(key < lkv->slots[min].max) ret = min;
        else
        {
            while(max > min)
            {
                x = (min + max) / 2;
                if(x == min)
                {
                    if(key >= lkv->slots[max].min) ret = max;
                    else if(key >= lkv->slots[x].min) ret = x;
                    break;
                }
                if(key >=  lkv->slots[x].min && key <= lkv->slots[x].max) 
                            //|| (x < (n - 1) && key <= lkv->slots[x+1].min)))
                {
                    ret = x;
                    break;
                }
                else if(key > lkv->slots[x].max) min = x;
                else max = x;
            }
        }
        if((x = ret)>= 0 && x < n)
        {
            while(x < n && key >= lkv->slots[x].min) 
            {
                ret = x++;
            }
        }
    }
    return ret;
}

int lkv_find_kv(LKV *lkv, int k, int64_t key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    LVVKV *kvs = NULL;

    if(lkv && k >= 0 && k < lkv->state->count 
            && (n = lkv->slots[k].count) > 0)
    {
        kvs = lkv->map + lkv->slots[k].nodeid;
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
        //fprintf(stdout, "find_kv(%d) min:%d max:%d count:%d ret:%d\n", key, lkv->slots[k].min, lkv->slots[k].max, lkv->slots[k].count, ret);
    }
    return ret;
}

int lkv_find_kv2(LKV *lkv, int k, int64_t key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    LVVKV *kvs = NULL;

    if(lkv && k >= 0 && k < lkv->state->count 
            && (n = lkv->slots[k].count) > 0)
    {
        kvs = lkv->map + lkv->slots[k].nodeid;
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

int lkv_in(LKV *lkv, int64_t key, u32_t *list)
{
    int i = 0, k = 0, z = 0, ret = 0, n = 0;
    LVVKV *kvs = NULL;

    if(lkv && lkv->state && (n = lkv->state->count) > 0)
    {
        RWLOCK_RDLOCK(lkv->rwlock);
        k = lkv_find_slot(lkv, key);
        i = lkv_find_kv(lkv, k, key);
        do
        {
            kvs = lkv->map + lkv->slots[k].nodeid;
            if(i >= 0 && key == kvs[i].key && i < lkv->slots[k].count)
            {
                if(key == lkv->slots[k].max)
                {
                    ret += lkv->slots[k].count - i;
                    if(list)
                    {
                        while(i < lkv->slots[k].count) list[z++] = kvs[i++].val;
                    }
                }
                else
                {
                    while(i < lkv->slots[k].count && key == kvs[i].key)
                    {
                        if(list)list[z++] = kvs[i].val;
                        ++ret;
                        ++i;
                    }
                }
            }
            i=0;
        }while(++k < n && lkv->slots[k].min == key);
        RWLOCK_UNLOCK(lkv->rwlock);
    }
    return ret;
}

int lkv_range(LKV *lkv, int64_t from, int64_t to, u32_t *list)
{
    int i = 0, ii = 0, k = 0, kk = 0, j = 0, x = 0, z = 0, ret = 0, n = 0;
    LVVKV *kvs = NULL;

    if(lkv && lkv->state)
    {
        if(from == to) return lkv_in(lkv, from, list);
        RWLOCK_RDLOCK(lkv->rwlock);
        k = lkv_find_slot(lkv, from);
        kk = lkv_find_slot2(lkv, to);
        i = lkv_find_kv(lkv, k, from);
        ii = lkv_find_kv2(lkv, kk, to);
        if(k == -1 || kk == -1 || i == -1 || ii == -1) goto end;
        if(k == kk)
        {
            ret = ii + 1 - i;
            if(list)
            {
                kvs = lkv->map + lkv->slots[k].nodeid;
                for(x = i; x <= ii; x++) list[z++] = kvs[x].val;
            }
        }
        else
        {
            n =  lkv->slots[k].count;
            ret = n - i;
            if(list)
            {
                kvs = lkv->map + lkv->slots[k].nodeid;
                for(x = i; x < n; x++) list[z++] = kvs[x].val;
            }
            for(j = k+1; j < kk; j++)
            {
                ret += lkv->slots[j].count;
                if(list)
                {
                    kvs = lkv->map + lkv->slots[j].nodeid;
                    for(x = 0; x < lkv->slots[j].count; x++) list[z++] = kvs[x].val;
                }
            }
            ret += ii + 1;
            if(list)
            {
                kvs = lkv->map + lkv->slots[kk].nodeid;
                for(x = 0; x <= ii; x++) list[z++] = kvs[x].val;
            }
        }
end:
        RWLOCK_UNLOCK(lkv->rwlock);
    }
    return ret;
}

int lkv_rangefrom(LKV *lkv, int64_t key, u32_t *list) /* key = from */
{
    int i = 0, k = 0, x = 0, z = 0, ret = 0, n = 0;
    LVVKV *kvs = NULL;

    if(lkv && lkv->state)
    {
        RWLOCK_RDLOCK(lkv->rwlock);
        if((k = lkv_find_slot(lkv, key)) >= 0 && (i = lkv_find_kv(lkv, k, key)) >= 0)
        {
            //fprintf(stdout, "k:%d i:%d\n", k, i);
            kvs = lkv->map + lkv->slots[k].nodeid;
            n =  lkv->slots[k].count;
            if(list)
            {
                for(x = i; x < n; x++) list[z++] = kvs[x].val;
            }
            ret = n - i;
            for(i = k + 1; i <  lkv->state->count; i++)
            {
                ret += lkv->slots[i].count;
                if(list)
                {
                    kvs = lkv->map + lkv->slots[i].nodeid;
                    n = lkv->slots[i].count;
                    for(x = 0; x < n; x++) list[z++] = kvs[x].val;
                }
            }
        }
        RWLOCK_UNLOCK(lkv->rwlock);
    }
    //fprintf(stdout, "%s::%d k:%d ret:%d/%d\n", __FILE__, __LINE__, k, ret, z);
    return ret;
}

int lkv_rangeto(LKV *lkv, int64_t key, u32_t *list) /* key = to */
{
    int i = 0, k = 0, x = 0, j = 0, z = 0, ret = 0, n = 0;
    LVVKV *kvs = NULL;

    if(lkv && lkv->state && (n = (lkv->state->count)) > 0)
    {
        RWLOCK_RDLOCK(lkv->rwlock);
        if((k = lkv_find_slot2(lkv, key)) >= 0 && k < n 
                && (i = lkv_find_kv2(lkv, k, key)) >= 0)
        {
            for(j = 0; j < k; j++)
            {
                ret += lkv->slots[j].count;
                if(list)
                {
                    kvs = lkv->map + lkv->slots[j].nodeid;
                    for(x = 0; x < lkv->slots[j].count; x++)
                    {
                        list[z++] = kvs[x].val;
                    }
                }
            }
            ret += i + 1;
            if(list)
            {
                kvs = lkv->map + lkv->slots[k].nodeid;
                for(x = 0; x <= i; x++)
                {
                    list[z++] = kvs[x].val;
                }
            }
        }
        RWLOCK_UNLOCK(lkv->rwlock);
    }
    //fprintf(stdout, "%s::%d k:%d ret:%d/%d\n", __FILE__, __LINE__, k, ret, z);
    return ret;
}

int lkv_ins(LKV *lkv, int64_t *keys, int nkeys, u32_t *list)
{
    int ret = 0, i = 0, n = 0;
    u32_t *plist = list;

    if(lkv && lkv->state && keys && nkeys > 0)
    {
       for(i = 0; i < nkeys; i++) 
       {
            n = lkv_in(lkv, keys[i], plist);
            if(list) plist += n;
            ret += n;
       }
    }
    return ret;
}
int lkv_del(LKV *lkv, u32_t no)
{
    int ret = -1, n = 0;

    if(lkv)
    {
        RWLOCK_WRLOCK(lkv->rwlock);
        if((n = (lkv->vsize/sizeof(LVVV))) > 0 && no < n)
        {
            lkv_remove(lkv, no);
            lkv->vmap[no].off = -1;
            ret = 0;
        }
        RWLOCK_UNLOCK(lkv->rwlock);
    }
    return ret;
}
#endif
int lkv_get(LKV *lkv, u32_t no, u32_t *val)
{
    int ret = -1, n = 0;

    if(lkv)
    {
        RWLOCK_RDLOCK(lkv->rwlock);
        if((n = (lkv->vsize/sizeof(LVVV))) > 0 && no < n)
        {
            if(val) *val = lkv->vmap[no].val;
            ret = 0;
        }
        RWLOCK_UNLOCK(lkv->rwlock);
    }
    return ret;
}

int lkv_set(LKV *lkv, u32_t no, int64_t key)
{
    int ret = -1;

    if(lkv)
    {
       RWLOCK_WRLOCK(lkv->rwlock);
       lkv_vset(lkv, no, key);
#ifdef __LKV_USE_IDX__
       if(lkv->vmap[no].off  < 0)
       {
           lkv_insert(lkv, no, key);
       }
       else
       {
           if(key != lkv->vmap[no].val)
           {
                lkv_remove(lkv, no);
                lkv_insert(lkv, no, key);
           }
       }
#endif
       lkv->vmap[no].val = key;
       ret = 0;
       RWLOCK_UNLOCK(lkv->rwlock);
    }
    return ret;
}


void lkv_close(LKV *lkv)
{
    if(lkv)
    {
        if(lkv->state) munmap(lkv->state, lkv->msize);
        if(lkv->fd) close(lkv->fd);
        if(lkv->vmap) munmap(lkv->vmap, lkv->vmsize);
        if(lkv->vfd) close(lkv->vfd);
        RWLOCK_DESTROY(lkv->rwlock);
        free(lkv);
    }
    return ;
}

#ifdef LKV_TEST
#include "timer.h"
#define MASK  120000
//rm -rf /tmp/1.idx* && gcc -O2 -o lkv lkv.c -DLKV_TEST -DTEST_KV -DHAVE_PTHREAD -lpthread && ./lkv
int main()
{
    LKV *lkv = NULL;
    int i = 0, j = 0, n = 0, total = 0, no = 0, stat[MASK], stat2[MASK],
        v = 0, num = 1000000, base = 60000000;
    int64_t val = 0, from = 0, to = 0, *res = NULL, all_mask = 200000;
    int64_t inputs[256], nos[256], last[256], tall[200000];
    int64_t all = 0;
    time_t stime = 0, etime = 0;
    void *timer = NULL;

    if((lkv = lkv_init("/tmp/1.idx")))
    {
        res = (int64_t *)calloc(base, sizeof(int64_t));
        TIMER_INIT(timer);
#ifdef TEST_KV
        TIMER_RESET(timer);
        for(i = 0; i < num; i++)
        {
            no = random()%base;
            LKV_SET(lkv, no, i);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "set(%d) time used:%lld\n", num, PT_LU_USEC(timer));
        TIMER_RESET(timer);
        for(i = 0; i < num; i++)
        {
            no = random()%base;
            v = LKV_GET(lkv, no);
        }
        fprintf(stdout, "get(%d) time used:%lld\n", num, PT_LU_USEC(timer));
#endif

#ifdef TEST_RFROM
            lkv_set(lkv, 1, 22);
            lkv_set(lkv, 2, 25);
            lkv_set(lkv, 3, 20);
            lkv_set(lkv, 4, 13);
            lkv_set(lkv, 6, 22);
            n = lkv_rangefrom(lkv, 21, NULL);
            fprintf(stdout, "rangefrom(21) => %d\n", n);
#endif
#ifdef TEST_DEB
        /*
            n = lkv_in(lkv, 16615, NULL);
            fprintf(stdout, "16615:%d\n", n);
        */
        n = 0;
        for(i = 0; i < lkv->state->count; i++)
        {
            /*
            if(lkv->slots[i].min <= 26650 && lkv->slots[i].max >= 26650)
            {
                fprintf(stdout, "%d:[min:%d max:%d]\n", i, lkv->slots[i].min, lkv->slots[i].max);
            }
            */
            n+= lkv->slots[i].count;
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
            lkv_set(lkv, i, no);
            tall[no]++;
        }
        for(i = 0; i < all_mask; i++)
        {
            n = lkv_in(lkv, i, NULL);
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
            lkv_set(lkv, i, no);
            tall[no]++;
        }
        no = (rand()%all_mask);
        total = 0;for(i = no; i < all_mask; i++) total += tall[i];
        fprintf(stdout, "rangefrom(%d):%d/%d\n", no, lkv_rangefrom(lkv, no, NULL), total);
        total = 0;for(i = 0; i <= no; i++) total += tall[i];
        fprintf(stdout, "rangeto(%d):%d/%d\n", no, lkv_rangeto(lkv, no, NULL), total);
        from = (rand()%all_mask);
        to = (rand()%all_mask);
        if(from > to){from += to; to = from - to; from = from - to;}
        total = 0;for(i = from; i <= to; i++) total += tall[i];
        fprintf(stdout, "range(%d,%d):%d/%d\n", from, no, lkv_range(lkv, from, to, NULL), total);
        for(i = 0; i < all_mask; i++)
        {
            n = lkv_in(lkv, i, NULL);
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
           lkv_set(lkv, i, val);
           last[j] = i;
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "set() 40000000 data, time used:%lld\n", PT_LU_USEC(timer));
        TIMER_RESET(timer);
        for(i = 0; i < n; i++)
        {
            lkv_del(lkv, last[i]);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "del() time used:%lld\n", PT_LU_USEC(timer));
        TIMER_RESET(timer);
        for(i = 0; i < n; i++)
        {
            val = inputs[i];
            no = nos[i];
            stat2[no] = lkv_in(lkv, val, res);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "in() time used:%lld\n", PT_LU_USEC(timer));
        TIMER_RESET(timer);
        total = lkv_ins(lkv, inputs, n, NULL);
        TIMER_SAMPLE(timer);
        fprintf(stdout, "ins(keys, NULL) total:%d time used:%lld\n", total, PT_LU_USEC(timer));
        TIMER_RESET(timer);
        total = lkv_ins(lkv, inputs, n, res);
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
        for(i = 0; i < lkv->state->count; i++)
        {
            fprintf(stdout, "%d:{min:%d max:%d}(%d)\n", i, lkv->slots[i].min, lkv->slots[i].max, lkv->slots[i].count);
        }
        */
#ifdef TEST_RANGEFILTER
            lkv_set(lkv, 1, 1234567);
            lkv_set(lkv, 2, 1567890);
            fprintf(stdout, "rangefrom():%d\n", lkv_rangefrom(lkv, 1569000, NULL));
            fprintf(stdout, "rangeto():%d\n", lkv_rangeto(lkv, 1111111, NULL));
            fprintf(stdout, "range():%d\n", lkv_range(lkv, 1111111, 1400000, NULL));
#endif
#ifdef TEST_RANGE
        srand(time(NULL));
        TIMER_RESET(timer);
        for(i = 1; i < 40000000; i++)
        {
            val = 1356969600 + (rand()%31536000);
            lkv_set(lkv, i, val);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "set() 40000000 timestamps,  time used:%lld\n", PT_LU_USEC(timer));
        srand(time(NULL));
        TIMER_RESET(timer);
        all = 0;
        for(i = 0; i < 1000; i++)
        {
            val = 1356969600 + (rand()%31536000);
            all += lkv_rangefrom(lkv, val, res);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "rangefrom() 1000 times total:%lld, time used:%lld\n", (long long int)all, PT_LU_USEC(timer));
        srand(time(NULL));
        TIMER_RESET(timer);
        all = 0;
        for(i = 0; i < 1000; i++)
        {
            val = 1356969600 + (rand()%31536000);
            all += lkv_rangeto(lkv, val, res);
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
            all += lkv_range(lkv, from, to, res);
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
            all += lkv_range(lkv, from, to, NULL);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "range(null) 1000 times total:%lld, time used:%lld\n", (long long int)all, PT_LU_USEC(timer));

#endif
        lkv_close(lkv);
        TIMER_CLEAN(timer);
        free(res);
    }
}
#endif
