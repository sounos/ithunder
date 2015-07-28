#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include "ikv.h"
#include "rwlock.h"
IKV *ikv_init(char *file)
{
    IKV *ikv = NULL;
    char path[1024];
    struct stat st = {0};
    off_t size = 0;
    int i = 0;

    if(file && (ikv = (IKV *)calloc(1, sizeof(IKV))))
    {
#ifdef __IKV_USE_IDX__
       if((ikv->fd = open(file, O_CREAT|O_RDWR, 0644)) > 0) 
       {
           size = ikv->msize = (off_t)sizeof(IVVSTATE) + (off_t)sizeof(IVVKV) * (off_t)IVV_NODES_MAX;

           ikv->state = (IVVSTATE*)mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, ikv->fd, 0);
           ikv->map = (IVVKV *)((char *)ikv->state + sizeof(IVVSTATE));
           fstat(ikv->fd, &st);
           ikv->size = st.st_size;
           if(st.st_size < sizeof(IVVSTATE))
           {
                i = ftruncate(ikv->fd, sizeof(IVVSTATE));
                memset(ikv->state, 0, sizeof(IVVSTATE));
                ikv->size = sizeof(IVVSTATE);
                for(i = 0; i < IVV_SLOT_MAX; i++)
                {
                   ikv->state->slots[i].nodeid = -1;
                }
           }
           ikv->roots = ikv->state->roots;
           ikv->slots = ikv->state->slots;
           //fprintf(stdout, "size:%lld/%d\n", ikv->size, sizeof(IVVSTATE));
       }
       else
       {
           fprintf(stderr, "open %s failed, %s\n", file, strerror(errno));
       }
#endif
       sprintf(path, "%s.v", file);
       if((ikv->vfd = open(path, O_CREAT|O_RDWR, 0644)) > 0) 
       {
            size = ikv->vmsize = (off_t)sizeof(IVVV) *  (off_t)IVV_NODES_MAX;
            ikv->vmap = (IVVV *)mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, ikv->vfd, 0);
            fstat(ikv->vfd, &st);
            ikv->vsize = st.st_size;
       }
       else
       {
           fprintf(stderr, "open %s failed, %s\n", file, strerror(errno));
       }
       RWLOCK_INIT(ikv->rwlock);
    }
    return ikv;
}

/* ikv set val */
int ikv_vset(IKV *ikv, u32_t no, int32_t val)
{
    off_t size = (off_t)((no / IVV_IVVV_INC) + 1) 
            * (off_t)IVV_IVVV_INC * (off_t) sizeof(IVVV);
    int ret = -1, n = 0, i = 0;

    if(ikv && no >= 0 && no < IVV_NODES_MAX)
    {
        if(size > ikv->vsize)
        {
            n = ftruncate(ikv->vfd, size);
            //memset(((char *)ikv->vmap+ikv->vsize), 0, size - ikv->vsize);
            i = ikv->vsize / sizeof(IVVV);
            n = size / sizeof(IVVV);
#ifdef __IKV_USE_IDX__
            while(i < n) {ikv->vmap[i].off = -1;ikv->vmap[i].val=0;++i;}
#endif
            ikv->vsize = size;
        }
        ret = 0;
    }
    return ret;
}

/* ikv get val */
int ikv_vget(IKV *ikv, u32_t no, int32_t *val)
{
    int ret = -1, n = 0;

    if((n = (ikv->vsize/sizeof(IVVV))) > 0 && no >= 0 && no < n)
    {
        if(val) *val = ikv->vmap[no].val; 
        ret = 0;
    }
    return ret;
}

#ifdef __IKV_USE_IDX__
/* new bolt  */
int ikv_slot_new(IKV *ikv)
{
    int ret = -1, i = 0, n = 0, id = 0;
    off_t size = 0;

    if(ikv && ikv->state)
    {
        if(ikv->state->nleft > 0)
        {
            n = --(ikv->state->nleft);
            ret = ikv->state->qleft[n];
        }
        else
        {
            size = (off_t)sizeof(IVVKV) * (off_t)IVV_SLOT_NUM * IVV_SLOT_INC + ikv->size; 
            n = ftruncate(ikv->fd, size);
            memset(((char *)ikv->state+ikv->size), 0, (size - ikv->size));
            ret = (ikv->size - (off_t)sizeof(IVVSTATE)) / (off_t)sizeof(IVVKV);
            id = ret + IVV_SLOT_NUM;
            i = 1;
            while(i < IVV_SLOT_INC)
            {
                ikv->state->qleft[(ikv->state->nleft++)] = id;
                id += IVV_SLOT_NUM;
                ++i;
            }
            ikv->size = size;
        }
    }
    return ret;
}

int ikv_insert(IKV *ikv, u32_t no, int32_t key)
{
    int ret = -1, i = 0, k = -1, nodeid = 0, pos = 0, num = 0, 
        n = 0, x = 0, min = 0, max = 0, m = 0, num2 = 0;
    IVVKV *kvs = NULL, *kv = NULL, *kv1 = NULL, *kv2 = NULL;
    IVVV *vnodes = NULL;

    if(ikv && ikv->state && (vnodes = ikv->vmap))
    {
        if((n = ikv->state->count) > 0) 
        {
            max = n - 1;
            min = 0;
            if(key <= ikv->slots[min].max) k = min;
            else if(key >= ikv->slots[max].min) k = max;
            else
            {
                while(max > min)
                {
                    x = (min + max) / 2;
                    if(x == min)
                    {
                        if(key >= ikv->slots[max].min) k = max;
                        else k = x;
                        break;
                    }
                    if(key >=  ikv->slots[x].min && (key <= ikv->slots[x].max
                                || (x < (n - 1) && key <= ikv->slots[x+1].min)))
                    {
                        k = x;
                        break;
                    }
                    else if(key > ikv->slots[x].max) min = x;
                    else max = x;
                }
            }
        }
        if(k >= 0 && k < n && ikv->slots[k].count ==  IVV_SLOT_NUM
                && ikv->slots[k].max == key)
        {
            while(k < (n - 1) && ikv->slots[k].count ==  IVV_SLOT_NUM
                    && key == ikv->slots[k+1].min)++k;
        }
        /* 未满的slot 直接插入 */
        if(k >= 0 && k < n && ikv->slots[k].count < IVV_SLOT_NUM)
        {
            m = x = ikv->slots[k].count++;
            kvs = ikv->map + ikv->slots[k].nodeid;
            while(x > 0 && key < kvs[x-1].key)
            {
                kvs[x].key = kvs[x-1].key;
                kvs[x].val = kvs[x-1].val;
                ikv->vmap[(kvs[x].val)].off = (ikv->slots[k].nodeid + x);
                --x;
            }
            kvs[x].key = key;
            kvs[x].val = no;
            ikv->vmap[no].off = (ikv->slots[k].nodeid + x);
            ikv->slots[k].min = kvs[0].key;
            ikv->slots[k].max = kvs[m].key;
        }
        else
        {
            nodeid = ikv_slot_new(ikv);
            /* slot已满转移元素到新的slot */
            if(k >= 0 && k < n && ikv->slots[k].count == IVV_SLOT_NUM) 
            {
                kv1 = ikv->map + ikv->slots[k].nodeid + IVV_SLOT2_NUM;
                kv2 = ikv->map + nodeid;
                num = IVV_SLOT2_NUM;
                if(key < kv1[0].key)
                {
                    /* 转移尾部一半元素到新slot */
                    for(i = 0; i < IVV_SLOT2_NUM; i++)
                    {
                        kv2[i].key = kv1[i].key;
                        kv2[i].val = kv1[i].val;
                        ikv->vmap[(kv2[i].val)].off = (nodeid + i);
                    }
                    /* 插入到本slot */
                    kvs = ikv->map + ikv->slots[k].nodeid;
                    i = IVV_SLOT2_NUM;
                    while(i > 0 && key < kvs[i-1].key)
                    {
                        kvs[i].key = kvs[i-1].key;
                        kvs[i].val = kvs[i-1].val;
                        ikv->vmap[(kvs[i].val)].off = (ikv->slots[k].nodeid + i);
                        --i;
                    }
                    kvs[i].key = key;
                    kvs[i].val = no;
                    ikv->vmap[(kvs[i].val)].off = (ikv->slots[k].nodeid + i);
                    num = IVV_SLOT2_NUM;
                    num2 = IVV_SLOT2_NUM + 1;
                }
                else
                {
                    i = 0;
                    x = 0;
                    while(i < IVV_SLOT2_NUM)
                    {
                        if(key <= kv1[i].key && x == i)
                        {
                            kv2->key = key;
                            kv2->val = no;
                            ikv->vmap[(kv2->val)].off = (nodeid + x);
                            ++x;
                            ++kv2;
                        }
                        kv2->key = kv1[i].key;
                        kv2->val = kv1[i].val;
                        ikv->vmap[(kv2->val)].off = (nodeid + x);
                        ++kv2;
                        ++x;
                        ++i;
                    }
                    if(x == i)
                    {
                        kv2->key = key;
                        kv2->val = no;
                        ikv->vmap[(kv2->val)].off = (nodeid + x);
                    }
                    num = IVV_SLOT2_NUM + 1;
                    num2 = IVV_SLOT2_NUM;
                }
                /* rebuild slot */
                kvs = ikv->map + ikv->slots[k].nodeid;
                ikv->slots[k].count = num2;
                ikv->slots[k].min = kvs[0].key; 
                ikv->slots[k].max = kvs[num2-1].key; 
                pos = k+1;
                k = ikv->state->count++; 
                while(k > pos)
                {
                    memcpy(&(ikv->slots[k]), &(ikv->slots[k-1]), sizeof(IVVSLOT));
                    x = (ikv->slots[k].nodeid / IVV_SLOT_NUM);
                    ikv->roots[x] = k;
                    --k;
                }
            }
            else
            {
                /* 插入新的slot */
                kv = ikv->map + nodeid;
                kv->key = key;
                kv->val = no;
                ikv->vmap[no].off = nodeid;
                num = 1;
                m = k = ikv->state->count++; 
                while(k > 0 && key < ikv->slots[k-1].min)
                {
                    memcpy(&(ikv->slots[k]), &(ikv->slots[k-1]), sizeof(IVVSLOT));
                    x = (ikv->slots[k].nodeid / IVV_SLOT_NUM);
                    ikv->roots[x] = k;
                    --k;
                }
            }
            kv = ikv->map + nodeid;
            ikv->slots[k].min = kv[0].key;
            ikv->slots[k].max = kv[num-1].key;
            ikv->slots[k].nodeid = nodeid;
            ikv->slots[k].count = num;
            x = (ikv->slots[k].nodeid / IVV_SLOT_NUM);
            ikv->roots[x] = k;
        }
    }
    return ret;
}

int ikv_remove(IKV *ikv, u32_t no)
{
    int ret = -1, i = 0, x = 0, n = 0, slotid = 0;
    u32_t nodeid = 0, rootid = 0;
    IVVKV *kvs = NULL;

    if(ikv && ikv->state && ikv->vmap 
            && (n = ikv->vsize/sizeof(IVVV)) > 0 && no >= 0 && no < n
            && (nodeid = ikv->vmap[no].off) >= 0)
    {
        rootid = (nodeid / IVV_SLOT_NUM);
        slotid = (int)ikv->roots[rootid];
        if(slotid < 0) return ret;
        i = nodeid % IVV_SLOT_NUM;
        kvs = ikv->map + ikv->slots[slotid].nodeid;    
        while(i < (ikv->slots[slotid].count-1))
        {
            memcpy(&(kvs[i]), &(kvs[i+1]), sizeof(IVVKV));
            x = kvs[i].val;
            ikv->vmap[x].off = (i + ikv->slots[slotid].nodeid);
            ++i;
        }
        if(ikv->slots[slotid].count == 1)
        {
            i = slotid;
            ikv->state->qleft[(ikv->state->nleft++)] = ikv->slots[slotid].nodeid;
            ikv->roots[rootid] = -1;
            while(i < (ikv->state->count-1))
            {
                memcpy(&(ikv->slots[i]), &(ikv->slots[i+1]), sizeof(IVVSLOT));
                rootid = (ikv->slots[i].nodeid / IVV_SLOT_NUM);
                ikv->roots[rootid] = i;
                ++i;
            }
            --(ikv->state->count);
        }
        else
        {
            n = --(ikv->slots[slotid].count);
            ikv->slots[slotid].min = kvs[0].key;
            ikv->slots[slotid].max = kvs[n - 1].key;
        }
        ikv->vmap[no].off = -1;
    }
    return ret;
}

int ikv_find_slot(IKV *ikv, int32_t key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    if(ikv && (n = ikv->state->count) > 0)
    {
        max = n - 1;
        min = 0;
        if(key <= ikv->slots[min].max) ret = min;
        else if(key > ikv->slots[max].min) ret = max;
        else
        {
            while(max > min)
            {
                x = (min + max) / 2;
                if(x == min)
                {
                    if(key <= ikv->slots[x].max) ret = x;
                    else if(key <= ikv->slots[max].max) ret = max;
                    break;
                }
                if(key >=  ikv->slots[x].min && key <= ikv->slots[x].max)
                            //|| (x < (n - 1) && key <= ikv->slots[x+1].min)))
                {
                    ret = x;
                    break;
                }
                else if(key > ikv->slots[x].max) min = x;
                else max = x;
            }
        }
        if((x = ret)>= 0 && x < n)
        {
            while(x >= 0 && key <= ikv->slots[x].max)
            {
                ret = x--;
            }
        }
    }
    return ret;
}

int ikv_find_slot2(IKV *ikv, int32_t key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    if(ikv && (n = ikv->state->count) > 0)
    {
        max = n - 1;
        min = 0;
        if(key >= ikv->slots[max].min) ret = max;
        else if(key < ikv->slots[min].max) ret = min;
        else
        {
            while(max > min)
            {
                x = (min + max) / 2;
                if(x == min)
                {
                    if(key >= ikv->slots[max].min) ret = max;
                    else if(key >= ikv->slots[x].min) ret = x;
                    break;
                }
                if(key >=  ikv->slots[x].min && key <= ikv->slots[x].max) 
                            //|| (x < (n - 1) && key <= ikv->slots[x+1].min)))
                {
                    ret = x;
                    break;
                }
                else if(key > ikv->slots[x].max) min = x;
                else max = x;
            }
        }
        if((x = ret)>= 0 && x < n)
        {
            while(x < n && key >= ikv->slots[x].min) 
            {
                ret = x++;
            }
        }
    }
    return ret;
}

int ikv_find_kv(IKV *ikv, int k, int32_t key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    IVVKV *kvs = NULL;

    if(ikv && k >= 0 && k < ikv->state->count 
            && (n = ikv->slots[k].count) > 0)
    {
        kvs = ikv->map + ikv->slots[k].nodeid;
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
        //fprintf(stdout, "find_kv(%d) min:%d max:%d count:%d ret:%d\n", key, ikv->slots[k].min, ikv->slots[k].max, ikv->slots[k].count, ret);
    }
    return ret;
}

int ikv_find_kv2(IKV *ikv, int k, int32_t key)
{
    int n = 0, min = 0, max = 0, x = 0, ret = -1;
    IVVKV *kvs = NULL;

    if(ikv && k >= 0 && k < ikv->state->count 
            && (n = ikv->slots[k].count) > 0)
    {
        kvs = ikv->map + ikv->slots[k].nodeid;
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

int ikv_in(IKV *ikv, int32_t key, u32_t *list)
{
    int i = 0, k = 0, z = 0, ret = 0, n = 0;
    IVVKV *kvs = NULL;

    if(ikv && ikv->state && (n = ikv->state->count) > 0)
    {
        RWLOCK_RDLOCK(ikv->rwlock);
        k = ikv_find_slot(ikv, key);
        i = ikv_find_kv(ikv, k, key);
        do
        {
            kvs = ikv->map + ikv->slots[k].nodeid;
            if(i >= 0 && key == kvs[i].key && i < ikv->slots[k].count)
            {
                if(key == ikv->slots[k].max)
                {
                    ret += ikv->slots[k].count - i;
                    if(list)
                    {
                        while(i < ikv->slots[k].count) list[z++] = kvs[i++].val;
                    }
                }
                else
                {
                    while(i < ikv->slots[k].count && key == kvs[i].key)
                    {
                        if(list)list[z++] = kvs[i].val;
                        ++ret;
                        ++i;
                    }
                }
            }
            i=0;
        }while(++k < n && ikv->slots[k].min == key);
        RWLOCK_UNLOCK(ikv->rwlock);
    }
    return ret;
}

int ikv_range(IKV *ikv, int32_t from, int32_t to, u32_t *list)
{
    int i = 0, ii = 0, k = 0, kk = 0, j = 0, x = 0, z = 0, ret = 0, n = 0;
    IVVKV *kvs = NULL;

    if(ikv && ikv->state)
    {
        if(from == to) return ikv_in(ikv, from, list);
        RWLOCK_RDLOCK(ikv->rwlock);
        k = ikv_find_slot(ikv, from);
        kk = ikv_find_slot2(ikv, to);
        i = ikv_find_kv(ikv, k, from);
        ii = ikv_find_kv2(ikv, kk, to);
        if(k == -1 || kk == -1 || i == -1 || ii == -1) goto end;
        if(k == kk)
        {
            ret = ii + 1 - i;
            if(list)
            {
                kvs = ikv->map + ikv->slots[k].nodeid;
                for(x = i; x <= ii; x++) list[z++] = kvs[x].val;
            }
        }
        else
        {
            n =  ikv->slots[k].count;
            ret = n - i;
            if(list)
            {
                kvs = ikv->map + ikv->slots[k].nodeid;
                for(x = i; x < n; x++) list[z++] = kvs[x].val;
            }
            for(j = k+1; j < kk; j++)
            {
                ret += ikv->slots[j].count;
                if(list)
                {
                    kvs = ikv->map + ikv->slots[j].nodeid;
                    for(x = 0; x < ikv->slots[j].count; x++) list[z++] = kvs[x].val;
                }
            }
            ret += ii + 1;
            if(list)
            {
                kvs = ikv->map + ikv->slots[kk].nodeid;
                for(x = 0; x <= ii; x++) list[z++] = kvs[x].val;
            }
        }
end:
        RWLOCK_UNLOCK(ikv->rwlock);
    }
    return ret;
}

int ikv_rangefrom(IKV *ikv, int32_t key, u32_t *list) /* key = from */
{
    int i = 0, k = 0, x = 0, z = 0, ret = 0, n = 0;
    IVVKV *kvs = NULL;

    if(ikv && ikv->state)
    {
        RWLOCK_RDLOCK(ikv->rwlock);
        if((k = ikv_find_slot(ikv, key)) >= 0 && (i = ikv_find_kv(ikv, k, key)) >= 0)
        {
            //fprintf(stdout, "k:%d i:%d\n", k, i);
            kvs = ikv->map + ikv->slots[k].nodeid;
            n =  ikv->slots[k].count;
            if(list)
            {
                for(x = i; x < n; x++) list[z++] = kvs[x].val;
            }
            ret = n - i;
            for(i = k + 1; i <  ikv->state->count; i++)
            {
                ret += ikv->slots[i].count;
                if(list)
                {
                    kvs = ikv->map + ikv->slots[i].nodeid;
                    n = ikv->slots[i].count;
                    for(x = 0; x < n; x++) list[z++] = kvs[x].val;
                }
            }
        }
        RWLOCK_UNLOCK(ikv->rwlock);
    }
    //fprintf(stdout, "%s::%d k:%d ret:%d/%d\n", __FILE__, __LINE__, k, ret, z);
    return ret;
}

int ikv_rangeto(IKV *ikv, int32_t key, u32_t *list) /* key = to */
{
    int i = 0, k = 0, x = 0, j = 0, z = 0, ret = 0, n = 0;
    IVVKV *kvs = NULL;

    if(ikv && ikv->state && (n = (ikv->state->count)) > 0)
    {
        RWLOCK_RDLOCK(ikv->rwlock);
        if((k = ikv_find_slot2(ikv, key)) >= 0 && k < n 
                && (i = ikv_find_kv2(ikv, k, key)) >= 0)
        {
            for(j = 0; j < k; j++)
            {
                ret += ikv->slots[j].count;
                if(list)
                {
                    kvs = ikv->map + ikv->slots[j].nodeid;
                    for(x = 0; x < ikv->slots[j].count; x++)
                    {
                        list[z++] = kvs[x].val;
                    }
                }
            }
            ret += i + 1;
            if(list)
            {
                kvs = ikv->map + ikv->slots[k].nodeid;
                for(x = 0; x <= i; x++)
                {
                    list[z++] = kvs[x].val;
                }
            }
        }
        RWLOCK_UNLOCK(ikv->rwlock);
    }
    //fprintf(stdout, "%s::%d k:%d ret:%d/%d\n", __FILE__, __LINE__, k, ret, z);
    return ret;
}

int ikv_ins(IKV *ikv, int32_t *keys, int nkeys, u32_t *list)
{
    int ret = 0, i = 0, n = 0;
    u32_t *plist = list;

    if(ikv && ikv->state && keys && nkeys > 0)
    {
       for(i = 0; i < nkeys; i++) 
       {
            n = ikv_in(ikv, keys[i], plist);
            if(list) plist += n;
            ret += n;
       }
    }
    return ret;
}
int ikv_del(IKV *ikv, u32_t no)
{
    int ret = -1, n = 0;

    if(ikv)
    {
        RWLOCK_WRLOCK(ikv->rwlock);
        if((n = (ikv->vsize/sizeof(IVVV))) > 0 && no < n)
        {
            ikv_remove(ikv, no);
            ikv->vmap[no].off = -1;
            ret = 0;
        }
        RWLOCK_UNLOCK(ikv->rwlock);
    }
    return ret;
}
#endif
int ikv_get(IKV *ikv, u32_t no, u32_t *val)
{
    int ret = -1, n = 0;

    if(ikv)
    {
        RWLOCK_RDLOCK(ikv->rwlock);
        if((n = (ikv->vsize/sizeof(IVVV))) > 0 && no < n)
        {
            if(val) *val = ikv->vmap[no].val;
            ret = 0;
        }
        RWLOCK_UNLOCK(ikv->rwlock);
    }
    return ret;
}

int ikv_set(IKV *ikv, u32_t no, int32_t key)
{
    int ret = -1;

    if(ikv)
    {
       RWLOCK_WRLOCK(ikv->rwlock);
       ikv_vset(ikv, no, key);
#ifdef __IKV_USE_IDX__
       if(ikv->vmap[no].off  < 0)
       {
           ikv_insert(ikv, no, key);
       }
       else
       {
           if(key != ikv->vmap[no].val)
           {
                ikv_remove(ikv, no);
                ikv_insert(ikv, no, key);
           }
       }
#endif
       ikv->vmap[no].val = key;
       ret = 0;
       RWLOCK_UNLOCK(ikv->rwlock);
    }
    return ret;
}


void ikv_close(IKV *ikv)
{
    if(ikv)
    {
        if(ikv->state) munmap(ikv->state, ikv->msize);
        if(ikv->fd) close(ikv->fd);
        if(ikv->vmap) munmap(ikv->vmap, ikv->vmsize);
        if(ikv->vfd) close(ikv->vfd);
        RWLOCK_DESTROY(ikv->rwlock);
        free(ikv);
    }
    return ;
}

#ifdef IKV_TEST
#include "timer.h"
#define MASK  120000
//rm -rf /tmp/1.idx* && gcc -O2 -o ikv ikv.c -DIKV_TEST -DTEST_KV -DHAVE_PTHREAD -lpthread && ./ikv
int main()
{
    IKV *ikv = NULL;
    int i = 0, j = 0, n = 0, total = 0, no = 0, stat[MASK], stat2[MASK],
        v = 0, num = 1000000, base = 60000000;
    int32_t val = 0, from = 0, to = 0, *res = NULL, all_mask = 200000;
    int32_t inputs[256], nos[256], last[256], tall[200000];
    int32_t all = 0;
    time_t stime = 0, etime = 0;
    void *timer = NULL;

    if((ikv = ikv_init("/tmp/1.idx")))
    {
        res = (int32_t *)calloc(base, sizeof(int32_t));
        TIMER_INIT(timer);
#ifdef TEST_KV
        TIMER_RESET(timer);
        for(i = 0; i < num; i++)
        {
            no = random()%base;
            IKV_SET(ikv, no, i);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "set(%d) time used:%lld\n", num, PT_LU_USEC(timer));
        TIMER_RESET(timer);
        for(i = 0; i < num; i++)
        {
            no = random()%base;
            v = IKV_GET(ikv, no);
        }
        fprintf(stdout, "get(%d) time used:%lld\n", num, PT_LU_USEC(timer));
#endif

#ifdef TEST_RFROM
            ikv_set(ikv, 1, 22);
            ikv_set(ikv, 2, 25);
            ikv_set(ikv, 3, 20);
            ikv_set(ikv, 4, 13);
            ikv_set(ikv, 6, 22);
            n = ikv_rangefrom(ikv, 21, NULL);
            fprintf(stdout, "rangefrom(21) => %d\n", n);
#endif
#ifdef TEST_DEB
        /*
            n = ikv_in(ikv, 16615, NULL);
            fprintf(stdout, "16615:%d\n", n);
        */
        n = 0;
        for(i = 0; i < ikv->state->count; i++)
        {
            /*
            if(ikv->slots[i].min <= 26650 && ikv->slots[i].max >= 26650)
            {
                fprintf(stdout, "%d:[min:%d max:%d]\n", i, ikv->slots[i].min, ikv->slots[i].max);
            }
            */
            n+= ikv->slots[i].count;
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
            ikv_set(ikv, i, no);
            tall[no]++;
        }
        for(i = 0; i < all_mask; i++)
        {
            n = ikv_in(ikv, i, NULL);
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
            ikv_set(ikv, i, no);
            tall[no]++;
        }
        no = (rand()%all_mask);
        total = 0;for(i = no; i < all_mask; i++) total += tall[i];
        fprintf(stdout, "rangefrom(%d):%d/%d\n", no, ikv_rangefrom(ikv, no, NULL), total);
        total = 0;for(i = 0; i <= no; i++) total += tall[i];
        fprintf(stdout, "rangeto(%d):%d/%d\n", no, ikv_rangeto(ikv, no, NULL), total);
        from = (rand()%all_mask);
        to = (rand()%all_mask);
        if(from > to){from += to; to = from - to; from = from - to;}
        total = 0;for(i = from; i <= to; i++) total += tall[i];
        fprintf(stdout, "range(%d,%d):%d/%d\n", from, no, ikv_range(ikv, from, to, NULL), total);
        for(i = 0; i < all_mask; i++)
        {
            n = ikv_in(ikv, i, NULL);
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
           ikv_set(ikv, i, val);
           last[j] = i;
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "set() 40000000 data, time used:%lld\n", PT_LU_USEC(timer));
        TIMER_RESET(timer);
        for(i = 0; i < n; i++)
        {
            ikv_del(ikv, last[i]);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "del() time used:%lld\n", PT_LU_USEC(timer));
        TIMER_RESET(timer);
        for(i = 0; i < n; i++)
        {
            val = inputs[i];
            no = nos[i];
            stat2[no] = ikv_in(ikv, val, res);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "in() time used:%lld\n", PT_LU_USEC(timer));
        TIMER_RESET(timer);
        total = ikv_ins(ikv, inputs, n, NULL);
        TIMER_SAMPLE(timer);
        fprintf(stdout, "ins(keys, NULL) total:%d time used:%lld\n", total, PT_LU_USEC(timer));
        TIMER_RESET(timer);
        total = ikv_ins(ikv, inputs, n, res);
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
        for(i = 0; i < ikv->state->count; i++)
        {
            fprintf(stdout, "%d:{min:%d max:%d}(%d)\n", i, ikv->slots[i].min, ikv->slots[i].max, ikv->slots[i].count);
        }
        */
#ifdef TEST_RANGEFILTER
            ikv_set(ikv, 1, 1234567);
            ikv_set(ikv, 2, 1567890);
            fprintf(stdout, "rangefrom():%d\n", ikv_rangefrom(ikv, 1569000, NULL));
            fprintf(stdout, "rangeto():%d\n", ikv_rangeto(ikv, 1111111, NULL));
            fprintf(stdout, "range():%d\n", ikv_range(ikv, 1111111, 1400000, NULL));
#endif
#ifdef TEST_RANGE
        srand(time(NULL));
        TIMER_RESET(timer);
        for(i = 1; i < 40000000; i++)
        {
            val = 1356969600 + (rand()%31536000);
            ikv_set(ikv, i, val);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "set() 40000000 timestamps,  time used:%lld\n", PT_LU_USEC(timer));
        srand(time(NULL));
        TIMER_RESET(timer);
        all = 0;
        for(i = 0; i < 1000; i++)
        {
            val = 1356969600 + (rand()%31536000);
            all += ikv_rangefrom(ikv, val, res);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "rangefrom() 1000 times total:%lld, time used:%lld\n", (long long int)all, PT_LU_USEC(timer));
        srand(time(NULL));
        TIMER_RESET(timer);
        all = 0;
        for(i = 0; i < 1000; i++)
        {
            val = 1356969600 + (rand()%31536000);
            all += ikv_rangeto(ikv, val, res);
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
            all += ikv_range(ikv, from, to, res);
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
            all += ikv_range(ikv, from, to, NULL);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "range(null) 1000 times total:%lld, time used:%lld\n", (long long int)all, PT_LU_USEC(timer));

#endif
        ikv_close(ikv);
        TIMER_CLEAN(timer);
        free(res);
    }
}
#endif
