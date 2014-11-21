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
                imap->size = sizeof(IMMSTATE);
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
            while(i < n) {imap->vmap[i].off = -1;imap->vmap[i].val=0;++i;}
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
    int ret = -1, i = 0, n = 0, id = 0;
    off_t size = 0;

    if(imap && imap->state)
    {
        if(imap->state->nleft > 0)
        {
            n = --(imap->state->nleft);
            ret = imap->state->qleft[n];
        }
        else
        {
            size = (off_t)sizeof(IMMKV) * (off_t)IMM_SLOT_NUM * IMM_SLOT_INC + imap->size; 
            n = ftruncate(imap->fd, size);
            memset(((char *)imap->state+imap->size), 0, (size - imap->size));
            ret = (imap->size - (off_t)sizeof(IMMSTATE)) / (off_t)sizeof(IMMKV);
            id = ret + IMM_SLOT_NUM;
            i = 1;
            while(i < IMM_SLOT_INC)
            {
                imap->state->qleft[(imap->state->nleft++)] = id;
                id += IMM_SLOT_NUM;
                ++i;
            }
            imap->size = size;
        }
    }
    return ret;
}

int imap_insert(IMAP *imap, u32_t no, int32_t key)
{
    int ret = -1, i = 0, k = -1, nodeid = 0, pos = 0, num = 0, 
        n = 0, x = 0, min = 0, max = 0, m = 0, num2 = 0;
    IMMKV *kvs = NULL, *kv = NULL, *kv1 = NULL, *kv2 = NULL;
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
                        if(key >= imap->slots[max].min) k = max;
                        else k = x;
                        break;
                    }
                    if(key >=  imap->slots[x].min && (key <= imap->slots[x].max
                                || (x < (n - 1) && key <= imap->slots[x+1].min)))
                    {
                        k = x;
                        break;
                    }
                    else if(key > imap->slots[x].max) min = x;
                    else max = x;
                }
            }
        }
        if(k >= 0 && k < n && imap->slots[k].count ==  IMM_SLOT_NUM
                && imap->slots[k].max == key)
        {
            while(k < (n - 1) && imap->slots[k].count ==  IMM_SLOT_NUM
                    && key == imap->slots[k+1].min)++k;
        }
        /* 未满的slot 直接插入 */
        if(k >= 0 && k < n && imap->slots[k].count < IMM_SLOT_NUM)
        {
            m = x = imap->slots[k].count++;
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
            imap->slots[k].min = kvs[0].key;
            imap->slots[k].max = kvs[m].key;
        }
        else
        {
            nodeid = imap_slot_new(imap);
            /* slot已满转移元素到新的slot */
            if(k >= 0 && k < n && imap->slots[k].count == IMM_SLOT_NUM) 
            {
                kv1 = imap->map + imap->slots[k].nodeid + IMM_SLOT2_NUM;
                kv2 = imap->map + nodeid;
                num = IMM_SLOT2_NUM;
                if(key < kv1[0].key)
                {
                    /* 转移尾部一半元素到新slot */
                    for(i = 0; i < IMM_SLOT2_NUM; i++)
                    {
                        kv2[i].key = kv1[i].key;
                        kv2[i].val = kv1[i].val;
                        imap->vmap[(kv2[i].val)].off = (nodeid + i);
                    }
                    /* 插入到本slot */
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
                    imap->vmap[(kvs[i].val)].off = (imap->slots[k].nodeid + i);
                    num = IMM_SLOT2_NUM;
                    num2 = IMM_SLOT2_NUM + 1;
                }
                else
                {
                    i = 0;
                    x = 0;
                    while(i < IMM_SLOT2_NUM)
                    {
                        if(key <= kv1[i].key && x == i)
                        {
                            kv2->key = key;
                            kv2->val = no;
                            imap->vmap[(kv2->val)].off = (nodeid + x);
                            ++x;
                            ++kv2;
                        }
                        kv2->key = kv1[i].key;
                        kv2->val = kv1[i].val;
                        imap->vmap[(kv2->val)].off = (nodeid + x);
                        ++kv2;
                        ++x;
                        ++i;
                    }
                    if(x == i)
                    {
                        kv2->key = key;
                        kv2->val = no;
                        imap->vmap[(kv2->val)].off = (nodeid + x);
                    }
                    num = IMM_SLOT2_NUM + 1;
                    num2 = IMM_SLOT2_NUM;
                }
                /* rebuild slot */
                kvs = imap->map + imap->slots[k].nodeid;
                imap->slots[k].count = num2;
                imap->slots[k].min = kvs[0].key; 
                imap->slots[k].max = kvs[num2-1].key; 
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
                /* 插入新的slot */
                kv = imap->map + nodeid;
                kv->key = key;
                kv->val = no;
                imap->vmap[no].off = nodeid;
                num = 1;
                m = k = imap->state->count++; 
                while(k > 0 && key < imap->slots[k-1].min)
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
            x = (imap->slots[k].nodeid / IMM_SLOT_NUM);
            imap->roots[x] = k;
        }
    }
    return ret;
}

int imap_remove(IMAP *imap, u32_t no)
{
    int ret = -1, i = 0, x = 0, n = 0, slotid = 0;
    u32_t nodeid = 0, rootid = 0;
    IMMKV *kvs = NULL;

    if(imap && imap->state && imap->vmap 
            && (n = imap->vsize/sizeof(IMMV)) > 0 && no >= 0 && no < n
            && (nodeid = imap->vmap[no].off) >= 0)
    {
        rootid = (nodeid / IMM_SLOT_NUM);
        slotid = (int)imap->roots[rootid];
        if(slotid < 0) return ret;
        i = nodeid % IMM_SLOT_NUM;
        kvs = imap->map + imap->slots[slotid].nodeid;    
        while(i < (imap->slots[slotid].count-1))
        {
            memcpy(&(kvs[i]), &(kvs[i+1]), sizeof(IMMKV));
            x = kvs[i].val;
            imap->vmap[x].off = (i + imap->slots[slotid].nodeid);
            ++i;
        }
        if(imap->slots[slotid].count == 1)
        {
            i = slotid;
            imap->state->qleft[(imap->state->nleft++)] = imap->slots[slotid].nodeid;
            imap->roots[rootid] = -1;
            while(i < (imap->state->count-1))
            {
                memcpy(&(imap->slots[i]), &(imap->slots[i+1]), sizeof(IMMSLOT));
                rootid = (imap->slots[i].nodeid / IMM_SLOT_NUM);
                imap->roots[rootid] = i;
                ++i;
            }
            --(imap->state->count);
        }
        else
        {
            n = --(imap->slots[slotid].count);
            imap->slots[slotid].min = kvs[0].key;
            imap->slots[slotid].max = kvs[n - 1].key;
        }
        imap->vmap[no].off = -1;
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
        else if(key > imap->slots[max].min) ret = max;
        else
        {
            while(max > min)
            {
                x = (min + max) / 2;
                if(x == min)
                {
                    if(key <= imap->slots[x].max) ret = x;
                    else if(key <= imap->slots[max].max) ret = max;
                    break;
                }
                if(key >=  imap->slots[x].min && key <= imap->slots[x].max)
                            //|| (x < (n - 1) && key <= imap->slots[x+1].min)))
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
        if(key >= imap->slots[max].min) ret = max;
        else if(key < imap->slots[min].max) ret = min;
        else
        {
            while(max > min)
            {
                x = (min + max) / 2;
                if(x == min)
                {
                    if(key >= imap->slots[max].min) ret = max;
                    else if(key >= imap->slots[x].min) ret = x;
                    break;
                }
                if(key >=  imap->slots[x].min && key <= imap->slots[x].max) 
                            //|| (x < (n - 1) && key <= imap->slots[x+1].min)))
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
            if(i >= 0 && key == kvs[i].key && i < imap->slots[k].count)
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
        if(from == to) return imap_in(imap, from, list);
        RWLOCK_RDLOCK(imap->rwlock);
        k = imap_find_slot(imap, from);
        kk = imap_find_slot2(imap, to);
        i = imap_find_kv(imap, k, from);
        ii = imap_find_kv2(imap, kk, to);
        if(k == -1 || kk == -1 || i == -1 || ii == -1) goto end;
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
            for(j = k+1; j < kk; j++)
            {
                ret += imap->slots[j].count;
                if(list)
                {
                    kvs = imap->map + imap->slots[j].nodeid;
                    for(x = 0; x < imap->slots[j].count; x++) list[z++] = kvs[x].val;
                }
            }
            ret += ii + 1;
            if(list)
            {
                kvs = imap->map + imap->slots[kk].nodeid;
                for(x = 0; x <= ii; x++) list[z++] = kvs[x].val;
            }
        }
end:
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
            //fprintf(stdout, "k:%d i:%d\n", k, i);
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
            ret = 0;
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
//rm -rf /tmp/1.idx* && gcc -O2 -o imap imap.c -DIMAP_TEST -DTEST_IN -DHAVE_PTHREAD -lpthread && ./imap
int main()
{
    IMAP *imap = NULL;
    int i = 0, j = 0, n = 0, total = 0, no = 0, stat[MASK], stat2[MASK];
    int32_t val = 0, from = 0, to = 0, *res = NULL, all_mask = 200000;
    int32_t inputs[256], nos[256], last[256], tall[200000];
    int32_t all = 0;
    time_t stime = 0, etime = 0;
    void *timer = NULL;

    if((imap = imap_init("/tmp/1.idx")))
    {
        res = (int32_t *)calloc(60000000, sizeof(int32_t));
        TIMER_INIT(timer);
#ifdef TEST_RFROM
            imap_set(imap, 1, 22);
            imap_set(imap, 2, 25);
            imap_set(imap, 3, 20);
            imap_set(imap, 4, 13);
            imap_set(imap, 6, 22);
            n = imap_rangefrom(imap, 21, NULL);
            fprintf(stdout, "rangefrom(21) => %d\n", n);
#endif
#ifdef TEST_DEB
        /*
            n = imap_in(imap, 16615, NULL);
            fprintf(stdout, "16615:%d\n", n);
        */
        n = 0;
        for(i = 0; i < imap->state->count; i++)
        {
            /*
            if(imap->slots[i].min <= 26650 && imap->slots[i].max >= 26650)
            {
                fprintf(stdout, "%d:[min:%d max:%d]\n", i, imap->slots[i].min, imap->slots[i].max);
            }
            */
            n+= imap->slots[i].count;
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
            imap_set(imap, i, no);
            tall[no]++;
        }
        for(i = 0; i < all_mask; i++)
        {
            n = imap_in(imap, i, NULL);
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
            imap_set(imap, i, no);
            tall[no]++;
        }
        no = (rand()%all_mask);
        total = 0;for(i = no; i < all_mask; i++) total += tall[i];
        fprintf(stdout, "rangefrom(%d):%d/%d\n", no, imap_rangefrom(imap, no, NULL), total);
        total = 0;for(i = 0; i <= no; i++) total += tall[i];
        fprintf(stdout, "rangeto(%d):%d/%d\n", no, imap_rangeto(imap, no, NULL), total);
        from = (rand()%all_mask);
        to = (rand()%all_mask);
        if(from > to){from += to; to = from - to; from = from - to;}
        total = 0;for(i = from; i <= to; i++) total += tall[i];
        fprintf(stdout, "range(%d,%d):%d/%d\n", from, no, imap_range(imap, from, to, NULL), total);
        for(i = 0; i < all_mask; i++)
        {
            n = imap_in(imap, i, NULL);
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
            no = nos[i];
            stat2[no] = imap_in(imap, val, res);
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
        for(i = 0; i < imap->state->count; i++)
        {
            fprintf(stdout, "%d:{min:%d max:%d}(%d)\n", i, imap->slots[i].min, imap->slots[i].max, imap->slots[i].count);
        }
        */
#ifdef TEST_RANGEFILTER
            imap_set(imap, 1, 1234567);
            imap_set(imap, 2, 1567890);
            fprintf(stdout, "rangefrom():%d\n", imap_rangefrom(imap, 1569000, NULL));
            fprintf(stdout, "rangeto():%d\n", imap_rangeto(imap, 1111111, NULL));
            fprintf(stdout, "range():%d\n", imap_range(imap, 1111111, 1400000, NULL));
#endif
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
