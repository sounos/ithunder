#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include "kvmap.h"
/* insert color  */
void kv_insert_color(KVMAP *map, KVNODE *elm)
{
    KVNODE *parent, *gparent, *tmp;
    while ((parent = KV_PARENT(elm)) &&
            KV_COLOR(parent) == KV_RED)
    {
        gparent = KV_PARENT(parent);
        if (parent == KV_LEFT(gparent))
        {
            tmp = KV_RIGHT(gparent);
            if (tmp && KV_COLOR(tmp) == KV_RED)
            {
                KV_COLOR(tmp) = KV_BLACK;
                KV_SET_BLACKRED(parent, gparent);
                elm = gparent;
                continue;
            }
            if (KV_RIGHT(parent) == elm)
            {
                KV_ROTATE_LEFT(map, parent, tmp);
                tmp = parent;
                parent = elm;
                elm = tmp;
            }
            KV_SET_BLACKRED(parent, gparent);
            KV_ROTATE_RIGHT(map, gparent, tmp);
        } 
        else
        {
            tmp = KV_LEFT(gparent);
            if (tmp && KV_COLOR(tmp) == KV_RED)
            {
                KV_COLOR(tmp) = KV_BLACK;
                KV_SET_BLACKRED(parent, gparent);
                elm = gparent;
                continue;
            }
            if (KV_LEFT(parent) == elm)
            {
                KV_ROTATE_RIGHT(map, parent, tmp);
                tmp = parent;
                parent = elm;
                elm = tmp;
            }
            KV_SET_BLACKRED(parent, gparent);
            KV_ROTATE_LEFT(map, gparent, tmp);
        }
    }
    KV_COLOR(map->rbh_root) = KV_BLACK;
}

void kv_remove_color(KVMAP *map, KVNODE *parent, KVNODE *elm)
{
    KVNODE *tmp;
    while ((elm == NULL || KV_COLOR(elm) == KV_BLACK) &&
            elm != KV_ROOT(map))
    {
        if (KV_LEFT(parent) == elm)
        {
            tmp = KV_RIGHT(parent);
            if (KV_COLOR(tmp) == KV_RED)
            {
                KV_SET_BLACKRED(tmp, parent);
                KV_ROTATE_LEFT(map, parent, tmp);
                tmp = KV_RIGHT(parent);
            }
            if ((KV_LEFT(tmp) == NULL ||
                        KV_COLOR(KV_LEFT(tmp)) == KV_BLACK) &&
                    (KV_RIGHT(tmp) == NULL ||
                     KV_COLOR(KV_RIGHT(tmp)) == KV_BLACK))
            {
                KV_COLOR(tmp) = KV_RED;
                elm = parent;
                parent = KV_PARENT(elm);
            }
            else
            {
                if (KV_RIGHT(tmp) == NULL ||
                        KV_COLOR(KV_RIGHT(tmp)) == KV_BLACK)
                {
                    KVNODE *oleft;
                    if ((oleft = KV_LEFT(tmp)))
                        KV_COLOR(oleft) = KV_BLACK;
                    KV_COLOR(tmp) = KV_RED;
                    KV_ROTATE_RIGHT(map, tmp, oleft);
                    tmp = KV_RIGHT(parent);
                }
                KV_COLOR(tmp) = KV_COLOR(parent);
                KV_COLOR(parent) = KV_BLACK;
                if (KV_RIGHT(tmp))
                    KV_COLOR(KV_RIGHT(tmp)) = KV_BLACK;
                KV_ROTATE_LEFT(map, parent, tmp);
                elm = KV_ROOT(map);
                break;
            }
        } 
        else
        {
            tmp = KV_LEFT(parent);
            if (KV_COLOR(tmp) == KV_RED)
            {
                KV_SET_BLACKRED(tmp, parent);
                KV_ROTATE_RIGHT(map, parent, tmp);
                tmp = KV_LEFT(parent);
            }
            if ((KV_LEFT(tmp) == NULL ||
                        KV_COLOR(KV_LEFT(tmp)) == KV_BLACK) &&
                    (KV_RIGHT(tmp) == NULL ||
                     KV_COLOR(KV_RIGHT(tmp)) == KV_BLACK))
            {
                KV_COLOR(tmp) = KV_RED;
                elm = parent;
                parent = KV_PARENT(elm);
            } 
            else
            {
                if (KV_LEFT(tmp) == NULL ||
                        KV_COLOR(KV_LEFT(tmp)) == KV_BLACK)
                {
                    KVNODE *oright;
                    if ((oright = KV_RIGHT(tmp)))
                        KV_COLOR(oright) = KV_BLACK;
                    KV_COLOR(tmp) = KV_RED;
                    KV_ROTATE_LEFT(map, tmp, oright);
                    tmp = KV_LEFT(parent);
                }
                KV_COLOR(tmp) = KV_COLOR(parent);
                KV_COLOR(parent) = KV_BLACK;
                if (KV_LEFT(tmp))
                    KV_COLOR(KV_LEFT(tmp)) = KV_BLACK;
                KV_ROTATE_RIGHT(map, parent, tmp);
                elm = KV_ROOT(map);
                break;
            }
        }
    }
    if (elm)
        KV_COLOR(elm) = KV_BLACK;
}

KVNODE *kv_remove(KVMAP *map, KVNODE *elm)
{
    KVNODE *child = NULL, *parent = NULL, *left = NULL, *old = elm;
    int color;

    KVMAP_MINMAX_REBUILD(map, elm);

    if (KV_LEFT(elm) == NULL)
        child = KV_RIGHT(elm);
    else if (KV_RIGHT(elm) == NULL)
        child = KV_LEFT(elm);
    else
    {
        elm = KV_RIGHT(elm);
        while ((left = KV_LEFT(elm)))
            elm = left;
        child = KV_RIGHT(elm);
        parent = KV_PARENT(elm);
        color = KV_COLOR(elm);
        if (child)
            KV_PARENT(child) = parent;
        if (parent)
        {
            if (KV_LEFT(parent) == elm)
                KV_LEFT(parent) = child;
            else
                KV_RIGHT(parent) = child;
            KV_AUGMENT(parent);
        } 
        else
            KV_ROOT(map) = child;
        if (KV_PARENT(elm) == old)
            parent = elm;
        KV_NODE_SET(elm, old);
        if (KV_PARENT(old))
        {
            if (KV_LEFT(KV_PARENT(old)) == old)
                KV_LEFT(KV_PARENT(old)) = elm;
            else
                KV_RIGHT(KV_PARENT(old)) = elm;
            KV_AUGMENT(KV_PARENT(old));
        } 
        else
            KV_ROOT(map) = elm;
        KV_PARENT(KV_LEFT(old)) = elm;
        if (KV_RIGHT(old))
            KV_PARENT(KV_RIGHT(old)) = elm;
        if (parent)
        {
            left = parent;
            do
            {
                KV_AUGMENT(left);
            } while ((left = KV_PARENT(left)));
        }
        goto color;
    }
    parent = KV_PARENT(elm);
    color = KV_COLOR(elm);
    if (child)
        KV_PARENT(child) = parent;
    if (parent)
    {
        if (KV_LEFT(parent) == elm)
            KV_LEFT(parent) = child;
        else
            KV_RIGHT(parent) = child;
        KV_AUGMENT(parent);
    } 
    else
        KV_ROOT(map) = child;
color:
    if (color == KV_BLACK)
        kv_remove_color(map, parent, child);
    return (old);
}

/* Inserts a node into the KV tree */
KVNODE *kv_insert(KVMAP *map, KVNODE *elm)
{
    KVNODE *tmp;
    KVNODE *parent = NULL;
    int64_t  comp = 0;
    tmp = KV_ROOT(map);
    while (tmp)
    {
        parent = tmp;
        comp = (int64_t)elm->key - (int64_t)parent->key;
        if (comp < 0)
            tmp = KV_LEFT(tmp);
        else if (comp > 0)
            tmp = KV_RIGHT(tmp);
        else
            return (tmp);
    }
    KV_SET(elm, parent);
    if (parent != NULL)
    {
        if (comp < 0)
            KV_LEFT(parent) = elm;
        else
            KV_RIGHT(parent) = elm;
        KV_AUGMENT(parent);
    } 
    else
        KV_ROOT(map) = elm;
    kv_insert_color(map, elm);
    return (NULL);
}

KVNODE *kv_find(KVMAP *map, KVNODE *elm)
{
    KVNODE *tmp = KV_ROOT(map);
    int64_t  comp = 0;
    while (tmp)
    {
        comp = (int64_t)elm->key - (int64_t)tmp->key;
        if (comp < 0)
            tmp = KV_LEFT(tmp);
        else if (comp > 0)
            tmp = KV_RIGHT(tmp);
        else
            return (tmp);
    }
    return (NULL);
}

KVNODE *kv_next(KVNODE *elm)
{
    KVNODE *pt = NULL, *ppt = NULL;
    if (KV_RIGHT(elm))
    {
        elm = KV_RIGHT(elm);
        while (KV_LEFT(elm))
            elm = KV_LEFT(elm);
    } 
    else
    {
        if (KV_PARENT(elm) &&
                (elm == KV_LEFT(KV_PARENT(elm))))
            elm = KV_PARENT(elm);
        else
        {
            if((pt = KV_PARENT(elm)) && (elm == KV_RIGHT(pt)) 
                    && (ppt = KV_PARENT(pt)) && pt == KV_LEFT(ppt)) 
            {
                elm = ppt;
            }
            //else root node
        }
    }
    return (elm);
}

KVNODE *kv_prev(KVNODE *elm)
{
    KVNODE *pt = NULL, *ppt = NULL;
    if (KV_LEFT(elm))
    {
        elm = KV_LEFT(elm);
        while (KV_RIGHT(elm))
            elm = KV_RIGHT(elm);
    } 
    else
    {
        if (KV_PARENT(elm) &&
                (elm == KV_RIGHT(KV_PARENT(elm))))
        {
            elm = KV_PARENT(elm);

        }
        else
        {
            if((pt = KV_PARENT(elm)) && (elm == KV_LEFT(pt)) 
                    && (ppt = KV_PARENT(pt)) && pt == KV_RIGHT(ppt)) 
            {
                elm = ppt;
            }
            //else root node
        }
    }
    return (elm);
}

KVNODE *kv_minmax(KVMAP *map, int val)
{
    KVNODE *tmp = KV_ROOT(map), *parent = NULL;
    while (tmp)
    {
        parent = tmp;
        if (val < 0)
            tmp = KV_LEFT(parent);
        else
            tmp = KV_RIGHT(parent);
    }
    return (parent);
}

#ifdef _DEBUG_KVMAP
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "kvmap.h"
#include "timer.h"

int main()
{
    void *map = NULL, *timer = NULL, *dp = NULL, *olddp = NULL;
    unsigned int i = 0, key = 0, count = 1000000;

    if((map = KVMAP_INIT()))
    {
        TIMER_INIT(timer);
        //test min
        i = 0;
        while(i++ < count)
        {
            key = (unsigned int)(random() & 0xffffffff);
            dp = (void *)((long)i);
            KVMAP_ADD(map, key, dp, olddp);
            if(olddp)fprintf(stdout, "old[%u:%ld]\n", key, (long)olddp);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "insert nodes(%d) count:%d/%d free:%d total:%d time used:%lld min:%u max:%u\n", count,  PKV(map)->count, i, PKV(map)->free_count, PKV(map)->total, PT_LU_USEC(timer), PKV_MINK(map), PKV_MAXK(map));
        i = 0;
        do
        {
            KVMAP_POP_MIN(map, key, dp);
            fprintf(stdout, "pop[%u:%ld]\n", key, (long)dp); 
            i++;
        }while(PKV(map)->count > 0);
        TIMER_SAMPLE(timer);
        fprintf(stdout, "pop min(%d) node count:%d free:%d total:%d time used:%lld\n", i,  PKV(map)->count, PKV(map)->free_count, PKV(map)->total, PT_LU_USEC(timer));
        /*
        //test max
        i = 0;
        while(i < count)
        {
            key = (unsigned int)random();
            dp = (void *)((long)++i);
            KVMAP_ADD(map, key, dp, olddp);
            if(olddp)fprintf(stdout, "old[%u:%08x]\n", key, olddp);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "insert nodes(%d) count:%d/%d free:%d total:%d time used:%lld\n", count,  PKV(map)->count, i, PKV(map)->free_count, PKV(map)->total, PT_LU_USEC(timer));
        i = 0;
        do
        {
            KVMAP_POP_MAX(map, key, dp);
            fprintf(stdout, "%u:[%08x]\n", key, (long)dp); 
            i++;
        }while(PKV(map)->count > 0);
        TIMER_SAMPLE(timer);
        fprintf(stdout, "pop max(%d) node count:%d free:%d total:%d time used:%lld\n", i,  PKV(map)->count, PKV(map)->free_count, PKV(map)->total, PT_LU_USEC(timer));

        */
        TIMER_CLEAN(timer);
        KVMAP_CLEAN(map);
    }
}
//gcc -o kmap kvmap.c -D_DEBUG_KVMAP && ./kmap
#endif
