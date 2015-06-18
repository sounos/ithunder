#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include "rbmap.h"

void rb_insert_color(RBMAP *map, RBNODE *elm)
{
    RBNODE *parent, *gparent, *tmp;
    while ((parent = RB_PARENT(elm)) &&
            RB_COLOR(parent) == RB_RED)
    {
        gparent = RB_PARENT(parent);
        if (parent == RB_LEFT(gparent))
        {
            tmp = RB_RIGHT(gparent);
            if (tmp && RB_COLOR(tmp) == RB_RED)
            {
                RB_COLOR(tmp) = RB_BLACK;
                RB_SET_BLACKRED(parent, gparent);
                elm = gparent;
                continue;
            }
            if (RB_RIGHT(parent) == elm)
            {
                RB_ROTATE_LEFT(map, parent, tmp);
                tmp = parent;
                parent = elm;
                elm = tmp;
            }
            RB_SET_BLACKRED(parent, gparent);
            RB_ROTATE_RIGHT(map, gparent, tmp);
        } 
        else
        {
            tmp = RB_LEFT(gparent);
            if (tmp && RB_COLOR(tmp) == RB_RED)
            {
                RB_COLOR(tmp) = RB_BLACK;
                RB_SET_BLACKRED(parent, gparent);
                elm = gparent;
                continue;
            }
            if (RB_LEFT(parent) == elm)
            {
                RB_ROTATE_RIGHT(map, parent, tmp);
                tmp = parent;
                parent = elm;
                elm = tmp;
            }
            RB_SET_BLACKRED(parent, gparent);
            RB_ROTATE_LEFT(map, gparent, tmp);
        }
    }
    RB_COLOR(map->rbh_root) = RB_BLACK;
}

void rb_remove_color(RBMAP *map, RBNODE *parent, RBNODE *elm)
{
    RBNODE *tmp;
    while ((elm == NULL || RB_COLOR(elm) == RB_BLACK) &&
            elm != RB_ROOT(map))
    {
        if (RB_LEFT(parent) == elm)
        {
            tmp = RB_RIGHT(parent);
            if (RB_COLOR(tmp) == RB_RED)
            {
                RB_SET_BLACKRED(tmp, parent);
                RB_ROTATE_LEFT(map, parent, tmp);
                tmp = RB_RIGHT(parent);
            }
            if ((RB_LEFT(tmp) == NULL ||
                        RB_COLOR(RB_LEFT(tmp)) == RB_BLACK) &&
                    (RB_RIGHT(tmp) == NULL ||
                     RB_COLOR(RB_RIGHT(tmp)) == RB_BLACK))
            {
                RB_COLOR(tmp) = RB_RED;
                elm = parent;
                parent = RB_PARENT(elm);
            }
            else
            {
                if (RB_RIGHT(tmp) == NULL ||
                        RB_COLOR(RB_RIGHT(tmp)) == RB_BLACK)
                {
                    RBNODE *oleft;
                    if ((oleft = RB_LEFT(tmp)))
                        RB_COLOR(oleft) = RB_BLACK;
                    RB_COLOR(tmp) = RB_RED;
                    RB_ROTATE_RIGHT(map, tmp, oleft);
                    tmp = RB_RIGHT(parent);
                }
                RB_COLOR(tmp) = RB_COLOR(parent);
                RB_COLOR(parent) = RB_BLACK;
                if (RB_RIGHT(tmp))
                    RB_COLOR(RB_RIGHT(tmp)) = RB_BLACK;
                RB_ROTATE_LEFT(map, parent, tmp);
                elm = RB_ROOT(map);
                break;
            }
        } 
        else
        {
            tmp = RB_LEFT(parent);
            if (RB_COLOR(tmp) == RB_RED)
            {
                RB_SET_BLACKRED(tmp, parent);
                RB_ROTATE_RIGHT(map, parent, tmp);
                tmp = RB_LEFT(parent);
            }
            if ((RB_LEFT(tmp) == NULL ||
                        RB_COLOR(RB_LEFT(tmp)) == RB_BLACK) &&
                    (RB_RIGHT(tmp) == NULL ||
                     RB_COLOR(RB_RIGHT(tmp)) == RB_BLACK))
            {
                RB_COLOR(tmp) = RB_RED;
                elm = parent;
                parent = RB_PARENT(elm);
            } 
            else
            {
                if (RB_LEFT(tmp) == NULL ||
                        RB_COLOR(RB_LEFT(tmp)) == RB_BLACK)
                {
                    RBNODE *oright;
                    if ((oright = RB_RIGHT(tmp)))
                        RB_COLOR(oright) = RB_BLACK;
                    RB_COLOR(tmp) = RB_RED;
                    RB_ROTATE_LEFT(map, tmp, oright);
                    tmp = RB_LEFT(parent);
                }
                RB_COLOR(tmp) = RB_COLOR(parent);
                RB_COLOR(parent) = RB_BLACK;
                if (RB_LEFT(tmp))
                    RB_COLOR(RB_LEFT(tmp)) = RB_BLACK;
                RB_ROTATE_RIGHT(map, parent, tmp);
                elm = RB_ROOT(map);
                break;
            }
        }
    }
    if (elm)
        RB_COLOR(elm) = RB_BLACK;
}

RBNODE *rb_remove(RBMAP *map, RBNODE *elm)
{
    RBNODE *child, *parent, *old = elm;
    int color;
    if (RB_LEFT(elm) == NULL)
        child = RB_RIGHT(elm);
    else if (RB_RIGHT(elm) == NULL)
        child = RB_LEFT(elm);
    else
    {
        RBNODE *left;
        elm = RB_RIGHT(elm);
        while ((left = RB_LEFT(elm)))
            elm = left;
        child = RB_RIGHT(elm);
        parent = RB_PARENT(elm);
        color = RB_COLOR(elm);
        if (child)
            RB_PARENT(child) = parent;
        if (parent)
        {
            if (RB_LEFT(parent) == elm)
                RB_LEFT(parent) = child;
            else
                RB_RIGHT(parent) = child;
            RB_AUGMENT(parent);
        } 
        else
            RB_ROOT(map) = child;
        if (RB_PARENT(elm) == old)
            parent = elm;
        RB_NODE_SET(elm, old);
        if (RB_PARENT(old))
        {
            if (RB_LEFT(RB_PARENT(old)) == old)
                RB_LEFT(RB_PARENT(old)) = elm;
            else
                RB_RIGHT(RB_PARENT(old)) = elm;
            RB_AUGMENT(RB_PARENT(old));
        } 
        else
            RB_ROOT(map) = elm;
        RB_PARENT(RB_LEFT(old)) = elm;
        if (RB_RIGHT(old))
            RB_PARENT(RB_RIGHT(old)) = elm;
        if (parent)
        {
            left = parent;
            do
            {
                RB_AUGMENT(left);
            } while ((left = RB_PARENT(left)));
        }
        goto color;
    }
    parent = RB_PARENT(elm);
    color = RB_COLOR(elm);
    if (child)
        RB_PARENT(child) = parent;
    if (parent)
    {
        if (RB_LEFT(parent) == elm)
            RB_LEFT(parent) = child;
        else
            RB_RIGHT(parent) = child;
        RB_AUGMENT(parent);
    } 
    else
        RB_ROOT(map) = child;
color:
    if (color == RB_BLACK)
        rb_remove_color(map, parent, child);
    return (old);
}

/* Inserts a node into the RB tree */
RBNODE *rb_insert(RBMAP *map, RBNODE *elm)
{
    RBNODE *tmp;
    RBNODE *parent = NULL;
    double  comp = 0.0;
    tmp = RB_ROOT(map);
    while (tmp)
    {
        parent = tmp;
        comp = (elm->key - parent->key);
        if (comp < 0)
            tmp = RB_LEFT(tmp);
        else if (comp > 0)
            tmp = RB_RIGHT(tmp);
        else
            return (tmp);
    }
    RB_SET(elm, parent);
    if (parent != NULL)
    {
        if (comp < 0)
            RB_LEFT(parent) = elm;
        else
            RB_RIGHT(parent) = elm;
        RB_AUGMENT(parent);
    } 
    else
        RB_ROOT(map) = elm;
    rb_insert_color(map, elm);
    return (NULL);
}

RBNODE *rb_find(RBMAP *map, RBNODE *elm)
{
    RBNODE *tmp = RB_ROOT(map);
    double  comp = 0.0;
    while (tmp)
    {
        comp = (elm->key - tmp->key);
        if (comp < 0)
            tmp = RB_LEFT(tmp);
        else if (comp > 0)
            tmp = RB_RIGHT(tmp);
        else
            return (tmp);
    }
    return (NULL);
}

RBNODE *rb_next(RBNODE *elm)
{
    RBNODE *pt = NULL, *ppt = NULL;

    if (RB_RIGHT(elm))
    {
        elm = RB_RIGHT(elm);
        while (RB_LEFT(elm))
            elm = RB_LEFT(elm);
    } 
    else
    {
        if (RB_PARENT(elm) &&
                (elm == RB_LEFT(RB_PARENT(elm))))
            elm = RB_PARENT(elm);
        else
        {
            if((pt = RB_PARENT(elm)) && (elm == RB_RIGHT(pt)) 
                    && (ppt = RB_PARENT(pt)) && pt == RB_LEFT(ppt)) 
            {
                elm = ppt;
            }
        }
    }
    return (elm);
}

RBNODE *rb_prev(RBNODE *elm)
{
    RBNODE *pt = NULL, *ppt = NULL;
    if (RB_LEFT(elm))
    {
        elm = RB_LEFT(elm);
        while (RB_RIGHT(elm))
            elm = RB_RIGHT(elm);
    } 
    else
    {
        if (RB_PARENT(elm) &&
                (elm == RB_RIGHT(RB_PARENT(elm))))
            elm = RB_PARENT(elm);
        else
        {
            if((pt = RB_PARENT(elm)) && (elm == RB_LEFT(pt)) 
                    && (ppt = RB_PARENT(pt)) && pt == RB_RIGHT(ppt)) 
            {
                elm = ppt;
            }
        }
    }
    return (elm);
}

RBNODE *rb_minmax(RBMAP *map, int val)
{
    RBNODE *tmp = RB_ROOT(map);
    RBNODE *parent = NULL;
    while (tmp)
    {
        parent = tmp;
        if (val < 0)
            tmp = RB_LEFT(tmp);
        else
            tmp = RB_RIGHT(tmp);
    }
    return (parent);
}

#ifdef _DEBUG_RBMAP
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rbmap.h"
#include "timer.h"

int main()
{
    void *map = NULL, *timer = NULL, *dp = NULL, *olddp = NULL;
    long i = 0, key = 0, count = 10000;

    if((map = RBMAP_INIT()))
    {
        TIMER_INIT(timer);
        while(i < count)
        {
            key = random()%count;
            dp = (void *)++i;
            RBMAP_ADD(map, key, dp, olddp);
            //if(olddp)fprintf(stdout, "old[%d:%08x]\n", key, olddp);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "insert %d nodes count:%d free:%d total:%d time used:%lld\n", count,  PRB(map)->count, PRB(map)->free_count, PRB(map)->total, PT_LU_USEC(timer));
        i = 0;
        while(i < count)
        {
            key = i++;
            dp = NULL;
            RBMAP_GET(map, key, dp);
            //if(dp)fprintf(stdout, "%ld:[%ld]\n", key, (long)dp);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "get %d nodes count:%d free:%d total:%d time used:%lld\n", count,  PRB(map)->count, PRB(map)->free_count, PRB(map)->total, PT_LU_USEC(timer));
        do
        {
            RBMAP_POP_MIN(map, key, dp);
            //if(dp) fprintf(stdout, "%ld:[%ld]\n", key, (long)dp); 
        }while(RB_ROOT(map));
        TIMER_SAMPLE(timer);
        fprintf(stdout, "pop min(%d) node count:%d free:%d total:%d time used:%lld\n", count,  PRB(map)->count, PRB(map)->free_count, PRB(map)->total, PT_LU_USEC(timer));
        i = 0;
        while(i < count)
        {
            key = random()%count;
            dp = (void *)i;
            RBMAP_ADD(map, key, dp, olddp);
            ++i;
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "insert %d nodes count:%d free:%d total:%d time used:%lld\n", count,  PRB(map)->count, PRB(map)->free_count, PRB(map)->total, PT_LU_USEC(timer));
        do
        {
            RBMAP_POP_MAX(map, key, dp);
            //if(dp) fprintf(stdout, "%ld:[%ld]\n", key, (long)dp); 
        }while(RB_ROOT(map));
        TIMER_SAMPLE(timer);
        fprintf(stdout, "pop max(%d) nodes count:%d free:%d total:%d time used:%lld\n", count,  PRB(map)->count, PRB(map)->free_count, PRB(map)->total, PT_LU_USEC(timer));
        TIMER_SAMPLE(timer);
        TIMER_CLEAN(timer);
        /*
        */
        RBMAP_CLEAN(map);
    }
}
#endif
