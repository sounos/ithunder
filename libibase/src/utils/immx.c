#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include "immx.h"
/* insert color  */
void immx_insert_color(IMMX *map, IMMXNODE *elm)
{
    IMMXNODE *parent, *gparent, *tmp;
    while ((parent = IMMX_PARENT(elm)) &&
            IMMX_COLOR(parent) == IMMX_RED)
    {
        gparent = IMMX_PARENT(parent);
        if (parent == IMMX_LEFT(gparent))
        {
            tmp = IMMX_RIGHT(gparent);
            if (tmp && IMMX_COLOR(tmp) == IMMX_RED)
            {
                IMMX_COLOR(tmp) = IMMX_BLACK;
                IMMX_SET_BLACKRED(parent, gparent);
                elm = gparent;
                continue;
            }
            if (IMMX_RIGHT(parent) == elm)
            {
                IMMX_ROTATE_LEFT(map, parent, tmp);
                tmp = parent;
                parent = elm;
                elm = tmp;
            }
            IMMX_SET_BLACKRED(parent, gparent);
            IMMX_ROTATE_RIGHT(map, gparent, tmp);
        } 
        else
        {
            tmp = IMMX_LEFT(gparent);
            if (tmp && IMMX_COLOR(tmp) == IMMX_RED)
            {
                IMMX_COLOR(tmp) = IMMX_BLACK;
                IMMX_SET_BLACKRED(parent, gparent);
                elm = gparent;
                continue;
            }
            if (IMMX_LEFT(parent) == elm)
            {
                IMMX_ROTATE_RIGHT(map, parent, tmp);
                tmp = parent;
                parent = elm;
                elm = tmp;
            }
            IMMX_SET_BLACKRED(parent, gparent);
            IMMX_ROTATE_LEFT(map, gparent, tmp);
        }
    }
    IMMX_COLOR(map->rbh_root) = IMMX_BLACK;
}

void immx_remove_color(IMMX *map, IMMXNODE *parent, IMMXNODE *elm)
{
    IMMXNODE *tmp;
    while ((elm == NULL || IMMX_COLOR(elm) == IMMX_BLACK) &&
            elm != IMMX_ROOT(map))
    {
        if (IMMX_LEFT(parent) == elm)
        {
            tmp = IMMX_RIGHT(parent);
            if (IMMX_COLOR(tmp) == IMMX_RED)
            {
                IMMX_SET_BLACKRED(tmp, parent);
                IMMX_ROTATE_LEFT(map, parent, tmp);
                tmp = IMMX_RIGHT(parent);
            }
            if ((IMMX_LEFT(tmp) == NULL ||
                        IMMX_COLOR(IMMX_LEFT(tmp)) == IMMX_BLACK) &&
                    (IMMX_RIGHT(tmp) == NULL ||
                     IMMX_COLOR(IMMX_RIGHT(tmp)) == IMMX_BLACK))
            {
                IMMX_COLOR(tmp) = IMMX_RED;
                elm = parent;
                parent = IMMX_PARENT(elm);
            }
            else
            {
                if (IMMX_RIGHT(tmp) == NULL ||
                        IMMX_COLOR(IMMX_RIGHT(tmp)) == IMMX_BLACK)
                {
                    IMMXNODE *oleft;
                    if ((oleft = IMMX_LEFT(tmp)))
                        IMMX_COLOR(oleft) = IMMX_BLACK;
                    IMMX_COLOR(tmp) = IMMX_RED;
                    IMMX_ROTATE_RIGHT(map, tmp, oleft);
                    tmp = IMMX_RIGHT(parent);
                }
                IMMX_COLOR(tmp) = IMMX_COLOR(parent);
                IMMX_COLOR(parent) = IMMX_BLACK;
                if (IMMX_RIGHT(tmp))
                    IMMX_COLOR(IMMX_RIGHT(tmp)) = IMMX_BLACK;
                IMMX_ROTATE_LEFT(map, parent, tmp);
                elm = IMMX_ROOT(map);
                break;
            }
        } 
        else
        {
            tmp = IMMX_LEFT(parent);
            if (IMMX_COLOR(tmp) == IMMX_RED)
            {
                IMMX_SET_BLACKRED(tmp, parent);
                IMMX_ROTATE_RIGHT(map, parent, tmp);
                tmp = IMMX_LEFT(parent);
            }
            if ((IMMX_LEFT(tmp) == NULL ||
                        IMMX_COLOR(IMMX_LEFT(tmp)) == IMMX_BLACK) &&
                    (IMMX_RIGHT(tmp) == NULL ||
                     IMMX_COLOR(IMMX_RIGHT(tmp)) == IMMX_BLACK))
            {
                IMMX_COLOR(tmp) = IMMX_RED;
                elm = parent;
                parent = IMMX_PARENT(elm);
            } 
            else
            {
                if (IMMX_LEFT(tmp) == NULL ||
                        IMMX_COLOR(IMMX_LEFT(tmp)) == IMMX_BLACK)
                {
                    IMMXNODE *oright;
                    if ((oright = IMMX_RIGHT(tmp)))
                        IMMX_COLOR(oright) = IMMX_BLACK;
                    IMMX_COLOR(tmp) = IMMX_RED;
                    IMMX_ROTATE_LEFT(map, tmp, oright);
                    tmp = IMMX_LEFT(parent);
                }
                IMMX_COLOR(tmp) = IMMX_COLOR(parent);
                IMMX_COLOR(parent) = IMMX_BLACK;
                if (IMMX_LEFT(tmp))
                    IMMX_COLOR(IMMX_LEFT(tmp)) = IMMX_BLACK;
                IMMX_ROTATE_RIGHT(map, parent, tmp);
                elm = IMMX_ROOT(map);
                break;
            }
        }
    }
    if (elm)
        IMMX_COLOR(elm) = IMMX_BLACK;
}

IMMXNODE *immx_remove(IMMX *map, IMMXNODE *elm)
{
    IMMXNODE *child, *parent, *old = elm;
    int color;

    IMMX_MINMAX_REBUILD(map, elm);

    if (IMMX_LEFT(elm) == NULL)
        child = IMMX_RIGHT(elm);
    else if (IMMX_RIGHT(elm) == NULL)
        child = IMMX_LEFT(elm);
    else
    {
        IMMXNODE *left;
        elm = IMMX_RIGHT(elm);
        while ((left = IMMX_LEFT(elm)))
            elm = left;
        child = IMMX_RIGHT(elm);
        parent = IMMX_PARENT(elm);
        color = IMMX_COLOR(elm);
        if (child)
            IMMX_PARENT(child) = parent;
        if (parent)
        {
            if (IMMX_LEFT(parent) == elm)
                IMMX_LEFT(parent) = child;
            else
                IMMX_RIGHT(parent) = child;
            IMMX_AUGMENT(parent);
        } 
        else
            IMMX_ROOT(map) = child;
        if (IMMX_PARENT(elm) == old)
            parent = elm;
        IMMX_NODE_SET(elm, old);
        if (IMMX_PARENT(old))
        {
            if (IMMX_LEFT(IMMX_PARENT(old)) == old)
                IMMX_LEFT(IMMX_PARENT(old)) = elm;
            else
                IMMX_RIGHT(IMMX_PARENT(old)) = elm;
            IMMX_AUGMENT(IMMX_PARENT(old));
        } 
        else
            IMMX_ROOT(map) = elm;
        IMMX_PARENT(IMMX_LEFT(old)) = elm;
        if (IMMX_RIGHT(old))
            IMMX_PARENT(IMMX_RIGHT(old)) = elm;
        if (parent)
        {
            left = parent;
            do
            {
                IMMX_AUGMENT(left);
            } while ((left = IMMX_PARENT(left)));
        }
        goto color;
    }
    parent = IMMX_PARENT(elm);
    color = IMMX_COLOR(elm);
    if (child)
        IMMX_PARENT(child) = parent;
    if (parent)
    {
        if (IMMX_LEFT(parent) == elm)
            IMMX_LEFT(parent) = child;
        else
            IMMX_RIGHT(parent) = child;
        IMMX_AUGMENT(parent);
    } 
    else
        IMMX_ROOT(map) = child;
color:
    if (color == IMMX_BLACK)
        immx_remove_color(map, parent, child);
    return (old);
}

/* Inserts a node into the IMMX tree */
IMMXNODE *immx_insert(IMMX *map, IMMXNODE *elm)
{
    IMMXNODE *tmp;
    IMMXNODE *parent = NULL;
    int  comp = 0;
    tmp = IMMX_ROOT(map);
    while (tmp)
    {
        parent = tmp;
        comp = (elm->key - parent->key);
        if (comp < 0)
            tmp = IMMX_LEFT(tmp);
        else if (comp > 0)
            tmp = IMMX_RIGHT(tmp);
        else
            return (tmp);
    }
    IMMX_SET(elm, parent);
    if (parent != NULL)
    {
        if (comp < 0)
            IMMX_LEFT(parent) = elm;
        else
            IMMX_RIGHT(parent) = elm;
        IMMX_AUGMENT(parent);
    } 
    else
        IMMX_ROOT(map) = elm;
    immx_insert_color(map, elm);
    return (NULL);
}

IMMXNODE *immx_find(IMMX *map, IMMXNODE *elm)
{
    IMMXNODE *tmp = IMMX_ROOT(map);
    int  comp = 0;
    while (tmp)
    {
        comp = (elm->key - tmp->key);
        if (comp < 0)
            tmp = IMMX_LEFT(tmp);
        else if (comp > 0)
            tmp = IMMX_RIGHT(tmp);
        else
            return (tmp);
    }
    return (NULL);
}

IMMXNODE *immx_next(IMMXNODE *elm)
{
    if (IMMX_RIGHT(elm))
    {
        elm = IMMX_RIGHT(elm);
        while (IMMX_LEFT(elm))
            elm = IMMX_LEFT(elm);
    } else
    {
        if (IMMX_PARENT(elm) &&
                (elm == IMMX_LEFT(IMMX_PARENT(elm))))
            elm = IMMX_PARENT(elm);
        else
        {
            while (IMMX_PARENT(elm) &&
                    (elm == IMMX_RIGHT(IMMX_PARENT(elm))))
                elm = IMMX_PARENT(elm);
            elm = IMMX_PARENT(elm);
        }
    }
    return (elm);
}

IMMXNODE *immx_prev(IMMXNODE *elm)
{
    if (IMMX_LEFT(elm))
    {
        elm = IMMX_LEFT(elm);
        while (IMMX_RIGHT(elm))
            elm = IMMX_RIGHT(elm);
    } 
    else
    {
        if (IMMX_PARENT(elm) &&
                (elm == IMMX_RIGHT(IMMX_PARENT(elm))))
            elm = IMMX_PARENT(elm);
        else
        {
            while (IMMX_PARENT(elm) &&
                    (elm == IMMX_LEFT(IMMX_PARENT(elm))))
                elm = IMMX_PARENT(elm);
            elm = IMMX_PARENT(elm);
        }
    }
    return (elm);
}

IMMXNODE *immx_minmax(IMMX *map, int val)
{
    IMMXNODE *tmp = IMMX_ROOT(map);
    IMMXNODE *parent = NULL;
    while (tmp)
    {
        parent = tmp;
        if (val < 0)
            tmp = IMMX_LEFT(tmp);
        else
            tmp = IMMX_RIGHT(tmp);
    }
    return (parent);
}

#ifdef _DEBUG_IMMX
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rbmap.h"
#include "timer.h"

int main()
{
    long i = 0, key = 0, data = 0, count = 10000;
    void *map = NULL, *timer = NULL;

    if((map = IMMX_INIT()))
    {
        TIMER_INIT(timer);
        while(i < count)
        {
            key = random()%count;
            IMMX_ADD(map, key);
            //if(olddp)fprintf(stdout, "old[%d:%08x]\n", key, olddp);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "insert %d nodes count:%d free:%d total:%d time used:%lld\n", count,  PIMMX(map)->count, PIMMX(map)->free_count, PIMMX(map)->total, PT_LU_USEC(timer));
        i = 0;
        while(i < count)
        {
            key = i++;
            IMMX_GET(map, key, data);
            //if(dp)fprintf(stdout, "%ld:[%ld]\n", key, (long)dp);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "get %d nodes count:%d free:%d total:%d time used:%lld\n", count,  PIMMX(map)->count, PIMMX(map)->free_count, PIMMX(map)->total, PT_LU_USEC(timer));
        do
        {
            IMMX_POP_MIN(map, key, data);
            //if(dp) fprintf(stdout, "%ld:[%ld]\n", key, (long)dp); 
        }while(IMMX_ROOT(map));
        TIMER_SAMPLE(timer);
        fprintf(stdout, "pop min(%d) node count:%d free:%d total:%d time used:%lld\n", count,  PIMMX(map)->count, PIMMX(map)->free_count, PIMMX(map)->total, PT_LU_USEC(timer));
        i = 0;
        while(i < count)
        {
            key = random()%count;
            IMMX_ADD(map, key);
            ++i;
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "insert %d nodes count:%d free:%d total:%d time used:%lld\n", count,  PIMMX(map)->count, PIMMX(map)->free_count, PIMMX(map)->total, PT_LU_USEC(timer));
        do
        {
            IMMX_POP_MAX(map, key, data);
            //if(dp) fprintf(stdout, "%ld:[%ld]\n", key, (long)dp); 
        }while(IMMX_ROOT(map));
        TIMER_SAMPLE(timer);
        fprintf(stdout, "pop max(%d) nodes count:%d free:%d total:%d time used:%lld\n", count,  PIMMX(map)->count, PIMMX(map)->free_count, PIMMX(map)->total, PT_LU_USEC(timer));
        TIMER_SAMPLE(timer);
        TIMER_CLEAN(timer);
        IMMX_CLEAN(map);
    }
}
#endif
