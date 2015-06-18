#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include "immx.h"
/* insert color  */
void imx_insert_color(IMMX *map, IMXNODE *elm)
{
    IMXNODE *parent, *gparent, *tmp;
    while ((parent = IMX_PARENT(elm)) &&
            IMX_COLOR(parent) == IMX_RED)
    {
        gparent = IMX_PARENT(parent);
        if (parent == IMX_LEFT(gparent))
        {
            tmp = IMX_RIGHT(gparent);
            if (tmp && IMX_COLOR(tmp) == IMX_RED)
            {
                IMX_COLOR(tmp) = IMX_BLACK;
                IMX_SET_BLACKRED(parent, gparent);
                elm = gparent;
                continue;
            }
            if (IMX_RIGHT(parent) == elm)
            {
                IMX_ROTATE_LEFT(map, parent, tmp);
                tmp = parent;
                parent = elm;
                elm = tmp;
            }
            IMX_SET_BLACKRED(parent, gparent);
            IMX_ROTATE_RIGHT(map, gparent, tmp);
        } 
        else
        {
            tmp = IMX_LEFT(gparent);
            if (tmp && IMX_COLOR(tmp) == IMX_RED)
            {
                IMX_COLOR(tmp) = IMX_BLACK;
                IMX_SET_BLACKRED(parent, gparent);
                elm = gparent;
                continue;
            }
            if (IMX_LEFT(parent) == elm)
            {
                IMX_ROTATE_RIGHT(map, parent, tmp);
                tmp = parent;
                parent = elm;
                elm = tmp;
            }
            IMX_SET_BLACKRED(parent, gparent);
            IMX_ROTATE_LEFT(map, gparent, tmp);
        }
    }
    IMX_COLOR(map->rbh_root) = IMX_BLACK;
}

void imx_remove_color(IMMX *map, IMXNODE *parent, IMXNODE *elm)
{
    IMXNODE *tmp;
    while ((elm == NULL || IMX_COLOR(elm) == IMX_BLACK) &&
            elm != IMX_ROOT(map))
    {
        if (IMX_LEFT(parent) == elm)
        {
            tmp = IMX_RIGHT(parent);
            if (IMX_COLOR(tmp) == IMX_RED)
            {
                IMX_SET_BLACKRED(tmp, parent);
                IMX_ROTATE_LEFT(map, parent, tmp);
                tmp = IMX_RIGHT(parent);
            }
            if ((IMX_LEFT(tmp) == NULL ||
                        IMX_COLOR(IMX_LEFT(tmp)) == IMX_BLACK) &&
                    (IMX_RIGHT(tmp) == NULL ||
                     IMX_COLOR(IMX_RIGHT(tmp)) == IMX_BLACK))
            {
                IMX_COLOR(tmp) = IMX_RED;
                elm = parent;
                parent = IMX_PARENT(elm);
            }
            else
            {
                if (IMX_RIGHT(tmp) == NULL ||
                        IMX_COLOR(IMX_RIGHT(tmp)) == IMX_BLACK)
                {
                    IMXNODE *oleft;
                    if ((oleft = IMX_LEFT(tmp)))
                        IMX_COLOR(oleft) = IMX_BLACK;
                    IMX_COLOR(tmp) = IMX_RED;
                    IMX_ROTATE_RIGHT(map, tmp, oleft);
                    tmp = IMX_RIGHT(parent);
                }
                IMX_COLOR(tmp) = IMX_COLOR(parent);
                IMX_COLOR(parent) = IMX_BLACK;
                if (IMX_RIGHT(tmp))
                    IMX_COLOR(IMX_RIGHT(tmp)) = IMX_BLACK;
                IMX_ROTATE_LEFT(map, parent, tmp);
                elm = IMX_ROOT(map);
                break;
            }
        } 
        else
        {
            tmp = IMX_LEFT(parent);
            if (IMX_COLOR(tmp) == IMX_RED)
            {
                IMX_SET_BLACKRED(tmp, parent);
                IMX_ROTATE_RIGHT(map, parent, tmp);
                tmp = IMX_LEFT(parent);
            }
            if ((IMX_LEFT(tmp) == NULL ||
                        IMX_COLOR(IMX_LEFT(tmp)) == IMX_BLACK) &&
                    (IMX_RIGHT(tmp) == NULL ||
                     IMX_COLOR(IMX_RIGHT(tmp)) == IMX_BLACK))
            {
                IMX_COLOR(tmp) = IMX_RED;
                elm = parent;
                parent = IMX_PARENT(elm);
            } 
            else
            {
                if (IMX_LEFT(tmp) == NULL ||
                        IMX_COLOR(IMX_LEFT(tmp)) == IMX_BLACK)
                {
                    IMXNODE *oright;
                    if ((oright = IMX_RIGHT(tmp)))
                        IMX_COLOR(oright) = IMX_BLACK;
                    IMX_COLOR(tmp) = IMX_RED;
                    IMX_ROTATE_LEFT(map, tmp, oright);
                    tmp = IMX_LEFT(parent);
                }
                IMX_COLOR(tmp) = IMX_COLOR(parent);
                IMX_COLOR(parent) = IMX_BLACK;
                if (IMX_LEFT(tmp))
                    IMX_COLOR(IMX_LEFT(tmp)) = IMX_BLACK;
                IMX_ROTATE_RIGHT(map, parent, tmp);
                elm = IMX_ROOT(map);
                break;
            }
        }
    }
    if (elm)
        IMX_COLOR(elm) = IMX_BLACK;
}

IMXNODE *imx_remove(IMMX *map, IMXNODE *elm)
{
    IMXNODE *child, *parent, *old = elm;
    int color;

    IMMX_MINMAX_REBUILD(map, elm);

    if (IMX_LEFT(elm) == NULL)
        child = IMX_RIGHT(elm);
    else if (IMX_RIGHT(elm) == NULL)
        child = IMX_LEFT(elm);
    else
    {
        IMXNODE *left;
        elm = IMX_RIGHT(elm);
        while ((left = IMX_LEFT(elm)))
            elm = left;
        child = IMX_RIGHT(elm);
        parent = IMX_PARENT(elm);
        color = IMX_COLOR(elm);
        if (child)
            IMX_PARENT(child) = parent;
        if (parent)
        {
            if (IMX_LEFT(parent) == elm)
                IMX_LEFT(parent) = child;
            else
                IMX_RIGHT(parent) = child;
            IMX_AUGMENT(parent);
        } 
        else
            IMX_ROOT(map) = child;
        if (IMX_PARENT(elm) == old)
            parent = elm;
        IMX_NODE_SET(elm, old);
        if (IMX_PARENT(old))
        {
            if (IMX_LEFT(IMX_PARENT(old)) == old)
                IMX_LEFT(IMX_PARENT(old)) = elm;
            else
                IMX_RIGHT(IMX_PARENT(old)) = elm;
            IMX_AUGMENT(IMX_PARENT(old));
        } 
        else
            IMX_ROOT(map) = elm;
        IMX_PARENT(IMX_LEFT(old)) = elm;
        if (IMX_RIGHT(old))
            IMX_PARENT(IMX_RIGHT(old)) = elm;
        if (parent)
        {
            left = parent;
            do
            {
                IMX_AUGMENT(left);
            } while ((left = IMX_PARENT(left)));
        }
        goto color;
    }
    parent = IMX_PARENT(elm);
    color = IMX_COLOR(elm);
    if (child)
        IMX_PARENT(child) = parent;
    if (parent)
    {
        if (IMX_LEFT(parent) == elm)
            IMX_LEFT(parent) = child;
        else
            IMX_RIGHT(parent) = child;
        IMX_AUGMENT(parent);
    } 
    else
        IMX_ROOT(map) = child;
color:
    if (color == IMX_BLACK)
        imx_remove_color(map, parent, child);
    return (old);
}

/* Inserts a node into the IMX tree */
IMXNODE *imx_insert(IMMX *map, IMXNODE *elm)
{
    IMXNODE *tmp;
    IMXNODE *parent = NULL;
    int64_t  comp = 0;
    tmp = IMX_ROOT(map);
    while (tmp)
    {
        parent = tmp;
        comp = ((int64_t)elm->key - (int64_t)parent->key);
        if (comp < 0)
            tmp = IMX_LEFT(tmp);
        else if (comp > 0)
            tmp = IMX_RIGHT(tmp);
        else
            return (tmp);
    }
    IMX_SET(elm, parent);
    if (parent != NULL)
    {
        if (comp < 0)
            IMX_LEFT(parent) = elm;
        else
            IMX_RIGHT(parent) = elm;
        IMX_AUGMENT(parent);
    } 
    else
        IMX_ROOT(map) = elm;
    imx_insert_color(map, elm);
    return (NULL);
}

IMXNODE *imx_find(IMMX *map, IMXNODE *elm)
{
    IMXNODE *tmp = IMX_ROOT(map);
    int64_t  comp = 0;
    while (tmp)
    {
        comp = ((int64_t)elm->key - (int64_t)tmp->key);
        if (comp < 0)
            tmp = IMX_LEFT(tmp);
        else if (comp > 0)
            tmp = IMX_RIGHT(tmp);
        else
            return (tmp);
    }
    return (NULL);
}

IMXNODE *imx_next(IMXNODE *elm)
{
    IMXNODE *pt = NULL, *ppt = NULL;
    if (IMX_RIGHT(elm))
    {
        elm = IMX_RIGHT(elm);
        while (IMX_LEFT(elm))
            elm = IMX_LEFT(elm);
    } 
    else
    {
        if (IMX_PARENT(elm) &&
                (elm == IMX_LEFT(IMX_PARENT(elm))))
            elm = IMX_PARENT(elm);
        else
        {
            if((pt = IMX_PARENT(elm)) && (elm == IMX_RIGHT(pt)) 
                    && (ppt = IMX_PARENT(pt)) && pt == IMX_LEFT(ppt)) 
            {
                elm = ppt;
            }
            //else root node
        }
    }
    return (elm);
}

IMXNODE *imx_prev(IMXNODE *elm)
{
    IMXNODE *pt = NULL, *ppt = NULL;
    if (IMX_LEFT(elm))
    {
        elm = IMX_LEFT(elm);
        while (IMX_RIGHT(elm))
            elm = IMX_RIGHT(elm);
    } 
    else
    {
        if (IMX_PARENT(elm) &&
                (elm == IMX_RIGHT(IMX_PARENT(elm))))
            elm = IMX_PARENT(elm);
        else
        {
            if((pt = IMX_PARENT(elm)) && (elm == IMX_LEFT(pt)) 
                    && (ppt = IMX_PARENT(pt)) && pt == IMX_RIGHT(ppt)) 
            {
                elm = ppt;
            }
            //else root node
        }
    }
    return (elm);
}

IMXNODE *imx_minmax(IMMX *map, int val)
{
    IMXNODE *tmp = IMX_ROOT(map), *parent = NULL;
    while (tmp)
    {
        parent = tmp;
        if (val < 0)
            tmp = IMX_LEFT(parent);
        else
            tmp = IMX_RIGHT(parent);
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
    int i = 0, key = 0, count = 10000, ret = 0;
    void *map = NULL, *timer = NULL;

    if((map = IMMX_INIT()))
    {
        TIMER_INIT(timer);
        while(i++ < count)
        {
            key = random()%count;
            IMMX_ADD(map, key);
            //if(olddp)fprintf(stdout, "old[%d:%08x]\n", key, olddp);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "insert %d nodes count:%d free:%d total:%d time used:%lld\n", count,  PIMX(map)->count, PIMX(map)->free_count, PIMX(map)->total, PT_LU_USEC(timer));
        i = 0;
        while(i++ < count)
        {
            key = random()%count;
            ret = 0;
            IMMX_GET(map, key, ret);
            if(ret) fprintf(stdout, "%d:[%d]\n", key, ret);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "get %d nodes count:%d free:%d total:%d time used:%lld\n", count,  PIMX(map)->count, PIMX(map)->free_count, PIMX(map)->total, PT_LU_USEC(timer));
        do
        {
            ret = 0;
            IMMX_POP_MIN(map, key, ret);
            if(ret) fprintf(stdout, "%d:[%d]\n", key, ret); 
        }while(IMX_ROOT(map));
        TIMER_SAMPLE(timer);
        fprintf(stdout, "pop min(%d) node count:%d free:%d total:%d time used:%lld\n", count,  PIMX(map)->count, PIMX(map)->free_count, PIMX(map)->total, PT_LU_USEC(timer));
        i = 0;
        while(i < count)
        {
            key = random()%count;
            IMMX_ADD(map, key);
            ++i;
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "insert %d nodes count:%d free:%d total:%d time used:%lld\n", count,  PIMX(map)->count, PIMX(map)->free_count, PIMX(map)->total, PT_LU_USEC(timer));
        do
        {
            ret = 0;
            IMMX_POP_MAX(map, key, ret);
            if(ret) fprintf(stdout, "%d:[%d]\n", key, ret); 
        }while(IMX_ROOT(map));
        TIMER_SAMPLE(timer);
        fprintf(stdout, "pop max(%d) nodes count:%d free:%d total:%d time used:%lld\n", count,  PIMX(map)->count, PIMX(map)->free_count, PIMX(map)->total, PT_LU_USEC(timer));
        TIMER_SAMPLE(timer);
        TIMER_CLEAN(timer);
        IMMX_CLEAN(map);
    }
}
#endif
