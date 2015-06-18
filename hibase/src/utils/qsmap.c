#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include "qsmap.h"
void qs_insert_color(QSMAP *map, QSNODE *elm)
{
    QSNODE *parent, *gparent, *tmp;
    while ((parent = QS_PARENT(elm)) &&
            QS_COLOR(parent) == QS_RED)
    {
        gparent = QS_PARENT(parent);
        if (parent == QS_LEFT(gparent))
        {
            tmp = QS_RIGHT(gparent);
            if (tmp && QS_COLOR(tmp) == QS_RED)
            {
                QS_COLOR(tmp) = QS_BLACK;
                QS_SET_BLACKRED(parent, gparent);
                elm = gparent;
                continue;
            }
            if (QS_RIGHT(parent) == elm)
            {
                QS_ROTATE_LEFT(map, parent, tmp);
                tmp = parent;
                parent = elm;
                elm = tmp;
            }
            QS_SET_BLACKRED(parent, gparent);
            QS_ROTATE_RIGHT(map, gparent, tmp);
        } 
        else
        {
            tmp = QS_LEFT(gparent);
            if (tmp && QS_COLOR(tmp) == QS_RED)
            {
                QS_COLOR(tmp) = QS_BLACK;
                QS_SET_BLACKRED(parent, gparent);
                elm = gparent;
                continue;
            }
            if (QS_LEFT(parent) == elm)
            {
                QS_ROTATE_RIGHT(map, parent, tmp);
                tmp = parent;
                parent = elm;
                elm = tmp;
            }
            QS_SET_BLACKRED(parent, gparent);
            QS_ROTATE_LEFT(map, gparent, tmp);
        }
    }
    QS_COLOR(map->rbh_root) = QS_BLACK;
}

void qs_remove_color(QSMAP *map, QSNODE *parent, QSNODE *elm)
{
    QSNODE *tmp;
    while ((elm == NULL || QS_COLOR(elm) == QS_BLACK) &&
            elm != QS_ROOT(map))
    {
        if (QS_LEFT(parent) == elm)
        {
            tmp = QS_RIGHT(parent);
            if (QS_COLOR(tmp) == QS_RED)
            {
                QS_SET_BLACKRED(tmp, parent);
                QS_ROTATE_LEFT(map, parent, tmp);
                tmp = QS_RIGHT(parent);
            }
            if ((QS_LEFT(tmp) == NULL ||
                        QS_COLOR(QS_LEFT(tmp)) == QS_BLACK) &&
                    (QS_RIGHT(tmp) == NULL ||
                     QS_COLOR(QS_RIGHT(tmp)) == QS_BLACK))
            {
                QS_COLOR(tmp) = QS_RED;
                elm = parent;
                parent = QS_PARENT(elm);
            }
            else
            {
                if (QS_RIGHT(tmp) == NULL ||
                        QS_COLOR(QS_RIGHT(tmp)) == QS_BLACK)
                {
                    QSNODE *oleft;
                    if ((oleft = QS_LEFT(tmp)))
                        QS_COLOR(oleft) = QS_BLACK;
                    QS_COLOR(tmp) = QS_RED;
                    QS_ROTATE_RIGHT(map, tmp, oleft);
                    tmp = QS_RIGHT(parent);
                }
                QS_COLOR(tmp) = QS_COLOR(parent);
                QS_COLOR(parent) = QS_BLACK;
                if (QS_RIGHT(tmp))
                    QS_COLOR(QS_RIGHT(tmp)) = QS_BLACK;
                QS_ROTATE_LEFT(map, parent, tmp);
                elm = QS_ROOT(map);
                break;
            }
        } 
        else
        {
            tmp = QS_LEFT(parent);
            if (QS_COLOR(tmp) == QS_RED)
            {
                QS_SET_BLACKRED(tmp, parent);
                QS_ROTATE_RIGHT(map, parent, tmp);
                tmp = QS_LEFT(parent);
            }
            if ((QS_LEFT(tmp) == NULL ||
                        QS_COLOR(QS_LEFT(tmp)) == QS_BLACK) &&
                    (QS_RIGHT(tmp) == NULL ||
                     QS_COLOR(QS_RIGHT(tmp)) == QS_BLACK))
            {
                QS_COLOR(tmp) = QS_RED;
                elm = parent;
                parent = QS_PARENT(elm);
            } 
            else
            {
                if (QS_LEFT(tmp) == NULL ||
                        QS_COLOR(QS_LEFT(tmp)) == QS_BLACK)
                {
                    QSNODE *oright;
                    if ((oright = QS_RIGHT(tmp)))
                        QS_COLOR(oright) = QS_BLACK;
                    QS_COLOR(tmp) = QS_RED;
                    QS_ROTATE_LEFT(map, tmp, oright);
                    tmp = QS_LEFT(parent);
                }
                QS_COLOR(tmp) = QS_COLOR(parent);
                QS_COLOR(parent) = QS_BLACK;
                if (QS_LEFT(tmp))
                    QS_COLOR(QS_LEFT(tmp)) = QS_BLACK;
                QS_ROTATE_RIGHT(map, parent, tmp);
                elm = QS_ROOT(map);
                break;
            }
        }
    }
    if (elm)
        QS_COLOR(elm) = QS_BLACK;
}

QSNODE *qs_remove(QSMAP *map, QSNODE *elm)
{
    QSNODE *child, *parent, *old = elm;
    int color;

    QSMAP_MINMAX_REBUILD(map, elm);

    if (QS_LEFT(elm) == NULL)
        child = QS_RIGHT(elm);
    else if (QS_RIGHT(elm) == NULL)
        child = QS_LEFT(elm);
    else
    {
        QSNODE *left;
        elm = QS_RIGHT(elm);
        while ((left = QS_LEFT(elm)))
            elm = left;
        child = QS_RIGHT(elm);
        parent = QS_PARENT(elm);
        color = QS_COLOR(elm);
        if (child)
            QS_PARENT(child) = parent;
        if (parent)
        {
            if (QS_LEFT(parent) == elm)
                QS_LEFT(parent) = child;
            else
                QS_RIGHT(parent) = child;
            QS_AUGMENT(parent);
        } 
        else
            QS_ROOT(map) = child;
        if (QS_PARENT(elm) == old)
            parent = elm;
        QS_NODE_SET(elm, old);
        if (QS_PARENT(old))
        {
            if (QS_LEFT(QS_PARENT(old)) == old)
                QS_LEFT(QS_PARENT(old)) = elm;
            else
                QS_RIGHT(QS_PARENT(old)) = elm;
            QS_AUGMENT(QS_PARENT(old));
        } 
        else
            QS_ROOT(map) = elm;
        QS_PARENT(QS_LEFT(old)) = elm;
        if (QS_RIGHT(old))
            QS_PARENT(QS_RIGHT(old)) = elm;
        if (parent)
        {
            left = parent;
            do
            {
                QS_AUGMENT(left);
            } while ((left = QS_PARENT(left)));
        }
        goto color;
    }
    parent = QS_PARENT(elm);
    color = QS_COLOR(elm);
    if (child)
        QS_PARENT(child) = parent;
    if (parent)
    {
        if (QS_LEFT(parent) == elm)
            QS_LEFT(parent) = child;
        else
            QS_RIGHT(parent) = child;
        QS_AUGMENT(parent);
    } 
    else
        QS_ROOT(map) = child;
color:
    if (color == QS_BLACK)
        qs_remove_color(map, parent, child);
    return (old);
}

/* Inserts a node into the QS tree */
QSNODE *qs_insert(QSMAP *map, QSNODE *elm)
{
    QSNODE *tmp;
    QSNODE *parent = NULL;
    tmp = QS_ROOT(map);
    while (tmp)
    {
        parent = tmp;
        if (elm->key < parent->key)
            tmp = QS_LEFT(tmp);
        else 
            tmp = QS_RIGHT(tmp);
    }
    QS_SET(elm, parent);
    if (parent != NULL)
    {
        if (elm->key < parent->key)
            QS_LEFT(parent) = elm;
        else
            QS_RIGHT(parent) = elm;
        QS_AUGMENT(parent);
    } 
    else
        QS_ROOT(map) = elm;
    qs_insert_color(map, elm);
    return (NULL);
}

QSNODE *qs_find(QSMAP *map, QSNODE *elm)
{
    QSNODE *tmp = QS_ROOT(map);
    while (tmp)
    {
        if (elm->key < tmp->key)
            tmp = QS_LEFT(tmp);
        else if (elm->key > tmp->key)
            tmp = QS_RIGHT(tmp);
        else
            return (tmp);
    }
    return (NULL);
}

QSNODE *qs_next(QSNODE *elm)
{
    QSNODE *pt = NULL, *ppt = NULL;
    if (QS_RIGHT(elm))
    {
        elm = QS_RIGHT(elm);
        while (QS_LEFT(elm))
            elm = QS_LEFT(elm);
    } else
    {
        if (QS_PARENT(elm) &&
                (elm == QS_LEFT(QS_PARENT(elm))))
            elm = QS_PARENT(elm);
        else
        {
            if((pt = QS_PARENT(elm)) && (elm == QS_RIGHT(pt))
                    && (ppt = QS_PARENT(pt)) && pt == QS_LEFT(ppt))
            {
                elm = ppt;
            }
        }
    }
    return (elm);
}

QSNODE *qs_prev(QSNODE *elm)
{
    QSNODE *pt = NULL, *ppt = NULL;
    if (QS_LEFT(elm))
    {
        elm = QS_LEFT(elm);
        while (QS_RIGHT(elm))
            elm = QS_RIGHT(elm);
    } 
    else
    {
        if (QS_PARENT(elm) &&
                (elm == QS_RIGHT(QS_PARENT(elm))))
            elm = QS_PARENT(elm);
        else
        {
            if((pt = QS_PARENT(elm)) && (elm == QS_LEFT(pt))
                    && (ppt = QS_PARENT(pt)) && pt == QS_RIGHT(ppt))
            {
                elm = ppt;
            }
        }
    }
    return (elm);
}

QSNODE *qs_minmax(QSMAP *map, int val)
{
    QSNODE *tmp = QS_ROOT(map);
    QSNODE *parent = NULL;
    while (tmp)
    {
        parent = tmp;
        if (val < 0)
            tmp = QS_LEFT(tmp);
        else
            tmp = QS_RIGHT(tmp);
    }
    return (parent);
}

#ifdef _DEBUG_QSMAP
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rbmap.h"
#include "timer.h"

int main()
{
    void *map = NULL, *timer = NULL, *dp = NULL, *olddp = NULL;
    long i = 0, key = 0, count = 10000;

    if((map = QSMAP_INIT()))
    {
        TIMER_INIT(timer);
        while(i < count)
        {
            key = random()%count;
            dp = (void *)++i;
            QSMAP_ADD(map, key, dp, olddp);
            //if(olddp)fprintf(stdout, "old[%d:%08x]\n", key, olddp);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "insert %d nodes count:%d free:%d total:%d time used:%lld\n", count,  PQS(map)->count, PQS(map)->free_count, PQS(map)->total, PT_LU_USEC(timer));
        i = 0;
        while(i < count)
        {
            key = i++;
            dp = NULL;
            QSMAP_GET(map, key, dp);
            //if(dp)fprintf(stdout, "%ld:[%ld]\n", key, (long)dp);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "get %d nodes count:%d free:%d total:%d time used:%lld\n", count,  PQS(map)->count, PQS(map)->free_count, PQS(map)->total, PT_LU_USEC(timer));
        do
        {
            QSMAP_POP_MIN(map, key, dp);
            //if(dp) fprintf(stdout, "%ld:[%ld]\n", key, (long)dp); 
        }while(QS_ROOT(map));
        TIMER_SAMPLE(timer);
        fprintf(stdout, "pop min(%d) node count:%d free:%d total:%d time used:%lld\n", count,  PQS(map)->count, PQS(map)->free_count, PQS(map)->total, PT_LU_USEC(timer));
        i = 0;
        while(i < count)
        {
            key = random()%count;
            dp = (void *)i;
            QSMAP_ADD(map, key, dp, olddp);
            ++i;
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "insert %d nodes count:%d free:%d total:%d time used:%lld\n", count,  PQS(map)->count, PQS(map)->free_count, PQS(map)->total, PT_LU_USEC(timer));
        do
        {
            QSMAP_POP_MAX(map, key, dp);
            //if(dp) fprintf(stdout, "%ld:[%ld]\n", key, (long)dp); 
        }while(QS_ROOT(map));
        TIMER_SAMPLE(timer);
        fprintf(stdout, "pop max(%d) nodes count:%d free:%d total:%d time used:%lld\n", count,  PQS(map)->count, PQS(map)->free_count, PQS(map)->total, PT_LU_USEC(timer));
        TIMER_SAMPLE(timer);
        TIMER_CLEAN(timer);
        /*
        */
        QSMAP_CLEAN(map);
    }
}
#endif
