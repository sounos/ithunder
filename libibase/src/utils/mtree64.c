#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include "mtree64.h"
#define LL(xxxx) ((long long int) xxxx)
int mtree64_add(MTREE64 *mtree64, int64_t key, int64_t data, int64_t *old)
{
    MTRNODE64 *xp = NULL, *node = NULL;
    int ret = -1;

    if(mtree64)
    {
        if((xp = mtree64->root) && mtree64->total > 0 && mtree64->kmin && mtree64->kmax)
        {
            //fprintf(stdout, "%s::%d key:%d min:%d max:%d total:%d\n", __FILE__, __LINE__, key, mtree64->kmin->key, mtree64->kmax->key, mtree64->total);
            if(key == mtree64->kmin->key)
            {
                if(old) *old = mtree64->kmin->data;
                ret = 1;
            }
            else if(key == mtree64->kmax->key)
            {
                if(old) *old = mtree64->kmax->data;
                ret = 1;
            }
            else if(key < mtree64->kmin->key)
            {
                MTRNODE64_POP(mtree64, node);
                node->key = key;
                node->data = data;
                xp = mtree64->kmin;
                node->parent = xp;
                node->left = node->right = NULL;
                xp->left = node;
                /*
                if(xp->right == NULL)
                {
                    if((node->parent = xp->parent))
                        node->parent->left = node;
                    else 
                        mtree64->root = node;
                    xp->parent = node;
                    node->right = xp;
                    node->left = NULL;
                }
                else
                {
                    node->left = node->right = NULL;
                    node->parent = xp;
                    xp->left = node;
                }
                */
                mtree64->kmin = node;
                ++(mtree64->total);
                ret = 0;
            }
            else if(key > mtree64->kmax->key)
            {
                MTRNODE64_POP(mtree64, node);
                node->key = key;
                node->data = data;
                xp = mtree64->kmax;
                node->parent = xp;
                node->left = node->right = NULL;
                xp->right = node;
                /*
                if(xp->left == NULL)
                {
                    if((node->parent = xp->parent))
                        node->parent->right = node;
                    else
                        mtree64->root = node;
                    xp->parent = node;
                    node->left = xp;
                    node->right = NULL;
                }
                else
                {
                    node->left = node->right = NULL;
                    node->parent = xp;
                    xp->right = node;
                }
                */
                mtree64->kmax = node;
                ++(mtree64->total);
                ret = 0;
                //if(mtree64->kmin == NULL){fprintf(stdout, "%s::%d total:%d root:%p\n", __FILE__, __LINE__, mtree64->total, mtree64->root);_exit(-1);}
            }
            else
            {
                do
                {
                    if(key == xp->key)
                    {
                        if(old) *old = xp->data;
                        ret = 1;
                        break;
                    }
                    else if(key > xp->key) 
                    {
                        if(xp->right) xp = xp->right;
                        else
                        {
                            MTRNODE64_POP(mtree64, node);
                            node->parent = xp;
                            xp->right = node;
                            node->key = key;
                            node->data = data;
                            node->left = NULL;
                            node->right = NULL;
                            ++(mtree64->total);
                            ret = 0;
                            break;
                        }
                    }
                    else 
                    {
                        if(xp->left) xp = xp->left;
                        else
                        {
                            MTRNODE64_POP(mtree64, node);
                            xp->left = node;
                            node->parent = xp;
                            node->key = key;
                            node->data = data;
                            node->left = NULL;
                            node->right = NULL;
                            ++(mtree64->total);
                            ret = 0;
                            break;
                        }
                    }
                }while(xp);
            }
            //if(mtree64->kmin == NULL){fprintf(stdout, "%s::%d total:%d root:%p\n", __FILE__, __LINE__, mtree64->total, mtree64->root);_exit(-1);}
        }
        else
        {
            MTRNODE64_POP(mtree64, node);
            node->key = key;
            node->data = data;
            mtree64->root = mtree64->kmin = mtree64->kmax = node;
            node->parent = NULL;
            node->left = NULL;
            node->right = NULL;
            ++(mtree64->total);
            ret = 0;
        }
    }
    return ret;
}//while(0)

int mtree64_push(MTREE64 *mtree64, int64_t key, int64_t data)
{
    MTRNODE64 *xp = NULL, *node = NULL;
    int ret = -1;

    if(mtree64)
    {
        if((xp = mtree64->root) && mtree64->total > 0 && mtree64->kmin && mtree64->kmax)
        {
            //fprintf(stdout, "%s::%d key:%d min:%d max:%d total:%d\n", __FILE__, __LINE__, key, mtree64->kmin->key, mtree64->kmax->key, mtree64->total);
            if(key <= mtree64->kmin->key)
            {
                MTRNODE64_POP(mtree64, node);
                node->key = key;
                node->data = data;
                xp = mtree64->kmin;
                node->parent = xp;
                node->left = node->right = NULL;
                if(mtree64->kmin->key == key && xp->right == NULL)
                {
                    if(mtree64->kmax == xp) mtree64->kmax = node;
                    xp->right = node;
                }
                else
                {
                    xp->left = node;
                    mtree64->kmin = node;
                }
                /*
                if(xp->right == NULL)
                {
                    if((node->parent = xp->parent))
                        node->parent->left = node;
                    else 
                        mtree64->root = node;
                    xp->parent = node;
                    node->right = xp;
                    node->left = NULL;
                }
                else
                {
                    node->left = node->right = NULL;
                    node->parent = xp;
                    xp->left = node;
                }
                */
                ++(mtree64->total);
                ret = 0;
            }
            else if(key >= mtree64->kmax->key)
            {
                MTRNODE64_POP(mtree64, node);
                node->key = key;
                node->data = data;
                xp = mtree64->kmax;
                node->parent = xp;
                node->left = node->right = NULL;
                if(mtree64->kmax->key == key && xp->left == NULL)
                {
                    if(mtree64->kmin == xp) mtree64->kmin = node;
                    xp->left = node;
                }
                else
                {
                    xp->right = node;
                    mtree64->kmax = node;
                }
                /*
                if(xp->left == NULL)
                {
                    if((node->parent = xp->parent))
                        node->parent->right = node;
                    else
                        mtree64->root = node;
                    xp->parent = node;
                    node->left = xp;
                    node->right = NULL;
                }
                else
                {
                    node->left = node->right = NULL;
                    node->parent = xp;
                    xp->right = node;
                }
                */
                ++(mtree64->total);
                ret = 0;
                //if(mtree64->kmin == NULL){fprintf(stdout, "%s::%d total:%d root:%p\n", __FILE__, __LINE__, mtree64->total, mtree64->root);_exit(-1);}
            }
            else
            {
                do
                {
                    if(key == xp->key)
                    {
                        if(xp->left == NULL || xp->right == NULL)
                        {
                            MTRNODE64_POP(mtree64, node);
                            node->parent = xp;
                            if(xp->right == NULL)
                                xp->right = node;
                            else
                                xp->left = node;
                            node->key = key;
                            node->data = data;
                            node->left = NULL;
                            node->right = NULL;
                            ++(mtree64->total);
                            ret = 0;
                            break;
                        }
                        else 
                            xp = xp->left;
                    }
                    else if(key > xp->key) 
                    {
                        if(xp->right) xp = xp->right;
                        else
                        {
                            MTRNODE64_POP(mtree64, node);
                            node->parent = xp;
                            xp->right = node;
                            node->key = key;
                            node->data = data;
                            node->left = NULL;
                            node->right = NULL;
                            ++(mtree64->total);
                            ret = 0;
                            break;
                        }
                    }
                    else 
                    {
                        if(xp->left) xp = xp->left;
                        else
                        {
                            MTRNODE64_POP(mtree64, node);
                            xp->left = node;
                            node->parent = xp;
                            node->key = key;
                            node->data = data;
                            node->left = NULL;
                            node->right = NULL;
                            ++(mtree64->total);
                            ret = 0;
                            break;
                        }
                    }
                }while(xp);
            }
            //if(mtree64->kmin == NULL){fprintf(stdout, "%s::%d total:%d root:%p\n", __FILE__, __LINE__, mtree64->total, mtree64->root);_exit(-1);}
        }
        else
        {
            MTRNODE64_POP(mtree64, node);
            node->key = key;
            node->data = data;
            mtree64->root = mtree64->kmin = mtree64->kmax = node;
            node->parent = NULL;
            node->left = NULL;
            node->right = NULL;
            ++(mtree64->total);
            ret = 0;
        }
    }
    return ret;
}//while(0)

int mtree64_get(MTREE64 *mtree64, int64_t key, int64_t *data)
{
    MTRNODE64 *xp = NULL;
    int ret = -1;

    if(mtree64)
    {
        if((xp = mtree64->root) && mtree64->total > 0 && mtree64->kmin && mtree64->kmax)
        {
            //fprintf(stdout, "%s::%d key:%d min:%d max:%d total:%d\n", __FILE__, __LINE__, key, mtree64->kmin->key, mtree64->kmax->key, mtree64->total);
            if(key == mtree64->kmin->key)
            {
                if(data) *data = mtree64->kmin->data;
                ret = 0;
            }
            else if(key == mtree64->kmax->key)
            {
                if(data) *data = mtree64->kmax->data;
                ret = 0;
            }
            else if(key < mtree64->kmin->key || key >  mtree64->kmax->key)
            {
                ret = -2;
            }
            else
            {
                do
                {
                    if(key == xp->key)
                    {
                        if(data) *data = xp->data;
                        ret = 0;
                        break;
                    }
                    else if(key > xp->key) 
                    {
                        xp = xp->right;
                    }
                    else 
                    {
                        xp = xp->left;
                    }
                }while(xp);
            }
        }
    }
    return ret;
}//while(0)



/* pop min key */
int mtree64_pop_min(MTREE64 *mtree64, int64_t *key, int64_t *data)
{
    MTRNODE64 *xp = NULL, *node = NULL;
    int ret = -1;

    if(mtree64)
    {
        if((xp = mtree64->kmin))
        {
            if(key) *key = xp->key;
            if(data) *data = xp->data;
            if((node = xp->right))
            {
                if((node->parent = xp->parent)) 
                    xp->parent->left = node;
                else 
                    mtree64->root = node;
                while(node && node->left) node = node->left;
                mtree64->kmin = node;
                MTRNODE64_PUSH(mtree64, xp);
            }
            else
            {
                mtree64->kmin = xp->parent;
                if(xp->parent) xp->parent->left = NULL;
                MTRNODE64_PUSH(mtree64, xp);
            }
            if(--(mtree64->total) == 0)
                mtree64->root = mtree64->kmin = mtree64->kmax = NULL;
            ret = 0;
        }
    }
    return ret;
}

/* pop max key */
int mtree64_pop_max(MTREE64 *mtree64, int64_t *key, int64_t *data)
{
    MTRNODE64 *xp = NULL, *node = NULL;
    int ret = -1;

    if(mtree64)
    {
        if((xp = mtree64->kmax))
        {
            if(key ) *key = xp->key;
            if(data) *data = xp->data;
            if((node = xp->left))
            {
                if((node->parent = xp->parent)) 
                    xp->parent->right = node;
                else 
                    mtree64->root = node;
                while(node && node->right) node = node->right;
                mtree64->kmax = node;
                MTRNODE64_PUSH(mtree64, xp);
            }
            else
            {
                mtree64->kmax = xp->parent;
                if(xp->parent) xp->parent->right = NULL;
                MTRNODE64_PUSH(mtree64, xp);
            }
            if(--(mtree64->total) == 0)
                mtree64->root = mtree64->kmin = mtree64->kmax = NULL;
            ret = 0;
        }
    }
    return ret;
}

void mtree64_reset(MTREE64 *mtree64)
{

    if(mtree64 && mtree64->total > 0 && mtree64->root &&  mtree64->kmin && mtree64->kmax)
    {
        while(mtree64_pop_min(mtree64, NULL, NULL) == 0);
        mtree64->root = mtree64->kmin = mtree64->kmax = NULL;
    }
    return ;
}

void mtree64_clean(MTREE64 *mtree64)
{
    int i = 0;

    if(mtree64)
    {
        mtree64_reset(mtree64);
        for(i = 0; i <  mtree64->nlines; i++)
        {
            free(mtree64->lines[i]);
        }
        free(mtree64);
    }
    return ;
}

MTREE64 *mtree64_init()
{
    MTREE64 *mtree64 = NULL;
    MTRNODE64 *tmp = NULL;
    int i = 0;

    if((mtree64 = (MTREE64 *)calloc(1, sizeof(MTREE64))))
    {
        for(i = 0; i < MTRNODE64_LINE_NUM; i++)
        {
            tmp = &(mtree64->init[i]);
            MTRNODE64_PUSH(mtree64, tmp);
        }
    }
    return mtree64;
}

#ifdef _DEBUG_MTREE64
int main()
{
    int64_t key = 0, data = 0, last = 0, old = 0;
    int i = 0, x = 0, count = 500000;
    MTREE64 *mtree64 = NULL;
    
    if((mtree64 = mtree64_init()))
    {
        for(i = 0; i < count; i++)
        {
            key = (int64_t)(rand()%count);
            data = (int64_t)i;
            if(mtree64_add(mtree64, key, data, &old) == 1)
            {
                fprintf(stdout, "%s::%d key:%lld old:%lld\n", __FILE__, __LINE__, LL(key), LL(old));
                ++x;
            }
            else
            {
                //fprintf(stdout, "%s::%d %d:%d\n", __FILE__, __LINE__, key, i);
            }
        }
        fprintf(stdout, "%s::%d min:%lld max:%lld count:%d repeat:%d\n", __FILE__, __LINE__, LL(mtree64->kmin->key), LL(mtree64->kmax->key), mtree64->total, x);
        i = 0;
        while(mtree64_pop_min(mtree64, &key, &data) == 0)
        {
            if(key < last)
            {
                fprintf(stdout, "%s::%d i:%d %lld:%lld last:%d\n", __FILE__, __LINE__, i, LL(key), LL(data), last);
                _exit(-1);
            }
            i++;
            last = key;
        }
        mtree64_reset(mtree64);
        //push 
        for(i = 0; i < count; i++)
        {
            key = (int64_t) i % count;
            data = (int64_t) i;
            mtree64_push(mtree64, key, data);
        }
        fprintf(stdout, "%s::%d min:%lld max:%lld count:%d\n", __FILE__, __LINE__, LL(mtree64->kmin->key), LL(mtree64->kmax->key), mtree64->total);
        i = 0;
        last = 0;
        while(mtree64_pop_min(mtree64, &key, &data) == 0)
        {
            if(key < last)
            {
                fprintf(stdout, "%s::%d i:%d %lld:%lld last:%d\n", __FILE__, __LINE__, i, LL(key), LL(data), last);
                _exit(-1);
            }
            i++;
            last = key;
        }
        mtree64_clean(mtree64);
    }
    return 0; 
}
//gcc -o mtr64 mtree64.c -D_DEBUG_MTREE64 &&  ./mtr64
#endif
