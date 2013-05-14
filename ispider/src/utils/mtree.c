#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include "mtree.h"
#define LL(xxxx) ((long long int) xxxx)
int mtree_add(MTREE *mtree, int key, int data, int *old)
{
    MTREENODE *xp = NULL, *node = NULL;
    int ret = -1;

    if(mtree)
    {
        if((xp = mtree->root) && mtree->total > 0 && mtree->kmin && mtree->kmax)
        {
            if(key == mtree->kmin->key)
            {
                if(old) *old = mtree->kmin->data;
                ret = 1;
            }
            else if(key == mtree->kmax->key)
            {
                if(old) *old = mtree->kmax->data;
                ret = 1;
            }
            else if(key < mtree->kmin->key)
            {
                MTREENODE_POP(mtree, node);
                node->key = key;
                node->data = data;
                xp = mtree->kmin;
                node->parent = xp;
                node->left = node->right = NULL;
                xp->left = node;
                /*
                if(xp->right == NULL)
                {
                    if((node->parent = xp->parent))
                        node->parent->left = node;
                    else 
                        mtree->root = node;
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
                mtree->kmin = node;
                ++(mtree->total);
                ret = 0;
            }
            else if(key > mtree->kmax->key)
            {
                MTREENODE_POP(mtree, node);
                node->key = key;
                node->data = data;
                xp = mtree->kmax;
                node->parent = xp;
                node->left = node->right = NULL;
                xp->right = node;
                /*
                if(xp->left == NULL)
                {
                    if((node->parent = xp->parent))
                        node->parent->right = node;
                    else
                        mtree->root = node;
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
                mtree->kmax = node;
                ++(mtree->total);
                ret = 0;
                //if(mtree->kmin == NULL){fprintf(stdout, "%s::%d total:%d root:%p\n", __FILE__, __LINE__, mtree->total, mtree->root);_exit(-1);}
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
                            MTREENODE_POP(mtree, node);
                            node->parent = xp;
                            xp->right = node;
                            node->key = key;
                            node->data = data;
                            node->left = NULL;
                            node->right = NULL;
                            ++(mtree->total);
                            ret = 0;
                            break;
                        }
                    }
                    else 
                    {
                        if(xp->left) xp = xp->left;
                        else
                        {
                            MTREENODE_POP(mtree, node);
                            xp->left = node;
                            node->parent = xp;
                            node->key = key;
                            node->data = data;
                            node->left = NULL;
                            node->right = NULL;
                            ++(mtree->total);
                            ret = 0;
                            break;
                        }
                    }
                }while(xp);
            }
            //if(mtree->kmin == NULL){fprintf(stdout, "%s::%d total:%d root:%p\n", __FILE__, __LINE__, mtree->total, mtree->root);_exit(-1);}
        }
        else
        {
            MTREENODE_POP(mtree, node);
            node->key = key;
            node->data = data;
            mtree->root = mtree->kmin = mtree->kmax = node;
            node->parent = NULL;
            node->left = NULL;
            node->right = NULL;
            ++(mtree->total);
            ret = 0;
        }
    }
    return ret;
}//while(0)

int mtree_push(MTREE *mtree, int key, int data)
{
    MTREENODE *xp = NULL, *node = NULL;
    int ret = -1;

    if(mtree)
    {
        if((xp = mtree->root) && mtree->total > 0 && mtree->kmin && mtree->kmax)
        {
            //fprintf(stdout, "%s::%d key:%d min:%d max:%d total:%d\n", __FILE__, __LINE__, key, mtree->kmin->key, mtree->kmax->key, mtree->total);
            if(key <= mtree->kmin->key)
            {
                MTREENODE_POP(mtree, node);
                node->key = key;
                node->data = data;
                xp = mtree->kmin;
                node->parent = xp;
                node->left = node->right = NULL;
                if(mtree->kmin->key == key && xp->right == NULL)
                {
                    if(mtree->kmax == xp) mtree->kmax = node;
                    xp->right = node;
                }
                else
                {
                    xp->left = node;
                    mtree->kmin = node;
                }
                /*
                if(xp->right == NULL)
                {
                    if((node->parent = xp->parent))
                        node->parent->left = node;
                    else 
                        mtree->root = node;
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
                ++(mtree->total);
                ret = 0;
            }
            else if(key >= mtree->kmax->key)
            {
                MTREENODE_POP(mtree, node);
                node->key = key;
                node->data = data;
                xp = mtree->kmax;
                node->parent = xp;
                node->left = node->right = NULL;
                if(mtree->kmax->key == key && xp->left == NULL)
                {
                    if(mtree->kmin == xp) mtree->kmin = node;
                    xp->left = node;
                }
                else
                {
                    xp->right = node;
                    mtree->kmax = node;
                }
                /*
                if(xp->left == NULL)
                {
                    if((node->parent = xp->parent))
                        node->parent->right = node;
                    else
                        mtree->root = node;
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
                ++(mtree->total);
                ret = 0;
                //if(mtree->kmin == NULL){fprintf(stdout, "%s::%d total:%d root:%p\n", __FILE__, __LINE__, mtree->total, mtree->root);_exit(-1);}
            }
            else
            {
                do
                {
                    if(key == xp->key)
                    {
                        if(xp->left == NULL || xp->right == NULL)
                        {
                            MTREENODE_POP(mtree, node);
                            node->parent = xp;
                            if(xp->right == NULL)
                                xp->right = node;
                            else
                                xp->left = node;
                            node->key = key;
                            node->data = data;
                            node->left = NULL;
                            node->right = NULL;
                            ++(mtree->total);
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
                            MTREENODE_POP(mtree, node);
                            node->parent = xp;
                            xp->right = node;
                            node->key = key;
                            node->data = data;
                            node->left = NULL;
                            node->right = NULL;
                            ++(mtree->total);
                            ret = 0;
                            break;
                        }
                    }
                    else 
                    {
                        if(xp->left) xp = xp->left;
                        else
                        {
                            MTREENODE_POP(mtree, node);
                            xp->left = node;
                            node->parent = xp;
                            node->key = key;
                            node->data = data;
                            node->left = NULL;
                            node->right = NULL;
                            ++(mtree->total);
                            ret = 0;
                            break;
                        }
                    }
                }while(xp);
            }
            //if(mtree->kmin == NULL){fprintf(stdout, "%s::%d total:%d root:%p\n", __FILE__, __LINE__, mtree->total, mtree->root);_exit(-1);}
        }
        else
        {
            MTREENODE_POP(mtree, node);
            node->key = key;
            node->data = data;
            mtree->root = mtree->kmin = mtree->kmax = node;
            node->parent = NULL;
            node->left = NULL;
            node->right = NULL;
            ++(mtree->total);
            ret = 0;
        }
    }
    return ret;
}//while(0)

int mtree_get(MTREE *mtree, int key, int *data)
{
    MTREENODE *xp = NULL;
    int ret = -1;

    if(mtree)
    {
        if((xp = mtree->root) && mtree->total > 0 && mtree->kmin && mtree->kmax)
        {
            //fprintf(stdout, "%s::%d key:%d min:%d max:%d total:%d\n", __FILE__, __LINE__, key, mtree->kmin->key, mtree->kmax->key, mtree->total);
            if(key == mtree->kmin->key)
            {
                if(data) *data = mtree->kmin->data;
                ret = 0;
            }
            else if(key == mtree->kmax->key)
            {
                if(data) *data = mtree->kmax->data;
                ret = 0;
            }
            else if(key < mtree->kmin->key || key >  mtree->kmax->key)
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
int mtree_pop_min(MTREE *mtree, int *key, int *data)
{
    MTREENODE *xp = NULL, *node = NULL;
    int ret = -1;

    if(mtree)
    {
        if((xp = mtree->kmin))
        {
            if(key) *key = xp->key;
            if(data) *data = xp->data;
            if((node = xp->right))
            {
                if((node->parent = xp->parent)) 
                    xp->parent->left = node;
                else 
                    mtree->root = node;
                while(node && node->left) node = node->left;
                mtree->kmin = node;
                MTREENODE_PUSH(mtree, xp);
            }
            else
            {
                mtree->kmin = xp->parent;
                if(xp->parent) xp->parent->left = NULL;
                MTREENODE_PUSH(mtree, xp);
            }
            if(--(mtree->total) == 0)
                mtree->root = mtree->kmin = mtree->kmax = NULL;
            ret = 0;
        }
    }
    return ret;
}

/* pop max key */
int mtree_pop_max(MTREE *mtree, int *key, int *data)
{
    MTREENODE *xp = NULL, *node = NULL;
    int ret = -1;

    if(mtree)
    {
        if((xp = mtree->kmax))
        {
            if(key ) *key = xp->key;
            if(data) *data = xp->data;
            if((node = xp->left))
            {
                if((node->parent = xp->parent)) 
                    xp->parent->right = node;
                else 
                    mtree->root = node;
                while(node && node->right) node = node->right;
                mtree->kmax = node;
                MTREENODE_PUSH(mtree, xp);
            }
            else
            {
                mtree->kmax = xp->parent;
                if(xp->parent) xp->parent->right = NULL;
                MTREENODE_PUSH(mtree, xp);
            }
            if(--(mtree->total) == 0)
                mtree->root = mtree->kmin = mtree->kmax = NULL;
            ret = 0;
        }
    }
    return ret;
}

void mtree_reset(MTREE *mtree)
{

    if(mtree && mtree->total > 0 && mtree->root &&  mtree->kmin && mtree->kmax)
    {
        while(mtree_pop_min(mtree, NULL, NULL) == 0);
        mtree->root = mtree->kmin = mtree->kmax = NULL;
    }
    return ;
}

void mtree_clean(MTREE *mtree)
{
    int i = 0;

    if(mtree)
    {
        mtree_reset(mtree);
        for(i = 0; i <  mtree->nlines; i++)
        {
            free(mtree->lines[i]);
        }
        free(mtree);
    }
    return ;
}

MTREE *mtree_init()
{
    MTREE *mtree = NULL;
    MTREENODE *tmp = NULL;
    int i = 0;

    if((mtree = (MTREE *)calloc(1, sizeof(MTREE))))
    {
        for(i = 0; i < MTREENODE_LINE_NUM; i++)
        {
            tmp = &(mtree->init[i]);
            MTREENODE_PUSH(mtree, tmp);
        }
    }
    return mtree;
}

#ifdef _DEBUG_MTREE
int main()
{
    int key = 0, data = 0, last = 0, old = 0;
    int i = 0, x = 0, count = 500000;
    MTREE *mtree = NULL;
    
    if((mtree = mtree_init()))
    {
        for(i = 0; i < count; i++)
        {
            key = (int)(rand()%count);
            data = (int)i;
            if(mtree_add(mtree, key, data, &old) == 1)
            {
                fprintf(stdout, "%s::%d key:%lld old:%lld\n", __FILE__, __LINE__, LL(key), LL(old));
                ++x;
            }
            else
            {
                //fprintf(stdout, "%s::%d %d:%d\n", __FILE__, __LINE__, key, i);
            }
        }
        fprintf(stdout, "%s::%d min:%lld max:%lld count:%d repeat:%d\n", __FILE__, __LINE__, LL(mtree->kmin->key), LL(mtree->kmax->key), mtree->total, x);
        i = 0;
        while(mtree_pop_min(mtree, &key, &data) == 0)
        {
            if(key < last)
            {
                fprintf(stdout, "%s::%d i:%d %lld:%lld last:%d\n", __FILE__, __LINE__, i, LL(key), LL(data), last);
                _exit(-1);
            }
            i++;
            last = key;
        }
        mtree_reset(mtree);
        //push 
        for(i = 0; i < count; i++)
        {
            key = (int) i % count;
            data = (int) i;
            mtree_push(mtree, key, data);
        }
        fprintf(stdout, "%s::%d min:%lld max:%lld count:%d\n", __FILE__, __LINE__, LL(mtree->kmin->key), LL(mtree->kmax->key), mtree->total);
        i = 0;
        last = 0;
        while(mtree_pop_min(mtree, &key, &data) == 0)
        {
            if(key < last)
            {
                fprintf(stdout, "%s::%d i:%d %lld:%lld last:%d\n", __FILE__, __LINE__, i, LL(key), LL(data), last);
                _exit(-1);
            }
            i++;
            last = key;
        }
        mtree_clean(mtree);
    }
    return 0; 
}
//gcc -o mtr mtree.c -D_DEBUG_MTREE &&  ./mtr
#endif
