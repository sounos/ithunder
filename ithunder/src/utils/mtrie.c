#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include "mtrie.h"
#include "mutex.h"
#define MTRNODE_COPY(new, old)                                                  \
do                                                                              \
{                                                                               \
    new.key = old.key;                                                          \
    new.nchilds = old.nchilds;                                                  \
    new.data = old.data;                                                        \
    new.childs = old.childs;                                                    \
}while(0)
#define MTRNODE_SETK(node, val)                                                 \
do                                                                              \
{                                                                               \
    node.key = val;                                                             \
    node.nchilds = 0;                                                           \
    node.data = 0;                                                              \
    node.childs = 0;                                                            \
}while(0)
/* initialize mmap */
#define MTRIE_MAP_INIT(x)                                                                   \
do                                                                                          \
{                                                                                           \
    if(x)                                                                                   \
    {                                                                                       \
        if(x->map == NULL)                                                                  \
        {                                                                                   \
            x->size = (off_t)sizeof(MTRSTATE)                                               \
            + (off_t)MTRIE_NODES_MAX * (off_t)sizeof(MTRNODE);                              \
            if((x->map = mmap(NULL, x->size, PROT_READ|PROT_WRITE,                          \
                            MAP_ANON|MAP_PRIVATE, -1, 0)) && x->map != (void *)-1)          \
            {                                                                               \
                x->map_size = x->size;                                                      \
                x->state = (MTRSTATE *)(x->map);                                            \
                memset(x->map, 0,sizeof(MTRSTATE)+(sizeof(MTRNODE)*MTRIE_LINE_MAX));        \
                x->state->total = MTRIE_NODES_MAX;                                          \
                x->state->left = MTRIE_NODES_MAX - MTRIE_LINE_MAX;                          \
                x->state->current = MTRIE_LINE_MAX;                                         \
                x->nodes = (MTRNODE *)((char *)(x->map) + sizeof(MTRSTATE));                \
            }                                                                               \
            else                                                                            \
            {                                                                               \
                x->map = NULL;                                                              \
                x->state = NULL;                                                            \
                x->nodes = NULL;                                                            \
            }                                                                               \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* increment */
#define MTRIE_INCREMENT(x)                                                                  \
do                                                                                          \
{                                                                                           \
    if(x)                                                                                   \
    {                                                                                       \
        x->map_size += (off_t)MTRIE_INCREMENT_NUM * (off_t)sizeof(MTRNODE);                 \
        x->old_map = x->map;                                                                \
        if((x->map = mmap(NULL, x->map_size, PROT_READ|PROT_WRITE,                          \
                        MAP_ANON|MAP_PRIVATE, -1, 0)) && x->map != (void *)-1)              \
        {                                                                                   \
            if(x->old_map) memcpy(x->map, x->old_map, x->size);                             \
            x->state = (MTRSTATE *)(x->map);                                                \
            x->state->total += MTRIE_INCREMENT_NUM;                                         \
            x->state->left += MTRIE_INCREMENT_NUM;                                          \
            x->nodes = (MTRNODE *)((char *)(x->map) + sizeof(MTRSTATE));                    \
        }                                                                                   \
        else                                                                                \
        {                                                                                   \
            x->map = NULL;                                                                  \
            x->state = NULL;                                                                \
            x->nodes = NULL;                                                                \
        }                                                                                   \
        if(x->old_map) munmap(x->old_map, x->size);                                         \
        x->size = x->map_size;                                                              \
        x->old_map = NULL;                                                                  \
    }                                                                                       \
}while(0)

/* push node list */
#define MTRIE_PUSH(x, num, pos)                                                            \
do                                                                                          \
{                                                                                           \
    if(x && pos >= MTRIE_LINE_MAX && num > 0 && num <= MTRIE_LINE_MAX                     \
            && x->state && x->nodes)                                                        \
    {                                                                                       \
        x->nodes[pos].childs = x->state->list[num-1].head;                                  \
        x->state->list[num-1].head = pos;                                                   \
        x->state->list[num-1].count++;                                                      \
    }                                                                                       \
}while(0)

/* pop new nodelist */
#define MTRIE_POP(x, num, pos)                                                             \
do                                                                                          \
{                                                                                           \
    pos = -1;                                                                               \
    if(x && num > 0 && num <= MTRIE_LINE_MAX && x->state && x->nodes)                      \
    {                                                                                       \
        if(x->state->list[num-1].count > 0)                                                 \
        {                                                                                   \
            pos = x->state->list[num-1].head;                                               \
            x->state->list[num-1].head = x->nodes[pos].childs;                              \
            x->state->list[num-1].count--;                                                  \
        }                                                                                   \
        else                                                                                \
        {                                                                                   \
            if(x->state->left < num){MTRIE_INCREMENT(x);}                                  \
            pos = x->state->current;                                                        \
            x->state->current += num;                                                       \
            x->state->left -= num;                                                          \
        }                                                                                   \
    }                                                                                       \
}while(0)

        //memset(&(x->nodes[pos]), 0, sizeof(MTRNODE) * num);                                
/* add */
int mtrie_add(void *mtr, char *key, int nkey, int data)
{
    int ret = -1, x = 0, i = 0,j = 0, k = 0, n = 0, pos = 0, 
        z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MTRNODE *nodes = NULL, *childs = NULL;
    MTRIE *mtrie = (MTRIE *)mtr;

    if(mtrie && key && nkey > 0)
    {
        MUTEX_LOCK(mtrie->mutex);        
        if((nodes = mtrie->nodes) && mtrie->map && mtrie->state)
        {
            p = (unsigned char *)key;
            ep = (unsigned char *)(key + nkey);
            i = *p++;
            while(p < ep)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds  > 0 && nodes[i].childs >= MTRIE_LINE_MAX) 
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(*p == nodes[min].key) x = min;
                    else if(*p == nodes[max].key) x = max;
                    else if(*p < nodes[min].key) x = -1;
                    else if(*p > nodes[max].key) x = 1;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min) {x = z;break;}
                            if(*p == nodes[z].key) {x = z;break;}
                            else if(*p > nodes[z].key) min = z;
                            else max = z;
                        }
                    }
                }
                //new node
                if(x < MTRIE_LINE_MAX || nodes[x].key != *p)
                {
                    n  = nodes[i].nchilds + 1;
                    z = nodes[i].childs;
                    MTRIE_POP(mtrie, n, pos);
                    nodes = mtrie->nodes;
                    if(pos < MTRIE_LINE_MAX || pos > mtrie->state->current) 
                    {
                        goto end;
                    }
                    childs = &(nodes[pos]);
                    //memset(childs, 0, sizeof(MTRNODE) * n);
                    if(x == 0)
                    {
                        MTRNODE_SETK(childs[0], *p);
                        j = pos;
                    }
                    else if(x == -1) 
                    {
                        MTRNODE_SETK(childs[0], *p);
                        k = 1;
                        while(k < n)
                        {
                            MTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        j = pos;
                    }
                    else if(x == 1)
                    {
                        k = 0;
                        while(k < (n-1))
                        {
                            MTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MTRNODE_SETK(childs[k], *p);
                        j = pos + k ;
                    }
                    else
                    {
                        //0 1 3 4(6) 7 9 10
                        k = 0;
                        while(nodes[z].key < *p)
                        {
                            MTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MTRNODE_SETK(childs[k], *p);
                        x = k++;
                        while(k < n)
                        {
                            MTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        j =  pos + x;
                    }
                    MTRIE_PUSH(mtrie, nodes[i].nchilds, nodes[i].childs);
                    nodes[i].nchilds++;
                    nodes[i].childs = pos;
                    i = j;
                }
                else i = x;
                ++p;
            }
            //fprintf(stdout, "rrrrrr:%s::%d i:%d data:%d\r\n", __FILE__, __LINE__, i, data);
            if((ret = nodes[i].data) == 0)
                ret = nodes[i].data = data;
        }
        else 
        {
            ret = -4;
        }
end:
        MUTEX_UNLOCK(mtrie->mutex);        
    }else ret = -5;
    
    return ret;
}

/* add /return auto increment id*/
int mtrie_xadd(void *mtr, char *key, int nkey)
{
    int ret = -1, x = 0, i = 0,j = 0, k = 0, n = 0, pos = 0, 
        z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MTRNODE *nodes = NULL, *childs = NULL;
    MTRIE *mtrie = (MTRIE *)mtr;

    if(mtrie && key && nkey > 0)
    {
        MUTEX_LOCK(mtrie->mutex);        
        if((nodes = mtrie->nodes) && mtrie->map && mtrie->state)
        {
            p = (unsigned char *)key;
            ep = (unsigned char *)(key + nkey);
            i = *p++;
            while(p < ep)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds  > 0 && nodes[i].childs >= MTRIE_LINE_MAX) 
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(*p == nodes[min].key) x = min;
                    else if(*p == nodes[max].key) x = max;
                    else if(*p < nodes[min].key) x = -1;
                    else if(*p > nodes[max].key) x = 1;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min) {x = z;break;}
                            if(nodes[z].key == *p) {x = z;break;}
                            else if(*p > nodes[z].key) min = z;
                            else max = z;
                        }
                    }
                }
                //new node
                if(x < MTRIE_LINE_MAX || nodes[x].key != *p)
                {
                    n  = nodes[i].nchilds + 1;
                    z = nodes[i].childs;
                    MTRIE_POP(mtrie, n, pos);
                    nodes = mtrie->nodes;
                    if(pos < MTRIE_LINE_MAX || pos > mtrie->state->current) 
                    {
                        ret = -3;
                        goto end;
                    }
                    childs = &(nodes[pos]);
                    //memset(childs, 0, sizeof(MTRNODE) * n);
                    if(x == 0)
                    {
                        MTRNODE_SETK(childs[0], *p);
                        j = pos;
                    }
                    else if(x == -1) 
                    {
                        MTRNODE_SETK(childs[0], *p);
                        k = 1;
                        while(k < n)
                        {
                            MTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        j = pos;
                    }
                    else if(x == 1)
                    {
                        k = 0;
                        while(k < (n-1))
                        {
                            MTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MTRNODE_SETK(childs[k], *p);
                        j = pos + k ;
                    }
                    else
                    {
                        //0 1 3 4(6) 7 9 10
                        k = 0;
                        while(nodes[z].key < *p)
                        {
                            MTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MTRNODE_SETK(childs[k], *p);
                        x = k++;
                        while(k < n)
                        {
                            MTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        j =  pos + x;
                    }
                    MTRIE_PUSH(mtrie, nodes[i].nchilds, nodes[i].childs);
                    nodes[i].nchilds++;
                    nodes[i].childs = pos;
                    i = j;
                }
                else i = x;
                ++p;
            }
            if((ret = nodes[i].data) == 0)
                nodes[i].data = ret = ++(mtrie->state->id);
        }
        else 
        {
            ret = -4;
        }
end:
        MUTEX_UNLOCK(mtrie->mutex);        
    }
    else 
    {
        ret = -5;
    }
    return ret;
}

/* get */
int  mtrie_get(void *mtr, char *key, int nkey)
{
    int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MTRIE *mtrie = (MTRIE *)mtr;
    MTRNODE *nodes = NULL;

    if(mtrie && key && nkey > 0)
    {
        MUTEX_LOCK(mtrie->mutex);        
        if((nodes = mtrie->nodes) && mtrie->map && mtrie->state)
        {
            p = (unsigned char *)key;
            ep = (unsigned char *)(key + nkey);
            i = *p++;
            if(nkey == 1 && i >= 0 && i < mtrie->state->total){ret = nodes[i].data; goto end;}
            while(p < ep)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds > 0 && nodes[i].childs >= MTRIE_LINE_MAX)
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(*p == nodes[min].key) x = min;
                    else if(*p == nodes[max].key) x = max;
                    else if(*p < nodes[min].key) goto end;
                    else if(*p > nodes[max].key) goto end;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min){x = z;break;}
                            if(nodes[z].key == *p){x = z;break;}
                            else if(nodes[z].key < *p) min = z;
                            else max = z;
                        }
                        if(nodes[x].key != *p) goto end;
                    }
                    i = x;
                }
                if(i >= 0 && i < mtrie->state->total 
                        && (nodes[i].nchilds == 0 || (p+1) == ep))
                {
                    if(nodes[i].key != *p) goto end;
                    if(p+1 == ep) ret = nodes[i].data;
                    break;
                }
                ++p;
            }
        }
end:
        MUTEX_UNLOCK(mtrie->mutex);        
    }
    return ret;
}

/* delete */
int  mtrie_del(void *mtr, char *key, int nkey)
{
    int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MTRIE *mtrie = (MTRIE *)mtr;
    MTRNODE *nodes = NULL;

    if(mtrie && key && nkey > 0)
    {
        MUTEX_LOCK(mtrie->mutex);        
        if((nodes = mtrie->nodes) && mtrie->map && mtrie->state)
        {
            p = (unsigned char *)key;
            ep = (unsigned char *)(key + nkey);
            i = *p++;
            if(nkey == 1 && i >= 0 && i < mtrie->state->total && nodes[i].data != 0){ret = nodes[i].data; nodes[i].data = 0; goto end;}
            while(p < ep)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds  > 0 && nodes[i].childs >= MTRIE_LINE_MAX) 
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(*p == nodes[min].key) x = min;
                    else if(*p == nodes[max].key) x = max;
                    else if(*p < nodes[min].key) goto end;
                    else if(*p > nodes[max].key) goto end;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min){x = z;break;}
                            if(nodes[z].key == *p){x = z;break;}
                            else if(nodes[z].key < *p) min = z;
                            else max = z;
                        }
                        if(nodes[x].key != *p) goto end;
                    }
                    i = x;
                }
                if(i >= 0 && i < mtrie->state->total 
                        && (nodes[i].nchilds == 0 || (p+1) == ep))
                {
                    if(nodes[i].key != *p) goto end;
                    if((p+1) == ep) 
                    {
                        ret = nodes[i].data;
                        nodes[i].data = 0;
                    }
                    break;
                }
                ++p;
            }
        }
end:
        MUTEX_UNLOCK(mtrie->mutex);        
    }
    
    return ret;
}

/* find/min */
int  mtrie_find(void *mtr, char *key, int nkey, int *to)
{
    int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MTRIE *mtrie = (MTRIE *)mtr;
    MTRNODE *nodes = NULL;

    if(mtrie && key && nkey > 0)
    {
        *to = 0;
        MUTEX_LOCK(mtrie->mutex);        
        if((nodes = mtrie->nodes) && mtrie->map && mtrie->state)
        {
            p = (unsigned char *)key;
            ep = (unsigned char *)(key + nkey);
            i = *p++;
            if((ret = nodes[i].data) != 0){*to = 1;goto end;}
            if(nkey == 1 && i >= 0 && i < mtrie->state->total && nodes[i].data != 0){ret = nodes[i].data; *to = 1; goto end;}
            while(p < ep)
            {
                x = 0;
                //check 
                if((ret = nodes[i].data) != 0){*to = ((char *)(p+1) - key);goto end;}
                else if(nodes[i].nchilds  > 0 && nodes[i].childs >= MTRIE_LINE_MAX) 
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(*p == nodes[min].key) x = min;
                    else if(*p == nodes[max].key) x = max;
                    else if(*p < nodes[min].key) goto end;
                    else if(*p > nodes[max].key) goto end;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min){x = z;break;}
                            if(nodes[z].key == *p){x = z;break;}
                            else if(nodes[z].key < *p) min = z;
                            else max = z;
                        }
                        if(nodes[x].key != *p) goto end;
                    }
                    i = x;
                    if((ret = nodes[i].data) != 0){*to = ((char *)(p+1) - key);goto end;}
                }
                else break; 
                ++p;
            }
        }
end:
        MUTEX_UNLOCK(mtrie->mutex);        
    }
    return ret;
}

/* find/max */
int   mtrie_maxfind(void *mtr, char *key, int nkey, int *to)
{
    int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MTRIE *mtrie = (MTRIE *)mtr;
    MTRNODE *nodes = NULL;

    if(mtrie && key && nkey > 0)
    {
        *to = 0;
        MUTEX_LOCK(mtrie->mutex);        
        if((nodes = mtrie->nodes) && mtrie->map && mtrie->state)
        {
            p = (unsigned char *)key;
            ep = (unsigned char *)(key + nkey);
            i = *p++;
            if(nodes[i].data != 0){*to = 1;ret = nodes[i].data;}
            if(nkey == 1 && i >= 0 && i < mtrie->state->total && nodes[i].data != 0){ret = nodes[i].data; *to = 1; goto end;}
            while(p < ep)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds  > 0 && nodes[i].childs >= MTRIE_LINE_MAX) 
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(*p == nodes[min].key) x = min;
                    else if(*p == nodes[max].key) x = max;
                    else if(*p < nodes[min].key) goto end;
                    else if(*p > nodes[max].key) goto end;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min){x = z;break;}
                            if(nodes[z].key == *p){x = z;break;}
                            else if(nodes[z].key < *p) min = z;
                            else max = z;
                        }
                        if(nodes[x].key != *p) goto end;
                    }
                    i = x;
                    if(nodes[i].data != 0) 
                    {
                        ret = nodes[i].data;
                        *to = (char *)(p+1) - key;
                    }
                }
                else break; 
                ++p;
            }
        }
end:
        MUTEX_UNLOCK(mtrie->mutex);        
    }
    return ret;
}
/* add/reverse */
int   mtrie_radd(void *mtr, char *key, int nkey, int data)
{
    int ret = -1, x = 0, i = 0, k = 0, j = 0, n = 0, pos = 0, 
        z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MTRNODE *nodes = NULL, *childs = NULL;
    MTRIE *mtrie = (MTRIE *)mtr;

    if(mtrie && key && nkey > 0)
    {
        MUTEX_LOCK(mtrie->mutex);        
        if((nodes = mtrie->nodes) && mtrie->map && mtrie->state)
        {
            p = (unsigned char *)(key + + nkey - 1);
            ep = (unsigned char *)key;
            i = *p--;
            while(p >= ep)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds  > 0 && nodes[i].childs >= MTRIE_LINE_MAX) 
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(*p == nodes[min].key) x = min;
                    else if(*p == nodes[max].key) x = max;
                    else if(*p < nodes[min].key) x = -1;
                    else if(*p > nodes[max].key) x = 1;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min) {x = z;break;}
                            if(nodes[z].key == *p) {x = z;break;}
                            else if(nodes[z].key < *p) min = z;
                            else max = z;
                        }
                    }
                }
                //new node
                if(x < MTRIE_LINE_MAX || nodes[x].key != *p)
                {
                    n  = nodes[i].nchilds + 1;
                    z = nodes[i].childs;
                    MTRIE_POP(mtrie, n, pos);
                    nodes = mtrie->nodes;
                    if(pos < MTRIE_LINE_MAX || pos > mtrie->state->current) goto end;
                    childs = &(nodes[pos]);
                    //memset(childs, 0, sizeof(MTRNODE) * n);
                    if(x == 0)
                    {
                        MTRNODE_SETK(childs[0], *p);
                        j = pos;
                    }
                    else if(x == -1) 
                    {
                        MTRNODE_SETK(childs[0], *p);
                        k = 1;
                        while(k < n)
                        {
                            MTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        j = pos;
                    }
                    else if(x == 1)
                    {
                        k = 0;
                        while(k < (n-1))
                        {
                            MTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MTRNODE_SETK(childs[k], *p);
                        j = pos + k ;
                    }
                    else
                    {
                        //0 1 3 4(6) 7 9 10
                        k = 0;
                        while(nodes[z].key < *p)
                        {
                            MTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MTRNODE_SETK(childs[k], *p);
                        x = k++;
                        while(k < n)
                        {
                            MTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        j =  pos + x;
                    }
                    MTRIE_PUSH(mtrie, nodes[i].nchilds, nodes[i].childs);
                    nodes[i].nchilds++;
                    nodes[i].childs = pos;
                    i = j;
                }
                else i = x;
                --p;
            }
            if((ret = nodes[i].data) == 0)
                ret = nodes[i].data = data;
        }else ret = -4;
end:
        MUTEX_UNLOCK(mtrie->mutex);        
    }else ret = -5;
    return ret;
}

/* add/reverse /return auto increment id */
int   mtrie_rxadd(void *mtr, char *key, int nkey)
{
    int ret = -1, x = 0, i = 0,j = 0, k = 0, n = 0, pos = 0, 
        z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MTRNODE *nodes = NULL, *childs = NULL;
    MTRIE *mtrie = (MTRIE *)mtr;

    if(mtrie && key && nkey > 0)
    {
        MUTEX_LOCK(mtrie->mutex);        
        if((nodes = mtrie->nodes) && mtrie->map && mtrie->state)
        {
            p = (unsigned char *)(key + + nkey - 1);
            ep = (unsigned char *)key;
            i = *p--;
            while(p >= ep)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds  > 0 && nodes[i].childs >= MTRIE_LINE_MAX) 
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(*p == nodes[min].key) x = min;
                    else if(*p == nodes[max].key) x = max;
                    else if(*p < nodes[min].key) x = -1;
                    else if(*p > nodes[max].key) x = 1;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min) {x = z;break;}
                            if(nodes[z].key == *p) {x = z;break;}
                            else if(nodes[z].key < *p) min = z;
                            else max = z;
                        }
                    }
                }
                //new node
                if(x < MTRIE_LINE_MAX || nodes[x].key != *p)
                {
                    n  = nodes[i].nchilds + 1;
                    z = nodes[i].childs;
                    MTRIE_POP(mtrie, n, pos);
                    nodes = mtrie->nodes;
                    if(pos < MTRIE_LINE_MAX || pos > mtrie->state->current) goto end; 
                    childs = &(nodes[pos]);
                    //memset(childs, 0, sizeof(MTRNODE) * n);
                    if(x == 0)
                    {
                        MTRNODE_SETK(childs[0], *p);
                        j = pos;
                    }
                    else if(x == -1) 
                    {
                        MTRNODE_SETK(childs[0], *p);
                        k = 1;
                        while(k < n)
                        {
                            MTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        j = pos;
                    }
                    else if(x == 1)
                    {
                        k = 0;
                        while(k < (n-1))
                        {
                            MTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MTRNODE_SETK(childs[k], *p);
                        j = pos + k ;
                    }
                    else
                    {
                        //0 1 3 4(6) 7 9 10
                        k = 0;
                        while(nodes[z].key < *p)
                        {
                            MTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MTRNODE_SETK(childs[k], *p);
                        x = k++;
                        while(k < n)
                        {
                            MTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        j =  pos + x;
                    }
                    MTRIE_PUSH(mtrie, nodes[i].nchilds, nodes[i].childs);
                    nodes[i].nchilds++;
                    nodes[i].childs = pos;
                    i = j;
                }
                else i = x;
                --p;
            }
            if((ret = nodes[i].data) == 0)
                ret =  nodes[i].data = ++(mtrie->state->id);
        }else ret = -4;
end:
        MUTEX_UNLOCK(mtrie->mutex);        
    }else ret = -5;
    return ret;
}

/* get/reverse */
int   mtrie_rget(void *mtr, char *key, int nkey)
{
    int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MTRIE *mtrie = (MTRIE *)mtr;
    MTRNODE *nodes = NULL;

    if(mtrie && key && nkey > 0)
    {
        MUTEX_LOCK(mtrie->mutex);        
        if((nodes = mtrie->nodes) && mtrie->map && mtrie->state)
        {
            p = (unsigned char *)(key + nkey - 1);
            ep = (unsigned char *)key;
            i = *p--;
            if(nkey == 1 && i >= 0 && i < mtrie->state->total){ret = nodes[i].data; goto end;}
            while(p >= ep)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds > 0)
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(*p == nodes[min].key) x = min;
                    else if(*p == nodes[max].key) x = max;
                    else if(*p < nodes[min].key) goto end;
                    else if(*p > nodes[max].key) goto end;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min){x = z;break;}
                            if(nodes[z].key == *p){x = z;break;}
                            else if(nodes[z].key < *p) min = z;
                            else max = z;
                        }
                        if(nodes[x].key != *p) goto end;
                    }
                    i = x;
                }
                if(i >= 0 && i < mtrie->state->total 
                        && (nodes[i].nchilds == 0 || p == ep))
                {
                    if(nodes[i].key != *p) goto end;
                    if(p == ep) ret = nodes[i].data;
                    break;
                }
                --p;
            }
        }
end:
        MUTEX_UNLOCK(mtrie->mutex);        
    }
    return ret;
}

/* delete/reverse */
int   mtrie_rdel(void *mtr, char *key, int nkey)
{
    int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MTRIE *mtrie = (MTRIE *)mtr;
    MTRNODE *nodes = NULL;

    if(mtrie && key && nkey > 0)
    {
        MUTEX_LOCK(mtrie->mutex);        
        if((nodes = mtrie->nodes) && mtrie->map && mtrie->state)
        {
            p = (unsigned char *)(key + nkey - 1);
            ep = (unsigned char *)key;
            i = *p--;
            if(nkey == 1 && i >= 0 && i < mtrie->state->total &&  nodes[i].data != 0){ret = nodes[i].data; nodes[i].data = 0;goto end;}
            while(p >= ep)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds  > 0 && nodes[i].childs >= MTRIE_LINE_MAX) 
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(*p == nodes[min].key) x = min;
                    else if(*p == nodes[max].key) x = max;
                    else if(*p < nodes[min].key) goto end;
                    else if(*p > nodes[max].key) goto end;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min){x = z;break;}
                            if(nodes[z].key == *p){x = z;break;}
                            else if(nodes[z].key < *p) min = z;
                            else max = z;
                        }
                        if(nodes[x].key != *p) goto end;
                    }
                    i = x;
                }
                if(i >= 0 && i < mtrie->state->total 
                        && (nodes[i].nchilds == 0 || p == ep))
                {
                    if(nodes[i].key != *p) goto end;
                    if(p == ep) 
                    {
                        ret = nodes[i].data;
                        nodes[i].data = 0;
                    }
                    break;
                }
                --p;
            }
        }
end:
        MUTEX_UNLOCK(mtrie->mutex);        
    }
    return ret;
}

/* find/min/reverse */
int   mtrie_rfind(void *mtr, char *key, int nkey, int *to)
{
    int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MTRIE *mtrie = (MTRIE *)mtr;
    MTRNODE *nodes = NULL;

    if(mtrie && key && nkey > 0)
    {
        *to = 0;
        MUTEX_LOCK(mtrie->mutex);        
        if((nodes = mtrie->nodes) && mtrie->map && mtrie->state)
        {
            p = (unsigned char *)(key + nkey - 1);
            ep = (unsigned char *)key;
            i = *p--;
            if((ret = nodes[i].data) != 0){*to = 1;goto end;}
            while(p >= ep)
            {
                x = 0;
                //check 
                if((ret = nodes[i].data) != 0)
                {
                    *to = nkey - ((char *)p+1 - key);
                    goto end;
                }
                else if(nodes[i].nchilds  > 0 && nodes[i].childs >= MTRIE_LINE_MAX) 
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(*p == nodes[min].key) x = min;
                    else if(*p == nodes[max].key) x = max;
                    else if(*p < nodes[min].key) goto end;
                    else if(*p > nodes[max].key) goto end;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min){x = z;break;}
                            if(nodes[z].key == *p){x = z;break;}
                            else if(nodes[z].key < *p) min = z;
                            else max = z;
                        }
                        if(nodes[x].key != *p) goto end;
                    }
                    i = x;
                    if((ret = nodes[i].data) != 0)
                    {
                        *to = (nkey - ((char *)p+1 - key));
                        goto end;
                    }
                }
                else break; 
                --p;
            }
        }
end:
        MUTEX_UNLOCK(mtrie->mutex);        
    }
    return ret;
}

/* find/max/reverse */
int   mtrie_rmaxfind(void *mtr, char *key, int nkey, int *to)
{
    MTRIE *mtrie = (MTRIE *)mtr;
    int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MTRNODE *nodes = NULL;

    if(mtrie && key && nkey > 0)
    {
        *to = 0;
        MUTEX_LOCK(mtrie->mutex);        
        if((nodes = mtrie->nodes) && mtrie->map && mtrie->state)
        {
            p = (unsigned char *)(key+nkey-1);
            ep = (unsigned char *)key;
            i = *p--;
            if(nodes[i].data != 0){*to = 1;ret = nodes[i].data;}
            if(nkey == 1 && i >= 0 && i < mtrie->state->total){ret = nodes[i].data;*to = 1;goto end;}
            while(p >= ep)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds  > 0 && nodes[i].childs >= MTRIE_LINE_MAX) 
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(*p == nodes[min].key) x = min;
                    else if(*p == nodes[max].key) x = max;
                    else if(*p < nodes[min].key) goto end;
                    else if(*p > nodes[max].key) goto end;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min){x = z;break;}
                            if(nodes[z].key == *p){x = z;break;}
                            else if(nodes[z].key < *p) min = z;
                            else max = z;
                        }
                        if(nodes[x].key != *p) goto end;
                    }
                    i = x;
                    if(nodes[i].data != 0) 
                    {
                        ret = nodes[i].data;
                        *to = nkey - ((char *)p - key);
                    }
                }
                else break; 
                --p;
            }
        }
end:
        MUTEX_UNLOCK(mtrie->mutex);        
    }
    return ret;
}

/* import dict */
int mtrie_import(void *mtr, char *dictfile, int direction)
{
    MTRIE *mtrie = (MTRIE *)mtr;
    char word[MTRIE_WORD_MAX];
    FILE *fp = NULL;
    int n = 0, id = 0;

    if(mtrie && dictfile)
    {
        if((fp = fopen(dictfile, "rd")))
        {
            memset(word, 0, MTRIE_WORD_MAX);
            while(fgets(word, MTRIE_WORD_MAX - 1, fp))
            {
                n = strlen(word);
                while(word[n-1] == '\r' || word[n-1] == '\n') word[--n] = '\0';
                if(direction < 0)
                    id = mtrie_rxadd(mtrie, word, n);
                else 
                    id = mtrie_xadd(mtrie, word, n);
#ifdef _MTR_OUT
                fprintf(stdout, "%s => %d\r\n", word, id);
#endif
                memset(word, 0, MTRIE_WORD_MAX);
            }
            fclose(fp);
        }
        return 0;
    }
    return -1;
}

/* destroy */
void mtrie_destroy(void *mtr)
{
    MTRIE *mtrie = (MTRIE *)mtr;
    if(mtrie)
    {
        MUTEX_LOCK(mtrie->mutex);
        if(mtrie->map) 
        {
            munmap(mtrie->map, mtrie->map_size);
            mtrie->map = NULL;
        }
        mtrie->map_size = 0;
        MTRIE_MAP_INIT(mtrie); 
        MUTEX_UNLOCK(mtrie->mutex);
    }
    return ;
}

/* clean/reverse */
void  mtrie_clean(void *mtr)
{
    MTRIE *mtrie = (MTRIE *)mtr;
    if(mtrie)
    {
        MUTEX_DESTROY(mtrie->mutex);
        if(mtrie->map) 
        {
            munmap(mtrie->map, mtrie->map_size);
        }
        free(mtrie);
    }
    return ;
}

/* initialize */
MTRIE *mtrie_init()
{
    MTRIE *mtrie = NULL;

    if((mtrie = (MTRIE *)calloc(1, sizeof(MTRIE))))
    {
        MUTEX_INIT(mtrie->mutex);
        MTRIE_MAP_INIT(mtrie);
        mtrie->add         = mtrie_add;
        mtrie->xadd        = mtrie_xadd;
        mtrie->get         = mtrie_get;
        mtrie->del         = mtrie_del;
        mtrie->find        = mtrie_find;
        mtrie->maxfind     = mtrie_maxfind;
        mtrie->radd        = mtrie_radd;
        mtrie->rxadd       = mtrie_rxadd;
        mtrie->rget        = mtrie_rget;
        mtrie->rdel        = mtrie_rdel;
        mtrie->rfind       = mtrie_rfind;
        mtrie->rmaxfind    = mtrie_rmaxfind;
        mtrie->import      = mtrie_import;
        mtrie->clean       = mtrie_clean;
    }
    return mtrie;
}

#ifdef _DEBUG_MTRIE
//gcc -o mmtr mtrie.c -D_DEBUG_MTRIE && ./mmtr 
#define FILE_LINE_MAX 65536
static char *mmfile = "/tmp/test.mtrie";
int main(int argc, char **argv)
{
    char word[FILE_LINE_MAX], *p = NULL;
    MTRIE *mtrie = NULL;
    int i = 0, n = 0, x = 0;

    if((mtrie = mtrie_init(mmfile)))
    {
        fprintf(stdout, "sizeof(MTRNODE):%d\n", sizeof(MTRNODE));
        _exit(-1);
        p = "abbbxxx"; mtrie_add(mtrie, p, strlen(p), 1);fprintf(stdout, "add(%s:%d)\r\n", p, 1);
        p = "abb"; mtrie_add(mtrie, p, strlen(p), 2);fprintf(stdout, "add(%s:%d)\r\n", p, 2);
        p = "abbx"; mtrie_add(mtrie, p, strlen(p), 3);fprintf(stdout, "add(%s:%d)\r\n", p, 3);
        p = "abbxddd"; mtrie_add(mtrie, p, strlen(p), 4);fprintf(stdout, "add(%s:%d)\r\n", p, 4);
        p = "abbxdddx"; mtrie_add(mtrie, p, strlen(p), 5);fprintf(stdout, "add(%s:%d)\r\n", p, 5);
        p = "abbx";x = mtrie_get(mtrie, p, strlen(p)); if(x != 0)fprintf(stdout, "get(%s:%d)\r\n", p, x);
        p = "abbxddddd";x = mtrie_find(mtrie, p, strlen(p), &n); if(x != 0){fprintf(stdout, "find(%s => %.*s:%d)\r\n", p, n, p, x);}
        p = "abbxddddd";x = mtrie_maxfind(mtrie, p, strlen(p), &n); if(x != 0){fprintf(stdout, "maxfind(%s => %.*s:%d)\r\n", p, n, p, x);}
        //reverse 
        p = "asscxxx"; mtrie_radd(mtrie, p, strlen(p), 1);fprintf(stdout, "radd(%s:%d)\r\n", p, 1);
        p = "adfdsfscxxx"; mtrie_radd(mtrie, p, strlen(p), 2);fprintf(stdout, "radd(%s:%d)\r\n", p, 2);
        p = "dafdsfscxxx"; mtrie_radd(mtrie, p, strlen(p), 3);fprintf(stdout, "radd(%s:%d)\r\n", p, 3);
        p = "aeffdsxccc"; mtrie_radd(mtrie, p, strlen(p), 4);fprintf(stdout, "radd(%s:%d)\r\n", p, 4);
        p = "adsssxxscxxx"; mtrie_radd(mtrie, p, strlen(p), 5);fprintf(stdout, "radd(%s:%d)\r\n", p, 5);
        p = "dafdsfssddcxxx";x = mtrie_rget(mtrie, p, strlen(p)); if(x != 0)fprintf(stdout, "rget(%s:%d)\r\n", p, x);
        p = "adsssxxscxxx";x = mtrie_rfind(mtrie, p, strlen(p), &n); if(x != 0){fprintf(stdout, "rfind(%s => %s:%d)\r\n", p, p+(strlen(p)-n), x);}
        p = "sadfsfsdadfdsfscxxx";x = mtrie_rmaxfind(mtrie, p, strlen(p), &n); if(x != 0){fprintf(stdout, "rmaxfind(%s => %.*s:%d)\r\n", p, n, p+(strlen(p)-n), x);}
        //demo xadd delete  
        p = "xljflsjflsjfsf"; n = mtrie_xadd(mtrie, p, strlen(p)); fprintf(stdout, "xadd(%s) => %d\r\n", p, n);
        p = "xjflsjfsf"; n = mtrie_xadd(mtrie, p, strlen(p)); fprintf(stdout, "xadd(%s) => %d\r\n", p, n);
        p = "xjflsjfsfdsfaf"; n = mtrie_xadd(mtrie, p, strlen(p)); fprintf(stdout, "xadd(%s) => %d\r\n", p, n);
        p = "xjflsjfsf"; n = mtrie_xadd(mtrie, p, strlen(p)); fprintf(stdout, "xadd(%s) => %d\r\n", p, n);
        p = "xjflsjfsf"; n = mtrie_del(mtrie, p, strlen(p)); fprintf(stdout, "del(%s) => %d\r\n", p, n);
        //demo radd rdelete 
        p = "xljfdfandf"; n = mtrie_rxadd(mtrie, p, strlen(p)); fprintf(stdout, "rxadd(%s) => %d\r\n", p, n);
        p = "dfa,mdnf,d"; n = mtrie_rxadd(mtrie, p, strlen(p)); fprintf(stdout, "rxadd(%s) => %d\r\n", p, n);
        p = "dsfkjsdfdfdsfdsf"; n = mtrie_rxadd(mtrie, p, strlen(p)); fprintf(stdout, "rxadd(%s) => %d\r\n", p, n);
        p = "yypdfa,mdnf,d"; n = mtrie_rxadd(mtrie, p, strlen(p)); fprintf(stdout, "rxadd(%s) => %d\r\n", p, n);
        p = "dfa,mdnf,d"; n = mtrie_rxadd(mtrie, p, strlen(p)); fprintf(stdout, "rxadd(%s) => %d\r\n", p, n);
        p = "dfa,mdnf,d"; n = mtrie_rdel(mtrie, p, strlen(p)); fprintf(stdout, "rdel(%s) => %d\r\n", p, n);
        p = "154"; n = mtrie_xadd(mtrie, p, strlen(p)); fprintf(stdout, "rxadd(%s) => %d\r\n", p, n);
        p = "155"; n = mtrie_xadd(mtrie, p, strlen(p)); fprintf(stdout, "rxadd(%s) => %d\r\n", p, n);
        p = "151"; n = mtrie_xadd(mtrie, p, strlen(p)); fprintf(stdout, "rxadd(%s) => %d\r\n", p, n);
        p = "153"; n = mtrie_xadd(mtrie, p, strlen(p)); fprintf(stdout, "rxadd(%s) => %d\r\n", p, n);
        p = "150"; x = mtrie_get(mtrie, p, strlen(p)); fprintf(stdout, "%s:%d\r\n", p, x);
        p = "151"; x = mtrie_get(mtrie, p, strlen(p)); fprintf(stdout, "%s:%d\r\n", p, x);
        p = "152"; x = mtrie_get(mtrie, p, strlen(p)); fprintf(stdout, "%s:%d\r\n", p, x);
        p = "153"; x = mtrie_get(mtrie, p, strlen(p)); fprintf(stdout, "%s:%d\r\n", p, x);
        p = "154"; x = mtrie_get(mtrie, p, strlen(p)); fprintf(stdout, "%s:%d\r\n", p, x);
        p = "155"; x = mtrie_get(mtrie, p, strlen(p)); fprintf(stdout, "%s:%d\r\n", p, x);
#ifdef _MTR_DEMO
        p = "abxcd"; mtrie_add(mtrie, p, strlen(p), 1); 
        x = mtrie_get(mtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "abxfe"; mtrie_add(mtrie, p, strlen(p), 2); 
        x = mtrie_get(mtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "abxee"; mtrie_add(mtrie, p, strlen(p), 3);
        x = mtrie_get(mtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "abee"; mtrie_add(mtrie, p, strlen(p), 4);
        x = mtrie_get(mtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "abzf"; mtrie_add(mtrie, p, strlen(p), 5);
        x = mtrie_get(mtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "abnf"; mtrie_add(mtrie, p, strlen(p), 6);
        x = mtrie_get(mtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "abkf"; mtrie_add(mtrie, p, strlen(p), 7);
        x = mtrie_get(mtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "xbkf"; mtrie_add(mtrie, p, strlen(p), 8);
        x = mtrie_get(mtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "xbkf001"; mtrie_add(mtrie, p, strlen(p), 9);
        x = mtrie_get(mtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "xbkf002"; mtrie_add(mtrie, p, strlen(p), 10);
        x = mtrie_get(mtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "xbkf003"; mtrie_add(mtrie, p, strlen(p), 11);
        x = mtrie_get(mtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "xbkz0010"; mtrie_add(mtrie, p, strlen(p), 12);
        x = mtrie_get(mtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "xbkf0011"; mtrie_add(mtrie, p, strlen(p), 13);
        p = "xbkd0011"; mtrie_add(mtrie, p, strlen(p), 14);
        x = mtrie_get(mtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "xbkc0011"; mtrie_add(mtrie, p, strlen(p), 15);
        x = mtrie_get(mtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "xbka0011"; mtrie_add(mtrie, p, strlen(p), 16);
        x = mtrie_get(mtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "xbkb0011"; mtrie_add(mtrie, p, strlen(p), 17);
        x = mtrie_get(mtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "xbkc0011"; mtrie_add(mtrie, p, strlen(p), 18);
        x = mtrie_get(mtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
#endif
#ifdef _MTR_TEST
        for(i = 0; i < 5000000; i++)
        {
            n = sprintf(word, "word_%d", i);
            mtrie_add(mtrie, word, n, i+1);
        }
        for(i = 0; i < 5000000; i++)
        {
            n = sprintf(word, "word_%d", i);
            if((x = mtrie_get(mtrie, word, n)) != 0)
                fprintf(stdout, "%d\r\n", x);
        }
#endif
#ifdef _MTR_FILE
        if(argc < 2)
        {
            fprintf(stderr, "Usage:%s linefile\r\n", argv[0]);
            _exit(-1);
        }
        for(i = 1; i < argc; i++)
        {
            mtrie_import(mtrie, argv[i], 0);
        }
#endif
        mtrie->clean(mtrie);
    }
}
#endif
