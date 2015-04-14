#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include "mmtrie.h"
#include "mutex.h"
#define MLL64(xxxx) ((long long int)(xxxx))
#ifdef MAP_LOCKED
#define MMAP_SHARED MAP_SHARED|MAP_LOCKED
#else
#define MMAP_SHARED MAP_SHARED
#endif
#define MMTRNODE_COPY(new, old)                                                 \
do                                                                              \
{                                                                               \
    new.key = old.key;                                                          \
    new.nchilds = old.nchilds;                                                  \
    new.data = old.data;                                                        \
    new.childs = old.childs;                                                    \
}while(0)
#define MMTRNODE_SETK(node, val)                                                \
do                                                                              \
{                                                                               \
    node.key = val;                                                             \
    node.nchilds = 0;                                                           \
    node.data = 0;                                                              \
    node.childs = 0;                                                            \
}while(0)
/* initialize mmap */
#define MMTRIE_MAP_INIT(x)                                                                  \
do                                                                                          \
{                                                                                           \
    if(x && x->fd > 0)                                                                      \
    {                                                                                       \
        if(x->map == NULL)                                                                  \
        {                                                                                   \
            x->size = (off_t)sizeof(MMTRSTATE)                                              \
            + (off_t)MMTRIE_NODES_MAX * (off_t)sizeof(MMTRNODE);                            \
            if((x->map = (char *)mmap(NULL, x->size, PROT_READ|PROT_WRITE,                  \
                            MAP_SHARED, x->fd, 0)) && x->map != (void *)-1)                 \
            {                                                                               \
                x->state = (MMTRSTATE *)(x->map);                                           \
                x->nodes = (MMTRNODE *)((char *)(x->map) + sizeof(MMTRSTATE));              \
            }                                                                               \
            else                                                                            \
            {                                                                               \
                x->map = NULL;                                                              \
                x->state = NULL;                                                            \
                x->nodes = NULL;                                                            \
            }                                                                               \
        }                                                                                   \
        if(x->file_size == 0)                                                               \
        {                                                                                   \
            x->file_size = (off_t)sizeof(MMTRSTATE)                                         \
            + (off_t)MMTRIE_BASE_NUM * (off_t)sizeof(MMTRNODE);                             \
            if(ftruncate(x->fd, x->file_size) != 0)                                         \
            {                                                                               \
                fprintf(stderr, "init trie file_size:%lld failed, %s\n",                    \
                    MLL64(x->file_size), strerror(errno));                                  \
                _exit(-1);                                                                  \
            }                                                                               \
            if(x->map)memset(x->map, 0, x->file_size);                                      \
            x->state->total = MMTRIE_BASE_NUM;                                              \
            x->state->left = MMTRIE_BASE_NUM - MMTRIE_LINE_MAX;                             \
            x->state->current = MMTRIE_LINE_MAX;                                            \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* increment */
#define MMTRIE_INCREMENT(x)                                                                 \
do                                                                                          \
{                                                                                           \
    if(x && x->file_size < x->size)                                                         \
    {                                                                                       \
        x->old = x->file_size;                                                              \
        x->file_size += (off_t)MMTRIE_BASE_NUM * (off_t)sizeof(MMTRNODE);                   \
        if(ftruncate(x->fd, x->file_size) == 0)                                             \
        {                                                                                   \
            x->state->total += MMTRIE_BASE_NUM;                                             \
            x->state->left += MMTRIE_BASE_NUM;                                              \
            memset(x->map + x->old, 0, x->file_size - x->old);                              \
        }                                                                                   \
        else                                                                                \
        {                                                                                   \
            fprintf(stderr, "truncate trie file_size:%lld failed, %s\n",                    \
                    MLL64(x->file_size), strerror(errno));                                  \
            _exit(-1);                                                                      \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* push node list */
#define MMTRIE_PUSH(x, num, pos)                                                            \
do                                                                                          \
{                                                                                           \
    if(x && pos >= MMTRIE_LINE_MAX && num > 0 && num <= MMTRIE_LINE_MAX                     \
            && x->state && x->nodes)                                                        \
    {                                                                                       \
        x->nodes[pos].childs = x->state->list[num-1].head;                                  \
        x->state->list[num-1].head = pos;                                                   \
        x->state->list[num-1].count++;                                                      \
    }                                                                                       \
}while(0)

/* pop new nodelist */
#define MMTRIE_POP(x, num, pos)                                                             \
do                                                                                          \
{                                                                                           \
    pos = -1;                                                                               \
    if(x && num > 0 && num <= MMTRIE_LINE_MAX && x->state && x->nodes)                      \
    {                                                                                       \
        if(x->state->list[num-1].count > 0)                                                 \
        {                                                                                   \
            pos = x->state->list[num-1].head;                                               \
            x->state->list[num-1].head = x->nodes[pos].childs;                              \
            x->state->list[num-1].count--;                                                  \
        }                                                                                   \
        else                                                                                \
        {                                                                                   \
            if(x->state->left < num){MMTRIE_INCREMENT(x);}                                  \
            pos = x->state->current;                                                        \
            x->state->current += num;                                                       \
            x->state->left -= num;                                                          \
        }                                                                                   \
    }                                                                                       \
}while(0)

        //memset(&(x->nodes[pos]), 0, sizeof(MMTRNODE) * num);                                
/* add */
int mmtrie_add(MMTRIE *mmtrie, char *key, int nkey, int data)
{
    int ret = -1, x = 0, i = 0,j = 0, k = 0, n = 0, pos = 0, 
        z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MMTRNODE *nodes = NULL, *childs = NULL;

    if(mmtrie && key && nkey > 0)
    {
        MUTEX_LOCK(mmtrie->mutex);        
        if((nodes = mmtrie->nodes) && mmtrie->map && mmtrie->state)
        {
            p = (unsigned char *)key;
            ep = (unsigned char *)(key + nkey);
            i = *p++;
            while(p < ep)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds  > 0 && nodes[i].childs >= MMTRIE_LINE_MAX) 
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
                if(x < MMTRIE_LINE_MAX || nodes[x].key != *p)
                {
                    n  = nodes[i].nchilds + 1;
                    z = nodes[i].childs;
                    MMTRIE_POP(mmtrie, n, pos);
                    if(pos < MMTRIE_LINE_MAX || pos > mmtrie->state->current) 
                    {
                        ret = -3;
                        goto end;
                    }
                    childs = &(nodes[pos]);
                    //memset(childs, 0, n * sizeof(MMTRNODE));
                    if(x == 0)
                    {
                        MMTRNODE_SETK(childs[0], *p);
                        j = pos;
                    }
                    else if(x == -1) 
                    {
                        MMTRNODE_SETK(childs[0], *p);
                        k = 1;
                        while(k < n)
                        {
                            MMTRNODE_COPY(childs[k], nodes[z]);
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
                            MMTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MMTRNODE_SETK(childs[k], *p);
                        j = pos + k ;
                    }
                    else
                    {
                        //0 1 3 4(6) 7 9 10
                        k = 0;
                        while(nodes[z].key < *p)
                        {
                            MMTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MMTRNODE_SETK(childs[k], *p);
                        x = k++;
                        while(k < n)
                        {
                            MMTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        j =  pos + x;
                    }
                    MMTRIE_PUSH(mmtrie, nodes[i].nchilds, nodes[i].childs);
                    nodes[i].nchilds++;
                    nodes[i].childs = pos;
                    i = j;
                }
                else i = x;
                ++p;
            }
            if((ret = nodes[i].data) == 0)
            {
                ret = nodes[i].data = data;
            }
        }
        else 
        {
            ret = -4;
        }
end:
        MUTEX_UNLOCK(mmtrie->mutex);        
    }else ret = -5;
    
    return ret;
}

/* add /return auto increment id*/
int mmtrie_xadd(MMTRIE *mmtrie, char *key, int nkey)
{
    int ret = -1, x = 0, i = 0,j = 0, k = 0, n = 0, pos = 0, 
        z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MMTRNODE *nodes = NULL, *childs = NULL;

    if(mmtrie && key && nkey > 0)
    {
        MUTEX_LOCK(mmtrie->mutex);        
        if((nodes = mmtrie->nodes) && mmtrie->map && mmtrie->state)
        {
            p = (unsigned char *)key;
            ep = (unsigned char *)(key + nkey);
            i = *p++;
            while(p < ep)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds  > 0 && nodes[i].childs >= MMTRIE_LINE_MAX) 
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
                if(x < MMTRIE_LINE_MAX || nodes[x].key != *p)
                {
                    n  = nodes[i].nchilds + 1;
                    z = nodes[i].childs;
                    MMTRIE_POP(mmtrie, n, pos);
                    if(pos < MMTRIE_LINE_MAX || pos > mmtrie->state->current) 
                    {
                        ret = -3;
                        goto end;
                    }
                    childs = &(nodes[pos]);
                    //memset(childs, 0, n * sizeof(MMTRNODE));
                    if(x == 0)
                    {
                        MMTRNODE_SETK(childs[0], *p);
                        j = pos;
                    }
                    else if(x == -1) 
                    {
                        MMTRNODE_SETK(childs[0], *p);
                        k = 1;
                        while(k < n)
                        {
                            MMTRNODE_COPY(childs[k], nodes[z]);
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
                            MMTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MMTRNODE_SETK(childs[k], *p);
                        j = pos + k ;
                    }
                    else
                    {
                        //0 1 3 4(6) 7 9 10
                        k = 0;
                        while(nodes[z].key < *p)
                        {
                            MMTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MMTRNODE_SETK(childs[k], *p);
                        x = k++;
                        while(k < n)
                        {
                            MMTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        j =  pos + x;
                    }
                    MMTRIE_PUSH(mmtrie, nodes[i].nchilds, nodes[i].childs);
                    nodes[i].nchilds++;
                    nodes[i].childs = pos;
                    i = j;
                }
                else i = x;
                ++p;
            }
            if((ret = nodes[i].data) == 0)
            {
                nodes[i].data = ret = ++(mmtrie->state->id);
                //fprintf(stdout, "key:%.*s data:%d\r\n", nkey, key, ret);
            }
        }
        else 
        {
            ret = -4;
        }
end:
        MUTEX_UNLOCK(mmtrie->mutex);        
    }
    else 
    {
        ret = -5;
    }
    return ret;
}

/* get */
int  mmtrie_get(MMTRIE *mmtrie, char *key, int nkey)
{
    int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MMTRNODE *nodes = NULL;

    if(mmtrie && key && nkey > 0)
    {
        MUTEX_LOCK(mmtrie->mutex);        
        if((nodes = mmtrie->nodes) && mmtrie->map && mmtrie->state)
        {
            p = (unsigned char *)key;
            ep = (unsigned char *)(key + nkey);
            i = *p++;
            if(nkey == 1 && i >= 0 && i < mmtrie->state->total){ret = nodes[i].data; goto end;}
            while(p < ep)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds > 0 && nodes[i].childs >= MMTRIE_LINE_MAX)
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
                if(i >= 0 && i < mmtrie->state->total 
                        && (nodes[i].nchilds == 0 || (p+1) == ep))
                {
                    if(nodes[i].key != *p) goto end;
                    if((p+1) == ep) ret = nodes[i].data;
                    break;
                }
                ++p;
            }
        }
end:
        MUTEX_UNLOCK(mmtrie->mutex);        
    }
    return ret;
}

/* delete */
int  mmtrie_del(MMTRIE *mmtrie, char *key, int nkey)
{
    int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MMTRNODE *nodes = NULL;

    if(mmtrie && key && nkey > 0)
    {
        MUTEX_LOCK(mmtrie->mutex);        
        if((nodes = mmtrie->nodes) && mmtrie->map && mmtrie->state)
        {
            p = (unsigned char *)key;
            ep = (unsigned char *)(key + nkey);
            i = *p++;
            if(nkey == 1 && i >= 0 && i < mmtrie->state->total){ret = nodes[i].data; nodes[i].data = 0; goto end;}
            while(p < ep)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds > 0 && nodes[i].childs >= MMTRIE_LINE_MAX)
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
                if(i >= 0 && i < mmtrie->state->total 
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
        MUTEX_UNLOCK(mmtrie->mutex);        
    }
    return ret;
}


/* find/min */
int  mmtrie_find(MMTRIE *mmtrie, char *key, int nkey, int *to)
{
    int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MMTRNODE *nodes = NULL;

    if(mmtrie && key && nkey > 0)
    {
        *to = 0;
        MUTEX_LOCK(mmtrie->mutex);        
        if((nodes = mmtrie->nodes) && mmtrie->map && mmtrie->state)
        {
            p = (unsigned char *)key;
            ep = (unsigned char *)(key + nkey);
            i = *p++;
            if((ret = nodes[i].data) != 0){*to = 1;goto end;}
            if(nkey == 1 && i >= 0 && i < mmtrie->state->total)
            {
                if(nodes[i].data != 0){ret = nodes[i].data; *to = 1;}
                goto end;
            }
            while(p < ep)
            {
                x = 0;
                //check 
                if((ret = nodes[i].data) != 0){*to = ((char *)(p+1) - key);goto end;}
                else if(nodes[i].nchilds  > 0 && nodes[i].childs >= MMTRIE_LINE_MAX) 
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
        MUTEX_UNLOCK(mmtrie->mutex);        
    }
    return ret;
}

/* find/max */
int   mmtrie_maxfind(MMTRIE *mmtrie, char *key, int nkey, int *to)
{
    int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MMTRNODE *nodes = NULL;

    if(mmtrie && key && nkey > 0)
    {
        *to = 0;
        MUTEX_LOCK(mmtrie->mutex);        
        if((nodes = mmtrie->nodes) && mmtrie->map && mmtrie->state)
        {
            p = (unsigned char *)key;
            ep = (unsigned char *)(key + nkey);
            i = *p++;
            if(nodes[i].data != 0){*to = 1;ret = nodes[i].data;}
            if(nkey == 1 && i >= 0 && i < mmtrie->state->total)
            {
                if(nodes[i].data != 0){ret = nodes[i].data; *to = 1;}
                goto end;
            }
            while(p < ep)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds  > 0 && nodes[i].childs >= MMTRIE_LINE_MAX) 
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
        MUTEX_UNLOCK(mmtrie->mutex);        
    }
    return ret;
}
/* add/reverse */
int   mmtrie_radd(MMTRIE *mmtrie, char *key, int nkey, int data)
{
    int ret = -1, x = 0, i = 0, k = 0, j = 0, n = 0, pos = 0, 
        z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MMTRNODE *nodes = NULL, *childs = NULL;

    if(mmtrie && key && nkey > 0)
    {
        MUTEX_LOCK(mmtrie->mutex);        
        if((nodes = mmtrie->nodes) && mmtrie->map && mmtrie->state)
        {
            p = (unsigned char *)(key + + nkey - 1);
            ep = (unsigned char *)key;
            i = *p--;
            while(p >= ep)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds  > 0 && nodes[i].childs >= MMTRIE_LINE_MAX) 
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
                if(x < MMTRIE_LINE_MAX || nodes[x].key != *p)
                {
                    n  = nodes[i].nchilds + 1;
                    z = nodes[i].childs;
                    MMTRIE_POP(mmtrie, n, pos);
                    if(pos < MMTRIE_LINE_MAX || pos > mmtrie->state->current) goto end;
                    childs = &(nodes[pos]);
                    //memset(childs, 0, n * sizeof(MMTRNODE));
                    if(x == 0)
                    {
                        MMTRNODE_SETK(childs[0], *p);
                        j = pos;
                    }
                    else if(x == -1) 
                    {
                        MMTRNODE_SETK(childs[0], *p);
                        k = 1;
                        while(k < n)
                        {
                            MMTRNODE_COPY(childs[k], nodes[z]);
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
                            MMTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MMTRNODE_SETK(childs[k], *p);
                        j = pos + k ;
                    }
                    else
                    {
                        //0 1 3 4(6) 7 9 10
                        k = 0;
                        while(nodes[z].key < *p)
                        {
                            MMTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MMTRNODE_SETK(childs[k], *p);
                        x = k++;
                        while(k < n)
                        {
                            MMTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        j =  pos + x;
                    }
                    MMTRIE_PUSH(mmtrie, nodes[i].nchilds, nodes[i].childs);
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
        MUTEX_UNLOCK(mmtrie->mutex);        
    }else ret = -5;
    return ret;
}

/* add/reverse /return auto increment id */
int   mmtrie_rxadd(MMTRIE *mmtrie, char *key, int nkey)
{
    int ret = -1, x = 0, i = 0,j = 0, k = 0, n = 0, pos = 0, 
        z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MMTRNODE *nodes = NULL, *childs = NULL;

    if(mmtrie && key && nkey > 0)
    {
        MUTEX_LOCK(mmtrie->mutex);        
        if((nodes = mmtrie->nodes) && mmtrie->map && mmtrie->state)
        {
            p = (unsigned char *)(key + + nkey - 1);
            ep = (unsigned char *)key;
            i = *p--;
            while(p >= ep)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds  > 0 && nodes[i].childs >= MMTRIE_LINE_MAX) 
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
                if(x < MMTRIE_LINE_MAX || nodes[x].key != *p)
                {
                    n  = nodes[i].nchilds + 1;
                    z = nodes[i].childs;
                    MMTRIE_POP(mmtrie, n, pos);
                    if(pos < MMTRIE_LINE_MAX || pos > mmtrie->state->current) goto end;
                    childs = &(nodes[pos]);
                    //memset(childs, 0, n * sizeof(MMTRNODE));
                    if(x == 0)
                    {
                        MMTRNODE_SETK(childs[0], *p);
                        j = pos;
                    }
                    else if(x == -1) 
                    {
                        MMTRNODE_SETK(childs[0], *p);
                        k = 1;
                        while(k < n)
                        {
                            MMTRNODE_COPY(childs[k], nodes[z]);
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
                            MMTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MMTRNODE_SETK(childs[k], *p);
                        j = pos + k ;
                    }
                    else
                    {
                        //0 1 3 4(6) 7 9 10
                        k = 0;
                        while(nodes[z].key < *p)
                        {
                            MMTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MMTRNODE_SETK(childs[k], *p);
                        x = k++;
                        while(k < n)
                        {
                            MMTRNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        j =  pos + x;
                    }
                    MMTRIE_PUSH(mmtrie, nodes[i].nchilds, nodes[i].childs);
                    nodes[i].nchilds++;
                    nodes[i].childs = pos;
                    i = j;
                }
                else i = x;
                --p;
            }
            if((ret = nodes[i].data) == 0)
                ret =  nodes[i].data = ++(mmtrie->state->id);
        }else ret = -4;
end:
        MUTEX_UNLOCK(mmtrie->mutex);        
    }else ret = -5;
    return ret;
}

/* get/reverse */
int   mmtrie_rget(MMTRIE *mmtrie, char *key, int nkey)
{
    int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MMTRNODE *nodes = NULL;

    if(mmtrie && key && nkey > 0)
    {
        MUTEX_LOCK(mmtrie->mutex);        
        if((nodes = mmtrie->nodes) && mmtrie->map && mmtrie->state)
        {
            p = (unsigned char *)(key + nkey - 1);
            ep = (unsigned char *)key;
            i = *p--;
            if(nkey == 1 && i >= 0 && i < mmtrie->state->total){ret = nodes[i].data; goto end;}
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
                if(i >= 0 && i < mmtrie->state->total 
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
        MUTEX_UNLOCK(mmtrie->mutex);        
    }
    return ret;
}

/* delete/reverse */
int   mmtrie_rdel(MMTRIE *mmtrie, char *key, int nkey)
{
    int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MMTRNODE *nodes = NULL;

    if(mmtrie && key && nkey > 0)
    {
        MUTEX_LOCK(mmtrie->mutex);        
        if((nodes = mmtrie->nodes) && mmtrie->map && mmtrie->state)
        {
            p = (unsigned char *)(key + nkey - 1);
            ep = (unsigned char *)key;
            i = *p--;
            if(nkey == 1 && i >= 0 && i < mmtrie->state->total)
            {
                if(nodes[i].data != 0){ret = nodes[i].data;nodes[i].data=0;}
                goto end;
            }
            while(p >= ep)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds  > 0 && nodes[i].childs >= MMTRIE_LINE_MAX) 
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
                if(i >= 0 && i < mmtrie->state->total 
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
        MUTEX_UNLOCK(mmtrie->mutex);        
    }
    return ret;
}

/* find/min/reverse */
int   mmtrie_rfind(MMTRIE *mmtrie, char *key, int nkey, int *to)
{
    int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MMTRNODE *nodes = NULL;

    if(mmtrie && key && nkey > 0)
    {
        *to = 0;
        MUTEX_LOCK(mmtrie->mutex);        
        if((nodes = mmtrie->nodes) && mmtrie->map && mmtrie->state)
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
                else if(nodes[i].nchilds  > 0 && nodes[i].childs >= MMTRIE_LINE_MAX) 
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
        MUTEX_UNLOCK(mmtrie->mutex);        
    }
    return ret;
}

/* find/max/reverse */
int   mmtrie_rmaxfind(MMTRIE *mmtrie, char *key, int nkey, int *to)
{
    int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0;
    unsigned char *p = NULL, *ep = NULL;
    MMTRNODE *nodes = NULL;

    if(mmtrie && key && nkey > 0)
    {
        *to = 0;
        MUTEX_LOCK(mmtrie->mutex);        
        if((nodes = mmtrie->nodes) && mmtrie->map && mmtrie->state)
        {
            p = (unsigned char *)(key+nkey-1);
            ep = (unsigned char *)key;
            i = *p--;
            if(nodes[i].data != 0){*to = 1;ret = nodes[i].data;}
            if(nkey == 1 && i >= 0 && i < mmtrie->state->total)
            {
                if(nodes[i].data != 0){ret = nodes[i].data; *to = 1;}
                goto end;
            }
            while(p >= ep)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds  > 0 && nodes[i].childs >= MMTRIE_LINE_MAX) 
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
        MUTEX_UNLOCK(mmtrie->mutex);        
    }
    return ret;
}

/* prefix view */
int mmtrie_prefix_view(MMTRIE *mmtrie, int nodeid, unsigned char *prefix, int nprefix)
{
    unsigned char line[MMTRIE_LINE_MAX];
    MMTRNODE *nodes = NULL;
    int x = 0, m = 0,  n = 0;

    if(mmtrie && nodeid >= 0 && prefix && (nodes = mmtrie->nodes) 
            && mmtrie->map && mmtrie->state)
    {
        if(nodeid < MMTRIE_LINE_MAX)
            prefix[nprefix] = nodeid;
        else
            prefix[nprefix] = nodes[nodeid].key;
        n = nprefix + 1;
        prefix[n] = 0;
        strcpy((char *)line, (char *)prefix);
        if(nodes[nodeid].data) fprintf(stdout, "%s:%d\n", prefix, nodes[nodeid].data);
        if(nodes[nodeid].nchilds > 0 && (x = nodes[nodeid].childs) >= MMTRIE_LINE_MAX)
        {
            m = x + nodes[nodeid].nchilds;
            while(x < m)
            {
                if(nodes[x].nchilds > 0) 
                {
                    mmtrie_prefix_view(mmtrie, x, prefix, n);
                }
                else
                {
                    fprintf(stdout, "%s%c:%d\n", line, nodes[x].key, nodes[x].data);
                }
                ++x;
            }
        }
    }
    return 0;
}

/* mmtrie view */
int mmtrie_view(MMTRIE *mmtrie)
{
    unsigned char prefix[MMTRIE_LINE_MAX];
    int i = 0;

    if(mmtrie)
    {
        for(i = 1; i < MMTRIE_LINE_MAX; i++)
        {
            mmtrie_prefix_view(mmtrie, i, prefix, 0);
        }
    }
    return 0;
}

/* import dict */
int mmtrie_import(MMTRIE *mmtrie, char *dictfile, int direction)
{
    char word[MMTRIE_WORD_MAX];
    FILE *fp = NULL;
    int n = 0, id = 0;

    if(mmtrie && dictfile)
    {
        if((fp = fopen(dictfile, "rd")))
        {
            memset(word, 0, MMTRIE_WORD_MAX);
            while(fgets(word, MMTRIE_WORD_MAX - 1, fp))
            {
                n = strlen(word);
                while(word[n-1] == '\r' || word[n-1] == '\n') word[--n] = '\0';
                if(direction < 0)
                    id = mmtrie_rxadd(mmtrie, word, n);
                else 
                    id = mmtrie_xadd(mmtrie, word, n);
#ifdef _MMTR_OUT
                fprintf(stdout, "%s => %d\r\n", word, id);
#endif
                memset(word, 0, MMTRIE_WORD_MAX);
            }
            fclose(fp);
        }
        return 0;
    }
    return -1;
}

/* destroy */
void mmtrie_destroy(MMTRIE *mmtrie)
{
    if(mmtrie)
    {
        MUTEX_LOCK(mmtrie->mutex);
        if(mmtrie->map) 
        {
            munmap(mmtrie->map, mmtrie->size);
            mmtrie->map = NULL;
        }
        if(mmtrie->fd > 0 && ftruncate(mmtrie->fd, 0) == 0);
        mmtrie->file_size = 0;
        MMTRIE_MAP_INIT(mmtrie); 
        MUTEX_UNLOCK(mmtrie->mutex);
    }
    return ;
}

/* clean/reverse */
void  mmtrie_clean(MMTRIE *mmtrie)
{
    if(mmtrie)
    {
        MUTEX_DESTROY(mmtrie->mutex);
        if(mmtrie->map) 
        {
            munmap(mmtrie->map, mmtrie->size);
        }
        if(mmtrie->fd > 0) close(mmtrie->fd);
        free(mmtrie);
    }
    return ;
}

/* initialize */
MMTRIE *mmtrie_init(char *file)
{
    MMTRIE *mmtrie = NULL;
    struct stat st = {0};
    int fd = 0;

    if(file && (fd = open(file, O_CREAT|O_RDWR, 0644)) > 0)
    {
        if(fstat(fd, &st) == 0 && (mmtrie = (MMTRIE *)calloc(1, sizeof(MMTRIE))))
        {
            MUTEX_INIT(mmtrie->mutex);
            mmtrie->fd          = fd;
            mmtrie->file_size   = st.st_size;
            MMTRIE_MAP_INIT(mmtrie);
            mmtrie->add         = mmtrie_add;
            mmtrie->xadd        = mmtrie_xadd;
            mmtrie->get         = mmtrie_get;
            mmtrie->del         = mmtrie_del;
            mmtrie->find        = mmtrie_find;
            mmtrie->maxfind     = mmtrie_maxfind;
            mmtrie->radd        = mmtrie_radd;
            mmtrie->rxadd       = mmtrie_rxadd;
            mmtrie->rget        = mmtrie_rget;
            mmtrie->rdel        = mmtrie_rdel;
            mmtrie->rfind       = mmtrie_rfind;
            mmtrie->rmaxfind    = mmtrie_rmaxfind;
            mmtrie->import      = mmtrie_import;
            mmtrie->clean       = mmtrie_clean;
        }
        else 
            close(fd);
    }
    return mmtrie;
}

#ifdef _DEBUG_MMTRIE_VIEW
//gcc -o mmview mmtrie.c -D_DEBUG_MMTRIE_VIEW && ./mmview
int main(int argc, char **argv)
{
    MMTRIE *mmtrie = NULL;
    char *file = NULL;

    if(argc < 2)
    {
        fprintf(stderr, "Usage:%s file\n", argv[0]);
        _exit(-1);
    }

    if((file = argv[1]) && (mmtrie = mmtrie_init(file)))
    {
        mmtrie_view(mmtrie);
        mmtrie_clean(mmtrie);
    }
}
#endif
#ifdef _DEBUG_MMTRIE
//gcc -o mmtr mmtrie.c -D_DEBUG_MMTRIE && ./mmtr 
#define FILE_LINE_MAX 65536
static char *mmfile = "/tmp/test.mmtrie";
int main(int argc, char **argv)
{
    char word[FILE_LINE_MAX], *p = NULL;
    MMTRIE *mmtrie = NULL;
    int i = 0, n = 0, x = 0;

    if((mmtrie = mmtrie_init(mmfile)))
    {
        fprintf(stdout, "sizeof(MMTRNODE):%d\n", sizeof(MMTRNODE));
        _exit(-1);
        p = "abbbxxx"; mmtrie_add(mmtrie, p, strlen(p), 1);fprintf(stdout, "add(%s:%d)\r\n", p, 1);
        p = "abb"; mmtrie_add(mmtrie, p, strlen(p), 2);fprintf(stdout, "add(%s:%d)\r\n", p, 2);
        p = "abbx"; mmtrie_add(mmtrie, p, strlen(p), 3);fprintf(stdout, "add(%s:%d)\r\n", p, 3);
        p = "abbxddd"; mmtrie_add(mmtrie, p, strlen(p), 4);fprintf(stdout, "add(%s:%d)\r\n", p, 4);
        p = "abbxdddx"; mmtrie_add(mmtrie, p, strlen(p), 5);fprintf(stdout, "add(%s:%d)\r\n", p, 5);
        p = "abbx";x = mmtrie_get(mmtrie, p, strlen(p)); if(x != 0)fprintf(stdout, "get(%s:%d)\r\n", p, x);
        p = "abbxddddd";x = mmtrie_find(mmtrie, p, strlen(p), &n); if(x != 0){fprintf(stdout, "find(%s => %.*s:%d)\r\n", p, n, p, x);}
        p = "abbxddddd";x = mmtrie_maxfind(mmtrie, p, strlen(p), &n); if(x != 0){fprintf(stdout, "maxfind(%s => %.*s:%d)\r\n", p, n, p, x);}
        //reverse 
        p = "asscxxx"; mmtrie_radd(mmtrie, p, strlen(p), 1);fprintf(stdout, "radd(%s:%d)\r\n", p, 1);
        p = "adfdsfscxxx"; mmtrie_radd(mmtrie, p, strlen(p), 2);fprintf(stdout, "radd(%s:%d)\r\n", p, 2);
        p = "dafdsfscxxx"; mmtrie_radd(mmtrie, p, strlen(p), 3);fprintf(stdout, "radd(%s:%d)\r\n", p, 3);
        p = "aeffdsxccc"; mmtrie_radd(mmtrie, p, strlen(p), 4);fprintf(stdout, "radd(%s:%d)\r\n", p, 4);
        p = "adsssxxscxxx"; mmtrie_radd(mmtrie, p, strlen(p), 5);fprintf(stdout, "radd(%s:%d)\r\n", p, 5);
        p = "dafdsfssddcxxx";x = mmtrie_rget(mmtrie, p, strlen(p)); if(x != 0)fprintf(stdout, "rget(%s:%d)\r\n", p, x);
        p = "adsssxxscxxx";x = mmtrie_rfind(mmtrie, p, strlen(p), &n); if(x != 0){fprintf(stdout, "rfind(%s => %s:%d)\r\n", p, p+(strlen(p)-n), x);}
        p = "sadfsfsdadfdsfscxxx";x = mmtrie_rmaxfind(mmtrie, p, strlen(p), &n); if(x != 0){fprintf(stdout, "rmaxfind(%s => %.*s:%d)\r\n", p, n, p+(strlen(p)-n), x);}
        //demo xadd delete  
        p = "xljflsjflsjfsf"; n = mmtrie_xadd(mmtrie, p, strlen(p)); fprintf(stdout, "xadd(%s) => %d\r\n", p, n);
        p = "xjflsjfsf"; n = mmtrie_xadd(mmtrie, p, strlen(p)); fprintf(stdout, "xadd(%s) => %d\r\n", p, n);
        p = "xjflsjfsfdsfaf"; n = mmtrie_xadd(mmtrie, p, strlen(p)); fprintf(stdout, "xadd(%s) => %d\r\n", p, n);
        p = "xjflsjfsf"; n = mmtrie_xadd(mmtrie, p, strlen(p)); fprintf(stdout, "xadd(%s) => %d\r\n", p, n);
        p = "xjflsjfsf"; n = mmtrie_del(mmtrie, p, strlen(p)); fprintf(stdout, "del(%s) => %d\r\n", p, n);
        //demo radd rdelete 
        p = "xljfdfandf"; n = mmtrie_rxadd(mmtrie, p, strlen(p)); fprintf(stdout, "rxadd(%s) => %d\r\n", p, n);
        p = "dfa,mdnf,d"; n = mmtrie_rxadd(mmtrie, p, strlen(p)); fprintf(stdout, "rxadd(%s) => %d\r\n", p, n);
        p = "dsfkjsdfdfdsfdsf"; n = mmtrie_rxadd(mmtrie, p, strlen(p)); fprintf(stdout, "rxadd(%s) => %d\r\n", p, n);
        p = "yypdfa,mdnf,d"; n = mmtrie_rxadd(mmtrie, p, strlen(p)); fprintf(stdout, "rxadd(%s) => %d\r\n", p, n);
        p = "dfa,mdnf,d"; n = mmtrie_rxadd(mmtrie, p, strlen(p)); fprintf(stdout, "rxadd(%s) => %d\r\n", p, n);
        p = "dfa,mdnf,d"; n = mmtrie_rdel(mmtrie, p, strlen(p)); fprintf(stdout, "rdel(%s) => %d\r\n", p, n);
        p = "154"; n = mmtrie_xadd(mmtrie, p, strlen(p)); fprintf(stdout, "rxadd(%s) => %d\r\n", p, n);
        p = "155"; n = mmtrie_xadd(mmtrie, p, strlen(p)); fprintf(stdout, "rxadd(%s) => %d\r\n", p, n);
        p = "151"; n = mmtrie_xadd(mmtrie, p, strlen(p)); fprintf(stdout, "rxadd(%s) => %d\r\n", p, n);
        p = "153"; n = mmtrie_xadd(mmtrie, p, strlen(p)); fprintf(stdout, "rxadd(%s) => %d\r\n", p, n);
        p = "150"; x = mmtrie_get(mmtrie, p, strlen(p)); fprintf(stdout, "%s:%d\r\n", p, x);
        p = "151"; x = mmtrie_get(mmtrie, p, strlen(p)); fprintf(stdout, "%s:%d\r\n", p, x);
        p = "152"; x = mmtrie_get(mmtrie, p, strlen(p)); fprintf(stdout, "%s:%d\r\n", p, x);
        p = "153"; x = mmtrie_get(mmtrie, p, strlen(p)); fprintf(stdout, "%s:%d\r\n", p, x);
        p = "154"; x = mmtrie_get(mmtrie, p, strlen(p)); fprintf(stdout, "%s:%d\r\n", p, x);
        p = "155"; x = mmtrie_get(mmtrie, p, strlen(p)); fprintf(stdout, "%s:%d\r\n", p, x);
#ifdef _MMTR_DEMO
        p = "abxcd"; mmtrie_add(mmtrie, p, strlen(p), 1); 
        x = mmtrie_get(mmtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "abxfe"; mmtrie_add(mmtrie, p, strlen(p), 2); 
        x = mmtrie_get(mmtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "abxee"; mmtrie_add(mmtrie, p, strlen(p), 3);
        x = mmtrie_get(mmtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "abee"; mmtrie_add(mmtrie, p, strlen(p), 4);
        x = mmtrie_get(mmtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "abzf"; mmtrie_add(mmtrie, p, strlen(p), 5);
        x = mmtrie_get(mmtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "abnf"; mmtrie_add(mmtrie, p, strlen(p), 6);
        x = mmtrie_get(mmtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "abkf"; mmtrie_add(mmtrie, p, strlen(p), 7);
        x = mmtrie_get(mmtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "xbkf"; mmtrie_add(mmtrie, p, strlen(p), 8);
        x = mmtrie_get(mmtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "xbkf001"; mmtrie_add(mmtrie, p, strlen(p), 9);
        x = mmtrie_get(mmtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "xbkf002"; mmtrie_add(mmtrie, p, strlen(p), 10);
        x = mmtrie_get(mmtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "xbkf003"; mmtrie_add(mmtrie, p, strlen(p), 11);
        x = mmtrie_get(mmtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "xbkz0010"; mmtrie_add(mmtrie, p, strlen(p), 12);
        x = mmtrie_get(mmtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "xbkf0011"; mmtrie_add(mmtrie, p, strlen(p), 13);
        p = "xbkd0011"; mmtrie_add(mmtrie, p, strlen(p), 14);
        x = mmtrie_get(mmtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "xbkc0011"; mmtrie_add(mmtrie, p, strlen(p), 15);
        x = mmtrie_get(mmtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "xbka0011"; mmtrie_add(mmtrie, p, strlen(p), 16);
        x = mmtrie_get(mmtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "xbkb0011"; mmtrie_add(mmtrie, p, strlen(p), 17);
        x = mmtrie_get(mmtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
        p = "xbkc0011"; mmtrie_add(mmtrie, p, strlen(p), 18);
        x = mmtrie_get(mmtrie, p, strlen(p)); fprintf(stdout, "%s:%d\n", p, x);
#endif
#ifdef _MMTR_TEST
        for(i = 0; i < 5000000; i++)
        {
            n = sprintf(word, "word_%d", i);
            mmtrie_add(mmtrie, word, n, i+1);
        }
        for(i = 0; i < 5000000; i++)
        {
            n = sprintf(word, "word_%d", i);
            if((x = mmtrie_get(mmtrie, word, n)) != 0)
                fprintf(stdout, "%d\r\n", x);
        }
#endif
#ifdef _MMTR_FILE
        if(argc < 2)
        {
            fprintf(stderr, "Usage:%s linefile\r\n", argv[0]);
            _exit(-1);
        }
        for(i = 1; i < argc; i++)
        {
            mmtrie_import(mmtrie, argv[i], 0);
        }
#endif
        mmtrie->clean(mmtrie);
    }
}
#endif
