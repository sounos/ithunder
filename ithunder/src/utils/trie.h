#include <stdlib.h>
#include "mutex.h"
#ifndef _TRIE_H
#define _TRIE_H
#ifdef __cplusplus
extern "C" {
#endif
#define BYTE_SIZE  256
#define WORD_MAX_SIZE 1024
typedef struct _HWORD
{
    int n;
    unsigned int ps;
    unsigned char buf[WORD_MAX_SIZE];
}HWORD;
typedef struct _HNODE
{
    int count;
    int chr;
    void *dptr;
    struct _HNODE *list;
}HNODE;
void hnode_view(HNODE *hnode);
void hnode_list(HNODE *hnode, char **p);
void hnode_clean(HNODE *hnode);
#define UNS(s) ((unsigned char)s)
#define HN(ptr) ((HNODE *)ptr)
#define HCNT(ptr) (ptr->count)
#define HCHR(ptr) (ptr->chr)
#define HNL(ptr) (ptr->list)
#define HNP(ptr, n) (&(ptr->list[n]))
#define HNPC(ptr, n) (ptr->list[n].chr)
#define HNCPY(ptr, m, n) memcpy(&(ptr->list[m]), &(ptr->list[n]), sizeof(HNODE))
#define HNUNSET(ptr, n) memset(&(ptr->list[n]), 0, sizeof(HNODE))
       //fprintf(stdout, "Ready realloc %d bytes\n", (HCNT(ptr)+1));               
#define HN_FIND(ptr, s, min, max, n)                                                    \
{                                                                                       \
    n = -1;                                                                             \
    if(HNL(ptr))                                                                        \
    {                                                                                   \
        min = 0;                                                                        \
        max = HCNT(ptr) - 1;                                                            \
        if(UNS(s) <= HNPC(ptr, min)) n = min;                                           \
        else if(UNS(s) == HNPC(ptr, max)) n = max;                                      \
        else if(UNS(s) > HNPC(ptr, max)) n = HCNT(ptr);                                 \
        else                                                                            \
        {                                                                               \
            while(max > min)                                                            \
            {                                                                           \
                n = ((max + min) / 2);                                                  \
                if(n == min)break;                                                      \
                if(UNS(s) == HNPC(ptr, n))break;                                        \
                if(UNS(s) > HNPC(ptr, n)) min = n;                                      \
                else max = n;                                                           \
            }                                                                           \
            if(UNS(s) > HNPC(ptr, n))n++;                                               \
        }                                                                               \
    }                                                                                   \
}       
#define HN_RESIZE(hptr)                                                                 \
{                                                                                       \
    if(HNL(hptr) == NULL) HNL(hptr) = (HNODE *)calloc((HCNT(hptr)+1), sizeof(HNODE));   \
    else HNL(hptr) = (HNODE *) realloc(HNL(hptr), sizeof(HNODE) * (1+HCNT(hptr)));      \
    if(HNL(hptr)) HCNT(hptr)++;                                                         \
}
#define HN_ADD(hptr, s, min, max, n)                                                    \
{                                                                                       \
    HN_RESIZE(hptr);                                                                    \
    if(HNL(hptr))                                                                       \
    {                                                                                   \
        if(n == -1) n = 0;                                                              \
        max = HCNT(hptr) - 1;min = max - 1;                                             \
        while(n < max) HNCPY(hptr, max--, min--);                                       \
        HNUNSET(hptr, n);                                                               \
        HNPC(hptr, n) = UNS(s);                                                          \
    }                                                                                   \
}
//fprintf(stdout, "1:max:%d:%d\n", max, UNS(s));
//fprintf(stdout, "2:max:%d\n", UNS(s));
#define HN_DEL(ptr, n)                                                                  \
{                                                                                       \
    if(HN(ptr) && HNL(ptr))                                                             \
    {                                                                                   \
        while(n < HCNT(ptr)) HNCPY(ptr, n, ++n);                                        \
        HNUNSET(ptr, n);                                                                \
        MAX(ptr)--;                                                                     \
    }                                                                                   \
}
#define HN_NEXT(ptr, n, p)                                                          \
{                       \
    if(ptr && HNL(ptr)) \
    {                   \
                        \
    }                       \
}
typedef struct _TIRETAB
{
    HNODE table[BYTE_SIZE];
    HNODE *pnode;
    HNODE *newlist;
    int count;
    int size;
    int i ;
    int n ;
    int min;
    int max;
    int bits;
    unsigned int ps;
    MUTEX mutex;
}TRIETAB;
void trietab_view(void *ptr);
void trietab_clean(void *ptr);
#define  PRT(ptr)       ((TRIETAB *)(ptr))
#define  PSH(ptr)       (PRT(ptr)->ps)
#define  HBCNT(ptr)     (PRT(ptr)->count)
#define  HBSIZE(ptr)    (PRT(ptr)->size)
#define  HBTB(ptr)      (PRT(ptr)->table)
#define  HBTBN(ptr, n)  (PRT(ptr)->table[n])
#define  NHB(ptr)       (PRT(ptr)->n)
#define  IHB(ptr)       (PRT(ptr)->i)
#define  MINHB(ptr)     (PRT(ptr)->min)
#define  MAXHB(ptr)     (PRT(ptr)->max)
#define  HBND(ptr)      (PRT(ptr)->pnode)
#define  HBNL(ptr)      (PRT(ptr)->newlist)
#define  TRIETAB_INIT(ptr)                                                                  \
do                                                                                          \
{                                                                                           \
    if((ptr = calloc(1, sizeof(TRIETAB))))                                                  \
    {                                                                                       \
        MUTEX_INIT(PRT(ptr)->mutex);                                                       \
    }                                                                                       \
}while(0)

/* ADD word/block node to trietab */ 
#define  TRIETAB_ADD(ptr, key, nkey, pdata)                                                 \
do                                                                                          \
{                                                                                           \
    if(ptr && key && nkey > 0)                                                              \
    {                                                                                       \
        MUTEX_LOCK(PRT(ptr)->mutex);                                                        \
        HBND(ptr) = (HNODE *)&(HBTBN(ptr, UNS(key[0])));                                    \
        for(IHB(ptr) = 1; IHB(ptr) < nkey; ++IHB(ptr))                                      \
        {                                                                                   \
            PSH(ptr) = UNS(key[IHB(ptr)]);                                                  \
            if(HBND(ptr))                                                                   \
            {                                                                               \
                HN_FIND(HBND(ptr), PSH(ptr), MINHB(ptr), MAXHB(ptr), NHB(ptr));             \
                if(NHB(ptr) < 0 || NHB(ptr) >= HCNT(HBND(ptr))                              \
                        || HNPC(HBND(ptr), NHB(ptr)) != PSH(ptr))                           \
                {                                                                           \
                    HN_ADD(HBND(ptr), PSH(ptr), MINHB(ptr), MAXHB(ptr), NHB(ptr));          \
                    ++HBCNT(ptr);                                                           \
                    HBSIZE(ptr) += sizeof(HNODE);                                           \
                }                                                                           \
                HBND(ptr) = HNP(HBND(ptr), NHB(ptr));                                       \
            }                                                                               \
        }                                                                                   \
        if(HBND(ptr)) HBND(ptr)->dptr = (void *)pdata;                                      \
        else pdata = NULL;                                                                  \
        MUTEX_UNLOCK(PRT(ptr)->mutex);                                                      \
    }                                                                                       \
}while(0)

/* ADD reverse word/block node to trietab */ 
#define  TRIETAB_RADD(ptr, key, nkey, pdata)                                                \
do{                                                                                         \
    if(ptr && key && nkey > 0)                                                              \
    {                                                                                       \
        MUTEX_LOCK(PRT(ptr)->mutex);                                                        \
        HBND(ptr) = (HNODE *)&(HBTBN(ptr, UNS(key[nkey - 1])));                             \
        for(IHB(ptr) = (nkey-2); IHB(ptr) >= 0; --IHB(ptr))                                 \
        {                                                                                   \
            PSH(ptr) = UNS(key[IHB(ptr)]);                                                  \
            if(HBND(ptr))                                                                   \
            {                                                                               \
                HN_FIND(HBND(ptr), PSH(ptr), MINHB(ptr), MAXHB(ptr), NHB(ptr));             \
                if(NHB(ptr) < 0 || NHB(ptr) >= HCNT(HBND(ptr))                              \
                        || HNPC(HBND(ptr), NHB(ptr)) != PSH(ptr))                           \
                {                                                                           \
                    HN_ADD(HBND(ptr), PSH(ptr), MINHB(ptr), MAXHB(ptr), NHB(ptr));          \
                    ++HBCNT(ptr);                                                           \
                    HBSIZE(ptr) += sizeof(HNODE);                                           \
                }                                                                           \
                HBND(ptr) = HNP(HBND(ptr), NHB(ptr));                                       \
            }                                                                               \
        }                                                                                   \
        if(HBND(ptr)) HBND(ptr)->dptr = (void *)pdata;                                      \
        else pdata = NULL;                                                                  \
        MUTEX_UNLOCK(PRT(ptr)->mutex);                                                      \
    }                                                                                       \
}while(0)

/* GET word/block node from trietab */ 
#define TRIETAB_GET(ptr, key, nkey, pdata)                                                  \
do{                                                                                         \
    pdata = NULL;                                                                           \
    if(ptr && key && nkey > 0)                                                              \
    {                                                                                       \
        MUTEX_LOCK(PRT(ptr)->mutex);                                                        \
        HBND(ptr) = (HNODE *)&(HBTBN(ptr, UNS(key[0])));                                    \
        if((IHB(ptr) = 1) < nkey)                                                           \
        {                                                                                   \
            do                                                                              \
            {                                                                               \
                PSH(ptr) = UNS(key[IHB(ptr)]);                                              \
                if(HBND(ptr))                                                               \
                {                                                                           \
                    HN_FIND(HBND(ptr), PSH(ptr), MINHB(ptr), MAXHB(ptr), NHB(ptr));         \
                    if(NHB(ptr) < 0 || NHB(ptr) >= HCNT(HBND(ptr))                          \
                            || HNPC(HBND(ptr), NHB(ptr)) != PSH(ptr))                       \
                    {                                                                       \
                        HBND(ptr) = NULL;                                                   \
                        break;                                                              \
                    }                                                                       \
                    HBND(ptr) = HNP(HBND(ptr), NHB(ptr));                                   \
                }else break;                                                                \
            }while(++IHB(ptr) < nkey);                                                      \
        }                                                                                   \
        if(HBND(ptr)) pdata = HBND(ptr)->dptr;                                              \
        MUTEX_UNLOCK(PRT(ptr)->mutex);                                                      \
    }                                                                                       \
}while(0)                                                                                           

/* GET reverse word/block node from trietab */ 
#define TRIETAB_RGET(ptr, key, nkey, pdata)                                                 \
do{                                                                                         \
    pdata = NULL;                                                                           \
    if(ptr && key)                                                                          \
    {                                                                                       \
        MUTEX_LOCK(PRT(ptr)->mutex);                                                        \
        HBND(ptr) = (HNODE *)&(HBTBN(ptr, UNS(key[nkey -1])));                              \
        if((IHB(ptr) = (nkey-2)) >= 0)                                                      \
        {                                                                                   \
            do                                                                              \
            {                                                                               \
                PSH(ptr) = UNS(key[IHB(ptr)]);                                              \
                if(HBND(ptr))                                                               \
                {                                                                           \
                    HN_FIND(HBND(ptr), PSH(ptr), MINHB(ptr), MAXHB(ptr), NHB(ptr));         \
                    if(NHB(ptr) < 0 || NHB(ptr) >= HCNT(HBND(ptr))                          \
                            || HNPC(HBND(ptr), NHB(ptr)) != PSH(ptr))                       \
                    {                                                                       \
                        HBND(ptr) = NULL;                                                   \
                        break;                                                              \
                    }                                                                       \
                    HBND(ptr) = HNP(HBND(ptr), NHB(ptr));                                   \
                }else break;                                                                \
            }while(--IHB(ptr) >= 0);                                                        \
        }                                                                                   \
        if(HBND(ptr)) pdata = HBND(ptr)->dptr;                                              \
        MUTEX_UNLOCK(PRT(ptr)->mutex);                                                      \
    }                                                                                       \
}while(0)

/* FIND word/block node with MIN style from trietab */ 
#define TRIETAB_MIN_FIND(ptr, key, nkey, pdata, pos)                                        \
do{                                                                                         \
    pdata = NULL;pos = -1;                                                                  \
    if(ptr && key && nkey > 0)                                                              \
    {                                                                                       \
        MUTEX_LOCK(PRT(ptr)->mutex);                                                        \
        HBND(ptr) = (HNODE *)&(HBTBN(ptr, UNS(key[0])));                                    \
        if(HBND(ptr)->dptr){pos = 0;pdata = HBND(ptr)->dptr;}                               \
        if((IHB(ptr) = 1) < nkey)                                                           \
        {                                                                                   \
            do                                                                              \
            {                                                                               \
                PSH(ptr) = UNS(key[IHB(ptr)]);                                              \
                if(HBND(ptr))                                                               \
                {                                                                           \
                    if(HBND(ptr)->dptr){pos = IHB(ptr);break;}                              \
                    HN_FIND(HBND(ptr), PSH(ptr), MINHB(ptr), MAXHB(ptr), NHB(ptr));         \
                    if(NHB(ptr) >= 0 && NHB(ptr) < HCNT(HBND(ptr))                          \
                            && HNPC(HBND(ptr), NHB(ptr)) == PSH(ptr))                       \
                    {                                                                       \
                        HBND(ptr) = HNP(HBND(ptr), NHB(ptr));                               \
                    }                                                                       \
                    else                                                                    \
                    {                                                                       \
                        HBND(ptr) = NULL;                                                   \
                        break;                                                              \
                    }                                                                       \
                }else break;                                                                \
            }while(++IHB(ptr) < nkey);                                                      \
        }                                                                                   \
        if(HBND(ptr) && HBND(ptr)->dptr) {pdata = HBND(ptr)->dptr; pos = IHB(ptr);}         \
        MUTEX_UNLOCK(PRT(ptr)->mutex);                                                      \
    }                                                                                       \
}while(0)                                                                                           

/* FIND word/block node with MAX style from trietab */ 
#define TRIETAB_MAX_FIND(ptr, key, nkey, pdata, pos)                                        \
do{                                                                                         \
    pdata = NULL;pos = -1;                                                                  \
    if(ptr && key && nkey > 0)                                                              \
    {                                                                                       \
        MUTEX_LOCK(PRT(ptr)->mutex);                                                        \
        HBND(ptr) = (HNODE *)&(HBTBN(ptr, UNS(key[0])));                                    \
        if(HBND(ptr)->dptr){pos = 0;pdata = HBND(ptr)->dptr;}                               \
        if((IHB(ptr) = 1) < nkey)                                                           \
        {                                                                                   \
            do                                                                              \
            {                                                                               \
                PSH(ptr) = UNS(key[IHB(ptr)]);                                              \
                if(HBND(ptr))                                                               \
                {                                                                           \
                    if(HBND(ptr)->dptr){pos = IHB(ptr);pdata = HBND(ptr)->dptr;}            \
                    HN_FIND(HBND(ptr), PSH(ptr), MINHB(ptr), MAXHB(ptr), NHB(ptr));         \
                    if(NHB(ptr) >= 0 && NHB(ptr) < HCNT(HBND(ptr))                          \
                            && HNPC(HBND(ptr), NHB(ptr)) == PSH(ptr))                       \
                    {                                                                       \
                        HBND(ptr) = HNP(HBND(ptr), NHB(ptr));                               \
                    }                                                                       \
                    else                                                                    \
                    {                                                                       \
                        HBND(ptr) = NULL;                                                   \
                        break;                                                              \
                    }                                                                       \
                }else break;                                                                \
            }while(++IHB(ptr) < nkey);                                                      \
        }                                                                                   \
        if(HBND(ptr) && HBND(ptr)->dptr) {pdata = HBND(ptr)->dptr; pos = IHB(ptr);}         \
        MUTEX_UNLOCK(PRT(ptr)->mutex);                                                      \
    }                                                                                       \
}while(0)                                                                                           

/* FIND reverse word/block node with MIN style from trietab */ 
#define TRIETAB_MIN_RFIND(ptr, key, nkey, pdata, pos)                                       \
do{                                                                                         \
    pdata = NULL;pos = -1;                                                                  \
    if(ptr && key && nkey > 0)                                                              \
    {                                                                                       \
        MUTEX_LOCK(PRT(ptr)->mutex);                                                        \
        HBND(ptr) = (HNODE *)&(HBTBN(ptr, UNS(key[nkey -1])));                              \
        if(HBND(ptr)->dptr){pos = 0;pdata = HBND(ptr)->dptr;}                               \
        if(pdata == NULL && (IHB(ptr) = (nkey-2)) >= 0)                                     \
        {                                                                                   \
            do                                                                              \
            {                                                                               \
                PSH(ptr) = UNS(key[IHB(ptr)]);                                              \
                if(HBND(ptr))                                                               \
                {                                                                           \
                    HN_FIND(HBND(ptr), PSH(ptr), MINHB(ptr), MAXHB(ptr), NHB(ptr));         \
                    if(NHB(ptr) >= 0 && NHB(ptr) < HCNT(HBND(ptr))                          \
                            && HNPC(HBND(ptr), NHB(ptr)) == PSH(ptr))                       \
                    {                                                                       \
                        HBND(ptr) = HNP(HBND(ptr), NHB(ptr));                               \
                        if(HBND(ptr)->dptr){pos = nkey - IHB(ptr);break;}                   \
                    }                                                                       \
                    else                                                                    \
                    {                                                                       \
                        HBND(ptr) = NULL;                                                   \
                        break;                                                              \
                    }                                                                       \
                }else break;                                                                \
            }while(--IHB(ptr) >= 0);                                                        \
        }                                                                                   \
        if(HBND(ptr)) pdata = HBND(ptr)->dptr;                                              \
        MUTEX_UNLOCK(PRT(ptr)->mutex);                                                      \
    }                                                                                       \
}while(0)                                                                                           

/* FIND reverse word/block node with max style from trietab */ 
#define TRIETAB_MAX_RFIND(ptr, key, nkey, pdata, pos)                                       \
do{                                                                                         \
    pdata = NULL;pos = -1;                                                                  \
    if(ptr && key && nkey > 0)                                                              \
    {                                                                                       \
        MUTEX_LOCK(PRT(ptr)->mutex);                                                        \
        HBND(ptr) = (HNODE *)&(HBTBN(ptr, UNS(key[nkey -1])));                              \
        if(HBND(ptr)->dptr){pos = 0;pdata = HBND(ptr)->dptr;}                               \
        if((IHB(ptr) = (nkey - 2)) >= 0)                                                    \
        {                                                                                   \
            do                                                                              \
            {                                                                               \
                PSH(ptr) = UNS(key[IHB(ptr)]);                                              \
                if(HBND(ptr))                                                               \
                {                                                                           \
                    HN_FIND(HBND(ptr), PSH(ptr), MINHB(ptr), MAXHB(ptr), NHB(ptr));         \
                    if(NHB(ptr) >= 0 && NHB(ptr) < HCNT(HBND(ptr))                          \
                            && HNPC(HBND(ptr), NHB(ptr)) == PSH(ptr))                       \
                    {                                                                       \
                        HBND(ptr) = HNP(HBND(ptr), NHB(ptr));                               \
                        if(HBND(ptr)->dptr)                                                 \
                        {                                                                   \
                            pos = nkey - IHB(ptr);                                          \
                            pdata = HBND(ptr)->dptr;                                        \
                        }                                                                   \
                    }                                                                       \
                    else                                                                    \
                    {                                                                       \
                        HBND(ptr) = NULL;                                                   \
                        break;                                                              \
                    }                                                                       \
                }else break;                                                                \
            }while(--IHB(ptr) >= 0);                                                        \
        }                                                                                   \
        MUTEX_UNLOCK(PRT(ptr)->mutex);                                                      \
    }                                                                                       \
}while(0)         

/* DEL word/block node from trietab */ 
#define TRIETAB_DEL(ptr, key, nkey, pdata)                                                  \
do{                                                                                         \
    pdata = NULL;                                                                           \
    if(ptr && key && nkey > 0)                                                              \
    {                                                                                       \
        MUTEX_LOCK(PRT(ptr)->mutex);                                                        \
        HBND(ptr) = (HNODE *)&(HBTBN(ptr, UNS(key[0])));                                    \
        if((IHB(ptr) = 1) < nkey)                                                           \
        {                                                                                   \
            do                                                                              \
            {                                                                               \
                PSH(ptr) = UNS(key[IHB(ptr)]);                                              \
                if(HBND(ptr))                                                               \
                {                                                                           \
                    HN_FIND(HBND(ptr), PSH(ptr), MINHB(ptr), MAXHB(ptr), NHB(ptr));         \
                    if(NHB(ptr) < 0 || NHB(ptr) >= HCNT(HBND(ptr))                          \
                            || HNPC(HBND(ptr), NHB(ptr)) != PSH(ptr))                       \
                    {                                                                       \
                        HBND(ptr) = NULL;                                                   \
                        break;                                                              \
                    }                                                                       \
                    HBND(ptr) = HNP(HBND(ptr), NHB(ptr));                                   \
                }else break;                                                                \
            }while(++IHB(ptr) < nkey);                                                      \
        }                                                                                   \
        if(HBND(ptr)) {pdata = HBND(ptr)->dptr; HBND(ptr)->dptr = NULL;}                    \
        MUTEX_UNLOCK(PRT(ptr)->mutex);                                                      \
    }                                                                                       \
}while(0)                                                                                           

/* DEL reverse word/block node from trietab */ 
#define TRIETAB_RDEL(ptr, key, nkey, pdata)                                                 \
do{                                                                                         \
    pdata = NULL;                                                                           \
    if(ptr && key)                                                                          \
    {                                                                                       \
        MUTEX_LOCK(PRT(ptr)->mutex);                                                        \
        HBND(ptr) = (HNODE *)&(HBTBN(ptr, UNS(key[nkey -1])));                              \
        if((IHB(ptr) = (nkey-2)) >= 0)                                                      \
        {                                                                                   \
            do                                                                              \
            {                                                                               \
                PSH(ptr) = UNS(key[IHB(ptr)]);                                              \
                if(HBND(ptr))                                                               \
                {                                                                           \
                    HN_FIND(HBND(ptr), PSH(ptr), MINHB(ptr), MAXHB(ptr), NHB(ptr));         \
                    if(NHB(ptr) < 0 || NHB(ptr) >= HCNT(HBND(ptr))                          \
                            || HNPC(HBND(ptr), NHB(ptr)) != PSH(ptr))                       \
                    {                                                                       \
                        HBND(ptr) = NULL;                                                   \
                        break;                                                              \
                    }                                                                       \
                    HBND(ptr) = HNP(HBND(ptr), NHB(ptr));                                   \
                }else break;                                                                \
            }while(--IHB(ptr) >= 0);                                                        \
        }                                                                                   \
        if(HBND(ptr)) {pdata = HBND(ptr)->dptr; HBND(ptr)->dptr = NULL;}                    \
        MUTEX_UNLOCK(PRT(ptr)->mutex);                                                      \
    }                                                                                       \
}while(0)

/* clean trietab */
#define TRIETAB_CLEAN(ptr)                                                                  \
do{                                                                                         \
    if(ptr)                                                                                 \
    {                                                                                       \
        trietab_clean(ptr);                                                                 \
        MUTEX_DESTROY(PRT(ptr)->mutex);                                                     \
        free(ptr);                                                                          \
        ptr = NULL;                                                                         \
    }                                                                                       \
}while(0)

#ifdef __cplusplus
}
#endif
#endif
