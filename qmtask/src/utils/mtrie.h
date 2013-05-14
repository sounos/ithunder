#ifndef _MTRIE_H_
#define _MTRIE_H_
#ifdef __cplusplus
extern "C" {
#endif
#define MTRIE_PATH_MAX             256
#define MTRIE_LINE_MAX             256
#define MTRIE_INCREMENT_NUM        100000
#define MTRIE_NODES_MAX            1000000
#define MTRIE_WORD_MAX             4096
#include "mutex.h"
typedef struct _MTRLIST
{
    int count;
    int head;
}MTRLIST;
/* trie node */
typedef struct _MTRNODE
{
    short key;
    short nchilds;
    int data;
    int childs;
}MTRNODE;
/* state */
typedef struct _MTRSTATE
{
    int id;
    int current;
    int total;
    int left;
    MTRLIST list[MTRIE_LINE_MAX];
}MTRSTATE;
/* MEM trie */
typedef struct _MTRIE
{
    MTRSTATE    *state;
    MTRNODE     *nodes;
    char        *map;
    char        *old_map;
    MUTEX       *mutex;
    off_t       map_size;
    off_t       size;

    int  (*add)(void *, char *key, int nkey, int data);
    int  (*xadd)(void *, char *key, int nkey);
    int  (*get)(void *, char *key, int nkey);
    int  (*del)(void *, char *key, int nkey);
    int  (*find)(void *, char *key, int nkey, int *len);
    int  (*maxfind)(void *, char *key, int nkey, int *len);
    int  (*radd)(void *, char *key, int nkey, int data);
    int  (*rxadd)(void *, char *key, int nkey);
    int  (*rget)(void *, char *key, int nkey);
    int  (*rdel)(void *, char *key, int nkey);
    int  (*rfind)(void *, char *key, int nkey, int *len);
    int  (*rmaxfind)(void *, char *key, int nkey, int *len);
    int  (*import)(void *, char *dictfile, int direction);
    void (*clean)(void *);
}MTRIE;
/* initialize */
MTRIE   *mtrie_init();
/* add */
int   mtrie_add(void *, char *key, int nkey, int data);
/* add return auto_increment_id */
int   mtrie_xadd(void *, char *key, int nkey);
/* get */
int   mtrie_get(void *, char *key, int nkey);
/* delete */
int   mtrie_del(void *, char *key, int nkey);
/* find/min */
int   mtrie_find(void *, char *key, int nkey, int *len);
/* find/max */
int   mtrie_maxfind(void *, char *key, int nkey, int *len);
/* add/reverse */
int   mtrie_radd(void *, char *key, int nkey, int data);
/* add/reverse return auto_increment_id */
int   mtrie_rxadd(void *, char *key, int nkey);
/* get/reverse */
int   mtrie_rget(void *, char *key, int nkey);
/* del/reverse */
int   mtrie_rdel(void *, char *key, int nkey);
/* find/min/reverse */
int   mtrie_rfind(void *, char *key, int nkey, int *len);
/* find/max/reverse */
int   mtrie_rmaxfind(void *, char *key, int nkey, int *len);
/* import dict if direction value is -1, add word reverse */
int   mtrie_import(void *, char *dictfile, int direction);
/* destroy */
void mtrie_destroy(void *);
/* clean/reverse */
void  mtrie_clean(void *);
#define MTR(x) ((MTRIE *)x)
#ifdef __cplusplus
     }
#endif
#endif
