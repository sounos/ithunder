#ifndef _MMTRIE_H_
#define _MMTRIE_H_
#include "mutex.h"
#ifdef __cplusplus
extern "C" {
#endif
#define MMTRIE_PATH_MAX             256
#define MMTRIE_LINE_MAX             256
#define MMTRIE_BASE_NUM             1000000
#define MMTRIE_NODES_MAX            1000000000
#define MMTRIE_WORD_MAX             4096
typedef struct _MMTRLIST
{
    int count;
    int head;
}MMTRLIST;
/* trie node */
typedef struct _MMTRNODE
{
    short key;
    short nchilds;
    int data;
    int childs;
}MMTRNODE;
/* state */
typedef struct _MMTRSTATE
{
    int id;
    int current;
    int total;
    int left;
    MMTRLIST list[MMTRIE_LINE_MAX];
}MMTRSTATE;
/* MEM trie */
typedef struct _MMTRIE
{
    MMTRSTATE   *state;
    MMTRNODE    *nodes;
    char        *map;
    off_t       size;
    off_t       old;
    off_t       file_size;
    int         fd;
    int         bits;
    MUTEX       *mutex;

    int  (*add)(struct _MMTRIE *, char *key, int nkey, int data);
    int  (*xadd)(struct _MMTRIE *, char *key, int nkey);
    int  (*get)(struct _MMTRIE *, char *key, int nkey);
    int  (*del)(struct _MMTRIE *, char *key, int nkey);
    int  (*find)(struct _MMTRIE *, char *key, int nkey, int *len);
    int  (*maxfind)(struct _MMTRIE *, char *key, int nkey, int *len);
    int  (*radd)(struct _MMTRIE *, char *key, int nkey, int data);
    int  (*rxadd)(struct _MMTRIE *, char *key, int nkey);
    int  (*rget)(struct _MMTRIE *, char *key, int nkey);
    int  (*rdel)(struct _MMTRIE *, char *key, int nkey);
    int  (*rfind)(struct _MMTRIE *, char *key, int nkey, int *len);
    int  (*rmaxfind)(struct _MMTRIE *, char *key, int nkey, int *len);
    int  (*import)(struct _MMTRIE *, char *dictfile, int direction);
    void (*clean)(struct _MMTRIE *);
}MMTRIE;
/* initialize */
MMTRIE   *mmtrie_init(char *file);
/* add */
int   mmtrie_add(struct _MMTRIE *, char *key, int nkey, int data);
/* add return auto_increment_id */
int   mmtrie_xadd(struct _MMTRIE *, char *key, int nkey);
/* get */
int   mmtrie_get(struct _MMTRIE *, char *key, int nkey);
/* delete */
int   mmtrie_del(struct _MMTRIE *, char *key, int nkey);
/* find/min */
int   mmtrie_find(struct _MMTRIE *, char *key, int nkey, int *len);
/* find/max */
int   mmtrie_maxfind(struct _MMTRIE *, char *key, int nkey, int *len);
/* add/reverse */
int   mmtrie_radd(struct _MMTRIE *, char *key, int nkey, int data);
/* add/reverse return auto_increment_id */
int   mmtrie_rxadd(struct _MMTRIE *, char *key, int nkey);
/* get/reverse */
int   mmtrie_rget(struct _MMTRIE *, char *key, int nkey);
/* del/reverse */
int   mmtrie_rdel(struct _MMTRIE *, char *key, int nkey);
/* find/min/reverse */
int   mmtrie_rfind(struct _MMTRIE *, char *key, int nkey, int *len);
/* find/max/reverse */
int   mmtrie_rmaxfind(struct _MMTRIE *, char *key, int nkey, int *len);
/* import dict if direction value is -1, add word reverse */
int   mmtrie_import(struct _MMTRIE *, char *dictfile, int direction);
/* destroy */
void mmtrie_destroy(struct _MMTRIE *);
/* clean/reverse */
void  mmtrie_clean(struct _MMTRIE *);
#define MMTR(x) ((MMTRIE *)x)
#ifdef __cplusplus
     }
#endif
#endif
