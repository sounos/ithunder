#ifndef _IDB_H
#define _IDB_H
#include "ibase.h"
#ifndef IDB_BSIZE 
#define IDB_BSIZE        512
#endif
#ifndef IDB_NUM_MAX      
#define IDB_NUM_MAX      32
#endif
#ifndef IDB_MMB_SIZE
#define IDB_MMB_SIZE     1004
#endif
#ifndef IDB_INCREMENT    
#define IDB_INCREMENT    100000
#endif
#ifndef IDB_INDEX_NUM  
#define IDB_INDEX_NUM      1
#endif
#ifndef IDB_IKEY_MAX     
#define IDB_IKEY_MAX     1000000
#endif
#ifndef IDB_PATH_MAX 
#define IDB_PATH_MAX     256
#endif
#ifndef IDB_BUF_SIZE
#define IDB_BUF_SIZE     4096
#endif
#ifndef IDB_IDX_MAX
#define IDB_IDX_MAX         34359738368ll     
#define IDB_IDX_INCREMENT   33554432     
#endif
#ifndef IDB_MBLOCKS_MAX
#define IDB_MBLOCKS_MAX      32
//#define IDB_MBLOCK_MAX       16777216
#define IDB_MBLOCK_MAX       33554432
//#define IDB_MBLOCK_MAX     67108864
#endif
/* key */
typedef struct _IKEY
{
   int blockid;
   int block_size;
   int btotal;
}IKEY;
/* index */
typedef struct _IDX
{
    int     fd;
    char    *map;
    off_t   end;
    off_t   size;
}IDX;
/* link */
typedef struct _ILINK
{
    int total;
    int first;
    int last;
}ILINK;
/* iblock */
typedef struct _IDB
{
    IDX     hash[IDB_INDEX_NUM];
    ILINK   *links;
    int     link_fd;
    int     link_size;
    IKEY    *kmap;
    int     kmap_fd;
    off_t   kmap_size;
    int     kmap_maxid;
    char    *qmblocks[IDB_MBLOCKS_MAX];
    int     nqmblocks;
    MUTEX   mutex;
    void    *logger;

    int  (*set_basedir)(struct _IDB *, char *dir);
    int  (*add)(struct _IDB *, int key, char *data, int ndata);
    int  (*index)(struct _IDB *, int key, int docid, int no, int bits_fields, 
            int term_count, int prevnext_size, char *prevnext);
    int  (*get)(struct _IDB *, int key, IBDATA *mm);
    int  (*update)(struct _IDB *, int key, char *data, int ndata);
    int  (*del)(struct _IDB *, int key);
    void (*free)(struct _IDB *, IBDATA *mm);
    void (*clean)(struct _IDB *);
}IDB;
#define PIDB(x) ((IDB *)x) 
/* initialize iblock */
IDB *idb_init();
/* set base directory */
int idb_set_basedir(IDB *, char *dir);
/* add */
int idb_add(IDB *, int key, char *data, int ndata);
/* index */
int idb_index(IDB *, int termid, int docid, int no, int fields, 
        int term_count, int prevnext_size, char *prevnext);
/* get data */
int idb_get(IDB *, int key, IBDATA *dbmm);
/* update data */
int idb_update(IDB *, int key, char *data, int ndata);
/* delete data */
int idb_del(IDB *, int key);
/* free data*/
void  idb_free(IDB *, IBDATA *dbmm);
/* clean db */
void idb_clean(IDB *);
#endif
