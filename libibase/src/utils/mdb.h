#include "mutex.h"
#ifndef _MDB_H_
#define _MDB_H_
#include <pthread.h>
#define MDB_LNK_MAX          8388608
#define MDB_LNK_INCREMENT    65536
#define MDB_MDBX_MAX          2000000000
#define MDB_MDBX_BASE         1000000
#define MDB_BASE_SIZE        64
#define MDB_PATH_MAX         1024
#define MDB_DIR_FILES        64
#define MDB_BUF_SIZE         4096
#define MDB_XMBLOCKS_MAX      15
#define MDB_MBLOCKS_MAX      1024
#define MDB_MBLOCK_BASE      4096
#define MDB_MBLOCK_MAX       67108864
#define MDB_MUTEX_MAX        65536
#define MDB_USE_MMAP         0x01
//#define  MDB_MBLOCK_MAX      1048576
//#define  MDB_MBLOCK_MAX      2097152
//#define  MDB_MBLOCK_MAX        4194304
//#define MDB_MBLOCK_MAX         8388608
//#define MDB_MBLOCK_MAX       16777216
//#define MDB_MBLOCK_MAX       33554432
//#define MDB_MFILE_SIZE     2097152
//#define MDB_MFILE_SIZE       8388608  //8M
//#define MDB_MFILE_SIZE       16777216   //16M
//#define MDB_MFILE_SIZE       33554432   //32M
//#define MDB_MFILE_SIZE       67108864   //64M
//#define MDB_MFILE_SIZE     134217728  //128M
//#define MDB_MFILE_SIZE       268435456  //256M
#define MDB_MFILE_SIZE     536870912  //512M
//#define MDB_MFILE_SIZE       1073741824 //1G
#define MDB_MFILE_MAX        8192
#define MDB_BLOCK_INCRE_LEN      0x0
#define MDB_BLOCK_INCRE_DOUBLE   0x1
typedef struct _XMIO
{
    int     fd;
    int     bits;
    char    *map;
    off_t   old;
    off_t   end;
    off_t   size;
    pthread_rwlock_t mutex;
}XMIO;
typedef struct _XMBLOCK
{
    char *mblocks[MDB_MBLOCKS_MAX];
    int nmblocks;
    int total;
}XMBLOCK;
typedef struct _XMSTATE
{
    int status;
    int mode;
    int last_id;
    int last_off;
    int mdb_id_max;
    int data_len_max;
    int block_incre_mode;
}XMSTATE;
typedef struct _MDB
{
    int     status;
    int     block_max;
    off_t   mm_total;
    off_t   xx_total;
    void    *kmap;
    void    *logger;
    XMSTATE  *state;
    XMIO     stateio;
    XMIO     lnkio;
    XMIO     dbxio;
    XMIO     dbsio[MDB_MFILE_MAX];
    XMBLOCK  xblocks[MDB_XMBLOCKS_MAX];
    char    basedir[MDB_PATH_MAX];
    pthread_rwlock_t mutex;
    pthread_rwlock_t mutex_lnk;
    pthread_rwlock_t mutex_dbx;
    pthread_rwlock_t mutex_mblock;
    pthread_rwlock_t mutexs[MDB_MUTEX_MAX];
}MDB;
/* initialize db */
MDB* mdb_init(char *dir, int is_mmap);
/* set block incre mode */
int mdb_set_block_incre_mode(MDB *db, int mode);
/* set tag */
int mdb_set_tag(MDB *db, int id, int tag);
/* get tag */
int mdb_get_tag(MDB *db, int id, int *tag);
/* get data id */
int mdb_data_id(MDB *db, char *key, int nkey);
/* chunk data */
int mdb_chunk_data(MDB *db, int id, char *data, int ndata, int length);
/* set data return blockid */
int mdb_set_data(MDB *db, int id, char *data, int ndata);
/* set mod_time */
int mdb_update_modtime(MDB *db, int id);
/* get mod_time */
time_t mdb_get_modtime(MDB *db, int id);
/* xchunk data */
int mdb_xchunk_data(MDB *db, char *key, int nkey, char *data, int ndata, int length);
/* set data */
int mdb_xset_data(MDB *db, char *key, int nkey, char *data, int ndata);
/* add data */
int mdb_add_data(MDB *db, int id, char *data, int ndata);
/* resize */
int mdb_xresize(MDB *db, char *key, int nkey, int length);
/* xadd data */
int mdb_xadd_data(MDB *db, char *key, int nkey, char *data, int ndata);
/* get data */
int mdb_get_data(MDB *db, int id, char **data);
/* get data len */
int mdb_get_data_len(MDB *db, int id);
/* xget data */
int mdb_xget_data(MDB *db, char *key, int nkey, char **data, int *ndata);
/* xget data len */
int mdb_xget_data_len(MDB *db, char *key, int nkey);
/* check key dataid/len */
int mdb_xcheck(MDB *db, char *key, int nkey, int *len, time_t *mod_time);
/* truncate block */
void *mdb_truncate_block(MDB *db, int id, int ndata);
/* get data block address and len */
int mdb_exists_block(MDB *db, int id, char **ptr);
/* read data */
int mdb_read_data(MDB *db, int id, char *data);
/* pread data */
int mdb_pread_data(MDB *db, int id, char *data, int len, int off);
/* xread data */
int mdb_xread_data(MDB *db, char *key, int nkey, char *data);
/* xpread data */
int mdb_xpread_data(MDB *db, char *key, int nkey, char *data, int len, int off);
char *mdb_new_data(MDB *db, size_t size);
/* free data */
void mdb_free_data(MDB *db, char *data, size_t size);
/* delete data */
int mdb_del_data(MDB *db, int id);
/* delete data */
int mdb_xdel_data(MDB *db, char *key, int nkey);
/* destroy */
void mdb_destroy(MDB *db);
/* reset */
void mdb_reset(MDB *db);
/* clean db */
void mdb_clean(MDB *db);
#define PMDB(xxx) ((MDB *)xxx)
#endif
