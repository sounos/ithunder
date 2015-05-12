#include "mutex.h"
#ifndef _CDB_H_
#define _CDB_H_
#include <pthread.h>
#define CDB_LNK_MAX          8388608
#define CDB_LNK_INCREMENT    65536
#define CDB_CDBX_MAX          2000000000
#define CDB_CDBX_BASE         1000000
#define CDB_BASE_SIZE        4096
#define CDB_PATH_MAX         1024
#define CDB_DIR_FILES        64
#define CDB_BUF_SIZE         4096
#define CDB_XCBLOCKS_MAX      15
#define CDB_MBLOCKS_MAX      1024
#define CDB_MBLOCK_BASE      4096
#define CDB_MBLOCK_MAX       67108864
#define CDB_MUTEX_MAX        65536
#define CDB_USE_MMAP         0x01
#define CDB_BIGFILE_SIZE     2147483648
//#define  CDB_MBLOCK_MAX      1048576
//#define  CDB_MBLOCK_MAX      2097152
//#define  CDB_MBLOCK_MAX        4194304
//#define CDB_MBLOCK_MAX         8388608
//#define CDB_MBLOCK_MAX       16777216
//#define CDB_MBLOCK_MAX       33554432
//#define CDB_MFILE_SIZE     2097152
//#define CDB_MFILE_SIZE       8388608  //8M
//#define CDB_MFILE_SIZE       16777216   //16M
//#define CDB_MFILE_SIZE       33554432   //32M
//#define CDB_MFILE_SIZE       67108864   //64M
//#define CDB_MFILE_SIZE     134217728  //128M
//#define CDB_MFILE_SIZE       268435456  //256M
#define CDB_MFILE_SIZE     536870912  //512M
//#define CDB_MFILE_SIZE       1073741824 //1G
#define CDB_MFILE_MAX        8192
#define CDB_BLOCK_INCRE_LEN      0x0
#define CDB_BLOCK_INCRE_DOUBLE   0x1
typedef struct _XCIO
{
    int     fd;
    int     bits;
    char    *map;
    off_t   old;
    off_t   end;
    off_t   size;
    pthread_rwlock_t mutex;
}XCIO;
typedef struct _XCBLOCK
{
    char *mblocks[CDB_MBLOCKS_MAX];
    int nmblocks;
    int total;
}XCBLOCK;
typedef struct _XCSTATE
{
    int status;
    int mode;
    int last_id;
    size_t last_off;
    int cdb_id_max;
    int data_len_max;
    int block_incre_mode;
}XCSTATE;
typedef struct _CDB
{
    int     status;
    int     block_max;
    off_t   mm_total;
    off_t   xx_total;
    void    *kmap;
    void    *logger;
    XCSTATE  *state;
    XCIO     stateio;
    XCIO     lnkio;
    XCIO     dbxio;
    XCIO     dbsio[CDB_MFILE_MAX];
    XCBLOCK  xblocks[CDB_XCBLOCKS_MAX];
    char    basedir[CDB_PATH_MAX];
    pthread_rwlock_t mutex;
    pthread_rwlock_t mutex_lnk;
    pthread_rwlock_t mutex_dbx;
    pthread_rwlock_t mutex_mblock;
    pthread_rwlock_t mutexs[CDB_MUTEX_MAX];
}CDB;
/* initialize db */
CDB* cdb_init(char *dir, int is_mmap);
/* set block incre mode */
int cdb_set_block_incre_mode(CDB *db, int mode);
/* set tag */
int cdb_set_tag(CDB *db, int id, int tag);
/* get tag */
int cdb_get_tag(CDB *db, int id, int *tag);
/* get data id */
int cdb_data_id(CDB *db, char *key, int nkey);
/* chunk data */
int cdb_chunk_data(CDB *db, int id, char *data, int ndata, int length);
/* set data return blockid */
int cdb_set_data(CDB *db, int id, char *data, int ndata);
/* set mod_time */
int cdb_update_modtime(CDB *db, int id);
/* get mod_time */
time_t cdb_get_modtime(CDB *db, int id);
/* xchunk data */
int cdb_xchunk_data(CDB *db, char *key, int nkey, char *data, int ndata, int length);
/* set data */
int cdb_xset_data(CDB *db, char *key, int nkey, char *data, int ndata);
/* add data */
int cdb_add_data(CDB *db, int id, char *data, int ndata);
/* resize */
int cdb_xresize(CDB *db, char *key, int nkey, int length);
/* xadd data */
int cdb_xadd_data(CDB *db, char *key, int nkey, char *data, int ndata);
/* get data */
int cdb_get_data(CDB *db, int id, char **data);
/* get data len */
int cdb_get_data_len(CDB *db, int id);
/* xget data */
int cdb_xget_data(CDB *db, char *key, int nkey, char **data, int *ndata);
/* xget data len */
int cdb_xget_data_len(CDB *db, char *key, int nkey);
/* check key dataid/len */
int cdb_xcheck(CDB *db, char *key, int nkey, int *len, time_t *mod_time);
/* truncate block */
void *cdb_truncate_block(CDB *db, int id, int ndata);
/* get data block address and len */
int cdb_exists_block(CDB *db, int id, char **ptr);
/* read data */
int cdb_read_data(CDB *db, int id, char *data);
/* pread data */
int cdb_pread_data(CDB *db, int id, char *data, int len, int off);
/* xread data */
int cdb_xread_data(CDB *db, char *key, int nkey, char *data);
/* xpread data */
int cdb_xpread_data(CDB *db, char *key, int nkey, char *data, int len, int off);
char *cdb_new_data(CDB *db, size_t size);
/* free data */
void cdb_free_data(CDB *db, char *data, size_t size);
/* delete data */
int cdb_del_data(CDB *db, int id);
/* delete data */
int cdb_xdel_data(CDB *db, char *key, int nkey);
/* destroy */
void cdb_destroy(CDB *db);
/* reset */
void cdb_reset(CDB *db);
/* clean db */
void cdb_clean(CDB *db);
#define PCDB(xxx) ((CDB *)xxx)
#endif
