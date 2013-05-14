#include "mutex.h"
#ifndef _DB_H_
#define _DB_H_
#ifdef HAVE_PTHREAD
#include <pthread.h>
#endif
#define DB_LNK_MAX          2097152
#define DB_LNK_INCREMENT    65536
#define DB_DBX_MAX          2000000000
#define DB_DBX_BASE         1000000
#define DB_BASE_SIZE        16
#define DB_PATH_MAX         1024
#define DB_DIR_FILES        64
#define DB_BUF_SIZE         4096
#define DB_XBLOCKS_MAX      14
#define DB_MBLOCKS_MAX      1024
#define DB_MBLOCK_BASE      4096
#define DB_MBLOCK_MAX       33554432
#define DB_MUTEX_MAX        65536
#define DB_USE_MMAP         0x01
//#define  DB_MBLOCK_MAX      1048576
//#define  DB_MBLOCK_MAX      2097152
//#define  DB_MBLOCK_MAX        4194304
//#define DB_MBLOCK_MAX         8388608
//#define DB_MBLOCK_MAX       16777216
//#define DB_MBLOCK_MAX       33554432
//#define DB_MFILE_SIZE     2097152
//#define DB_MFILE_SIZE       8388608  //8M
//#define DB_MFILE_SIZE       16777216   //16M
//#define DB_MFILE_SIZE       33554432   //32M
//#define DB_MFILE_SIZE       67108864   //64M
//#define DB_MFILE_SIZE     134217728  //128M
#define DB_MFILE_SIZE       268435456  //256M
//#define DB_MFILE_SIZE     536870912  //512M
//#define DB_MFILE_SIZE       1073741824 //1G
#define DB_MFILE_MAX        8129
#define DB_BLOCK_INCRE_LEN      0x0
#define DB_BLOCK_INCRE_DOUBLE   0x1
typedef struct _DBX
{
    int block_size;
    int blockid;
    int ndata;
    int index;
    int mod_time;
}DBX;
typedef struct _XIO
{
    int     fd;
    int     bits;
    char    *map;
    void    *mutex;
    off_t   old;
    off_t   end;
    off_t   size;
}XIO;
typedef struct _XLNK
{
    int index;
    int blockid;
    int count;
}XLNK;
typedef struct _XXMM
{
    int block_size;
    int blocks_max;
}XXMM;
typedef struct _XBLOCK
{
    char *mblocks[DB_MBLOCKS_MAX];
    int nmblocks;
    int total;
}XBLOCK;
typedef struct _XSTATE
{
    int status;
    int mode;
    int last_id;
    int last_off;
    int db_id_max;
    int data_len_max;
    int block_incre_mode;
}XSTATE;
typedef struct _DB
{
    int     status;
    int     block_max;
    off_t   mm_total;
    off_t   xx_total;
    MUTEX   *mutex;
    MUTEX   *mutex_lnk;
    MUTEX   *mutex_dbx;
    MUTEX   *mutex_mblock;
    void    *kmap;
    void    *logger;
    XSTATE  *state;
    XIO     stateio;
    XIO     lnkio;
    XIO     dbxio;
    XIO     dbsio[DB_MFILE_MAX];
    XBLOCK  xblocks[DB_XBLOCKS_MAX];
    char    basedir[DB_PATH_MAX];
#ifdef HAVE_PTHREAD
    pthread_mutex_t mutexs[DB_MUTEX_MAX];
#endif
}DB;
/* initialize db */
DB* db_init(char *dir, int is_mmap);
/* set block incre mode */
int db_set_block_incre_mode(DB *db, int mode);
/* get data id */
int db_data_id(DB *db, char *key, int nkey);
/* chunk data */
int db_chunk_data(DB *db, int id, char *data, int ndata, int length);
/* set data return blockid */
int db_set_data(DB *db, int id, char *data, int ndata);
/* set mod_time */
int db_update_modtime(DB *db, int id);
/* get mod_time */
time_t db_get_modtime(DB *db, int id);
/* xchunk data */
int db_xchunk_data(DB *db, char *key, int nkey, char *data, int ndata, int length);
/* set data */
int db_xset_data(DB *db, char *key, int nkey, char *data, int ndata);
/* add data */
int db_add_data(DB *db, int id, char *data, int ndata);
/* xadd data */
int db_xadd_data(DB *db, char *key, int nkey, char *data, int ndata);
/* get data */
int db_get_data(DB *db, int id, char **data);
/* get data len */
int db_get_data_len(DB *db, int id);
/* xget data */
int db_xget_data(DB *db, char *key, int nkey, char **data, int *ndata);
/* xget data len */
int db_xget_data_len(DB *db, char *key, int nkey);
/* check key dataid/len */
int db_xcheck(DB *db, char *key, int nkey, int *len, time_t *mod_time);
/* get data block address and len */
int db_exists_block(DB *db, int id, char **ptr);
/* read data */
int db_read_data(DB *db, int id, char *data);
/* pread data */
int db_pread_data(DB *db, int id, char *data, int len, int off);
/* xread data */
int db_xread_data(DB *db, char *key, int nkey, char *data);
/* xpread data */
int db_xpread_data(DB *db, char *key, int nkey, char *data, int len, int off);
/* free data */
void db_free_data(DB *db, char *data, size_t size);
/* delete data */
int db_del_data(DB *db, int id);
/* delete data */
int db_xdel_data(DB *db, char *key, int nkey);
/* destroy */
void db_destroy(DB *db);
/* clean db */
void db_clean(DB *db);
#define PDB(xxx) ((DB *)xxx)
#endif
