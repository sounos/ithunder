#define _XOPEN_SOURCE 500
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include "mdb.h"
#include "rwlock.h"
#include "mmtrie.h"
#include "logger.h"
#include "xmm.h"
#ifdef MAP_LOCKED
#define MMAP_SHARED MAP_SHARED|MAP_LOCKED
#else
#define MMAP_SHARED MAP_SHARED
#endif
#define MDB_CHECK_MMAP(xdb, index)                                                           \
do                                                                                          \
{                                                                                           \
    if(xdb->state->mode && xdb->dbsio[index].fd > 0)                                     \
    {                                                                                       \
        if(xdb->dbsio[index].map == NULL || xdb->dbsio[index].map == (void *)-1)            \
        {                                                                                   \
            xdb->dbsio[index].map = mmap(NULL, MDB_MFILE_SIZE, PROT_READ|PROT_WRITE,         \
                    MAP_SHARED, xdb->dbsio[index].fd, 0);                                   \
        }                                                                                   \
    }                                                                                       \
}while(0)
#ifndef LL
#define LL(xll) ((long long int)xll)
#endif
static MXXMM mdb_xblock_list[] = {{4096,1024},{8192,1024},{16384,1024},{32768,1024},{65536,1024},{131072,1024},{262144,1024},{524288,512},{1048576,256},{2097152,64},{4194304,32},{8388608,16},{16777216,8},{33554432,4},{67108864,2}};
int mdb_mkdir(char *path)
{
    struct stat st;
    char fullpath[MDB_PATH_MAX];
    char *p = NULL;
    int level = -1, ret = -1;

    if(path)
    {
        strcpy(fullpath, path);
        p = fullpath;
        while(*p != '\0')
        {
            if(*p == '/' )
            {
                level++;
                while(*p != '\0' && *p == '/' && *(p+1) == '/')++p;
                if(level > 0)
                {
                    *p = '\0';                    
                    memset(&st, 0, sizeof(struct stat));
                    ret = stat(fullpath, &st);
                    if(ret == 0 && !S_ISDIR(st.st_mode)) return -1;
                    if(ret != 0 && mkdir(fullpath, 0755) != 0) return -1;
                    *p = '/';
                }
            }
            ++p;
        }
        return 0;
    }
    return -1;
}
int mdb__resize(MDB *db, int id, int length);
/* initialize dbfile */
MDB *mdb_init(char *dbdir, int mode)
{
    char path[MDB_PATH_MAX];
    struct stat st = {0};
    MDB *db = NULL;

    if(dbdir && (db = (MDB *)xmm_mnew(sizeof(MDB))))
    {
        RWLOCK_INIT(db->mutex_lnk);
        RWLOCK_INIT(db->mutex_dbx);
        RWLOCK_INIT(db->mutex_mblock);
        RWLOCK_INIT(db->mutex);
        strcpy(db->basedir, dbdir);
        /* initialize kmap */
        sprintf(path, "%s/%s", dbdir, "db.kmap");    
        mdb_mkdir(path);
        db->kmap = mmtrie_init(path);
        /* logger */
        sprintf(path, "%s/%s", dbdir, "db.log");    
        LOGGER_INIT(db->logger, path);
        //LOGGER_SET_LEVEL(db->logger, 2);
        /* open state */
        sprintf(path, "%s/%s", dbdir, "db.state");    
        if((db->stateio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(db->stateio.fd, &st) == 0)
        {
            db->stateio.end = st.st_size;
            if(st.st_size == 0)
            {
                db->stateio.end = db->stateio.size = sizeof(XMSTATE);
                if(ftruncate(db->stateio.fd, db->stateio.end) != 0)
                {
                    FATAL_LOGGER(db->logger, "ftruncate state %s failed, %s\n", path, strerror(errno));
                    _exit(-1);
                }
            }
            if((db->stateio.map = mmap(NULL, db->stateio.end, PROT_READ|PROT_WRITE,  
                            MAP_SHARED, db->stateio.fd, 0)) == NULL || db->stateio.map == (void *)-1)
            {
                FATAL_LOGGER(db->logger, "mmap state:%s failed, %s\n", path, strerror(errno));
                _exit(-1);
            }
            db->state = (XMSTATE *)(db->stateio.map);
            if(st.st_size == 0) memset(db->state, 0, sizeof(XMSTATE));
            db->state->mode = mode;
        }
        else
        {
            FATAL_LOGGER(db->logger, "open link file:%s failed, %s\n", path, strerror(errno));
            _exit(-1);
        }
        /* open link */
        sprintf(path, "%s/%s", dbdir, "db.lnk");    
        if((db->lnkio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(db->lnkio.fd, &st) == 0)
        {
            db->lnkio.end = st.st_size;
            if(st.st_size == 0)
            {
                db->lnkio.end = db->lnkio.size = sizeof(XMLNK) * MDB_LNK_MAX;
                if(ftruncate(db->lnkio.fd, db->lnkio.end) != 0)
                {
                    FATAL_LOGGER(db->logger, "ftruncate %s failed, %s\n", path, strerror(errno));
                    _exit(-1);
                }
            }
            if((db->lnkio.map = mmap(NULL, db->lnkio.end, PROT_READ|PROT_WRITE,  
                            MAP_SHARED, db->lnkio.fd, 0)) == NULL || db->lnkio.map == (void *)-1)
            {
                FATAL_LOGGER(db->logger, "mmap link:%s failed, %s\n", path, strerror(errno));
                _exit(-1);
            }
            if(st.st_size == 0) memset(db->lnkio.map, 0, db->lnkio.end);
        }
        else
        {
            FATAL_LOGGER(db->logger, "open link file:%s failed, %s\n", path, strerror(errno));
            _exit(-1);
        }
        /* open dbx */
        sprintf(path, "%s/%s", dbdir, "db.dbx");    
        if((db->dbxio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(db->dbxio.fd, &st) == 0)
        {
            db->dbxio.end = st.st_size;
            db->dbxio.size = MDB_MDBX_MAX * sizeof(MDBX);
            if((db->dbxio.map = mmap(NULL, db->dbxio.size, PROT_READ|PROT_WRITE,  
                            MAP_SHARED, db->dbxio.fd, 0)) == NULL || db->dbxio.map == (void *)-1)
            {
                FATAL_LOGGER(db->logger,"mmap dbx:%s failed, %s\n", path, strerror(errno));
                _exit(-1);
            }
            if(st.st_size == 0)
            {
                db->dbxio.end = sizeof(MDBX) * MDB_MDBX_BASE;
                if(ftruncate(db->dbxio.fd, db->dbxio.end) != 0)
                {
                    FATAL_LOGGER(db->logger, "ftruncate %s failed, %s\n", path, strerror(errno));
                    _exit(-1);
                }
                memset(db->dbxio.map, 0, db->dbxio.end);
            }
            db->dbxio.old = db->dbxio.end;
        }
        else
        {
            fprintf(stderr, "open link file:%s failed, %s\n", path, strerror(errno));
            _exit(-1);
        }
        /* open dbs */
        int i = 0;
        for(i = 0; i <= db->state->last_id; i++)
        {
            sprintf(path, "%s/base/%d/%d.db", dbdir, i/MDB_DIR_FILES, i);    
            mdb_mkdir(path);
            RWLOCK_INIT(db->dbsio[i].mutex);
            if((db->dbsio[i].fd = open(path, O_CREAT|O_RDWR, 0644)) > 0 
                    && fstat(db->dbsio[i].fd, &st) == 0)
            {
                if(st.st_size == 0)
                {
                    db->dbsio[i].size = MDB_MFILE_SIZE;
                    if(ftruncate(db->dbsio[i].fd, db->dbsio[i].size) != 0)
                    {
                        FATAL_LOGGER(db->logger, "ftruncate db:%s failed, %s", path, strerror(errno));
                        _exit(-1);
                    }
                }
                else
                {
                    db->dbsio[i].size = st.st_size;
                }
                MDB_CHECK_MMAP(db, i);
                //WARN_LOGGER(db->logger, "dbs[%d] path:%s fd:%d map:%p last:%d", i, path, db->dbsio[i].fd, db->dbsio[i].map, db->state->last_id);
                if(db->dbsio[i].map && db->state->last_id == 0 && db->state->last_off == 0)
                {
                    memset(db->dbsio[i].map, 0, MDB_MFILE_SIZE);
                    //WARN_LOGGER(db->logger, "dbs[%d] path:%s fd:%d map:%p last:%d", i, path, db->dbsio[i].fd, db->dbsio[i].map, db->state->last_id);
                }
            }
            else
            {
                FATAL_LOGGER(db->logger, "open db file:%s failed, %s", path, strerror(errno));
                _exit(-1);
            }
        }
        /* initialize mutexs  */
        for(i = 0; i < MDB_MUTEX_MAX; i++)
        {
            RWLOCK_INIT(db->mutexs[i]);
        }
    }
    return db;
}
void mdb_mutex_wrlock(MDB *db, int id)
{
    if(db)
    {
        RWLOCK_WRLOCK(db->mutexs[id%MDB_MUTEX_MAX]);
    }
    return ;
}
void mdb_mutex_rdlock(MDB *db, int id)
{
    if(db)
    {
        RWLOCK_RDLOCK(db->mutexs[id%MDB_MUTEX_MAX]);
    }
    return ;
}
void mdb_mutex_unlock(MDB *db, int id)
{
    if(db)
    {
        RWLOCK_UNLOCK(db->mutexs[id%MDB_MUTEX_MAX]);
    }
    return ;
}
/* read data */
int mdb_pread(MDB *db, int index, void *data, int ndata, off_t offset)
{
    int n = -1;

    if(db && index >= 0 && data && ndata > 0 && offset >= 0 && offset < MDB_MFILE_SIZE)
    {
        RWLOCK_RDLOCK(db->dbsio[index].mutex);
        if(lseek(db->dbsio[index].fd, offset, SEEK_SET) == offset)
            n = read(db->dbsio[index].fd, data, ndata);
        else
        {
            FATAL_LOGGER(db->logger, "lseek to dbsio[%d/%d] offset:%lld failed, %s", index, db->state->last_id, LL(offset), strerror(errno));
        }
        RWLOCK_UNLOCK(db->dbsio[index].mutex);
    }
    return n;
}
/* write data */
int mdb_pwrite(MDB *db, int index, void *data, int ndata, off_t offset)
{
    int n = -1;

    if(db && index >= 0 && data && ndata > 0 && offset >= 0 && offset < MDB_MFILE_SIZE)
    {
        RWLOCK_WRLOCK(db->dbsio[index].mutex);
        if(lseek(db->dbsio[index].fd, offset, SEEK_SET) == offset)
            n = write(db->dbsio[index].fd, data, ndata);
        else
        {
            FATAL_LOGGER(db->logger, "lseek to dbsio[%d/%d] offset:%lld failed, %s", index, db->state->last_id, LL(offset), strerror(errno));
        }
        RWLOCK_UNLOCK(db->dbsio[index].mutex);
    }
    return n;
}
/* set block incre mode */
int mdb_set_block_incre_mode(MDB *db, int mode)
{
    if(db && db->state)
    {
        db->state->block_incre_mode = mode;
        return 0;
    }
    return -1;
}

//4096 8192 16654
int mdb_xblock_index(int size)
{
    int i = 0, n = (size/MDB_MBLOCK_BASE);

    if(n > 0)
    {
        while((n /= 2))++i;
        if(size > mdb_xblock_list[i].block_size) ++i;
        //if((size%MDB_MBLOCK_BASE) > 0)++i;
    }
    return i;
}

/* push mblock */
void mdb_push_mblock(MDB *db, char *mblock, int block_index)
{
    int x = 0;

    if(db && mblock && block_index >= 0 && block_index < MDB_XMBLOCKS_MAX)
    {
        RWLOCK_WRLOCK(db->mutex_mblock);
        if(db->xblocks[block_index].nmblocks < mdb_xblock_list[block_index].blocks_max)
        {
            x = db->xblocks[block_index].nmblocks++;
            db->xblocks[block_index].mblocks[x] = mblock;
        }
        else
        {
            //WARN_LOGGER(db->logger, "free-xblock[%d]{%d}->total:%d", block_index, mdb_xblock_list[block_index].block_size, db->xblocks[block_index].total);
            db->xx_total += (off_t)mdb_xblock_list[block_index].block_size;
            xmm_free(mblock, mdb_xblock_list[block_index].block_size);
            --(db->xblocks[block_index].total);
        }
        RWLOCK_UNLOCK(db->mutex_mblock);
    }
    return ;
}
/* db pop mblock */
char *mdb_pop_mblock(MDB *db, int block_index)
{
    char *mblock = NULL;
    int x = 0;

    if(db && block_index >= 0 && block_index < MDB_XMBLOCKS_MAX)
    {
        RWLOCK_WRLOCK(db->mutex_mblock);
        if(db->xblocks[block_index].nmblocks > 0)
        {
            x = --(db->xblocks[block_index].nmblocks);
            //WARN_LOGGER(db->logger, "pop_qmblock() block_index:%d nmblocks:%d", block_index, x);
            mblock = db->xblocks[block_index].mblocks[x];
            db->xblocks[block_index].mblocks[x] = NULL;
        }
        else
        {
            mblock = (char *)xmm_new(mdb_xblock_list[block_index].block_size);
            db->mm_total += (off_t)mdb_xblock_list[block_index].block_size;
            if((db->xblocks[block_index].total)++ > mdb_xblock_list[block_index].blocks_max)
            {
                WARN_LOGGER(db->logger, "new-xblock[%d]{%d}->total:%d", block_index, mdb_xblock_list[block_index].block_size, db->xblocks[block_index].total);
            }
        }
        RWLOCK_UNLOCK(db->mutex_mblock);
    }
    return mblock;
}

/* new memory */
char *mdb_new_data(MDB *db, size_t size)
{
    char *data = NULL;
    int x = 0 ;
    if(db)
    {
        if(size > MDB_MBLOCK_MAX)
        {
            data = (char *)xmm_new(size);
            //WARN_LOGGER(db->logger, "xmm_new(%lu)", size);
        }
        else 
        {
            x = mdb_xblock_index(size);
            data = mdb_pop_mblock(db, x);
            if(mdb_xblock_list[x].block_size < size) 
            {
                FATAL_LOGGER(db->logger, "mdb_pop_mblock() data:%p size:%lu index:%d", data, size, x);
                _exit(-1);
            }
        }
        /*
        if(db->mm_total > (off_t)1073741824)
        {
            WARN_LOGGER(db->logger, "xblock_max:%d mm:%lld xx:%lld 32:%d 16M:%d 8M:%d 4M:%d 2M:%d 1M:%d 512K:%d 256K:%d 128K:%d 64K:%d 32K:%d 16K:%d 8K:%d 4K:%d", db->block_max, LL(db->mm_total), LL(db->xx_total), db->xblocks[13].total, db->xblocks[12].total, db->xblocks[11].total, db->xblocks[10].total, db->xblocks[9].total, db->xblocks[8].total, db->xblocks[7].total, db->xblocks[6].total, db->xblocks[5].total, db->xblocks[4].total, db->xblocks[3].total, db->xblocks[2].total, db->xblocks[1].total, db->xblocks[0].total);
        }
        */
    }
    return data;
}

/* free data */
void mdb_free_data(MDB *db, char *data, size_t size)
{
    int x = 0;

    if(data && size > 0) 
    {
        if(size > MDB_MBLOCK_MAX) 
        {
            //WARN_LOGGER(db->logger, "xmm_free(%p,%lu)", data, size);
            xmm_free(data, size);
        }
        else 
        {
            x = mdb_xblock_index(size);
            if(mdb_xblock_list[x].block_size < size)
            {
                FATAL_LOGGER(db->logger, "mdb_push_mblock() data:%p size:%lu index:%d", data, size, x);
                _exit(-1);
            }
            mdb_push_mblock(db, data, x);
        }
    }
    return ;
}

#define MDB_BLOCKS_COUNT(xxlen) ((xxlen/MDB_BASE_SIZE)+((xxlen%MDB_BASE_SIZE) > 0))
/* push block */
int mdb_push_block(MDB *db, int index, int blockid, int block_size)
{
    XMLNK *links = NULL, *link = NULL, lnk = {0};
    int x = 0, ret = -1;

    if(db && blockid >= 0 && (x = MDB_BLOCKS_COUNT(block_size)) > 0 && db->status == 0
            && x < MDB_LNK_MAX && index >= 0 && index < MDB_MFILE_MAX)
    {
        RWLOCK_WRLOCK(db->mutex_lnk);
        if((links = (XMLNK *)(db->lnkio.map)))
        {
            if(links[x].count > 0)
            {
                if(db->dbsio[index].map)
                {
                    link = (XMLNK *)(((char *)db->dbsio[index].map)
                            +((off_t)blockid * (off_t)MDB_BASE_SIZE));
                    link->index = links[x].index;
                    link->blockid = links[x].blockid;
                }
                else
                {
                    lnk.index = links[x].index;
                    lnk.blockid = links[x].blockid;
                    //if(pwrite(db->dbsio[index].fd, &lnk, sizeof(XMLNK), (off_t)blockid*(off_t)MDB_BASE_SIZE) < 0)
                    if(mdb_pwrite(db, index, &lnk, sizeof(XMLNK), (off_t)blockid*(off_t)MDB_BASE_SIZE) < 0)
                    {
                        FATAL_LOGGER(db->logger, "added link blockid:%d to index[%d] failed, %s",
                                blockid, index, strerror(errno));
                        _exit(-1);
                    }
                }
            }
            links[x].index = index;
            links[x].blockid = blockid;
            ++(links[x].count);
            ret = 0;
        }
        RWLOCK_UNLOCK(db->mutex_lnk);
    }
    return ret;
}

/* pop block */
int mdb_pop_block(MDB *db, int blocks_count, XMLNK *lnk)
{
    int x = 0, index = -1, left = 0, ret = -1, mdb_id = -1, block_id = -1, block_size = 0;
    XMLNK *links = NULL, *plink = NULL, link = {0};
    char path[MDB_PATH_MAX];

    if(db && (x = blocks_count) > 0 && x < MDB_LNK_MAX && lnk)
    {
        RWLOCK_WRLOCK(db->mutex_lnk);
        if((links = (XMLNK *)(db->lnkio.map)) && links[x].count > 0 
                && (index = links[x].index) >= 0 && index < MDB_MFILE_MAX
                && db->dbsio[index].fd > 0) 
        {
            lnk->count = blocks_count;
            lnk->index = index;
            lnk->blockid = links[x].blockid;
            ret = 0;
            if(--(links[x].count) > 0)
            {
                if((db->dbsio[index].map))
                {
                    plink = (XMLNK *)((char *)(db->dbsio[index].map)
                            +(off_t)links[x].blockid * (off_t)MDB_BASE_SIZE);
                    links[x].index = plink->index;
                    links[x].blockid = plink->blockid;
                }
                else
                {
                    //if(pread(db->dbsio[index].fd, &link, sizeof(XMLNK), (off_t)links[x].blockid*(off_t)MDB_BASE_SIZE) >0)
                    if(mdb_pread(db, index, &link, sizeof(XMLNK), (off_t)(links[x].blockid)*(off_t)MDB_BASE_SIZE) >0)
                    {
                        links[x].index = link.index;
                        links[x].blockid = link.blockid;
                    }
                    else
                    {
                        FATAL_LOGGER(db->logger, "pop_block() index:%d block_size:%d failed, %s", index, block_size, strerror(errno));
                        _exit(-1);
                    }
                }
            }
        }
        else
        {
            x = db->state->last_id;
            left = db->dbsio[x].size - db->state->last_off;
            if(left < (MDB_BASE_SIZE * blocks_count))
            {
                mdb_id = x;
                block_id = db->state->last_off/MDB_BASE_SIZE;
                block_size = left;
                db->state->last_off = MDB_BASE_SIZE * blocks_count;
                if((x = ++(db->state->last_id)) < MDB_MFILE_MAX 
                        && sprintf(path, "%s/base/%d/%d.db", db->basedir, x/MDB_DIR_FILES, x)
                        && mdb_mkdir(path) == 0
                        && (db->dbsio[x].fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                        && ftruncate(db->dbsio[x].fd, MDB_MFILE_SIZE) == 0)
                {
                    RWLOCK_INIT(db->dbsio[x].mutex);
                    db->dbsio[x].end = db->dbsio[x].size = MDB_MFILE_SIZE;
                    MDB_CHECK_MMAP(db, x);
                    lnk->count = blocks_count;
                    lnk->index = x;
                    lnk->blockid = 0;
                    ret = 0;
                }
                else
                {
                    FATAL_LOGGER(db->logger, "truncate new file[%s] failed, %s",
                            path, strerror(errno));
                    _exit(-1);
                }
            }
            else
            {
                lnk->count = blocks_count;
                lnk->index = x;
                lnk->blockid = (db->state->last_off/MDB_BASE_SIZE);
                db->state->last_off += MDB_BASE_SIZE * blocks_count;
                ret = 0;
            }
        }
        RWLOCK_UNLOCK(db->mutex_lnk);
        if(block_id >= 0)
        {
            ACCESS_LOGGER(db->logger, "push_block() blockid:%d index:%d block_size:%d", block_id, mdb_id, block_size);
            mdb_push_block(db, mdb_id, block_id, block_size);

        }
    }
    return ret;
}

#define CHECK_MDBXMIO(xdb, rid)                                                               \
do                                                                                          \
{                                                                                           \
    if(rid > db->state->mdb_id_max) db->state->mdb_id_max = rid;                              \
    if(rid < MDB_MDBX_MAX && (off_t)(rid * sizeof(MDBX)) >= xdb->dbxio.end)                    \
    {                                                                                       \
        xdb->dbxio.old = xdb->dbxio.end;                                                    \
        xdb->dbxio.end = (off_t)(((off_t)rid/(off_t)MDB_MDBX_BASE)+1);                        \
        xdb->dbxio.end *= (off_t)((off_t)sizeof(MDBX) * (off_t)MDB_MDBX_BASE);                 \
        if(ftruncate(xdb->dbxio.fd, xdb->dbxio.end) != 0)                                   \
        {                                                                                   \
            FATAL_LOGGER(xdb->logger, "ftruncate dbxio to %lld failed, %s",                 \
                    LL(xdb->dbxio.end), strerror(errno));                                   \
            _exit(-1);                                                                      \
        }                                                                                   \
        else                                                                                \
        {                                                                                   \
            if(xdb->dbxio.map) memset((char *)(xdb->dbxio.map)+xdb->dbxio.old, 0,           \
                    xdb->dbxio.end - xdb->dbxio.old);                                       \
            WARN_LOGGER(xdb->logger, "ftruncate dbxio[%d] to %lld", rid,LL(xdb->dbxio.end));\
        }                                                                                   \
    }                                                                                       \
}while(0)

/* set tag */
int mdb_set_tag(MDB *db, int id, int tag)
{
    MDBX *dbx = NULL;
    int ret = -1;
    if(db && id >= 0 && id < MDB_MDBX_MAX 
            && db->status == 0 && (dbx = (MDBX *)(db->dbxio.map)))
    {
        RWLOCK_WRLOCK(db->mutex_dbx);
        CHECK_MDBXMIO(db, id);
        dbx[id].tag = tag;
        RWLOCK_UNLOCK(db->mutex_dbx);
        ret = 0;
    }
    return ret;
}

/* get tag */
int mdb_get_tag(MDB *db, int id, int *tag)
{
    MDBX *dbx = NULL;
    int ret = -1;

    if(db && id >= 0 && id <= db->state->mdb_id_max
            && db->status == 0 && (dbx = (MDBX *)(db->dbxio.map)))
    {
        RWLOCK_RDLOCK(db->mutex_dbx);
        if(tag) *tag = dbx[id].tag;
        RWLOCK_UNLOCK(db->mutex_dbx);
        ret = 0;
    }
    return ret;
}

/* get data id */
int mdb_data_id(MDB *db, char *key, int nkey)
{
    if(db && key && nkey > 0)
    {
        return mmtrie_xadd(MMTR(db->kmap), key, nkey);
    }
    return -1;
}

int mdb__set__data(MDB *db, int id, char *data, int ndata)
{
    int ret = -1, index = 0, blocks_count = 0;
    XMLNK lnk = {0}, old = {0};
    MDBX *dbx = NULL;

    if(db && id >= 0 && id < MDB_MDBX_MAX && data && ndata > 0 
            && db->status == 0 && (dbx = (MDBX *)(db->dbxio.map)))
    {
        RWLOCK_WRLOCK(db->mutex_dbx);
        CHECK_MDBXMIO(db, id);
        RWLOCK_UNLOCK(db->mutex_dbx);
        mdb_mutex_wrlock(db, id);
        if(dbx[id].block_size < ndata)
        {
            if(dbx[id].block_size > 0)
            {
                old.index = dbx[id].index;
                old.blockid = dbx[id].blockid;
                old.count = MDB_BLOCKS_COUNT(dbx[id].block_size);
                dbx[id].block_size = 0;
                dbx[id].blockid = 0;
                dbx[id].ndata = 0;
                dbx[id].tag = 0;
            }
            blocks_count = MDB_BLOCKS_COUNT(ndata);
            if(mdb_pop_block(db, blocks_count, &lnk) == 0)
            {
                dbx[id].index = lnk.index;
                dbx[id].blockid = lnk.blockid;
                dbx[id].block_size = blocks_count * MDB_BASE_SIZE;
                ACCESS_LOGGER(db->logger, "pop_block() dbxid:%d blockid:%d index:%d block_size:%d ndata:%d",id, lnk.blockid, lnk.index, dbx[id].block_size, ndata);
                if(ndata > dbx[id].block_size)
                {
                    FATAL_LOGGER(db->logger, "Invalid blockid:%d ndata:%d block_count:%d", lnk.blockid, ndata, blocks_count);
                    _exit(-1);
                }
            }
            else
            {
                FATAL_LOGGER(db->logger, "pop_block(%d) failed, %s", blocks_count, strerror(errno));
                _exit(-1);
            }
        }
        if(dbx[id].block_size >= ndata && (index = dbx[id].index) >= 0 && db->dbsio[index].fd > 0)
        {
            if(db->state->mode && dbx[id].blockid >= 0 && db->dbsio[index].map)
            {
                if(memcpy((char *)(db->dbsio[index].map)+(off_t)dbx[id].blockid * (off_t)MDB_BASE_SIZE, data, ndata))
                {
                    dbx[id].ndata = ndata;
                    ret = id;
                }
                else
                {
                    FATAL_LOGGER(db->logger, "update index[%d] dbx[%d] ndata:%d to block[%d] block_size:%d end:%lld failed, %s", index, id, ndata, dbx[id].blockid, dbx[id].block_size, LL(db->dbxio.end), strerror(errno));
                    dbx[id].ndata = 0;
                    _exit(-1);
                }
            }
            else
            {
                //if(pwrite(db->dbsio[index].fd,data,ndata, (off_t)(dbx[id].blockid)*(off_t)MDB_BASE_SIZE)>0)
                if(mdb_pwrite(db, index, data, ndata, (off_t)(dbx[id].blockid)*(off_t)MDB_BASE_SIZE)>0)
                {
                    dbx[id].ndata = ndata;
                    ret = id;
                }
                else 
                {
                    FATAL_LOGGER(db->logger, "set fd:%d dbx[%d/%d] dbsio[%d/%d] ndata:%d to block[%d] block_size:%d end:%lld failed, %s", db->dbsio[index].fd, id, db->state->mdb_id_max, index, db->state->last_id, ndata, dbx[id].blockid, dbx[id].block_size, LL(db->dbsio[index].end), strerror(errno));
                    dbx[id].ndata = 0;
                    _exit(-1);
                }
            }
        }
        else
        {
            FATAL_LOGGER(db->logger, "Invalid index[%d] dbx[%d] ndata:%d to block[%d] block_size:%d off:%lld failed, %s", index, id, ndata, dbx[id].blockid, dbx[id].block_size, LL(db->dbxio.end), strerror(errno));
            _exit(-1);
        }
        if(dbx[id].ndata > db->state->data_len_max) db->state->data_len_max = dbx[id].ndata;
        dbx[id].mod_time = (int)time(NULL);
        mdb_mutex_unlock(db, id);
        if(old.count > 0)
        {
            ACCESS_LOGGER(db->logger, "push_block() dbxid:%d blockid:%d index:%d block_size:%d",id, old.blockid, old.index, old.count * MDB_BASE_SIZE);
            mdb_push_block(db, old.index, old.blockid, old.count * MDB_BASE_SIZE);
        }
    }
    return ret;
}

/* db xchunk data */
int mdb_xchunk_data(MDB *db, char *key, int nkey, char *data, int ndata, int length)
{
    int id = -1, ret = -1;

    if(db && key && nkey > 0 && data && ndata > 0)
    {
        if((id = mmtrie_xadd(MMTR(db->kmap), key, nkey)) > 0)
        {
            if(length > 0) mdb__resize(db, id, length);
            ret = mdb__set__data(db, id, data, ndata);
        }
    }
    return ret;
}

/* db set data */
int mdb_xset_data(MDB *db, char *key, int nkey, char *data, int ndata)
{
    int id = -1, ret = -1;

    if(db && key && nkey > 0 && data && ndata > 0)
    {
        if((id = mmtrie_xadd(MMTR(db->kmap), key, nkey)) > 0)
        {
            ret = mdb__set__data(db, id, data, ndata);
        }
    }
    return ret;
}

/* db chunk data */
int mdb_chunk_data(MDB *db, int id, char *data, int ndata, int length)
{
    int ret = -1;

    if(db && id >= 0 && data && ndata > 0)
    {
        if(length > 0) mdb__resize(db, id, length);
        ret = mdb__set__data(db, id, data, ndata);
    }
    return ret;
}

/* db set data */
int mdb_set_data(MDB *db, int id, char *data, int ndata)
{
    int ret = -1;

    if(db && id >= 0 && data && ndata > 0)
    {
        ret = mdb__set__data(db, id, data, ndata);
        //WARN_LOGGER(db->logger, "id:%d ndata:%d ret:%d", id, ndata, ret);
    }
    return ret;
}

/* db set modtime */
int mdb_update_modtime(MDB *db, int id)
{
    MDBX *dbx = NULL;
    int ret = -1;

    if(db && id >= 0 && id <= db->state->mdb_id_max)
    {
        if((dbx = (MDBX *)(db->dbxio.map)))
        {
            dbx[id].mod_time = (int)time(NULL);
        }
    }
    return ret;
}

/* db get modtime */
time_t mdb_get_modtime(MDB *db, int id)
{
    MDBX *dbx = NULL;
    time_t ret = 0;

    if(db && id >= 0 && id <= db->state->mdb_id_max)
    {
        if((dbx = (MDBX *)(db->dbxio.map)))
        {
            ret = (time_t)dbx[id].mod_time;
        }
    }
    return ret;
}

/* add data */
int mdb__add__data(MDB *db, int id, char *data, int ndata)
{
    int ret = -1, size = 0,  new_size = 0, blocks_count = 0, 
        oindex = 0, index = 0, nold = 0;
    char *block = NULL, *old = NULL, *mold = NULL;
    XMLNK lnk = {0}, old_lnk = {0};
    MDBX *dbx = NULL;

    if(db && id >= 0 && id < MDB_MDBX_MAX && data 
            && ndata > 0 && (dbx = (MDBX *)(db->dbxio.map)))
    {
        RWLOCK_WRLOCK(db->mutex_dbx);
        CHECK_MDBXMIO(db, id);
        RWLOCK_UNLOCK(db->mutex_dbx);
        mdb_mutex_wrlock(db, id);
        nold = dbx[id].ndata;
        //check block_size 
        if((size = (dbx[id].ndata + ndata)) > dbx[id].block_size)
        {
            old_lnk.index = dbx[id].index;
            old_lnk.blockid = dbx[id].blockid;
            old_lnk.count = MDB_BLOCKS_COUNT(dbx[id].block_size);
            if((new_size = dbx[id].block_size) > 0 
                    && db->state->block_incre_mode == MDB_BLOCK_INCRE_DOUBLE)
            {
                while(size > new_size) new_size *= 2;
                size = new_size;
            }
            blocks_count = MDB_BLOCKS_COUNT(size);
            if(mdb_pop_block(db, blocks_count, &lnk) != 0)
            {
                FATAL_LOGGER(db->logger, "pop_block(%d) failed, %s", blocks_count, strerror(errno));
                _exit(-1);
            }
            if(dbx[id].block_size > 0 && dbx[id].ndata > 0 && (oindex = old_lnk.index) >= 0 
                && (index = lnk.index) >= 0 && db->dbsio[oindex].fd > 0 && db->dbsio[index].fd > 0)
            {
                if(db->state->mode)
                {
                    if(db->dbsio[oindex].map && db->dbsio[index].map)
                    {
                        mold = (char *)(db->dbsio[oindex].map) +(off_t)old_lnk.blockid*(off_t)MDB_BASE_SIZE;
                        block = (char *)(db->dbsio[index].map)+lnk.blockid *(off_t)MDB_BASE_SIZE;
                        memcpy(block, mold, nold);
                    }
                    else
                    {
                            FATAL_LOGGER(db->logger, "moving index[%d] dbsio[%d]->ndata:%d data from index[%d]->map:%p to index[%d]->map:%p failed, %s", index, id, nold, oindex, db->dbsio[oindex].map, index, db->dbsio[index].map, strerror(errno));
                            _exit(-1);
                    }
                }
                else
                {
                    if((old = mdb_new_data(db, nold)))
                    {
                        if(mdb_pread(db, oindex, old, nold, (off_t)(old_lnk.blockid)*(off_t)MDB_BASE_SIZE) <= 0)
                        {
                            FATAL_LOGGER(db->logger, "read index[%d] dbx[%d] nold:%d data failed, %s", index, id, nold, strerror(errno));
                            _exit(-1);
                        }
                        if(mdb_pwrite(db, index, old, nold, (off_t)(lnk.blockid)*(off_t)MDB_BASE_SIZE) <= 0)
                        {
                            FATAL_LOGGER(db->logger, "write fd:%d dbx[%d/%d] dbsio[%d/%d] nold:%d to block[%d] block_size:%d end:%lld failed, %s", db->dbsio[index].fd, id, db->state->mdb_id_max, index, db->state->last_id, nold, dbx[id].blockid, dbx[id].block_size, LL(db->dbsio[index].end), strerror(errno));
                            _exit(-1);
                        }
                        mdb_free_data(db, old, nold);
                        old = NULL;
                    }
                }

            }
            dbx[id].index = lnk.index;
            dbx[id].blockid = lnk.blockid;
            dbx[id].block_size = lnk.count * MDB_BASE_SIZE;
            ACCESS_LOGGER(db->logger, "pop_block() dbxid:%d blockid:%d index:%d block_count:%d block_size:%d size:%d",id, lnk.blockid, lnk.index, blocks_count, dbx[id].block_size, size);
        }
        //write data
        if((index = dbx[id].index) >= 0 && db->dbsio[index].fd > 0)
        {
            //write data
            if(db->state->mode && db->dbsio[index].map)
            {
                block = (char *)(db->dbsio[index].map)+(off_t)dbx[id].blockid *(off_t)MDB_BASE_SIZE;
                memcpy(block+dbx[id].ndata, data, ndata);
            }
            else
            {
                //if(pwrite(db->dbsio[index].fd, data, ndata, (off_t)(dbx[id].blockid)*(off_t)MDB_BASE_SIZE+(off_t)(dbx[id].ndata)) <= 0)
                if(mdb_pwrite(db, index, data, ndata, (off_t)(dbx[id].blockid)*(off_t)MDB_BASE_SIZE+(off_t)(dbx[id].ndata)) <= 0)
                {
                    FATAL_LOGGER(db->logger, "write fd:%d dbx[%d/%d] dbsio[%d/%d] ndata:%d to block[%d] block_size:%d end:%lld failed, %s", db->dbsio[index].fd, id, db->state->mdb_id_max, index, db->state->last_id, ndata, dbx[id].blockid, dbx[id].block_size, LL(db->dbsio[index].end), strerror(errno));
                    _exit(-1);
                }

            }
            dbx[id].ndata += ndata;
            ret = id;
        }
        if(dbx[id].ndata > db->state->data_len_max) db->state->data_len_max = dbx[id].ndata;
        dbx[id].mod_time = (int)time(NULL);
        mdb_mutex_unlock(db, id);
        if(old_lnk.count > 0)
        {

            ACCESS_LOGGER(db->logger, "push_block() blockid:%d index:%d block_size:%d", old_lnk.blockid, old_lnk.index, old_lnk.count * MDB_BASE_SIZE);
            mdb_push_block(db, old_lnk.index, old_lnk.blockid, old_lnk.count * MDB_BASE_SIZE);
        }
    }
    return ret;
}

/* resize */
int mdb__resize(MDB *db, int id, int length)
{
    int ret = -1, size = 0,  new_size = 0, blocks_count = 0, x = 0, index = 0, nold = 0;
    char *block = NULL, *old = NULL, *mold = NULL;
    XMLNK lnk = {0}, old_lnk = {0};
    MDBX *dbx = NULL;

    if(db && id >= 0 && length > 0 && (dbx = (MDBX *)(db->dbxio.map)))
    {
        RWLOCK_WRLOCK(db->mutex_dbx);
        CHECK_MDBXMIO(db, id);
        RWLOCK_UNLOCK(db->mutex_dbx);
        mdb_mutex_wrlock(db, id);
        nold = dbx[id].ndata;
        //check block_size 
        if((size = length) > dbx[id].block_size)
        {
            old_lnk.index = dbx[id].index;
            old_lnk.blockid = dbx[id].blockid;
            old_lnk.count = MDB_BLOCKS_COUNT(dbx[id].block_size);
            if((new_size = dbx[id].block_size) > 0 
                    && db->state->block_incre_mode == MDB_BLOCK_INCRE_DOUBLE)
            {
                while(size > new_size) new_size *= 2;
                size = new_size;
            }
            blocks_count = MDB_BLOCKS_COUNT(size);
            if(mdb_pop_block(db, blocks_count, &lnk) != 0)
            {
                FATAL_LOGGER(db->logger, "pop_block(%d) failed, %s", blocks_count, strerror(errno));
                _exit(-1);
            }
            if(dbx[id].block_size > 0 && dbx[id].ndata > 0 && (x = old_lnk.index) >= 0 
                    && (index = lnk.index) >= 0 && db->dbsio[x].fd > 0 && db->dbsio[index].fd > 0)
            {
                if(db->state->mode)
                {
                    if(db->dbsio[x].map && db->dbsio[index].map)
                    {
                        mold = (char *)(db->dbsio[x].map) +(off_t)old_lnk.blockid*(off_t)MDB_BASE_SIZE;
                        block = (char *)(db->dbsio[index].map)+lnk.blockid *(off_t)MDB_BASE_SIZE;
                        memcpy(block, mold, nold);
                    }
                    else
                    {
                            FATAL_LOGGER(db->logger, "moving index[%d] dbsio[%d]->ndata:%d data from index[%d]->map:%p to index[%d]->map:%p failed, %s", index, id, nold, x, db->dbsio[x].map, index, db->dbsio[index].map, strerror(errno));
                            _exit(-1);
                    }
                }
                else
                {
                    if((old = mdb_new_data(db, nold)))
                    {
                        if(mdb_pread(db, index, old, nold, (off_t)(old_lnk.blockid)*(off_t)MDB_BASE_SIZE) <= 0)
                        {
                            FATAL_LOGGER(db->logger, "read index[%d] dbx[%d] nold:%d data failed, %s", index, id, nold, strerror(errno));
                            _exit(-1);
                        }
                        if(mdb_pwrite(db, index, old, nold, (off_t)(lnk.blockid)*(off_t)MDB_BASE_SIZE) <= 0)
                        {
                            FATAL_LOGGER(db->logger, "write fd:%d dbx[%d/%d] dbsio[%d/%d] nold:%d to block[%d] block_size:%d end:%lld failed, %s", db->dbsio[index].fd, id, db->state->mdb_id_max, index, db->state->last_id, nold, dbx[id].blockid, dbx[id].block_size, LL(db->dbsio[index].end), strerror(errno));
                            _exit(-1);
                        }
                        mdb_free_data(db, old, nold);
                        old = NULL;
                    }
                }

            }
            dbx[id].index = lnk.index;
            dbx[id].blockid = lnk.blockid;
            dbx[id].block_size = lnk.count * MDB_BASE_SIZE;
            ACCESS_LOGGER(db->logger, "pop_block() dbxid:%d blockid:%d index:%d block_count:%d block_size:%d size:%d",id, lnk.blockid, lnk.index, blocks_count, dbx[id].block_size, size);
        }
        dbx[id].mod_time = (int)time(NULL);
        mdb_mutex_unlock(db, id);
        if(old_lnk.count > 0)
        {

            ACCESS_LOGGER(db->logger, "push_block() blockid:%d index:%d block_size:%d", old_lnk.blockid, old_lnk.index, old_lnk.count * MDB_BASE_SIZE);
            mdb_push_block(db, old_lnk.index, old_lnk.blockid, old_lnk.count * MDB_BASE_SIZE);
        }
    }
    return ret;
}

/* xadd data */
int mdb_xresize(MDB *db, char *key, int nkey, int length)
{
    int id = -1, ret = -1;

    if(db && key && nkey > 0 && length > 0)
    {
        if((id = mmtrie_xadd(MMTR(db->kmap), key, nkey)) > 0)
        {
            ret = mdb__resize(db, id, length);
        }
    }
    return ret;
}

/* xadd data */
int mdb_xadd_data(MDB *db, char *key, int nkey, char *data, int ndata)
{
    int id = -1, ret = -1;

    if(db && key && nkey > 0 && data && ndata > 0)
    {
        if((id = mmtrie_xadd(MMTR(db->kmap), key, nkey)) > 0)
        {
            ret = mdb__add__data(db, id, data, ndata);
        }
    }
    return ret;
}

/* db add data */
int mdb_add_data(MDB *db, int id, char *data, int ndata)
{
    int ret = -1;

    if(db && id >= 0 && data && ndata > 0)
    {
        ret = mdb__add__data(db, id, data, ndata);
    }
    return ret;
}


/* xget data  len*/
int mdb_xget_data_len(MDB *db, char *key, int nkey)
{
    int id = -1, ret = -1;
    MDBX *dbx = NULL;

    if(db && key)
    {
        if((id = mmtrie_get(MMTR(db->kmap), key, nkey)) > 0 
                && (dbx = (MDBX *)db->dbxio.map)) 
        {
            ret = dbx[id].ndata; 
        }
    }
    return ret;
}

/* truncate block */
void *mdb_truncate_block(MDB *db, int id, int ndata)
{
    int index = 0, blocks_count = 0;
    XMLNK lnk = {0}, old = {0};
    MDBX *dbx = NULL;
    void *ret = NULL;

    if(db && id >= 0 && ndata > 0 && db->status == 0 && (dbx = (MDBX *)(db->dbxio.map)))
    {
        RWLOCK_WRLOCK(db->mutex_dbx);
        CHECK_MDBXMIO(db, id);
        RWLOCK_WRLOCK(db->mutex_dbx);
        mdb_mutex_rdlock(db, id);
        if(dbx[id].block_size < ndata)
        {
            if(dbx[id].block_size > 0)
            {
                old.index = dbx[id].index;
                old.blockid = dbx[id].blockid;
                old.count = MDB_BLOCKS_COUNT(dbx[id].block_size);
                dbx[id].block_size = 0;
                dbx[id].blockid = 0;
                dbx[id].ndata = 0;
                dbx[id].tag = 0;
            }
            blocks_count = MDB_BLOCKS_COUNT(ndata);
            if(mdb_pop_block(db, blocks_count, &lnk) == 0)
            {
                dbx[id].index = lnk.index;
                dbx[id].blockid = lnk.blockid;
                dbx[id].block_size = blocks_count * MDB_BASE_SIZE;
                ACCESS_LOGGER(db->logger, "pop_block() dbxid:%d blockid:%d index:%d block_size:%d ndata:%d",id, lnk.blockid, lnk.index, dbx[id].block_size, ndata);
                if(ndata > dbx[id].block_size)
                {
                    FATAL_LOGGER(db->logger, "Invalid blockid:%d ndata:%d block_count:%d", lnk.blockid, ndata, blocks_count);
                    _exit(-1);
                }
            }
            else
            {
                FATAL_LOGGER(db->logger, "pop_block(%d) failed, %s", blocks_count, strerror(errno));
                _exit(-1);
            }
        }
        if(dbx[id].block_size >= ndata && (index = dbx[id].index) >= 0 && db->dbsio[index].fd > 0)
        {
            if(db->state->mode && dbx[id].blockid >= 0 && db->dbsio[index].map)
            {
                ret = (char *)(db->dbsio[index].map)+(off_t)dbx[id].blockid * (off_t)MDB_BASE_SIZE;
                dbx[id].ndata = ndata;
            }
            else
            {
                ret = NULL;
            }
        }
        else
        {
            FATAL_LOGGER(db->logger, "Invalid index[%d] dbx[%d] ndata:%d to block[%d] block_size:%d off:%lld failed, %s", index, id, ndata, dbx[id].blockid, dbx[id].block_size, LL(db->dbxio.end), strerror(errno));
            _exit(-1);
        }
        if(dbx[id].ndata > db->state->data_len_max) db->state->data_len_max = dbx[id].ndata;
        dbx[id].mod_time = (int)time(NULL);
        mdb_mutex_unlock(db, id);
        if(old.count > 0)
        {
            ACCESS_LOGGER(db->logger, "push_block() dbxid:%d blockid:%d index:%d block_size:%d",id, old.blockid, old.index, old.count * MDB_BASE_SIZE);
            mdb_push_block(db, old.index, old.blockid, old.count * MDB_BASE_SIZE);
        }
    }
    return ret;
}

/* get data block address and len */
int mdb_exists_block(MDB *db, int id, char **ptr)
{
    int n = -1, index = -1;
    MDBX *dbx = NULL;

    if(db && id > 0 && ptr && db->state && (db->state->mode & MDB_USE_MMAP)
            && (dbx = (MDBX *)(db->dbxio.map)) && (index = dbx[id].index) >= 0
            && dbx[id].ndata > 0 && db->dbsio[index].map)
    {
        *ptr = db->dbsio[index].map+(off_t)dbx[id].blockid*(off_t)MDB_BASE_SIZE;
        n = dbx[id].ndata;
    }
    return n;
}

/* xcheck dataid/len */
int mdb_xcheck(MDB *db, char *key, int nkey, int *len, time_t *mod_time)
{
    MDBX *dbx = NULL;
    int id = -1;

    if(db && key && nkey > 0)
    {
        if((id = mmtrie_xadd(MMTR(db->kmap), key, nkey)) > 0 && id <= db->state->mdb_id_max) 
        {
            if((dbx = (MDBX *)(db->dbxio.map))) 
            {
                if(len) *len = dbx[id].ndata; 
                if(mod_time) *mod_time = dbx[id].mod_time;
            }
        }

    }
    return id;
}
int mdb__read__data(MDB *db, int id, char *data)
{
    int ret = -1, n = -1, index = 0;
    MDBX *dbx = NULL;

    if(db && id >= 0 && data && id <= db->state->mdb_id_max && (dbx = (MDBX *)(db->dbxio.map)))
    {
        if(dbx[id].blockid >= 0 && (n = dbx[id].ndata) > 0) 
        {
            if((index = dbx[id].index) >= 0 && db->dbsio[index].fd > 0)
            {
                if(db->state->mode && db->dbsio[index].map && dbx[id].ndata > 0)
                {
                    if(memcpy(data, (char *)(db->dbsio[index].map) + (off_t)dbx[id].blockid 
                                *(off_t)MDB_BASE_SIZE, n) > 0)
                        ret = n;
                }
                else
                {
                    //if(pread(db->dbsio[index].fd, data, n, (off_t)dbx[id].blockid*(off_t)MDB_BASE_SIZE)> 0)
                    if(mdb_pread(db, index, data, n, (off_t)(dbx[id].blockid)*(off_t)MDB_BASE_SIZE)> 0)
                        ret = n;
                }
            }
        }
    }
    return ret;
}


/* xget data */
int mdb_xget_data(MDB *db, char *key, int nkey, char **data, int *ndata)
{
    int id = -1, n = 0;
    MDBX *dbx = NULL;

    if(db && key && data && ndata)
    {
        *ndata = 0;
        if((id = mmtrie_get(MMTR(db->kmap), key, nkey)) > 0)
        {
            mdb_mutex_rdlock(db, id);
            if((dbx = (MDBX *)db->dbxio.map) && (n = dbx[id].ndata) > 0
                && dbx[id].block_size > 0 && (*data = (mdb_new_data(db, n)))) 
            {
                *ndata = mdb__read__data(db, id, *data);
            }
            mdb_mutex_unlock(db, id);
        }
        else
        {
            id = mmtrie_xadd(MMTR(db->kmap), key, nkey);
        }
    }
    return id;
}

/* get data len*/
int mdb_get_data_len(MDB *db, int id)
{
    MDBX *dbx = NULL;
    int ret = -1;

    if(db && id >= 0 && id <= db->state->mdb_id_max)
    {
        if((dbx = (MDBX *)(db->dbxio.map)))
        {
            ret = dbx[id].ndata;
        }
    }
    return ret;
}

/* get data */
int mdb_get_data(MDB *db, int id, char **data)
{
    int ret = -1, n = 0;
    MDBX *dbx = NULL;

    if(db && id >= 0 && id <= db->state->mdb_id_max)
    {
        mdb_mutex_rdlock(db, id);
        if((dbx = (MDBX *)(db->dbxio.map)) && (n = dbx[id].ndata) > 0 
                && dbx[id].block_size > 0 && (*data = mdb_new_data(db, n)))
        {
            if((ret = mdb__read__data(db, id, *data)) < 0)
            {
                mdb_free_data(db, *data, n);*data = NULL;
            }
        }
        mdb_mutex_unlock(db, id);
    }
    return ret;
}

/* xread data */
int mdb_xread_data(MDB *db, char *key, int nkey, char *data)
{
    int ret = -1, id = -1;

    if(db && key && nkey > 0 && data)
    {
        if((id = mmtrie_get(MMTR(db->kmap), key, nkey)) > 0) 
        {
            mdb_mutex_rdlock(db, id);
            ret = mdb__read__data(db, id, data);
            mdb_mutex_unlock(db, id);
        }
    }
    return ret;
}

/* pread data */
int mdb__pread__data(MDB *db, int id, char *data, int len, int off)
{
    int ret = -1, index = 0, n = -1;
    MDBX *dbx = NULL;

    if(db && id >= 0 && data && id <= db->state->mdb_id_max)
    {
        if((dbx = (MDBX *)(db->dbxio.map)) && dbx[id].blockid >= 0 
            && (n = dbx[id].ndata) > 0 && off < n 
            && (index = dbx[id].index) >= 0 && db->dbsio[index].fd > 0)
        {
            n -= off;
            if(len < n) n = len;
            if(db->state->mode && db->dbsio[index].map)
            {
                if(memcpy(data, (char *)(db->dbsio[index].map) + (off_t)dbx[id].blockid
                            *(off_t)MDB_BASE_SIZE + off, n) > 0)
                    ret = n;
            }
            else
            {
                //if(pread(db->dbsio[index].fd, data, n, (off_t)dbx[id].blockid *(off_t)MDB_BASE_SIZE+off) > 0)
                if(mdb_pread(db, index, data, n, (off_t)(dbx[id].blockid)*(off_t)MDB_BASE_SIZE+(off_t)off) > 0)
                    ret = n;

            }
        }
    }
    return ret;
}

/* xpread data */
int mdb_xpread_data(MDB *db, char *key, int nkey, char *data, int len, int off)
{
    int ret = -1, id = -1;

    if(db && key && nkey > 0 && data)
    {
        if((id = mmtrie_get(MMTR(db->kmap), key, nkey)) > 0)
        {
            mdb_mutex_rdlock(db, id);
            ret = mdb__pread__data(db, id, data, len, off);
            mdb_mutex_unlock(db, id);
        }
    }
    return ret;
}

/* read data */
int mdb_read_data(MDB *db, int id, char *data)
{
    int ret = -1;

    if(db && id >= 0 && data && id <= db->state->mdb_id_max)
    {
        mdb_mutex_rdlock(db, id);
        ret = mdb__read__data(db, id, data);
        mdb_mutex_unlock(db, id);
    }
    return ret;
}

/* pread data */
int mdb_pread_data(MDB *db, int id, char *data, int len, int off)
{
    int ret = -1;

    if(db && id >= 0 && data && id <= db->state->mdb_id_max)
    {
        mdb_mutex_rdlock(db, id);
        ret = mdb__pread__data(db, id, data, len, off);
        mdb_mutex_unlock(db, id);
    }
    return ret;
}


/* delete data */
int mdb_del_data(MDB *db, int id)
{
    MDBX *dbx = NULL;
    int ret = -1;

    if(db && id >= 0 && id <= db->state->mdb_id_max)
    {
        if((dbx = (MDBX *)(db->dbxio.map)))
        {
            ACCESS_LOGGER(db->logger, "push_block() dbxid:%d blockid:%d index:%d block_size:%d",id, dbx[id].blockid, dbx[id].index, dbx[id].block_size);
            if(dbx[id].block_size > 0)
            {
                mdb_push_block(db, dbx[id].index, dbx[id].blockid, dbx[id].block_size);
                mdb_mutex_wrlock(db, id);
                dbx[id].block_size = 0;
                dbx[id].blockid = 0;
                dbx[id].ndata = 0;
                dbx[id].tag = 0;
                mdb_mutex_unlock(db, id);
            }
            dbx[id].mod_time = (int)time(NULL);
            ret = id;
        }
    }
    return ret;
}

/* delete data */
int mdb_xdel_data(MDB *db, char *key, int nkey)
{
    int id = -1, ret = -1;
    MDBX *dbx = NULL;

    if(db && key && nkey > 0)
    {
        if((id = mmtrie_get(MMTR(db->kmap), key, nkey)) >= 0
                && (dbx = (MDBX *)(db->dbxio.map)))
        {
            ACCESS_LOGGER(db->logger, "push_block() dbxid:%d blockid:%d index:%d block_size:%d",id, dbx[id].blockid, dbx[id].index, dbx[id].block_size);
            if(dbx[id].block_size > 0)
            {
                mdb_push_block(db, dbx[id].index, dbx[id].blockid, dbx[id].block_size);
                mdb_mutex_wrlock(db, id);
                dbx[id].block_size = 0;
                dbx[id].blockid = 0;
                dbx[id].ndata = 0;
                dbx[id].tag = 0;
                mdb_mutex_unlock(db, id);
            }
            dbx[id].mod_time = (int)time(NULL);
            ret = id;
        }
    }
    return ret;
}

/* reset */
void mdb_reset(MDB *db)
{
    int ret = 0, i = 0, mode = 0;
    char path[MDB_PATH_MAX];

    if(db)
    {
        db->status = -1;
        RWLOCK_WRLOCK(db->mutex);
        RWLOCK_WRLOCK(db->mutex_dbx);
        RWLOCK_WRLOCK(db->mutex_lnk);
        RWLOCK_WRLOCK(db->mutex_mblock);
        mmtrie_destroy(db->kmap);
        /* dbx */
        if(db->dbxio.map) 
        {
            munmap(db->dbxio.map, db->dbxio.size);
            db->dbxio.map = NULL;
            db->dbxio.end = 0;
            db->dbxio.size = 0;
        }
        if(db->dbxio.fd > 0)
        {
            db->dbxio.end = sizeof(MDBX) * MDB_MDBX_BASE;
            db->dbxio.size = sizeof(MDBX) * MDB_MDBX_MAX;
            ret = ftruncate(db->dbxio.fd, db->dbxio.end);
            if((db->dbxio.map = (char *)mmap(NULL, db->dbxio.size, PROT_READ|PROT_WRITE,
                MAP_SHARED, db->dbxio.fd, 0)) == NULL || db->dbxio.map == (void *)-1)
            {
                FATAL_LOGGER(db->logger,"mmap dbx failed, %s", strerror(errno));
                _exit(-1);
            }
            memset(db->dbxio.map, 0, db->dbxio.end);
        }
        /* dbs */
        for(i = 0; i <= db->state->last_id; i++)
        {
            if(db->dbsio[i].map) 
            {
                munmap(db->dbsio[i].map, db->dbsio[i].end);
                db->dbsio[i].map = NULL;
                db->dbsio[i].end = 0;
            }
            RWLOCK_DESTROY(db->dbsio[i].mutex);
            //db->dbsio[i].mutex = NULL;
            if(db->dbsio[i].fd > 0)
            {
                close(db->dbsio[i].fd);
                db->dbsio[i].fd = 0;
            }
            if(sprintf(path, "%s/base/%d/%d.db", db->basedir, i/MDB_DIR_FILES, i))
            {
                ret = remove(path);
                WARN_LOGGER(db->logger, "remove db[%d][%s] => %d", i, path, ret);
            }
        }
        /* link */
        if(db->lnkio.map)
            memset(db->lnkio.map, 0, db->lnkio.end);
        /* state */
        if(db->stateio.map)
        {
            mode = db->state->mode;
            memset(db->stateio.map, 0, db->stateio.end);
            db->state->mode = mode;
        }
        db->status = 0;
        db->state->last_id = 0;
        db->state->last_off = 0;
        db->state->mdb_id_max = 0;
        RWLOCK_UNLOCK(db->mutex);
        RWLOCK_UNLOCK(db->mutex_lnk);
        RWLOCK_UNLOCK(db->mutex_dbx);
        RWLOCK_UNLOCK(db->mutex_mblock);
    }
    return ;
}

/* destroy */
void mdb_destroy(MDB *db)
{
    int ret = 0, i = 0, mode = 0;
    char path[MDB_PATH_MAX];

    if(db)
    {
        db->status = -1;
        RWLOCK_WRLOCK(db->mutex);
        RWLOCK_WRLOCK(db->mutex_dbx);
        RWLOCK_WRLOCK(db->mutex_lnk);
        RWLOCK_WRLOCK(db->mutex_mblock);
        mmtrie_destroy(db->kmap);
        /* dbx */
        if(db->dbxio.map) 
        {
            munmap(db->dbxio.map, db->dbxio.size);
            db->dbxio.map = NULL;
            db->dbxio.end = 0;
            db->dbxio.size = 0;
        }
        if(db->dbxio.fd > 0)
        {
            db->dbxio.end = sizeof(MDBX) * MDB_MDBX_BASE;
            db->dbxio.size = sizeof(MDBX) * MDB_MDBX_MAX;
            ret = ftruncate(db->dbxio.fd, db->dbxio.end);
            if((db->dbxio.map = (char *)mmap(NULL, db->dbxio.size, PROT_READ|PROT_WRITE,
                MAP_SHARED, db->dbxio.fd, 0)) == NULL || db->dbxio.map == (void *)-1)
            {
                FATAL_LOGGER(db->logger,"mmap dbx failed, %s", strerror(errno));
                _exit(-1);
            }
            memset(db->dbxio.map, 0, db->dbxio.end);
        }
        /* dbs */
        for(i = 0; i <= db->state->last_id; i++)
        {
            if(db->dbsio[i].map) 
            {
                munmap(db->dbsio[i].map, db->dbsio[i].end);
                db->dbsio[i].map = NULL;
                db->dbsio[i].end = 0;
            }
            RWLOCK_DESTROY(db->dbsio[i].mutex);
            if(db->dbsio[i].fd > 0)
            {
                close(db->dbsio[i].fd);
                db->dbsio[i].fd = 0;
            }
            if(sprintf(path, "%s/base/%d/%d.db", db->basedir, i/MDB_DIR_FILES, i))
            {
                ret = remove(path);
                WARN_LOGGER(db->logger, "remove db[%d][%s] => %d", i, path, ret);
            }
        }
        /* link */
        if(db->lnkio.map)
            memset(db->lnkio.map, 0, db->lnkio.end);
        /* state */
        if(db->stateio.map)
        {
            mode = db->state->mode;
            memset(db->stateio.map, 0, db->stateio.end);
            db->state->mode = mode;
        }
        /* open dbs */
        sprintf(path, "%s/base/0/0.db", db->basedir);    
        if((db->dbsio[0].fd = open(path, O_CREAT|O_RDWR, 0644)) > 0)
        {
            RWLOCK_INIT(db->dbsio[0].mutex);
            db->dbsio[0].size = MDB_MFILE_SIZE;
            if(ftruncate(db->dbsio[0].fd, db->dbsio[0].size) != 0)
            {
                FATAL_LOGGER(db->logger, "ftruncate db:%s failed, %s", path, strerror(errno));
                _exit(-1);
            }
            MDB_CHECK_MMAP(db, 0);
            if(db->dbsio[0].map) memset(db->dbsio[0].map, 0, MDB_MFILE_SIZE);
        }
        else
        {
            FATAL_LOGGER(db->logger, "open db file:%s failed, %s", path, strerror(errno));
            _exit(-1);
        }
        db->status = 0;
        db->state->last_id = 0;
        db->state->last_off = 0;
        db->state->mdb_id_max = 0;
        RWLOCK_UNLOCK(db->mutex);
        RWLOCK_UNLOCK(db->mutex_lnk);
        RWLOCK_UNLOCK(db->mutex_dbx);
        RWLOCK_UNLOCK(db->mutex_mblock);
    }
    return ;
}

/* clean */
void mdb_clean(MDB *db)
{
    int i = 0, j = 0;
    if(db)
    {
        for(i = 0; i <= db->state->last_id; i++)
        {
            if(db->dbsio[i].map)munmap(db->dbsio[i].map, db->dbsio[i].end);
            if(db->dbsio[i].fd)close(db->dbsio[i].fd);
            RWLOCK_DESTROY(db->dbsio[i].mutex);
        }
        if(db->dbxio.map)munmap(db->dbxio.map, db->dbxio.size);
        if(db->dbxio.fd)close(db->dbxio.fd);
        if(db->lnkio.map)munmap(db->lnkio.map, db->lnkio.end);
        if(db->lnkio.fd)close(db->lnkio.fd);
        if(db->stateio.map)munmap(db->stateio.map, db->stateio.end);
        if(db->stateio.fd)close(db->stateio.fd);
        for(i = 0; i < MDB_XMBLOCKS_MAX; i++)
        {
            for(j = 0; j < db->xblocks[i].nmblocks; j++)
            {
                if(db->xblocks[i].mblocks[j])
                {
                    xmm_free(db->xblocks[i].mblocks[j], MDB_MBLOCK_BASE * (i+1));
                }
            }
        }
        if(MMTR(db->kmap))mmtrie_clean(MMTR(db->kmap));
        for(i = 0; i < MDB_MUTEX_MAX; i++)
        {
            RWLOCK_DESTROY(db->mutexs[i]);
        }
        LOGGER_CLEAN(db->logger);
        RWLOCK_DESTROY(db->mutex_lnk);
        RWLOCK_DESTROY(db->mutex_dbx);
        RWLOCK_DESTROY(db->mutex_mblock);
        RWLOCK_DESTROY(db->mutex);
        xmm_free(db, sizeof(MDB));
    }
    return ;
}

#ifdef _DEBUG_MDB
int main(int argc, char **argv)
{
    char *dbdir = "/tmp/db";
    char *key = NULL, *data = NULL;
    int id = 0, n = 0;
    MDB *db = NULL;

    if((db = mdb_init(dbdir, 0)))
    {
        //fprintf(stdout, "mdb_xblock_index(4095):%d, mdb_xblock_index(4096):%d, mdb_xblock_index(10080):%d, mdb_xblock_index(10000000):%d \n", mdb_xblock_index(4095), mdb_xblock_index(4096), mdb_xblock_index(10080), mdb_xblock_index(10000000));
        //return -1;
        mdb_destroy(db);
        key = "xxxxx";
        data = "askfjsdlkfjsdlkfasdf";
        /* set data with key */
        if((id = mdb_xset_data(db, key, strlen(key), data, strlen(data))) > 0)
        {
            fprintf(stdout, "%s::%d xset_data(%s):%s => id:%d\n", __FILE__, __LINE__, key, data, id);
            if((id = mdb_xget_data(db, key, strlen(key), &data, &n)) > 0 && n > 0)
            {
                data[n] = '\0';
                fprintf(stdout, "%s::%d xget_data(%s):%s\n",  __FILE__, __LINE__, key, data);
                mdb_free_data(db, data, n);
            }
        }
        else
        {
            fprintf(stderr, "%s::%d xset_data(%s) failed, %s\n",
                    __FILE__, __LINE__, key, strerror(errno));
            _exit(-1);
        }
        /* del data with key */
        if(mdb_xdel_data(db, key, strlen(key)) > 0)
        {
            fprintf(stdout, "%s::%d xdel_data(%s):%s\n", __FILE__, __LINE__, key, data);
            if((id = mdb_xget_data(db, key, strlen(key), &data, &n)) > 0 && n > 0)
            {
                data[n] = '\0';
                fprintf(stdout, "%s::%d xget_data(%s):%s\n", __FILE__, __LINE__, key, data);
                mdb_free_data(db, data, n);
            }
            else
            {
                fprintf(stdout, "%s::%d xget_data(%s) null\n", __FILE__, __LINE__, key);
            }
        }
        data = "dsfklajslfkjdsl;fj;lsadfklweoirueowir";
        /* set data with id */
        if((mdb_set_data(db, id, data, strlen(data))) > 0)
        {
            fprintf(stdout, "%s::%d set_data(%d):%s\n", __FILE__, __LINE__, id, data);
            data = NULL;
            if((n = mdb_get_data(db, id, &data)) > 0)
            {
                data[n] = '\0';
                fprintf(stdout, "%s::%d get_data(%d):%s\n", __FILE__, __LINE__, id, data);
                mdb_free_data(db, data, n);
            }
        }
        /* delete data with id */
        if(mdb_del_data(db, id) > 0)
        {
            fprintf(stdout, "%s::%d del_data(%d):%s\n", __FILE__, __LINE__, id, data);
            data = NULL;
            if((n = mdb_get_data(db, id, &data)) > 0)
            {
                data[n] = '\0';
                fprintf(stdout, "%s::%d get_data(%d):%s\n", __FILE__, __LINE__, id, data);
                mdb_free_data(db, data, n);
            }
            else
            {
                fprintf(stdout, "%s::%d get_data(%d) null\n",  __FILE__, __LINE__, id);
            }
        }
        data = "sadfj;dslfm;';qkweprkpafk;ldsfma;ldskf;lsdf;sdfk;";
        /* reset data with id */
        if((mdb_set_data(db, id, data, strlen(data))) > 0)
        {
            fprintf(stdout, "%s::%d set_data(%d):%s\n",  __FILE__, __LINE__, id, data);
            data = NULL;
            if((n = mdb_get_data(db, id, &data)) > 0)
            {
                data[n] = '\0';
                fprintf(stdout, "%s::%d get_data(%d):%s\n", __FILE__, __LINE__, id, data);
                mdb_free_data(db, data, n);
            }
    }
        mdb_destroy(db);
        data = "saddfadfdfak;";
        mdb_xadd_data(db, "keyxxxx", 7, data, strlen(data));
        /* add data */
        if((id = mdb_xadd_data(db, "keyx", 4, "data:0\n", 7)) > 0)
        {
            char line[1024], key[256];
            int i = 0, j = 0;
            for(i = 1; i < 100; i++)
            {
                //sprintf(key, "key:%d\n", i);
                for(j = 0; j < 1000; j++)
                {
                    n = sprintf(line, "line:%d\n", j); 
                    mdb_add_data(db, i, line, n);
                }
            }
            //char *s = "dsfkhasklfjalksjfdlkasdjflsjdfljsadlf";
            //mdb_add_data(db, id, s, strlen(s));
            data = NULL;
            if((n = mdb_get_data(db, id, &data)) > 0)
            {
                data[n] = '\0';
                fprintf(stdout, "%s::%d get_data(%d):[%d]%s\n", __FILE__, __LINE__, id, n, data);
                //mdb_free_data(db, data);
            }
        }
        mdb_clean(db);
    }
    return 0;
}
//gcc -o db db.c mmtrie.c xmm.c -D_DEBUG_MDB
#endif
