#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include "idb.h"
#include "mutex.h"
#include "logger.h"
#include "zvbcode.h"
#define IDB_KEY_NAME     "db.kmap"
#define IDB_LINK_NAME    "db.link"
#define IDB_LOG_NAME     "db.log"
#define IDB_IDX_EXT      "idx"
#ifdef MAP_LOCKED
#define MMAP_SHARED MAP_SHARED|MAP_LOCKED
#else
#define MMAP_SHARED MAP_SHARED
#endif
/* mkdir */
int idb_mkdir(char *path)
{
    char fullpath[IDB_PATH_MAX];
    int level = -1, ret = -1;
    struct stat st = {0};
    char *p = NULL;

    if(path)
    {
        strcpy(fullpath, path);
        p = fullpath;
        while(*p != '\0')
        {
            if(*p == '/' )
            {
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
                level++;
            }
            ++p;
        }
        return 0;
    }
    return -1;
}

/* set base directory */
int idb_set_basedir(IDB *db, char *dir)
{
    char path[IDB_PATH_MAX];
    struct stat st = {0};
    int i = 0, ret = -1;

    if(db && dir)
    {
        //key
        sprintf(path, "%s/%s", dir, IDB_KEY_NAME); 
        idb_mkdir(path);
        if((db->kmap_fd = open(path, O_CREAT|O_RDWR, 0644)) <= 0)
        {
            fprintf(stderr, "open file(%s) failed, %s\r\n", path, strerror(errno));
            _exit(-1);
            return -1;
        }
        fstat(db->kmap_fd, &st);
        db->kmap_size =  st.st_size;
        if(st.st_size == 0)
        {
            db->kmap_size = sizeof(IKEY) * IDB_IKEY_MAX;
            ret = ftruncate(db->kmap_fd, db->kmap_size);
        }
        db->kmap_maxid = db->kmap_size/sizeof(IKEY);
        if((db->kmap = (IKEY *)mmap(NULL, db->kmap_size, PROT_READ|PROT_WRITE, 
            MMAP_SHARED, db->kmap_fd, 0)) == NULL || db->kmap == (void *)-1)
        {
            fprintf(stderr, "mmap fd[%d] file(%s) failed, %s\r\n", 
                    db->kmap_fd, path, strerror(errno));
            _exit(-1);
            return -1;
        }
        
        /* link */
        sprintf(path, "%s/%s", dir, IDB_LINK_NAME);
        if((db->link_fd = open(path, O_CREAT|O_RDWR, 0644)) <= 0)
        {
            fprintf(stderr, "open file(%s) failed, %s\r\n", path, strerror(errno));
            _exit(-1);
            return -1;
        }
        fstat(db->link_fd, &st);
        db->link_size = st.st_size;
        if(st.st_size == 0)
        {
            db->link_size = sizeof(ILINK) * IDB_INDEX_NUM * IDB_NUM_MAX;
            ret = ftruncate(db->link_fd, db->link_size);
        }
        if((db->links = (ILINK *)mmap(NULL, db->link_size, PROT_READ|PROT_WRITE, 
                        MMAP_SHARED, db->link_fd, 0)) == NULL || db->links == (void *)-1)
        {
            fprintf(stderr, "mmap fd[%d] file(%s) failed, %s\r\n", 
                    db->link_fd, path, strerror(errno));
            _exit(-1);
            return -1;
        }
        //indexs
        for(i = 0; i < IDB_INDEX_NUM; i++)
        {
            sprintf(path, "%s/%d.%s", dir, i, IDB_IDX_EXT);
            if((db->hash[i].fd = open(path, O_CREAT|O_RDWR, 0644)) <= 0)
            {
                fprintf(stderr, "open index[%d](%s) failed, %s\r\n",
                        i, path, strerror(errno));
                _exit(-1);
            }
            memset(&st, 0, sizeof(struct stat));
            if(fstat(db->hash[i].fd, &st) == 0)
            {
                db->hash[i].size = db->hash[i].end = st.st_size;
#ifndef _DISABLE_MEMINDEX
                if(db->hash[i].end < IDB_IDX_MAX) db->hash[i].size = IDB_IDX_MAX;
                if((db->hash[i].map = (char *)mmap(NULL, db->hash[i].size, PROT_READ|PROT_WRITE,
                    MMAP_SHARED, db->hash[i].fd, 0)) == NULL || db->hash[i].map == (char *)-1)
                {
                    fprintf(stderr, "mmap index[%d][%s] failed, %s\n", i, path, strerror(errno));
                    _exit(-1);
                }
#endif
            }
            else 
            {
                fprintf(stderr, "stat index[%d](%s) failed, %s\r\n",
                        i, path, strerror(errno));
                _exit(-1);
            }
        }
        //log
        sprintf(path, "%s/%s", dir, IDB_LOG_NAME);
        LOGGER_INIT(db->logger, path);
        return 0;
    }
    return ret;
}

/* push mblock */
void idb_push_mblock(IDB *idb, char *mblock)
{
    int x = 0;

    if(idb && mblock)
    {
        if(idb->nqmblocks < IDB_MBLOCKS_MAX)
        {
            x = idb->nqmblocks++;
            idb->qmblocks[x] = mblock;
            //ACCESS_LOGGER(idb->logger, "nqmblocks:%d", idb->nqmblocks);
        }
        else
        {
            free(mblock);
        }
    }
    return ;
}

/* idb pop mblock */
char *idb_pop_mblock(IDB *idb)
{
    char *mblock = NULL;
    int x = 0;

    if(idb)
    {
        if(idb->nqmblocks > 0)
        {
            x = --(idb->nqmblocks);
            mblock = idb->qmblocks[x];
            idb->qmblocks[x] = NULL;
        }
        else
        {
            mblock = (char *)calloc(1, IDB_MBLOCK_MAX);
        }
    }
    return mblock;
}
#define IDB_FREE_DBMM(xdb, mm)                              \
do                                                          \
{                                                           \
    if(xdb && mm && mm->data)                               \
    {                                                       \
        if(mm->ndata > IDB_MBLOCK_MAX)                      \
        {                                                   \
            free(mm->data);                                 \
        }                                                   \
        else                                                \
        {                                                   \
            idb_push_mblock(xdb, mm->data);                 \
        }                                                   \
        mm->data = NULL;                                    \
        mm->ndata = 0;                                      \
    }                                                       \
}while(0)

/* free malloc data */
void idb_free(IDB *idb, IBDATA *dbmm)
{
    if(idb && dbmm)
    {
        MUTEX_LOCK(idb->mutex);
        IDB_FREE_DBMM(idb, dbmm);
        MUTEX_UNLOCK(idb->mutex);
    }
    return ;
}

//msync(blck->kmap, blck->kmap_size, MS_SYNC);                            
/* ikey */
#define IDB_KEY(blck, xid, xkey)                                                    \
do                                                                                  \
{                                                                                   \
    if(blck && xid >= 0 && blck->kmap)                                              \
    {                                                                               \
        if(xid >= blck->kmap_maxid)                                                 \
        {                                                                           \
            if(blck->kmap_size > 0)munmap(blck->kmap, blck->kmap_size);             \
            blck->kmap = NULL;                                                      \
            blck->kmap_size = (off_t)((xid/IDB_INCREMENT)+1);                       \
            blck->kmap_size *= ((off_t)IDB_INCREMENT * (off_t)sizeof(IKEY));        \
            if(ftruncate(blck->kmap_fd, blck->kmap_size) == 0                       \
                && (blck->kmap = (IKEY *)mmap(NULL, blck->kmap_size,                \
                        PROT_READ|PROT_WRITE, MMAP_SHARED, blck->kmap_fd, 0))       \
                && blck->kmap  != (IKEY *) -1)                                      \
            {                                                                       \
                blck->kmap_maxid = (int)(blck->kmap_size/(off_t)sizeof(IKEY));      \
            }else break;                                                            \
        }                                                                           \
        if(blck->kmap && blck->kmap != (IKEY *)-1 && xid < blck->kmap_maxid)        \
        {                                                                           \
            xkey = &(blck->kmap[xid]);                                              \
        }                                                                           \
    }                                                                               \
}while(0)

/* push block */
int idb_push_block(IDB *db, int index, int blockid, int blocksize)
{
    int id = 0, x = 0, i = 0, ret = -1;
    ILINK link = {0}, *plink = NULL;

    if(db && db->links)
    {
        id =  IDB_NUM_MAX * index;
        x =  (blocksize/IDB_BSIZE);
        while((x >>= 1) > 0)++i;
        if(i < IDB_NUM_MAX) id += i;
        //fprintf(stdout, "%s::%d push_block(%d) index[%d] blocksize:%d total:%d\r\n", __FILE__, __LINE__, blockid, id, blocksize, db->links[id].total);
        if(db->links[id].total == 0)
        {
            db->links[id].first = db->links[id].last = blockid;
            //fprintf(stdout, "%s::%d push blockid:%d  block_size:%d\n", __FILE__, __LINE__, blockid, blocksize);
            //fprintf(stdout, "%s::%d total:%d blockid:%d\r\n", __FILE__, __LINE__, db->links[id].total, blockid);
        }
        else
        {
            link.first = db->links[id].last;
            //fprintf(stdout, "%s::%d push blockid:%d  block_size:%d total:%d\n", __FILE__, __LINE__, blockid, blocksize, db->links[id].total);
#ifdef _DISABLE_MEMINDEX
            if(lseek(db->hash[index].fd, (off_t)blockid * (off_t)IDB_BSIZE, SEEK_SET) < 0 
                    || write(db->hash[index].fd, &link, sizeof(ILINK)) <= 0)
            {
                FATAL_LOGGER(db->logger, "write block[%d] link failed, %s", 
                        blockid, strerror(errno));
            }
#else
            if(db->hash[index].map && (plink = (ILINK *)(db->hash[index].map 
                            + (off_t)blockid * (off_t)IDB_BSIZE)))
            {
                memset(plink, 0, sizeof(ILINK));
                plink->first = db->links[id].last;
            }
#endif
            db->links[id].last = blockid;
        }
        db->links[id].total++;
        DEBUG_LOGGER(db->logger, "push_block(%d) index[%d] blocksize:%d total:%d", 
                blockid, id, blocksize, db->links[id].total);
        ret = 0;
    }
    return ret;
}

/* new block */
int idb_newblock(IDB *db, int index, int blocksize)
{
    int id = 0, x = 0, i = 0, blockid = -1;
    ILINK link = {0}, *plink = NULL;
    char *map = NULL;

    if(db && db->links)
    {
        id =  IDB_NUM_MAX * index;
        x =  (blocksize/IDB_BSIZE);
        while((x >>= 1) > 0)++i;
        if(i < IDB_NUM_MAX) id += i;
        DEBUG_LOGGER(db->logger, "total:%d pop_block(%d) block_size:%d", 
                db->links[id].total, blockid, blocksize);
        if(db->links[id].total > 0)
        {
            blockid = db->links[id].last;
            //fprintf(stdout, "%s::%d pop blockid:%d  block_size:%d total:%d\n", __FILE__, __LINE__, blockid, blocksize, db->links[id].total);
            db->links[id].total--;
#ifdef _DISABLE_MEMINDEX
            if(lseek(db->hash[index].fd, (off_t)(blockid * IDB_BSIZE), SEEK_SET) >= 0 
                    && read(db->hash[index].fd, &link, sizeof(ILINK)) > 0)
            {
                db->links[id].last = link.first;
                DEBUG_LOGGER(db->logger, "pop old block[%d](%d)", blockid, blocksize);
            }
#else
            if((map = db->hash[index].map) && (plink = (ILINK *)(map 
                            + ((off_t)blockid * (off_t)IDB_BSIZE))))
            {
                //fprintf(stdout, "%s::%d blockid:%d map:%p link:%p\n", __FILE__, __LINE__, blockid, map, plink);
                db->links[id].last = plink->first;
            }
#endif
        }
        else
        {
            blockid = (db->hash[index].end/IDB_BSIZE);
            db->hash[index].end += (off_t)blocksize;
            if(ftruncate(db->hash[index].fd, db->hash[index].end) != 0)
                return -1;
            DEBUG_LOGGER(db->logger, "pop new block[%d](%d)", blockid, blocksize);
#ifndef _DISABLE_MEMINDEX
            if(db->hash[index].end > db->hash[index].size)
            {
                munmap(db->hash[index].map, db->hash[index].size);
                db->hash[index].size += (off_t)IDB_IDX_INCREMENT;
                db->hash[index].map = (char *)mmap(NULL, db->hash[index].size, PROT_READ|PROT_WRITE,
                    MMAP_SHARED, db->hash[index].fd, 0);
            }
#endif
        }
    }
    return blockid;
}

/* add data */
int idb_add_data(IDB *db, int key, char *data, int ndata)
{
    int ret = -1, left = 0, index = 0, fd = 0, blockid = 0, block_size = 0;
    char *block = NULL, *old = NULL, *new = NULL, *map = NULL;
    IKEY *ikey = NULL;

    if(db && key >= 0 && data && ndata > 0)
    {
        IDB_KEY(db, key, ikey);
        if(ikey)
        {
            //ERROR_LOGGER(db->logger, "id:%d blockid:%d block_size:%d ndata:%d mmb_off:%d", key, ikey->blockid, ikey->block_size, ndata, ikey->mmb_off);
            //check left
            index = key % IDB_INDEX_NUM;
            fd = db->hash[index].fd;
            left =  ikey->block_size - ikey->btotal; 
            //ERROR_LOGGER(db->logger, "id:%d blockid:%d block_size:%d ndata:%d mmb_left:%d mmb_off:%d", key, ikey->blockid, ikey->block_size, ndata, mmb_left, ikey->mmb_off);
            if(ndata > left)
            {
                //ERROR_LOGGER(db->logger, "id:%d blockid:%d block_size:%d ndata:%d mmb_off:%d", key, ikey->blockid, ikey->block_size, ndata, ikey->mmb_off);
                block_size = ikey->block_size;
                if(block_size <= 0) block_size = IDB_BSIZE;
                while((block_size - ikey->btotal) < ndata) 
                    block_size *= 2;
                blockid = idb_newblock(db, index, block_size);
#ifdef _DISABLE_MEMINDEX
                if(ikey->btotal > 0 && (old = (char *)calloc(1, ikey->block_size)))
                {
                    if(lseek(fd, ((off_t)ikey->blockid * (off_t)IDB_BSIZE), SEEK_SET) >= 0 
                            && read(fd, old, ikey->btotal) > 0)
                    {
                        if(lseek(fd, (off_t)blockid * (off_t)IDB_BSIZE, SEEK_SET) < 0 
                            || write(fd, old, ikey->btotal) <= 0)
                        {
                            FATAL_LOGGER(db->logger, "write new block[%d](%d) failed, %s", 
                                    blockid, ikey->btotal, strerror(errno));
                        }
                    }
                    else
                    {
                        FATAL_LOGGER(db->logger, "read old block[%d](%d) failed, %s", 
                                ikey->blockid, ikey->btotal, strerror(errno));
                    }
                    free(old);
                    old = NULL;
                }
#else
                map = db->hash[index].map;
                if(ikey->btotal > 0 && map && map != (char *)-1)
                {
                    if((old = (map + ((off_t)ikey->blockid * (off_t)IDB_BSIZE)))
                        && (new = (map + ((off_t)blockid * (off_t)IDB_BSIZE))))
                    {
                        memcpy(new, old, ikey->btotal);
                    }
                    else
                    {
                        FATAL_LOGGER(db->logger, "remove block[%d] to new block[%d] failed, %s",
                                ikey->blockid, blockid, strerror(errno));
                    }
                }
#endif
                if(ikey->block_size > 0 && ikey->blockid > 0) 
                {
                    idb_push_block(db, index, ikey->blockid, ikey->block_size);
                    //fprintf(stdout, "%s::%d push old blockid:%d block_size:%d\n", __FILE__, __LINE__, ikey->blockid, ikey->block_size);
                }
                ikey->block_size = block_size;
                ikey->blockid = blockid;
                ret = 0;
                //ERROR_LOGGER(db->logger, "id:%d blockid:%d block_size:%d ndata:%d mmb_off:%d", key, ikey->blockid, ikey->block_size, ndata, ikey->mmb_off);
            }
#ifdef _DISABLE_MEMINDEX
            if(lseek(fd, ((off_t)ikey->blockid*(off_t)IDB_BSIZE+(off_t)ikey->btotal), 
                        SEEK_SET) >= 0 && write(fd, data, ndata)>0)
            {
                ikey->btotal += ndata;
                ret = 0;
            }
            else
            {
                FATAL_LOGGER(db->logger, "write index[%d] to block[%d](%d) failed, %s",
                        key, ikey->blockid, ndata, strerror(errno));
            }
#else
            map = db->hash[index].map;
            if(map && map != (char *)-1 && (block=(map+((off_t)ikey->blockid*(off_t)IDB_BSIZE))))
            {
                memcpy(block+ikey->btotal, data, ndata);
                ikey->btotal += ndata;
                ret = 0;
            }
#endif
        }
    }
    return ret;
}

/* add */
int idb_add(IDB *db, int key, char *data, int ndata)
{
    int ret = -1;

    if(db && data && ndata > 0)
    {
        MUTEX_LOCK(db->mutex);
        ret = idb_add_data(db, key, data, ndata);
        MUTEX_UNLOCK(db->mutex);
    }
    return ret;
}

/* get data */
int idb_get(IDB *db, int key, IBDATA *dbmm)
{
    int fd = 0, total = -1, index = 0;
    IKEY *ikey = NULL;
    char *p = NULL;

    if(db && key >= 0 && dbmm)
    {
        MUTEX_LOCK(db->mutex);
        IDB_KEY(db, key, ikey);
        index = key % IDB_INDEX_NUM;
        if(ikey && (total = ikey->btotal) > 0 
                && (fd = db->hash[index].fd) > 0)
        {
                if(ikey->block_size <= IDB_MBLOCK_MAX)
                {
                    dbmm->data = idb_pop_mblock(db);
                }
                else 
                {
                    dbmm->data = (char *)calloc(1, ikey->block_size);
                }
                dbmm->ndata = ikey->block_size;
#ifndef _DISABLE_MEMINDEX
            if(dbmm->data && (p = (db->hash[index].map+((off_t)ikey->blockid * (off_t)IDB_BSIZE))))
            {
                memcpy(dbmm->data, p, total);
            }
            else 
            {
                total = -1;
                IDB_FREE_DBMM(db, dbmm);
            }
#else
            if(lseek(fd, ((off_t)ikey->blockid * (off_t)IDB_BSIZE), SEEK_SET) < 0 
                    || read(fd, dbmm->data, total) <= 0)
            {
                total = -1;
                IDB_FREE_DBMM(db, dbmm);
            }
#endif
            //fprintf(stdout, "%s|%d|%d|%d|%s\n", (char *)(*data), ikey->btotal, total, ikey->mmb_off, ikey->mmblock);
        }
        MUTEX_UNLOCK(db->mutex);
    }
    return total;
}

/* delete data */
int idb_del(IDB *db, int key)
{
    IKEY *ikey = NULL;
    int ret = -1;

    if(db && key >= 0)
    {
        MUTEX_LOCK(db->mutex);
        IDB_KEY(db, key, ikey);
        if(ikey)
        {
            idb_push_block(db, (key%IDB_INDEX_NUM), ikey->blockid, ikey->block_size);
            memset(ikey, 0, sizeof(IKEY));
            ikey->blockid = -1;
            ret = 0;
        }
        MUTEX_UNLOCK(db->mutex);
    }
    return ret;
}

/* update data */
int idb_update(IDB *db, int key, char *data, int ndata)
{
    int ret = -1;

    if(db && key >= 0 && data && ndata > 0)
    {
        if(idb_del(db, key) == 0)
            ret = idb_add(db, key, data, ndata);
    }
    return ret;
}

/* index */
/*
int idb_index(IDB *db, int termid, int docid, int no, int bit_fields, 
        int term_count, int prevnext_size, char *pdata)
{
    int left = 0, ndocid = 0, n = 0, *np = NULL, need = 0, ret = -1;
    char *p = NULL, *pold = NULL, *mdata = NULL, buf[IDB_BUF_SIZE];
    IKEY *ikey = NULL;

    if(db)
    {
        MUTEX_LOCK(db->mutex);
        IDB_KEY(db, termid, ikey);
        if(ikey)
        {
            need = 5 * sizeof(int) + prevnext_size;
#ifndef _DISABLE_MEMBLOCK
            left = IDB_MMB_SIZE - ikey->mmb_off;
            if(left > need) pold = ikey->mmblock + ikey->mmb_off;
            DEBUG_LOGGER(db->logger, "need:%d left:%d off:%d",  need, left, ikey->mmb_off);
#endif
            if(need < IDB_BUF_SIZE)
            {
                pold = buf;
            }
            else
            {
                if((pold = mdata = (char *)calloc(1, need)) == NULL)
                    goto err;
            }
            p = pold;
#ifndef     _DISABLE_COMPRESS
            ndocid = docid - ikey->last_docid;
            n = ndocid; np = &n; ZVBCODE(np, p);
            n = term_count; np = &n;  ZVBCODE(np, p);
            n = no; np = &n; ZVBCODE(np, p);
            n = bit_fields; np = &n; ZVBCODE(np, p);
            n = prevnext_size; np = &n; ZVBCODE(np, p);
#else
            memcpy(p, &docid, sizeof(int));p += sizeof(int);
            memcpy(p, &term_count, sizeof(int));p += sizeof(int);
            memcpy(p, &no, sizeof(int));p += sizeof(int);
            memcpy(p, &bit_fields, sizeof(int));p += sizeof(int);
            memcpy(p, &prevnext_size, sizeof(int));p += sizeof(int);
#endif
            if(prevnext_size > 0)
            {
                memcpy(p, pdata, prevnext_size);
                p += prevnext_size;
            }
            else if(prevnext_size < 0)
            {
                fprintf(stderr, "ERROR prevnext_size:%d\n", prevnext_size);
                _exit(-1);
            }
            if(pold == buf)
                ret = idb_add_data(db, termid, buf, (p - buf));
            else if(pold == mdata)
                ret = idb_add_data(db, termid, mdata, (p - mdata));
            else
            {
                goto err;
            }
            //IDB_KEY(db, termid, ikey);
            //if(ikey)
            //{
            ikey->last_docid = docid;
            //}
            if(mdata) free(mdata);
        }
err:
        MUTEX_UNLOCK(db->mutex);
    }
    return ret;
}
*/

/* clean */
void idb_clean(IDB *db)
{
    int i = 0;

    if(db)
    {
        if(db->links) 
        {
            //msync(db->links, db->link_size, MS_SYNC);
            munmap(db->links, db->link_size);
        }
        if(db->kmap) 
        {
            //msync(db->kmap, db->kmap_size, MS_SYNC);
            munmap(db->kmap, db->kmap_size);
        }
        for(i = 0; i < db->nqmblocks; i++)
        {
            if(db->qmblocks[i]) free(db->qmblocks[i]);
        }
        LOGGER_CLEAN(db->logger);
        MUTEX_DESTROY(db->mutex);
        free(db);
    }
    return ;
}

/* initialize  */
IDB *idb_init()
{
    IDB *db = NULL;

    if((db = (IDB *)calloc(1, sizeof(IDB))))
    {
        MUTEX_INIT(db->mutex);
        db->set_basedir     =   idb_set_basedir; 
        db->add             =   idb_add; 
        db->get             =   idb_get; 
        db->update          =   idb_update; 
        db->del             =   idb_del; 
        db->free            =   idb_free; 
        db->clean           =   idb_clean; 
    }
    return db;
}

#ifdef _DEBUG_IDB
#define TEST_BUF_SIZE   65536
//gcc -o db idb.c -I utils/ -D_DEBUG_IDB && ./db
int main()
{
    char buf[TEST_BUF_SIZE], *p = NULL, *s = NULL;
    IDB *db = NULL;
    int id = 0, i = 0, j = 0, n = 0;

    if((db = idb_init()))
    {
        db->set_basedir(db, "/tmp/db");
        //add data
        for(i = 0; i < 10; i++)
        {
            for(j = 0; j < 1000000; j++)
            {
                n = sprintf(buf, "%d\r\n", j);
                db->add(db, i, buf, n);
            }
        }
        //get
        if((n = db->get(db, 6, &p)) > 0)
        {
            fprintf(stdout, "%s", p);
            fprintf(stdout, "len:%d\r\n", n);
            db->free(p);
        }
        if((p = s = (char *)calloc(1, 32 * 1024 * 1024)))
        {
            for(i = 0; i < 2000000; i++)
            {
                p += sprintf(p, "%d\r\n", i);
            }
            if(db->update(db, 6, s, (p - s)) == 0)
            {
                if((n = db->get(db, 6, &p)) > 0)
                {
                    fprintf(stdout, "%s", p);
                    fprintf(stdout, "len:%d\r\n", n);
                    db->free(p);
                }
            }
            free(s);
        }
        db->clean(db);
    }

}
#endif
