#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/file.h>
#include <sys/time.h>
#include <zlib.h>
#include <ibase.h>
#ifdef  HAVE_SCWS
#include <scws/scws.h>
#endif
#include "xmm.h"
#include "hidoc.h"
#include "html.h"
#include "mutex.h"
#include "timer.h"
#include "logger.h"
#include "mtree.h"
#include "mmtree64.h"
#include "mmtrie.h"
#include "mmqueue.h"
#include "iqueue.h"
#include "zvbcode.h"
#include "db.h"
#define HINDEX_DICT_NAME        "hindex.dict"
#define HINDEX_XDICT_NAME       "hindex.xdict"
#define HINDEX_DOC_NAME         "hindex.doc"
#define HINDEX_STATE_NAME       "hindex.state"
#define HINDEX_LOG_NAME         "hindex.log"
#define HINDEX_SRC_NAME         "hindex.src"
#define HINDEX_PACKET_NAME      "hindex.packet"
#define HINDEX_HITASK_NAME      "hindex.task"
#define HINDEX_MAP_NAME         "hindex.map"
#define HINDEX_KMAP_NAME        "hindex.kmap"
#define HINDEX_XINDEX_NAME      "hindex.xindex"
#define HINDEX_XINT_NAME        "hindex.xint"
#define HINDEX_XLONG_NAME       "hindex.xlong"
#define HINDEX_XDOUBLE_NAME     "hindex.xdouble"
#define HINDEX_FDICT_NAME       "hindex.fdict"
#define HINDEX_FMAP_NAME        "hindex.fmap"
#define HINDEX_NODEMAP_NAME     "hindex.namemap"
#define HINDEX_MMQUEUE_NAME     "hindex.mmqueue"
#define HINDEX_BSTERM_NAME      "hindex.bsterm"
#define HINDEX_DB_DIR           "db"
#define HINDEX_UPDATE_DIR       "update"
#define HIDOC_GLOBALID_DEFAULT  200000000
#ifndef LL64
#define LL64(x) ((long long int ) x)
#endif
#ifndef LLU
#define LLU(x) ((unsigned long long int ) x)
#endif
#define UCHR(p) ((unsigned char *)p)
#define ISSIGN(p) (*p == '@' || *p == '.' || *p == '-' || *p == '_')
#define ISNUM(p) ((*p >= '0' && *p <= '9'))
#define ISCHAR(p) ((*p >= 'A' && *p <= 'Z')||(*p >= 'a' && *p <= 'z'))
#define REALLOC(xp, np, len)                                                            \
do                                                                                      \
{                                                                                       \
    if(len > np)                                                                        \
    {                                                                                   \
        if(xp) free(xp);                                                                \
        xp = NULL;                                                                      \
        np = ((len/HI_DOCBLOCK_MAX)+((len%HI_DOCBLOCK_MAX) != 0)) * HI_DOCBLOCK_MAX;    \
    }else np = HI_DOCBLOCK_MAX;                                                         \
    if(xp == NULL) xp = (char *)calloc(1, np);                                          \
}while(0)
/* update index  */
void hidoc_update(HIDOC *hidoc, int mid, int flag);
/* set index status */
int hidoc_set_idx_status(HIDOC *hidoc, int64_t globalid, int status);
int hidoc__set__header(HIDOC *hidoc, int mid, FHEADER *fheader);
int hidoc_mid(HIDOC *hidoc, int64_t globalid);
int hidoc_checkid(HIDOC *hidoc, int64_t globalid);
/* set index status */
int hidoc__set__idx__status(HIDOC *hidoc, int64_t globalid, int status);
int hidoc__set__rank(HIDOC *hidoc, int64_t globalid, double rank);
int hidoc__set__category(HIDOC *hidoc, int64_t globalid, int flag, int64_t category);
int hidoc_set_int_index(HIDOC *hidoc, int, int );
int hidoc_set_long_index(HIDOC *hidoc, int, int );
int hidoc_set_double_index(HIDOC *hidoc, int, int );
int hidoc_set_all_int_fields(HIDOC *hidoc, int64_t globalid, int *list);
int hidoc_set_all_long_fields(HIDOC *hidoc, int64_t globalid, int64_t *list);
int hidoc_set_all_double_fields(HIDOC *hidoc, int64_t globalid, double *list);
int hidoc_set_int_fields(HIDOC *hidoc, FXINT *list, int count);
int hidoc_set_long_fields(HIDOC *hidoc, FXLONG *list, int count);
int hidoc_set_double_fields(HIDOC *hidoc, FXDOUBLE *list, int count);
/* mkdir force */
int pmkdir(char *path)
{
    char fullpath[IB_PATH_MAX];
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

/* set document basedir  */
int hidoc_set_basedir(HIDOC *hidoc, char *basedir)
{
    char path[FILE_PATH_MAX];//, *p = NULL;
    int i = 0, j = 0, taskid = 0;
    HITASK *tasks = NULL;
    struct stat st = {0};
    off_t size = 0;

    if(hidoc)
    {
        strcpy(hidoc->basedir, basedir);
        /* dict */
        sprintf(path, "%s/%s", basedir, HINDEX_DICT_NAME);
        pmkdir(path);
        if((hidoc->mmtrie = mmtrie_init(path)) == NULL)
        {
            fprintf(stderr, "Initialize dict(%s) failed, %s\n", path, strerror(errno));
            _exit(-1);
            return -1;
        }
        /* xdict */
        sprintf(path, "%s/%s", basedir, HINDEX_XDICT_NAME);
        if((hidoc->xdict = mmtrie_init(path)) == NULL)
        {
            fprintf(stderr, "Initialize xdict(%s) failed, %s\n", path, strerror(errno));
            _exit(-1);
            return -1;
        }
        /* hindex doc */
        //sprintf(path, "%s/%s", basedir, HINDEX_DOC_NAME);
        //hidoc->outdocfd = open(path, O_CREAT|O_RDWR|O_APPEND, 0644);
        /* mmqueue */
        sprintf(path, "%s/%s", basedir, HINDEX_MMQUEUE_NAME);
        hidoc->mmqueue = mmqueue_init(path);
        /* data */
        sprintf(path, "%s/%s", basedir, HINDEX_DB_DIR);
        hidoc->db = db_init(path, 0);
        /* update */
        sprintf(path, "%s/%s", basedir, HINDEX_UPDATE_DIR);
        hidoc->update = db_init(path, 0);
        /* histate */
        sprintf(path, "%s/%s", basedir, HINDEX_STATE_NAME);
        if((hidoc->histatefd = open(path, O_CREAT|O_RDWR, 0644)) <= 0)
        {
            fprintf(stderr, "open state file(%s) failed, %s\n", path, strerror(errno));
            _exit(-1);
        }
        else
        {
            if((hidoc->state = (HISTATE *)mmap(NULL, sizeof(HISTATE), PROT_READ|PROT_WRITE,
                    MAP_SHARED, hidoc->histatefd, 0)) == NULL || hidoc->state == (void *)-1)
            {
                fprintf(stderr, "mmap state failed, %s\n", strerror(errno));
                _exit(-1);
            }
            fstat(hidoc->histatefd, &st);
            if(st.st_size < sizeof(HISTATE))
            {
                if(ftruncate(hidoc->histatefd, 0) != 0
                        || ftruncate(hidoc->histatefd, sizeof(HISTATE)) != 0)_exit(-1);
                memset(hidoc->state, 0, sizeof(HISTATE));
            }
            if(hidoc->state->nnodes > 0)
            {
                for(i = 1; i < HI_NODE_MAX; i++)
                {
                    if(hidoc->state->nodes[i].status > 0 && hidoc->state->nodes[i].ntasks > 0
                            && (tasks = hidoc->state->nodes[i].tasks))
                    {
                        for(j = 0; j < HI_TASKS_MAX; j++)
                        {
                            if(tasks[j].status > 0)
                            {
                                taskid = i * HI_TASKS_MAX + j;
                                iqueue_push(hidoc->queue, taskid);
                            }
                        }
                    }
                }
            }
        }
#ifdef  _SRC_
        sprintf(path, "%s/%s", basedir, HINDEX_SRC_NAME);
        hidoc->fp = (void *)fopen(path, "w");
#endif
        /* log */
        sprintf(path, "%s/%s", basedir, HINDEX_LOG_NAME);
        LOGGER_INIT(hidoc->logger, path);
        LOGGER_SET_LEVEL(hidoc->logger, hidoc->log_access);
        //p = path;
        /* name map */
        sprintf(path, "%s/%s", basedir, HINDEX_NODEMAP_NAME);
        hidoc->namemap = mmtrie_init(path);
        /* kmap */
        sprintf(path, "%s/%s", basedir, HINDEX_KMAP_NAME);
        hidoc->kmap = mmtrie_init(path);
        ////if(hidoc->state->kmaproot == 0) hidoc->state->kmaproot = mmtree64_new_tree(hidoc->kmap);
        /* block terms map */
        sprintf(path, "%s/%s", basedir, HINDEX_MAP_NAME);
        hidoc->map = mmtrie_init(path);
        /* bsterm file */
        sprintf(path, "%s/%s", basedir, HINDEX_BSTERM_NAME);
        if((hidoc->bstermio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(hidoc->bstermio.fd, &st) == 0)
        {
            hidoc->bstermio.end = st.st_size;
            size = sizeof(BSTERM) * HI_BSTERM_MAX;
            if(st.st_size > size) size = st.st_size;
            if((hidoc->bstermio.map = (char *)mmap(NULL, size, PROT_READ|PROT_WRITE, 
                            MAP_SHARED, hidoc->bstermio.fd, 0)) 
                    && hidoc->bstermio.map != (void *)-1)
            {
                hidoc->bstermio.size = size;
            }
            else
            {
                fprintf(stderr, "mmap bsterm file (%s) failed, %s\n", path, strerror(errno));
                _exit(-1);
            }
        }
        else
        {
            fprintf(stderr, "open bsterm file(%s) failed, %s\n", path, strerror(errno));
            _exit(-1);
        }
        /* index */
        sprintf(path, "%s/%s", basedir, HINDEX_XINDEX_NAME);
        if((hidoc->xindexio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(hidoc->xindexio.fd, &st) == 0)
        {
            hidoc->xindexio.end = st.st_size;
            size = sizeof(XINDEX) * HI_XINDEX_MAX;
            if(st.st_size > size) size = st.st_size;
            if((hidoc->xindexio.map = mmap(NULL, size, PROT_READ|PROT_WRITE, 
                            MAP_SHARED, hidoc->xindexio.fd, 0)) 
                    && hidoc->xindexio.map != (void *)-1)
            {
                hidoc->xindexio.size = size;
            }
            else
            {
                fprintf(stderr, "mmap xindex file (%s) failed, %s\n", path, strerror(errno));
                _exit(-1);
            }
        }
        else
        {
            fprintf(stderr, "open xindex file(%s) failed, %s\n", path, strerror(errno));
            _exit(-1);
        }
        //hidoc_set_int_index(hidoc, hidoc->state->int_index_from, hidoc->state->int_index_count);
        //hidoc_set_long_index(hidoc, hidoc->state->long_index_from, hidoc->state->long_index_count);
        //hidoc_set_double_index(hidoc, hidoc->state->double_index_from, hidoc->state->double_index_count);
        return 0;
    }
    return -1;
}

/* set int index */
int hidoc_set_int_index(HIDOC *hidoc, int int_index_from, int int_index_count)
{
    char path[FILE_PATH_MAX];
    struct stat st = {0};
    off_t size = 0;

    if(hidoc && int_index_from >= 0 && int_index_count > 0)
    {
        hidoc->state->int_index_from = int_index_from;
        hidoc->state->int_index_count = int_index_count;
        /* index */
        sprintf(path, "%s/%s", hidoc->basedir, HINDEX_XINT_NAME);
        if((hidoc->xintio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(hidoc->xintio.fd, &st) == 0)
        {
            hidoc->xintio.end = st.st_size;
            size = (sizeof(int) * int_index_count) * HI_XINDEX_MAX;
            if(st.st_size > size) size = st.st_size;
            if((hidoc->xintio.map = mmap(NULL, size, PROT_READ|PROT_WRITE, 
                            MAP_SHARED, hidoc->xintio.fd, 0)) 
                    && hidoc->xintio.map != (void *)-1)
            {
                hidoc->xintio.size = size;
                return 0;
            }
            else
            {
                fprintf(stderr, "mmap int file (%s) failed, %s\n", path, strerror(errno));
                _exit(-1);
            }
        }
        else
        {
            fprintf(stderr, "open int file(%s) failed, %s\n", path, strerror(errno));
            _exit(-1);
        }
    }
    return -1;
}

/* set long index */
int hidoc_set_long_index(HIDOC *hidoc, int long_index_from, int long_index_count)
{
    char path[FILE_PATH_MAX];
    struct stat st = {0};
    off_t size = 0;

    if(hidoc && long_index_from >= 0 && long_index_count > 0)
    {
        hidoc->state->long_index_from = long_index_from;
        hidoc->state->long_index_count = long_index_count;
        /* index */
        sprintf(path, "%s/%s", hidoc->basedir, HINDEX_XLONG_NAME);
        if((hidoc->xlongio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(hidoc->xlongio.fd, &st) == 0)
        {
            hidoc->xlongio.end = st.st_size;
            size = (sizeof(int64_t) * long_index_count) * HI_XINDEX_MAX;
            if(st.st_size > size) size = st.st_size;
            if((hidoc->xlongio.map = mmap(NULL, size, PROT_READ|PROT_WRITE, 
                            MAP_SHARED, hidoc->xlongio.fd, 0)) 
                    && hidoc->xlongio.map != (void *)-1)
            {
                hidoc->xlongio.size = size;
                return 0;
            }
            else
            {
                fprintf(stderr, "mmap int file (%s) failed, %s\n", path, strerror(errno));
                _exit(-1);
            }
        }
        else
        {
            fprintf(stderr, "open int file(%s) failed, %s\n", path, strerror(errno));
            _exit(-1);
        }
    }
    return -1;
}

/* set double index */
int hidoc_set_double_index(HIDOC *hidoc, int double_index_from, int double_index_count)
{
    char path[FILE_PATH_MAX];
    struct stat st = {0};
    off_t size = 0;

    if(hidoc && double_index_from >= 0 && double_index_count > 0)
    {
        hidoc->state->double_index_from = double_index_from;
        hidoc->state->double_index_count = double_index_count;
        /* index */
        sprintf(path, "%s/%s", hidoc->basedir, HINDEX_XDOUBLE_NAME);
        if((hidoc->xdoubleio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(hidoc->xdoubleio.fd, &st) == 0)
        {
            hidoc->xdoubleio.end = st.st_size;
            size = (sizeof(double) * double_index_count) * HI_XINDEX_MAX;
            if(st.st_size > size) size = st.st_size;
            if((hidoc->xdoubleio.map = mmap(NULL, size, PROT_READ|PROT_WRITE, 
                            MAP_SHARED, hidoc->xdoubleio.fd, 0)) 
                    && hidoc->xdoubleio.map != (void *)-1)
            {
                hidoc->xdoubleio.size = size;
                return 0;
            }
            else
            {
                fprintf(stderr, "mmap double file (%s) failed, %s\n", path, strerror(errno));
                _exit(-1);
            }
        }
        else
        {
            fprintf(stderr, "open double file(%s) failed, %s\n", path, strerror(errno));
            _exit(-1);
        }
    }
    return -1;
}

/* check dump */
int hidoc__check__dump(HIDOC *hidoc)
{
    int ret = -1;
    if(hidoc && hidoc->dumpfd <= 0 && hidoc->state 
            && strlen(hidoc->state->dumpfile) > 0)
    {
        ret = hidoc->dumpfd = open(hidoc->state->dumpfile, O_CREAT|O_RDONLY, 0644);
    }
    return ret;
}

/* reset dump */
int hidoc__set__dump(HIDOC *hidoc, char *dumpfile)
{
    int ret = -1, fd = 0;

    if(hidoc && dumpfile && (fd = open(dumpfile, O_RDONLY, 0644)) > 0)
    {
        if(hidoc->dumpfd > 0) 
        {
            WARN_LOGGER(hidoc->logger, "reset_dumpi(%s)", dumpfile);
            close(hidoc->dumpfd);
            hidoc->dumpfd = 0;
        }
        strcpy(hidoc->state->dumpfile, dumpfile);
        hidoc->state->dump_offset = 0;
        ret = hidoc->dumpfd = fd;
    }
    return ret;
}

/* reset dump */
int hidoc_set_dump(HIDOC *hidoc, char *dumpfile)
{
    int ret = -1;
    if(hidoc && dumpfile)
    {
        MUTEX_LOCK(hidoc->mutex);
        ret = hidoc__set__dump(hidoc, dumpfile);
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;
}

/* get dump info */
int hidoc_get_dumpinfo(HIDOC *hidoc, char *out, char *end)
{
    int ret = -1, n = 0;
    struct stat st = {0};
    char *p = NULL;

    if(hidoc && (p = out) && hidoc->state 
            && (n = strlen(hidoc->state->dumpfile))
            && (end - out) > (n + 64)
            && stat(hidoc->state->dumpfile, &st) == 0)
    {
        ret = sprintf(p, "({\"size\":\"%lld\",\"offset\":\"%lld\",\"source\":\"%s\"})", 
                (long long int)st.st_size, (long long int)hidoc->state->dump_offset, 
                hidoc->state->dumpfile);
    }
    return ret;
}

/* import new dict */
int hidoc_set_dict(HIDOC *hidoc, char *dict_file, char *dict_charset, char *dict_rules)
{
    int i = 0;
    if(hidoc)
    {
#ifdef HAVE_SCWS
        scws_t segmentor = NULL;
        if(dict_charset)strcpy(hidoc->dict_charset, dict_charset);
        if(dict_rules)strcpy(hidoc->dict_rules, dict_rules);
        if(dict_file)strcpy(hidoc->dict_file, dict_file);
        if((hidoc->segmentor = scws_new()))
        {
            scws_set_charset((scws_t)(hidoc->segmentor), dict_charset);
            scws_set_rule((scws_t)(hidoc->segmentor), dict_rules);
            scws_set_dict((scws_t)(hidoc->segmentor), dict_file, SCWS_XDICT_XDB);
            for(i = 0; i < IB_SEGMENTORS_MIN; i++)
            {
                if((segmentor = scws_new()))
                {
                    hidoc->qsegmentors[i] = segmentor;
                    ((scws_t)(segmentor))->r = ((scws_t)(hidoc->segmentor))->r;
                    ((scws_t)(segmentor))->d = ((scws_t)(hidoc->segmentor))->d;
                    ((scws_t)(segmentor))->mblen = ((scws_t)(hidoc->segmentor))->mblen;
                    //scws_set_charset((scws_t)(segmentor), hidoc->dict_charset);
                    //scws_set_rule((scws_t)(segmentor), hidoc->dict_rules);
                    //scws_set_dict((scws_t)(segmentor), hidoc->dict_file, SCWS_XDICT_XDB);
                    hidoc->nqsegmentors++;
                }
            }
        }
        else 
        {
            _exit(-1);
        }
#else
        mmtrie_import(hidoc->mmtrie, dict_file, -1);
#endif
        return 0;
    }
    return -1;
}

/* import new dict */
int hidoc_set_forbidden_dict(HIDOC *hidoc, char *dict_file)
{
    if(hidoc && dict_file)
    {
        return 0;
    }
    return -1;
}

/* push segmentor */
void hidoc_push_segmentor(HIDOC *hidoc, void *segmentor)
{
    int x = 0;

    if(hidoc && segmentor)
    {
        MUTEX_LOCK(hidoc->mutex_segmentor);
        if(hidoc->nqsegmentors < IB_SEGMENTORS_MAX)
        {
            x = hidoc->nqsegmentors++; 
            hidoc->qsegmentors[x] = segmentor;
        }
        else
        {
#ifdef HAVE_SCWS
           ((scws_t)segmentor)->d = NULL;
           ((scws_t)segmentor)->r = NULL;
            scws_free((scws_t)segmentor);
#endif
        }
        MUTEX_UNLOCK(hidoc->mutex_segmentor);
    }
    return ;
}

/* pop segmentor */
void *hidoc_pop_segmentor(HIDOC *hidoc)
{
    void *segmentor = NULL;
    int x = 0;

    if(hidoc && hidoc->segmentor)
    {
        MUTEX_LOCK(hidoc->mutex_segmentor);
        if(hidoc->nqsegmentors > 0)
        {
            x = --(hidoc->nqsegmentors);
            segmentor = hidoc->qsegmentors[x];
        }
        else
        {
#ifdef HAVE_SCWS
            if((segmentor = scws_new()))
            {
                ((scws_t)(segmentor))->r = ((scws_t)(hidoc->segmentor))->r;
                ((scws_t)(segmentor))->d = ((scws_t)(hidoc->segmentor))->d;
                ((scws_t)(segmentor))->mblen = ((scws_t)(hidoc->segmentor))->mblen;
                //scws_set_charset((scws_t)(segmentor), hidoc->dict_charset);
                //scws_set_rule((scws_t)(segmentor), hidoc->dict_rules);
                //scws_set_dict((scws_t)(segmentor), hidoc->dict_file, SCWS_XDICT_XDB);
            }
#endif
        }
        MUTEX_UNLOCK(hidoc->mutex_segmentor);
    }
    return segmentor;
}

/* set content compress */
int hidoc_set_ccompress_status(HIDOC *hidoc, int status)
{
    if(hidoc && hidoc->state)
    {
        hidoc->state->ccompress_status = status;
        return 0;
    }
    return -1;
}

/* set phrase status */
int hidoc_set_phrase_status(HIDOC *hidoc, int status)
{
    if(hidoc && hidoc->state)
    {
        hidoc->state->phrase_status = status;
        return 0;
    }
    return -1;
}

#define CHECK_BSTERMIO(hidoc, xid)                                                          \
do                                                                                          \
{                                                                                           \
    if((off_t)xid*(off_t)sizeof(BSTERM) >= hidoc->bstermio.end)                             \
    {                                                                                       \
        hidoc->bstermio.old = hidoc->bstermio.end;                                          \
        hidoc->bstermio.end = (xid / HI_BSTERM_BASE)+1;                                     \
        hidoc->bstermio.end *= (off_t)(sizeof(BSTERM) * (off_t)HI_BSTERM_BASE);             \
        if(ftruncate(hidoc->bstermio.fd, hidoc->bstermio.end) != 0)break;                   \
        memset(hidoc->bstermio.map + hidoc->bstermio.old, 0,                                \
                hidoc->bstermio.end - hidoc->bstermio.old);                                 \
    }                                                                                       \
}while(0)

#define CHECK_PACKETIO(hidoc)                                                               \
do                                                                                          \
{                                                                                           \
    if((off_t)hidoc->state->packettotal*(off_t)sizeof(IPACKET) >= hidoc->packetio.end)      \
    {                                                                                       \
        hidoc->packetio.old = hidoc->packetio.end;                                          \
        hidoc->packetio.end = (hidoc->state->packettotal / HI_PACKET_BASE)+1;               \
        hidoc->packetio.end *= (off_t)(sizeof(IPACKET) * (off_t)HI_PACKET_BASE);            \
        if(ftruncate(hidoc->packetio.fd, hidoc->packetio.end) != 0)break;                   \
        memset(hidoc->packetio.map + hidoc->packetio.old, 0,                                \
                hidoc->packetio.end - hidoc->packetio.old);                                 \
    }                                                                                       \
}while(0)

#define CHECK_XINDEXIO(hidoc)                                                               \
do                                                                                          \
{                                                                                           \
    if(((off_t)hidoc->state->xindextotal*(off_t)sizeof(XINDEX)) >= hidoc->xindexio.end)     \
    {                                                                                       \
        hidoc->xindexio.old = hidoc->xindexio.end;                                          \
        hidoc->xindexio.end = (hidoc->state->xindextotal / HI_XINDEX_BASE)+1;               \
        hidoc->xindexio.end *= (off_t)sizeof(XINDEX) * (off_t)HI_XINDEX_BASE;               \
        if(ftruncate(hidoc->xindexio.fd, hidoc->xindexio.end) != 0)break;                   \
        memset(hidoc->xindexio.map+hidoc->xindexio.old, 0,                                  \
                hidoc->xindexio.end - hidoc->xindexio.old);                                 \
    }                                                                                       \
}while(0)

#define CHECK_XINTIO(hidoc)                                                             \
do                                                                                      \
{                                                                                       \
    if(hidoc->state->int_index_count > 0 && ((off_t)hidoc->state->xindextotal           \
                * (off_t)hidoc->state->int_index_count * (off_t)sizeof(int))            \
            >= hidoc->xintio.end)                                                       \
    {                                                                                   \
        hidoc->xintio.old = hidoc->xintio.end;                                          \
        hidoc->xintio.end = (hidoc->state->xindextotal / HI_XINDEX_BASE)+1;             \
        hidoc->xintio.end *= (off_t)(sizeof(int)                                        \
                * (off_t)hidoc->state->int_index_count                                  \
                * HI_XINDEX_BASE);                                                      \
        if(ftruncate(hidoc->xintio.fd, hidoc->xintio.end) != 0)break;                   \
        memset(hidoc->xintio.map + hidoc->xintio.old, 0,                                \
                hidoc->xintio.end - hidoc->xintio.old);                                 \
    }                                                                                   \
}while(0)

#define CHECK_XLONGIO(hidoc)                                                            \
do                                                                                      \
{                                                                                       \
    if(hidoc->state->long_index_count > 0 && ((off_t)hidoc->state->xindextotal          \
                * (off_t)hidoc->state->long_index_count * (off_t)sizeof(int64_t))       \
            >= hidoc->xlongio.end)                                                      \
    {                                                                                   \
        hidoc->xlongio.old = hidoc->xlongio.end;                                        \
        hidoc->xlongio.end = (hidoc->state->xindextotal / HI_XINDEX_BASE)+1;            \
        hidoc->xlongio.end *= (off_t)(sizeof(int64_t)                                   \
                * (off_t)hidoc->state->long_index_count                                 \
                * HI_XINDEX_BASE);                                                      \
        if(ftruncate(hidoc->xlongio.fd, hidoc->xlongio.end) != 0)break;                 \
        memset(hidoc->xlongio.map + hidoc->xlongio.old, 0,                              \
                hidoc->xlongio.end - hidoc->xlongio.old);                               \
    }                                                                                   \
}while(0)

#define CHECK_XDOUBLEIO(hidoc)                                                          \
do                                                                                      \
{                                                                                       \
    if(hidoc->state->double_index_count > 0 && ((off_t)hidoc->state->xindextotal        \
                * (off_t)hidoc->state->double_index_count * (off_t)sizeof(double))      \
            >= hidoc->xdoubleio.end)                                                    \
    {                                                                                   \
        hidoc->xdoubleio.old = hidoc->xdoubleio.end;                                    \
        hidoc->xdoubleio.end = (hidoc->state->xindextotal / HI_XINDEX_BASE)+1;          \
        hidoc->xdoubleio.end *= (off_t)(sizeof(double)                                  \
                * (off_t)hidoc->state->double_index_count                               \
                * HI_XINDEX_BASE);                                                      \
        if(ftruncate(hidoc->xdoubleio.fd, hidoc->xdoubleio.end) != 0)break;             \
        memset(hidoc->xdoubleio.map + hidoc->xdoubleio.old, 0,                          \
                hidoc->xdoubleio.end - hidoc->xdoubleio.old);                           \
    }                                                                                   \
}while(0)
/* set synonym terms status */
int hidoc_set_bterm(HIDOC *hidoc, char *term, int nterm, int status)
{
    int ret = -1, termid = 0, xid = 0, n = 0;
    char line[HI_LINE_SIZE];
    BSTERM *bsterms = NULL;

    if(hidoc && term && nterm > 0 
            && (termid = mmtrie_xadd((MMTRIE *)(hidoc->xdict), term, nterm)) > 0
            &&  (n = sprintf(line, "b:%d", termid)) > 0
            && (xid = mmtrie_xadd((MMTRIE *)(hidoc->map), line, n)) > 0
            && (bsterms = (BSTERM *)(hidoc->bstermio.map)))
    {
        MUTEX_LOCK(hidoc->mutex);
        CHECK_BSTERMIO(hidoc, xid);
        if(xid > hidoc->state->bterm_id_max) hidoc->state->bterm_id_max = xid;
        if(nterm > HI_TERM_SIZE)
        {
            WARN_LOGGER(hidoc->logger, "term:%.*s too long than len:%d", nterm, term, HI_TERM_SIZE);
        }
        else
        {
            bsterms[xid].bterm.id = termid;
            bsterms[xid].bterm.status = status;
            bsterms[xid].bterm.len = nterm;
            memcpy(bsterms[xid].term, term, nterm);
            ret = 0;
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;
}

/* update bterms status */
int hidoc_update_bterm(HIDOC *hidoc, int termid, int status)
{
    BSTERM *bsterms = NULL;
    int ret = -1;

    if(hidoc && termid > 0 && (bsterms = (BSTERM *)(hidoc->bstermio.map)))
    {
        MUTEX_LOCK(hidoc->mutex);
        if(termid <= hidoc->state->bterm_id_max)
        {
            bsterms[termid].bterm.status = status;
            ret = 0;
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;
}

/* delete bterm */
int hidoc_del_bterm(HIDOC *hidoc, int termid)
{
    BSTERM *bsterms = NULL;
    int ret = -1;

    if(hidoc && termid > 0 && (bsterms = (BSTERM *)(hidoc->bstermio.map)))
    {
        MUTEX_LOCK(hidoc->mutex);
        if(termid <= hidoc->state->bterm_id_max)
        {
            bsterms[termid].bterm.status = 0;
            ret = 0;
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;
}

/* list bterms */
int hidoc_list_bterms(HIDOC *hidoc, char *out)
{
    char *p = NULL, *pp = NULL;
    BSTERM *bsterms = NULL;
    int i = 0, ret = 0;

    if(hidoc && (p = out) && (bsterms = (BSTERM *)(hidoc->bstermio.map)))
    {
        MUTEX_LOCK(hidoc->mutex);
        if(hidoc->state->bterm_id_max > 0)
        {
            p += sprintf(p, "({\"bterms\":{");
            pp = p;
            for(i = 1; i <= hidoc->state->bterm_id_max; i++)
            {
                if(bsterms[i].bterm.status > 0)
                    p += sprintf(p, "\"%d\":{\"id\":\"%d\", \"status\":\"%d\", \"text\":\"%s\"},", i, bsterms[i].bterm.id, bsterms[i].bterm.status, bsterms[i].term);
            }
            if(p > pp)--p;
            p += sprintf(p, "}})");
            ret = p - out;
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;
}

/* add block terms */
int hidoc_add_bterm(HIDOC *hidoc, char *term, int nterm)
{
    int ret = -1, termid = 0, xid = 0, n = 0;
    char line[HI_LINE_SIZE];
    BSTERM *bsterms = NULL;

    if(hidoc && term && nterm > 0 
            && (termid = mmtrie_xadd((MMTRIE *)(hidoc->xdict), term, nterm)) > 0
            &&  (n = sprintf(line, "b:%d", termid)) > 0
            && (xid = mmtrie_xadd((MMTRIE *)(hidoc->map), line, n)) > 0
            && (bsterms = (BSTERM *)(hidoc->bstermio.map)))
    {
        MUTEX_LOCK(hidoc->mutex);
        CHECK_BSTERMIO(hidoc, xid);
        if(xid > hidoc->state->bterm_id_max) hidoc->state->bterm_id_max = xid;
        if(nterm > HI_TERM_SIZE)
        {
            WARN_LOGGER(hidoc->logger, "term:%.*s too long than len:%d", nterm, term, HI_TERM_SIZE);
        }
        else
        {
            bsterms[xid].bterm.id = termid;
            bsterms[xid].bterm.status = IB_BTERM_BLOCK;
            bsterms[xid].bterm.len = nterm;
            memcpy(bsterms[xid].term, term, nterm);
            ret = 0;
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;
}

/* update bterm */
int hidoc_sync_bterms(HIDOC *hidoc)
{
    struct timeval tv = {0};
    int ret = -1;

    if(hidoc && hidoc->state)
    {
        MUTEX_LOCK(hidoc->mutex);
        gettimeofday(&tv, NULL);
        hidoc->state->bterm_mod_time = (off_t)tv.tv_sec * (off_t)10000000 + (off_t)tv.tv_usec;
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;
}

/* read bterm */
int hidoc_read_bterms(HIDOC *hidoc, int taskid, char *data, int ndata)
{
    int k = 0, nodeid = -1, n = -1, i = 0, left = 0, ret = -1;
    BSTERM *bsterms = NULL;
    HINODE *nodes = NULL;
    HITASK *tasks = NULL;
    char *p = NULL;

    if(hidoc && hidoc->state && (p = data) && (left = ndata) > 0 && hidoc->state->bterm_id_max > 0
            && taskid > 0 && taskid < (HI_NODE_MAX * HI_TASKS_MAX))
    {
        MUTEX_LOCK(hidoc->mutex);
        k = taskid % HI_TASKS_MAX;
        if((nodeid = (taskid / HI_TASKS_MAX)) < HI_NODE_MAX 
                && (nodes = hidoc->state->nodes) && (tasks = nodes[nodeid].tasks) 
                && tasks[k].status > 0 && tasks[k].bterm_mod_time < hidoc->state->bterm_mod_time
                && (bsterms = (BSTERM *)(hidoc->bstermio.map)))
        {
            for(i = 0; i <= hidoc->state->bterm_id_max; i++)
            {
                if(bsterms[i].bterm.len > 0)
                {
                    n = bsterms[i].bterm.len + sizeof(BTERM);
                    if(left < n)
                    {
                        WARN_LOGGER(hidoc->logger, "Nospace bsterms[%d] taskid:%d", i, taskid);
                        goto err;
                    }
                    else
                    {
                        memcpy(p, &(bsterms[i]), n);
                        p += n;
                        left -= n;
                    }
                }
                else
                {
                        WARN_LOGGER(hidoc->logger, "Nocontent bsterms[%d] taskid:%d", i, taskid);
                }
            }
            ret =  p - data;
            tasks[k].bterm_last_time = hidoc->state->bterm_mod_time;
        }
err:
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;
}

/* over update bterm */
int hidoc_over_bterms(HIDOC *hidoc, int taskid)
{
    int k = 0, nodeid = -1, ret = -1;
    HINODE *nodes = NULL;
    HITASK *tasks = NULL;

    if(hidoc && hidoc->state && taskid > 0 && taskid < (HI_NODE_MAX * HI_TASKS_MAX))
    {
        MUTEX_LOCK(hidoc->mutex);
        k = taskid % HI_TASKS_MAX;
        if((nodeid = (taskid / HI_TASKS_MAX)) < HI_NODE_MAX 
                && (nodes = hidoc->state->nodes) && (tasks = nodes[nodeid].tasks) 
                && tasks[k].status > 0)
        {
            tasks[k].bterm_mod_time = tasks[k].bterm_last_time;
            ret = 0;
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;
}
/* set block terms status */
int hidoc_set_bterm(HIDOC *hidoc, char *term, int nterm, int status)
{
    int ret = -1, termid = 0, xid = 0, n = 0;
    char line[HI_LINE_SIZE];
    BSTERM *bsterms = NULL;

    if(hidoc && term && nterm > 0 
            && (termid = mmtrie_xadd((MMTRIE *)(hidoc->xdict), term, nterm)) > 0
            &&  (n = sprintf(line, "b:%d", termid)) > 0
            && (xid = mmtrie_xadd((MMTRIE *)(hidoc->map), line, n)) > 0
            && (bsterms = (BSTERM *)(hidoc->bstermio.map)))
    {
        MUTEX_LOCK(hidoc->mutex);
        CHECK_BSTERMIO(hidoc, xid);
        if(xid > hidoc->state->bterm_id_max) hidoc->state->bterm_id_max = xid;
        if(nterm > HI_TERM_SIZE)
        {
            WARN_LOGGER(hidoc->logger, "term:%.*s too long than len:%d", nterm, term, HI_TERM_SIZE);
        }
        else
        {
            bsterms[xid].bterm.id = termid;
            bsterms[xid].bterm.status = status;
            bsterms[xid].bterm.len = nterm;
            memcpy(bsterms[xid].term, term, nterm);
            ret = 0;
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;
}

/* update bterms status */
int hidoc_update_bterm(HIDOC *hidoc, int termid, int status)
{
    BSTERM *bsterms = NULL;
    int ret = -1;

    if(hidoc && termid > 0 && (bsterms = (BSTERM *)(hidoc->bstermio.map)))
    {
        MUTEX_LOCK(hidoc->mutex);
        if(termid <= hidoc->state->bterm_id_max)
        {
            bsterms[termid].bterm.status = status;
            ret = 0;
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;
}

/* delete bterm */
int hidoc_del_bterm(HIDOC *hidoc, int termid)
{
    BSTERM *bsterms = NULL;
    int ret = -1;

    if(hidoc && termid > 0 && (bsterms = (BSTERM *)(hidoc->bstermio.map)))
    {
        MUTEX_LOCK(hidoc->mutex);
        if(termid <= hidoc->state->bterm_id_max)
        {
            bsterms[termid].bterm.status = 0;
            ret = 0;
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;
}

/* list bterms */
int hidoc_list_bterms(HIDOC *hidoc, char *out)
{
    char *p = NULL, *pp = NULL;
    BSTERM *bsterms = NULL;
    int i = 0, ret = 0;

    if(hidoc && (p = out) && (bsterms = (BSTERM *)(hidoc->bstermio.map)))
    {
        MUTEX_LOCK(hidoc->mutex);
        if(hidoc->state->bterm_id_max > 0)
        {
            p += sprintf(p, "({\"bterms\":{");
            pp = p;
            for(i = 1; i <= hidoc->state->bterm_id_max; i++)
            {
                if(bsterms[i].bterm.status > 0)
                    p += sprintf(p, "\"%d\":{\"id\":\"%d\", \"status\":\"%d\", \"text\":\"%s\"},", i, bsterms[i].bterm.id, bsterms[i].bterm.status, bsterms[i].term);
            }
            if(p > pp)--p;
            p += sprintf(p, "}})");
            ret = p - out;
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;
}

/* add block terms */
int hidoc_add_bterm(HIDOC *hidoc, char *term, int nterm)
{
    int ret = -1, termid = 0, xid = 0, n = 0;
    char line[HI_LINE_SIZE];
    BSTERM *bsterms = NULL;

    if(hidoc && term && nterm > 0 
            && (termid = mmtrie_xadd((MMTRIE *)(hidoc->xdict), term, nterm)) > 0
            &&  (n = sprintf(line, "b:%d", termid)) > 0
            && (xid = mmtrie_xadd((MMTRIE *)(hidoc->map), line, n)) > 0
            && (bsterms = (BSTERM *)(hidoc->bstermio.map)))
    {
        MUTEX_LOCK(hidoc->mutex);
        CHECK_BSTERMIO(hidoc, xid);
        if(xid > hidoc->state->bterm_id_max) hidoc->state->bterm_id_max = xid;
        if(nterm > HI_TERM_SIZE)
        {
            WARN_LOGGER(hidoc->logger, "term:%.*s too long than len:%d", nterm, term, HI_TERM_SIZE);
        }
        else
        {
            bsterms[xid].bterm.id = termid;
            bsterms[xid].bterm.status = IB_BTERM_BLOCK;
            bsterms[xid].bterm.len = nterm;
            memcpy(bsterms[xid].term, term, nterm);
            ret = 0;
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;
}

/* update bterm */
int hidoc_sync_bterms(HIDOC *hidoc)
{
    struct timeval tv = {0};
    int ret = -1;

    if(hidoc && hidoc->state)
    {
        MUTEX_LOCK(hidoc->mutex);
        gettimeofday(&tv, NULL);
        hidoc->state->bterm_mod_time = (off_t)tv.tv_sec * (off_t)10000000 + (off_t)tv.tv_usec;
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;
}

/* read bterm */
int hidoc_read_bterms(HIDOC *hidoc, int taskid, char *data, int ndata)
{
    int k = 0, nodeid = -1, n = -1, i = 0, left = 0, ret = -1;
    BSTERM *bsterms = NULL;
    HINODE *nodes = NULL;
    HITASK *tasks = NULL;
    char *p = NULL;

    if(hidoc && hidoc->state && (p = data) && (left = ndata) > 0 && hidoc->state->bterm_id_max > 0
            && taskid > 0 && taskid < (HI_NODE_MAX * HI_TASKS_MAX))
    {
        MUTEX_LOCK(hidoc->mutex);
        k = taskid % HI_TASKS_MAX;
        if((nodeid = (taskid / HI_TASKS_MAX)) < HI_NODE_MAX 
                && (nodes = hidoc->state->nodes) && (tasks = nodes[nodeid].tasks) 
                && tasks[k].status > 0 && tasks[k].bterm_mod_time < hidoc->state->bterm_mod_time
                && (bsterms = (BSTERM *)(hidoc->bstermio.map)))
        {
            for(i = 0; i <= hidoc->state->bterm_id_max; i++)
            {
                if(bsterms[i].bterm.len > 0)
                {
                    n = bsterms[i].bterm.len + sizeof(BTERM);
                    if(left < n)
                    {
                        WARN_LOGGER(hidoc->logger, "Nospace bsterms[%d] taskid:%d", i, taskid);
                        goto err;
                    }
                    else
                    {
                        memcpy(p, &(bsterms[i]), n);
                        p += n;
                        left -= n;
                    }
                }
                else
                {
                        WARN_LOGGER(hidoc->logger, "Nocontent bsterms[%d] taskid:%d", i, taskid);
                }
            }
            ret =  p - data;
            tasks[k].bterm_last_time = hidoc->state->bterm_mod_time;
        }
err:
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;
}

/* over update bterm */
int hidoc_over_bterms(HIDOC *hidoc, int taskid)
{
    int k = 0, nodeid = -1, ret = -1;
    HINODE *nodes = NULL;
    HITASK *tasks = NULL;

    if(hidoc && hidoc->state && taskid > 0 && taskid < (HI_NODE_MAX * HI_TASKS_MAX))
    {
        MUTEX_LOCK(hidoc->mutex);
        k = taskid % HI_TASKS_MAX;
        if((nodeid = (taskid / HI_TASKS_MAX)) < HI_NODE_MAX 
                && (nodes = hidoc->state->nodes) && (tasks = nodes[nodeid].tasks) 
                && tasks[k].status > 0)
        {
            tasks[k].bterm_mod_time = tasks[k].bterm_last_time;
            ret = 0;
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;
}

/* check id */
int hidoc_checkid(HIDOC *hidoc, int64_t globalid)
{
    char line[HI_LINE_SIZE];
    int n = 0, mid = -1;

    if(hidoc && globalid && (n = sprintf(line, "%llu", LLU(globalid))) > 0)
    {
        mid = mmtrie_get(hidoc->kmap, line, n);
    }
    return mid;
}

/* mid */
int hidoc_mid(HIDOC *hidoc, int64_t globalid)
{
    char line[HI_LINE_SIZE];
    int n = 0, mid = -1;

    if(hidoc && globalid && (n = sprintf(line, "%llu", LLU(globalid))) > 0)
    {
        mid = mmtrie_xadd(hidoc->kmap, line, n);
    }
    return mid;
}

/* add document */
int hidoc_push_index(HIDOC *hidoc, IFIELD *fields, int flag, IBDATA *block)
{
    //int mid = 0, i = 0, id = 0, x = 0, newid = 0, ret = -1; *int_index = NULL, *intidx = NULL;
    int mid = 0, newid = 0, ret = -1;
    //double *double_index = NULL, *doubleidx = NULL;
    //int64_t *long_index = NULL, *longidx = NULL;
    DOCHEADER *docheader = NULL;
    XINDEX *xindexs = NULL;
    //char *p = NULL;

    if(hidoc && block->ndata > 0 && (docheader = (DOCHEADER *)block->data) && fields)
    {
        MUTEX_LOCK(hidoc->mutex);
        if(docheader->size == block->ndata && (xindexs = (XINDEX *)(hidoc->xindexio.map))
                && (mid = hidoc_mid(hidoc, docheader->globalid)) > 0)
        {
            if(mid >= hidoc->state->xindextotal){hidoc->state->xindextotal = mid+1;newid = mid;}
            ACCESS_LOGGER(hidoc->logger, "globalid:%lld id:%d total:%d", docheader->globalid, mid, hidoc->state->xindextotal);
            CHECK_XINDEXIO(hidoc);
            //CHECK_XINTIO(hidoc);
            //CHECK_XLONGIO(hidoc);
            //CHECK_XDOUBLEIO(hidoc);
            //if(hidoc->state->xindextotal <= 0) hidoc->state->xindextotal = 1;
            if(newid)
            {
                xindexs[mid].status = docheader->status;
                xindexs[mid].globalid = docheader->globalid;
                xindexs[mid].crc = docheader->crc;
                xindexs[mid].slevel = docheader->slevel;
                xindexs[mid].rank = docheader->rank;
            }
            else
            {
                xindexs[mid].globalid = docheader->globalid;
                xindexs[mid].status = docheader->status;
                xindexs[mid].slevel = docheader->slevel;
                if(flag & IB_RANK_SET) xindexs[mid].rank = docheader->rank;
                if(xindexs[mid].crc != docheader->crc)
                    hidoc_update(hidoc, mid, 1);
                else
                    hidoc_update(hidoc, mid, 0);
            }
            if(xindexs[mid].status < 0)
            {
                ACCESS_LOGGER(hidoc->logger, "update-index{gloablid:%lld mid:%d rank:%f status:%d}", LL64(docheader->globalid), mid, xindexs[mid].rank, docheader->status);
            }
            else
            {
                ACCESS_LOGGER(hidoc->logger, "update-index{gloablid:%lld mid:%d rank:%f}", LL64(docheader->globalid), mid, xindexs[mid].rank);
            }
            if(flag & IB_CATBIT_SET)
            {
                xindexs[mid].category |= docheader->category;
            }
            else if(flag & IB_CATBIT_UNSET)
            {
                xindexs[mid].category &= ~(docheader->category);
            }
            else
            {
                xindexs[mid].category = docheader->category;
            }
            /*
            p = (char *)docheader;
            if(docheader->intblock_size > 0 && hidoc->state->int_index_count > 0 && (x = hidoc->state->int_index_from) >= 0 
                    && docheader->intblock_off > 0 && (int_index = (int *)hidoc->xintio.map))
            {
                id = mid * hidoc->state->int_index_count;
                intidx = (int *)(p + docheader->intblock_off);
                for(i = 0; i < hidoc->state->int_index_count; i++)
                {
                    if(hidoc->state->need_update_numbric || (fields[x+i].flag & IB_IS_NEED_UPDATE))
                    {
                        int_index[id+i] = intidx[i];
                    }
                }
            }
            if(docheader->longblock_size > 0 && hidoc->state->long_index_count > 0 && (x = hidoc->state->long_index_from) >= 0 
                    && docheader->longblock_off > 0 && (long_index = (int64_t *)hidoc->xlongio.map))
            {
                id = mid * hidoc->state->long_index_count;
                longidx = (int64_t *)(p + docheader->longblock_off);
                for(i = 0; i < hidoc->state->long_index_count; i++)
                {
                    if(hidoc->state->need_update_numbric || (fields[x+i].flag & IB_IS_NEED_UPDATE))
                    {
                        long_index[id+i] = longidx[i];
                    }
                }
                //memcpy(&(long_index[id]), p + docheader->longblock_off, docheader->longblock_size);
            }
            if(docheader->doubleblock_size > 0 && hidoc->state->double_index_count > 0 && (x = hidoc->state->double_index_from) >= 0 
                    && docheader->doubleblock_off > 0 && (double_index = (double *)hidoc->xdoubleio.map))
            {
                id = mid * hidoc->state->double_index_count;
                doubleidx = (double *)(p + docheader->doubleblock_off);
                for(i = 0; i < hidoc->state->double_index_count; i++)
                {
                    if(hidoc->state->need_update_numbric || (fields[x+i].flag & IB_IS_NEED_UPDATE))
                    {
                        double_index[id+i] = doubleidx[i];
                    }
                }
                //memcpy(&(double_index[id]), p + docheader->doubleblock_off, docheader->doubleblock_size);
            }
            */
            db_set_data(PDB(hidoc->db), mid, block->data, block->ndata);
            ret = 0;
        }
        else
        {
            FATAL_LOGGER(hidoc->logger, "new-index{gloablid:%lld} total:%d failed, %s", LL64(docheader->globalid), hidoc->state->xindextotal, strerror(errno));
            _exit(-1);
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;
}

/* read index */
int hidoc_read_index(HIDOC *hidoc, int taskid, char *data, int *len, int *count)
{
    int id = -1, nodeid = 0, x = 0, k = 0, *px = NULL, 
        left = 0, last = 0, n = 0;//, *int_index = 0, z = 0;
    DOCHEADER *docheader = NULL;
    //int64_t *long_index = NULL;
    //double *double_index = NULL;
    XINDEX *xindexs = NULL;
    HITASK *tasks = NULL;
    HINODE *nodes = NULL;
    char *p = NULL;

    if(hidoc && hidoc->state && taskid >= 0 && taskid < (HI_NODE_MAX * HI_TASKS_MAX)
            && data && len && *len > 0 && count) 
    {
        MUTEX_LOCK(hidoc->mutex);
        *count = 0;
        k = taskid % HI_TASKS_MAX;
        if((nodeid = (taskid / HI_TASKS_MAX)) < HI_NODE_MAX 
                && (xindexs = (XINDEX *)(hidoc->xindexio.map))
                && (nodes = hidoc->state->nodes) && (tasks = nodes[nodeid].tasks) 
                && tasks[k].status > 0)
        {
            tasks[k].count = 0;
            //check limit 
            if(nodes[nodeid].limit > 0  && tasks[k].nxindexs >= nodes[nodeid].limit) goto end;
            //check last over
            if(tasks[k].popid != tasks[k].over) 
            {
                //fprintf(stdout, "%s::%d id:%d over:%d\n", __FILE__, __LINE__, tasks[k].popid, tasks[k].over);
                tasks[k].popid = tasks[k].last;
            }
            else 
                tasks[k].last = tasks[k].popid;
            p = data;
            left = *len;
            while(left > HI_LEFT_LEN)
            {
                last = tasks[k].popid;
                if(nodes[nodeid].type == HI_NODE_DOCD || nodes[nodeid].type == HI_NODE_PARSERD)
                {
                    //if(tasks[k].popid == 0) tasks[k].popid = 1;
                    if((tasks[k].popid+1) < hidoc->state->xindextotal)
                    {
                        if(tasks[k].popid > 0) id = ++(tasks[k].popid);
                        else id = tasks[k].popid = 1;
                        if(id > hidoc->state->docpopid) 
                            hidoc->state->docpopid = id;
                        if(id > nodes[nodeid].total) 
                            nodes[nodeid].total = id;
                    }
                    else 
                    {
                        id = -1;
                        break;
                    }
                }
                else if(nodes[nodeid].type == HI_NODE_INDEXD)
                {
                    if((x = tasks[k].popid) == nodes[nodeid].last)
                    {
                        if(hidoc->state->popid == 0) hidoc->state->popid = 1;
                        if((id = hidoc->state->popid) > 0 && id <= hidoc->state->docpopid)
                        {
                            if(nodes[nodeid].total == 0) 
                            {
                                nodes[nodeid].last = nodes[nodeid].first = id;
                            }
                            else
                            {
                                xindexs[x].next = id;
                                xindexs[id].prev = x;
                            }
                            xindexs[id].nodeid = nodeid;
                            //xindexs[id].status = 1;
                            tasks[k].popid = id;
                            nodes[nodeid].last = id;
                            nodes[nodeid].total++;
                            hidoc->state->popid++;
                        }
                        else id = -1;
                    }
                    else
                    {
                        if(nodes[nodeid].total > 0 && tasks[k].popid < nodes[nodeid].last)
                        {
                            if(tasks[k].nxindexs == 0 && tasks[k].count == 0)
                            {
                                tasks[k].popid = id = nodes[nodeid].first;
                            }
                            else
                            {
                                x = tasks[k].popid;
                                id = xindexs[x].next;
                                tasks[k].popid = id;
                            }
                        }
                        else id = -1;
                    }
                }
                else id = -1;
                if(id > 0)
                {
                    if(nodes[nodeid].type == HI_NODE_INDEXD && xindexs[id].nodeid != nodeid)
                    {
                        FATAL_LOGGER(hidoc->logger, "Invalid xindex[%d].nodeid[%d] to task[%s:%d].nodeid:%d", id, xindexs[id].nodeid, tasks[k].ip, tasks[k].port, nodeid);
                        _exit(-1);
                    }
                    if((n = db_get_data_len(PDB(hidoc->db), id)) <= (left - HI_LEFT_LEN))
                    {
                        px = (int *)p;
                        p += sizeof(int);
                        docheader = (DOCHEADER *)p;
                        ACCESS_LOGGER(hidoc->logger, "id:%d globalid:%lld", id, xindexs[id].globalid);
                        if((n = db_read_data(PDB(hidoc->db), id, p)) > sizeof(DOCHEADER) 
                                && docheader->globalid == xindexs[id].globalid)
                        {
                            ACCESS_LOGGER(hidoc->logger, "globalid:%lld/%lld mid:%d total:%d size:%d", LL(docheader->globalid), LL(xindexs[id].globalid), id, hidoc->state->xindextotal, docheader->size);
                            if(docheader->size < 0 || docheader->size != n) 
                            {
                                FATAL_LOGGER(hidoc->logger, "Invalid data id:%d size:%d n:%d", id, docheader->size, n);
                                break;
                            }
                            docheader->status = xindexs[id].status;
                            docheader->slevel = xindexs[id].slevel;
                            docheader->category = xindexs[id].category;
                            docheader->rank = xindexs[id].rank;
                            //update int/double index
                            if(nodes[nodeid].type != HI_NODE_PARSERD)
                            {
                                /*
                                if(docheader->intblock_size > 0 && docheader->intblock_off >= 0
                                        && hidoc->state->int_index_count > 0
                                        && (int_index = (int *)(hidoc->xintio.map)))
                                {
                                    z = hidoc->state->int_index_count * id;
                                    memcpy(p+docheader->intblock_off, &(int_index[z]), docheader->intblock_size);
                                }
                                if(docheader->longblock_size > 0 && docheader->longblock_off >= 0
                                        && hidoc->state->long_index_count > 0
                                        && (long_index = (int64_t *)(hidoc->xlongio.map)))
                                {
                                    z = hidoc->state->long_index_count * id;
                                    memcpy(p+docheader->longblock_off, &(long_index[z]), docheader->longblock_size);
                                }
                                if(docheader->doubleblock_size > 0 && docheader->doubleblock_off >= 0
                                        && hidoc->state->double_index_count > 0
                                        && (double_index = (double *)(hidoc->xdoubleio.map)))
                                {
                                    z = hidoc->state->double_index_count * id;
                                    memcpy(p+docheader->doubleblock_off, &(double_index[z]), docheader->doubleblock_size);
                                }
                                */
                            }
                            //DEBUG_LOGGER(hidoc->logger, "read-index{status:%d node:%d type:%d task[%s:%d] gloablid:%d mid:%d rank:%f}", xindexs[id].status, nodeid, nodes[nodeid].type, tasks[k].ip, tasks[k].port, docheader->globalid, id, xindexs[id].rank);
                            *px = n;
                            left -= n + sizeof(int);
                            p += n;
                            tasks[k].count++;
                        }
                        else
                        {
                            p -= sizeof(int);
                            FATAL_LOGGER(hidoc->logger, "Invalid data id:%lld globalid:%lld docheader:%d", id, LL64(xindexs[id].globalid), LL64(docheader->globalid));
                            break;
                        }
                        last = tasks[k].popid;
                    }
                    else 
                    {
                        tasks[k].popid = last;
                        break;
                    }
                }
                else 
                {
                    break;
                }
            }
end:
            *len -= left;
            if((*count = tasks[k].count) > 0) 
            {
                id = tasks[k].popid = last ;
            }
            else id = -1;
        }
        else id = -2;
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return id;
}

/* over index */
int hidoc_over_index(HIDOC *hidoc, int taskid, int id)
{
    int ret = -1, k = 0, nodeid = -1;
    HITASK *tasks = NULL;
    if(hidoc && taskid >= 0 && taskid < HI_NODE_MAX * HI_TASKS_MAX 
            && hidoc->state && id > 0 && id < hidoc->state->xindextotal)
    {
        MUTEX_LOCK(hidoc->mutex);
        k = taskid % HI_TASKS_MAX;
        if((nodeid = (taskid / HI_TASKS_MAX)) < HI_NODE_MAX 
                && (tasks = hidoc->state->nodes[nodeid].tasks)
                && tasks[k].status > 0 && tasks[k].popid == id)
        {
            //fprintf(stdout, "%s::%d over_index(id:%d over:%d)\n", __FILE__, __LINE__, tasks[k].popid, tasks[k].over);
            tasks[k].over = id;
            tasks[k].nxindexs += tasks[k].count;
            ret = 0;
            //DEBUG_LOGGER(hidoc->logger, "taskid:%d id:%d popid:%d count:%d nindexs:%d node->total:%d indextotal:%d docpopid:%d", taskid, id, tasks[k].popid, tasks[k].count, tasks[k].nxindexs, hidoc->state->nodes[nodeid].total, hidoc->state->xindextotal, hidoc->state->docpopid);
            tasks[k].count = 0;
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;

}

/* pop document */
int hidoc_pop_document(HIDOC *hidoc, HINDEX *hindex, IBDATA *block)
{
    //DOCHEADER *docheader = NULL;

    if(hidoc && hindex && hidoc->state && block && hidoc->outdocfd > 0)
    {
	    MUTEX_LOCK(hidoc->mutex);
        //DEBUG_LOGGER(hidoc->logger, "ready for pop_document() offset:%lld", hidoc->state->outdoc_offset);
        if(lseek(hidoc->outdocfd,  hidoc->state->outdoc_offset, SEEK_SET) >= 0
            && read(hidoc->outdocfd, &(block->ndata), sizeof(int)) > 0
            && block->ndata > 0)
        {
            //DEBUG_LOGGER(hidoc->logger, "ready for pop_document() len:%d", block->ndata);
            hidoc->state->outdoc_offset += (off_t)sizeof(int);
            REALLOC(hindex->out, hindex->nout, block->ndata);
            if((block->data = hindex->out) 
                    &&  lseek(hidoc->outdocfd,  hidoc->state->outdoc_offset, SEEK_SET) >= 0
                    && read(hidoc->outdocfd, block->data, block->ndata) > 0)
            {
                //DOCHEADER *docheader = (DOCHEADER *)block->data;
                //DEBUG_LOGGER(hidoc->logger, "pop_document(%d) len:%d", docheader->docid, block->ndata);
                hidoc->state->outdoc_offset += (off_t)(block->ndata);
            }
            else
            {
                hidoc->state->outdoc_offset -= (off_t)sizeof(int);
            }
        }
	    MUTEX_UNLOCK(hidoc->mutex);
        return 0;
    }
    return -1;
}

/* parse HTML document */
int hidoc_parseHTML(HIDOC *hidoc, HINDEX *hindex, char *url, int date, char *content, int ncontent, IBDATA *block)
{
    int ret = -1, i = 0, nfields = 0, n = 0;
    IFIELD fields[IB_FIELDS_MAX];
    char sdate[IB_DATE_MAX];
    FHEADER fheader = {0};
    HTML *html = NULL;

    if(hidoc && hindex && url && content && ncontent > 0 && block)
    {
        //DEBUG_LOGGER(hidoc->logger, "ready for parsing HTML[%s]", url);
        if((html = html_init()))
        {
            html->get_content(html, content, ncontent, 1, 1);
            if(html->ncontent > 0) 
            {
                nfields = i = html->nfields;
                while(--i >= 0)
                {
                    if(html->fieldlist[i].len > 0 && html->fieldlist[i].from >= 0)
                    {
                        fields[i].flag |= (IB_IS_NEED_INDEX|IB_DATATYPE_TEXT);
                        fields[i].offset = html->fieldlist[i].from;
                        fields[i].length = html->fieldlist[i].len;
                    }
                }
                if((n = strlen(url)) > 0
                    && html->add_field(html, url, strlen(url)) == 0)
                {
                    i = nfields++;
                    fields[i].flag |= IB_DATATYPE_TEXT;
                    fields[i].offset = html->fieldlist[i].from;
                    fields[i].length = html->fieldlist[i].len;
                }
                if(((n = sprintf(sdate, "%u", (unsigned int)date))) > 0
                    && html->add_field(html, sdate, n) == 0)
                {
                    i = nfields++;
                    fields[i].flag = (IB_IS_NEED_INDEX|IB_DATATYPE_INT);
                    fields[i].offset = html->fieldlist[i].from;
                    fields[i].length = html->fieldlist[i].len;
                }
                if(fheader.status < 0)
                {
                    hidoc_set_idx_status(hidoc, (int64_t)fheader.globalid, fheader.status);
                }
                else
                {
                    ret = hidoc_genindex(hidoc, hindex, &fheader, fields, nfields, html->content, html->ncontent, block);
                }
            }
            else
            {
                ERROR_LOGGER(hidoc->logger, "parsed ncontent(%d) url(%s)", 
                        html->ncontent, url);
            }
            html->clean(&html);
        }
    }
    return ret;
}

/* segment */
int hidoc_segment(HIDOC *hidoc, HINDEX *hindex, char *base, char *start, char *end, int bit_fields)
{
    int termid = 0, nterm = 0, i = 0, x = 0, n = 0, old = 0, *pold = &old;
    char *ps = NULL, *s = NULL, *es = NULL;
    void *map = NULL;

    if(hidoc && hindex && (map = hindex->map))
    {
        s = start;
        es = end;
        while(s < es)
        {
            while(s < es && (*(UCHR(s))) < 0x80
                    && !ISCHAR(UCHR(s)) && !ISNUM(UCHR(s))) ++s;
            if((*(UCHR(s))) < 0x80)
            {
                ps = s;
                while(ps < es && (*(UCHR(ps))) < 0x80
                    && (ISSIGN(ps) || ISCHAR(UCHR(ps)) || ISNUM(UCHR(ps)))) ++ps;
                nterm = (ps - s);
                //termid = mmtrie_xadd((MMTRIE *)(hidoc->mmtrie), s, nterm);
            }
            else
            {
                n = es - s;
                termid = 0;nterm = 0;
                if(mmtrie_maxfind((MMTRIE *)(hidoc->mmtrie), s, n, &nterm) > 0)
                {
                    ps = s;
                }
            }
            if(ps && nterm > 0 && (termid = mmtrie_xadd((MMTRIE *)(hidoc->xdict), ps, nterm)) > 0)
            {
                if(MTREE_ADD(map, termid, hindex->nterms, pold) == 0)
                {
                    if(hindex->nterms < HI_TERMS_MAX)
                    {
                        i = hindex->nterms++;
                        memset(&(hindex->terms[i]), 0, sizeof(STERM));
                        memset(&(hindex->nodes[i]), 0, sizeof(TERMNODE));
                        hindex->nodes[i].termid = termid;
                        hindex->nodes[i].term_offset = s - base;
                        hindex->terms[i].termid = termid;
                        hindex->terms[i].term_len = nterm;
                        hindex->term_text_total += nterm;
                    }
                    else
                    {
                        WARN_LOGGER(hidoc->logger, "overflow term limit term:%.*s ", nterm, ps);
                        break;
                    }
                }
                else 
                {
                    i = (int)old;
                }
                if(i <= HI_TERMS_MAX && hindex->nodes[i].noffsets < HI_OFFSET_MAX)
                {
                    x = hindex->nodes[i].noffsets++;
                    hindex->nodes[i].offsets[x] = s - base;
                    hindex->terms[i].bit_fields |= bit_fields;
                    hindex->terms[i].term_count++;
                    hindex->term_offsets_total++;
                }
                else
                {
                    WARN_LOGGER(hidoc->logger, "term:%.*s offsets overflow", nterm, ps);
                }
                s += nterm;
            }
            else
            {
                if(((unsigned char )(*s)) >= 252) n = 6;
                else if(((unsigned char )(*s)) >= 248) n = 5;
                else if(((unsigned char )(*s)) >= 240) n = 4;
                else if(((unsigned char )(*s)) >= 224) n = 3;
                else if(((unsigned char )(*s)) >= 192) n = 2;
                else n = 1;
                s += n;
            }
        }
        return 0;
    }
    return -1;
}

/* rsegment */
int hidoc_rsegment(HIDOC *hidoc, HINDEX *hindex, char *base, char *start, char *end, int bit_fields)
{
    char *p = NULL, *ps = NULL, *s = NULL, *es = NULL, *ep = NULL;
    int termid = 0, nterm = 0, i = 0, x = 0, n = 0, last = -1, old = 0, *pold = &old;
    void *map = NULL;

    if(hidoc && hindex && (map = hindex->map) && start && end && start < end)
    {
        //fprintf(stdout, "%s::%d %s\n", __FILE__, __LINE__, start);
        s = start;
        es = end;
        --es;
        while(es >= s)
        {
            while(es >= s && (*(UCHR(es))) < 0x80
                    && !ISCHAR(UCHR(es)) && !ISNUM(UCHR(es))) --es;
            if(es < s)break;
            ps = NULL, termid = 0;nterm = 0;
            if(*(UCHR(es)) < 0x80)
            {
                ps = es;
                while(ps >= s && (*(UCHR(ps))) < 0x80
                    && (ISSIGN(ps) || ISCHAR(UCHR(ps)) || ISNUM(UCHR(ps)))) --ps;
                ++ps;
                nterm = (es - ps + 1);
                //termid = mmtrie_rxadd((MMTRIE *)(hidoc->mmtrie), ps, nterm);
                //fprintf(stdout, "%s::%d %d:%.*s\n", __FILE__, __LINE__, termid, nterm, ps);
            }
            else
            {
                n = es - s  + 1;
                if((mmtrie_rmaxfind((MMTRIE *)(hidoc->mmtrie), s, n, &nterm)) > 0)
                {
                    ps = s;
                }
                //fprintf(stdout, "%s::%d %d:%.*s\n", __FILE__, __LINE__, termid, nterm, (es + 1 - nterm));
            }
            if(ps && nterm > 0 && (termid = mmtrie_xadd((MMTRIE *)(hidoc->xdict), ps, nterm)) > 0)
            {
                es -= nterm;
                if((MTREE_ADD(map, termid, hindex->nterms, pold)) == 0)
                {
                    if(hindex->nterms < HI_TERMS_MAX)
                    {
                        i = hindex->nterms++;
                        memset(&(hindex->terms[i]), 0, sizeof(STERM));
                        memset(&(hindex->nodes[i]), 0, sizeof(TERMNODE));
                        hindex->nodes[i].termid = termid;
                        hindex->nodes[i].term_offset = es + 1 - base;
                        hindex->terms[i].termid = termid;
                        hindex->terms[i].term_len = nterm;
                        hindex->term_text_total += nterm;
                    }
                    else
                    {
                        WARN_LOGGER(hidoc->logger, "overflow term limit term:%.*s ", nterm, ps);
                        break;
                    }
                    //fprintf(stdout, "x:%d pos:%d %.*s\n", x, hindex->nodes[i].term_offset, hindex->terms[i].term_len, base + hindex->nodes[i].term_offset);
                }
                else 
                {
                    i = (int)old;
                }
                if(i < HI_TERMS_MAX && hindex->nodes[i].nroffsets < HI_OFFSET_MAX)
                {
                    x = hindex->nodes[i].nroffsets++;
                    hindex->nodes[i].roffsets[x] = es + 1 - base;
                    hindex->terms[i].bit_fields |= bit_fields;
                    hindex->terms[i].term_count++;
                    hindex->term_offsets_total++;
                }
                else
                {
                    WARN_LOGGER(hidoc->logger, "term:%.*s offset overflow termid:%d", nterm, ps, termid);
                }
                if(hidoc->state->phrase_status != HI_PHRASE_DISABLED)
                {
                    if(i >= 0 && i < HI_TERMS_MAX && ep && last >= 0 && last <= HI_TERMS_MAX && ep == s)
                    {
                        hindex->nodes[i].nexts[last] = 1; 
                        hindex->nodes[last].prevs[i] = 1; 
                    }
                    ep = es + nterm;
                    last = i;
                }
            }
            else
            {
                if((p = (es - 5)) >= s && ((unsigned char )(*p)) >= 252) n = 6;
                else if((p = (es - 4)) >= s && ((unsigned char )(*p)) >= 248) n = 5;
                else if((p = (es - 3)) >= s && ((unsigned char )(*p)) >= 240) n = 4;
                else if((p = (es - 2)) >= s && ((unsigned char )(*p)) >= 224) n = 3;
                else if((p = (es - 1)) >= s && ((unsigned char )(*p)) >= 192) n = 2;
                else n = 1;
                es -= n;
            }
        }
        return 0;
    }
    return -1;
}

#ifdef HAVE_SCWS
int scws_segment(HIDOC *hidoc, HINDEX *hindex, char *base, char *start, char *end, int bit_fields)
{
    int termid = 0, nterm = 0, i = 0, x = 0, last = -1, old = 0, *pold = &old;
    char line[HI_LINE_SIZE], *p = NULL, *pp = NULL, *epp = NULL, *s = NULL, *es = NULL, *ep = NULL;
    scws_res_t res = NULL, cur = NULL;
    scws_t segmentor = NULL;
    void *map = NULL;


    if(hidoc && hindex && (map = hindex->map) && start && end && start < end
            && (segmentor=(scws_t)hidoc_pop_segmentor(hidoc)))
    {
        s = start;
        es = end;
        //segmentor = (scws_t)(hidoc->segmentor);
        scws_send_text(segmentor, s, es - s);
        while ((res = cur = scws_get_result(segmentor)))
        {
            while (cur != NULL)
            {
                pp = s = start + cur->off;
                if(cur->attr && cur->attr[0] == 'u' && cur->attr[1] == 'n'
                    && !ISNUM(s) && !ISCHAR(s))
                {
                    cur = cur->next;
                    continue;
                }
                nterm = cur->len;
                epp = s + nterm;
                p = line; 
                while(pp < epp)
                {
                    if(*pp >= 'A' && *pp <= 'Z')
                        *p++ = *pp + 'a' - 'A';
                    else 
                        *p++ = *pp;
                    ++pp;
                }
                *p = '\0';
                if(nterm > 0 && (termid=mmtrie_xadd((MMTRIE *)(hidoc->xdict), line, nterm)) > 0)
                {
                    if((MTREE_ADD(map, termid, hindex->nterms, pold)) == 0)
                    {
                        if(hindex->nterms < HI_TERMS_MAX)
                        {
                            i = hindex->nterms++;
                            memset(&(hindex->terms[i]), 0, sizeof(STERM));
                            memset(&(hindex->nodes[i]), 0, sizeof(TERMNODE));
                            hindex->nodes[i].termid = termid;
                            hindex->nodes[i].term_offset = s - base;
                            hindex->terms[i].termid = termid;
                            hindex->terms[i].term_len = nterm;
                            hindex->term_text_total += nterm;
                        }
                        else
                        {

                            WARN_LOGGER(hidoc->logger, "overflow limit term:%s", line);
                            break;
                        }
                        //fprintf(stdout, "x:%d pos:%d %.*s\n", x, hindex->nodes[i].term_offset, hindex->terms[i].term_len, base + hindex->nodes[i].term_offset);
                    }
                    else 
                    {
                        i = (int)old;
                    }
                    if(i < HI_TERMS_MAX && hindex->nodes[i].noffsets < HI_OFFSET_MAX)
                    {
                        x = hindex->nodes[i].noffsets++;
                        hindex->nodes[i].offsets[x] = s - base;
                        hindex->terms[i].bit_fields |= bit_fields;
                        hindex->terms[i].term_count++;
                        hindex->term_offsets_total++;
                    }
                    else
                    {
                        WARN_LOGGER(hidoc->logger, "term:%s offset overflow termid:%d", line, termid);
                    }
                    if(hidoc->state->phrase_status != HI_PHRASE_DISABLED)
                    {
                        if(i >= 0 && i < HI_TERMS_MAX && ep && last >= 0 && last <= HI_TERMS_MAX && ep == s)
                        {
                            hindex->nodes[i].prevs[last] = 1; 
                            hindex->nodes[last].nexts[i] = 1; 
                        }
                        ep = s + nterm;
                        last = i;
                    }
                }
                else
                {
                    WARN_LOGGER(hidoc->logger, "add term:%s failed, %s", line, strerror(errno));
                }
                cur = cur->next;
            }
            scws_free_result(res);
        }
        hidoc_push_segmentor(hidoc, segmentor);
    }
    return 0;
}
#endif
#define HINDEX_RESET(xo)                                                                \
do                                                                                      \
{                                                                                       \
    xo->nterms = 0;                                                                     \
    xo->term_text_total = 0;                                                            \
    xo->term_offsets_total = 0;                                                         \
}while(0)

/* parse document */
int hidoc_genindex(HIDOC *hidoc, HINDEX *hindex, FHEADER *fheader, IFIELD *fields, int nfields, 
        char *content, int ncontent, IBDATA *block)
{
    int ret = -1,  i = 0, j = 0, x = 0, last = 0, to = 0, n = 0, mm = 0, 
        *np = NULL, index_int_from = -1, index_long_from = -1, index_double_from = -1, 
        index_int_num = 0, index_long_num = 0, index_double_num = 0, index_text_num = 0;
    char *s = NULL, *es = NULL, *p = NULL, *pp = NULL, *ps = NULL;
    STERM *termlist = NULL, *sterm = NULL;
    DOCHEADER *docheader = NULL;
    int64_t nl = 0, *npl = NULL;
    XFIELD *xfields = NULL;
    size_t nzcontent = 0;
    void *timer = NULL;
    double *npf = NULL;

    if(hidoc && hindex && fheader && fields && nfields  > 0 && content && ncontent > 0 
            && block && hindex->map)
    {
        TIMER_INIT(timer);
        HINDEX_RESET(hindex);
        MTREE_RESET(hindex->map);
        TIMER_SAMPLE(timer);
        if(ncontent > HI_TXTDOC_MAX)
        {
            WARN_LOGGER(hidoc->logger, "document:%lld length:%d too large", LLU(fheader->globalid), ncontent);
            //fprintf(stdout, "%s\r\n", content);
            return -1;
        }
        to = nfields;
        if(nfields > IB_FIELDS_MAX) to = IB_FIELDS_MAX;
        i = 0;
        while(i < to)
        {
            s = content + fields[i].offset;
            es = s + fields[i].length;
            if((fields[i].flag & IB_IS_NEED_INDEX))
            {
                if(fields[i].flag & IB_DATATYPE_TEXT)
                {
                    n = 1 << i;
#ifdef HAVE_SCWS
                    scws_segment(hidoc, hindex, content, s, es, n);
                    index_text_num++;
#endif
                }
                else if(fields[i].flag & IB_DATATYPE_INT)
                {
                    if(index_int_from < 0) index_int_from = i;
                    index_int_num++;
                }
                else if(fields[i].flag & IB_DATATYPE_LONG)
                {
                    if(index_long_from < 0) index_long_from = i;
                    index_long_num++;
                }
                else if(fields[i].flag & IB_DATATYPE_DOUBLE)
                {
                    if(index_double_from < 0) index_double_from = i;
                    index_double_num++;
                }
            }
            ++i;
        }
        //rsegment 
#ifndef HAVE_SCWS
        i = to - 1;
        while(i >= 0)
        {
            s = content + fields[i].offset;
            es = s + fields[i].length;
            if((fields[i].flag & IB_IS_NEED_INDEX))
            {
                if(fields[i].flag & IB_DATATYPE_TEXT)
                {
                    n = 1 << i;
                    hidoc_rsegment(hidoc, hindex, content, s, es, n);
                }
            }
            --i;
        }
#endif
        TIMER_SAMPLE(timer);
        DEBUG_LOGGER(hidoc->logger, "segment content(%d) time used:%lld", ncontent, PT_LU_USEC(timer));
        if(hindex->nterms == 0) 
        {
            /*
            int x = 0 ;
            for(x = 0; x < nfields; x++)
            {
                fprintf(stdout, "%d:{%.*s}\n", x, fields[x].length, content + fields[x].offset);
            }
            fprintf(stdout, "segment content{%s} len:%d result:0 terms\n", content, ncontent);
            _exit(-1);
            */
            FATAL_LOGGER(hidoc->logger, "NO terms globalid:%lld index_text_num:%d content:%s", LL64(fheader->globalid), index_text_num, content);
            goto end;
        }
        block->ndata = sizeof(DOCHEADER) 
                + sizeof(XFIELD) * nfields 
                + hindex->nterms * sizeof(STERM)
                + hindex->term_offsets_total * sizeof(int) 
                + hindex->term_text_total 
                + sizeof(int) * index_int_num 
                + sizeof(int64_t) * index_long_num 
                + sizeof(double) * index_double_num;
        if(hidoc->state->ccompress_status != HI_CCOMPRESS_DISABLED)
        {
            nzcontent = compressBound(ncontent);
            block->ndata += nzcontent;
        }
        else
        {
            block->ndata += ncontent;
        }
        REALLOC(hindex->block, hindex->nblock, block->ndata);
        block->data = hindex->block;
        TIMER_SAMPLE(timer);
        DEBUG_LOGGER(hidoc->logger, "malloc block[%d] time used:%lld", hindex->nblock, PT_LU_USEC(timer));
        docheader = (DOCHEADER *)block->data;
        memset(block->data, 0, sizeof(DOCHEADER));
        if((docheader->globalid = fheader->globalid) == 0)
        {
            FATAL_LOGGER(hidoc->logger, "fheader->globalid is 0");
            goto end;
        }
        docheader->status = fheader->status;
        docheader->dbid = fheader->dbid;
        docheader->secid = fheader->secid;
        docheader->crc = fheader->crc;
        docheader->category = fheader->category;
        docheader->slevel = fheader->slevel;
        docheader->rank = fheader->rank;
        //fprintf(stdout, "%s::%d dbid:%d secid:%d\n", __FILE__, __LINE__, docheader->dbid, docheader->secid);
        if(docheader->category == 0)
        {
            FATAL_LOGGER(hidoc->logger, "invalid category globalid:%lld", LL64(fheader->globalid));
            goto end;
        }
        //copy fields
        docheader->nfields = nfields;
        docheader->nterms = hindex->nterms;
        p = block->data + sizeof(DOCHEADER);
        xfields = (XFIELD *)p;
        for(i = 0; i < nfields; i++) 
        {
            xfields[i].from = fields[i].offset;
        }
        //memcpy(p, fields, sizeof(IFIELD) * nfields);
        p += sizeof(XFIELD) * nfields;
        //compress && dump copy terms_map
        termlist = sterm = (STERM *)p;
        p += sizeof(STERM) * hindex->nterms;
        /*
        i = 0;
        while(i < hindex->nodes[x].noffsets && j >= 0)
        {
            if(hindex->nodes[x].offsets[i] == hindex->nodes[x].roffsets[j])
            {
                n = hindex->nodes[x].offsets[i] - last;
                last = hindex->nodes[x].offsets[i];
                ++i;--j;
            }
            else if(hindex->nodes[x].offsets[i] > hindex->nodes[x].roffsets[j])
            {
                n = hindex->nodes[x].roffsets[j] - last;
                last = hindex->nodes[x].roffsets[j];
                --j;
            }
            else
            {
                n = hindex->nodes[x].offsets[i] - last;
                last = hindex->nodes[x].offsets[i];
                ++i;
            }
            if(n > 0)
            {
                np = &n;
                ZVBCODE(np, p);
                sterm->term_count++;
                docheader->terms_total++;
            }
        }
        */
#ifdef HAVE_SCWS
        //x = hindex->nterms - 1;
        x = 0;
        do
        {
            memcpy(sterm, &(hindex->terms[x]), sizeof(STERM));
            last = 0;
            pp = p;
            sterm->posting_offset = p - (char *)sterm; 
            j = 0;
            do
            {
                n = (hindex->nodes[x].offsets[j] - last);
                np = &n;
                ZVBCODE(np, p);
                docheader->terms_total++;
                last = hindex->nodes[x].offsets[j];
            }while(++j < hindex->nodes[x].noffsets);
            /*
             //test uncomress posting
            last = 0;
            s = pp;
            while(s < p)
            {
                to = 0;
                np = &to;
                UZVBCODE(s, n, np);
                last += to;
                fprintf(stdout, "x:%d termid:%d %.*s\n", x, hindex->nodes[x].termid, sterm->term_len, content + last);
            }
            */
            sterm->posting_size = p - pp;
            ++sterm;
        }while(++x  < hindex->nterms);
#else
        x = hindex->nterms - 1;
        do
        {
            memcpy(sterm, &(hindex->terms[x]), sizeof(STERM));
            last = 0;
            pp = p;
            sterm->posting_offset = p - (char *)sterm; 
            j = hindex->nodes[x].nroffsets - 1;
            do
            {
                n = (hindex->nodes[x].roffsets[j] - last);
                np = &n;
                ZVBCODE(np, p);
                docheader->terms_total++;
                last = hindex->nodes[x].roffsets[j];
            }while(--j >= 0);
            /*
             //test uncomress posting
            last = 0;
            s = pp;
            while(s < p)
            {
                to = 0;
                np = &to;
                UZVBCODE(s, n, np);
                last += to;
                fprintf(stdout, "x:%d termid:%d %.*s\n", x, hindex->nodes[x].termid, sterm->term_len, content + last);
            }
            */
            sterm->posting_size = p - pp;
            ++sterm;
        }while(--x  >= 0);
#endif
        TIMER_SAMPLE(timer);
        DEBUG_LOGGER(hidoc->logger, "compress posting[%d] time used:%lld", hindex->nterms, PT_LU_USEC(timer));
        //compress content 
        docheader->content_off = p - (char *)block->data;
        //fprintf(stdout, "%s::%d off:%d p:%p\n", __FILE__, __LINE__, docheader->content_off, p);
        if(hidoc->state->ccompress_status != HI_CCOMPRESS_DISABLED)
        {
            if((ret = compress((Bytef *)p, (uLongf *)&nzcontent,
                            (Bytef *)(content), (uLong)(ncontent))) == Z_OK)
            {
                TIMER_SAMPLE(timer);
                DEBUG_LOGGER(hidoc->logger, "compress content(%d) time used:%lld", ncontent, PT_LU_USEC(timer));
                docheader->content_size = ncontent;
                docheader->content_zsize = nzcontent;
                p += nzcontent;
            }
            else goto end; 
        }
        else
        {
            memcpy(p, content, ncontent);
            docheader->content_size = ncontent;
            p += ncontent;
        }
        //copy prevnext block
        docheader->prevnext_off = p - (char *)block->data; 
        //fprintf(stdout, "%s::%d off:%d p:%p ncontent:%d x:%d\n", __FILE__, __LINE__, docheader->content_off, p, ncontent, docheader->prevnext_off - docheader->content_off);
        if(hidoc->state->phrase_status != HI_PHRASE_DISABLED)
        {
            termlist = (STERM *)((char *)block->data + sizeof(DOCHEADER) + sizeof(XFIELD) * nfields);
            pp = p;
#ifdef HAVE_SCWS
            for(i = 0; i < hindex->nterms; i++)
            {
                last = 0;
                ps = p;
                for(j = 0; j < hindex->nterms; j++)
                {
                    if(hindex->nodes[i].prevs[j])
                    {
                        mm = j << 1;
                        n = mm - last;
                        last = mm;
                        //if(docheader->globalid == 1306048490225284ll){WARN_LOGGER(hidoc->logger, "i:%d j:%d last:%d mm:%d n:%d term:%.*s", i, j, last, mm, n, hindex->terms[i].term_len, content + hindex->nodes[i].term_offset);}
                        np = &n;
                        ZVBCODE(np, p);
                    }
                    if(hindex->nodes[i].nexts[j])
                    {
                        mm = ((j << 1) | 1);
                        n = mm - last;
                        last = mm;
                        //if(docheader->globalid == 1306048490225284ll){WARN_LOGGER(hidoc->logger, "i:%d j:%d last:%d mm:%d n:%d term:%.*s", i, j, last, mm, n, hindex->terms[i].term_len, content + hindex->nodes[i].term_offset);}
                        np = &n;
                        ZVBCODE(np, p);
                    }
                }
                termlist[i].prevnext_size = p - ps;
            }
#else
            int k = 0;
            for(i = (hindex->nterms - 1); i >= 0; i--)
            {
                last = 0;
                x = 0;
                ps = p;
                for(j = (hindex->nterms - 1); j >= 0; j--)
                {
                    if(hindex->nodes[i].prevs[j])
                    {
                        mm = j << 1;
                        n = mm - last;
                        last = mm;
                        np = &n;
                        ZVBCODE(np, p);
                    }
                    if(hindex->nodes[i].nexts[j])
                    {
                        mm = ((j << 1) | 1);
                        n = mm - last;
                        last = mm;
                        np = &n;
                        ZVBCODE(np, p);
                    }
                    ++x;
                }
                termlist[k].prevnext_size = p - ps;
                ++k;
            }
#endif
            /*
            //test prevnext
            s = pp;
            for(i = 0; i < hindex->nterms; i++)
            {
                last = 0;
                es = s + termlist[i].prevnext_size;
                while(s < es)
                {
                    x = 0;
                    np = &x;
                    UZVBCODE(s, n, np);
                    last += x;
                    if(last > hindex->nterms)
                    {
                        fprintf(stdout, "%s::%d nterm:%d i:%d last:%d x:%d\n", __FILE__, __LINE__, hindex->nterms, i, last, x);
                        _exit(-1);
                    }
                }
            }
            */
            docheader->prevnext_size = p - pp;
        }
        /*
        char *content = block->data + docheader->content_off;
        int j = 0;
        IFIELD *fieldslist = block->data + sizeof(DOCHEADER);
        for(j = 0; j < docheader->nfields; j++)
        {
            if((fieldslist[j].flag & IB_DATATYPE_TEXT) && (fieldslist[j].flag & IB_IS_NEED_INDEX))
                fprintf(stdout, "%d:%.*s\n", j, fieldslist[j].length, content + fieldslist[j].offset);
        }
        */
        //copy term block 
        docheader->textblock_off = p - (char *)block->data;

        pp = p;
#ifdef HAVE_SCWS
        x = 0 ;
        do
        {
            s = content + hindex->nodes[x].term_offset;
            es = s + hindex->terms[x].term_len;
            while(s < es)
            {
                if(*s >= 'A' && *s <= 'Z') *p++ = *s + 'a' - 'A';
                else *p++ = *s;
                ++s;
            }
            //memcpy(p, (content + hindex->nodes[x].term_offset), hindex->terms[x].term_len);
            //p += hindex->terms[x].term_len;
            //fprintf(stdout, "%s::%d x:%d off:%d %d:%.*s\n", __FILE__, __LINE__, x, hindex->nodes[x].term_offset, hindex->nodes[x].termid, hindex->terms[x].term_len, content + hindex->nodes[x].term_offset);
        }while(++x < hindex->nterms);
#else
        x = hindex->nterms - 1;
        do
        {
            memcpy(p, (content + hindex->nodes[x].term_offset), hindex->terms[x].term_len);
            //fprintf(stdout, "%s::%d x:%d off:%d %d:%.*s\n", __FILE__, __LINE__, x, hindex->nodes[x].term_offset, hindex->nodes[x].termid, hindex->terms[x].term_len, content + hindex->nodes[x].term_offset);
            p += hindex->terms[x].term_len;
        }while(--x >= 0);
#endif
        docheader->textblock_size = p - pp;
        //fprintf(stdout, "pp:%s\n", pp);
        //_exit(-1);
        //copy int/double index
        if(index_int_num > 0)
        {
            docheader->intblock_off = p - (char *)block->data;
            pp = p;
            ACCESS_LOGGER(hidoc->logger, "global:%lld int index from:%d num:%d", LL64(docheader->globalid), index_int_from, index_int_num);
            np = (int *)p;
            i = index_int_from;
            to = i + index_int_num;
            do{
                s = content + fields[i].offset;
                n = (int)atoi(s);
                //fprintf(stdout, "%d:{n:%d,s:%.*s}\n", i, n, fields[i].length, s);
                *np++ = n;
            }while(++i < to);
            p = (char *)np;
            docheader->intindex_from = index_int_from;
            docheader->intblock_size = p - pp;
            if(hidoc->state->int_index_count == 0) 
                hidoc_set_int_index(hidoc, index_int_from, index_int_num);
        }
        if(index_long_num > 0)
        {
            docheader->longblock_off = p - (char *)block->data;
            pp = p;
            ACCESS_LOGGER(hidoc->logger, "global:%lld long index from:%d num:%d", LL64(docheader->globalid), index_long_from, index_long_num);
            npl = (int64_t *)p;
            i = index_long_from;
            to = i + index_long_num;
            do
            {
                s = content + fields[i].offset;
                nl = (int64_t)atoll(s);
                *npl++ = nl;
            }while(++i < to);
            p = (char *)npl;
            docheader->longindex_from = index_long_from;
            docheader->longblock_size = p - pp;
            if(hidoc->state->long_index_count == 0) 
                hidoc_set_long_index(hidoc, index_long_from, index_long_num);
        }
        if(index_double_num > 0)
        {
            docheader->doubleblock_off = p - (char *)block->data;
            pp = p;
            ACCESS_LOGGER(hidoc->logger, "global:%lld double index from:%d num:%d", LL64(docheader->globalid), index_double_from, index_double_num);
            npf = (double *)p;
            i = index_double_from;
            to = i + index_double_num;
            do{*npf++ = atof(content + fields[i].offset);}while(++i < to);
            p = (char *)npf;
            docheader->doubleindex_from = index_double_from;
            docheader->doubleblock_size = p - pp;
            if(hidoc->state->double_index_count == 0)
                hidoc_set_double_index(hidoc, index_double_from, index_double_num);
        }
        if(docheader->intblock_size == 0 
                && docheader->longblock_size  == 0 
                && docheader->doubleblock_size == 0)
        {
            WARN_LOGGER(hidoc->logger, "global:%lld no int/long/double index", LL64(docheader->globalid));
        }
        ret = docheader->size = block->ndata = p - (char *)block->data;
        if(ret <= 0) {FATAL_LOGGER(hidoc->logger, "bad block globalid:%lld", LL64(fheader->globalid));}
end:
        //if(ret == -1 && block->data){free(block->data); block->data = NULL;}
        TIMER_CLEAN(timer);
    }
    else
    {
        FATAL_LOGGER(hidoc->logger, "Invalid Document:%lld fields:%p nfields:%d content:%p ncontent:%d block:%p map:%p", LL64(fheader->globalid), fields, nfields, content, ncontent, block, hindex->map);

    }
    return ret;
}

/* parse hispider document */
int hidoc_parse_document(HIDOC *hidoc, HINDEX *hindex)
{
    char *content = NULL, *data = NULL, path[FILE_PATH_MAX];
    int *xint = NULL, n = 0, ret = -1, is_ok = 0, ncontent = 0;
    double *xdouble = NULL;
    int64_t *xlong = NULL;
    struct stat st =  {0};
    IFIELD *fields = NULL;
    FHEADER fheader = {0};
    IBDATA block = {0};

    if(hidoc && hidoc->state && hindex) 
    {
	    MUTEX_LOCK(hidoc->mutex);
        if(hidoc->dumpfd <= 0 || fstat(hidoc->dumpfd, &st) != 0 
                || hidoc->state->dump_offset >= st.st_size) 
        {
            hidoc__check__dump(hidoc);
            FATAL_LOGGER(hidoc->logger, "Invalid offset:%lld file_size:%lld", (long long)hidoc->state->dump_offset, (long long)st.st_size);
            goto end;
        }
        if(lseek(hidoc->dumpfd,  (off_t)hidoc->state->dump_offset, 
                    SEEK_SET) == hidoc->state->dump_offset 
                && (n = read(hidoc->dumpfd, &fheader, sizeof(FHEADER))) == sizeof(FHEADER))
        {
            DEBUG_LOGGER(hidoc->logger, "offset:%lld fheader{status:%d globalid:%lld category:%lld rank:%f slevel:%d size:%d", LL64(hidoc->state->dump_offset), fheader.status, LL64(fheader.globalid), LL64(fheader.category), fheader.rank, fheader.slevel, fheader.size);
            hidoc->state->dump_offset += (off_t)sizeof(FHEADER);
            if(fheader.size <= 0)
            {
                if(fheader.flag & IB_STATUS_SET)
                {
                    ret = hidoc__set__idx__status(hidoc, (int64_t)fheader.globalid, fheader.status);
                }
                DEBUG_LOGGER(hidoc->logger, "set_idx_status(%lld, %d) -> %d", LL64(fheader.globalid), fheader.status, ret);
                if(fheader.flag & IB_RANK_SET)
                {
                    ret = hidoc__set__rank(hidoc, (int64_t)fheader.globalid, fheader.rank);
                    ACCESS_LOGGER(hidoc->logger, "set_rank(%lld, %f) -> %d", LL64(fheader.globalid), fheader.rank, ret);
                }
                if(fheader.flag & (IB_CATBIT_SET|IB_CATBIT_UNSET))
                {
                    ret = hidoc__set__category(hidoc, (int64_t)fheader.globalid, fheader.flag, fheader.category);
                    ACCESS_LOGGER(hidoc->logger, "set_category(%lld, %lld) -> %d", LL64(fheader.globalid), LL64(fheader.category), ret);
                }
                ret = 0;
            }
            else
            {
                if((fheader.flag & IB_DUMP_SET))
                {
                    if(read(hidoc->dumpfd, path, fheader.size) == fheader.size)
                    {
                        path[fheader.size] = 0;
                        WARN_LOGGER(hidoc->logger, "reset_dump(%s)", path);
                        hidoc__set__dump(hidoc, path);
                        ret = 0;
                    }
                    else
                    {
                        hidoc->state->dump_offset -= (off_t)sizeof(FHEADER);
                    }
                }
                else
                {
                    REALLOC(hindex->data, hindex->ndata, (fheader.size+1));
                    //REMALLOC(hidoc->data, hidoc->ndata, fheader.size, hidoc->logger);
                    ret = 0;
                    if((data = hindex->data) && lseek(hidoc->dumpfd, (off_t)hidoc->state->dump_offset,
                                SEEK_SET) == hidoc->state->dump_offset
                            && (ret = read(hidoc->dumpfd, data, fheader.size)) == fheader.size)
                    {
                        data[fheader.size] = '\0';
                        hidoc->state->dump_offset += (off_t)fheader.size;
                        is_ok = 1;
                        ret = 0;
                    }
                    else
                    {
                        hidoc->state->dump_offset -= (off_t)sizeof(FHEADER);
                        WARN_LOGGER(hidoc->logger, "read fd:%d data:%p offset:%lld id:%lld size:%d/%d failed, %s", hidoc->dumpfd, data, LL64(hidoc->state->dump_offset), LL64(fheader.globalid), fheader.size, ret, strerror(errno));
                        ret = -1;
                    }
                }
            }
        }
end:
	    MUTEX_UNLOCK(hidoc->mutex);
        if(is_ok && data)
        {
            if((fheader.flag & IB_INT_SET) && fheader.size == sizeof(int) * hidoc->state->int_index_count)
            {
                xint = (int *)data;
                if((ret = hidoc_set_all_int_fields(hidoc,  fheader.globalid, xint)) > 0)
                    hidoc__set__header(hidoc, ret, &fheader);
                ACCESS_LOGGER(hidoc->logger, "set_int(%lld/%lld/%f) -> %d", LL64(fheader.globalid), LL64(fheader.category), fheader.rank, ret);
                ret = 0;
            }
            else if((fheader.flag & IB_LONG_SET) &&  fheader.size == sizeof(int64_t) * hidoc->state->long_index_count)
            {
                xlong = (int64_t *)data;
                if((ret = hidoc_set_all_long_fields(hidoc, fheader.globalid, xlong)) > 0)
                    hidoc__set__header(hidoc, ret, &fheader);
                ACCESS_LOGGER(hidoc->logger, "set_long(%lld/%lld/%f) -> %d", LL64(fheader.globalid), LL64(fheader.category), fheader.rank, ret);
                ret = 0;
            }
            else if((fheader.flag & IB_DOUBLE_SET) &&  fheader.size == sizeof(double) * hidoc->state->double_index_count)
            {
                xdouble = (double *)data;
                if((ret = hidoc_set_all_double_fields(hidoc, fheader.globalid, xdouble)) > 0)
                    hidoc__set__header(hidoc, ret, &fheader);
                ACCESS_LOGGER(hidoc->logger, "set_double(%lld/%lld/%f) -> %d", LL64(fheader.globalid), LL64(fheader.category), fheader.rank, ret);
                ret = 0;
            }
            else
            {
                fields = (IFIELD *)data;
                content = data + fheader.nfields * sizeof(IFIELD);
                if((ncontent = (fheader.size - fheader.nfields * sizeof(IFIELD))) > 0 
                        && (ret = hidoc_genindex(hidoc, hindex, &fheader, fields, 
                                fheader.nfields, content, ncontent, &block))> 0)
                {
                    hidoc_push_index(hidoc, fields, fheader.flag, &block);
                    ret = 0;
                    /*
                       DOCHEADER *docheader = (DOCHEADER *)block.data;
                       char *content = block.data + docheader->content_off;
                       int j = 0;
                       for(j = 0; j < docheader->nfields; j++)
                       {
                       if((fields[j].flag & IB_DATATYPE_TEXT) && (fields[j].flag & IB_IS_NEED_INDEX))
                       fprintf(stdout, "%d:%.*s\n", j, fields[j].length, content + fields[j].offset);
                       }
                       */
                }
                else
                {
                    //fprintf(stderr, "%s::%d nfields:%d data->size:%d block->ndata:%d\n", __FILE__, __LINE__, fheader.nfields, fheader.size, block.ndata);
                    ERROR_LOGGER(hidoc->logger, "parse document:%lld ncontent:%d failed", LL64(fheader.globalid), ncontent);
                }
            }
        }
    }
    return ret;
}

/* add documents  */
int hidoc_add_documents(HIDOC *hidoc, char **hidocs, int nhidocs)
{
    int ret = -1, i = 0, fd = 0, ncontent = 0;
    FHEADER fheader = {0};
    IFIELD *fields = NULL;
    HINDEX *hindex = NULL;
    char *content = NULL;
    IBDATA block = {0};

    if(hidoc && hidocs && nhidocs > 0 && (hindex = hindex_new()))
    {
        for(i = 0; i < nhidocs; i++)
        {
            if((fd = open(hidocs[i], O_RDONLY, 0644)) > 0)
            {
                DEBUG_LOGGER(hidoc->logger, "ready for parsing doc[%s]", hidocs[i]);
                while(read(fd, &fheader, sizeof(FHEADER)) > 0)
                {
                    //fprintf(stdout ,"%s::%d nfields:%d size:%d \n", __FILE__, __LINE__, fheader.nfields, fheader.size);
                    if(fheader.status < 0) 
                        hidoc_set_idx_status(hidoc, (int64_t)fheader.globalid, fheader.status);
                    else
                    {
                        REALLOC(hindex->data, hindex->ndata, fheader.size);
                        //fprintf(stdout ,"%s::%d OK[%p] \n", __FILE__, __LINE__, hidoc->data);
                        if(hindex->data && read(fd, hindex->data, fheader.size) > 0) 
                        {
                            //fprintf(stdout ,"%s::%d OK \n", __FILE__, __LINE__);
                            fields = (IFIELD *)(hindex->data);
                            content = (char *)(hindex->data + fheader.nfields * sizeof(IFIELD));
                            hindex->data[fheader.size] = '\0';
                            /*
                               int x = 0 ;
                               for(x = 0; x < fheader.nfields; x++)
                               {
                               fprintf(stdout, "%d:{%.*s}\n", x, fields[x].length, content + fields[x].offset);
                               }
                               */

                            ncontent = fheader.size - fheader.nfields * sizeof(IFIELD);
                            //fprintf(stdout ,"%s::%d content:%s ncontent:%d\n", 
                            //        __FILE__, __LINE__, content, ncontent);
                            if((ret = hidoc_genindex(hidoc, hindex, &fheader, fields, fheader.nfields, 
                                            content, ncontent, &block))> 0)
                            {
                                //fprintf(stdout ,"%s::%d OK \n", __FILE__, __LINE__);
                                hidoc->doc_total++;
                                hidoc->size_total += ncontent;
                                hidoc_push_index(hidoc, fields, fheader.flag, &block);
                            }
                            else
                            {
                                ERROR_LOGGER(hidoc->logger, "parse document:%lld failed", LL64(fheader.globalid));
                                //fprintf(stderr, "genindex(%d) failed, %s\r\n", hidoc->doc_total, strerror(errno));
                                //_exit(-1);
                            }
                        }
                        else
                        {
                            fprintf(stderr, "read file failed, %s\n", strerror(errno));
                            _exit(-1);
                        }
                    }
                }
                DEBUG_LOGGER(hidoc->logger, "doc_total:%d doc_size:%lld", hidoc->doc_total, hidoc->size_total);
                close(fd);
            }
            else 
            {
                fprintf(stderr, "open file(%s) failed, %s\n", hidocs[i], strerror(errno));
                _exit(-1);
            }
        }
        hindex_clean(hindex);
    }
    return ret;
}

/* add node */
int hidoc_add_node(HIDOC *hidoc, int type, char *name, int limit)
{
    int nodeid = -1, i = 0, n = 0;

    if(hidoc && hidoc->state->nnodes < HI_NODE_MAX && type >= 0 
            && name && (n = strlen(name)) > 0 && limit >= 0) 
    {
        //fprintf(stdout, "%s::%d nodeid:%d type:%d name:%s limit:%d\n", __FILE__, __LINE__, nodeid, type, name, limit);
        MUTEX_LOCK(hidoc->mutex);
        if((nodeid = (mmtrie_get(hidoc->namemap, name, n) - 1)) < 0)
        {
            for(i = 1; i < HI_NODE_MAX; i++)
            {
                if(hidoc->state->nodes[i].status == 0)
                {
                    strcpy(hidoc->state->nodes[i].name, name);
                    hidoc->state->nodes[i].status = 1;  
                    hidoc->state->nodes[i].type = type;  
                    hidoc->state->nodes[i].limit = limit;  
                    mmtrie_add(hidoc->namemap, name, n, i);
                    nodeid = i;
                    hidoc->state->nnodes++;
                    if(type == HI_NODE_INDEXD) hidoc->state->nidxnodes++;
                    break;
                }
            }
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return nodeid;
}

/* delete node */
int hidoc_del_node(HIDOC *hidoc, int nodeid)
{
    char taskstr[HI_LINE_SIZE], *name = NULL;
    int id = -1, i = 0, n = 0, x = 0;
    HITASK *tasks = NULL;

    if(hidoc && nodeid > 0 && nodeid < HI_NODE_MAX)
    {
        MUTEX_LOCK(hidoc->mutex);
        if(hidoc->state->nodes[nodeid].status > 0 && (name = hidoc->state->nodes[nodeid].name)
                && (n = strlen(name)) > 0 && (tasks = hidoc->state->nodes[nodeid].tasks))
        {
            for(i = 0; i < HI_TASKS_MAX; i++)
            {
                if(tasks[i].status > 0 && (x = sprintf(taskstr, "%d:%s:%d", 
                    hidoc->state->nodes[nodeid].type, tasks[i].ip, tasks[i].port)) > 0)
                    mmtrie_del(hidoc->namemap, taskstr, x);
                if(tasks[i].mmqid > 0)mmqueue_close(MMQ(hidoc->mmqueue), tasks[i].mmqid);
                tasks[i].mmqid = 0;
                tasks[i].nqueue = 0;
            }
            mmtrie_del(hidoc->namemap, name, n);
            if(hidoc->state->nodes[nodeid].type == HI_NODE_INDEXD 
                    && --(hidoc->state->nidxnodes) == 0)
                hidoc->state->popid = 0;
            memset(&(hidoc->state->nodes[nodeid]), 0, sizeof(HINODE));
            if(--(hidoc->state->nnodes) == 0)
                hidoc->state->docpopid = 0;
            id = nodeid;
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return id;
}

/* set node limit */
int hidoc_set_node_limit(HIDOC *hidoc, int nodeid, int limit)
{
    int id = -1;

    if(hidoc && nodeid > 0 && nodeid < HI_NODE_MAX && limit >= 0)
    {
        MUTEX_LOCK(hidoc->mutex);
        hidoc->state->nodes[nodeid].limit = limit;
        id = nodeid;
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return id;
}

int hidoc_list_nodes(HIDOC *hidoc, char *out, char *end)
{
    char *p = NULL, *pp = NULL;
    int n = 0, i = 0;

    if(hidoc && out && out < end)
    {
        MUTEX_LOCK(hidoc->mutex);
        p = out;
        p += sprintf(p, "({'count':'%d','nodes':{", hidoc->state->nnodes);
        pp = p;
        for(i = 1; i < HI_NODE_MAX; i++)
        {
            if(hidoc->state->nodes[i].status > 0 && p < end)
            {
                p += sprintf(p, "'%d':{'type':'%d', 'name':'%s', 'limit':'%d', "
                        "'ntasks':'%d', 'total':'%d'},", i, hidoc->state->nodes[i].type,
                        hidoc->state->nodes[i].name, hidoc->state->nodes[i].limit, 
                        hidoc->state->nodes[i].ntasks, 
                        hidoc->state->nodes[i].total);
            }
        }
        if(p != pp)--p;
        p += sprintf(p, "}})");
        n = p - out;
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return n;
}

/* add task */
int hidoc_add_task(HIDOC *hidoc, int nodeid, char *ip, int port)
{
    int taskid = -1, i = 0, n = 0;
    char taskstr[HI_LINE_SIZE];
    HINODE *nodes = NULL;
    HITASK *tasks = NULL;

    if(hidoc && ip && port > 0 && nodeid < HI_NODE_MAX 
            && (nodes = hidoc->state->nodes) && nodes[nodeid].status > 0
            && nodes[nodeid].ntasks < HI_TASKS_MAX
            && (n = sprintf(taskstr, "%d:%s:%d", nodes[nodeid].type, ip, port)) > 0
            && (taskid = (mmtrie_get(hidoc->namemap, taskstr, n) - 1)) < 0
            && (tasks = nodes[nodeid].tasks))
    {
        MUTEX_LOCK(hidoc->mutex);
        for(i = 0; i < HI_TASKS_MAX; i++)
        {
            if(tasks[i].status == 0)
            {
                tasks[i].status = 1;
                strcpy(tasks[i].ip, ip);
                tasks[i].port = port;
                taskid = nodeid * HI_TASKS_MAX + i; 
                mmtrie_add(hidoc->namemap, taskstr, n, taskid+1);
                //fprintf(stdout, "%s::%d nodeid:%d taskid:%d\n", __FILE__, __LINE__, nodeid, taskid);
                iqueue_push(hidoc->queue, taskid);
                nodes[nodeid].ntasks++;
                tasks[i].mmqid = mmqueue_new(MMQ(hidoc->mmqueue));
                break;
            }
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return taskid;
}

/* delete task */
int hidoc_del_task(HIDOC *hidoc, int taskid)
{
    int id = -1, nodeid = 0, n = 0;
    char taskstr[HI_LINE_SIZE];
    HITASK *tasks = NULL;
    HINODE *nodes = NULL;

    if(hidoc && taskid >= 0 && taskid < (HI_NODE_MAX * HI_TASKS_MAX)
            && (nodeid = (taskid/HI_TASKS_MAX)) < HI_NODE_MAX
            && (nodes = hidoc->state->nodes) && (tasks = nodes[nodeid].tasks))
    {
        MUTEX_LOCK(hidoc->mutex);
        id = taskid % HI_TASKS_MAX;
        //fprintf(stdout, "%s::%d nodeid:%d id:%d\n", __FILE__, __LINE__, nodeid, id);
        if((n = sprintf(taskstr, "%d:%s:%d", nodes[nodeid].type, 
                        tasks[id].ip, tasks[id].port)) > 0)
                mmtrie_del(hidoc->namemap, taskstr, n);
        if(tasks[id].status > 0) nodes[nodeid].ntasks--;
        if(tasks[id].mmqid > 0)mmqueue_close(MMQ(hidoc->mmqueue), tasks[id].mmqid);
        memset(&(tasks[id]), 0, sizeof(HITASK));
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return id;
}

/* pop task */
int hidoc_pop_task(HIDOC *hidoc, HITASK *task)
{
    int taskid = -1, nodeid = -1, id = -1;
    HITASK *tasks = NULL;

    if(hidoc && task && QTOTAL(hidoc->queue) > 0)
    {
        MUTEX_LOCK(hidoc->mutex);
        iqueue_pop(hidoc->queue, &taskid);
        if(taskid >= 0 && taskid < (HI_TASKS_MAX * HI_NODE_MAX)
                && (nodeid = taskid/HI_TASKS_MAX) < HI_NODE_MAX
                && (tasks = hidoc->state->nodes[nodeid].tasks)
                && (id = (taskid % HI_TASKS_MAX)) >= 0)
        {
            memcpy(task, &(tasks[id]), sizeof(HITASK));
        }
        else taskid = -1;
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return taskid;
}

/* push task to queue */
int hidoc_push_task(HIDOC *hidoc, int taskid)
{
    int id = -1, nodeid = -1;
    HITASK *tasks = NULL;

    if(hidoc && hidoc->queue && taskid >= 0 && taskid < (HI_NODE_MAX * HI_TASKS_MAX)
            && (nodeid = (taskid / HI_TASKS_MAX)) < HI_NODE_MAX)
    {
        MUTEX_LOCK(hidoc->mutex);
        id = taskid % HI_TASKS_MAX;
        if((tasks = hidoc->state->nodes[nodeid].tasks) 
                && tasks[id].status > 0)
        {
            iqueue_push(hidoc->queue, taskid);
            id = taskid;
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return id;
}

/* list tasks */
int hidoc_list_tasks(HIDOC *hidoc, int nodeid, char *out, char *end)
{
    char *p = NULL, *pp = NULL;
    int n = 0, id = 0, i = 0;
    HITASK *tasks = NULL;
    HINODE *nodes = NULL;

    if(hidoc && out && end && out < end && nodeid > 0 && nodeid < HI_NODE_MAX)
    {
        MUTEX_LOCK(hidoc->mutex);
        if((nodes = hidoc->state->nodes) && nodes[nodeid].status > 0 
                && (tasks = nodes[nodeid].tasks))
        {
            p = out;
            p += sprintf(p, "({'id':'%d', 'name':'%s', 'total':'%d', 'type':'%d', 'limit':'%d',"
                    "'count':'%d',", nodeid, nodes[nodeid].name, nodes[nodeid].total,
                    nodes[nodeid].type, nodes[nodeid].limit, nodes[nodeid].ntasks);
            pp = p;
            if(nodes[nodeid].ntasks > 0)
            {
                p += sprintf(p, "'tasks':{");
                for(i = 0; i < HI_TASKS_MAX; i++)
                {
                    if(tasks[i].status > 0)
                    {
                        id = nodeid * HI_TASKS_MAX + i;
                        p += sprintf(p, "'%d':{'host':'%s:%d', 'npackets':'%d'},",
                            id, tasks[i].ip, tasks[i].port, tasks[i].nxindexs);
                    }
                }
                --p;
                p += sprintf(p, "}");
            }
            if(p == pp) --p;
            p += sprintf(p, "})");
            n = p - out;
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return n;
}

/* push upindex */
void hidoc_update(HIDOC *hidoc, int mid, int flag)
{
    int i = 0, j = 0, id = 0;
    XINDEX *xindexs = NULL;
    HINODE *nodes = NULL;

    if(hidoc && (xindexs = (XINDEX *)(hidoc->xindexio.map)))
    {
        if((nodes = hidoc->state->nodes))
        {
            //added to node[id]->tasks queue
            if((id = xindexs[mid].nodeid) > 0 && nodes[id].status > 0 
                    && nodes[id].ntasks > 0 && xindexs[mid].nodeid > 0)    
            {
                for(j = 0; j < HI_TASKS_MAX; j++)
                {
                    if(nodes[id].tasks[j].status > 0)
                    {
                        if(nodes[id].tasks[j].mmqid <= 0)
                            nodes[id].tasks[j].mmqid = mmqueue_new(MMQ(hidoc->mmqueue));
                        if(nodes[id].tasks[j].mmqid > 0)
                        {
                            mmqueue_push(MMQ(hidoc->mmqueue), nodes[id].tasks[j].mmqid, mid);
                            nodes[id].tasks[j].nqueue++;
                        }
                    }
                }
            }
            //added to docNode/ParserNode queue
            if(mid <= hidoc->state->docpopid)
            {
                for(i = 1; i < HI_NODE_MAX; i++)
                {
                    if((nodes[i].type == HI_NODE_DOCD || (nodes[i].type == HI_NODE_PARSERD && flag))
                            && nodes[i].status > 0 && nodes[i].ntasks > 0)
                    {
                        for(j = 0; j < HI_TASKS_MAX; j++)
                        {
                            if(nodes[i].tasks[j].status > 0)
                            {
                                if(nodes[i].tasks[j].mmqid <= 0)
                                    nodes[i].tasks[j].mmqid = mmqueue_new(MMQ(hidoc->mmqueue));
                                if(nodes[i].tasks[j].mmqid > 0)
                                {
                                    mmqueue_push(MMQ(hidoc->mmqueue), nodes[i].tasks[j].mmqid, mid);
                                    nodes[i].tasks[j].nqueue++;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    return ;
}

/* read upindex */
int hidoc_read_upindex(HIDOC *hidoc, int taskid, char *data, int *len, int *count)
{
    int ret = -1, mid = -1, nodeid = -1, id = 0, k = 0, n = 0,  
        left = 0, prev = 0, next = 0, *px = NULL;//, *int_index = NULL, z = 0;
    DOCHEADER *docheader = NULL;
    //int64_t *long_index = NULL;
    //double *double_index = NULL;
    XINDEX *xindexs = NULL;
    HINODE *nodes = NULL;
    HITASK *tasks = NULL;
    char *p = NULL;

    if(hidoc && taskid >= 0 && data && taskid < HI_NODE_MAX * HI_TASKS_MAX
            && len && count && *len > 0)
    {
        MUTEX_LOCK(hidoc->mutex);
        *count = 0;
        k = taskid % HI_TASKS_MAX;
        if((nodeid = (taskid / HI_TASKS_MAX)) < HI_NODE_MAX  && (nodes = hidoc->state->nodes)
                && (tasks = hidoc->state->nodes[nodeid].tasks) && tasks[k].status > 0
                && (xindexs =  (XINDEX *)(hidoc->xindexio.map)))
        {
            if(tasks[k].nupdates > 0 && tasks[k].upid != tasks[k].upover)
            {
                if((*len = db_read_data(PDB(hidoc->update), taskid, p)) > 0)
                {
                    *count =  tasks[k].upcount;
                    ret = mid = tasks[k].upid;
                }
                else
                {
                    tasks[k].upid = tasks[k].upover = 0;
                }
            }
            else if(mmqueue_total(MMQ(hidoc->mmqueue), tasks[k].mmqid) > 0 && tasks[k].mmqid > 0)
            {
                tasks[k].upcount = 0;
                left = *len;
                p = data;
                id = mmqueue_head(MMQ(hidoc->mmqueue), tasks[k].mmqid, &mid);
                do
                {
                    if(id > 0 && mid > 0)
                    {
                        if(nodes[nodeid].type == HI_NODE_INDEXD && xindexs[mid].nodeid != nodeid)
                        {
                            prev = xindexs[mid].prev;
                            next = xindexs[mid].next;
                            FATAL_LOGGER(hidoc->logger, "Invalid rootid:%d id:%d prev:%d[nodeid:%d] next:%d[nodeid:%d] xindex[%d].nodeid[%d] to task[%s:%d].nodeid:%d nqueue:%d", tasks[k].mmqid, id, prev, xindexs[prev].nodeid, next, xindexs[next].nodeid, mid, xindexs[mid].nodeid, tasks[k].ip, tasks[k].port, nodeid, tasks[k].nqueue);
                            tasks[k].mmqid = 0;tasks[k].nqueue = 0;
                            break;
                        }
                        if(db_get_data_len(PDB(hidoc->db), mid) > (left-HI_LEFT_LEN)) break;
                        tasks[k].upid = mid;
                        px = (int *)p;
                        p += sizeof(int);
                        docheader = (DOCHEADER *)p;
                        if((n = db_read_data(PDB(hidoc->db), mid, p)) > sizeof(DOCHEADER) 
                                && xindexs[mid].globalid == (int64_t)docheader->globalid)
                        {
                            DEBUG_LOGGER(hidoc->logger, "update_index(%d) globalid:%lld mid:%d status:%d nodeid:%d task[%s:%d]", id,  LL64(docheader->globalid), mid, xindexs[mid].status, xindexs[mid].nodeid, tasks[k].ip, tasks[k].port);
                            docheader->status = xindexs[mid].status;
                            docheader->slevel = xindexs[mid].slevel;
                            docheader->category = xindexs[mid].category;
                            docheader->rank = xindexs[mid].rank;
                            //update int/double index
                            if(nodes[nodeid].type != HI_NODE_PARSERD)
                            {
                                /*
                                if(docheader->intblock_size > 0 && docheader->intblock_off >= 0
                                        && hidoc->state->int_index_count > 0
                                        && (int_index = (int *)(hidoc->xintio.map)))
                                {
                                    z = hidoc->state->int_index_count * mid;
                                    memcpy(p+docheader->intblock_off, &(int_index[z]), docheader->intblock_size);
                                }
                               if(docheader->longblock_size > 0 && docheader->longblock_off >= 0
                                        && hidoc->state->long_index_count > 0
                                        && (long_index = (int64_t *)(hidoc->xlongio.map)))
                                {
                                    z = hidoc->state->long_index_count * mid;
                                    memcpy(p+docheader->longblock_off, &(long_index[z]), docheader->longblock_size);
                                }
                                if(docheader->doubleblock_size > 0 && docheader->doubleblock_off >= 0
                                        && hidoc->state->double_index_count > 0
                                        && (double_index = (double *)(hidoc->xdoubleio.map)))
                                {
                                    z = hidoc->state->double_index_count * mid;
                                    memcpy(p+docheader->doubleblock_off, &(double_index[z]), docheader->doubleblock_size);
                                }
                                */
                            }
                            left -= n + sizeof(int);
                            p += n;
                            *px = n;
                            tasks[k].upcount++;
                        }
                        else
                        {
                            p -= sizeof(int);
                            FATAL_LOGGER(hidoc->logger, "Invalid data id:%d globalid:%lld docheader:%lld", mid, LL64(xindexs[mid].globalid), LL64(docheader->globalid));
                            mid = -1;
                            _exit(-1);
                            break;
                        }
                        mmqueue_pop(MMQ(hidoc->mmqueue),tasks[k].mmqid, &mid);
                        tasks[k].nqueue--;
                        id = mmqueue_head(MMQ(hidoc->mmqueue), tasks[k].mmqid, &mid);
                    }
                    else 
                    {
                        FATAL_LOGGER(hidoc->logger, "Invalid qid:%d mid:%d", id, mid);
                        tasks[k].mmqid = 0;tasks[k].nqueue = 0;
                        mid = -1;
                        break;
                    }
                }while(tasks[k].nqueue > 0 && left > HI_LEFT_LEN && id > 0 && mid > 0);
                if((*count = tasks[k].upcount) > 0 && (*len -= left) > 0
                        && db_set_data(PDB(hidoc->update), taskid, data, *len) >= 0)
                {
                    ret = tasks[k].upid;
                }
            }
            else ret = -1;
        }
        else ret = -2;
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;
}

/* over upindex */
int hidoc_over_upindex(HIDOC *hidoc, int taskid, int upid)
{
    int ret = -1, k = 0, nodeid = -1;
    HITASK *tasks = NULL;

    if(hidoc && taskid >= 0 && taskid < HI_NODE_MAX * HI_TASKS_MAX 
            && hidoc->state && upid > 0 && upid < hidoc->state->xindextotal)
    {
        MUTEX_LOCK(hidoc->mutex);
        k = taskid % HI_TASKS_MAX;
        if((nodeid = (taskid / HI_TASKS_MAX)) < HI_NODE_MAX 
                && (tasks = hidoc->state->nodes[nodeid].tasks) 
                && tasks[k].status > 0)
        {
            tasks[k].upover = upid;
            tasks[k].nupdates += tasks[k].upcount;
            ret = 0;
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;
}
/* set idx status */
int hidoc__set__category(HIDOC *hidoc, int64_t globalid, int flag, int64_t category)
{
    XINDEX *xindexs = NULL;
    int mid = 0;

    if(hidoc && (xindexs = (XINDEX *)(hidoc->xindexio.map))
            && (mid = hidoc_checkid(hidoc, globalid)) > 0 && mid < hidoc->state->xindextotal)
    {
        CHECK_XINDEXIO(hidoc);
        //CHECK_XINTIO(hidoc);
        //CHECK_XLONGIO(hidoc);
        //CHECK_XDOUBLEIO(hidoc);
        if(flag == 0)
        {
            xindexs[mid].category = category;
        }
        else if(flag & IB_CATBIT_SET)
        {
            xindexs[mid].category |= category;
        }
        else if(flag & IB_CATBIT_UNSET)
        {
            xindexs[mid].category &= ~(category);
        }
        hidoc_update(hidoc, mid, 0);
    }
    else mid = -1;
    return mid;
}
/* set  rank */
int hidoc__set__rank(HIDOC *hidoc, int64_t globalid, double rank)
{
    XINDEX *xindexs = NULL;
    int mid = 0;

    if(hidoc && (xindexs = (XINDEX *)(hidoc->xindexio.map))
            && (mid = hidoc_checkid(hidoc, globalid)) > 0 && mid < hidoc->state->xindextotal)
    {
        CHECK_XINDEXIO(hidoc);
        //CHECK_XINTIO(hidoc);
        //CHECK_XLONGIO(hidoc);
        //CHECK_XDOUBLEIO(hidoc);
        xindexs[mid].rank = rank;
        hidoc_update(hidoc, mid, 0);
    }
    else mid = -1;
    return mid;
}

/* set idx status */
int hidoc__set__idx__status(HIDOC *hidoc, int64_t globalid, int status)
{
    XINDEX *xindexs = NULL;
    int mid = 0;
    if(hidoc && (xindexs = (XINDEX *)(hidoc->xindexio.map))
            && (mid = hidoc_checkid(hidoc, globalid)) > 0 && mid < hidoc->state->xindextotal)
    {
        CHECK_XINDEXIO(hidoc);
        //CHECK_XINTIO(hidoc);
        //CHECK_XLONGIO(hidoc);
        //CHECK_XDOUBLEIO(hidoc);
        xindexs[mid].status = status;
        hidoc_update(hidoc, mid, 0);
    }
    else mid = -1;
    return mid;
}

/* set index header with fheader */
int hidoc__set__header(HIDOC *hidoc, int mid, FHEADER *fheader)
{
    XINDEX *xindexs = NULL;
    int ret = -1;

    if(hidoc && fheader && (xindexs = (XINDEX *)(hidoc->xindexio.map))
            && mid > 0 && mid < hidoc->state->xindextotal)
    {
        CHECK_XINDEXIO(hidoc);
        if(fheader->flag & IB_STATUS_SET) xindexs[mid].status = fheader->status;
        if(fheader->flag & IB_RANK_SET) xindexs[mid].rank = fheader->rank;
        if(fheader->flag & IB_CATBIT_SET)
        {
            xindexs[mid].category |= fheader->category;
        }
        else if(fheader->flag & IB_CATBIT_UNSET)
        {
            xindexs[mid].category &= ~(fheader->category);
        }
        ret = mid;
    }
    return ret;
}

/* set index status */
int hidoc_set_idx_status(HIDOC *hidoc, int64_t globalid, int status)
{
    int ret = -1;

    MUTEX_LOCK(hidoc->mutex);
    ret = hidoc__set__idx__status(hidoc, globalid, status);
    MUTEX_UNLOCK(hidoc->mutex);
    return ret;
}

/* set rank */
int hidoc_set_rank(HIDOC *hidoc, XFLOAT *list, int count)
{
    int ret = -1, i = 0, mid = 0;
    XINDEX *xindexs = NULL;

    if(hidoc && list && count > 0)
    {
        MUTEX_LOCK(hidoc->mutex);
        if((xindexs = (XINDEX *)(hidoc->xindexio.map)))
        {
            for(i = 0; i < count; i++)
            {
                if((mid = hidoc_checkid(hidoc, list[i].id)) > 0 && mid < hidoc->state->xindextotal)
                {
                    if(xindexs[mid].rank != list[i].val)
                    {
                        xindexs[mid].rank = list[i].val;
                        hidoc_update(hidoc, mid, 0);
                    }
                }
            }
            ret = count;
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;
}

/* set slevel */
int hidoc_set_slevel(HIDOC *hidoc, XLONG *list, int count)
{
    int ret = -1, i = 0, mid = 0;
    XINDEX *xindexs = NULL;

    if(hidoc && list && count > 0)
    {
        MUTEX_LOCK(hidoc->mutex);
        if((xindexs = (XINDEX *)(hidoc->xindexio.map)))
        {
            for(i = 0; i < count; i++)
            {
                if((mid = hidoc_checkid(hidoc, list[i].id)) > 0 && mid < hidoc->state->xindextotal)
                {
                    if(xindexs[mid].slevel != list[i].val)
                    {
                        xindexs[mid].slevel = list[i].val;
                        hidoc_update(hidoc, mid, 0);
                    }
                }
            }
            ret = count;
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;
}

/* set category */
int hidoc_set_category(HIDOC *hidoc, XLONG *list, int count)
{
    int ret = -1, i = 0, mid = 0;
    XINDEX *xindexs = NULL;

    if(hidoc && list && count > 0)
    {
        MUTEX_LOCK(hidoc->mutex);
        if((xindexs = (XINDEX *)(hidoc->xindexio.map)))
        {
            for(i = 0; i < count; i++)
            {
                if((mid = hidoc_checkid(hidoc, list[i].id)) > 0 && mid < hidoc->state->xindextotal)
                {
                    if(xindexs[mid].category != list[i].val)
                    {
                        xindexs[mid].category = list[i].val;
                        hidoc_update(hidoc, mid, 0);
                    }
                }
            }
            ret = count;
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return ret;
}

/* set int fields */
int hidoc_set_int_fields(HIDOC *hidoc, FXINT *list, int count)
{
    int i = 0, no = 0, mid = 0, *int_index = NULL;

    if(hidoc && list && count > 0)
    {
        MUTEX_LOCK(hidoc->mutex);
        for(i = 0; i < count; i++)
        {
            if((mid = hidoc_checkid(hidoc, list[i].id)) > 0 && mid < hidoc->state->xindextotal
                    && hidoc->state->int_index_count > 0
                    && (no = (hidoc->state->int_index_count * mid)) >= 0
                    && (int_index = (int *)hidoc->xintio.map))
            {
                no += list[i].no;
                int_index[no] = list[i].val;
                hidoc_update(hidoc, mid, 0);
            }
            else mid = -1;
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return mid;
}

/* set long fields */
int hidoc_set_long_fields(HIDOC *hidoc, FXLONG *list, int count)
{
    int i = 0, no = 0, mid = 0;
    int64_t*index = NULL;

    if(hidoc && list && count > 0)
    {
        MUTEX_LOCK(hidoc->mutex);
        for(i = 0; i < count; i++)
        {
            if((mid = hidoc_checkid(hidoc, list[i].id)) > 0 && mid < hidoc->state->xindextotal
                    && hidoc->state->long_index_count > 0
                    && (no = (hidoc->state->long_index_count * mid)) >= 0
                    && (index = (int64_t *)(hidoc->xlongio.map)))
            {
                no += list[i].no;
                index[no] = list[i].val;
                hidoc_update(hidoc, mid, 0);
            }
            else mid = -1;
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return mid;
}

/* set double fields */
int hidoc_set_double_fields(HIDOC *hidoc, FXDOUBLE *list, int count)
{
    int i = 0, no = 0, mid = 0;
    double *double_index = NULL;

    if(hidoc && list && count > 0)
    {
        MUTEX_LOCK(hidoc->mutex);
        for(i = 0; i < count; i++)
        {
            if((mid = hidoc_checkid(hidoc, list[i].id)) > 0 && mid < hidoc->state->xindextotal
                    && hidoc->state->double_index_count > 0
                    && (no = (hidoc->state->double_index_count * mid)) >= 0
                    && (double_index = (double *)hidoc->xdoubleio.map))
            {
                no += list[i].no;
                double_index[no] = list[i].val;
            }
            else mid = -1;
        }
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return mid;
}

/* set int fields */
int hidoc_set_all_int_fields(HIDOC *hidoc, int64_t globalid, int *list)
{
    int *int_index = NULL;
    int no = 0, mid = 0;

    if(hidoc && globalid && list && hidoc->state->int_index_count > 0)
    {
        MUTEX_LOCK(hidoc->mutex);
        if((mid = hidoc_checkid(hidoc, globalid)) > 0 && mid < hidoc->state->xindextotal
            && (no = (hidoc->state->int_index_count * mid)) >= 0
            && (int_index = (int *)hidoc->xintio.map))
        {
            //memcpy(&(int_index[no]), list, sizeof(int) * hidoc->state->int_index_count);
            hidoc_update(hidoc, mid, 0);
        }
        else mid = -1;
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return mid;
}

/* set all long fields */
int hidoc_set_all_long_fields(HIDOC *hidoc, int64_t globalid, int64_t *list)
{
    int64_t *long_index = NULL;
    int no = 0, mid = 0;

    if(hidoc && globalid && list && hidoc->state->long_index_count > 0)
    {
        MUTEX_LOCK(hidoc->mutex);
        if((mid = hidoc_checkid(hidoc, globalid)) > 0 && mid < hidoc->state->xindextotal
            && (no = (hidoc->state->long_index_count * mid)) >= 0
            && (long_index = (int64_t *)hidoc->xlongio.map))
        {
            //memcpy(&(long_index[no]), list, sizeof(int64_t) * hidoc->state->long_index_count);
            hidoc_update(hidoc, mid, 0);
        }
        else mid = -1;
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return mid;
}

/* set all double fields */
int hidoc_set_all_double_fields(HIDOC *hidoc, int64_t globalid, double *list)
{
    double *double_index = NULL;
    int no = 0, mid = 0;

    if(hidoc && globalid && list && hidoc->state->double_index_count > 0)
    {
        MUTEX_LOCK(hidoc->mutex);
        if((mid = hidoc_checkid(hidoc, globalid)) > 0 && mid < hidoc->state->xindextotal
            && (no = (hidoc->state->double_index_count * mid)) >= 0
            && (double_index = (double *)hidoc->xdoubleio.map))
        {
            //memcpy(&(double_index[no]), list, sizeof(double) * hidoc->state->double_index_count);
            hidoc_update(hidoc, mid, 0);
        }
        else mid = -1;
        MUTEX_UNLOCK(hidoc->mutex);
    }
    return mid;
}

/* resync index */
int hidoc_resync(HIDOC *hidoc)
{
    HITASK *tasks = NULL;
    int i = 0, j = 0;

    if(hidoc)
    {
        if(hidoc->state->nnodes > 0)
        {
            for(i = 1; i < HI_NODE_MAX; i++)
            {
                if((hidoc->state->nodes[i].type == HI_NODE_DOCD
                        || hidoc->state->nodes[i].type == HI_NODE_INDEXD)
                        && hidoc->state->nodes[i].status > 0 
                        && hidoc->state->nodes[i].ntasks > 0
                        && (tasks = hidoc->state->nodes[i].tasks))
                {
                    for(j = 0; j < HI_TASKS_MAX; j++)
                    {
                        if(tasks[j].status > 0)
                        {
                            tasks[j].count = 0;
                            tasks[j].last = 0;
                            tasks[j].popid = tasks[j].over = 0;
                            tasks[j].nxindexs = 0;
                            tasks[j].upcount = 0;
                            tasks[j].upid = tasks[j].upover = 0;
                            tasks[j].nupdates = 0;
                        }
                    }
                }
            }
        }

        return 0;
    }
    return -1;
}

/* new hindex */
HINDEX *hindex_new()
{
    HINDEX *hindex = NULL;

    if((hindex = (HINDEX *)xmm_mnew(sizeof(HINDEX))))
    {
        hindex->html = html_init();
        hindex->map = mtree_init();
    }
    return hindex;
}

/* clean hindex */
void hindex_clean(HINDEX *hindex)
{
    if(hindex)
    {
        if(hindex->html) html_clean(&(hindex->html));
        if(hindex->map){MTREE_CLEAN(hindex->map);}
        if(hindex->block){free(hindex->block);}
        if(hindex->data){free(hindex->data);}
        if(hindex->out){free(hindex->out);}
        xmm_free(hindex, sizeof(HINDEX));
    }
    return ;
}

/* document clean */
void hidoc_clean(HIDOC *hidoc)
{
    int i = 0;
    if(hidoc)
    {
        if(hidoc->dumpfd > 0)close(hidoc->dumpfd);
        if(hidoc->outdocfd > 0)close(hidoc->outdocfd);
        if(hidoc->state) munmap(hidoc->state, sizeof(HISTATE));
        if(hidoc->histatefd > 0)close(hidoc->histatefd);
        if(hidoc->xindexio.map) munmap(hidoc->xindexio.map, hidoc->xindexio.size);
        if(hidoc->xindexio.fd > 0) close(hidoc->xindexio.fd);
        if(hidoc->xintio.map) munmap(hidoc->xintio.map, hidoc->xintio.size);
        if(hidoc->xintio.fd > 0) close(hidoc->xintio.fd);
        if(hidoc->xlongio.map) munmap(hidoc->xlongio.map, hidoc->xlongio.size);
        if(hidoc->xlongio.fd > 0) close(hidoc->xlongio.fd);
        if(hidoc->xdoubleio.map) munmap(hidoc->xdoubleio.map, hidoc->xdoubleio.size);
        if(hidoc->xdoubleio.fd > 0) close(hidoc->xdoubleio.fd);
        if(hidoc->bstermio.map) munmap(hidoc->bstermio.map, hidoc->bstermio.size);
        if(hidoc->bstermio.fd > 0) close(hidoc->bstermio.fd);
        if(hidoc->db) db_clean(PDB(hidoc->db));
        if(hidoc->update) db_clean(PDB(hidoc->update));
        if(hidoc->map) mmtrie_clean(hidoc->map);
        MUTEX_DESTROY(hidoc->mutex);
        MUTEX_DESTROY(hidoc->mutex_segmentor);
        if(hidoc->xdict){mmtrie_clean(hidoc->xdict);}
        if(hidoc->mmtrie){mmtrie_clean(hidoc->mmtrie);}
        if(hidoc->namemap){mmtrie_clean(hidoc->namemap);}
        if(hidoc->kmap){mmtrie_clean(hidoc->kmap);}
        if(hidoc->queue){iqueue_clean(hidoc->queue);}
        if(hidoc->mmqueue){mmqueue_clean(MMQ(hidoc->mmqueue));}
#ifdef HAVE_SCWS
        for(i = 0; i < hidoc->nqsegmentors; i++)
        {
            ((scws_t)(hidoc->qsegmentors[i]))->r = NULL;
            ((scws_t)(hidoc->qsegmentors[i]))->d = NULL;
            scws_free((scws_t)(hidoc->qsegmentors[i]));
        }
        if(hidoc->segmentor) scws_free((scws_t)hidoc->segmentor);
#endif
        if(hidoc->logger){LOGGER_CLEAN(hidoc->logger);}
        xmm_free(hidoc, sizeof(HIDOC));
        hidoc = NULL;
    }
}

/* initialize */
HIDOC *hidoc_init()
{
    HIDOC *hidoc = NULL;

    if((hidoc = (HIDOC *)xmm_mnew(sizeof(HIDOC))))
    {
        MUTEX_INIT(hidoc->mutex);
        MUTEX_INIT(hidoc->mutex_segmentor);
        hidoc->queue                    = iqueue_init();
        hidoc->set_basedir              = hidoc_set_basedir;
        hidoc->set_int_index            = hidoc_set_int_index;
        hidoc->set_long_index           = hidoc_set_long_index;
        hidoc->set_double_index         = hidoc_set_double_index;
        hidoc->set_dict                 = hidoc_set_dict;
        //hidoc->set_forbidden_dict       = hidoc_set_forbidden_dict;
        hidoc->set_ccompress_status     = hidoc_set_ccompress_status;
        hidoc->set_phrase_status        = hidoc_set_phrase_status;
        hidoc->genindex                 = hidoc_genindex;
        hidoc->set_dump                 = hidoc_set_dump;
        hidoc->get_dumpinfo             = hidoc_get_dumpinfo;
        hidoc->pop_document             = hidoc_pop_document;
        hidoc->parse_document           = hidoc_parse_document;
        hidoc->parseHTML                = hidoc_parseHTML;
        hidoc->add_node                 = hidoc_add_node;
        hidoc->del_node                 = hidoc_del_node;
        hidoc->set_node_limit           = hidoc_set_node_limit;
        hidoc->list_nodes               = hidoc_list_nodes;
        hidoc->add_task                 = hidoc_add_task;
        hidoc->del_task                 = hidoc_del_task;
        hidoc->pop_task                 = hidoc_pop_task;
        hidoc->push_task                = hidoc_push_task;
        hidoc->list_tasks               = hidoc_list_tasks;
        hidoc->read_index               = hidoc_read_index;
        hidoc->over_index               = hidoc_over_index;
        hidoc->read_upindex             = hidoc_read_upindex;
        hidoc->over_upindex             = hidoc_over_upindex;
        hidoc->set_bterm                = hidoc_set_bterm;
        hidoc->update_bterm             = hidoc_update_bterm;
        hidoc->list_bterms              = hidoc_list_bterms;
        hidoc->add_bterm                = hidoc_add_bterm;
        hidoc->del_bterm                = hidoc_del_bterm;
        hidoc->sync_bterms              = hidoc_sync_bterms;
        hidoc->read_bterms              = hidoc_read_bterms;
        hidoc->over_bterms              = hidoc_over_bterms;
        hidoc->set_idx_status           = hidoc_set_idx_status;
        hidoc->set_rank                 = hidoc_set_rank;
        hidoc->set_category             = hidoc_set_category;
        hidoc->set_slevel               = hidoc_set_slevel;
        hidoc->set_int_fields           = hidoc_set_int_fields;
        hidoc->set_long_fields          = hidoc_set_long_fields;
        hidoc->set_double_fields        = hidoc_set_double_fields;
        hidoc->clean                    = hidoc_clean;
    }
    return hidoc;
}

#ifdef _DEBUG_HIDOC
#include <pthread.h>
//#define THREAD_NUM  0
static char *basedir = "/data/hibase/doc";
static char *dict = "/data/hibase/dict/dict.txt";
static int nthreads = 0;
void running(void *arg)
{
    HIDOC *hidoc = NULL;
    HINDEX *hindex = NULL;

    if(arg && (hidoc = hidoc_init()))
    {
        hidoc->set_basedir(hidoc, basedir);
        hidoc->set_dict(hidoc, dict);
        hidoc->set_dump(hidoc, (char *)arg);
        if((hindex = hindex_new()))
        {
            while(hidoc->parse_document(hidoc, hindex) == 0);
            hindex_clean(hindex);
        }
        hidoc->clean(&hidoc);
    }
    nthreads--;
    pthread_exit(NULL);

}

/* main */
int main(int argc, char **argv)
{
    HIDOC *hidoc = NULL;
    char **hidocs = NULL;
    int nhidocs = 0, i = 0;
    pthread_t threadid = {0};
    void *timer = NULL;

    if(argc > 1 && argv )
    {
#ifdef HAVE_PTHREAD
        TIMER_INIT(timer);
        for(i = 0; i < THREAD_NUM; i++)
        {
            if(pthread_create((pthread_t *)&threadid, NULL, (void *)&running, argv[1]) != 0)
            {
                _exit(-1);
            }
            else
            {
                nthreads++;
            }
        }
        while(nthreads)usleep(100);
        TIMER_SAMPLE(timer);
        fprintf(stdout, "time used:%lld\n", PT_LU_USEC(timer));
        TIMER_CLEAN(timer);
#else
        if((hidoc = hidoc_init()))
        {
            TIMER_INIT(timer);
            hidocs = &(argv[1]);
            nhidocs = argc - 1;
            hidoc->set_basedir(hidoc, basedir);
#ifdef _DEBUG_TASKS
            int taskid = -1, id = 0, n = 0, nodeid = -1;
            char xbuf[8192], *p = NULL, *end = NULL;
            HITASK task = {0};

            nodeid = hidoc_add_node(hidoc, HI_NODE_DOCD, "ds1", 20000000);
            hidoc_add_task(hidoc, nodeid, "10.0.6.82", 4936);
            fprintf(stdout, "%s::%d nodeid:%d nnodes:%d\n", __FILE__, __LINE__, nodeid, hidoc->state->nnodes); 
            hidoc_del_node(hidoc, nodeid);
            fprintf(stdout, "%s::%d nodeid:%d nnodes:%d\n", __FILE__, __LINE__, nodeid, hidoc->state->nnodes); 
            nodeid = hidoc_add_node(hidoc, HI_NODE_INDEXD, "bs1", 20000000);
            fprintf(stdout, "%s::%d nodeid:%d nnodes:%d\n", __FILE__, __LINE__, nodeid, hidoc->state->nnodes); 
            nodeid = hidoc_add_node(hidoc, HI_NODE_INDEXD, "bs2", 20000000);
            hidoc_add_task(hidoc, nodeid, "10.0.6.83", 4936);
            hidoc_add_task(hidoc, nodeid, "10.0.6.84", 4936);
            p = xbuf;end = p + 8192;
            if((n=hidoc_list_nodes(hidoc, p, end)) > 0)
            {
                xbuf[n] = 0;
                fprintf(stdout, "nodes:%s\n", xbuf);
            }
            fprintf(stdout, "qtotal:%d\n", QTOTAL(hidoc->queue));
            while((id = hidoc_pop_task(hidoc, &task)) >= 0)
            {
                fprintf(stdout, "%s::%d pop_task():%d\n", __FILE__, __LINE__, id);
                hidoc_del_task(hidoc, id);
            }
            if((n=hidoc_list_tasks(hidoc, nodeid, p, end)) > 0)
            {
                xbuf[n] = 0;
                fprintf(stdout, "task:%s\n", xbuf);
            }
            _exit(-1);
#endif
#ifdef REINDEX
            hidoc_reindex(hidoc);
            goto end;
#endif
#ifdef REDOC
            hidoc_redoc(hidoc);
            goto end;
#endif
 
            hidoc->set_dict(hidoc, dict);
            //fprintf(stdout ,"%s::%d OK \n", __FILE__, __LINE__);
            hidoc_add_documents(hidoc, hidocs, nhidocs);
            TIMER_SAMPLE(timer);
            fprintf(stdout, "indexed %d documents time used:%lld\n", 
                    hidoc->doc_total, PT_LU_USEC(timer));
end:
            TIMER_CLEAN(timer);
            hidoc->clean(&hidoc);
        }
#endif
    }
    else
    {
        fprintf(stderr, "Usage:%s index_source_file\n", argv[0]);
        _exit(-1);
    }
    return -1;
}
#endif
//gcc -o hidoc hidoc.c -I html -I utils/ html/*.c utils/*.c  -D_FILE_OFFSET_BITS=64 -D_DEBUG_HIDOC -lz -lm && ./hidoc /tmp/html/hidoc.doc
//gcc -o hidoc hidoc.c -I html -I utils/ html/*.c utils/*.c  -D_FILE_OFFSET_BITS=64 -D_DEBUG_HIDOC -lz -lm -lpthread -DHAVE_PTHREAD && ./hidoc /tmp/html/hispider.doc
