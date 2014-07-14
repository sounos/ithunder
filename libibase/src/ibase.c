#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <string.h>
#include <math.h>
#include <errno.h>
#include <zlib.h>
#ifdef HAVE_SCWS
#include <scws/scws.h>
#endif
#include "ibase.h"
#include "zvbcode.h"
#include "timer.h"
#include "mmtrie.h"
#include "logger.h"
#include "mtree64.h"
#include "db.h"
#include "xmm.h"
#include "immx.h"
#include "imap.h"
#include "lmap.h"
#include "dmap.h"
#define UCHR(p) ((unsigned char *)p)
#define ISSIGN(p) (*p == '@' || *p == '.' || *p == '-' || *p == '_')
#define ISNUM(p) ((*p >= '0' && *p <= '9'))
#define ISCHAR(p) ((*p >= 'A' && *p <= 'Z')||(*p >= 'a' && *p <= 'z'))
#define PIHEADER(ibase, docid) &(((IHEADER *)(ibase->headersio.map))[docid])
#ifndef LLI
#define LLI(x) ((long long int) x)
#endif
#ifdef MAP_LOCKED
#define MMAP_SHARED MAP_SHARED|MAP_LOCKED
#else
#define MMAP_SHARED MAP_SHARED
#endif

/* nocache open file */
int ibase_ncopen(char *path, int flag, int mode)
{
    int fd = -1;
    if(path)
    {
#ifdef F_NOCACHE
        if((fd = open(path, flag, mode)) > 0)
        {
            fcntl(fd, F_NOCACHE, 1);
        }
        return fd;
#endif
#ifdef O_DIRECORDT
	flag |= O_DIRECORDT;
#endif
        fd = open(path, flag, mode);
    }
    return fd;
}

/* mkdir force */
int ibase_mkdir(char *path)
{
    struct stat st;
    char fullpath[IB_PATH_MAX];
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

/* set dict file */
int ibase_set_dict(IBASE *ibase, char *dict_charset, char *dict_file, char *dict_rules)
{
    int i = 0;

    if(ibase)
    {   
#ifdef HAVE_SCWS
        scws_t segmentor = NULL;
        if(dict_charset)strcpy(ibase->dict_charset, dict_charset);
        if(dict_rules)strcpy(ibase->dict_rules, dict_rules);
        if(dict_file)strcpy(ibase->dict_file, dict_file);
        if((ibase->segmentor = scws_new()))
        {
            scws_set_charset((scws_t)(ibase->segmentor), dict_charset);
            scws_set_rule((scws_t)(ibase->segmentor), dict_rules);
            scws_set_dict((scws_t)(ibase->segmentor), dict_file, SCWS_XDICT_XDB);
            for(i = 0; i < IB_SEGMENTORS_MIN; i++)
            {
                if((segmentor = scws_new()))
                {
                    ibase->qsegmentors[i] = segmentor;
                    //scws_set_charset((scws_t)(segmentor), ibase->dict_charset);
                    //scws_set_rule((scws_t)(segmentor), ibase->dict_rules);
                    //scws_set_dict((scws_t)(segmentor), ibase->dict_file, SCWS_XDICT_XDB);
                    ((scws_t)segmentor)->d = ((scws_t)ibase->segmentor)->d;
                    ((scws_t)segmentor)->r = ((scws_t)ibase->segmentor)->r;
                    ((scws_t)segmentor)->mblen = ((scws_t)ibase->segmentor)->mblen;
                    ibase->nqsegmentors++;
                }
            }
        }
        else 
        {
            _exit(-1);
        }
#endif
    }
    return 0;
}

/* resume */
int ibase_set_basedir(IBASE *ibase, char *dir, int used_for, int mmsource_status)
{
    char path[IB_PATH_MAX];
    struct stat st = {0};
    int ret = -1, i = 0;
    void *timer = NULL;
    off_t size = 0;

    if(ibase)
    {
        TIMER_INIT(timer);
        strcpy(ibase->basedir, dir);
        /* dict */
        sprintf(path, "%s/%s", dir, IB_DICT_NAME);
        ibase_mkdir(path);
        if((ibase->mmtrie = mmtrie_init(path)) == NULL)
        {
            fprintf(stderr, "initialize mmtrie(%s) failed, %s\r\n", path, strerror(errno));
            _exit(-1);
        }
        sprintf(path, "%s/%s", dir, IB_XDICT_NAME);
        if((ibase->xmmtrie = mmtrie_init(path)) == NULL)
        {
            fprintf(stderr, "initialize mmtrie(%s) failed, %s\r\n", path, strerror(errno));
            _exit(-1);
        }
        /* logger */
        sprintf(path, "%s/%s", dir, IB_LOGGER_NAME);
        LOGGER_ROTATE_INIT(ibase->logger, path, LOG_ROTATE_DAY);
        /*  state */
        sprintf(path, "%s/%s", dir, IB_STATE_NAME);
        if((ibase->stateio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
            && fstat(ibase->stateio.fd, &st) == 0)
        {
            if((ibase->state = (IBSTATE *)mmap(NULL, sizeof(IBSTATE), PROT_READ|PROT_WRITE,
                            MMAP_SHARED, ibase->stateio.fd, 0)) == NULL || ibase->state == (void *)-1)
            {
                fprintf(stderr, "mmap state file (%s) failed, %s\r\n", path, strerror(errno));
                _exit(-1);
            }
            if(st.st_size < sizeof(IBSTATE) 
                    && ftruncate(ibase->stateio.fd, sizeof(IBSTATE)) == 0)
            {
                memset(ibase->state, 0, sizeof(IBSTATE));
            }
            ibase->state->used_for = used_for;
        }
        else
        {
            fprintf(stderr, "open state file (%s) failed, %s\r\n", path, strerror(errno));
            _exit(-1);
        }
        /* termstate */
        sprintf(path, "%s/%s", dir, IB_TERMSTATE_NAME);
        if((ibase->termstateio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(ibase->termstateio.fd, &st) == 0)
        {
            ibase->termstateio.end = st.st_size;
            size = (off_t)sizeof(TERMSTATE) * (off_t)IB_TERMSTATE_MAX;
            if(st.st_size > size) size = st.st_size;
            if((ibase->termstateio.map = (char *)mmap(NULL, size,
                PROT_READ|PROT_WRITE, MAP_SHARED, ibase->termstateio.fd, 0)) != (void *)-1)
            {
                ibase->termstateio.size = size;
            }
            else
            {
                fprintf(stderr, "mmap termstate file (%s) failed, %s\r\n", path, strerror(errno));
                _exit(-1);
            }
        }
        else
        {
            fprintf(stderr, "open termstateio[%s] failed, %s\n", path, strerror(errno));
            _exit(-1);
        }
        /* globalid to docid */
        sprintf(path, "%s/%s", dir, IB_DOCMAP_NAME);
        if((ibase->docmap = mmtrie_init(path)) == NULL)
        {
            fprintf(stderr, "initialize docmap(%s) failed, %s\r\n", path, strerror(errno));
            _exit(-1);
        }
        /* not for qparserd */
        if(used_for == IB_USED_FOR_INDEXD)
        {
            /* headers */
            sprintf(path, "%s/%s", dir, IB_HEADERS_NAME);
            if((ibase->headersio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                    && fstat(ibase->headersio.fd, &st) == 0)
            {
                ibase->headersio.end = st.st_size;
                size = (off_t)sizeof(IHEADER) * (off_t)IB_HEADERS_MAX;
                if(st.st_size > size) size = st.st_size;
                if((ibase->headersio.map = (char *)mmap(NULL, size,
                                PROT_READ|PROT_WRITE, MAP_SHARED, ibase->headersio.fd, 0)) != (void *)-1)
                {
                    ibase->headersio.size = size;
                }
                else
                {
                    fprintf(stderr, "mmap headers file (%s) failed, %s\r\n", path, strerror(errno));
                    _exit(-1);
                }
            }
            else
            {
                fprintf(stderr, "open headersio[%s] failed, %s\n", path, strerror(errno));
                _exit(-1);
            }
        }
        /* source */
        if(used_for != IB_USED_FOR_QPARSERD)
        {
            sprintf(path, "%s/%s", dir, IB_SOURCE_DIR);
            ibase->state->mmsource_status = mmsource_status;
            if(ibase->state->mmsource_status != IB_MMSOURCE_DISABLED)
            {
                ibase->source = db_init(path, 1);
                ACCESS_LOGGER(ibase->logger, "db_init(%s, 1)", path);
            }
            else
            {
                ibase->source = db_init(path, 0);
                ACCESS_LOGGER(ibase->logger, "db_init(%s, 0)", path);
            }
        }
        /* index */
        if(used_for == IB_USED_FOR_INDEXD)
        {
            /* index db */
            sprintf(path, "%s/%s", dir, IB_INDEX_DIR);
            ibase->index = db_init(path, 1);
            db_set_block_incre_mode(ibase->index, DB_BLOCK_INCRE_DOUBLE);
        }
        sprintf(path, "%s/%s/", dir, IB_IDX_DIR);
        ibase_mkdir(path);
        for(i = IB_INT_OFF; i < IB_INT_TO; i++)
        {
            if(ibase->state->mfields[i])
            {
                sprintf(path, "%s/%s/%d.int", dir, IB_IDX_DIR, i);
                ibase->state->mfields[i] = imap_init(path);
            }
        }
        for(i = IB_LONG_OFF; i < IB_LONG_TO; i++)
        {
            if(ibase->state->mfields[i])
            {
                sprintf(path, "%s/%s/%d.long", dir, IB_IDX_DIR, i);
                ibase->state->mfields[i] = lmap_init(path);
            }
        }
        for(i = IB_DOUBLE_OFF; i < IB_DOUBLE_TO; i++)
        {
            if(ibase->state->mfields[i])
            {
                sprintf(path, "%s/%s/%d.double", dir, IB_IDX_DIR, i);
                ibase->state->mfields[i] = dmap_init(path);
            }
        }
        /* check int/long/double index*/
        /*
        ibase_check_int_index(ibase);
        ibase_check_long_index(ibase);
        ibase_check_double_index(ibase);
        */
        TIMER_SAMPLE(timer);
        DEBUG_LOGGER(ibase->logger, "resume ibase time used:%lld", PT_USEC_U(timer));
        TIMER_CLEAN(timer);
        ret = 0;
    }
    return ret;
}

void ibase_check_int_idx(IBASE *ibase, int no)
{
    char path[IB_PATH_MAX];

    if(ibase && no >= IB_INT_OFF && no < IB_INT_TO && !(ibase->state->mfields[no]))
    {
        sprintf(path, "%s/%s/%d.int", ibase->basedir, IB_IDX_DIR, no);
        ibase->state->mfields[no] = imap_init(path);
    }
    return ;
}

/* check int index  */
int ibase_check_int_index(IBASE *ibase)
{
    char path[IB_PATH_MAX];
    struct stat st = {0};
    off_t size = 0;

    if(ibase->state->used_for == IB_USED_FOR_INDEXD 
            && ibase->state->int_index_fields_num > 0 
            && ibase->intidxio.map == NULL)
    {
        sprintf(path, "%s/%s", ibase->basedir, IB_INTIDX_NAME);
        if((ibase->intidxio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(ibase->intidxio.fd, &st) == 0)
        {
            ibase->intidxio.end = st.st_size;
            size = (off_t)sizeof(int) * (off_t)ibase->state->int_index_fields_num * (off_t)IB_HEADERS_MAX;
            if(st.st_size > size) size = st.st_size;
            if((ibase->intidxio.map = (char *)mmap(NULL, size,
                            PROT_READ|PROT_WRITE, MAP_SHARED, ibase->intidxio.fd, 0)) != (void *)-1)
            {
                ibase->intidxio.size = size;
            }
            else
            {
                fprintf(stderr, "mmap intindex file (%s) failed, %s\r\n", path, strerror(errno));
                _exit(-1);
            }
        }
        else
        {
            fprintf(stderr, "open intidxio[%s] failed, %s\n", path, strerror(errno));
            _exit(-1);
        }
    }
    return 0;
}

/* ibasee  set numeric index /int/double fields */
int ibase_set_int_index(IBASE *ibase, int int_index_from, int int_fields_num)
{
    int i = 0, n = 0;

    if(ibase && ibase->state)
    {
        if(ibase->state->int_index_fields_num != int_fields_num
                && ibase->state->int_index_fields_num != 0)
        {
            FATAL_LOGGER(ibase->logger, "int index num(%d) should be rebuild to num(%d)",
                    ibase->state->int_index_fields_num, int_fields_num);
            _exit(-1);
        }
        if(ibase->state->int_index_fields_num == 0)
        {
            if(ibase->state->used_for == IB_USED_FOR_INDEXD)
            {
                n = IB_INT_OFF + int_fields_num;
                for(i = IB_INT_OFF; i < n; i++)
                {
                    ibase_check_int_idx(ibase, i);
                }
            }
            ibase->state->int_index_from = int_index_from;
            ibase->state->int_index_fields_num = int_fields_num;
        }
        //ibase_check_int_index(ibase);
        return 0;
    }
    return -1;
}

void ibase_check_long_idx(IBASE *ibase, int no)
{
    char path[IB_PATH_MAX];

    if(ibase && no >= IB_LONG_OFF && no < IB_LONG_TO && !(ibase->state->mfields[no]))
    {
        sprintf(path, "%s/%s/%d.long", ibase->basedir, IB_IDX_DIR, no);
        ibase->state->mfields[no] = lmap_init(path);
    }
    return ;
}

/* check long index  */
int ibase_check_long_index(IBASE *ibase)
{
    char path[IB_PATH_MAX];
    struct stat st = {0};
    off_t size = 0;

    if(ibase->state->used_for == IB_USED_FOR_INDEXD 
            && ibase->state->long_index_fields_num > 0
            && ibase->longidxio.map == NULL)
    {
        sprintf(path, "%s/%s", ibase->basedir, IB_LONGIDX_NAME);
        if((ibase->longidxio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(ibase->longidxio.fd, &st) == 0)
        {
            ibase->longidxio.end = st.st_size;
            size = (off_t)sizeof(int64_t) * (off_t)ibase->state->long_index_fields_num * (off_t)IB_HEADERS_MAX;
            if(st.st_size > size) size = st.st_size;
            if((ibase->longidxio.map = (char *)mmap(NULL, size,
                            PROT_READ|PROT_WRITE, MAP_SHARED, ibase->longidxio.fd, 0)) != (void *)-1)
            {
                ibase->longidxio.size = size;
            }
            else
            {
                fprintf(stderr, "mmap longindex file (%s) failed, %s\r\n", path, strerror(errno));
                _exit(-1);
            }
        }
        else
        {
            fprintf(stderr, "open longidxio[%s] failed, %s\n", path, strerror(errno));
            _exit(-1);
        }
    }
    return 0;
} 

/* ibasee  set long index  fields */
int ibase_set_long_index(IBASE *ibase, int long_index_from, int long_fields_num)
{
    int i = 0, n = 0;

    if(ibase && ibase->state)
    {
        if(ibase->state->long_index_fields_num != long_fields_num
                && ibase->state->long_index_fields_num != 0)
        {
            FATAL_LOGGER(ibase->logger, "long index num(%d) should be rebuild to num(%d)",
                    ibase->state->long_index_fields_num, long_fields_num);
            _exit(-1);
        }
        if(ibase->state->long_index_fields_num == 0)
        {
            if(ibase->state->used_for == IB_USED_FOR_INDEXD)
            {
                n = IB_LONG_OFF + long_fields_num;
                for(i = IB_LONG_OFF; i < n; i++)
                {
                    ibase_check_long_idx(ibase, i);
                }
            }
            ibase->state->long_index_from = long_index_from;
            ibase->state->long_index_fields_num = long_fields_num;
        }
        //ibase_check_long_index(ibase);
        return 0;
    }
    return -1;
}

void ibase_check_double_idx(IBASE *ibase, int no)
{
    char path[IB_PATH_MAX];

    if(ibase && no >= IB_DOUBLE_OFF && no < IB_DOUBLE_TO && !(ibase->state->mfields[no]))
    {
        sprintf(path, "%s/%s/%d.double", ibase->basedir, IB_IDX_DIR, no);
        ibase->state->mfields[no] = dmap_init(path);
    }
    return ;
}

/* double index  */
int ibase_check_double_index(IBASE *ibase)
{
    char path[IB_PATH_MAX];
    struct stat st = {0};
    off_t size = 0;

    if(ibase->state->used_for == IB_USED_FOR_INDEXD 
            && ibase->state->double_index_fields_num > 0
            && ibase->doubleidxio.map  == NULL)
    {
        sprintf(path, "%s/%s", ibase->basedir, IB_DOUBLEIDX_NAME);
        if((ibase->doubleidxio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(ibase->doubleidxio.fd, &st) == 0)
        {
            ibase->doubleidxio.end = st.st_size;
            size = (off_t)sizeof(double) * (off_t)ibase->state->double_index_fields_num * (off_t)IB_HEADERS_MAX;
            if(st.st_size > size) size = st.st_size;
            if((ibase->doubleidxio.map = (char *)mmap(NULL, size, PROT_READ|PROT_WRITE,
                            MAP_SHARED, ibase->doubleidxio.fd, 0)) != (void *)-1)
            {
                ibase->doubleidxio.size = size;
            }
            else
            {
                fprintf(stderr, "mmap double index file(%s) failed, %s\r\n", path, strerror(errno));
                _exit(-1);
            }
        }
        else
        {
            fprintf(stderr, "open doubleidxio[%s] failed, %s\n", path, strerror(errno));
            _exit(-1);
        }
    }
    return 0;
}

/* ibasee  set numeric index /int/double fields */
int ibase_set_double_index(IBASE *ibase, int double_index_from, int double_fields_num)
{
    int i = 0, n = 0;
    if(ibase && ibase->state)
    {
        if(ibase->state->double_index_fields_num != double_fields_num
            && ibase->state->double_index_fields_num != 0) 
        {
            FATAL_LOGGER(ibase->logger, "double index num(%d) should be rebuild to num(%d)",
                    ibase->state->double_index_fields_num, double_fields_num);
            _exit(-1);
        }
        if(ibase->state->double_index_fields_num == 0)
        {
            if(ibase->state->used_for == IB_USED_FOR_INDEXD)
            {
                n = IB_DOUBLE_OFF + double_fields_num;
                for(i = IB_DOUBLE_OFF; i < n; i++)
                {
                    ibase_check_double_idx(ibase, i);
                }
            }
            ibase->state->double_index_from = double_index_from;
            ibase->state->double_index_fields_num = double_fields_num;
        }
        //ibase_check_double_index(ibase);
        return 0;
    }
    return -1;
}

/* push mmx */
void ibase_push_mmx(IBASE *ibase, void *mmx)
{
    int x = 0;

    if(ibase && mmx)
    {
        MUTEX_LOCK(ibase->mutex_mmx);
        if(ibase->nqmmxs < IB_MMX_MAX)
        {
            IMMX_RESET(mmx);
            x = ibase->nqmmxs++;
            ibase->qmmxs[x] = mmx;
        }
        else
        {
            IMMX_CLEAN(mmx);
        }
        MUTEX_UNLOCK(ibase->mutex_mmx);
    }
    return ;
}

/* ibase pop mmx */
void *ibase_pop_mmx(IBASE *ibase)
{
    void *mmx = NULL;
    int x = 0;

    if(ibase)
    {
        MUTEX_LOCK(ibase->mutex_mmx);
        if(ibase->nqmmxs > 0)
        {
            x = --(ibase->nqmmxs);
            mmx = ibase->qmmxs[x];
            ibase->qmmxs[x] = NULL;
        }
        else
        {
            mmx = IMMX_INIT();
        }
        MUTEX_UNLOCK(ibase->mutex_mmx);
    }
    return mmx;
}

/* push stree */
void ibase_push_stree(IBASE *ibase, void *stree)
{
    int x = 0;

    if(ibase && stree)
    {
        MUTEX_LOCK(ibase->mutex_stree);
        if(ibase->nqstrees < IB_STREES_MAX)
        {
            mtree64_reset(MTR64(stree));
            x = ibase->nqstrees++;
            ibase->qstrees[x] = stree;
        }
        else
        {
            mtree64_clean(MTR64(stree));
        }
        MUTEX_UNLOCK(ibase->mutex_stree);
    }
    return ;
}

/* ibase pop mtree */
void *ibase_pop_stree(IBASE *ibase)
{
    void *stree = NULL;
    int x = 0;

    if(ibase)
    {
        MUTEX_LOCK(ibase->mutex_stree);
        if(ibase->nqstrees > 0)
        {
            x = --(ibase->nqstrees);
            stree = ibase->qstrees[x];
            ibase->qstrees[x] = NULL;
        }
        else
        {
            stree = mtree64_init();
        }
        MUTEX_UNLOCK(ibase->mutex_stree);
    }
    return stree;
}

/* push iterm */
void ibase_push_itermlist(IBASE *ibase, ITERM *itermlist)
{
    int x = 0;

    if(ibase && itermlist)
    {
        MUTEX_LOCK(ibase->mutex_itermlist);
        if(ibase->nqiterms < IB_QITERMS_MAX)
        {
            x = ibase->nqiterms++;
            ibase->qiterms[x] = itermlist;
        }
        else
        {
            xmm_free(itermlist, sizeof(ITERM) * IB_QUERY_MAX);
        }
        MUTEX_UNLOCK(ibase->mutex_itermlist);
    }
    return ;
}

/* ibase pop iterm */
ITERM *ibase_pop_itermlist(IBASE *ibase)
{
    ITERM *itermlist = NULL;
    int x = 0;

    if(ibase)
    {
        MUTEX_LOCK(ibase->mutex_itermlist);
        if(ibase->nqiterms > 0)
        {
            x = --(ibase->nqiterms);
            itermlist = ibase->qiterms[x];
            memset(itermlist, 0, sizeof(ITERM) * IB_QUERY_MAX);
            ibase->qiterms[x] = NULL;
        }
        else
        {
            itermlist = (ITERM *)xmm_mnew(IB_QUERY_MAX * sizeof(ITERM));
        }
        MUTEX_UNLOCK(ibase->mutex_itermlist);
    }
    return itermlist;
}

/* push chunk */
void ibase_push_chunk(IBASE *ibase, ICHUNK *chunk)
{
    int x = 0;

    if(ibase && chunk)
    {
        MUTEX_LOCK(ibase->mutex_chunk);
        if(ibase->nqchunks < IB_CHUNKS_MAX)
        {
            x = ibase->nqchunks++;
            ibase->qchunks[x] = chunk;
        }
        else
        {
            xmm_free(chunk, sizeof(ICHUNK));
        }
        MUTEX_UNLOCK(ibase->mutex_chunk);
    }
    return ;
}

/* ibase pop chunk */
ICHUNK *ibase_pop_chunk(IBASE *ibase)
{
    ICHUNK *chunk = NULL;
    int x = 0;

    if(ibase)
    {
        MUTEX_LOCK(ibase->mutex_chunk);
        if(ibase->nqchunks > 0)
        {
            x = --(ibase->nqchunks);
            chunk = ibase->qchunks[x];
            ibase->qchunks[x] = NULL;
        }
        else
        {
            chunk = (ICHUNK *)xmm_new(sizeof(ICHUNK));
        }
        MUTEX_UNLOCK(ibase->mutex_chunk);
    }
    return chunk;
}


/* push segmentor */
void ibase_push_segmentor(IBASE *ibase, void *segmentor)
{
    int x = 0;

    if(ibase && segmentor)
    {
        MUTEX_LOCK(ibase->mutex_segmentor);
        if(ibase->nqsegmentors < IB_SEGMENTORS_MAX)
        {
            x = ibase->nqsegmentors++; 
            ibase->qsegmentors[x] = segmentor;
        }
        else
        {
#ifdef HAVE_SCWS
            ((scws_t)segmentor)->d = NULL;
            ((scws_t)segmentor)->r = NULL;
            scws_free((scws_t)segmentor);
#endif
        }
        MUTEX_UNLOCK(ibase->mutex_segmentor);
    }
    return ;
}

/* pop segmentor */
void *ibase_pop_segmentor(IBASE *ibase)
{
    void *segmentor = NULL;
    int x = 0;

    if(ibase && ibase->segmentor)
    {
        MUTEX_LOCK(ibase->mutex_segmentor);
        if(ibase->nqsegmentors > 0)
        {
            x = --(ibase->nqsegmentors);
            segmentor = ibase->qsegmentors[x];
        }
        else
        {
#ifdef HAVE_SCWS
            if((segmentor = scws_new()))
            {
                ((scws_t)segmentor)->d = ((scws_t)ibase->segmentor)->d;
                ((scws_t)segmentor)->r = ((scws_t)ibase->segmentor)->r;
                ((scws_t)segmentor)->mblen = ((scws_t)ibase->segmentor)->mblen;
                //scws_set_charset((scws_t)(segmentor), ibase->dict_charset);
                //scws_set_rule((scws_t)(segmentor), ibase->dict_rules);
                //scws_set_dict((scws_t)(segmentor), ibase->dict_file, SCWS_XDICT_XDB);
                if(ibase->nsegmentors++ > IB_SEGMENTORS_MIN)
                {
                    WARN_LOGGER(ibase->logger, "segmentors:%d", ibase->nsegmentors);
                }
            }
#endif
        }
        MUTEX_UNLOCK(ibase->mutex_segmentor);
    }
    return segmentor;
}
/* query parser */
int ibase_qparser(IBASE *ibase, int fid, char *query_str, char *not_str, IQUERY *query)
{
    int termid = 0, nterm = 0, i = 0, x = 0, found = 0, last = -1, last_no = -1, size = 0,
        n = 0, k = 0, N = 0, z = 0, from = 0, to = 0, min = 0, prevnext = 0, j = 0, 
        max = 0, nqterms = 0, nxqterms = 0, list[IB_QUERY_MAX];
    char line[IB_LINE_MAX], *p = NULL, *pp = NULL, 
         *epp = NULL, *s = NULL, *es = NULL;
    QTERM xqterms[IB_QUERY_MAX], qterms[IB_QUERY_MAX];
    TERMSTATE *termstates = NULL;

#ifdef HAVE_SCWS
    scws_res_t res = NULL, cur = NULL;
    scws_t segmentor = NULL;

    if(ibase && query_str && query && (termstates = ((TERMSTATE *)ibase->termstateio.map)))
    {
        nqterms = query->nqterms;
        ACCESS_LOGGER(ibase->logger, "Ready parse(query_str:%s nsegs:%d)", query_str, ibase->nqsegmentors);
        if((segmentor = (scws_t)ibase_pop_segmentor(ibase)))
        {
            ACCESS_LOGGER(ibase->logger, "starting parse(query_str:%s nsegs:%d)", query_str, ibase->nqsegmentors);
            memcpy(qterms, query->qterms, sizeof(QTERM) * IB_QUERY_MAX);
            memset(xqterms, 0, sizeof(QTERM) * IB_QUERY_MAX);
            for(i = 0; i < nqterms; i++){xqterms[i].id = qterms[i].id;list[i]=i;}
            s = query_str;
            es = s + strlen(query_str);
            /* query string */
            if(es > s)
            {
                scws_send_text(segmentor, s, es - s);
                while ((res = cur = scws_get_result(segmentor)))
                {
                    while (cur != NULL)
                    {
                        pp = s = query_str + cur->off;
                        nterm = cur->len;
                        if(last == cur->off) prevnext = 1;
                        else prevnext = 0;
                        last = cur->off + cur->len;
                        epp = s + nterm;
                        p = line;
                        size = 0;
                        while(pp < epp)
                        {
                            size++;
                            if(*pp >= 'A' && *pp <= 'Z') *p++ = *pp++ + 'a' - 'A';
                            else 
                            {
                                if(*((unsigned char *)pp) > 127)
                                {
                                    n = 0;
                                    if(*((unsigned char*)s) >= 252) n = 6;
                                    else if(*((unsigned char *)s) >= 248) n = 5;
                                    else if(*((unsigned char *)s) >= 240) n = 4;
                                    else if(*((unsigned char *)s) >= 224) n = 3;
                                    else if(*((unsigned char *)s) >= 192) n = 2;
                                    else n = 1;
                                    while(n-- > 0)*p++ = *pp++;
                                    size++;
                                }
                                else
                                {
                                    *p++ = *pp++;
                                }
                            }
                        }
                        *p = '\0';
                        if((nterm = (p - line)) > 0)
                        {
                            if((termid=mmtrie_get((MMTRIE *)(ibase->mmtrie), line, nterm)) > 0)
                            {
                                ACCESS_LOGGER(ibase->logger, "found termid:%d term:%s len:%d in query_str:%s ", termid, line, nterm, query_str);
                                if(nqterms <= IB_QUERY_MAX)
                                {
                                    found = -1;x = -1;
                                    if(nqterms == 0)
                                    {
                                        x = 0;
                                    }
                                    else
                                    {
                                        min = 0;max = nqterms - 1;
                                        from = list[min];
                                        to = list[max];
                                        if(termid == qterms[from].id) found = from;
                                        else if(termid == qterms[to].id) found = to;
                                        else if(max>min && termid>qterms[from].id && termid<qterms[to].id)
                                        {
                                            while(max > min)
                                            {
                                                z = (max + min)/2;
                                                if(z == min){break;}
                                                k = list[z]; 
                                                if(termid == qterms[k].id){found = k;break;}
                                                else if(termid > qterms[k].id) min = z;
                                                else max = z;
                                            }
                                        }
                                    }
                                    if(found < 0)
                                    {
                                        if(nqterms < IB_QUERY_MAX)
                                        {
                                            x = nqterms++;
                                            list[x] = x;
                                            i = x;
                                            while(i > 0)
                                            {
                                                z = list[i - 1];
                                                if(qterms[z].id > termid) 
                                                {
                                                    list[i] = z;
                                                    list[i-1] = x;
                                                }
                                                else break;
                                                --i;
                                            }
                                        }
                                    }
                                    else 
                                    {
                                        x = found;
                                    }
                                    if(x >= 0 && x < IB_QUERY_MAX)
                                    {
                                        qterms[x].id = termid;
                                        qterms[x].size = size;
                                        if(fid >=0)
                                        {
                                            query->flag |= IB_QUERY_FIELDS;
                                            qterms[x].bithit |= 1 << fid;
                                        }
                                        qterms[x].flag |= QTERM_BIT_AND;
                                        if(prevnext && last_no >= 0)
                                        {
                                            qterms[x].prev |= 1 << last_no;
                                            qterms[last_no].next |= 1 << x;
                                        }
                                        last_no = x;
                                    }
                                }
                                ACCESS_LOGGER(ibase->logger, "found termid:%d term:%s len:%d in query_str:%s ", termid, line, nterm, query_str);
                            }
                            else if((termid = mmtrie_xadd((MMTRIE *)(ibase->xmmtrie), line, nterm)) > 0)
                            {
                                ACCESS_LOGGER(ibase->logger, "term:%s len:%d in query_str:%s not found", line, nterm, query_str);
                                x = -1;
                                for(i = 0; i < nxqterms; i++)
                                {
                                    if(xqterms[i].id == termid)
                                    {
                                        x = i;
                                        break;
                                    }
                                }
                                if(x == -1 && (x = nxqterms++) < IB_QUERY_MAX) 
                                {
                                    xqterms[x].id = termid;
                                }
                            }
                        }
                        cur = cur->next;
                    }
                    scws_free_result(res);
                }
            }
            /* not str */
            if((s = not_str) && (es = (not_str + strlen(not_str))) > s)
            {
                scws_send_text(segmentor, s, es - s);
                while ((res = cur = scws_get_result(segmentor)))
                {
                    while (cur != NULL)
                    {
                        pp = s = not_str + cur->off;
                        nterm = cur->len;
                        epp = s + nterm;
                        p = line;
                        while(pp < epp)
                        {
                            if(*pp >= 'A' && *pp <= 'Z') *p++ = *pp + 'a' - 'A';
                            else *p++ = *pp;
                            ++pp;
                        }
                        *p = '\0';

                        if((nterm  = (p - line)) > 0)
                        {
                            if((termid=mmtrie_get((MMTRIE *)(ibase->mmtrie), line, nterm)) > 0)
                            {
                                ACCESS_LOGGER(ibase->logger, "found termid:%d term:%s len:%d in not_str:%s ", termid, line, nterm, not_str);
                                if(nqterms <= IB_QUERY_MAX)
                                {
                                    found = -1;x = -1;
                                    if(nqterms == 0)
                                    {
                                        x = 0;
                                    }
                                    else
                                    {
                                        min = 0;max = nqterms - 1;
                                        from = list[min];
                                        to = list[max];
                                        if(termid == qterms[from].id) found = from;
                                        else if(termid == qterms[to].id) found = to;
                                        else if(max>min&& termid>qterms[from].id && termid<qterms[to].id)
                                        {
                                            while(max > min)
                                            {
                                                z = (max + min)/2;
                                                if(z == min){break;}
                                                k = list[z]; 
                                                if(termid == qterms[k].id){found = k;break;}
                                                else if(termid > qterms[k].id) min = z;
                                                else max = z;
                                            }
                                        }
                                    }
                                    if(found < 0)
                                    {
                                        if(nqterms < IB_QUERY_MAX)
                                        {
                                            x = nqterms++;
                                            list[x] = x;
                                            i = x;
                                            while(i > 0)
                                            {
                                                z = list[i - 1];
                                                if(qterms[z].id > termid) 
                                                {
                                                    list[i] = z;
                                                    list[i-1] = x;
                                                }
                                                else break;
                                                --i;
                                            }
                                        }
                                    }
                                    else 
                                    {
                                        x = found;
                                    }
                                    if(x >= 0 && x < IB_QUERY_MAX)
                                    {
                                        qterms[x].id = termid;
                                        if(fid >=0)
                                        {
                                            query->flag |= IB_QUERY_FIELDS;
                                            qterms[x].bitnot |= 1 << fid;
                                        }
                                        else
                                        {
                                            qterms[x].flag |= QTERM_BIT_NOT;
                                        }
                                    }
                                }
                            }
                            else if((termid = mmtrie_xadd((MMTRIE *)(ibase->xmmtrie), line, nterm)) > 0)
                            {
                                ACCESS_LOGGER(ibase->logger, "term:%s len:%d in not_str:%s not found", line, nterm, not_str);
                                x = -1;
                                for(i = 0; i < nxqterms; i++)
                                {
                                    if(xqterms[i].id == termid)
                                    {
                                        x = i;
                                        break;
                                    }
                                }
                                if(x == -1 && (x = nxqterms++) < IB_QUERY_MAX) 
                                {
                                    xqterms[x].id = termid;
                                }
                            }
                        }
                        cur = cur->next;
                    }
                    scws_free_result(res);
                }
            }
            ibase_push_segmentor(ibase, segmentor);
            ACCESS_LOGGER(ibase->logger, "over parse(query_str:%s nsegs:%d nqterms:%d)", query_str, ibase->nqsegmentors, nqterms);
        }
        if(nqterms > 0)
        {
            N = ibase->state->dtotal;
            ACCESS_LOGGER(ibase->logger, "starting state-idf(query_str:%s nqterms:%d dtotal:%lld ttotal:%lld nsegs:%d)", query_str, nqterms, (long long)N, (long long)ibase->state->ttotal, ibase->nqsegmentors);
            query->ravgdl = 1.0;
            if(ibase->state->ttotal != 0)
                query->ravgdl = (double)ibase->state->dtotal/(double)ibase->state->ttotal;
            /* relist */
            if(nqterms > IB_QUERY_MAX) nqterms = IB_QUERY_MAX;
            //MUTEX_LOCK(ibase->mutex_termstate);
            i = 0, x = 0;
            for(i = 0; i < nqterms; ++i)
            {
                z = list[i];
                termid = query->qterms[i].id  = qterms[z].id;
                query->qterms[i].size = qterms[z].size;
                query->qterms[i].bitnot = qterms[z].bitnot;
                query->qterms[i].bithit = qterms[z].bithit;
                //fprintf(stdout, "i:%d termid:%d\n", i, qterms[z].id);
                if(termid <= ibase->state->termid && (n = (termstates[termid].total)) > 0)
                {
                    query->qterms[i].idf = log(((double)N-(double )n+0.5f)/((double)n + 0.5f));
                    ACCESS_LOGGER(ibase->logger, "terms[%d] n:%lld N:%lld idf:%f", termid, (long long)n, (long long)N, query->qterms[i].idf);
                    if(query->qterms[i].idf < 0) query->qterms[i].idf = 1.0f;
                    if(termstates[termid].status == IB_BTERM_BLOCK)
                    {
                        query->flag |= IB_QUERY_FORBIDDEN;
                    }
                    else if(termstates[termid].status == IB_BTERM_DOWN 
                            && query->qweight)
                    {
                        query->qterms[i].flag |= QTERM_BIT_DOWN;
                        //fprintf(stdout, "%s::%d i:%d\n", __FILE__, __LINE__, i);
                    }
                    else
                    {
                        if(!(qterms[z].flag & QTERM_BIT_NOT)) query->nvqterms++;
                    }
                }
                if(qterms[z].flag & QTERM_BIT_AND) query->operators.bitsand |= 1 << i;
                if(qterms[z].flag & QTERM_BIT_NOT) query->operators.bitsnot |= 1 << i;
                if(qterms[z].prev || qterms[z].next) 
                {
                    for(j = 0; j < nqterms; j++)
                    {
                        k = list[j];
                        if(qterms[z].prev & (1 << k)) 
                        {
                            //fprintf(stdout, "prev z:%d k:%d i:%d j:%d\n", z, k, i, j);
                            query->qterms[i].prev |= 1 << j;
                        }
                        if(qterms[z].next & (1 << k)) 
                        {
                            //fprintf(stdout, "next z:%d k:%d i:%d j:%d\n", z, k, i, j);
                            query->qterms[i].next |= 1 << j;
                        }
                    }
                }
            }
            //MUTEX_UNLOCK(ibase->mutex_termstate);
            query->nqterms = nqterms;
            query->nquerys = query->nqterms + nxqterms;
            //if(nxqterms > query->nqterms)
            //query->nquerys = query->nqterms;
            ACCESS_LOGGER(ibase->logger, "over state-idf(query_str:%s nsegs:%d)", query_str, ibase->nqsegmentors);
        }
        return query->nqterms;
    }
#else
    return ibase_qparser_X(ibase, query_str, not_str, query);
#endif
    return 0;
}

/* set index status */
int ibase_set_index_status(IBASE *ibase, int status)
{
    if(ibase && ibase->state)
    {
        ibase->state->index_status = status;
        return 0;
    }
    return -1;
}

/* set phrase status */
int ibase_set_phrase_status(IBASE *ibase, int status)
{
    if(ibase && ibase->state)
    {
        ibase->state->phrase_status = status;
        return 0;
    }
    return -1;
}

/* get docid with globalid */
int ibase_docid(IBASE *ibase, int64_t globalid)
{
    char line[IB_LINE_MAX];
    int docid = 0, n = 0;

    if(ibase && ibase->docmap && globalid && (n = sprintf(line, "%lld", IBLL(globalid))) >0)
        docid = mmtrie_get(ibase->docmap, line, n);
    return docid;
}

/* updated terms status in the document */
int ibase_update_terms_modtime(IBASE *ibase, int docid)
{
    int n = 0, off = 0, size = 0;
    TERMSTATE *termstate = NULL;
    DOCHEADER *docheader = NULL;
    //IHEADER *headers = NULL;
    STERM *termlist = NULL;
    char *data = NULL;

    if(ibase && docid > 0 && ibase->state && ibase->state->index_status != IB_INDEX_DISABLED
            && (n = db_get_data(PDB(ibase->source), docid, &data)) > 0)
    {
        MUTEX_LOCK(ibase->mutex_termstate);
        if((termstate = (TERMSTATE *)(ibase->termstateio.map)))
        {
            if((docheader = (DOCHEADER *)data) && n >= sizeof(DOCHEADER)
                    && (off = sizeof(DOCHEADER) + sizeof(XFIELD) * docheader->nfields) > 0
                    && (size = (off + docheader->nterms * sizeof(STERM))) <= n
                    && (termlist = (STERM *)(data + off)))
            {
                /*
                for(i = 0; i < docheader->nterms; i++)
                {
                    if((id = termlist[i].termid) <= ibase->state->termid)
                        termstate[id].mod_time = time(NULL);
                }
                */
            }
        }
        MUTEX_UNLOCK(ibase->mutex_termstate);
        db_free_data(PDB(ibase->source), data, n);
        return 0;
    }
    return -1;
}
/* set xheader */
int ibase_set_xheader(IBASE *ibase, XHEADER *xheader)
{
    int docid = 0, ret = -1;
    int64_t globalid = -1;
    IHEADER *headers = NULL;

    if(ibase && ibase->state->used_for == IB_USED_FOR_INDEXD 
            && xheader && (globalid = (int64_t)xheader->globalid))
    {
        MUTEX_LOCK(ibase->mutex);
        if((headers = (IHEADER *)(ibase->headersio.map)) 
                && (docid = ibase_docid(ibase, globalid)) > 0 
                && docid <= ibase->state->docid)
        {
            headers[docid].rank      = xheader->rank;
            headers[docid].category  = xheader->category;
            headers[docid].slevel    = xheader->slevel;
            ret = 0;
            //ret = ibase_update_terms_modtime(ibase, docid);
        }
        MUTEX_UNLOCK(ibase->mutex);
    }
    return ret;
}

/* set int field */
int ibase_set_int_fields(IBASE *ibase, int64_t globalid, int *vals)
{
    int docid = 0, *intidx = NULL, no = -1, ret = -1;

    if(ibase && ibase->state->used_for == IB_USED_FOR_INDEXD && globalid)
    {
        MUTEX_LOCK(ibase->mutex);
        if((docid = ibase_docid(ibase, globalid)) > 0 && docid <= ibase->state->docid 
                && ibase->state->int_index_fields_num > 0
                && (intidx = (int *)(ibase->intidxio.map)))
        {
            no = ibase->state->int_index_fields_num * docid;
            memcpy(&(intidx[no]), vals, sizeof(int) * ibase->state->int_index_fields_num);
            ret = 0;
            //ret = ibase_update_terms_modtime(ibase, docid);
        }
        MUTEX_UNLOCK(ibase->mutex);
    }
    return ret;
}

/* set long field */
int ibase_set_long_fields(IBASE *ibase, int64_t globalid, int64_t *vals)
{
    int docid = 0, no = -1, ret = -1;
    int64_t *longidx = NULL;

    if(ibase && ibase->state->used_for == IB_USED_FOR_INDEXD && globalid)
    {
        MUTEX_LOCK(ibase->mutex);
        if((docid = ibase_docid(ibase, globalid)) > 0 && docid <= ibase->state->docid 
                && ibase->state->long_index_fields_num > 0
                && (longidx = (int64_t *)(ibase->longidxio.map)))
        {
            no = ibase->state->long_index_fields_num * docid;
            memcpy(&(longidx[no]), vals, sizeof(int64_t) * ibase->state->long_index_fields_num);
            ret = 0;
            //ret = ibase_update_terms_modtime(ibase, docid);
        }
        MUTEX_UNLOCK(ibase->mutex);
    }
    return ret;
}

/* set double fields */
int ibase_set_double_fields(IBASE *ibase, int64_t globalid, double *vals)
{
    int docid = 0, no = -1, ret = -1;
    double *doubleidx = NULL;

    if(ibase && ibase->state->used_for == IB_USED_FOR_INDEXD && globalid && vals)
    {
        MUTEX_LOCK(ibase->mutex);
        if((docid = ibase_docid(ibase, globalid)) > 0 && docid <= ibase->state->docid 
                && ibase->state->double_index_fields_num > 0
                && (doubleidx = (double *)(ibase->doubleidxio.map)))
        {
            no = ibase->state->double_index_fields_num * docid;
            memcpy(&(doubleidx[no]), vals, sizeof(double) * ibase->state->double_index_fields_num);
            ret = 0;
            //ret = ibase_update_terms_modtime(ibase, docid);
        }
        MUTEX_UNLOCK(ibase->mutex);
    }
    return ret;
}

/* set rank */
int ibase_set_rank(IBASE *ibase, int64_t globalid, double rank)
{
    int docid = 0, ret = -1;
    IHEADER *headers = NULL;

    if(ibase && ibase->state->used_for == IB_USED_FOR_INDEXD && globalid)
    {
        MUTEX_LOCK(ibase->mutex);
        if((headers = (IHEADER *)(ibase->headersio.map)) 
                && (docid = ibase_docid(ibase, globalid)) > 0 
                && docid <= ibase->state->docid)
        {
            headers[docid].rank = rank;
            ACCESS_LOGGER(ibase->logger, "set_rank(%f) to docid[%d/%d]", rank, globalid, docid);
            ret = 0;
            //ret = ibase_update_terms_modtime(ibase, docid);
        }
        MUTEX_UNLOCK(ibase->mutex);
    }
    return ret;
}

/* set category */
int ibase_set_category(IBASE *ibase, int64_t globalid, int64_t category)
{
    int docid = 0, ret = -1;
    IHEADER *headers = NULL;

    if(ibase && ibase->state->used_for == IB_USED_FOR_INDEXD && globalid)
    {
        MUTEX_LOCK(ibase->mutex);
        if((headers = (IHEADER *)(ibase->headersio.map)) 
                && (docid = ibase_docid(ibase, globalid)) > 0 
                && docid <= ibase->state->docid)
        {
            headers[docid].category = category;
            ACCESS_LOGGER(ibase->logger, "set_category(%lld) to docid[%d/%d]", IBLL(category), globalid, docid);
            //ibase_update_terms_modtime(ibase, docid);
            ret = 0;
        }
        MUTEX_UNLOCK(ibase->mutex);
    }
    return ret;
}

/* set slevel */
int ibase_set_slevel(IBASE *ibase, int64_t globalid, int slevel)
{
    int docid = 0, ret = -1;
    IHEADER *headers = NULL;

    if(ibase && ibase->state->used_for == IB_USED_FOR_INDEXD && globalid)
    {
        MUTEX_LOCK(ibase->mutex);
        if((headers = (IHEADER *)(ibase->headersio.map)) 
                && (docid = ibase_docid(ibase, globalid)) > 0 
                && docid <= ibase->state->docid)
        {
            headers[docid].slevel = slevel;
            ACCESS_LOGGER(ibase->logger, "set_slevel(%d) to docid[%lld/%d]", slevel, IBLL(globalid), docid);
            //ibase_update_terms_modtime(ibase, docid);
            ret = 0;
        }
        MUTEX_UNLOCK(ibase->mutex);
    }
    return ret;
}

/* set int field */
int ibase_set_int_field(IBASE *ibase, int64_t globalid, int field_no, int val)
{
    int docid = 0, *intidx = NULL, no = -1, ret = -1;

    if(ibase && ibase->state->used_for == IB_USED_FOR_INDEXD && globalid)
    {
        MUTEX_LOCK(ibase->mutex);
        no = field_no - ibase->state->int_index_from;
        if((docid = ibase_docid(ibase, globalid)) > 0 && docid <= ibase->state->docid 
                && ibase->state->int_index_fields_num > 0
                && (intidx = (int *)(ibase->intidxio.map))
                && no >= 0 && no < ibase->state->int_index_fields_num)
        {
            no += ibase->state->int_index_fields_num * docid;
            intidx[no] = val;
            DEBUG_LOGGER(ibase->logger, "set_int_field(%d => %d) to docid[%lld/%d]", field_no, val, IBLL(globalid), docid);
            //ibase_update_terms_modtime(ibase, docid);
            ret = 0;
        }
        MUTEX_UNLOCK(ibase->mutex);
    }
    return ret;
}

/* set long field */
int ibase_set_long_field(IBASE *ibase, int64_t globalid, int field_no, int64_t val)
{
    int docid = 0, no = -1, ret = -1;
    int64_t *longidx = NULL;

    if(ibase && ibase->state->used_for == IB_USED_FOR_INDEXD && globalid)
    {
        MUTEX_LOCK(ibase->mutex);
        no = field_no - ibase->state->long_index_from;
        if((docid = ibase_docid(ibase, globalid)) > 0 && docid <= ibase->state->docid 
                && ibase->state->long_index_fields_num > 0
                && (longidx = (int64_t *)(ibase->longidxio.map))
                && no >= 0 && no < ibase->state->long_index_fields_num)
        {
            no += ibase->state->long_index_fields_num * docid;
            longidx[no] = val;
            DEBUG_LOGGER(ibase->logger, "set_long_field(%d => %lld) to docid[%lld/%d]", field_no, (long long)val, IBLL(globalid), docid);
            //ibase_update_terms_modtime(ibase, docid);
            ret = 0;
        }
        MUTEX_UNLOCK(ibase->mutex);
    }
    return ret;
}

/* set double field */
int ibase_set_double_field(IBASE *ibase, int64_t globalid, int field_no, double val)
{
    int docid = 0, no = -1, ret = -1;
    double *doubleidx = NULL;

    if(ibase && ibase->state->used_for == IB_USED_FOR_INDEXD && globalid && field_no >= 0)
    {
        MUTEX_LOCK(ibase->mutex);
        no = field_no - ibase->state->double_index_from;
        if((docid = ibase_docid(ibase, globalid)) > 0 && docid <= ibase->state->docid 
                && ibase->state->double_index_fields_num > 0
                && (doubleidx = (double *)(ibase->doubleidxio.map))
                && no >= 0 && no < ibase->state->double_index_fields_num)
        {
            no += ibase->state->double_index_fields_num * docid;
            doubleidx[no] = val;
            DEBUG_LOGGER(ibase->logger, "set_double_field(%d => %f) to docid[%lld/%d]", field_no, val, IBLL(globalid), docid);
            //ibase_update_terms_modtime(ibase, docid);
            ret = 0;
        }
        MUTEX_UNLOCK(ibase->mutex);
    }
    return ret;
}

/* set index compress status */
int ibase_set_compression_status(IBASE *ibase, int status)
{
    if(ibase && ibase->state)
    {
        ibase->state->compression_status = status;
        return 0;
    }
    return -1;
}

/* set  source file mmap status */
int ibase_set_mmsource_status(IBASE *ibase, int status)
{
    if(ibase && ibase->state)
    {
        ibase->state->mmsource_status = status;
        return 0;
    }
    return -1;
}


/* enable document */
int ibase_enable_document(IBASE *ibase, int64_t globalid)
{
    IHEADER *headers = NULL;
    int docid = 0;

    if(ibase && ibase->state->used_for == IB_USED_FOR_INDEXD && globalid)
    {
        MUTEX_LOCK(ibase->mutex);
        if((docid = ibase_docid(ibase, globalid))>= 0 && docid <= ibase->state->docid 
                && (headers = (IHEADER *)ibase->headersio.map))
        {
            headers[docid].status = 0;
            //headers[docid].globalid &= ~(1 << 31); 
            //ibase_update_terms_modtime(ibase, docid);
        }
        MUTEX_UNLOCK(ibase->mutex);
    }
    return 0;
}

/* disable document */
int ibase_disable_document(IBASE *ibase, int64_t globalid)
{
    IHEADER *headers = NULL;
    int docid = 0;

    if(ibase && ibase->state->used_for == IB_USED_FOR_INDEXD && globalid)
    {
        MUTEX_LOCK(ibase->mutex);
        if((docid = ibase_docid(ibase, globalid)) >= 0 && docid <= ibase->state->docid 
                && (headers = (IHEADER *)ibase->headersio.map))
        {
            headers[docid].status = -1;
            //headers[docid].globalid |= 1 << 31; 
            //ibase_update_terms_modtime(ibase, docid);
        }
        MUTEX_UNLOCK(ibase->mutex);
    }
    return 0;
}

/* enable term */
int ibase_enable_term(IBASE *ibase, int termid)
{
    TERMSTATE *termstatelist = NULL;

    if(ibase && termid >= 0)
    {
        MUTEX_LOCK(ibase->mutex_termstate);
        if(termid <= ibase->state->termid && (termstatelist = (TERMSTATE *)ibase->termstateio.map))
        {
            termstatelist[termid].status = 0;
            //termstatelist[termid].mod_time = time(NULL);
        }
        MUTEX_UNLOCK(ibase->mutex_termstate);
    }
    return 0;
}

/* disable term */
int ibase_disable_term(IBASE *ibase, int termid)
{
    TERMSTATE *termstatelist = NULL;

    if(ibase && termid >= 0)
    {
        MUTEX_LOCK(ibase->mutex_termstate);
        if(termid <= ibase->state->termid && (termstatelist = (TERMSTATE *)ibase->termstateio.map))
        {
            termstatelist[termid].status = -1;
            //termstatelist[termid].mod_time = time(NULL);
        }
        MUTEX_UNLOCK(ibase->mutex_termstate);
    }
    return 0;
}
#define ADD_TERMSTATE(ibase, xtermid)                                                       \
do                                                                                          \
{                                                                                           \
    if(ibase->state && ibase->termstateio.fd > 0)                                           \
    {                                                                                       \
        if(((off_t)xtermid * (off_t)sizeof(TERMSTATE)) >= ibase->termstateio.end)           \
        {                                                                                   \
            ibase->termstateio.old = ibase->termstateio.end;                                \
            ibase->termstateio.end = ((off_t)((xtermid / IB_TERMSTATE_BASE) + 1)            \
                * (off_t)IB_TERMSTATE_BASE * (off_t)sizeof(TERMSTATE));                     \
            if(ftruncate(ibase->termstateio.fd, ibase->termstateio.end) != 0)break;         \
            memset(ibase->termstateio.map+ibase->termstateio.old, 0,                        \
                    ibase->termstateio.end - ibase->termstateio.old);                       \
        }                                                                                   \
        if(xtermid > ibase->state->termid) ibase->state->termid = xtermid;                  \
    }                                                                                       \
}while(0)
/* block term */
int ibase_update_bterm(IBASE *ibase, BTERM *bterm, char *term)
{
    TERMSTATE *termstatelist = NULL;

    if(ibase && bterm && bterm->id > 0 && bterm->len > 0 && term 
            && (mmtrie_add((MMTRIE *)ibase->mmtrie, term, bterm->len, bterm->id))>0)
    {
        MUTEX_LOCK(ibase->mutex_termstate);
        if(bterm->id > ibase->state->termid){ADD_TERMSTATE(ibase, bterm->id);}
        if((termstatelist = (TERMSTATE *)ibase->termstateio.map))
        {
            termstatelist[bterm->id].status = bterm->status;
            //fprintf(stdout, "%s::%d termid:%d status:%d\n", __FILE__, __LINE__, bterm->id, bterm->status);
        }
        MUTEX_UNLOCK(ibase->mutex_termstate);
    }
    return 0;
}


/* set log level */
int ibase_set_log_level(IBASE *ibase, int level)
{
    if(ibase && ibase->logger)
    {
        LOGGER_SET_LEVEL(ibase->logger, level);
        if(ibase->index){LOGGER_SET_LEVEL(PDB(ibase->index)->logger, level);}
        if(ibase->source){LOGGER_SET_LEVEL(PDB(ibase->source)->logger, level);}
    }
    return 0;
}

/* ibase clean */
void ibase_clean(IBASE *ibase)
{
    int i = 0; 

    if(ibase)
    {
        if(ibase->index) db_clean(PDB(ibase->index));
        /* source */
        if(ibase->source) db_clean(PDB(ibase->source));
        //state
        if(ibase->stateio.map)
        {
            munmap(ibase->stateio.map, ibase->stateio.size);
        }
        if(ibase->stateio.fd > 0)close(ibase->stateio.fd);
        //headers
        if(ibase->headersio.map)
        {
            munmap(ibase->headersio.map, ibase->headersio.size);
        }
        if(ibase->headersio.fd > 0)close(ibase->headersio.fd);
        if(ibase->termstateio.map)
        {
            munmap(ibase->termstateio.map, ibase->termstateio.size);
        }
        if(ibase->termstateio.fd > 0)close(ibase->termstateio.fd);
        /*
        if(ibase->intidxio.map)
        {
            munmap(ibase->intidxio.map, ibase->intidxio.size);
        }
        if(ibase->intidxio.fd > 0)close(ibase->intidxio.fd);
        if(ibase->doubleidxio.map)
        {
            munmap(ibase->doubleidxio.map, ibase->doubleidxio.size);
        }
        if(ibase->doubleidxio.fd > 0)close(ibase->doubleidxio.fd);
        */
        for(i = IB_INT_OFF; i < IB_INT_TO; i++){if(ibase->state->mfields[i]) imap_close(ibase->state->mfields[i]);}
        for(i = IB_LONG_OFF; i < IB_LONG_TO; i++){if(ibase->state->mfields[i]) lmap_close(ibase->state->mfields[i]);}
        for(i = IB_DOUBLE_OFF; i < IB_DOUBLE_TO; i++){if(ibase->state->mfields[i]) dmap_close(ibase->state->mfields[i]);}
        for(i = 0; i < ibase->nqblocks; i++){xmm_free(ibase->qblocks[i], IB_DOCUMENT_MAX);}
        for(i = 0; i < ibase->nqiblocks; i++){xmm_free(ibase->qiblocks[i], sizeof(IBLOCK));}
        for(i = 0; i < ibase->nqiterms; i++){xmm_free(ibase->qiterms[i], sizeof(ITERM) * IB_QUERY_MAX);}
        for(i = 0; i < ibase->nqxmaps; i++){xmm_free(ibase->qxmaps[i], sizeof(XMAP));}
        for(i = 0; i < ibase->nqstrees; i++){mtree64_clean((MTR64(ibase->qstrees[i])));}
        for(i = 0; i < ibase->nqmmxs; i++){IMMX_CLEAN(ibase->qmmxs[i]);}
        for(i = 0; i < ibase->nqchunks; i++){xmm_free(ibase->qchunks[i], sizeof(ICHUNK));}
#ifdef HAVE_SCWS
        for(i = 0; i < ibase->nqsegmentors; i++)
        {
            ((scws_t)ibase->qsegmentors[i])->d = NULL;
            ((scws_t)ibase->qsegmentors[i])->r = NULL;
            scws_free((scws_t)(ibase->qsegmentors[i]));
        }
        if(ibase->segmentor) scws_free((scws_t)ibase->segmentor);
#endif
        if(ibase->mmtrie) mmtrie_clean((MMTRIE *)ibase->mmtrie);
        if(ibase->xmmtrie) mmtrie_clean((MMTRIE *)ibase->xmmtrie);
        if(ibase->docmap) mmtrie_clean(ibase->docmap);
        MUTEX_DESTROY(ibase->mutex);
        MUTEX_DESTROY(ibase->mutex_itermlist);
        MUTEX_DESTROY(ibase->mutex_chunk);
        MUTEX_DESTROY(ibase->mutex_block);
        MUTEX_DESTROY(ibase->mutex_iblock);
        MUTEX_DESTROY(ibase->mutex_stree);
        MUTEX_DESTROY(ibase->mutex_mmx);
        MUTEX_DESTROY(ibase->mutex_xmap);
        MUTEX_DESTROY(ibase->mutex_record);
        MUTEX_DESTROY(ibase->mutex_segmentor);
        MUTEX_DESTROY(ibase->mutex_termstate);
        if(ibase->logger){LOGGER_CLEAN(ibase->logger);}
        free(ibase);
        ibase = NULL;
    }
    return ;
}

/* ibase initialize */
IBASE *ibase_init()
{
    IBASE *ibase = NULL;

    if((ibase = (IBASE *)calloc(1, sizeof(IBASE))))
    {
        //KVMAP_INIT(ibase->keymap);
        MUTEX_INIT(ibase->mutex);
        MUTEX_INIT(ibase->mutex_itermlist);
        MUTEX_INIT(ibase->mutex_chunk);
        MUTEX_INIT(ibase->mutex_block);
        MUTEX_INIT(ibase->mutex_iblock);
        MUTEX_INIT(ibase->mutex_stree);
        MUTEX_INIT(ibase->mutex_mmx);
        MUTEX_INIT(ibase->mutex_xmap);
        MUTEX_INIT(ibase->mutex_record);
        MUTEX_INIT(ibase->mutex_segmentor);
        MUTEX_INIT(ibase->mutex_termstate);
        ibase->set_basedir              = ibase_set_basedir;
        ibase->set_int_index            = ibase_set_int_index;
        ibase->set_long_index           = ibase_set_long_index;
        ibase->set_double_index         = ibase_set_double_index;
        ibase->add_document             = ibase_add_document;
        ibase->enable_document          = ibase_enable_document;
        ibase->disable_document         = ibase_disable_document;
        ibase->enable_term              = ibase_enable_term;
        ibase->disable_term             = ibase_disable_term;
        ibase->set_xheader              = ibase_set_xheader;
        ibase->set_int_fields           = ibase_set_int_fields;
        ibase->set_long_fields          = ibase_set_long_fields;
        ibase->set_double_fields        = ibase_set_double_fields;
        ibase->set_rank                 = ibase_set_rank;
        ibase->set_category             = ibase_set_category;
        ibase->set_slevel               = ibase_set_slevel;
        ibase->set_int_field            = ibase_set_int_field;
        ibase->set_long_field           = ibase_set_long_field;
        ibase->set_double_field         = ibase_set_double_field;
        ibase->qparser                  = ibase_qparser;
        ibase->set_index_status         = ibase_set_index_status;
        ibase->set_phrase_status        = ibase_set_phrase_status;
        ibase->set_compression_status   = ibase_set_compression_status;
        ibase->set_mmsource_status      = ibase_set_mmsource_status;
        ibase->set_log_level            = ibase_set_log_level;
        ibase->read_summary             = ibase_read_summary;
        ibase->clean                    = ibase_clean;
    }
    return ibase;
}

#ifdef _DEBUG_IBASE
#include <getopt.h>
static struct option long_options[] = {
    {"index", 0, 0, 'i'},
    {"query", 0, 0, 'q'},
    {"basedir", 1, 0, 'd'},
    {"qstring", 1, 0, 's'},
    {"topN", 1, 0, 'n'},
    {"fields", 1, 0, 'f'},
    {"list", 1, 0, 'l'},
    {0, 0, 0, 0}
};
#define Usage(S)                                                                        \
do                                                                                      \
{                                                                                       \
    fprintf(stderr, "Usage:%s \n"                                                       \
    "\t--basedir=ibase basedir \n"                                                      \
    "\t--index \n"                                                                      \
    "\t--query \n"                                                                      \
    "\t--qstring=query string\n"                                                        \
    "\t--fields=query fields\n"                                                         \
    "\t--topN=top record(s) number\n"                                                   \
    "\t--list=index source file(s) list delimiter with ',' or ' '\n", S);               \
    _exit(-1);                                                                          \
}while(0)
/* testing */
//gcc -o hibase *.c utils/*.c -I utils/ -D_DEBUG -D_FILE_OFFSET_BITS=64 -D_DEBUG_IBASE  -lz -lm
int main(int argc, char **argv)
{
    int i = 0, n = 0, fd = 0, action_index = 0, action_query = 0,
        topN = 0, fields = 0, option_index = 0;
    char *basedir = NULL, *query_str = NULL, c = 0, *p = NULL,
         *ep = NULL, *file = NULL, *summary = NULL;
    IRECORD *records = NULL;
    IQUERY query = {0};
    IRES *res = NULL;
    IBDATA block = {0};
    IBASE *ibase = NULL;
    void *timer = NULL;

    while((c = getopt_long(argc, argv, "d:s:n:f:liq",
            long_options, &option_index)) != -1)
    {
        switch(c)
        {
            case 'i' :
                action_index = 1;
                break;
            case 'q' :
                action_query = 1;
                break;
            case 'd' :
                basedir = optarg;
                break;
            case 's' :
                query_str = optarg;
                break;
            case 'n' :
                topN = atoi(optarg);
                break;
            case 'f' :
                fields = atoi(optarg);
                break;
            case 'l' :
                p = optarg;
                break;
            default :
                break;
        }
    }
    if(basedir == NULL || (action_index == 0 && action_query == 0)) {Usage(argv[0]);}
    if(action_index && p == NULL) {Usage(argv[0]);}
    if(action_query && query_str == NULL) {Usage(argv[0]);}
    if(topN <= 0) topN = 200;
    //if(fields == 0) fields = IB_FIELD_NULL;
    if((ibase = ibase_init()))
    {
        TIMER_INIT(timer);
        ibase->set_basedir(ibase, basedir);
        ibase->set_int_index(ibase, basedir, 3, 6);
        TIMER_SAMPLE(timer);
        /* index */
        if(action_index)
        {
            ep = p;
            while(*p != '\0' || ep)
            {
                if(*p == ',' || *p == ' ' )
                {
                    *p = '\0';
                    file = ep;
                    ep = ++p;
                }else ++p;
                if(*p == '\0') file = ep;
                if(file && (fd = ibase_ncopen(file, O_RDONLY, 0644)) > 0)
                {
                    while(read(fd, &(block.ndata), sizeof(int)) > 0
                            && (block.data = (char *)calloc(1, block.ndata))
                            && read(fd, block.data, block.ndata) > 0)
                    {
                        ++n;
                        ibase->add_document(ibase, &block);
                        free(block.data);
                        memset(&block, 0, sizeof(IBDATA));
                    }
                    if(block.data) free(block.data);
                    memset(&block, 0, sizeof(IBDATA));
                    close(fd);
                    file = ep = NULL;
                }
            }
            TIMER_SAMPLE(timer);
            fprintf(stdout, "index %d document(s) used:%lld\n", n, PT_LU_USEC(timer));
        }
        /* query  */
        if(action_query && query_str && topN > 0)
        {
            /*
            if(ibase_qparser(ibase, query_str, &query) > 0)
            {
                query.display[0].flag = IB_IS_DISPLAY|IB_IS_HIGHLIGHT;
                query.display[1].flag = IB_IS_DISPLAY|IB_IS_HIGHLIGHT;
                query.display[2].flag = IB_IS_DISPLAY|IB_IS_HIGHLIGHT;
                query.display[3].flag = IB_IS_DISPLAY|IB_IS_HIGHLIGHT;
                query.display[4].flag = IB_IS_DISPLAY|IB_IS_HIGHLIGHT;
                query.display[5].flag = IB_IS_DISPLAY|IB_IS_HIGHLIGHT;
                query.ntop = topN;
                TIMER_SAMPLE(timer);
                if((res = ibase_query(ibase, &query)) && res->count > 0)
                {
                    TIMER_SAMPLE(timer);
                    records = (IRECORD *)((char *)res + sizeof(IRES));
                    for(i = 0; i < res->count; i++)
                    {
                        fprintf(stdout, "%d:[docid:%d nhits:%d score:%f]\n", i,
                                records[i].docid, records[i].nhits, records[i].score);
                    }
                    fprintf(stdout, "query(%s) fields:%d count:%d total:%d "
                            "doc_total:%d time used:%lld\n",
                            query_str, query.fields_filter, res->count,
                            res->total, (int)ibase->state->dtotal, PT_LU_USEC(timer));
                    if(ibase_read_summary(ibase, &query, res, records, &summary,
                                0, 10, "<font color=red>", "</font>") > 0)
                    {
                        fprintf(stdout, "summary:%s\n", summary);
                        ibase_free(ibase, summary);
                    }
                    ibase_free(ibase, res);
                }
            }
            */
            TIMER_SAMPLE(timer);

        }
	    TIMER_CLEAN(timer);
        ibase_clean(ibase);
    }
}
//gcc -o hibase ibase.c idb.c index.c query.c summary.c  utils/*.c -I utils/ -D_FILE_OFFSET_BITS=64 -D_DEBUG_IBASE  -lz -lm -g && ./hibase --basedir=/data/ibase --index --list=/data/hibase/doc/hindex.doc
//gcc -o hibase ibase.c idb.c index.c query.c summary.c utils/*.c -I utils/ -D_DEBUG -D_FILE_OFFSET_BITS=64 -D_DEBUG_IBASE  -lz -lm -g && ./hibase --basedir=/data/ibase --query --topN=100 --qstring="hello"
#endif
