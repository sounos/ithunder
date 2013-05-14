#ifndef __KINDEX__H__
#define __KINDEX__H__
#include <ibase.h>
#include <mtask.h>
#include <dbase.h>
#include "mutex.h"
#define K_IP_MAX            16
#define K_PATH_MAX          256
#define K_LINE_MAX          1024 
#define K_CHARSET_MAX       64
#define K_BLOCK_SIZE        65536
#define K_DBLINE_MAX        512
#define K_DBNAME_MAX        256
#define K_FIELDNAME_MAX     256
#define K_DOCUMENT_MAX      (1024 * 1024 * 2)
#define K_SEGMENTORS_MIN    32
#define K_SEGMENTORS_MAX    256
#define K_COMPRESS_DISABLED 1
#define K_PHRASE_DISABLED   1
#define K_OFFSET_MAX        512
#define K_TERMS_MAX         1024
typedef struct _TERMNODE
{
    short noffsets;
    short nroffsets;
    int   termid;
    int   term_offset;
    int   nprevnexts;
    short offsets[K_OFFSET_MAX];
    short roffsets[K_OFFSET_MAX];
    char  prevs[K_TERMS_MAX];
    char  nexts[K_TERMS_MAX];
}TERMNODE;
typedef struct _XINDEX
{
    int nterms;
    int term_text_total;
    int term_offsets_total;
    int nblock;
    int ndata;
    char *block;
    char *data;
    void *map;
    void *logger;
    void *timer;
    BJSON request;
    BJSON record;
    STERM terms[K_TERMS_MAX];
    TERMNODE nodes[K_TERMS_MAX];
    MTASK mtask;
    DBASE sdb;
    DBASE rdb;
}XINDEX;
typedef struct _KSTATE
{
    off_t start_time;
    off_t usecs;
    off_t ntasks;
    off_t npackets;
    off_t task_rio_time;
    off_t task_wio_time;
    off_t packet_rio_time;
    off_t packet_wio_time;
}KSTATE;
typedef struct _KINDEX
{
    int     log_level;
    int     compress_status;
    int     phrase_status;
    int     nqsegmentors;
    int     qtask_server_port;
    int     qtask_commitid;
    int     qtask_queueid;
    int     s_port;
    int     r_port;
    int     statefd;
    KSTATE  *state;
    void    *mdict;
    void    *xdict;
    void    *logger;
    MUTEX   *mutex;
    MUTEX   *mutex_segmentor;
    void    *segmentor;
    void    *qsegmentors[K_SEGMENTORS_MAX];
    char    qtask_server_host[K_IP_MAX];
    char    s_host[K_IP_MAX];
    char    s_property_name[K_FIELDNAME_MAX];
    char    s_key_name[K_FIELDNAME_MAX];
    char    s_text_index_name[K_FIELDNAME_MAX];
    char    s_int_index_name[K_FIELDNAME_MAX];
    char    s_long_index_name[K_FIELDNAME_MAX];
    char    s_double_index_name[K_FIELDNAME_MAX];
    char    s_display_fields_name[K_FIELDNAME_MAX];
    char    r_host[K_IP_MAX];
    char    r_key_name[K_FIELDNAME_MAX];
    char    r_index_block_name[K_FIELDNAME_MAX];
    char    dict_charset[K_CHARSET_MAX];
    char    dict_file[K_PATH_MAX];
    char    dict_rules[K_PATH_MAX];
    char    basedir[K_PATH_MAX];
}KINDEX;
/* new xindex */
XINDEX *xindex_new();
/* clean hindex */
void xindex_clean(XINDEX *xindex);
/* initialize */
KINDEX *kindex_init();
/* set basedir */
int kindex_set_basedir(KINDEX *kindex, char *basedir);
/* set dict */
int kindex_set_dict(KINDEX *kindex, char *dictfile, char *charset, char *rules);
/* set qtask host */
int kindex_set_qtask_server(KINDEX *kindex, char *ip, int port, int commitid, int queueid);
/* set data-souce db */
int kindex_set_source_db(KINDEX *kindex, char *host, int port,  char *key_name, char *property_name, 
        char *text_index_name, char *int_index_name, char *long_index_name, 
        char *double_index_name, char *display_name);
/* set res-storage db */
int kindex_set_res_db(KINDEX *kindex, char *host, int port, char *key_name, char *index_block_name);
/* make index */
int kindex_work(KINDEX *kindex, XINDEX *xindex);
/* state */
int kindex_state(KINDEX *kindex, char *out);
/* clean KINDEX */
void kindex_clean(KINDEX *kindex);
#endif
