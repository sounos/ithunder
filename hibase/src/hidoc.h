#include <ibase.h>
#ifndef _HIDOC_H_
#define _HIDOC_H_
#ifdef __cplusplus
extern "C" {
#endif
#include "html.h"
#include "mutex.h"
#include "mmtrie.h"
#define  FILE_PATH_MAX          256
#define  HI_DOCBLOCK_MAX        1048576
#ifndef  HI_PACKETDOC_COUNT
#define  HI_PACKETDOC_COUNT     256
#define  HI_QX_COUNT            256
#endif
#define  HI_DOCMBLOCK_MAX       1048576
#define  HI_PACKET_MAX          2000000000
#define  HI_PACKET_BASE         10000000
#define  HI_XINDEX_MAX          2000000000
#define  HI_XINDEX_BASE         10000000
#define  HI_TERMX_MAX           2000000000
#define  HI_TERMX_BASE          10000000
#define  HI_HITASK_MAX          1000
#define  HI_HITASK_INCREMENT    100
#define  HI_NODE_DOCD           0x00
#define  HI_NODE_PARSERD        0x01
#define  HI_NODE_INDEXD         0x02
#define  HI_NODE_MAX            1024
#define  HI_NAME_MAX            64
#define  HI_CHARSET_MAX         128
#define  HI_IP_MAX              16
#define  HI_TASKS_MAX           32
#define  HI_DUMP_MAX            64
#define  HI_LINE_SIZE           1024
#define  HI_TERM_SIZE           96
#define  HI_BSTERM_MAX          10000000
#define  HI_BSTERM_BASE         100000
#define  HI_SYNTERM_MAX          10000000
#define  HI_SYNTERM_BASE         100000
static char *server_type_list[] = {"DocNode", "ParserNode", "IndexNode"}; 
#define  HI_SERVERLIST_NUM      2
#define  HI_PHRASE_ENABELD      0x00
#define  HI_PHRASE_DISABLED     0x01
#define  HI_PHRASE_NOTSELF      0x02
#define  HI_CCOMPRESS_ENABELD   0x00
#define  HI_CCOMPRESS_DISABLED  0x01
#define  HI_LEFT_LEN            4096
#define  HI_FXDOUBLE_MAX        1024
#define  HI_FXINT_MAX           1024
#define  HI_XFLOAT_MAX          1024
#define  HI_XLONG_MAX           1024
#define  HI_SEGMENTORS_MIN      32
#define  HI_SEGMENTORS_MAX      1024
/* sync data task */
typedef struct _HITASK
{
    short  status;
    short  port;
    char   ip[HI_IP_MAX];
    int    nodeid;
    int    last;
    int    popid;
    int    over;
    int    nxindexs;
    int    count;
    int    mmqid;
    int    nqueue;
    int    upid;
    int    upover;
    int    nupdates;
    int    upcount;
    off_t  synterm_mod_time;
    off_t  synterm_last_time;
    off_t  bterm_mod_time;
    off_t  bterm_last_time;
}HITASK;
/* data node */
typedef struct _HINODE
{
    short status;
    short type;
    int first;
    int last;
    int total;
    int limit;
    int ntasks;
    char name[HI_NAME_MAX];
    HITASK tasks[HI_TASKS_MAX];
}HINODE;
typedef struct _HIDUMP
{
    int fd;
    int bits;
    uint32_t modtime;
    uint32_t ltime;
    off_t offset;
    char file[FILE_PATH_MAX];
}HIDUMP;
/* histate */
typedef struct _HISTATE
{
    off_t bterm_mod_time;
    off_t synterm_mod_time;
    int16_t   ccompress_status;
    int16_t  phrase_status;
    int   ntasks;
    int   taskqid;
    int   need_update_numbric;
    int   packettotal;
    int   xindextotal;
    int   docpopid;
    int   popid;
    int   rootid;
    int   nnodes;
    int   nidxnodes;
    int   int_index_from;
    int   int_index_count;
    int   long_index_from;
    int   long_index_count;
    int   double_index_from;
    int   double_index_count;
    int   bterm_id_max;
    int   synterm_id_max;
    uint32_t   stime;
    HINODE nodes[HI_NODE_MAX];
    HIDUMP dumps[HI_DUMP_MAX];
}HISTATE;
/* term node */
typedef struct _PREVNEXT
{
    unsigned short prev;
    unsigned short next;
}PREVNEXT;
#define HI_TERMS_MAX 8192
#define HI_OFFSET_MAX 2048
#define HI_TXTDOC_MAX 131072
typedef struct _TERMNODE
{
    int   termid;
    int   term_offset;
    int   nprevnexts;
    short noffsets;
    short nroffsets;
    int   offsets[HI_OFFSET_MAX];
    int   roffsets[HI_OFFSET_MAX];
    char  prevs[HI_TERMS_MAX];
    char  nexts[HI_TERMS_MAX];
}TERMNODE;
/* index */
typedef struct _HINDEX
{
    HTML *html;
    void *map;
    int nterms;
    int term_text_total;
    int term_offsets_total;
    STERM terms[HI_TERMS_MAX];
    TERMNODE nodes[HI_TERMS_MAX];
    char *block;
    int nblock;
    char *data;
    int ndata;
    char *out;
    int nout;
}HINDEX;
typedef struct _IPACKET
{
    int   nodeid;
    int   count;
    int   length;
    int   prev;
    int   next;
    off_t offset;
}IPACKET;
typedef struct _XFLOAT
{
    int64_t id;
    double val;
}XFLOAT;
typedef struct _XLONG
{
    int64_t id;
    int64_t val;
}XLONG;
typedef struct _XINDEX
{
    int     status;
    int     nodeid;
    int     prev;
    int     next;
    int     crc;
    int     slevel;
    int64_t globalid;
    int64_t category;
    double  rank;
}XINDEX;
typedef struct _XMIO
{
    int     fd;
    int     bits;
    void    *map;
    off_t   old;
    off_t   end;
    off_t   size;
}XMIO;
typedef struct _BSTERM
{
    BTERM bterm;
    char term[HI_TERM_SIZE];
}BSTERM;
/* idoc */
typedef struct _HIDOC
{
    void    *fp;
    void    *segmentor;
    MMTRIE  *xdict;
    MMTRIE  *mmtrie;
    MMTRIE  *namemap;
    XMIO     packetio;
    XMIO     xindexio;
    XMIO     xintio;
    XMIO     xlongio;
    XMIO     xdoubleio;
    XMIO     bstermio;
    XMIO     syntermio;
    XMIO     stermio;
    MMTRIE  *map;
    HISTATE *state;
    void    *db;
    void    *update;
    void    *queue;
    void    *mmqueue;
    void    *kmap;
    void    *qsegmentors[HI_SEGMENTORS_MAX];
    int     nqsegmentors;
    int     histatefd;
    MUTEX   *mutex;
    MUTEX   *mutex_segmentor;
    void    *logger;
    int     log_access;
    int     doc_total;
    off_t   size_total;
    char    basedir[FILE_PATH_MAX];
    char    dict_charset[HI_CHARSET_MAX];
    char    dict_file[FILE_PATH_MAX];
    char    dict_rules[FILE_PATH_MAX];
    
    int (*set_basedir)(struct _HIDOC *, char *basedir, int ntasks);
    int (*set_dump)(struct _HIDOC *, int no, char *dump);
    int (*get_dumpinfo)(struct _HIDOC *, char *out);
    int (*set_int_index)(struct _HIDOC *, int int_index_from, int int_index_count);
    int (*set_long_index)(struct _HIDOC *, int long_index_from, int long_index_count);
    int (*set_double_index)(struct _HIDOC *, int double_index_from, int double_index_count);
    int (*set_phrase_status)(struct _HIDOC *, int status);
    int (*set_ccompress_status)(struct _HIDOC *, int status);
    int (*set_dict)(struct _HIDOC *, char *dictfile, char *charset, char *rules);
    //int (*set_forbidden_dict)(struct _HIDOC *, char *dictfile);
    int (*resume)(struct _HIDOC *);
    int (*genindex)(struct _HIDOC *, HINDEX *hindex, FHEADER *fheader, IFIELD *fields, int nfields, 
            char *content, int ncontent, IBDATA *block);
    int (*pop_packetid)(struct _HIDOC *, int nodeid, IPACKET *packet);
    int (*read_packet)(struct _HIDOC *, int packetid, char *data, int ndata);
    int (*add_node)(struct _HIDOC *, int type, char *name, int limit);
    int (*del_node)(struct _HIDOC *, int nodeid);
    int (*set_node_limit)(struct _HIDOC *, int nodeid, int limit);
    int (*list_nodes)(struct _HIDOC *, char *out, char *end);
    int (*add_task)(struct _HIDOC *, int nodeid, char *ip, int port);
    int (*del_task)(struct _HIDOC *, int taskid);
    int (*pop_task)(struct _HIDOC *, HITASK *task);
    int (*list_tasks)(struct _HIDOC *, int nodeid, char *out, char *end);
    int (*push_task)(struct _HIDOC *, int taskid);
    int (*over_task)(struct _HIDOC *, int taskid, int packetid);
    int (*read_index)(struct _HIDOC *, int taskid, char *data, int *len, int *count);
    int (*over_index)(struct _HIDOC *, int taskid, int id);
    int (*read_upindex)(struct _HIDOC *, int taskid, char *data, int *len, int *count);
    int (*over_upindex)(struct _HIDOC *, int taskid, int upid);
    int (*set_synterm)(struct _HIDOC *, char **terms, int num);
    int (*sync_synterms)(struct _HIDOC *);
    int (*read_synterms)(struct _HIDOC *, int taskid, char *data, int len);
    int (*over_synterms)(struct _HIDOC *, int taskid);
    int (*set_bterm)(struct _HIDOC *, char *, int nterm, int status);
    int (*update_bterm)(struct _HIDOC *, int termid, int status);
    int (*del_bterm)(struct _HIDOC *, int termid);
    int (*list_bterms)(struct _HIDOC *, char *out);
    int (*add_bterm)(struct _HIDOC *, char *, int nterm);
    int (*sync_bterms)(struct _HIDOC *);
    int (*read_bterms)(struct _HIDOC *, int taskid, char *data, int len);
    int (*over_bterms)(struct _HIDOC *, int taskid);
    int (*set_idx_status)(struct _HIDOC *, int64_t globalid, int status);
    int (*set_rank)(struct _HIDOC *, XFLOAT *list, int count);
    int (*set_slevel)(struct _HIDOC *, XLONG *list, int count);
    int (*set_category)(struct _HIDOC *, XLONG *list, int count);
    int (*set_int_fields)(struct _HIDOC *, FXINT *list, int count);
    int (*set_long_fields)(struct _HIDOC *, FXLONG *list, int count);
    int (*set_double_fields)(struct _HIDOC *, FXDOUBLE *list, int count);
    int (*parse_document)(struct _HIDOC *, int no, HINDEX *hindex);
    int (*parseHTML)(struct _HIDOC *, HINDEX *hindex, char *url, int date, 
            char *content, int ncontent, IBDATA *block);
    void (*clean)(struct _HIDOC *);
}HIDOC;
/* initialize */
HIDOC *hidoc_init();
/* gen index */
int hidoc_genindex(HIDOC *hidoc, HINDEX *hindex, FHEADER *fheader, IFIELD *fields, int nfields, 
        char *content, int ncontent, IBDATA *block);
/* resync index */
int hidoc_resync(HIDOC *hidoc);
/* new hindex */
HINDEX *hindex_new();
/* clean hindex */
void hindex_clean(HINDEX *hindex);
#ifdef __cplusplus
 }
#endif
#endif
