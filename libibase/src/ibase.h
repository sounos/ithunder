#include <inttypes.h>
#ifndef _IBASE_H_
#define _IBASE_H_
#ifdef __cplusplus
extern "C" {
#endif
#define  IB_ONLINE              0x00
#define  IB_OFFLINE             0x01
#define  IB_KEYTERM_MAX         256
#ifndef  IB_QUERY_MAX
#define  IB_QUERY_MAX           32
#define  IB_QUERY2_MAX          64
#endif
#define  IB_IDX_MAX             32
#define  IB_INT_OFF             32
#define  IB_INT_TO              64
#define  IB_LONG_OFF            64
#define  IB_LONG_TO             96
#define  IB_DOUBLE_OFF          96
#define  IB_DOUBLE_TO           128
#define  IB_DIS_OFF             128
#define  IB_DIS_TO              160
#define  IB_IN_MAX              512
#define  IB_INFIELD_MAX         8
#define  IB_SECURITY_OK         0x01
#define  IB_SECURITY_FORBIDDEN  0xffffffff
#define  IB_CATEGORY_MAX        64
#define  IB_GROUP_MAX           1024
#define  IB_SLEVEL_MAX          32
#define  IB_BITSCAT_MAX         4
#define  IB_TOPK_NUM            2000
#define  IB_NTOP_MAX            8192
#define  IB_FIELDS_MAX          160
#define  IB_QUERY_RSORT         0x01
#define  IB_QUERY_SORT          0x02
#define  IB_QUERY_PHRASE        0x04
#define  IB_QUERY_CATGROUP      0x08
#define  IB_QUERY_RANK          0x10
#define  IB_QUERY_RELEVANCE     0x20
#define  IB_CLEAR_CACHE         0x40
#define  IB_QUERY_BOOLAND       0x80
#define  IB_QUERY_FORBIDDEN     0x100
#define  IB_QUERY_FIELDS        0x200
#define  IB_IS_DISPLAY          0x01
#define  IB_IS_HIGHLIGHT        0x02
#define  IB_SUMMARY_MAX         8192
#define  IB_SUMMARY_UNIT_MAX    2
#define  IB_PHRASE_LIMIT        128
#define  IB_DOCUMENT_MAX        2097152
#define  IB_PHRASE_SCORE        3
#define  IB_NTERM_SCORE         1
#define  IB_BOOL_SCORE          2
#define  IB_HITS_SCORE          2
#define  IB_FHITS_SCORE         2
#define  IB_XCATUP_SCORE        2
#define  IB_XCATDOWN_SCORE      24
#define  IB_POS_MAX             512
#define  IB_SUMMARY_SCORE       10
#define  IB_BM25_K              1.2f
#define  IB_BM25_K1             2.0f
#define  IB_BM25_P1             3.0f
#define  IB_BM25_P2             1.5f
#define  IB_BM25_B              0.75f
#define  IB_BM25_B1             0.25f
#define  IB_FILTER_SCORE        0.05f
#define  IB_TERMS_MAX           4096
#define  IB_OFFSET_MAX          1024
#define  IB_TERM_MAX            256
#define  IB_BITSPHRASE_MAX      128
#define  IB_BUF_SIZE            65536
#define  IB_LINE_MAX            1024
#define  IB_BITMAP_COUNT        48
#define  IB_BITMAPS_MAX         2048
#define  IB_XNODE_MAX           10000
#define  IB_XMAPS_MAX           2048
#define  IB_STREES_MAX          2048
#define  IB_MMX_MAX             2048
#define  IB_USED_FOR_INDEXD     0x00
#define  IB_USED_FOR_QDOCD      0x01
#define  IB_USED_FOR_QPARSERD   0x02
#define  IB_DB_MAX              64
#define  IB_SEC_MAX             1024
#define  IB_SYNTERM_MAX         16
#define  IBLL(xxx) ((long long int)(xxx))
/*
 * ;16384=16K 32768=32K 65536=64K 131072=128K 262144=256K 524288=512K 786432=768K 
 * ;1048576=1M  2097152=2M 4194304=4M 8388608 = 8M 16777216=16M  33554432=32M
 */
#define  IB_DICT_NAME            "ibase.term"
#define  IB_XDICT_NAME           "ibase.xterm"
#define  IB_TERMSTATE_NAME       "ibase.termstate"
#define  IB_STATE_NAME           "ibase.state"
#define  IB_DOCMAP_NAME          "ibase.docmap"
#define  IB_MHEADER_NAME         "ibase.mheader"
#define  IB_ITREE_NAME           "ibase.itree"
#define  IB_SYNDB_DIR            "syndb"
#define  IB_IDX_DIR              "idx"
#define  IB_INDEX_DIR            "index"
#define  IB_POSTING_DIR          "posting"
#define  IB_SOURCE_DIR           "source"
#define  IB_LOGGER_NAME          "ibase.log"
#define  IB_HEADERS_BASE         1000000
#define  IB_TERMSTATE_BASE       1000000
#define  IB_NUMERIC_BASE         1000000
#define  IB_TERMSTATE_MAX        2000000000
#define  IB_HEADERS_MAX          2000000000
#ifndef  IB_PATH_MAX
#define  IB_PATH_MAX             1024
#define  IB_CHARSET_MAX          256
#endif
#pragma pack(push, 4)
/* block data */
typedef struct _IBDATA
{
    int bits;
    int ndata;
    char *data;
}IBDATA;
/* XNODE */
typedef struct _XNODE
{
    int   docid;
    int   bithits;
    int   bitfields;
    int   nvhits;
    int   nhits;
    int   nhitfields;
    int   bithit;
    int   bitnot;
    short which;
    short no;
    short xmin;
    short xmax;
    short hits[IB_QUERY_MAX];
    short bitquery[IB_QUERY_MAX];
    short bitphrase[IB_QUERY_MAX];
}XNODE;
/* XMAP */
typedef struct _XMAP
{
    int count;
    int min;
    int max;
    int bits;
    XNODE *xnodes[IB_XNODE_MAX];
}XMAP;
/*
typedef struct _IWHO
{
    short total;
    short count;
    int fields;
    int bithits;
    int prevnext_count;
    short whoes[IB_QUERY_MAX];
    short hits[IB_QUERY_MAX];
    short bitphrase[IB_BITSPHRASE_MAX];
}IWHO;
*/
/* term */
typedef struct _ITERM
{
    int termid;
    int docid;
    int ndocid;
    int fields;
    int term_count;
    int prevnext_size;
    int last;
    int weight;
    int bithit;
    int bitnot;
    int16_t bits;
    int16_t no;
    int16_t synno;
    int16_t term_len;
    char *p;
    char *end;
    char *sprevnext;
    char *eprevnext;
    char *sposting;
    char *eposting;
    XNODE xnode;
    IBDATA mm;
    IBDATA posting;
    double idf;
}ITERM;
/* term for store */
typedef struct _STERM
{
    int termid;
    int bit_fields;
    int term_len;
    int term_count;
    int posting_offset;
    int posting_size;
    int prevnext_count;
    int prevnext_size;
}STERM;
typedef struct _SYNTERM
{
    int synid;
    int count;
    int syns[IB_SYNTERM_MAX];
}SYNTERM;
/* term state */
typedef struct _TERMSTATE
{
    short   status;
    short   len;
    int     total;
    int     synid;
    //int     last_docid;
    //int     mod_time;
}TERMSTATE;

/* block/blacklist term */
#define IB_BTERM_OK         0x00
#define IB_BTERM_BLOCK      0x01
#define IB_BTERM_DOWN       0x02
typedef struct BTERM
{
    short status;
    short len;
    int  id;
}BTERM;
/* field */
#define  IB_INDEX_MAX           160
#define  IB_DATE_MAX            64
#define  IB_DATATYPE_INT        0x01
#define  IB_DATATYPE_DOUBLE     0x02
#define  IB_DATATYPE_TEXT       0x04
#define  IB_DATATYPE_LONG       0x08
#define  IB_IS_ALLOW_NULL       0x10
#define  IB_IS_NEED_INDEX       0x20
#define  IB_IS_NEED_UPDATE      0x40
#define  IB_IS_UPDATE_STATUS    0x80
#define  IB_DATATYPE_NUMBRIC    (IB_DATATYPE_INT|IB_DATATYPE_DOUBLE|IB_DATATYPE_LONG)
typedef struct _IFIELD
{
    int   flag;
    int   offset;
    int   length;
}IFIELD;
typedef struct _XFIELD
{
    int from;
}XFIELD;
typedef struct _FXDOUBLE
{
    int64_t id;
    double val;
    int no;
    int bits;
}FXDOUBLE;
typedef struct _FXINT
{
    int64_t id;
    int val;
    int no;
}FXINT;
typedef struct QINT
{
    int fieldid;
    int num;
    int32_t vals[IB_IN_MAX];
}QINT;
typedef struct QLONG
{
    int fieldid;
    int num;
    int64_t vals[IB_IN_MAX];
}QLONG;
typedef struct QDOUBLE
{
    int fieldid;
    int num;
    double vals[IB_IN_MAX];
}QDOUBLE;
typedef struct _FXLONG
{
    int64_t id;
    int64_t val;
    int no;
    int bits;
}FXLONG;
typedef struct _VINT
{
    int32_t val;
    uint32_t nodeid;
}VINT;
typedef struct _VLONG
{
    int64_t  val;
    uint32_t nodeid;
    uint32_t bits;
}VLONG;
typedef struct _VDOUBLE
{
    double  val;
    uint32_t nodeid;
    uint32_t bits;
}VDOUBLE;
#define IB_CATBIT_SET   0x01
#define IB_CATBIT_UNSET 0x02
#define IB_RANK_SET     0x04
#define IB_INT_SET      0x08
#define IB_LONG_SET     0x10
#define IB_DOUBLE_SET   0x20
#define IB_STATUS_SET   0x40
#define IB_DUMP_SET     0x80
#define IB_IGNORE_STAT  0x800
/* document header */
typedef struct _ISCHEMA
{
    short   intindex_from;
    short   longindex_from;
    short   doubleindex_from;
    short   intblock_size;
    short   longblock_size;
    short   doubleblock_size;
}ISCHEMA;
typedef struct _DOCHEADER
{
    short   status;
    short   nfields;
    short   slevel;
    short   nterms;
    short   intindex_from;
    short   longindex_from;
    short   doubleindex_from;
    short   intblock_size;
    short   longblock_size;
    short   doubleblock_size;
    short   secid;
    short   dbid;
    int     terms_total;
    int     crc;
    int     size;
    int     content_off;
    int     content_size;
    int     content_zsize;
    int     prevnext_off;
    int     prevnext_size;
    int     textblock_off;
    int     textblock_size;
    int     intblock_off;
    int     longblock_off;
    int     doubleblock_off;
    int64_t globalid;
    int64_t category;
    double  rank;
}DOCHEADER;
/* type field header */
typedef struct _FHEADER
{
    short   status;
    short   flag;
    short   nfields;
    short   slevel;
    short   secid;
    short   dbid;
    int     crc;
    int     size;
    int64_t globalid;
    int64_t category;
    double  rank;
}FHEADER;
/* typedef struct xheader */
typedef struct _XHEADER
{
    int     status;
    int     slevel;
    int64_t globalid;
    int64_t category;
    double  rank;
}XHEADER;
typedef struct _MHEADER
{
    short status;
    short secid;
    int   crc;
    int   docid;
}MHEADER;
/* doc header for index */
typedef struct _IHEADER
{
    short     status;
    short     slevel;
    int     terms_total;
    int64_t globalid;
    int64_t category;
    double  rank;
}IHEADER;
/* term block */
typedef struct _WBLOCK
{
    int  score;
    int  nterm;
    int  last;
    int  bits;
    char *p;
    char *end;
}WBLOCK;

/* position */
typedef struct _IPOS
{
    int pos;
    int len;
}IPOS;

/* IBLOCK */
typedef struct _IBLOCK
{
    int score;
    int from;
    int len;
    int bits;
    IPOS poslist[IB_POS_MAX];
    int list[IB_POS_MAX];
    int count;
    int uniq;
    int last_pos;
    int last_size;
}IBLOCK;
/* display */
typedef struct _IDISPLAY
{
    short flag;
    short id;
    //int offset;
    //int length;
}IDISPLAY;
#define IB_INT_INDEX_MAX        32
#define IB_LONG_INDEX_MAX       32
#define IB_DOUBLE_INDEX_MAX     32
#define IB_RANGE_FROM           0x01
#define IB_RANGE_TO             0x02
#define IB_RANGE_IN             0x04
/* int range */
typedef struct  _IRANGE
{
    int     flag;
    int     field_id;
    int     from;
    int     to;
}IRANGE;
/* long range*/
typedef struct  _LRANGE
{
    int     flag;
    int     field_id;
    int64_t from;
    int64_t to;
}LRANGE;
/* float range */
typedef struct _FRANGE
{
    int flag;
    int field_id;
    double from;
    double to;
}FRANGE;
#define IB_BITFIELDS_FILTER  0x01
#define IB_BITFIELDS_BLOCK   0x02
#define IB_INT_BITS_MAX      32
#define IB_LONG_BITS_MAX     64
/* int bits */
typedef struct _IBITS
{
    short   flag;
    short   field_id;
    int     bits;
}IBITS;
/* long bits */
typedef struct _LBITS
{
    int     flag;
    int     field_id;
    int64_t bits;
}LBITS;
#define QTERM_BIT_AND       0x01
#define QTERM_BIT_NOT       0x02
#define QTERM_BIT_BLOCK     0x04
#define QTERM_BIT_PREV      0x08
#define QTERM_BIT_NEXT      0x10
#define QTERM_BIT_FORBIDDEN 0x20
#define QTERM_BIT_DOWN      0x40
#define QTERM_BIT_SYN       0x80
/* query term */
typedef struct _QTERM
{
    short flag;
    short size;
    int   id;
    int   synid;
    int   synno;
    int   prev;
    int   next;
    int   bithit;
    int   bitnot;
    double idf;
}QTERM;

/* operator */
typedef struct _IOPERATOR
{
    int bitsand;
    int bitsnot;
}IOPERATOR;
#define IB_SORT_BY_INT      0x01
#define IB_SORT_BY_LONG     0x02
#define IB_SORT_BY_DOUBLE   0x04
#define IB_GROUPBY_INT      0x08
#define IB_GROUPBY_LONG     0x10
#define IB_GROUPBY_DOUBLE   0x20

/* query */
typedef struct _IQUERY
{
    short       from;
    short       count;
    short       secid;
    short       dbid;
    short       ntop;
    short       nqterms;
    short       nquerys;
    short       orderby;
    short       groupby;
    short       int_range_count;
    short       long_range_count;
    short       double_range_count;
    short       int_bits_count;
    short       long_bits_count;
    short       status;
    short       nvqterms;
    short       int_in_num;
    short       long_in_num;
    short       double_in_num;
    short       nquery;
    short       hitscale[IB_QUERY_MAX]; 
    short       slevel_filter[IB_SLEVEL_MAX]; 
    int         flag;//is_sort/is_rsort/is_phrase/is_relevance/is_clear_cache/is_query_and/is_query_forbidden
    int         qweight;
    int         qfhits;
    int         qid;
    int         fields_filter;
    int         base_hits;
    int         base_fhits;
    int         base_phrase;
    int         base_nterm;
    int         base_xcatup;
    int         base_xcatdown;
    int         base_rank;
    IOPERATOR   operators;
    IRANGE      int_range_list[IB_INT_INDEX_MAX];
    LRANGE      long_range_list[IB_LONG_INDEX_MAX];
    FRANGE      double_range_list[IB_DOUBLE_INDEX_MAX];
    IBITS       int_bits_list[IB_INT_INDEX_MAX];
    LBITS       long_bits_list[IB_LONG_INDEX_MAX];
    IDISPLAY    display[IB_FIELDS_MAX];
    QTERM       qterms[IB_QUERY_MAX];
    double      ravgdl;
    int64_t     bitxcat_up;
    int64_t     bitxcat_down;
    int64_t     catgroup_filter;
    int64_t     category_filter;
    int64_t     catblock_filter;
    int64_t     multicat_filter;
    int16_t     nosecs[IB_SEC_MAX];
    QINT        int_in_list[IB_INFIELD_MAX];
    QLONG       long_in_list[IB_INFIELD_MAX];
    QDOUBLE     double_in_list[IB_INFIELD_MAX];
}IQUERY;

/* weight */
typedef struct _IWEIGHT
{
    int no;
    int id;
    double score;
}IWEIGHT;
#define  IBLONG(xmx) ((int64_t)(xmx))
#define  IB_SCORE_BASE  ((int64_t)100000000)
#define  IB_INT_BASE    1000000
#define  IB_FLOAT_LIMIT 1000000
#define  IB_INT_SCORE(X) ((int)((X) * IB_INT_BASE))
#define  IB_LONG_SCORE(X) ((int64_t)(((double)(X)) * (double)IB_SCORE_BASE))
#define  IB_LONG2FLOAT(X) ((double)(((double)(X)) / (double)IB_SCORE_BASE))
#define  IB_INT2LONG_SCORE(X) ((int64_t)X << 32)
/* record */
typedef struct _IRECORD
{
    int64_t         globalid;
    int64_t         score;
}IRECORD, *PIRECORD;
typedef struct _IKEYV
{
    int64_t key;
    int64_t val;
}IKEYV;
/* hits */
typedef struct _IHITS
{
    int     nhits;
    int     total;
    IWEIGHT weights[IB_QUERY_MAX];
}IHITS, *PIHITS;
/* result */
typedef struct _IRES
{
    short status;
    short count;
    short ncatgroups;
    short ngroups; 
    short dbid;
    short flag;
    int qid;
    int io_time;
    int sort_time;
    u_int32_t doctotal;
    u_int32_t total;
    int catgroups[IB_CATEGORY_MAX];
    IKEYV groups[IB_GROUP_MAX];
}IRES;
typedef struct _IBIO
{
    int fd;
    uint32_t rootid;
    char *map;
    off_t size;
    off_t end;
    off_t old;
}IBIO;
/* state */
typedef struct _IBSTATE
{
    int     used_for;
    int     termid;
    int     docid;
    int     index_status;
    int     phrase_status;
    int     compression_status;
    int     mmsource_status;
    int     int_index_from;
    int     int_index_fields_num;
    int     long_index_from;
    int     long_index_fields_num;
    int     double_index_from;
    int     double_index_fields_num;
    int     nsecs;
    int64_t ttotal;
    int64_t dtotal;
    int     secs[IB_SEC_MAX];
    int     ids[IB_SEC_MAX]; 
    void    *mfields[IB_SEC_MAX][IB_FIELDS_MAX];
    IBIO    headers[IB_SEC_MAX];
}IBSTATE;

#define IB_REQ_QPARSE            1
#define IB_RESP_QPARSE           2
#define IB_REQ_QUERY             3
#define IB_RESP_QUERY            4
#define IB_REQ_SUMMARY           5
#define IB_RESP_SUMMARY          6
#define IB_REQ_INDEX             7
#define IB_RESP_INDEX            8
#define IB_REQ_UPDATE            9
#define IB_RESP_UPDATE           10
#define IB_REQ_DELETE            11
#define IB_RESP_DELETE           12
#define IB_REQ_UPDATE_BTERM      13
#define IB_RESP_UPDATE_BTERM     14
#define IB_REQ_CLEAR_CACHE       15
#define IB_RESP_CLEAR_CACHE      16
#define IB_REQ_UPDATE_SYNTERM    17
#define IB_RESP_UPDATE_SYNTERM   18
#define IB_STATUS_OK             0x00
#define IB_STATUS_ERR            0x01
#define IB_STATUS_FORBIDDEN      0x02
/* request/response header */
typedef struct _IHEAD
{
    int     id;
    int     length;
    int     cmd;
    int     nodeid;
    int     cid;
    int     status;
}IHEAD;
/* result chunk  */
typedef struct _ICHUNK
{
    IHEAD   resp;
    IRES    res;
    IRECORD records[IB_TOPK_NUM];
}ICHUNK;
/* query set */
typedef struct _IQSET
{
    int         nqterms;
    int         count;
    QTERM       qterms[IB_QUERY_MAX];
    IDISPLAY    displaylist[IB_FIELDS_MAX];
    IRES        res;
}IQSET;
#define IB_INDEX_ENABLED        0x00
#define IB_INDEX_DISABLED       0x01
#define IB_COMPRESSION_ENABLED  0x00
#define IB_COMPRESSION_DISABLED 0x01
#define IB_PHRASE_ENABLED       0x00
#define IB_PHRASE_DISABLED      0x01
#define IB_CACHE_ENABLED        0x00
#define IB_CACHE_DISABLED       0x01
#define IB_MMSOURCE_ENABLED     0x00
#define IB_MMSOURCE_DISABLED    0x01
#define IB_MMSOURCE_NULL        0x02
#define IB_MAPS_MAX             2048
#define IB_QITERMS_MAX          2048
#define IB_CHUNKS_MAX           2048
#define IB_BLOCKS_MAX           1024
#define IB_IBLOCKS_MAX          2048
#define IB_SEGMENTORS_MAX       1024
#define IB_SEGMENTORS_SET       256
#define IB_SEGMENTORS_MIN       64
/* ibase main struct */
typedef struct _IBASE
{
    IBIO stateio;
    IBIO termstateio;
    IBIO mheadersio;
    char basedir[IB_PATH_MAX];
    char dict_rules[IB_PATH_MAX];
    char dict_file[IB_PATH_MAX];
    char dict_charset[IB_CHARSET_MAX];
    int nqsegmentors;
    int nqblocks; 
    int nqiblocks; 
    int nqiterms;
    int nqxmaps; 
    int nqstrees;
    int nqmmxs;
    int nqchunks;
    int nsegmentors;
    void *mutex;
    void *mutex_itermlist;
    void *mutex_chunk;
    void *mutex_block;
    void *mutex_iblock;
    void *mutex_stree;
    void *mutex_mmx;
    void *mutex_xmap;
    void *mutex_record;
    void *mutex_segmentor;
    void *mutex_termstate;

    IBSTATE *state;
    void *qsegmentors[IB_SEGMENTORS_MAX];
    char *qblocks[IB_BLOCKS_MAX];
    IBLOCK *qiblocks[IB_IBLOCKS_MAX];
    ITERM *qiterms[IB_QITERMS_MAX];
    XMAP *qxmaps[IB_XMAPS_MAX];
    void *qstrees[IB_STREES_MAX];
    void *qmmxs[IB_MMX_MAX];
    ICHUNK  *qchunks[IB_CHUNKS_MAX];
    void *mindex[IB_SEC_MAX];
    void *mposting[IB_SEC_MAX];
    void *index; /* index db */
    void *syndb; /* synonym db */
    void *mmtrie; /* dict */
    void *xmmtrie; /* dict */
    void *docmap; /* globalid to docid map*/
    void *source; /* document source DB */
    void *segmentor;
    void *logger;
    void *delmap;
    
    int     (*set_basedir)(struct _IBASE *ibase, char *basedir, int use_for, int mmsource_status);
    int     (*set_int_index)(struct _IBASE *ibase, int secid, int int_index_from, int int_fields_num);
    int     (*set_long_index)(struct _IBASE *ibase, int secid, int int_index_from, int int_fields_num);
    int     (*set_double_index)(struct _IBASE *ibase, int secid, int double_index_from, int double_fields_num);
    int     (*add_document)(struct _IBASE *ibase, IBDATA *block);
    int     (*enable_document)(struct _IBASE *ibase, int64_t globalid);
    int     (*disable_document)(struct _IBASE *ibase, int64_t globalid);
    int     (*enable_term)(struct _IBASE *ibase, int termid);
    int     (*disable_term)(struct _IBASE *ibase, int termid);
    int     (*set_xheader)(struct _IBASE *ibase, XHEADER *xheader);
    int     (*set_int_fields)(struct _IBASE *ibase, int64_t globalid, int *);
    int     (*set_long_fields)(struct _IBASE *ibase, int64_t globalid, int64_t *);
    int     (*set_double_fields)(struct _IBASE *ibase, int64_t globalid, double *);
    int     (*set_rank)(struct _IBASE *ibase, int64_t globalid, double rank);
    int     (*set_category)(struct _IBASE *ibase, int64_t globalid, int64_t category);
    int     (*set_slevel)(struct _IBASE *ibase, int64_t globalid, int slevel);
    int     (*set_int_field)(struct _IBASE *ibase, int64_t globalid, int field_no, int val);
    int     (*set_long_field)(struct _IBASE *ibase, int64_t globalid, int field_no, int64_t val);
    int     (*set_double_field)(struct _IBASE *ibase, int64_t globalid, int field_no, double val);
    int     (*qparser)(struct _IBASE *ibase, int fid, char *query_str, char *not_str, IQUERY *query);
    int     (*synparser)(struct _IBASE *ibase,  IQUERY *query);
    int     (*set_index_status)(struct _IBASE *ibase, int status);
    int     (*set_phrase_status)(struct _IBASE *ibase, int status);
    int     (*set_compression_status)(struct _IBASE *ibase, int status);
    int     (*set_mmsource_status)(struct _IBASE *ibase, int status);
    ICHUNK *(*query)(struct _IBASE *ibase, IQUERY *query);
    int     (*read_summary)(struct _IBASE *ibase, IQSET *qset, IRECORD *records, 
            char *summary, char *highlight_start, char *highlight_end);
    int     (*set_state)(struct _IBASE *ibase, int state);
    int     (*set_log_level)(struct _IBASE *ibase, int level);
    void    (*clean)(struct _IBASE *ibase);
}IBASE;
/* initialize ibase */
IBASE *ibase_init();
/* mkdir force */
int ibase_set_basedir(IBASE *, char *dir, int use_for, int mmsource_status);
/* set dict file */
int ibase_set_dict(IBASE *ibase, char *dict_charset, char *dictfile, char *rules);
void ibase_check_mindex(IBASE *ibase, int secid);
/* set int index */
int ibase_set_int_index(IBASE *ibase, int secid, int index_from, int index_fields_count);
/* check int index */
void ibase_check_int_idx(IBASE *ibase, int secid, int no);
int ibase_check_int_index(IBASE *ibase);
/* set long index */
int ibase_set_long_index(IBASE *ibase, int secid, int index_from, int index_fields_count);
void ibase_check_long_idx(IBASE *ibase, int secid, int no);
/* check long index */
int ibase_check_long_index(IBASE *ibase);
void ibase_check_double_idx(IBASE *ibase, int secid, int no);
/* set double index */
int ibase_set_double_index(IBASE *ibase, int secid, int index_from, int index_fields_count);
/* check double index */
int ibase_check_double_index(IBASE *ibase);
/* add document */
int ibase_add_document(IBASE *ibase, IBDATA *block);
/* update document */
int ibase_update_document(IBASE *ibase, IBDATA *block);
/* enable document */
int ibase_enable_document(IBASE *ibase, int64_t globalid);
/* disable document */
int ibase_disable_document(IBASE *ibase, int64_t globalid);
/* enable term */
int ibase_enable_term(IBASE *ibase, int termid);
/* disable term */
int ibase_disable_term(IBASE *ibase, int termid);
/* block */
int ibase_update_bterm(IBASE *ibase, BTERM *bterm, char *term);
/* update synonym term */
int ibase_update_synterm(IBASE *ibase, SYNTERM *term);
/* query parser ,return term count */
int ibase_qparser(IBASE *ibase, int fid, char *query_str, char *not_str, IQUERY *query);
/* synonym parser */
int ibase_synparser(IBASE *ibase, IQUERY *query);
/* set index status */
int ibase_set_index_status(IBASE *ibase, int status);
/* set phrase status */
int ibase_set_phrase_status(IBASE *ibase, int status);
/* set index compression status */
int ibase_set_compression_status(IBASE *ibase, int status);
/* set source file mmap status */
int ibase_set_mmsource_status(IBASE *ibase, int status);
/* get docid with globalid */
int ibase_docid(IBASE *ibase, int64_t globalid);
/* set xheader */
int ibase_set_xheader(IBASE *ibase, XHEADER *xheader);
/* set xint_fields */
int ibase_set_int_fields(IBASE *ibase, int64_t globalid, int *);
/* set long fields */
int ibase_set_long_fields(IBASE *ibase, int64_t globalid , int64_t *);
/* set xdouble fields */
int ibase_set_double_fields(IBASE *ibase, int64_t globalid, double *);
/* set rank */
int ibase_set_rank(IBASE *ibase, int64_t globalid, double rank);
/* set catgory */
int ibase_set_category(IBASE *ibase, int64_t globalid, int64_t category);
/* set slevel */
int ibase_set_slevel(IBASE *ibase, int64_t globalid, int slevel);
/* set int field */
int ibase_set_int_field(IBASE *ibase, int64_t globalid, int field_no, int val);
/* set long field */
int ibase_set_long_field(IBASE *ibase, int64_t globalid, int field_no, int64_t val);
/* set double field */
int ibase_set_double_field(IBASE *ibase, int64_t globalid, int field_no, double val);
/* set log level */
int ibase_set_log_level(IBASE *ibase, int level);
/* query return IRES contain records list */
//ICHUNK *ibase_query(IBASE *ibase, IQUERY *query);
/* query with bitmap merging */
//ICHUNK *ibase_mquery(IBASE *ibase, IQUERY *query);
/* get secs */
int ibase_get_secs(IBASE *ibase, int16_t *nosecs, int *secs);
/* query with binary list merging */
ICHUNK *ibase_bquery(IBASE *ibase, IQUERY *query, int secid);
ICHUNK *ibase_query(IBASE *ibase, IQUERY *query, int secid);
/* mtree merging */
ICHUNK *ibase_xquery(IBASE *ibase, IQUERY *query, int secid);
/* push iterm */
void ibase_push_itermlist(IBASE *ibase, ITERM *itermlist);
/* ibase pop iterm */
ITERM *ibase_pop_itermlist(IBASE *ibase);
/* push chunk */
void ibase_push_chunk(IBASE *ibase, ICHUNK *chunk);
/* ibase pop chunk */
ICHUNK *ibase_pop_chunk(IBASE *ibase);
/* push block */
void ibase_push_block(IBASE *ibase, char *block);
/* ibase pop block */
char *ibase_pop_block(IBASE *ibase);
/* push mtree */
void ibase_push_stree(IBASE *ibase, void *stree);
/* ibase pop mtree */
void *ibase_pop_stree(IBASE *ibase);
/* pop/push mmx */
void ibase_push_mmx(IBASE *ibase, void *mmx);
void *ibase_pop_mmx(IBASE *ibase);
/* bound items */
int ibase_bound_items(IBASE *ibase, int count);
/* read items */
int ibase_read_items(IBASE *ibase, int64_t *list, int count, char *out);
/* read summary with highlight,  return summary length */
int ibase_read_summary(IBASE *ibase, IQSET *qset, IRECORD *records, char *summary, 
        char *highlight_start, char *highlight_end);
/* clean */
void ibase_clean(IBASE *ibase);
#pragma pack(pop)
#ifdef __cplusplus
 }
#endif
#endif
