#ifndef __XTASK_H__
#define __XTASK_H__
#define HOST_INCRE_NUM          100000   
#define PROXY_INCRE_NUM         10000
#define DNS_INCRE_NUM           256
#define URL_INCRE_NUM           1000000   
#define IP_INCRE_NUM            100000
#define USER_INCRE_NUM          256
#define Q_TYPE_URL              0x01
#define Q_TYPE_HOST             0x02
#define ERR_PROXY               0x01
#define ERR_HTTP_RESP           0x02
#define ERR_PROGRAM             0x04
#define ERR_CONTENT_TYPE        0x08
#define ERR_HOST_IP             0x10
#define ERR_TASK_CONN           0x20
#define ERR_TASK_TIMEOUT        0x40
#define ERR_DATA                0x80
#define ERR_NETWORK             0x100
#define ERR_NODATA              0x200
#define ERR_NEED_RETRY          (ERR_HTTP_RESP|ERR_NETWORK|ERR_TASK_TIMEOUT|ERR_PROGRAM|ERR_TASK_CONN|ERR_PROXY|ERR_HOST_IP)
#define URL_IS_POST             0x01
#define URL_IS_LIST             0x02
#define URL_IS_FILE             0x04
#define URL_IS_PAGE             0x08
#define URL_IS_LINK             0x1000
#define URL_LEN_MAX             8192
#define PATTERN_LEN_MAX         4096
#define XT_PATTERN_SIZE         16384 
#define XT_URL_SIZE             4096
#define XT_LEVEL_UP             1
#define XT_LEVEL_DOWN           -1
#define XT_PAGE_NUM             32
#define XT_DOCUMENT_MAX         1048576 
#define XT_BLOCKS_MAX           1024
#define XT_DUMP_TASK_MAX        1024
#define XT_DUMP_MAX             8589934592 
#define PROXY_STATUS_OK         1
#define PROXY_STATUS_ERR        -1
#define HOST_STATUS_READY       1
#define HOST_STATUS_OK          2
#define HOST_STATUS_ERR         -1
#define HOST_LEVEL_FIFO         1
#define HOST_LEVEL_RR           0
#define URL_STATUS_INIT         0
#define URL_STATUS_OK           1
#define URL_STATUS_ERR          -1
#define DNS_STATUS_OK           1
#define DNS_STATUS_ERR          -1
#define DNS_STATUS_READY        2
#define TASK_WAIT_TIMEOUT       1000000
#define TASK_WAIT_MAX           10000000
#define TASK_RETRY_TIMES        4
#define DNS_TIMEOUT_MAX         4
#define DNS_PATH_MAX            256
#define DNS_BUF_SIZE            65536
#define DNS_TASK_MAX            32
#define DNS_IP_MAX              16
#define DNS_NAME_MAX            128
#define HTML_MAX_SIZE           1048576
#define TASK_STATE_INIT         0x00
#define TASK_STATE_OK           0x02
#define TASK_STATE_ERROR        0x046
#define XT_SPEED_INTERVAL       1000000
#define COOKIE_LINE_MAX         8192
#define XT_IP_MAX       64
#define XT_PATH_MAX     256
#define XT_NAME_MAX     64
#define XT_NODES_MAX    256
#define XT_TEXT_FROM    0
#define XT_INT_FROM     32
#define XT_LONG_FROM    64
#define XT_DOUBLE_FROM  96  
#define XT_DISPLAY_FROM 128
#define XT_FIELDS_MAX   256
#define XT_REGX_MAX     32
#define XT_DISPLAY_MAX  128
#define XT_INDEX_MAX    32
#define XT_LINE_MAX     65536
#define XT_TABLE_MAX    100000
#define XT_TABLE_BASE   1000
#define XT_NODE_MAX     40000000
#define XT_NODE_BASE    100000
#define XT_PROXY_MAX    1000000
#define XT_PROXY_BASE   10000
#define XT_DNS_MAX      100000
#define XT_DNS_BASE     1000
#define XT_HOST_MAX     50000000
#define XT_HOST_BASE    100000
#define XT_URL_MAX      2000000000
#define XT_URL_BASE     100000
#define XT_URLNODE_MAX  2000000000
#define XT_URLNODE_BASE 100000
#define XT_STATUS_INIT  0x00
#define XT_STATUS_OK    0x01
#define XT_TASK_TYPE_NORMAL      0x00
#define XT_TASK_TYPE_UPDATE      0x01
#define XT_TASK_TYPE_FILE        0x02
#define XT_TASK_TYPE_PRIORITY    0x04
#define HTTP_USER_AGENT         "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/534.54.16 (KHTML, like Gecko) Version/5.1.4 Safari/534.54.16"
#define HTTP_ACCEPT_TYPE        "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
#define HTTP_ACCEPT_LANGUAGE    "zh-cn,zh;q=0.5"
#define HTTP_ACCEPT_ENCODING    "deflate,gzip"
#define HTTP_ACCEPT_CHARSET     "utf-8,gb2312,gbk;q=0.7,*;q=0.7"
#define HTTP_RESP_OK            "HTTP/1.0 200 OK"
#define HTTP_BAD_REQUEST        "HTTP/1.0 400 Bad Request\r\n\r\n"
#define HTTP_NOT_FOUND          "HTTP/1.0 404 Not Found\r\n\r\n" 
#define HTTP_NOT_MODIFIED       "HTTP/1.0 304 Not Modified\r\n\r\n"
#define HTTP_NO_CONTENT         "HTTP/1.0 204 No Content\r\n\r\n"
#define HTTP_DOWNLOAD_TIMEOUT   10000000
#define HTTP_TASK_MAX           64
#define HTTP_BLOCK_MAX          2097152
#define HTTP_CHUNK_MAX          8388608 
#define HTTP_URL_MAX            8192
#define HTTP_HOST_MAX           256
#define CHARSET_MAX             64
#define HTTP_DEFAULT_PORT       80
#define XT_REQ_DOWNLOAD     0x01
#define XT_RESP_DOWNLOAD    0x1001
#define XT_REQ_EXTRACT      0x02
#define XT_RESP_EXTRACT     0x1002
#define XT_TASK_PAGE        0x00
#define XT_TASK_LIST        0x01
#define XT_TASK_FILE        0x02
#define XT_TASK_UPDATE      0x04
#define XT_TASK_RETRY       0x08
#define XT_TASK_OVER        0x10
#define HTTP_TASK_INIT      0x00
#define HTTP_TASK_READY     0x01
#define HTTP_TASK_OVER      0x02
#define XT_TASK_SIZE        8388608
#define HTTP_HEADERS_MAX    8192
#define XT_EXTRACT_MAX      1024
#define XT_ROW_MAX          65536         
#define XT_CHUNK_SIZE       8388608
//#define XT_CHUNK_SIZE       33554432
//#define XT_CHUNK_SIZE       67108864
#define XT_CHUNK_MAX        8388608
typedef struct _XTHEAD
{
    short status;
    short flag;
    int cmd;
    int length;
}XTHEAD;
#define XT_COOKIE_MAX       64
typedef struct _XTKV
{
    char *k;
    char *v;
    short nk;
    short nv;
    int id;
}XTKV;
typedef struct _XTMETA
{
    short status;
    short respid;
    short port;
    short proxy_port;
    int id;
    int ip;
    int proxy_ip;
    int nurl;
    int nheaders;
}XTMETA;
typedef struct _XTREC
{
    int id;
    int length;
    int err;
    int last_modified;
    short nlocation;
    short ncookie;
}XTREC;
typedef struct _XTROW
{
    int id;
    short nurl;
    short ntemplates;
    int length;
}XTROW;
/*
typedef struct _XTLINK
{
    int nurl;
    int nodeid;
    int parent;
    int fieldid;
    int flag;
}XTLINK;
*/
typedef struct _XTRES
{
    int id;
    int nodeid;
    int length;
}XTRES;
typedef struct _XTREG
{
    int length;
    int nodeid;
    short flag;
    short fieldid; 
}XTREG;
typedef struct _XTITEM
{
    int tableid;
    int length;
    int count;
    XTREG regs[XT_REGX_MAX];
}XTITEM;
typedef struct _XTPNEXT
{
    int next;
    int len;
}XTPNEXT;
typedef struct _XTMM
{
    int head;
    int tail;
    short length;
    short flag;
}XTMM;
typedef struct _XTUNIT
{
    int off;
    int length;
    int flag;
}XTUNIT;
typedef struct _XTRECORD
{
    int tableid;
    int length;
    int id;
    XTUNIT mms[XT_FIELDS_MAX];
}XTRECORD;
typedef struct _XTFIELD
{
    int id;
    short status;
    short flag;
    char name[XT_NAME_MAX];
}XTFIELD;
typedef struct _XTTABLE
{
    int   status;
    short text_index_total;
    short int_index_total;
    short long_index_total;
    short double_index_total;
    short display_total;
    short nfields;
    int   bits;
    char  name[XT_NAME_MAX];
    XTFIELD fields[XT_FIELDS_MAX];
    /* textIndex[0-31] intIndex[32-63] longIndex[64-95] 
     * doubleIndex[96-127] display[128-255] */
}XTTABLE;
typedef struct _XTNODE
{
    short status;
    short level;
    int id;
    int uid;
    int mapid;
    int parent;
    int nchilds;
    int childs_root;
    int ntemplates;
    int templates_root;
    int nurlchilds;
    int urlroot;
    char name[XT_NAME_MAX];
}XTNODE;
typedef struct _XTURLNODE
{
    short status;
    short level;
    int flag;
    int id;
    int parent;
    int brother;
    int nodeid;
    int hostid;
    int host_mid;
    int urlmap_id;
    int url_db_id;
    int nchilds;
    int childs_root;
    int childs_mid;
    int last_modified;
}XTURLNODE;
#define REG_IS_LINK              0x01
#define REG_IS_FILE              0x02
#define REG_IS_NEED_CLEARHTML    0x04
#define REG_IS_NEED_ANTISPAM     0x08
#define REG_IS_POST              0x10
#define REG_IS_UNIQE             0x20
#define REG_IS_LIST              0x40
#define REG_IS_URL               0x80
#define REG_IS_PAGE              0x100
#define REG_IS_DATETIME          0x200
typedef struct _IREGX
{
    short flag;
    short fieldid;
    int   nodeid;
}IREGX;
/* url pattern list */
#define PURL_TYPE_CHAR 0
#define PURL_TYPE_INT  1
#define PURL_NUM_MAX 256
typedef struct _PURL
{
    short type;
    short length;
    int from;
    int to;
    int bits;
    char *sfrom;
    char *sto;
}PURL;
#define TMP_IS_PUBLIC       0x01
#define TMP_IS_GLOBAL       0x02
#define TMP_IS_IGNORECASE   0x04
#define TMP_IS_LINK         0x08
#define TMP_IS_POST         0x10
/* template regular expression */
typedef struct _ITEMPLATE
{
    short status;
    short nfields;
    IREGX map[XT_FIELDS_MAX];
    char  pattern[PATTERN_LEN_MAX];
    char  url[XT_URL_SIZE];
    char  link[XT_URL_SIZE];
    int   tableid;
    IREGX linkmap;
    int   flags;
    int   nodeid;
    int   mmid;
}ITEMPLATE;
/*proxy */
typedef struct _XTPROXY
{
    int ip;
    unsigned short port;
    short status;
}XTPROXY;
typedef struct _XTDNS
{
    int status;
    int ip;
}XTDNS;
typedef struct _XTHOST
{
    short status;
    short level;
    int id;
    int db_host_id;
    int db_ip_id;
    int db_cookie_id;
    int nurlchilds;
    int urlroot;
    int qwait;
}XTHOST;
typedef struct _XTURL
{
    int status;
    int id;
}XTURL;
typedef struct _XTIO
{
    int     fd;
    int     bits;
    char    *map;
    off_t   old;
    off_t   end;
    off_t   size;
}XTIO;
typedef struct _XTSTATE
{
    char dumpdir[XT_PATH_MAX];
    int  dump_id_max;
    int status;
    int table_id_max;
    int table_id_left;
    int table_total;
    int field_id_max;
    int node_id_max;
    int node_id_left;
    int node_total;
    int template_id_max;
    int node_uid_max;
    int proxy_id_max;
    int proxy_id_left;
    int proxy_total;
    int dns_id_max;
    int dns_id_left;
    int dns_total;
    int host_id_max;
    int host_id_left;
    int host_total;
    int cookie_uid_max;
    int urlnode_id_max;
    int urlnode_id_left;
    int urlnode_total;
    int urlmap_root;
    int qproxy;
    int qdns;
    int qhost;
    int qlist;
    int qpriority;
    int qwait;
    int qfile;
    int qupdate;
    int qextract;
    int qdump;
    int download_task_id;
    int file_task_id;
    int update_task_id;
    int extract_task_id;
    int host_task_wait;
    int download_task_wait;
    int file_task_wait;
    int update_task_wait;
    int extract_task_wait;
    int qtask_download;
    int qtask_file;
    int qtask_update;
    int qtask_extract;
    int qfile_dump;
    int qretry;
}XTSTATE;
typedef struct _XTASK
{
    char basedir[XT_PATH_MAX];
    XTIO stateio;
    XTIO tableio;
    XTIO nodeio;
    XTIO proxyio; 
    XTIO dnsio; 
    XTIO hostio; 
    XTIO urlnodeio; 
    XTSTATE *state;
    int dumpfd;
    int nqblocks;
    char *qblocks[XT_BLOCKS_MAX];
    void *logger;
    void *mmdb;
    void *temp;
    void *db;
    void *res;
    void *queue;
    void *kmap;
    void *map;
    void *urlmap;
    void *mutex;
    void *table_mutex;
    void *node_mutex;
    void *proxy_mutex;
    void *dns_mutex;
    void *host_mutex;
    void *urlnode_mutex;
    void *download_mutex;
    void *extract_mutex;
    void *record_mutex;
    void *block_mutex;
    void *dump_mutex;
}XTASK;
/* initialize XTASK */
XTASK *xtask_init(char *basedir);
/* add table */
int xtask_add_table(XTASK *xtask, char *name);
/* add field */
int xtask_add_field(XTASK *xtask, int tableid, int flag, char *name);
/* rename field */
int xtask_rename_field(XTASK *xtask, int tableid, int fieldid, char *name);
/* delete field */
int xtask_delete_field(XTASK *xtask, int tableid, int fieldid);
/* rename table */
int xtask_rename_table(XTASK *xtask, int id, char *name);
/* delete table */
int xtask_delete_table(XTASK *xtask, int id);
/* view table */
int xtask_view_table(XTASK *xtask, int id, char *block, int nblock);
/* list tables */
int xtask_list_tables(XTASK *xtask, char *block, int nblock);
/* view database */
int xtask_view_database(XTASK *xtask, char *block, int nblock);
/* add node */
int xtask_add_node(XTASK *xtask, int parent, char *name);
/* update node */
int xtask_update_node(XTASK *xtask, int parent, int nodeid, char *name);
/* get node */
int xtask_get_node(XTASK *xtask, int id, XTNODE *node);
/* delete node */
int xtask_delete_node(XTASK *xtask, int nodeid);
/* get node childs */
int xtask_get_node_childs(XTASK *xtask, int nodeid, XTNODE *node);
/* view childs node */
int xtask_view_node_childs(XTASK *xtask, int nodeid, char *out, int nout);
/* view node url childs */
int xtask_view_urlchilds(XTASK *xtask, int nodeid, char *out, int nout, int from, int to);
/* add template */
int xtask_add_template(XTASK *xtask, int nodeid, ITEMPLATE *);
/* get template */
int xtask_get_template(XTASK *xtask, int templateid, ITEMPLATE *);
/* delete template */
int xtask_delete_template(XTASK *xtask, int nodeid, int templateid);
/* update template */
int xtask_update_template(XTASK *xtask, int templateid, ITEMPLATE *);
/* view templates */
int xtask_view_templates(XTASK *xtask, int nodeid, char *out, int nout);
/* get node templates */
int xtask_get_node_templates(XTASK *xtask, int nodeid, char *out, int nout);
/* List tnode */
int xtask_list_nodes(XTASK *xtask, int nodeid, FILE *fp);
/* add proxy */
int xtask_add_proxy(XTASK *xtask, char *host);
/* get proxy */
int xtask_get_proxy(XTASK *xtask, XTPROXY *proxy);
/* set proxy status */
int xtask_set_proxy_status(XTASK *xtask, int id, char *host, int status);
/* delete proxy */
int xtask_del_proxy(XTASK *xtask, int id, char *host);
/* view proxy */
int xtask_view_proxylist(XTASK *xtask, char *block, int nblock);
/* add dns */
int xtask_add_dns(XTASK *xtask, char *dns);
/* set dns status */
int xtask_set_dns_status(XTASK *xtask, int id, char *dns, int status);
/* delete dns */
int xtask_del_dns(XTASK *xtask, int id, char *dns);
/* pop dns */
int xtask_pop_dns(XTASK *xtask, char *dns);
/* view dns list*/
int xtask_view_dnslist(XTASK *xtask, char *block, int nblock);
/* add host */
int xtask_add_host(XTASK *xtask, char *host);
/* pop host */
int xtask_pop_host(XTASK *xtask, char *host);
/* reset host */
void xtask_reset_host(XTASK *xtask, int hostid);
/* set host iplist */
int xtask_set_host_ip(XTASK *xtask, char *host, int *iplist, int niplist);
/* get host ip */
int xtask_get_host_ip(XTASK *xtask, char *host);
/* set host status*/
int xtask_set_host_status(XTASK *xtask, int id, int status);
/* set host level*/
int xtask_set_host_level(XTASK *xtask, int id, int level);
/* set host cookie */
int xtask_set_host_cookie(XTASK *xtask, int id, char *cookie);
/* delete host cookie */
int xtask_del_host_cookie(XTASK *xtask, int id);
/* add url */
int xtask_add_url(XTASK *xtask, int parentid, int nodeid, char *url, int flag);
/* get url */
int xtask_get_url(XTASK *xtask, int urlid, char *url);
/* view urlurlnode childs */
int xtask_view_urlnodes(XTASK *xtask, int urlnodeid, char *out, int nout, int from, int to);
/* set url status */
int xtask_set_url_status(XTASK *xtask, int urlid, char *url, short status, short err);
/* set url level */
int xtask_set_url_level(XTASK *xtask, int urlid, char *url, short level);
/* get url task */
int xtask_get_url_task(XTASK *xtask);
/* get urlnode */
int xtask_get_urlnode(XTASK *xtask, int urlnodeid, XTURLNODE *);
/* delete urlnode */
int xtask_delete_urlnode(XTASK *xtask, int urlnodeid);
/* new download task */
int xtask_new_download(XTASK *xtask, int flag, int *id, char *out, int nout);
/* reset download task */
int xtask_reset_download(XTASK *xtask, int flag, int taskid);
/* over download task */
int xtask_over_download(XTASK *xtask, int flag, int id, char *data, int ndata);
/* new extract task */
int xtask_new_extract(XTASK *xtask, int *id, char *out, int nout);
/* reset extract task */
int xtask_reset_extract(XTASK *xtask, int taskid);
/* over extract task */
int xtask_over_extract(XTASK *xtask, int id, char *data, int ndata);
/* view record */
int xtask_view_record(XTASK *xtask, int urlid, char *data, int ndata);
/* set dump dir */
void xtask_set_dumpdir(XTASK *xtask, char *dir);
/* dump record */
int xtask_dump(XTASK *xtask);
/* close xtask */
void xtask_clean(XTASK *xtask);
#endif
