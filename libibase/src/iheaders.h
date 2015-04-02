#ifdef IB_GLOBALID_32BIT
/* document header */
typedef struct _DOCHEADER
{
    int     status;
    int     nfields;
    int     globalid;
    int     crc;
    int     nterms;
    int     terms_total;
    int     content_off;
    int     content_size;
    int     content_zsize;
    int     nexts_off;
    int     nexts_size;
    int     textblock_off;
    int     textblock_size;
    int     intindex_from;
    int     intblock_off;
    int     intblock_size;
    int     longindex_from;
    int     longblock_off;
    int     longblock_size;
    int     doubleindex_from;
    int     doubleblock_off;
    int     doubleblock_size;
    int     size;
    int     slevel;  
    int64_t category;
    double  rank;
}DOCHEADER;

typedef struct _FHEADER
{
    short   status;
    short   flag;
    int     nfields;
    int     globalid;
    int     crc;
    int     size;
    int     slevel;  
    int64_t category;
    double  rank;
}FHEADER;
/* typedef struct xheader */
typedef struct _XHEADER
{
    int     status;
    int     globalid;
    int     nints;
    int     nlongs;
    int     ndoubles;
    int     slevel;
    int64_t category;
    double  rank;
}XHEADER;
/* index header */
typedef struct _IHEADER
{
    short   status;
    short   terms_total;
    int     slevel;
    int     globalid;
    int     crc;
    int64_t category;
    double  rank;
}IHEADER;
#else
/* document header */
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
    int     terms_total;
    int     crc;
    int     size;
    int     content_off;
    int     content_size;
    int     content_zsize;
    int     nexts_off;
    int     nexts_size;
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

/* doc header for index */
typedef struct _IHEADER
{
    int     status;
    int     slevel;
    int     terms_total;
    int     crc;
    int64_t globalid;
    int64_t category;
    double  rank;
}IHEADER;
#endif
