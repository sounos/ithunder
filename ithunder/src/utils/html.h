#include <stdio.h>
#include <unistd.h>
#include <string.h>
#ifndef _HTML_H
#define _HTML_H
#define HTML_TAG_NMAX 200000
#define HTML_FIELD_MAX 256
#define LEVEL_MAX  10000
//#define HTML_CONTENT_MAX  16777216 
//#define HTML_CONTENT_MAX  2097152
//#define HTML_CONTENT_MAX  4194304
//#define HTML_TITLE_MAX  1048576
#define HTML_MBLOCK_MAX  1048576
#define HTML_MTEXT_MAX   1048576
#define HTML_MLINK_MAX   1048576
#define HTML_TITLE_MAX   65536
#define HTML_LINK_SCALE  0.50f
#define HTML_LINK_FILTER 50
typedef struct _HBLOCK
{
    int npairs;
    int nbytes;
    int nlinks;
    int id;
    char *start;
    char *last;
    struct _HBLOCK *parent;
}HBLOCK;
typedef struct _HFIELD
{
    int from;
    int len;
}HFIELD;
#define HTML_LINK_IMG   0x01
#define HTML_LINK_URL   0x02
#define HTML_LINKS_MAX  8192
typedef struct _HLINK
{
    int off;
    short ntitle;
    short nauthor;
    short nlink; 
    short flag;
}HLINK;
typedef struct _HTML
{
    char block[HTML_MBLOCK_MAX];
	char content[HTML_MTEXT_MAX];
	char link[HTML_MLINK_MAX];
    char *plink;
    int nblock;
	int ncontent;
	int ntitle;
    int titleoff;
    int nblocks;
    int nfields;
    int nlinks;
	HBLOCK blocklist[HTML_TAG_NMAX];
    HFIELD fieldlist[HTML_FIELD_MAX];
    HLINK  linklist[HTML_LINKS_MAX];
    void *table;
}HTML;
/* Initialize HTML */
HTML *html_init();
/* get content */
int html_get_content(HTML *html, char *content, size_t len, int filter, int new_filed);
/* add field */
int html_add_field(HTML *html, char *content, int ncontent);
/* reset HTML */
void html_reset(HTML *);
/* clean html */
void html_clean(HTML *);
#endif
