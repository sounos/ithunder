#include <stdio.h>
#include <unistd.h>
#include <string.h>
#ifndef _HTML_H
#define _HTML_H
#define HTML_TAG_NMAX 2000000
#define HTML_FIELD_MAX 256
#define LEVEL_MAX  100000
#define HTML_CONTENT_MAX  16777216 
#define HTML_TITLE_MAX  1048576
#define HTML_LINK_SCALE 0.50f
typedef struct _HBLOCK
{
    char *start;
    int nbytes;
    int nlinks;
    struct _HBLOCK *parent;
    int npairs;
    char *last;
    int id;
}HBLOCK;
typedef struct _HFIELD
{
    int from;
    int len;
}HFIELD;
typedef struct _HTML
{
    char *block;
    int nblock;
	char *content;
	int ncontent;
	char *title;
	int ntitle;
    int titleoff;
	HBLOCK blocklist[HTML_TAG_NMAX];
    int nblocks;
    HFIELD fieldlist[HTML_FIELD_MAX];
    int nfields;
    void *table;
		
	int 	(*get_content)(struct _HTML *html, char *content, size_t len, 
            int filter, int new_field);
    int     (*add_field)(struct _HTML *html, char *text, int ntext);
	void 	(*reset)(struct _HTML *html);
	void	(*clean)(struct _HTML **html);
}HTML;
/* Initialize HTML */
HTML *html_init();
/* clean html */
void html_clean(HTML **);
#endif
