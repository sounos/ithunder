#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include "mtrie.h"
#include "mime.h"

/* doc_type_add */
int mime_add(MIME_MAP *mime_map, char *mime, int len)
{
    int id = -1;

    if(mime_map && mime)
    {
        id = mime_map->num;
        mtrie_add(mime_map->map, mime, len, ++(mime_map->num));
    }
    return id;
}

/* add mime line */
int mime_add_line(MIME_MAP *mime_map, char *p, char *end)
{
    int n = 0;
    char *s = NULL;

    if(mime_map && p && end)
    {
        while(p < end)
        {
            while(p < end && (*p == 0x20 || *p == '\t' || *p == '\r' || *p == '\n') ) ++p;
            s = p;
            while(p < end && (*p != ',' && *p != ';' && *p != 0x20 && *p != '\t'))++p;
            if((n = (p - s)) > 0)
            {
                mime_add(mime_map, s, n);
            }
            ++p;
        }
        return 0;
    }
    return -1;
}

/* query doc type map */
int mime_id(MIME_MAP *mime_map, char *mime, int len)
{
    int id = -1;

    if(mime_map && mime)
    {
        id = mtrie_get(mime_map->map, mime, len) - 1;
    }
    return id;
}

/* doc_type_map init */
int mime_map_init(MIME_MAP *mime_map)
{
    if(mime_map && mime_map->map == NULL)
    {
        mime_map->map = mtrie_init();
        return 0;
    }
    return -1;
}

/* clean doc_type_map */
void mime_map_clean(MIME_MAP *mime_map)
{
    if(mime_map)
    {
        mtrie_clean(mime_map->map);
    }
}
#ifdef _DEBUG_MIME
static char *mimelist =
"application/SLA;"
"application/STEP;"
"application/acad;"
"application/andrew-inset;"
"application/clariscad;"
"application/drafting;"
"application/dsptype;"
"application/dxf;"
"application/i-deas;"
"application/mac-binhex40;"
"application/mac-compactpro;"
"application/mspowerpoint;"
"application/msword;"
"application/octet-stream;"
"application/oda;"
"application/pdf;"
"application/postscript;"
"application/pro_eng;"
"application/set;"
"application/smil;"
"application/solids;"
"application/vda;"
"application/vnd.mif;"
"application/vnd.ms-excel;"
"application/x-bcpio;"
"application/x-cdlink;"
"application/x-chess-pgn;"
"application/x-cpio;"
"application/x-csh;"
"application/x-director;"
"application/x-dvi;"
"application/x-freelance;"
"application/x-futuresplash;"
"application/x-gtar;"
"application/x-gzip;"
"application/x-hdf;"
"application/x-ipix;"
"application/x-ipscript;"
"application/x-javascript;"
"application/x-koan;"
"application/x-latex;"
"application/x-lisp;"
"application/x-lotusscreencam;"
"application/x-netcdf;"
"application/x-sh;"
"application/x-shar;"
"application/x-shockwave-flash;"
"application/x-stuffit;"
"application/x-sv4cpio;"
"application/x-sv4crc;"
"application/x-tar;"
"application/x-tcl;"
"application/x-tex;"
"application/x-texinfo;"
"application/x-troff-man;"
"application/x-troff-me;"
"application/x-troff-ms;"
"application/x-troff;"
"application/x-ustar;"
"application/x-wais-source;"
"application/zip;"
"audio/TSP-audio;"
"audio/basic;"
"audio/midi;"
"audio/mpeg;"
"audio/x-aiff;"
"audio/x-pn-realaudio-plugin;"
"audio/x-pn-realaudio;"
"audio/x-realaudio;"
"audio/x-wav;"
"chemical/x-pdb;"
"image/cmu-raster;"
"image/gif;"
"image/ief;"
"image/jpeg;"
"image/png;"
"image/tiff;"
"image/x-portable-anymap;"
"image/x-portable-bitmap;"
"image/x-portable-graymap;"
"image/x-portable-pixmap;"
"image/x-rgb;"
"image/x-xbitmap;"
"image/x-xpixmap;"
"image/x-xwindowdump;"
"model/iges;"
"model/mesh;"
"model/vrml;"
"text/css;"
"text/html;"
"text/plain;"
"text/richtext;"
"text/rtf;"
"text/sgml;"
"text/tab-separated-values;"
"text/x-setext;"
"text/xml;"
"video/mpeg;"
"video/quicktime;"
"video/vnd.vivo;"
"video/x-fli;"
"video/x-msvideo;"
"video/x-sgi-movie;"
"www/mime;"
"x-conference/x-cooltalk;";
int main()
{
    MIME_MAP dmap = {0};
    char *p = NULL, *end = NULL;

    if(mime_map_init(&dmap) == 0)
    {
        p = mimelist ;
        end = p + strlen(p);
        mime_add_line(&dmap, p, end);
        p = "video/mpeg";fprintf(stdout, "%s:%d\n", p, mime_id(&dmap, p, strlen(p)));
        mime_map_clean(&dmap);
    }
}
//gcc -o dtmap mime.c utils/trie.c -I utils -D_DEBUG_MIME && ./dtmap
#endif
