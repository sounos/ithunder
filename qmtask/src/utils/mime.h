#ifndef _MIME_H
#define _MIME_H
#define MIME_NUM_MAX   1024
typedef struct _MIME_MAP
{
    void *map;
    int num;
    int bits;
}MIME_MAP;
/* initialize mime */
int mime_map_init(MIME_MAP *mime_map);
/* add mime */
int mime_add(MIME_MAP *mime_map, char *mime, int len);
/* add mime line */
int mime_add_line(MIME_MAP *mime_map, char *p, char *end);
/* return mime id*/
int mime_id(MIME_MAP *mime_map, char *mime, int len);
/* clean mime map*/
void mime_map_clean(MIME_MAP *mime_map);
#endif
