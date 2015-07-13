#ifndef __BMAP_H__
#define __BMAP_H__
#ifdef __cplusplus
extern "C" {
#endif
#include <pthread.h>
#define BMAP_ID_MAX     2000000000
#define BMAP_BASE_NUM   1000000 
typedef struct _BMAP
{
    int fd;
    int id_max;
    int bytes;
    int bit32;
    char *mbits;
    char *bits;
    pthread_rwlock_t mutex; 
}BMAP;
void *bmap_init(char *file);
int bmap_set(void *p, int id);
int bmap_unset(void *p, int id);
int bmap_check(void *p, int id);
void bmap_clean(void *bmap);
#ifdef __cplusplus
 }
#endif
#endif
