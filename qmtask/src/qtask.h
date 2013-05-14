#include "mutex.h"
#ifndef __QTASK__H__
#define __QTASK__H__
#define QT_PATH_MAX     256
#define QT_TASKS_MAX    256
#define QT_NAME_MAX     128
#define QT_TASK_TOP     0x01
#define QT_LINE_MAX     256
typedef struct _XTASK
{
    int     status;
    int     bits;
    int     qhead;
    int     queue;
    int     wait;
    int     workers;
    off_t   total;
    off_t   over;
    char name[QT_NAME_MAX];
}XTASK;
/* FILE IO */
typedef struct _QTIO
{
    int     fd;
    int     bits;
    void    *map;
    off_t   old;
    off_t   end;
    off_t   size;
}QTIO;
typedef struct _QTSTATE
{
    int qtasks;
    int id_max;
    int old;
    int bits;
    XTASK tasks[QT_TASKS_MAX];
}QTSTATE;
typedef struct _QTASK
{
    int     log_access;
    int     bits;
    MUTEX   *mutex;
    void    *logger;
    void    *map;
    void    *qhead;
    void    *queue;
    void    *wait;
    void    *old;
    void    *db;
    QTSTATE *state;
    QTIO    stateio;
    char    basedir[QT_PATH_MAX];
}QTASK;
QTASK *qtask_init(char *basedir);
int qtask_add_task(QTASK *qtask, char *name);
int qtask_rename_task(QTASK *qtask, int taskid, char *name);
int qtask_del_task(QTASK *qtask, int taskid);
int qtask_list_tasks(QTASK *qtask, char *out, char *end);
int qtask_update_workers(QTASK *qtask, int taskid, int num);
int qtask_gen_packet(QTASK *qtask,  char *packet, int length);
int qtask_push_packet(QTASK *qtask, int taskid, int flag, int packetid);
int qtask_over_packet(QTASK *qtask, int taskid, int packetid);
int qtask_pop_packet(QTASK *qtask, int taskid, int *packet_len);
int qtask_read_packet(QTASK *qtask, int taskid, int packetid, char *packet);
int qtask_remove_packet(QTASK *qtask, int packetid);
int qtask_repacket(QTASK *qtask, int taskid, int packetid);
void qtask_clean(QTASK *qtask);
#endif
