#ifdef __ITASK__H__
#define __ITASK__H__
#define QT_PATH_MAX     256
typedef struct _QHEAD
{
    int cmd;
    int flag;
    int req_queue_no;
    int over_queue_no;
    int64_t taskid;
}QHEAD;
typedef struct _QTASK
{
    int ntasks;
    void *qhead;
    void *queue;
    void *waitover;
}QTASK;
typedef struct _QTSTATE
{
    int nqueues;

}QTSTATE;
typedef struct _ITASK
{
    char basedir[QT_PATH_MAX];
    void *mutex;
    void *logger;
}ITASK;
ITASK *itask_init(char *basedir);
int itask_push_task(ITASK *itask, int queue_no, int flag, int64_t id);
int64_t itask_pop_task(ITASK *itask, int queue_no);
void itask_clean(ITASK *itask);
#endif
