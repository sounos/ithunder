#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/time.h>
#include "qtask.h"
#include "logger.h"
#include "mutex.h"
#include "mmtrie.h"
#include "mmtree.h"
#include "mmqueue.h"
#include "mtask.h"
#include "db.h"
#define QT_LOG_NAME     "qtask.log"
#define QT_STATE_NAME   "qtask.state"
#define QT_MAP_NAME     "qtask.map"
#define QT_QHEAD_NAME   "qtask.qhead"
#define QT_QUEUE_NAME   "qtask.queue"
#define QT_WAIT_NAME    "qtask.wait"
#define QT_OLD_NAME     "qtask.old"
#define QT_DB_NAME      "db"
#define LL64(xxxx) ((long long int)xxxx)
int pmkdir(char *path)
{
    struct stat st;
    char fullpath[QT_PATH_MAX];
    char *p = NULL;
    int level = -1, ret = -1;

    if(path)
    {
        strcpy(fullpath, path);
        p = fullpath;
        while(*p != '\0')
        {
            if(*p == '/' )
            {
                while(*p != '\0' && *p == '/' && *(p+1) == '/')++p;
                if(level > 0)
                {
                    *p = '\0';
                    memset(&st, 0, sizeof(struct stat));
                    ret = stat(fullpath, &st);
                    if(ret == 0 && !S_ISDIR(st.st_mode)) return -1;
                    if(ret != 0 && mkdir(fullpath, 0755) != 0) return -1;
                    *p = '/';
                }
                level++;
            }
            ++p;
        }
        return 0;
    }
    return -1;
}

QTASK *qtask_init(char *basedir)
{
    int i = 0, data = 0, nodeid = 0, rootid = 0, packetid = 0;
    char path[QT_PATH_MAX];
    struct stat st = {0};
    QTASK *qtask = NULL;

    if(basedir && (qtask = (QTASK *)calloc(1, sizeof(QTASK))))
    {
        MUTEX_INIT(qtask->mutex);
        strcpy(qtask->basedir, basedir);
        sprintf(path, "%s/%s", basedir, QT_LOG_NAME);
        pmkdir(path);
        LOGGER_INIT(qtask->logger, path);
        sprintf(path, "%s/%s", basedir, QT_MAP_NAME);
        qtask->map = mmtrie_init(path);
        sprintf(path, "%s/%s", basedir, QT_QHEAD_NAME);
        qtask->qhead = mmqueue_init(path);
        sprintf(path, "%s/%s", basedir, QT_QUEUE_NAME);
        qtask->queue = mmqueue_init(path);
        sprintf(path, "%s/%s", basedir, QT_OLD_NAME);
        qtask->old = mmqueue_init(path);
        sprintf(path, "%s/%s", basedir, QT_WAIT_NAME);
        qtask->wait = mmtree_init(path);
        sprintf(path, "%s/%s", basedir, QT_DB_NAME);
        qtask->db = db_init(path, 0);
        /* state */
        sprintf(path, "%s/%s", basedir, QT_STATE_NAME);
        if((qtask->stateio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(qtask->stateio.fd, &st) == 0)
        {
            if((qtask->stateio.map = mmap(NULL, sizeof(QTSTATE), PROT_READ|PROT_WRITE,
                    MAP_SHARED, qtask->stateio.fd, 0)) && qtask->stateio.map != (void *)-1)
            {
                qtask->state = (QTSTATE *)qtask->stateio.map;
                if(st.st_size < sizeof(QTSTATE))
                {
                    if(ftruncate(qtask->stateio.fd, sizeof(QTSTATE))) _exit(-1);
                    memset(qtask->state, 0, sizeof(QTSTATE));
                }
                qtask->stateio.size = sizeof(QTSTATE);
                if(qtask->state->old <= 0) 
                    qtask->state->old = mmqueue_new(qtask->old);
                for(i = 1; i < QT_TASKS_MAX; i++)
                {
                    if((rootid = qtask->state->tasks[i].wait) > 0)
                    {
                        while(mmtree_total(qtask->wait, qtask->state->tasks[i].wait) > 0)
                        {
                            if((nodeid = mmtree_min(qtask->wait, rootid, &packetid, &data)) > 0)
                            {
                                mmtree_remove(qtask->wait, rootid, nodeid, NULL, NULL);
                                mmqueue_push(MMQ(qtask->qhead), qtask->state->tasks[i].qhead, packetid);
                            }
                        }
                    }
                    qtask->state->tasks[i].workers = 0;
                }
            }

        }
    }
    return qtask;
}

/* add task */
int qtask_add_task(QTASK *qtask, char *name)
{
    int taskid = -1, i = 0, n = 0;

    if(qtask && name)
    {
        MUTEX_LOCK(qtask->mutex);
        if(qtask->state->qtasks < QT_TASKS_MAX && (n = strlen(name)) > 0
                && (taskid = (mmtrie_get(qtask->map, name, n) - 1) < 0)) 
        {
            for(i = 1; i < QT_TASKS_MAX; i++)
            {
                if(qtask->state->tasks[i].status <= 0)
                {
                    qtask->state->tasks[i].status = 1;
                    strcpy(qtask->state->tasks[i].name, name);
                    mmtrie_add(qtask->map, name, n, i);
                    qtask->state->tasks[i].qhead = mmqueue_new(MMQ(qtask->qhead)); 
                    qtask->state->tasks[i].queue = mmqueue_new(MMQ(qtask->queue)); 
                    qtask->state->tasks[i].wait = mmtree_new_tree(qtask->wait); 
                    taskid = i;
                    qtask->state->qtasks++;

                    //if(i == 1)fprintf(stdout, "%s::%d taskid:%d status:%d qhead:%d queue:%d\n", __FILE__, __LINE__, taskid, qtask->state->tasks[taskid].status, qtask->state->tasks[taskid].qhead, qtask->state->tasks[taskid].queue);
                    break;
                }
            }
        }
        MUTEX_UNLOCK(qtask->mutex);
    }
    return taskid;
}

/* rename task */
int qtask_rename_task(QTASK *qtask, int taskid, char *name)
{
    int ret = -1, n = 0, x = 0;

    if(qtask && taskid > 0 && taskid < QT_TASKS_MAX && name)
    {
        MUTEX_LOCK(qtask->mutex);
        if(qtask->state->qtasks > 0 && qtask->state->tasks[taskid].status > 0
                && (x = strlen(qtask->state->tasks[taskid].name)) > 0
                && (n = strlen(name)) > 0 && (mmtrie_get(qtask->map, name, n) - 1) < 0)
        {
            mmtrie_del(qtask->map, qtask->state->tasks[taskid].name, x);
            mmtrie_add(qtask->map, name, n, taskid);
            strcpy(qtask->state->tasks[taskid].name, name);
            ret = taskid;
        }
        MUTEX_UNLOCK(qtask->mutex);
    }
    return ret;
}

/* rename task */
int qtask_del_task(QTASK *qtask, int taskid)
{
    int ret = -1, n = 0;

    if(qtask && taskid > 0 && taskid < QT_TASKS_MAX)
    {
        MUTEX_LOCK(qtask->mutex);
        if(qtask->state->qtasks > 0 && qtask->state->tasks[taskid].status > 0
                && (n = strlen(qtask->state->tasks[taskid].name)) > 0)
        {
            mmtrie_del(qtask->map, qtask->state->tasks[taskid].name, n);
            mmtree_remove_tree(qtask->wait, qtask->state->tasks[taskid].wait); 
            mmqueue_close(MMQ(qtask->qhead), qtask->state->tasks[taskid].qhead); 
            mmqueue_close(MMQ(qtask->queue), qtask->state->tasks[taskid].queue); 
            memset(&(qtask->state->tasks[taskid]), 0, sizeof(XTASK));
            qtask->state->qtasks--;
            ret = taskid;
        }
        MUTEX_UNLOCK(qtask->mutex);
    }
    return ret;

}

/* list task */
int qtask_list_tasks(QTASK *qtask, char *out, char *end)
{
    int ret = -1, i = 0, worker = 0, qhead = 0, queue = 0, wait = 0;
    char *p = NULL, *pp = NULL;

    if(qtask && out && end && qtask->state->qtasks > 0)
    {
        MUTEX_LOCK(qtask->mutex);
        p = out;
        p += sprintf(p, "({'count':'%d','tasks':{", qtask->state->qtasks);
        pp = p;
        for(i = 1; i < QT_TASKS_MAX; i++)
        {
            if(qtask->state->tasks[i].status > 0 && p < end)
            {
                worker = qtask->state->tasks[i].workers;
                qhead = mmqueue_total(MMQ(qtask->qhead), qtask->state->tasks[i].qhead);
                queue = mmqueue_total(MMQ(qtask->queue), qtask->state->tasks[i].queue);
                wait = mmtree_total(qtask->wait, qtask->state->tasks[i].wait);
                p += sprintf(p, "'%d':{'name':'%s', 'worker':'%d','qhead':'%d', "
                        "'queue':'%d', 'wait':'%d', 'total':'%lld', 'over':'%lld'},", i, 
                        qtask->state->tasks[i].name, worker, qhead, queue, wait,
                        LL64(qtask->state->tasks[i].total), 
                        LL64(qtask->state->tasks[i].over));
            }
        }
        if(p != pp)--p;
        p += sprintf(p, "}})");
        ret = p - out;
        MUTEX_UNLOCK(qtask->mutex);
    }
    return ret;
}

/* update workers */
int qtask_update_workers(QTASK *qtask, int taskid, int num)
{
    int ret = -1;
    if(qtask && taskid > 0 && taskid < QT_TASKS_MAX)
    {
        MUTEX_LOCK(qtask->mutex);
        if(qtask->state->qtasks > 0 && qtask->state->tasks[taskid].status > 0)
        {
            qtask->state->tasks[taskid].workers += num;
            ret = qtask->state->tasks[taskid].workers;
        }
        MUTEX_UNLOCK(qtask->mutex);
    }
    return ret;
}

/* gen packetid */
int qtask_gen_packet(QTASK *qtask, char *packet, int packet_len)
{
    int packetid = 0, ret = 0;

    if(qtask && qtask->state && packet && packet_len > 0)
    {
        MUTEX_LOCK(qtask->mutex);
        if(qtask->old && qtask->state->old > 0) 
        {
            if(mmqueue_total(qtask->old, qtask->state->old) > 0)
                mmqueue_pop(qtask->old, qtask->state->old, &packetid);
            if(packetid <= 0 || packetid > qtask->state->id_max) 
            {
                if(packetid > qtask->state->id_max)
                {
                    WARN_LOGGER(qtask->logger, "Invalid old packetid:%d max:%d", packetid, qtask->state->id_max);
                }
                packetid = ++(qtask->state->id_max);
            }
        }
        else
        {
            packetid = ++(qtask->state->id_max);
        }
        MUTEX_UNLOCK(qtask->mutex);
        ret = db_set_data(PDB(qtask->db), packetid, packet, packet_len);
    }
    return ret;
}

/* remove task */
int qtask_remove_packet(QTASK *qtask, int packetid)
{
    int ret = -1;

    if(qtask && packetid > 0)
    {
        if(qtask->state->old > 0)
        {
            if(packetid > qtask->state->id_max)
            {
                WARN_LOGGER(qtask->logger, "Invalid packetid:%d max:%d", packetid, qtask->state->id_max);
            }
            else 
                mmqueue_push(qtask->old, qtask->state->old, packetid);
            ret = 0;
        }
        db_del_data(PDB(qtask->db), packetid);
    }
    return ret;
}

/* push task */
int qtask_push_packet(QTASK *qtask, int taskid, int flag, int packetid)
{
    int ret = -1;

    if(qtask && qtask->state && taskid > 0 && taskid < QT_TASKS_MAX && packetid > 0)
    {
        //fprintf(stdout, "%s::%d qhead:%d queue:%d\n", __FILE__, __LINE__, qtask->state->tasks[taskid].status, qtask->state->tasks[taskid].qhead, qtask->state->tasks[taskid].queue);
        if(qtask->state->tasks[taskid].status > 0 
                && qtask->state->tasks[taskid].qhead > 0 
                && qtask->state->tasks[taskid].queue > 0)
        {
            if(flag & MTASK_TO_QHEAD)
            {
                mmqueue_push(qtask->qhead, qtask->state->tasks[taskid].qhead, packetid);
            }
            else
            {
                mmqueue_push(qtask->queue, qtask->state->tasks[taskid].queue, packetid);
            }
            qtask->state->tasks[taskid].total++;
            ret = packetid;
        }
    }
    return ret;
}

/* pop task */
int qtask_pop_packet(QTASK *qtask, int taskid, int *packet_len)
{
    int ret = -1, packetid = 0;

    if(qtask && qtask->state && taskid > 0 && taskid < QT_TASKS_MAX && packet_len)
    {
        MUTEX_LOCK(qtask->mutex);
        if(qtask->state->tasks[taskid].status > 0 
                && qtask->state->tasks[taskid].wait > 0 
                && qtask->state->tasks[taskid].qhead > 0 
                && qtask->state->tasks[taskid].queue > 0)
        {
            if(mmqueue_total(qtask->qhead, qtask->state->tasks[taskid].qhead) > 0)
            {
                mmqueue_pop(qtask->qhead, qtask->state->tasks[taskid].qhead, &packetid);
            }
            else
            {
                mmqueue_pop(qtask->queue, qtask->state->tasks[taskid].queue, &packetid);
            }
        }
        MUTEX_UNLOCK(qtask->mutex);
        if(packetid > 0 && (*packet_len = db_get_data_len(PDB(qtask->db), packetid)) > 0)
        {
            mmtree_try_insert(qtask->wait, qtask->state->tasks[taskid].wait, packetid, 1, NULL);
            //fprintf(stdout, "%s::%d taskid:%d wait:%d\n", __FILE__, __LINE__, taskid, mmtree_total(qtask->wait, qtask->state->tasks[taskid].wait));
            ret = packetid;
        }
    }
    return ret;
}

/* read packet */
int qtask_read_packet(QTASK *qtask, int taskid, int packetid, char *packet)
{
    int ret = -1;

    if(packetid > 0)
    {
        ret = db_read_data(PDB(qtask->db), packetid, packet);
    }
    return ret;
}

/* over task */
int qtask_over_packet(QTASK *qtask, int taskid, int packetid)
{
    int ret = -1, id = 0;

    if(qtask && qtask->state && taskid > 0 && taskid < QT_TASKS_MAX && packetid > 0)
    {
        if(qtask->state->tasks[taskid].wait > 0 && (id = mmtree_find(qtask->wait, 
                        qtask->state->tasks[taskid].wait, packetid, NULL)) > 0)
        {
            mmtree_remove(qtask->wait, qtask->state->tasks[taskid].wait, id, NULL, NULL);
            qtask->state->tasks[taskid].over++;
            ret = 0;
        }
    }
    return ret;
}

/* retask */
int qtask_repacket(QTASK *qtask, int taskid, int packetid)
{
    int id = 0, ret = -1;

    if(qtask && qtask->state && taskid > 0 && taskid < QT_TASKS_MAX && packetid > 0)
    {
        if(qtask->state->tasks[taskid].wait > 0 && (id = mmtree_find(qtask->wait, 
                        qtask->state->tasks[taskid].wait, packetid, NULL)) > 0)
        {
            mmtree_remove(qtask->wait, qtask->state->tasks[taskid].wait, id, NULL, NULL);
            mmqueue_push(qtask->qhead, qtask->state->tasks[taskid].qhead, packetid);
            ret = 0;
        }
    }
    return ret;
}

/* clean qtask */
void qtask_clean(QTASK *qtask)
{
    if(qtask)
    {
        /* close state */
        if(qtask->stateio.map) munmap(qtask->stateio.map, sizeof(QTSTATE));
        if(qtask->stateio.fd > 0) close(qtask->stateio.fd);
        qtask->state = NULL;
        mmtrie_clean(qtask->map);
        mmtree_close(qtask->wait);
        mmqueue_clean(qtask->qhead);
        mmqueue_clean(qtask->queue);
        mmqueue_clean(qtask->old);
        db_clean(PDB(qtask->db));
        LOGGER_CLEAN(qtask->logger);
        MUTEX_DESTROY(qtask->mutex);
        free(qtask);
    }
    return ;
}

#ifdef _DEBUG_QTASK
int main()
{
    char *name = "saddd", *basedir = "/tmp/qtask", *end = NULL, buf[8192];
    int n = 0, nout = 0, taskid = 0, flag = 0, i = 0, oldtask = 0;
    int64_t data_id = 0, olddata = 0;
    QTASK *qtask = NULL;
    
    if((qtask = qtask_init(basedir)))
    {
        if(qtask_add_task(qtask, "task1") > 0 && qtask_list_tasks(qtask, buf, buf+8192) > 0)
        {
            fprintf(stdout, "add task1:%s\n", buf);
        }
        if(qtask_add_task(qtask, "task2") > 0 && qtask_list_tasks(qtask, buf, buf+8192) > 0)
        {
            fprintf(stdout, "add task2:%s\n", buf);
        }
        if(qtask_rename_task(qtask, 2, "task02") > 0 && qtask_list_tasks(qtask, buf, buf+8192) > 0)
        {
            fprintf(stdout, "rename task2 to task02:%s\n", buf);
        }
        if(qtask_add_task(qtask, "task03") > 0 && qtask_list_tasks(qtask, buf, buf+8192) > 0)
        {
            fprintf(stdout, "add task03:%s\n", buf);
        }   
        if(qtask_del_task(qtask, 1) > 0 && qtask_list_tasks(qtask, buf, buf+8192) > 0)
        {
            fprintf(stdout, "del task1:%s\n", buf);
        }
        if(qtask_add_task(qtask, "task01") > 0 && qtask_list_tasks(qtask, buf, buf+8192) > 0)
        {
            fprintf(stdout, "add task01:%s\n", buf);
        } 
        //push tasks 
        for(i = 1; i < 10000; i++)
        {
            taskid = (random()%3) + 1;
            //if(taskid == 1)fprintf(stdout, "taskid:%d\n", taskid);
            flag = 0;
            if(random()%33 == 0) flag = QT_TASK_TOP;
            qtask_push_task(qtask, taskid, flag, (int64_t)i);
        }
        /* list tasks */
        if(qtask_list_tasks(qtask, buf, buf+8192) > 0)
        {
            fprintf(stdout, "push_task(10000):%s\n", buf);
        } 
        for(i = 0; i < 1000; i++)
        {
            taskid = (random()%3) + 1;
            if((data_id = qtask_pop_task(qtask, taskid)) > 0)
            {
                if(i == 333) {oldtask = taskid; olddata = data_id;}
                else qtask_over_task(qtask, taskid, data_id);
            }
        }
        /* list tasks */
        if(qtask_list_tasks(qtask, buf, buf+8192) > 0)
        {
            fprintf(stdout, "pop_task(1000):%s\n", buf);
        } 
        /* retask */
        if(qtask_retask(qtask, oldtask, olddata) == 0)
        {
            if(qtask_list_tasks(qtask, buf, buf+8192) > 0)
            {
                fprintf(stdout, "retask(%d, %lld):%s\n",oldtask, (long long)olddata, buf);
            } 
        }
        qtask_clean(qtask);
    }
    return 0;
}
//gcc -o qtask qtask.c utils/*.c -I utils -D_DEBUG_QTASK && rm -rf /tmp/qtask; ./qtask
#endif
