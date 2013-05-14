#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>
#include <netdb.h>
#include <ibase.h>
#include "stime.h"
#include "xtask.h"
#include "xmm.h"
#include "mutex.h"
#include "mmtrie.h"
#include "vmap.h"
#include "qmap.h"
#include "kmap.h"
#include "mmqueue.h"
#include "logger.h"
#include "mmtrie.h"
#include "base64.h"
#include "http.h"
#include "md5.h"
#include "db.h"
#define _XOPEN_SOURCE 
#define XTASK_MMDB_DIR      "mmdb" 
#define XTASK_DB_DIR        "db" 
#define XTASK_RES_DIR       "res" 
#define XTASK_TEMP_DIR      "temp" 
#define XTASK_URLMAP_DIR    "urlmap"
#define XTASK_MAP_DIR       "map"
#define XTASK_QMAP_DIR      "qmap"
#define XTASK_QUEUE_NAME    "xtask.queue"
#define XTASK_STATE_NAME    "xtask.state"
#define XTASK_LOG_NAME      "xtask.log"
#define XTASK_KMAP_NAME     "xtask.kmap"
#define XTASK_PROXY_NAME    "xtask.proxy"
#define XTASK_DNS_NAME      "xtask.dns"
#define XTASK_HOST_NAME     "xtask.host"
#define XTASK_URL_NAME      "xtask.url"
#define XTASK_TABLE_NAME    "xtask.table"
#define XTASK_NODE_NAME     "xtask.node"
#define XTASK_MAP_NAME      "xtask.map"
#define XTASK_TEMPLATE_NAME "xtask.template"
#define XTASK_URLNODE_NAME  "xtask.urlnode"
#define LL64(xxxxx) ((long long int)xxxxx)
int xtask_check_parent(XTASK *xtask, int urlnodeid, int nodeid);
int xtask_set_brother(XTASK *xtask, int urlnodeid, int brother);

/* mkdir force */
int xtask_pmkdir(char *path)
{
    char fullpath[XT_PATH_MAX];
    int level = -1, ret = -1;
    struct stat st = {0};
    char *p = NULL;

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
#define XTIO_INCRE(xtask, name, NAME)                                               \
do                                                                                  \
{                                                                                   \
    if(xtask->name##io.map && xtask->name##io.size > xtask->name##io.end)           \
    {                                                                               \
        xtask->name##io.old = xtask->name##io.end;                                  \
        xtask->name##io.end += (off_t)XT_##NAME##_BASE * (off_t)sizeof(XT##NAME);   \
        if(ftruncate(xtask->name##io.fd, xtask->name##io.end)!= 0)                  \
        {                                                                           \
            ERROR_LOGGER(xtask->logger, "truncate  failed, %s", strerror(errno));   \
            break;                                                                  \
        }                                                                           \
        memset((char *)xtask->name##io.map + xtask->name##io.old, 0,                \
                xtask->name##io.end - xtask->name##io.old);                         \
    }                                                                               \
}while(0)
#ifdef HAVE_PTHREAD
#define MM_MUTEX_LOCK(task, id)  pthread_mutex_lock(&(task->mutexs[(id)%XT_MUTEX_MAX]))
#define MM_MUTEX_UNLOCK(task, id)  pthread_mutex_unlock(&(task->mutexs[(id)%XT_MUTEX_MAX]))
#define MM_HMUTEX_LOCK(task, id)  pthread_mutex_lock(&(task->hmutexs[(id)%XT_MUTEX_MAX]))
#define MM_HMUTEX_UNLOCK(task, id)  pthread_mutex_unlock(&(task->hmutexs[(id)%XT_MUTEX_MAX]))
#else
#define MM_MUTEX_LOCK(task, id)
#define MM_MUTEX_UNLOCK(task, id)
#define MM_HMUTEX_LOCK(task, id)
#define MM_HMUTEX_UNLOCK(task, id)
#endif

/* add table */
int xtask_add_table(XTASK *xtask, char *name)
{
    int ret = -1, i = 0, x = 0, n = 0, id = 0;
    XTTABLE *tablelist  = NULL;
    char line[XT_LINE_MAX];

    if(xtask && name && xtask->state && xtask->kmap 
            && (tablelist = (XTTABLE *)(xtask->tableio.map)) 
            && (n = sprintf(line, "t:%s", name)) > 0)
    {
        MUTEX_LOCK(xtask->table_mutex);
        if(xtask->state->table_id_left < 1)
        {
            XTIO_INCRE(xtask, table, TABLE);
            x = (xtask->tableio.end / sizeof(XTTABLE)) - 1;
            i = xtask->tableio.old / sizeof(XTTABLE);
            if(i == 0) i = 1;
            while(x >= i)
            {
                tablelist[x].bits = xtask->state->table_id_left;
                xtask->state->table_id_left = x;
                --x;
            }
        }
        id = xtask->state->table_id_left;
        if((ret = mmtrie_add(xtask->kmap, line, n, id)) == id)
        {
            xtask->state->table_id_left = tablelist[id].bits;
            strcpy(tablelist[id].name, name);
            tablelist[id].status = XT_STATUS_OK;
            xtask->state->table_total++;
        }
        if(id > xtask->state->table_id_max) xtask->state->table_id_max = id;
        MUTEX_UNLOCK(xtask->table_mutex);
    }
    return ret;
}

/* add field */
int xtask_add_field(XTASK *xtask, int tableid, int flag, char *name)
{
    int ret = -1, i = 0, x = -1, id = 0, n = 0, to = 0;
    XTTABLE *tablelist = NULL;
    char line[XT_LINE_MAX];

    if(xtask && xtask->state && tableid > 0 && flag && name
            && (tablelist = (XTTABLE *)(xtask->tableio.map))
                && (n = sprintf(line, "f:%s", name)))
    {
        MUTEX_LOCK(xtask->table_mutex); 
        if(tableid <= xtask->state->table_id_max)
        {
            x = xtask->state->field_id_max+1;
            if((id = mmtrie_add(xtask->kmap, line, n, x)) == x)
                xtask->state->field_id_max++;
            if(flag & IB_IS_NEED_INDEX) 
            {
                if(flag & IB_DATATYPE_TEXT) i = XT_TEXT_FROM;
                else if(flag & IB_DATATYPE_INT) i = XT_INT_FROM;
                else if(flag & IB_DATATYPE_LONG) i = XT_LONG_FROM;
                else if(flag & IB_DATATYPE_DOUBLE) i = XT_DOUBLE_FROM;
                to = i + XT_INDEX_MAX;
            }
            else
            {
                if(tablelist[tableid].display_total < XT_DISPLAY_MAX) 
                {
                    i = XT_DISPLAY_FROM;
                    to = i + XT_DISPLAY_MAX;
                }
            }
            if(i >= 0)
            {
                x = -1;
                while(i < to)
                {
                    if(tablelist[tableid].fields[i].id == id) break;
                    if(tablelist[tableid].fields[i].id == 0)
                    {
                        x = i;
                        break;
                    }
                    ++i;
                }
                if(x >= 0)
                {
                    strcpy(tablelist[tableid].fields[x].name, name);
                    tablelist[tableid].fields[x].id = id;
                    tablelist[tableid].fields[x].flag = flag;
                    if(flag & IB_IS_NEED_INDEX)
                    {
                        if(flag & IB_DATATYPE_INT) tablelist[tableid].int_index_total++;
                        else if(flag & IB_DATATYPE_LONG) tablelist[tableid].long_index_total++;
                        else if(flag & IB_DATATYPE_DOUBLE) tablelist[tableid].double_index_total++;
                        else if(flag & IB_DATATYPE_TEXT) tablelist[tableid].text_index_total++;
                        else tablelist[tableid].display_total++;
                    }
                    else tablelist[tableid].display_total++;
                    tablelist[tableid].nfields++;
                    ret = id;
                }
            }
        }
        MUTEX_UNLOCK(xtask->table_mutex); 
    }
    return ret;
}

/* rename field */
int xtask_rename_field(XTASK *xtask, int tableid, int fieldid, char *name)
{
    int ret = -1, i = 0, x = -1, id = 0, flag = 0, n = 0, to = 0;
    XTTABLE *tablelist = NULL;
    char line[XT_LINE_MAX];
    XTFIELD *fields = NULL;

    if(xtask && xtask->state && tableid > 0 && fieldid >= 0 && name
            && (tablelist = (XTTABLE *)(xtask->tableio.map))
                && (n = sprintf(line, "f:%s", name)))
    {
        MUTEX_LOCK(xtask->table_mutex); 
        if(tableid <= xtask->state->table_id_max 
                && fieldid >= 0 && fieldid < XT_FIELDS_MAX
                && (fields = tablelist[tableid].fields)
                && (flag = fields[fieldid].flag))
        {
            x = xtask->state->field_id_max+1;
            if((id = mmtrie_add(xtask->kmap, line, n, x)) == x)
                xtask->state->field_id_max++;
            if(flag & IB_IS_NEED_INDEX)
            {
                if(flag & IB_DATATYPE_TEXT) 
                {
                    i = XT_TEXT_FROM;
                    to = XT_TEXT_FROM + tablelist[tableid].text_index_total;
                }
                else if(flag & IB_DATATYPE_INT) 
                {
                    i = XT_INT_FROM;
                    to = XT_INT_FROM + tablelist[tableid].int_index_total;
                }
                else if(flag & IB_DATATYPE_LONG) 
                {
                    i = XT_LONG_FROM;
                    to = XT_LONG_FROM + tablelist[tableid].long_index_total;
                }
                else if(flag & IB_DATATYPE_DOUBLE) 
                {
                    i = XT_DOUBLE_FROM;
                    to = XT_DOUBLE_FROM + tablelist[tableid].double_index_total;
                }
                else 
                {
                    i = XT_DISPLAY_FROM;
                    to = XT_DISPLAY_FROM + tablelist[tableid].display_total;
                }
            }
            else 
            {
                i = XT_DISPLAY_FROM;
                to = XT_DISPLAY_FROM + tablelist[tableid].display_total;
            }
            if(i >= 0)
            {
                x = -1;
                while(i < to)
                {
                    if(tablelist[tableid].fields[i].id == id) 
                    {
                        x = i;
                        break;
                    }
                    ++i;
                }
                if(x == -1)
                {
                    strcpy(fields[fieldid].name, name);
                    fields[fieldid].id = id;
                    ret = fieldid;
                }
            }
        }
        MUTEX_UNLOCK(xtask->table_mutex); 
    }
    return ret;
}

/* delete field */
int xtask_delete_field(XTASK *xtask, int tableid, int fieldid)
{
    int ret = -1, flag = 0, x = 0, to = 0;
    XTTABLE *tablelist  = NULL;
    XTFIELD *fields = NULL;

    if(xtask && tableid > 0 && fieldid >= 0 && xtask->state 
            && (tablelist = (XTTABLE *)(xtask->tableio.map))) 
    {
        MUTEX_LOCK(xtask->table_mutex);
        if(tableid <= xtask->state->table_id_max 
                && tablelist[tableid].status > 0 
                && (fields = tablelist[tableid].fields)
                && (flag = fields[fieldid].flag) > 0)
        {
            if(flag & IB_IS_NEED_INDEX)
            {
                if(flag & IB_DATATYPE_TEXT) 
                    to = XT_TEXT_FROM + tablelist[tableid].text_index_total--;
                else if(flag & IB_DATATYPE_INT) 
                    to = XT_INT_FROM + tablelist[tableid].int_index_total--;
                else if(flag & IB_DATATYPE_LONG) 
                    to = XT_LONG_FROM + tablelist[tableid].long_index_total--;
                else if(flag & IB_DATATYPE_DOUBLE) 
                    to = XT_DOUBLE_FROM + tablelist[tableid].double_index_total--;
                else 
                    to = XT_DISPLAY_FROM + tablelist[tableid].display_total--;
            }
            else 
                to = XT_DISPLAY_FROM + tablelist[tableid].display_total--;
            tablelist[tableid].nfields--;
            x = fieldid;
            while(x < (to-1)) 
            {
                memcpy(&(fields[x]), &(fields[x+1]), sizeof(XTFIELD));
                ++x;
            }
            memset(&(fields[x]), 0, sizeof(XTFIELD));
            ret = fieldid;
        }
        MUTEX_UNLOCK(xtask->table_mutex);
    }
    return ret;
}

/* rename table */
int xtask_rename_table(XTASK *xtask, int id, char *name)
{
    XTTABLE *tablelist  = NULL;
    char line[XT_LINE_MAX];
    int ret = -1, n = 0;

    if(xtask && id > 0 && xtask->kmap && xtask->state 
            && (tablelist = (XTTABLE *)(xtask->tableio.map))
            && (n = sprintf(line, "t:%s", name)) > 0)
    {
        MUTEX_LOCK(xtask->table_mutex);
        if(mmtrie_add(xtask->kmap, line, n, id) == id 
                && id <= xtask->state->table_id_max 
                && tablelist[id].status > 0)
        {
            n = sprintf(line, "t:%s", tablelist[id].name);
            mmtrie_del(xtask->kmap, line, n);
            strcpy(tablelist[id].name, name);
            ret = id;
        }
        else
        {
            mmtrie_del(xtask->kmap, line, n);
        }
        MUTEX_UNLOCK(xtask->table_mutex);
    }
    return ret;
}

/* delete table */
int xtask_delete_table(XTASK *xtask, int id)
{
    XTTABLE *tablelist  = NULL;
    char line[XT_LINE_MAX];
    int ret = -1, n = 0;

    if(xtask && id > 0 && xtask->kmap && xtask->state 
            && (tablelist = (XTTABLE *)(xtask->tableio.map))) 
    {
        MUTEX_LOCK(xtask->table_mutex);
        if(id <= xtask->state->table_id_max 
            && tablelist[id].status > 0)
        {
            n = sprintf(line, "t:%s", tablelist[id].name);
            mmtrie_del(xtask->kmap, line, n);
            memset(&(tablelist[id]), 0, sizeof(XTTABLE));
            tablelist[id].bits = xtask->state->table_id_left;
            xtask->state->table_id_left = id;
            xtask->state->table_total--;
        }
        MUTEX_UNLOCK(xtask->table_mutex);
        ret = id;
    }
    return ret;
}

/* get table */
int xtask_get_table(XTASK *xtask, int id, XTTABLE *table)
{
    XTTABLE *tablelist  = NULL;
    int ret = -1;

    if(xtask && id > 0 && table && xtask->kmap && xtask->state 
            && (tablelist = (XTTABLE *)(xtask->tableio.map))) 
    {
        MUTEX_LOCK(xtask->table_mutex);
        if(id <= xtask->state->table_id_max 
            && tablelist[id].status > 0)
        {
            memcpy(table, &(tablelist[id]), sizeof(XTTABLE));
            ret = id;
        }
        MUTEX_UNLOCK(xtask->table_mutex);
    }
    return ret;
}

/* read all tables */
int xtask_read_tables(XTASK *xtask, char *out, int nout)
{
    XTTABLE *tablelist  = NULL;
    int ret = 0;

    if(xtask && out && (tablelist = (XTTABLE *)(xtask->tableio.map))) 
    {
        MUTEX_LOCK(xtask->table_mutex);
        if(xtask->state->table_id_max > 0 
                && nout > sizeof(XTTABLE) * xtask->state->table_id_max)
        {
            memcpy(out, tablelist, sizeof(XTTABLE) * (xtask->state->table_id_max+1));
            ret = xtask->state->table_id_max+1;
        }
        MUTEX_UNLOCK(xtask->table_mutex);
    }
    return ret;
}

/* view database */
int xtask_list_tables(XTASK *xtask, char *block, int nblock)
{
    char *p = NULL, *end = NULL;
    int ret = -1, i = 0, x = 0;
    XTTABLE *tablelist  = NULL;

    if(xtask && (p = block) && (end = (p + nblock)) > p 
            && (tablelist = (XTTABLE *)(xtask->tableio.map)) 
            && (x = xtask->state->table_total) > 0)
    {
        MUTEX_LOCK(xtask->table_mutex); 
        p += sprintf(p, "({\"tables\":{");  
        for(i = 1; i <= xtask->state->table_id_max; i++)
        {
            if(tablelist[i].status)
            {
                p += sprintf(p, "\"%d\":{\"name\":\"%s\", \"nfields\":\"%d\"},", 
                        i, tablelist[i].name, tablelist[i].nfields);
            }
        }
        if(*(p-1) == ',') --p;
        p += sprintf(p, "}})");
        MUTEX_UNLOCK(xtask->table_mutex); 
        ret = p - block;
    }
    return ret;
}

/* view table */
int xtask_view_table(XTASK *xtask, int id, char *block, int nblock)
{
    char *p = NULL, *end = NULL;
    int ret = -1, k = 0, x = 0;
    XTTABLE *tablelist  = NULL;
    XTFIELD *fields = NULL;

    if(xtask && (p = block) && (end = (p + nblock)) > p 
            && (tablelist = (XTTABLE *)(xtask->tableio.map)) 
            && (x = xtask->state->table_total) > 0)
    {
        MUTEX_LOCK(xtask->table_mutex); 
        if(id > 0 && id <= xtask->state->table_id_max 
                && tablelist[id].status && (fields = tablelist[id].fields))
        {
            p += sprintf(p, "({\"id\":\"%d\", \"name\":\"%s\",\"nfields\":\"%d\", \"fields\":{", 
                    id, tablelist[id].name, tablelist[id].nfields);
            if(tablelist[id].nfields > 0)
            {
                for(k = 0; k < XT_FIELDS_MAX; k++)
                {
                    if(fields[k].id > 0)
                    {
                        p += sprintf(p, "\"%d\":{\"id\":\"%d\", \"name\":\"%s\", \"flag\":\"%d\"},", k, k,  fields[k].name, fields[k].flag);
                    }
                }
            } 
            if(*(p-1) == ',') --p;
            p += sprintf(p, "}})");
            ret = p - block;
        }
        MUTEX_UNLOCK(xtask->table_mutex); 
    }
    return ret;
}
/* view database */
int xtask_view_database(XTASK *xtask, char *block, int nblock)
{
    char *p = NULL, *end = NULL;
    int ret = -1, i = 0, k = 0;
    XTTABLE *tablelist  = NULL;
    XTFIELD *fields = NULL;

    if(xtask && (p = block) && (end = (p + nblock)) > p 
            && (tablelist = (XTTABLE *)(xtask->tableio.map))
            && (xtask->state->table_total) > 0)
    {
        MUTEX_LOCK(xtask->table_mutex); 
        p += sprintf(p, "({\"tables\":{");  
        for(i = 1; i <= xtask->state->table_id_max; i++)
        {
            if(tablelist[i].status > 0 && (fields = tablelist[i].fields))
            {
                p += sprintf(p, "\"%d\":{\"id\":\"%d\", \"name\":\"%s\", \"nfields\":\"%d\", \"fields\":{", i, i, tablelist[i].name, tablelist[i].nfields);
                if(tablelist[i].nfields > 0)
                {
                    for(k = 0; k < XT_FIELDS_MAX; k++)
                    {
                        if(fields[k].id > 0)
                        {
                            p += sprintf(p, "\"%d\":{\"id\":\"%d\", \"name\":\"%s\", \"flag\":\"%d\"},", k, k,  fields[k].name, fields[k].flag);
                        }
                    }
                    p += sprintf(p, "},");
                } 
                --p;
                p += sprintf(p, "},");
            }
        }
        if(*(p-1) == ',') --p;
        p += sprintf(p, "}})");
        ret = p - block;
        MUTEX_UNLOCK(xtask->table_mutex); 
    }
    return ret;
}
/* add node */
int xtask_add_node(XTASK *xtask, int parent, char *name)
{
    int ret = -1, uid = 0, i = 0, k = 0, x = 0, n = 0, id = 0, old = 0;
    XTNODE *nodelist  = NULL;
    char line[XT_LINE_MAX];

    if(xtask && parent >= 0 && name && xtask->state && xtask->kmap 
            && (nodelist = (XTNODE *)(xtask->nodeio.map)) 
            && (n = sprintf(line, "n:%s", name)) > 0)
    {
        DEBUG_LOGGER(xtask->logger, "%d => %s", parent, name);
        MUTEX_LOCK(xtask->node_mutex);
        if(parent <= xtask->state->node_id_max)
        {
            k = xtask->state->node_uid_max+1;
            if((uid = mmtrie_add(xtask->kmap, line, n, k)) == k)
                xtask->state->node_uid_max++;
            if(xtask->state->node_id_left < 1)
            {
                XTIO_INCRE(xtask, node, NODE);
                x = (xtask->nodeio.end / sizeof(XTNODE)) - 1;
                i = xtask->nodeio.old / sizeof(XTNODE);
                if(i == 0) i = 1;
                while(x >= i)
                {
                    nodelist[x].uid = xtask->state->node_id_left;
                    xtask->state->node_id_left = x;
                    --x;
                }
            }
            id = xtask->state->node_id_left;
            if(nodelist[parent].childs_root == 0)
                nodelist[parent].childs_root = vmap_new_tree(xtask->map);
            n = vmap_try_insert(xtask->map, nodelist[parent].childs_root, uid, id, &old); 
            if(old == 0)
            {
                xtask->state->node_id_left = nodelist[id].uid;
                nodelist[parent].nchilds++;
                nodelist[parent].status = XT_STATUS_OK;
                nodelist[id].id = id;
                nodelist[id].uid = uid;
                nodelist[id].mapid = n;
                nodelist[id].parent = parent;
                nodelist[id].level = nodelist[parent].level + 1;
                strcpy(nodelist[id].name, name);
                nodelist[id].status = XT_STATUS_OK;
                xtask->state->node_total++;
                if(id > xtask->state->node_id_max) xtask->state->node_id_max = id;
                ret = id;
                DEBUG_LOGGER(xtask->logger, "%d:%s/%s", id, nodelist[id].name, name);
            }
        }
        MUTEX_UNLOCK(xtask->node_mutex);
    }
    return ret;
}

/* update node */
int xtask_update_node(XTASK *xtask, int parent, int nodeid, char *name)
{
    int ret = -1, uid = 0, k = 0, n = 0, old = 0;
    XTNODE *nodelist  = NULL;
    char line[XT_LINE_MAX];

    if(xtask && parent >= 0 && nodeid > 0 && name && xtask->state && xtask->kmap 
            && (nodelist = (XTNODE *)(xtask->nodeio.map)) 
            && (n = sprintf(line, "n:%s", name)) > 0)
    {
        MUTEX_LOCK(xtask->node_mutex);
        if(parent <= xtask->state->node_id_max && nodeid <= xtask->state->node_id_max)
        {
            k = xtask->state->node_uid_max+1;
            if((uid = mmtrie_add(xtask->kmap, line, n, k)) == k)
                xtask->state->node_uid_max++;
            n = vmap_try_insert(xtask->map, nodelist[parent].childs_root, uid, nodeid, &old); 
            if(old == 0)
            {
                vmap_remove(xtask->map, nodelist[parent].childs_root, nodelist[nodeid].mapid, &k, &old);
                nodelist[parent].status = XT_STATUS_OK;
                nodelist[nodeid].uid = uid;
                nodelist[nodeid].mapid = n;
                strcpy(nodelist[nodeid].name, name);
                nodelist[nodeid].status = XT_STATUS_OK;
                ret = nodeid;
            }
        }
        MUTEX_UNLOCK(xtask->node_mutex);
    }
    return ret;
}

/* add template */
int xtask_add_template(XTASK *xtask, int nodeid, ITEMPLATE *template)
{
    XTNODE *nodelist = NULL;
    int ret = -1, id = -1;

    if(xtask && nodeid > 0 && template && (nodelist = (XTNODE *)(xtask->nodeio.map)))
    {
        MUTEX_LOCK(xtask->node_mutex);
        if(nodeid <= xtask->state->node_id_max && nodelist[nodeid].status > 0)
        {
           id = ++xtask->state->template_id_max;
           if(nodelist[nodeid].templates_root == 0) 
               nodelist[nodeid].templates_root = vmap_new_tree(xtask->map);
           template->mmid = vmap_try_insert(xtask->map, 
                   nodelist[nodeid].templates_root, id, nodeid, NULL);
           template->nodeid = nodeid;
           if((ret = db_set_data(PDB(xtask->temp), id, (char *)template, sizeof(ITEMPLATE))) > 0)
            {
                nodelist[nodeid].ntemplates++;
            }
        }
        MUTEX_UNLOCK(xtask->node_mutex);
    }
    return ret;
}

/* get template */
int xtask_get_template(XTASK *xtask, int templateid, ITEMPLATE *template)
{
    int ret = -1;

    if(xtask && templateid > 0 && template)
    {
        MUTEX_LOCK(xtask->node_mutex);
        if(templateid <= xtask->state->template_id_max 
                && db_read_data(PDB(xtask->temp), templateid, (char *)template) > 0)
        {
            ret = templateid;
        }
        MUTEX_UNLOCK(xtask->node_mutex);
    }
    return ret;
}

/* read all templates */
int xtask_read_templates(XTASK *xtask, char *out, int nout)
{
    ITEMPLATE *template  = NULL;
    int ret = 0, i = 0;

    if(xtask && (template = (ITEMPLATE *)out))
    {
        MUTEX_LOCK(xtask->node_mutex);
        if(xtask->state->template_id_max > 0 
                && nout > (sizeof(ITEMPLATE) * xtask->state->template_id_max+1))
        {
            for(i = 0; i <= xtask->state->template_id_max; i++)
            {
                if(db_read_data(PDB(xtask->temp), i, (char *)template) < 0)
                    memset(template, 0, sizeof(ITEMPLATE));
                ++template;
            }
            ret = (xtask->state->template_id_max+1);
        }
        MUTEX_UNLOCK(xtask->node_mutex);
    }
    return ret;
}

/* delete template */
int xtask_delete_template(XTASK *xtask, int nodeid, int templateid)
{
    XTNODE *nodelist = NULL;
    int ret = -1, mid = 0;

    if(xtask && templateid > 0 && nodeid > 0 
            && (nodelist = (XTNODE *)(xtask->nodeio.map)))
    {
        MUTEX_LOCK(xtask->node_mutex);
        if(nodeid <= xtask->state->node_id_max 
            && templateid <= xtask->state->template_id_max)
        {
            if((mid = vmap_find(xtask->map, nodelist[nodeid].templates_root, templateid, NULL)) > 0)
                vmap_remove(xtask->map, nodelist[nodeid].templates_root, mid, NULL,NULL);
            ret = db_del_data(PDB(xtask->temp), templateid);
            nodelist[nodeid].ntemplates--;
        }
        MUTEX_UNLOCK(xtask->node_mutex);
    }
    return ret;
}

/* update template */
int xtask_update_template(XTASK *xtask, int templateid, ITEMPLATE *template)
{
    int ret = -1;

    if(xtask && templateid > 0 && template)
    {
        MUTEX_LOCK(xtask->node_mutex);
        if(templateid <= xtask->state->template_id_max)
        {
            ret = db_set_data(PDB(xtask->temp), templateid, (char *)template, sizeof(ITEMPLATE));
        }
        MUTEX_UNLOCK(xtask->node_mutex);
    }
    return ret;
}

/* view templates */
int xtask_view_templates(XTASK *xtask, int nodeid, char *out, int nout)
{
    int ret = -1,  i = 0, k = 0, x = 0, n = 0, id = 0;
    char *p = NULL, *end = NULL, pattern[XT_PATTERN_SIZE];
    ITEMPLATE template = {0};
    XTNODE *nodelist = NULL;

    if(xtask && nodeid > 0 &&  (p = out)  && (end = (out + nout)) > out 
            && (nodelist = (XTNODE *)(xtask->nodeio.map))) 
    {
        MUTEX_LOCK(xtask->node_mutex);
        if(nodeid <= xtask->state->node_id_max && xtask->state->node_total > 0)
        {
            p += sprintf(p, "({'id':'%d', 'name':'%s','ntemplates':'%d', 'templates':[",
                    nodeid,  nodelist[nodeid].name, nodelist[nodeid].ntemplates);
            if(nodelist[nodeid].templates_root  > 0 && (x = vmap_min(xtask->map, 
                            nodelist[nodeid].templates_root, &id, &k)) > 0)
            {
                do
                {
                    if(db_read_data(PDB(xtask->temp), id, (char *)&template) > 0)
                    {
                        if((n = strlen(template.pattern)) > 0)
                        {
                            base64_encode(pattern, (const unsigned char *)(template.pattern), n);
                        }
                        else 
                            pattern[0] = 0; 
                        p += sprintf(p, "{'id':'%d', 'tableid':'%d', 'flags':'%d', "
                                "'pattern':'%s', 'link':'%s',", id, template.tableid,
                                template.flags, pattern, template.link);
                        {
                            p += sprintf(p, "'linkmap':{'fieldid':'%d','nodeid':'%d', 'flag':'%d'},", 
                                    template.linkmap.fieldid, template.linkmap.nodeid,
                                    template.linkmap.flag);
                        }
                        p += sprintf(p, "'url':'%s', 'nfields':'%d', 'map':[",
                                template.url, template.nfields);
                        if(template.nfields > 0)
                        {
                            i = 0;
                            while(i < template.nfields && template.nfields < XT_FIELDS_MAX)
                            {
                                p += sprintf(p, "{'fieldid':'%d', 'nodeid':'%d', 'flag':'%d'},",
                                        template.map[i].fieldid, template.map[i].nodeid,
                                        template.map[i].flag);
                                i++;
                            }
                            --p;
                        }
                        p += sprintf(p, "%s","]},");

                    }
                }while((x = vmap_next(xtask->map, 
                                nodelist[nodeid].templates_root, x, &id, &k)) > 0);
                --p;
            }
            if(*(p-1) == ',') --p;
            p += sprintf(p, "]})");
            ret = p - out;
        }
        MUTEX_UNLOCK(xtask->node_mutex);
    }
    return ret;
}

/* get node templates */
int xtask_get_node_templates(XTASK *xtask, int nodeid, char *out, int nout)
{
    int ret = -1, k = 0, x = 0, n = 0, id = 0, left = nout;
    ITEMPLATE *template = NULL;
    XTNODE *nodelist = NULL;

    if(xtask && nodeid > 0 &&  (template = (ITEMPLATE *)out) && left >= sizeof(ITEMPLATE) 
            && (nodelist = (XTNODE *)(xtask->nodeio.map))) 
    {
        MUTEX_LOCK(xtask->node_mutex);
        if(nodeid <= xtask->state->node_id_max && xtask->state->node_total > 0)
        {
            if(nodelist[nodeid].templates_root  > 0 && (x = vmap_min(xtask->map, 
                            nodelist[nodeid].templates_root, &id, &k)) > 0)
            {
                do
                {
                    if(db_read_data(PDB(xtask->temp), id, (char *)template) > 0)
                    {
                        ++template;
                        ++n;
                        left -= sizeof(ITEMPLATE);
                    }
                }while(left >= sizeof(ITEMPLATE) && (x = vmap_next(xtask->map, 
                                nodelist[nodeid].templates_root, x, &id, &k)) > 0);
            }
            ret = n;
        }
        MUTEX_UNLOCK(xtask->node_mutex);
    }
    return ret;
}

/* get node templates id */
int xtask_get_node_templates_id(XTASK *xtask, int nodeid, char *out, int nout)
{
    int ret = 0, k = 0, x = 0, n = 0, id = 0, *ids = NULL;
    XTNODE *nodelist = NULL;

    if(xtask && nodeid > 0 &&  (ids = (int *)out) 
            && (nodelist = (XTNODE *)(xtask->nodeio.map))) 
    {
        MUTEX_LOCK(xtask->node_mutex);
        if(nodeid <= xtask->state->node_id_max && xtask->state->node_total > 0 
                && nout > vmap_total(xtask->map, nodelist[nodeid].templates_root) * sizeof(int))
        {
            if(nodelist[nodeid].templates_root  > 0 && (x = vmap_min(xtask->map, 
                            nodelist[nodeid].templates_root, &id, &k)) > 0)
            {
                do
                {
                    *ids++ = id;
                    n++;
                }while((x = vmap_next(xtask->map, nodelist[nodeid].templates_root,x,&id,&k))>0);
            }
            ret = n;
        }
        MUTEX_UNLOCK(xtask->node_mutex);
    }
    return ret;
}



/* get node */
int xtask_get_node(XTASK *xtask, int id, XTNODE *node)
{
    XTNODE *nodelist = NULL;
    int ret = -1;

    if(xtask && id >= 0 && node && (nodelist = (XTNODE *)(xtask->nodeio.map))) 
    {
        MUTEX_LOCK(xtask->node_mutex);
        if(id <= xtask->state->node_id_max && xtask->state->node_total > 0)
        {
            memcpy(node, &(nodelist[id]), sizeof(XTNODE));
            ret = id;
        }
        MUTEX_UNLOCK(xtask->node_mutex);
    }
    return ret;
}

/* remove node */
int xtask_remove_node(XTASK *xtask, int nodeid)
{
    int ret = -1, x = 0, uid = 0, id = 0;
    XTNODE *nodelist = NULL;

    if(xtask && nodeid > 0 && nodeid <= xtask->state->node_id_max 
            && (nodelist = (XTNODE *)(xtask->nodeio.map)))
    {
        if(nodelist[nodeid].nchilds > 0 &&  nodelist[nodeid].childs_root > 0
            && (x = vmap_min(xtask->map, nodelist[nodeid].childs_root, &uid, &id)) > 0)
        {
            do
            {
                if(nodelist[id].nchilds > 0)
                    xtask_remove_node(xtask, x);
                else
                {
                    memset(&(nodelist[id]), 0, sizeof(XTNODE));
                    nodelist[id].uid = xtask->state->node_id_left;
                    xtask->state->node_id_left = id;
                    xtask->state->node_total--;
                }
            }while((x = vmap_next(xtask->map, nodelist[nodeid].childs_root, x, &uid, &id)) > 0);
            vmap_remove_tree(xtask->map, nodelist[nodeid].childs_root);
            if((x = vmap_min(xtask->map, nodelist[nodeid].templates_root, &uid, &id)) > 0)
            {
                do
                {
                    db_del_data(PDB(xtask->temp), id);
                }while((x = vmap_next(xtask->map, 
                                nodelist[nodeid].templates_root, x, &uid, &id)) > 0);
            }
            vmap_remove_tree(xtask->map, nodelist[nodeid].templates_root);
            memset(&(nodelist[nodeid]), 0, sizeof(XTNODE));
            nodelist[nodeid].uid = xtask->state->node_id_left;
            xtask->state->node_id_left = nodeid;
            xtask->state->node_total--;
        }
        else
        {
            memset(&(nodelist[nodeid]), 0, sizeof(XTNODE));
            if(nodeid > 0)
            {
                nodelist[nodeid].uid = xtask->state->node_id_left;
                xtask->state->node_id_left = nodeid;
                xtask->state->node_total--;
            }
        }
        ret = nodeid;
    }
    return ret;
}

/* delete node */
int xtask_delete_node(XTASK *xtask, int nodeid)
{
    int ret = -1, uid = 0, id = 0, parent = 0;
    XTNODE *nodelist = NULL;

    if(xtask && nodeid >= 0  && (nodelist = (XTNODE *)(xtask->nodeio.map))) 
    {
        MUTEX_LOCK(xtask->node_mutex);
        if(nodeid != 0 && (parent = nodelist[nodeid].parent) >= 0)
        {
            vmap_remove(xtask->map,nodelist[parent].childs_root, nodelist[nodeid].mapid, &uid, &id);
            nodelist[parent].nchilds--;
        }
        ret = xtask_remove_node(xtask, nodeid);
        MUTEX_UNLOCK(xtask->node_mutex);
    }
    return ret;
}

/* get node childs */
int xtask_get_node_childs(XTASK *xtask, int nodeid, XTNODE *node)
{
    int ret = -1, n = 0, uid = 0, x = 0, id = 0;
    XTNODE *nodelist = NULL, *p = NULL;

    if(xtask && nodeid >= 0 && (p = node) && (nodelist = (XTNODE *)(xtask->nodeio.map))) 
    {
        MUTEX_LOCK(xtask->node_mutex);
        if(nodeid <= xtask->state->node_id_max && xtask->state->node_total > 0
                && nodelist[nodeid].nchilds > 0 &&  nodelist[nodeid].childs_root > 0
                && (x = vmap_min(xtask->map, nodelist[nodeid].childs_root, &uid, &id)) >0)
        {
            do
            {
                memcpy(p, &(nodelist[id]), sizeof(XTNODE));
                ++p;
                ++n;
            }while((x = vmap_next(xtask->map, nodelist[nodeid].childs_root, x, &uid, &id)) > 0);
            ret = n;
        }
        MUTEX_UNLOCK(xtask->node_mutex);
    }
    return ret;
}

/* view childs node */
int xtask_view_node_childs(XTASK *xtask, int nodeid, char *out, int nout)
{
    int ret = -1,  uid = 0, x = 0, id = 0;
    char *p = NULL, *end = NULL;
    XTNODE *nodelist = NULL;

    if(xtask && nodeid >= 0 &&  (p = out)  && (end = (out + nout)) > out 
            && (nodelist = (XTNODE *)(xtask->nodeio.map))) 
    {
        //fprintf(stdout, "%s::%d 1:%d 2:%d 3:%d 4:%d 5:%d 6:%d 7:%d 8:%d 9:%d\r\n",,__FILE__, __LINE__, vmap_total(xtask->map,1), vmap_total(xtask->map,2), vmap_total(xtask->map,3), vmap_total(xtask->map,4), vmap_total(xtask->map,5), vmap_total(xtask->map,6), vmap_total(xtask->map,7),vmap_total(xtask->map,8),vmap_total(xtask->map,9));
        MUTEX_LOCK(xtask->node_mutex);
        if(nodeid <= xtask->state->node_id_max && xtask->state->node_total > 0
                && nodelist[nodeid].nchilds > 0 &&  nodelist[nodeid].childs_root > 0
                && (x = vmap_min(xtask->map, nodelist[nodeid].childs_root, &uid, &id)) > 0)
        {
            //fprintf(stdout, "%s::%d min:%d nchilds:%d/%d\r\n", __FILE__, __LINE__, x, nodelist[nodeid].nchilds, vmap_total(xtask->map, nodelist[nodeid].childs_root));
            p += sprintf(p, "({'id':'%d', 'uid':'%d', 'parent':'%d', 'nchilds':'%d', 'childs':[",
                    nodeid,  nodelist[nodeid].uid, nodelist[nodeid].parent, nodelist[nodeid].nchilds);
            do
            {
                p += sprintf(p, "{'id':'%d', 'uid':'%d', 'name':'%s','nchilds':'%d'},",
                        id,  nodelist[id].uid, nodelist[id].name, nodelist[id].nchilds);
            }while((x = vmap_next(xtask->map, nodelist[nodeid].childs_root, x, &uid, &id)) > 0);
            --p;
            p += sprintf(p, "]})");
            ret = p - out;
            //vmap_view_tree(xtask->map, nodelist[0].childs_root, stdout);
            DEBUG_LOGGER(xtask->logger, "%d:%s", nodeid, out);
        }
        MUTEX_UNLOCK(xtask->node_mutex);
    }
    return ret;
}

/* add node url */
int xtask_add_node_url(XTASK *xtask, int nodeid, int urlid)
{
    int ret = -1, k = 0, old = 0;
    XTNODE *nodelist = NULL;

    if(xtask  && nodeid > 0 && nodeid <= xtask->state->node_id_max 
            && urlid > 0 && (nodelist = (XTNODE *)(xtask->nodeio.map)))
    {
        MUTEX_LOCK(xtask->node_mutex);
        if(nodelist[nodeid].parent == 0)
        {
            if(nodelist[nodeid].urlroot == 0) nodelist[nodeid].urlroot = vmap_new_tree(xtask->map);
            k = vmap_try_insert(xtask->map, nodelist[nodeid].urlroot, urlid, nodeid, &old);   
            if(k > 0 && old == 0)
            {
                nodelist[nodeid].nurlchilds++;
            }
        }
        MUTEX_UNLOCK(xtask->node_mutex);
        ret = urlid;
    }
    return ret;
}

/* remove node url */
int xtask_remove_node_url(XTASK *xtask, int nodeid, int urlid)
{
    XTNODE *nodelist = NULL;
    int ret = -1, id = 0;

    if(xtask  && nodeid > 0 && nodeid <= xtask->state->node_id_max 
            && urlid > 0 && (nodelist = (XTNODE *)(xtask->nodeio.map)))
    {
        MUTEX_LOCK(xtask->node_mutex);
        if(nodelist[nodeid].parent == 0 && (id = vmap_find(xtask->map, 
                        nodelist[nodeid].urlroot, urlid, NULL)) > 0)
        {
            vmap_remove(xtask->map, nodelist[nodeid].urlroot, id, NULL, NULL); 
            --nodelist[nodeid].nurlchilds;
        }
        ret = urlid;
        MUTEX_UNLOCK(xtask->node_mutex);
    }
    return ret;
}

/* view node url childs */
int xtask_view_urlchilds(XTASK *xtask, int nodeid, char *out, int nout, int from, int to)
{
    int ret = -1,  k = 0, x = 0, id = 0, i = 0, n = 0, npages = 0;
    XTURLNODE *urlnodelist = NULL;
    char *p = NULL, *end = NULL;
    XTNODE *nodelist = NULL;

    if(xtask && nodeid >= 0 &&  (p = out)  && (end = (out + nout)) > out 
            && (nodelist = (XTNODE *)(xtask->nodeio.map))
            && (urlnodelist = (XTURLNODE *)(xtask->urlnodeio.map))) 
    {
        MUTEX_LOCK(xtask->node_mutex);
        MUTEX_LOCK(xtask->urlnode_mutex);
        if(nodeid <= xtask->state->node_id_max && xtask->state->node_total > 0
                && nodelist[nodeid].nurlchilds > from &&  nodelist[nodeid].urlroot > 0
                && (x = vmap_min(xtask->map, nodelist[nodeid].urlroot, &id, &k)) > 0)
        {
            npages = (nodelist[nodeid].nurlchilds / XT_PAGE_NUM) 
                + ((nodelist[nodeid].nurlchilds%XT_PAGE_NUM)  > 0);
            p += sprintf(p, "({'id':'%d', 'nchilds':'%d', 'pages':'%d','childs':{", 
                nodeid, nodelist[nodeid].nurlchilds, npages);
            do
            {
                if(i >= from && i < to)
                {
                    p += sprintf(p, "'%d':{'id':'%d','nchilds':'%d','level':'%d',"
                        "'nodeid':'%d','url':'", id, id, urlnodelist[id].nchilds, 
                        urlnodelist[id].level, urlnodelist[id].nodeid);
                    if((n = db_read_data(PDB(xtask->mmdb), urlnodelist[id].url_db_id, p)) > 0)
                        p += n;
                    p += sprintf(p, "'},");
                }
                i++;
            }while(i<to&&(x=vmap_next(xtask->map,nodelist[nodeid].urlroot,x,&id,&k)) > 0);
            --p;
            p += sprintf(p, "}})");
        }
        MUTEX_UNLOCK(xtask->urlnode_mutex);
        MUTEX_UNLOCK(xtask->node_mutex);
        ret = p - out;
    }
    return ret;
}

/* List tnode */
int xtask_list_nodes(XTASK *xtask, int nodeid, FILE *fp)
{
    int i = 0, x = 0, rootid = 0, uid = 0, childid = 0;
    XTNODE *nodelist = NULL;

    if(xtask && nodeid >= 0 && nodeid <= xtask->state->node_id_max
            &&  xtask->state->node_total > 0
            && (nodelist = (XTNODE *)(xtask->nodeio.map))) 
    {
        for(i = 0; i < nodelist[nodeid].level; i++)
        {
            fprintf(fp, "%s", "  ┆");
        }
        if(nodelist[nodeid].nchilds > 0)
        {
            fprintf(fp, "--+[%d]%s[%d]\n", nodelist[nodeid].nchilds,
                    nodelist[nodeid].name, nodeid);
        }
        else
        {
            fprintf(fp, "---%s[%d]\n", nodelist[nodeid].name, nodeid);
        }
        if(nodelist[nodeid].nchilds > 0 && (rootid = nodelist[nodeid].childs_root) > 0
                && (x = vmap_min(xtask->map, rootid, &uid, &childid)) > 0)
        {
            do
            {
                xtask_list_nodes(xtask, childid, fp);
            }while((x = vmap_next(xtask->map, rootid, x, &uid, &childid)) > 0);
        }
        return 0;
    }
    return -1;
}

/* add proxy */
int xtask_add_proxy(XTASK *xtask, char *host)
{
    int ret = -1, i = 0, x = 0, n = 0, id = 0;
    char line[XT_LINE_MAX], *p = NULL;
    XTPROXY *proxylist  = NULL;

    if(xtask && host && xtask->state && xtask->kmap 
            && (proxylist = (XTPROXY *)(xtask->proxyio.map)) 
            && (n = sprintf(line, "p:%s", host)) > 0)
    {
        MUTEX_LOCK(xtask->proxy_mutex);
        if(xtask->state->proxy_id_left < 1)
        {
            XTIO_INCRE(xtask, proxy, PROXY);
            x = (xtask->proxyio.end / sizeof(XTPROXY)) - 1;
            i = xtask->proxyio.old / sizeof(XTPROXY);
            if(i == 0) i = 1;
            while(x >= i)
            {
                proxylist[x].ip = xtask->state->proxy_id_left;
                xtask->state->proxy_id_left = x;
                --x;
            }
        }
        id = xtask->state->proxy_id_left;
        if((ret = mmtrie_add(xtask->kmap, line, n, id)) == id)
        {
            xtask->state->proxy_id_left = proxylist[id].ip;
            p = host;
            while(*p != '\0' && *p != ':')++p;
            if(*p == ':')
            {
                *p = '\0';
                proxylist[id].ip = (int)inet_addr(host);
                *p++ = ':';
                proxylist[id].port = (unsigned short)atoi(p);
                proxylist[id].status = XT_STATUS_OK;
            }
            mmqueue_push(xtask->queue, xtask->state->qproxy, id);
            xtask->state->proxy_total++;
        }
        if(id > xtask->state->proxy_id_max) xtask->state->proxy_id_max = id;
        MUTEX_UNLOCK(xtask->proxy_mutex);
    }
    return ret;
}

/* get proxy */
int xtask_get_proxy(XTASK *xtask, XTPROXY *proxy)
{
    XTPROXY *proxylist = NULL;
    int ret = -1, id = 0;

    if(xtask && proxy && (proxylist = ((XTPROXY *)(xtask->proxyio.map))))
    {
        MUTEX_LOCK(xtask->proxy_mutex);
        while(xtask->state->proxy_total > 0 
                &&  mmqueue_pop(xtask->queue, xtask->state->qproxy, &id) > 0)
        {
            if(proxylist[id].status > 0)
            {
                memcpy(proxy, &(proxylist[id]), sizeof(XTPROXY));
                mmqueue_push(xtask->queue, xtask->state->qproxy, id);
                ret = id;
                break;
            }
        }
        MUTEX_UNLOCK(xtask->proxy_mutex);
    }
    return ret;
}


/* set proxy status */
int xtask_set_proxy_status(XTASK *xtask, int id, char *host, int status)
{
    XTPROXY *proxylist  = NULL;
    char line[XT_LINE_MAX];
    int ret = -1, n = 0;

    if(xtask && (id > 0 || host) && xtask->kmap && xtask->state 
            && (proxylist = (XTPROXY *)(xtask->proxyio.map))) 
    {
        if(id < 1)
        {
            n = sprintf(line, "p:%s", host);
            id = mmtrie_get(xtask->kmap, line, n);
        }
        MUTEX_LOCK(xtask->proxy_mutex);
        if(id > 0 && id <= xtask->state->proxy_id_max)
        {
            if(proxylist[id].status > 0 && status < 1)
            {
                xtask->state->proxy_total--;
            }
            else if(proxylist[id].status < 1 && status > 0)
            {
                xtask->state->proxy_total++;
            }
            proxylist[id].status = status;            
        }
        MUTEX_UNLOCK(xtask->proxy_mutex);
        ret = id;
    }
    return ret;
}

/* delete proxy */
int xtask_del_proxy(XTASK *xtask, int id, char *host)
{
    XTPROXY *proxylist  = NULL;
    unsigned char *s = NULL;
    char line[XT_LINE_MAX];
    int ret = -1, n = 0, x = 0;

    if(xtask && (id > 0 || host) && xtask->kmap && xtask->state 
            && (proxylist = (XTPROXY *)(xtask->proxyio.map))) 
    {
        if(id < 1) 
        {
            n = sprintf(line, "p:%s", host);
            id = mmtrie_del(xtask->kmap, line, n);
        }
        else if(id > 0 && id <= xtask->state->proxy_id_max)
        {
            s = (unsigned char *)&(proxylist[id].ip);
            n = sprintf(line, "p:%d.%d.%d.%d:%d", s[0], s[1], s[2], s[3], proxylist[id].port);
            x = mmtrie_del(xtask->kmap, line, n);
            if(x != id) id = -1;
        }
        MUTEX_LOCK(xtask->proxy_mutex);
        if(id > 0 && id <= xtask->state->proxy_id_max)
        {
            memset(&(proxylist[id]), 0, sizeof(XTPROXY));
            proxylist[id].ip = xtask->state->proxy_id_left;
            xtask->state->proxy_id_left = id;
            xtask->state->proxy_total--;
        }
        MUTEX_UNLOCK(xtask->proxy_mutex);
        ret = id;
    }
    return ret;
}

/* view proxy */
int xtask_view_proxylist(XTASK *xtask, char *block, int nblock)
{
    char *p = NULL, *end = NULL;
    XTPROXY *proxylist  = NULL;
    int ret = -1, i = 0, x = 0;
    unsigned char *s = NULL;

    if(xtask && (p = block) && (end = (p + nblock)) > p 
            && (proxylist = (XTPROXY *)(xtask->proxyio.map)) 
            && (x = xtask->state->proxy_total) > 0)
    {
        MUTEX_LOCK(xtask->proxy_mutex); 
        p += sprintf(p, "({");  
        for(i = 1; i <= xtask->state->proxy_id_max; i++)
        {
            if(proxylist[i].status)
            {
                s = (unsigned char *)&(proxylist[i].ip);
                p += sprintf(p, "\"%d\":{\"host\":\"%d.%d.%d.%d:%d\",\"status\":\"%d\"}", 
                        i, s[0],s[1],s[2],s[3], proxylist[i].port, proxylist[i].status);
                if(x > 1)
                {
                    *p++ = ',';
                    *p = '\0';
                }
                --x;
            }
        }
        p += sprintf(p, "})");
        MUTEX_UNLOCK(xtask->proxy_mutex); 
        ret = p - block;
    }
    return ret;
}

/* add dns */
int xtask_add_dns(XTASK *xtask, char *host)
{
    int ret = -1, i = 0, x = 0, n = 0, id = 0;
    char line[XT_LINE_MAX];
    XTDNS *dnslist  = NULL;

    if(xtask && host && xtask->state && xtask->kmap 
            && (dnslist = (XTDNS *)(xtask->dnsio.map)) 
            && (n = sprintf(line, "d:%s", host)) > 0)
    {
        MUTEX_LOCK(xtask->dns_mutex);
        if(xtask->state->dns_id_left < 1)
        {
            XTIO_INCRE(xtask, dns, DNS);
            x = (xtask->dnsio.end / sizeof(XTDNS)) - 1;
            i = xtask->dnsio.old / sizeof(XTDNS);
            if(i == 0) i = 1;
            while(x >= i)
            {
                dnslist[x].ip = xtask->state->dns_id_left;
                xtask->state->dns_id_left = x;
                --x;
            }
        }
        id = xtask->state->dns_id_left;
        if((ret = mmtrie_add(xtask->kmap, line, n, id)) == id)
        {
            xtask->state->dns_id_left = dnslist[id].ip;
            dnslist[id].ip = (int)inet_addr(host);
            dnslist[id].status = DNS_STATUS_OK;
            mmqueue_push(xtask->queue, xtask->state->qdns, id);
            xtask->state->dns_total++;
        }
        if(id > xtask->state->dns_id_max) xtask->state->dns_id_max = id;
        MUTEX_UNLOCK(xtask->dns_mutex);
    }
    return ret;
}

/* queue dns */
int xtask_queue_dns(XTASK *xtask)
{
    int ret = -1, id = 0, i = 0;
    XTDNS *dnslist = NULL;

    if(xtask && (dnslist = (XTDNS *)(xtask->dnsio.map)))
    {
        MUTEX_LOCK(xtask->dns_mutex);
        while(mmqueue_pop(xtask->queue, xtask->state->qdns, &id) > 0);
        i = 1;
        while(i <= xtask->state->dns_id_max)
        {
            if(dnslist[i].status > 0) 
            {
                mmqueue_push(xtask->queue, xtask->state->qdns, i);
            }
            i++;
        }
        MUTEX_UNLOCK(xtask->dns_mutex);
    }
    return ret;
}


/* set dns status */
int xtask_set_dns_status(XTASK *xtask, int id, char *host, int status)
{
    char line[XT_LINE_MAX];
    XTDNS *dnslist  = NULL;
    int ret = -1, n = 0;

    if(xtask && (id > 0 || host) && xtask->kmap && xtask->state 
            && (dnslist = (XTDNS *)(xtask->dnsio.map))) 
    {
        if(id < 1)
        {
            n = sprintf(line, "d:%s", host);
            id = mmtrie_get(xtask->kmap, line, n);
        }
        MUTEX_LOCK(xtask->dns_mutex);
        if(id > 0 && id <= xtask->state->dns_id_max)
        {
            if(dnslist[id].status > 0 && status < 1)
            {
                xtask->state->dns_total--;
            }
            else if(dnslist[id].status < 1 && status > 0)
            {
                xtask->state->dns_total++;
            }
            dnslist[id].status = status;            
        }
        MUTEX_UNLOCK(xtask->dns_mutex);
        ret = id;
    }
    return ret;
}

/* pop dns */
int xtask_pop_dns(XTASK *xtask, char *dns)
{
    int ret = -1, id = 0, n = 0;
    XTDNS *dnslist = NULL;
    unsigned char *s = NULL;

    if(xtask && dns && (dnslist = (XTDNS *)(xtask->dnsio.map)))
    {
        MUTEX_LOCK(xtask->dns_mutex);  
        while(mmqueue_pop(xtask->queue, xtask->state->qdns, &id) > 0)
        {
            if(id > 0 && dnslist[id].status > 0 && dnslist[id].ip) 
            {
                //fprintf(stdout, "%s::%d dns:%d\r\n",,__FILE__, __LINE__, id);
                s = (unsigned char *)&(dnslist[id].ip); 
                n = sprintf(dns, "%d.%d.%d.%d", s[0], s[1], s[2], s[3]);
                ret = id;
                break;
            }
        }
        MUTEX_UNLOCK(xtask->dns_mutex);
    }
    return ret;
}

/* reset dns */
int xtask_reset_dns(XTASK *xtask, int id)
{
    XTDNS *dnslist  = NULL;
    int ret = -1;

    if(xtask && id > 0 && (dnslist = (XTDNS *)(xtask->dnsio.map))) 
    {
        if(++(dnslist[id].nerrors) > DNS_ERRORS_MAX)
        {
            dnslist[id].status = DNS_STATUS_ERR;
        }
        else
        {
            mmqueue_push(xtask->queue, xtask->state->qdns, id);
        }
        ret = id;
    }
    return ret;
}

/* delete dns */
int xtask_del_dns(XTASK *xtask, int id, char *host)
{
    int ret = -1, n = 0, x = 0;
    unsigned char *s = NULL;
    char line[XT_LINE_MAX];
    XTDNS *dnslist  = NULL;

    if(xtask && (id > 0 || host) && xtask->kmap && xtask->state 
            && (dnslist = (XTDNS *)(xtask->dnsio.map))) 
    {
        if(id < 1) 
        {
            n = sprintf(line, "d:%s", host);
            id = mmtrie_del(xtask->kmap, line, n);
        }
        else if(id > 0 && id <= xtask->state->dns_id_max)
        {
            s = (unsigned char *)&(dnslist[id].ip);
            n = sprintf(line, "d:%d.%d.%d.%d", s[0], s[1], s[2], s[3]);
            x = mmtrie_del(xtask->kmap, line, n);
            if(x != id) id = -1;
        }
        MUTEX_LOCK(xtask->dns_mutex);
        if(id > 0 && id <= xtask->state->dns_id_max)
        {
            memset(&(dnslist[id]), 0, sizeof(XTDNS));
            dnslist[id].ip = xtask->state->dns_id_left;
            xtask->state->dns_id_left = id;
            xtask->state->dns_total--;
        }
        MUTEX_UNLOCK(xtask->dns_mutex);
        ret = id;
    }
    return ret;
}

/* view dns */
int xtask_view_dnslist(XTASK *xtask, char *block, int nblock)
{
    char *p = NULL, *end = NULL;
    XTDNS *dnslist  = NULL;
    int ret = -1, i = 0, x = 0;
    unsigned char *s = NULL;

    if(xtask && (p = block) && (end = (p + nblock)) > p 
            && (dnslist = (XTDNS *)(xtask->dnsio.map)) 
            && (x = xtask->state->dns_total) > 0)
    {
        MUTEX_LOCK(xtask->dns_mutex); 
        p += sprintf(p, "({\"dnslist\":{");  
        for(i = 1; i <= xtask->state->dns_id_max; i++)
        {
            if(dnslist[i].status)
            {
                s = (unsigned char *)&(dnslist[i].ip);
                p += sprintf(p, "\"%d\":{\"host\":\"%d.%d.%d.%d\",\"status\":\"%d\"}", 
                        i, s[0],s[1],s[2],s[3], dnslist[i].status);
                if(x > 1)
                {
                    *p++ = ',';
                    *p = '\0';
                }
                --x;
            }
        }
        p += sprintf(p, "}})");
        MUTEX_UNLOCK(xtask->dns_mutex); 
        ret = p - block;
    }
    return ret;
}

/* add host */
int xtask__hostid(XTASK *xtask, char *host)
{
    char line[XT_LINE_MAX], *p = NULL, *s = NULL;
    int ret = -1, i = 0, x = 0, n = 0, id = 0;
    XTHOST *hostlist  = NULL;

    if(xtask && (s = host) && xtask->state && xtask->kmap 
            && (hostlist = (XTHOST *)(xtask->hostio.map)))
    {
        p = line;*p++ = 'h';*p++ = ':';
        while(*s != 0)
        {
            if(*s >= 'A' && *s <= 'Z') *p++ = *s++ - ('A' - 'a');
            else *p++ = *s++;
        }
        *p = 0;
        //fprintf(stdout, "%s::%d host:%s\r\n",,__FILE__, __LINE__, line);
        n = p - line;
        MUTEX_LOCK(xtask->host_mutex);
        if(xtask->state->host_id_left < 1)
        {
            XTIO_INCRE(xtask, host, HOST);
            x = (xtask->hostio.end / sizeof(XTHOST)) - 1;
            i = xtask->hostio.old / sizeof(XTHOST);
            if(i == 0) i = 1;
            while(x >= i)
            {
                hostlist[x].id = xtask->state->host_id_left;
                xtask->state->host_id_left = x;
                --x;
            }
        }
        id = xtask->state->host_id_left;
        if((ret = mmtrie_add(xtask->kmap, line, n, id)) == id)
        {
            xtask->state->host_id_left = hostlist[id].id;
            hostlist[id].id = id;
            hostlist[id].status = HOST_STATUS_READY;
            n = sprintf(line, "host:%d", id);
            hostlist[id].db_host_id = db_xadd_data(PDB(xtask->mmdb), line, n, host, strlen(host));
            mmqueue_push(xtask->queue, xtask->state->qhost, id);
            xtask->state->host_total++;
            if(id > xtask->state->host_id_max) xtask->state->host_id_max = id;
            DEBUG_LOGGER(xtask->logger, "host:%s hostid:%d max:%d ret:%d", host, id, xtask->state->host_id_max, ret);
        }
        MUTEX_UNLOCK(xtask->host_mutex);
    }
    return ret;
}

/* pop host */
int xtask_pop_host(XTASK *xtask, char *host)
{
    int ret = -1, id = 0, n = 0, nodeid = 0, old = 0, expire = 0;
    unsigned int now = 0, k = 0;
    XTHOST *hostlist = NULL;

    if(xtask && host && (hostlist = (XTHOST *)(xtask->hostio.map)))
    {
        MUTEX_LOCK(xtask->host_mutex);
        now = (unsigned int)time(NULL);
        while((expire = ((nodeid = qmap_min(xtask->qmap, xtask->state->qhost_expire,
                (int *)&k, &id)) > 0 && k < now))
                || mmqueue_pop(xtask->queue, xtask->state->qhost, &id) > 0)
        {
            if(id > 0 && id <= xtask->state->host_id_max 
                    //&& vmap_find(xtask->map, xtask->state->host_task_wait, id, NULL) <= 0
                    //&& (hostlist[id].status & HOST_STATUS_READY) 
                    && (n = db_read_data(PDB(xtask->mmdb), hostlist[id].db_host_id, host)) > 0)
            {
                host[n] = '\0';
                old = 0;
                vmap_try_insert(xtask->map, xtask->state->host_task_wait, id, id, &old);
                if(expire)
                {
                    qmap_remove(xtask->qmap, xtask->state->qhost_expire, nodeid, NULL, NULL);
                    WARN_LOGGER(xtask->logger, "expired host:%s[%d] at  %u", host, id, k);
                }
                //mmqueue_push(xtask->queue, xtask->state->qhost, id);
                if(old == 0)
                {
                    ret = id;
                    break;
                }
            }
            k = 0;
        }
        MUTEX_UNLOCK(xtask->host_mutex);
    }
    return ret;
}

/* set host iplist */
int xtask_set_host_ip(XTASK *xtask, char *host, int *iplist, int niplist, unsigned int ttl)
{
    int ret = -1, hostid = 0, k = 0, n = 0;
    XTHOST *hostlist = NULL;
    char line[XT_LINE_MAX];
    unsigned int expire = 0;

    
    if(xtask && (hostid = xtask__hostid(xtask, host))> 0 && iplist && niplist > 0
        && (hostlist = (XTHOST *)(xtask->hostio.map)))
    {
        MUTEX_LOCK(xtask->host_mutex);
        if(hostid <= xtask->state->host_id_max)
        {
            /*
            fprintf(stdout, "%s::%d hostid:%d host:%s ",,__FILE__, __LINE__, hostid, host);
            for(k = 0; k < niplist; k++)
            {
                fprintf(stdout, " %d:%d", k, iplist[k]);
            }
            fprintf(stdout, "\r\n");
            */
            if(hostlist[hostid].db_ip_id == 0)
            {
                n = sprintf(line, "ip:%d", hostid);
                hostlist[hostid].db_ip_id = db_xadd_data(PDB(xtask->mmdb), line, n, 
                        (char *)iplist, sizeof(int) * niplist);
            }
            else
            {
                n = db_set_data(PDB(xtask->mmdb), hostlist[hostid].db_ip_id, 
                        (char *)iplist, sizeof(int) * niplist);
            }
            ret = hostid;
            if(!(hostlist[hostid].status & HOST_STATUS_WAIT))
            {
                if(hostlist[hostid].level > 0)
                    mmqueue_push(xtask->queue, xtask->state->qpriority, hostid);
                else
                {
                    mmqueue_push(xtask->queue, xtask->state->qwait, hostid);
                }
                hostlist[hostid].status |= HOST_STATUS_WAIT;
            }
            hostlist[hostid].status &= ~HOST_STATUS_READY;
            hostlist[hostid].status |= HOST_STATUS_OK;
            DEBUG_LOGGER(xtask->logger, "qwait host:%s hostid:%d total:%d status:%d", host, hostid, mmqueue_total(xtask->queue, xtask->state->qwait), hostlist[hostid].status);
            if((k = vmap_find(xtask->map, xtask->state->host_task_wait, hostid, NULL)) > 0)
            {
                expire = ((unsigned int)time(NULL)) + ttl;
                qmap_insert(xtask->qmap, xtask->state->qhost_expire, (int)expire, hostid, NULL);
                vmap_remove(xtask->map, xtask->state->host_task_wait, k, NULL, NULL);
                WARN_LOGGER(xtask->logger, "resolved host:%s[%d] which will be expired at %u", host, hostid, expire);
            }
        }
        MUTEX_UNLOCK(xtask->host_mutex);
    }
    return ret;
}

/* reset host queue */
void xtask__reset__host(XTASK *xtask, int hostid)
{
    int mid = 0;

    if( xtask && hostid > 0 && hostid <= xtask->state->host_id_max 
            && (mid = vmap_find(xtask->map, xtask->state->host_task_wait, hostid, NULL)) > 0)
    {
        mmqueue_push(xtask->queue, xtask->state->qhost, hostid);
        vmap_remove(xtask->map, xtask->state->host_task_wait, mid, NULL, NULL);
    }
    return ;
}

/* reset host queue */
void xtask_reset_host(XTASK *xtask, int hostid)
{
    int mid = 0;

    if(xtask && hostid > 0)
    {
        MUTEX_LOCK(xtask->host_mutex);
        if(hostid <= xtask->state->host_id_max && (mid = vmap_find(xtask->map, 
                        xtask->state->host_task_wait, hostid, NULL)) > 0)
        {
            mmqueue_push(xtask->queue, xtask->state->qhost, hostid);
            vmap_remove(xtask->map, xtask->state->host_task_wait, mid, NULL, NULL);
        }
        MUTEX_UNLOCK(xtask->host_mutex);
    }
    return ;
}


int xtask__host__ip(XTASK *xtask, XTHOST *host)
{
    int ret = -1, k = 0, n = 0, iplist[XT_IP_MAX];

    if(xtask && host)
    {
        if((host->status & HOST_STATUS_OK)
            && (n = db_read_data(PDB(xtask->mmdb),host->db_ip_id,(char *)iplist))> 0)
        {
            k = random()%(n/sizeof(int));
            ret = iplist[k];
        }
    }
    return ret;
}

/* get host ip */
int xtask_get_host_ip(XTASK *xtask, char *host)
{
    int ret = -1, hostid = 0;
    XTHOST *hostlist = NULL;

    if(xtask && host && (hostlist = (XTHOST *)(xtask->hostio.map))
        && (hostid = xtask__hostid(xtask, host)) > 0)
    {
        MUTEX_LOCK(xtask->host_mutex);
        if(hostid <= xtask->state->host_id_max)
        {
            ret = xtask__host__ip(xtask, &(hostlist[hostid]));
        }
        MUTEX_UNLOCK(xtask->host_mutex);
    }
    return ret;
}


int xtask_host_ip(XTASK *xtask, int hostid)
{
    XTHOST *hostlist = NULL;
    int ret = -1;

    if(xtask && hostid > 0 && (hostlist = (XTHOST *)(xtask->hostio.map)))
    {
        MUTEX_LOCK(xtask->host_mutex);
        DEBUG_LOGGER(xtask->logger, "hostid:%d status:%d", hostid, hostlist[hostid].status);
        if(hostid <= xtask->state->host_id_max)
        {
            ret = xtask__host__ip(xtask, &(hostlist[hostid]));
        }
        DEBUG_LOGGER(xtask->logger, "hostid:%d status:%d ret:%d", hostid, hostlist[hostid].status, ret);
        MUTEX_UNLOCK(xtask->host_mutex);
    }
    return ret;
}

/* set host status*/
int xtask_set_host_status(XTASK *xtask, int hostid, int status)
{
    int ret = -1;
    XTHOST *hostlist = NULL;

    if(xtask && hostid > 0 && (hostlist = (XTHOST *)(xtask->hostio.map)))
    {
        MUTEX_LOCK(xtask->host_mutex);
        if(hostid <= xtask->state->host_id_max)
        {
            hostlist[hostid].status = status;
            xtask__reset__host(xtask, hostid);
            ret = hostid;
        }
        MUTEX_UNLOCK(xtask->host_mutex);
    }
    return ret;
}

/* set host level*/
int xtask_set_host_level(XTASK *xtask, int hostid, int level)
{
    int ret = -1;
    XTHOST *hostlist = NULL;

    if(xtask && hostid > 0 && (hostlist = (XTHOST *)(xtask->hostio.map)))
    {
        MUTEX_LOCK(xtask->host_mutex);
        if(hostid <= xtask->state->host_id_max)
        {
            hostlist[hostid].level = level;
            ret = hostid;
        }
        MUTEX_UNLOCK(xtask->host_mutex);
    }
    return ret;
}

/* get cookie id with name */
int xtask_cookieid(XTASK *xtask, char *name, int name_len)
{
    int ret = -1, id = 0;

    if(xtask && name && name_len > 0)
    {
        MUTEX_LOCK(xtask->host_mutex);
        id = xtask->state->cookie_uid_max + 1;
        if((ret = mmtrie_add(xtask->kmap, name, name_len, id)) == id)
        {
            xtask->state->cookie_uid_max = id;
        }
        MUTEX_UNLOCK(xtask->host_mutex);
    }
    return ret;
}

/* initialize cookies */
int xtask_cookies_init(XTASK *xtask, XTCOOKIE *cookies, int from, char *cookie)
{
    char *p = NULL, *bs = NULL, *s = NULL, *pp = NULL, *start = 0;
    int ret = -1, n = 0, i = 0, k = 0;

    if(xtask && cookies &&  (i = from) >= 0 
            && i < XT_COOKIE_MAX && (p = bs = cookie))
    {
        i = 0;
        while(*p != '\0' && i < XT_COOKIE_MAX && (s = strstr(p, ";id:"))) 
        {
            start = p;
            cookies[i].off = p - bs; 
            cookies[i].clen = s - p; 
            if((pp = strchr(start, '=')) && pp < s)
            {
                ++pp;
                cookies[i].vlen = s - pp; 
            }
            p = s + 4;
            cookies[i].id = atoi(p);
            pp = ";expire:";
            n = strlen(pp);
            s = strstr(p, pp);
            while(*p != '\0' && *p != '\n')++p;
            if(*p == '\n')++p;
            /* expire */
            if(s < p)
            {
                cookies[i].expire = (unsigned int)atoi(s + n);
            }
            cookies[i].len = 0;
            if(cookies[i].vlen > 0)
            {
                cookies[i].len = p - start;
            }
            for(k = 0; k < i; k++)
            {
                if(cookies[k].id == cookies[i].id)
                {
                    memset(&(cookies[k]), 0, sizeof(XTCOOKIE));
                }
            }
            i++;
        }
        ret = i;
    }
    return ret;
}

/* get host cookies */
int xtask_get_host_cookie(XTASK *xtask, int hostid, char *cookie)
{
    char *p = NULL, line[XT_COOKIE_SIZE];
    XTCOOKIE cookies[XT_COOKIE_MAX];
    int ret = 0, ncookies = 0, i = 0;
    XTHOST *hostlist = NULL;
    time_t now = time(NULL);

    if(xtask && hostid > 0 && (p = cookie) && (hostlist = (XTHOST *)(xtask->hostio.map)) )
    {
         MM_HMUTEX_LOCK(xtask, hostid);
        if(hostid <= xtask->state->host_id_max && hostlist[hostid].db_cookie_id > 0) 
        {
            memset(cookies, 0, sizeof(XTCOOKIE) * XT_COOKIE_MAX);
            if(db_read_data(PDB(xtask->mmdb), hostlist[hostid].db_cookie_id, line) > 0
                && (ncookies = xtask_cookies_init(xtask, cookies, 0, line)) > 0)
            {
                for(i = 0; i < ncookies; i++)
                {
                    if(cookies[i].expire == 0 || cookies[i].expire > now)
                        p += sprintf(p, "%.*s; ", cookies[i].clen, line + cookies[i].off);
                }
                ret = p - cookie;
            }
        }
    }
    return ret;
}

/* add host cookies */
int xtask_add_host_cookie(XTASK *xtask, int hostid, char *cookie)
{
    int ret = -1, ncookies = 0, n = 0, i = 0, j = 0, nold = 0, len = 0;
    char *p = NULL, line[XT_COOKIE_SIZE], buf[XT_COOKIE_SIZE];
    XTCOOKIE cookies[XT_COOKIE_MAX], old[XT_COOKIE_MAX];
    XTHOST *hostlist = NULL;
    time_t now = time(NULL);

    if(xtask && hostid > 0 && (p = cookie) && (hostlist = (XTHOST *)(xtask->hostio.map)) )
    {
        memset(cookies, 0, sizeof(XTCOOKIE) * XT_COOKIE_MAX);
        memset(old, 0, sizeof(XTCOOKIE) * XT_COOKIE_MAX);
        ncookies = xtask_cookies_init(xtask, cookies, 0, cookie);
        //fprintf(stdout, "%s::%d ret[%d] = cookie_init(%s)\r\n", __FILE__, __LINE__, ncookies, cookie);
        MM_HMUTEX_LOCK(xtask, hostid);
        if(hostid <= xtask->state->host_id_max) 
        {
            if(hostlist[hostid].db_cookie_id > 0 && (n = db_read_data(PDB(xtask->mmdb),
                        hostlist[hostid].db_cookie_id, line)) > 0)
            {
                line[n] = 0;
                nold = xtask_cookies_init(xtask, old, 0, line);
                /* merge */
                for(i = 0; i < nold; i++)
                {
                    if(old[i].expire < now) old[i].len = 0;
                    else
                    {
                        for(j = 0; j < ncookies; j++)
                        {
                            if(old[i].id == cookies[j].id)
                            {
                                old[i].len = 0;
                            }
                        }
                    }
                }
            }
            /* final */
            p = buf;
            for(i = 0; i < ncookies; i++)
            {
                if(cookies[i].len > 0 && (cookies[i].expire == 0 || cookies[i].expire > now))
                    p += sprintf(p, "%.*s", cookies[i].len, cookie+cookies[i].off);    
            }
            for(i = 0; i < nold; i++)
            {
                if(cookies[i].len > 0 && (cookies[i].expire == 0 || cookies[i].expire > now))
                    p += sprintf(p, "%.*s", old[i].len, line+old[i].off);    
            }
            if((len = (p - buf)) > 0)
            {
                len++;
                if(hostlist[hostid].db_cookie_id == 0)
                {
                    n = sprintf(line, "cookie:%d", hostid);
                    hostlist[hostid].db_cookie_id = db_xset_data(PDB(xtask->mmdb), line, n, 
                            buf, len); 
                }
                else
                {
                    db_set_data(PDB(xtask->mmdb), hostlist[hostid].db_cookie_id, buf, len);
                }
                //fprintf(stdout, "cookie:%s\n", buf);
            }
            ret = hostid;
        }
        MM_HMUTEX_UNLOCK(xtask, hostid);
    }
    return ret;
}

/* parse cookie */
int xtask_parse_cookie(XTASK *xtask, int hostid, time_t date, char *cookie, int ncookie)
{
    char *p = NULL, *s = NULL, *ss = NULL, *end = NULL, line[XT_COOKIE_SIZE],
         *k = NULL, *v = NULL, *host = NULL, *pp = NULL;
    int ret = -1, domainid = 0, cookieid = 0, nk = 0, nv = 0, ne = 0, nh = 0;
    time_t datetime = 0, expire = 0;

    if(xtask && hostid > 0 && (p = cookie) && (end = (p + ncookie)) > p)
    {
        pp = line;
        while(*p != '\0' && p < end && (s = strchr(p, '='))) 
        {
            nk = 0; nv = 0; ne = 0; nh = 0;
            /* name=value; */
            k = p;
            nk = s - k;
            p = v = (s+1);
            if((s = strchr(p, ';')))
            {
                nv = s - v;
                p = s;
            }
            /* expire e=value;*/
            datetime = 0;
            if((s = strstr(p, ";e=")))
            {
                s += 3;
                ss = s;
                while(*s != '\0' && *s != ';')++s;
                if(*s == ';' && (ne = (s - ss)) > 0) 
                {
                    *s = '\0';
                    datetime = (time_t)atoi(ss);
                    *s = ';';
                }
                p = s;
            }
            /* domain h=value;*/
            host = NULL;
            if((s = strstr(p, ";h=")))
            {
                s += 3;
                ss = s; 
                while(*s != '\0' && *s != ';')++s;
                if(*s == ';' && (nh = (s - ss)) > 0) 
                {
                    host = ss;
                }
                p = s;
            }
            cookieid = xtask_cookieid(xtask, k, nk);
            if(host)domainid = xtask__hostid(xtask, host);
            expire = time(NULL);
            if(datetime  > 0 && date > 0 && datetime > date)
            {
                expire = time(NULL) + (datetime - date); 
            }
            else
            {
                if(datetime == 0) expire = 0;
            }
            pp += sprintf(pp, "%.*s=%.*s;id:%d;expire:%u;\n", nk, k, nv, v, cookieid, (unsigned int)expire); 
            while(*p != '\0' && *p != '\n')++p;
            if(*p == '\n')++p;
        }
        ret = xtask_add_host_cookie(xtask, hostid, line);
    }
    return ret;
}

/* delete host cookie */
int xtask_del_host_cookie(XTASK *xtask, int hostid)
{
    XTHOST *hostlist = NULL;
    int ret = -1;

    if(xtask && hostid > 0 && (hostlist = (XTHOST *)(xtask->hostio.map)))
    {
        MUTEX_LOCK(xtask->host_mutex);
        if(hostid <= xtask->state->host_id_max)
        {
            if(hostlist[hostid].db_cookie_id > 0)
            {
                db_del_data(PDB(xtask->mmdb), hostlist[hostid].db_cookie_id);
                hostlist[hostid].db_cookie_id = 0;
            }
            ret = hostid;
        }
        MUTEX_UNLOCK(xtask->host_mutex);
    }
    return ret;
}

/* add host url */
int xtask_add_host_url(XTASK *xtask, char *host, int urlid, int flag)
{
    int ret = -1, hostid = 0, k = 0, old = 0;
    XTURLNODE *urlnodelist = NULL;
    XTHOST *hostlist = NULL;

    if(xtask  && (hostid = xtask__hostid(xtask, host)) > 0 
            && urlid > 0 && (hostlist = (XTHOST *)(xtask->hostio.map))
            && (urlnodelist = (XTURLNODE *)(xtask->urlnodeio.map)))
    {
        MUTEX_LOCK(xtask->host_mutex);
        if(hostlist[hostid].urlroot == 0) hostlist[hostid].urlroot = vmap_new_tree(xtask->map);
        k = vmap_try_insert(xtask->map, hostlist[hostid].urlroot, urlid, hostid, &old); 
        if(old == 0 && urlid > 0 && urlid <= xtask->state->urlnode_id_max)
        {
            urlnodelist[urlid].hostid = hostid;
            urlnodelist[urlid].host_mid = k;
        }
        if(!(flag & REG_IS_FILE) && old == 0)
        {
            if(hostlist[hostid].qwait == 0)
                hostlist[hostid].qwait = mmqueue_new(xtask->queue);
            if(!(flag & REG_IS_LIST)) 
                mmqueue_push(xtask->queue, hostlist[hostid].qwait, urlid);
            DEBUG_LOGGER(xtask->logger, "qwait url:%d host:%s hostid:%d total:%d status:%d", urlid, host, hostid, mmqueue_total(xtask->queue, hostlist[hostid].qwait), hostlist[hostid].status);
            if(hostlist[hostid].status != HOST_STATUS_ERR 
                && !(hostlist[hostid].status & HOST_STATUS_WAIT))
            {
                if(hostlist[hostid].level > 0)
                    mmqueue_push(xtask->queue, xtask->state->qpriority, hostid);
                else
                {
                    mmqueue_push(xtask->queue, xtask->state->qwait, hostid);
                }
                hostlist[hostid].status |= HOST_STATUS_WAIT;
                DEBUG_LOGGER(xtask->logger, "qwait url:%d host:%s hostid:%d total:%d status:%d", urlid, host, hostid, mmqueue_total(xtask->queue, hostlist[hostid].qwait), hostlist[hostid].status);
            }
            hostlist[hostid].nurlchilds++;
        }
        ret = urlid;
        MUTEX_UNLOCK(xtask->host_mutex);
    }
    return ret;
}


/* pop host task */
int xtask_host_poptask(XTASK *xtask, int *hostid)
{
    int ret = -1, k = -1, urlid = 0, first = 0;
    XTHOST *hostlist = NULL;

    if(xtask && (hostlist = (XTHOST *)(xtask->hostio.map)))
    {
        MUTEX_LOCK(xtask->host_mutex);
        do
        {
            if(mmqueue_pop(xtask->queue, xtask->state->qpriority, &k) > 0)
            {
                mmqueue_push(xtask->queue, xtask->state->qpriority, k);
            }
            else if(mmqueue_pop(xtask->queue, xtask->state->qwait, &k) > 0)
            {
                mmqueue_push(xtask->queue, xtask->state->qwait, k);
            }
            if(k <= 0) break;
            if(mmqueue_pop(xtask->queue, hostlist[k].qwait, &urlid) > 0)
            {
                *hostid = k;
                ret = urlid;
                break;
            }
            if(!first){first = k; k = 0;}
        }while(k != first);
        MUTEX_UNLOCK(xtask->host_mutex);
    }
    return ret;
}

/* pop hostid task */
int xtask_host_newtask(XTASK *xtask, int hostid)
{
    XTHOST *hostlist = NULL;
    int ret = -1, urlid = 0;

    if(xtask && hostid > 0 && (hostlist = (XTHOST *)(xtask->hostio.map)))
    {
        MUTEX_LOCK(xtask->host_mutex);
        if(mmqueue_pop(xtask->queue, hostlist[hostid].qwait, &urlid) > 0)
        {
            ret = urlid;
        }
        MUTEX_UNLOCK(xtask->host_mutex);
    }
    return ret;
}

/* push hostid task */
int xtask_host_retask(XTASK *xtask, int hostid, int urlid)
{
    XTHOST *hostlist = NULL;
    int ret = -1;

    if(xtask && hostid > 0 && urlid > 0 && (hostlist = (XTHOST *)(xtask->hostio.map)))
    {
        MUTEX_LOCK(xtask->host_mutex);
        mmqueue_push(xtask->queue, hostlist[hostid].qwait, urlid);
        ret = urlid;
        MUTEX_UNLOCK(xtask->host_mutex);
    }
    return ret;
}

/* set qwait */
int xtask_host_qwait(XTASK *xtask, int hostid)
{
    XTHOST *hostlist = NULL;

    if(xtask && hostid > 0 && (hostlist = (XTHOST *)(xtask->hostio.map)))
    {
        MUTEX_LOCK(xtask->host_mutex);
        if(hostid <= xtask->state->host_id_max)
        {
            /* push back */
            if(mmqueue_total(xtask->queue, hostlist[hostid].qwait) > 0)
            {
                if(hostlist[hostid].level == HOST_LEVEL_FIFO)
                    mmqueue_push(xtask->queue, xtask->state->qpriority, hostid);
                else
                {
                    mmqueue_push(xtask->queue, xtask->state->qwait, hostid);
                    DEBUG_LOGGER(xtask->logger, "qwait hostid:%d total:%d", hostid, mmqueue_total(xtask->queue, xtask->state->qwait));
                }
                hostlist[hostid].status |= HOST_STATUS_WAIT;
            }
            else
            {
                hostlist[hostid].status &= ~HOST_STATUS_WAIT;
            }
            DEBUG_LOGGER(xtask->logger, "qwait hostid:%d total:%d status:%d", hostid, mmqueue_total(xtask->queue, hostlist[hostid].qwait), hostlist[hostid].status);
        }
        MUTEX_UNLOCK(xtask->host_mutex);
    }
    return hostid;
}

/* get qwait */
int xtask_qwait_hostid(XTASK *xtask)
{
    XTHOST *hostlist = NULL;
    int hostid = -1;

    if(xtask && (hostlist = (XTHOST *)(xtask->hostio.map)))
    {
        MUTEX_LOCK(xtask->host_mutex);
        if(mmqueue_pop(xtask->queue, xtask->state->qpriority, &hostid) > 0)
        {
            mmqueue_push(xtask->queue, xtask->state->qpriority, hostid);
        }
        else if(mmqueue_pop(xtask->queue, xtask->state->qwait, &hostid) > 0)
        {
            mmqueue_push(xtask->queue, xtask->state->qwait, hostid);
        }
        MUTEX_UNLOCK(xtask->host_mutex);
    }
    return hostid;
}

/* remove host url */
int xtask_remove_host_url(XTASK *xtask, int hostid, int urlid)
{
    XTNODE *hostlist = NULL;
    int ret = -1, id = 0;

    if(xtask  && hostid > 0 && hostid <= xtask->state->host_id_max 
            && urlid > 0 && (hostlist = (XTNODE *)(xtask->hostio.map)))
    {
        MUTEX_LOCK(xtask->host_mutex);
        if((id = vmap_find(xtask->map, hostlist[hostid].urlroot, urlid, NULL)) > 0 )
        {
            vmap_remove(xtask->map, hostlist[hostid].urlroot, id, NULL, NULL); 
            --hostlist[hostid].nurlchilds;
        }
        ret = urlid;
        MUTEX_UNLOCK(xtask->host_mutex);
    }
    return ret;
}

/* add url */
int xtask_add_url(XTASK *xtask, int parent, int nodeid, char *url, int flag)
{
    int ret = -1, n = 0, i = 0, k = 0, x = 0, id = 0, old = 0;
    char line[XT_LINE_MAX], *p = NULL, *s = NULL;
    unsigned char digest[MD5_LEN];
    XTURLNODE *urlnodelist = NULL;
    int64_t key = 0, *tmp = NULL;

    if(xtask && parent >= 0 && nodeid && (p = url)
            && (urlnodelist = (XTURLNODE *)(xtask->urlnodeio.map)))
    {
        s = line;
        while(*p != '\0')
        {
            if(*p >= 'A' && *p <= 'Z') *s++ = *p++ - ('A' - 'a');
            else *s++ = *p++;
        }
        *s = '\0';
        n = s - line;
        md5((unsigned char *)line, n, digest); 
        tmp = (int64_t *)digest; key = *tmp;
        //fprintf(stdout, "url:%s md5:%lld\r\n", line, key);
        MUTEX_LOCK(xtask->urlnode_mutex);
        if(xtask->state->urlmap_root == 0)
            xtask->state->urlmap_root = kmap_new_tree(xtask->urlmap);
        if(parent <= xtask->state->urlnode_id_max)
        {
            if(xtask->state->urlnode_id_left < 1)
            {
                XTIO_INCRE(xtask, urlnode, URLNODE);
                x = (xtask->urlnodeio.end / sizeof(XTURLNODE)) - 1;
                i = xtask->urlnodeio.old / sizeof(XTURLNODE);
                if(i == 0) i = 1;
                while(x >= i)
                {
                    urlnodelist[x].id = xtask->state->urlnode_id_left;
                    xtask->state->urlnode_id_left = x;
                    --x;
                }
            }
            id = xtask->state->urlnode_id_left;
            if((k = kmap_try_insert(xtask->urlmap, xtask->state->urlmap_root, 
                            key, id, &old)) > 0 && old == 0)
            {
                xtask->state->urlnode_id_left = urlnodelist[id].id;
                urlnodelist[id].urlmap_id = k;
                urlnodelist[id].id = id;
                urlnodelist[id].brother = id;
                urlnodelist[id].nodeid = nodeid;
                urlnodelist[id].parent = parent;
                urlnodelist[id].flag = flag;
                urlnodelist[id].status = XT_STATUS_OK;
                x = sprintf(line, "url:%d", id);
                urlnodelist[id].url_db_id = db_xadd_data(PDB(xtask->mmdb), line, x, url, n);
                DEBUG_LOGGER(xtask->logger, "flag:%d url:%s nurl:%d key:%lld k:%d id:%d", flag, line, n, LL64(key), k, id);
                xtask->state->urlnode_total++;
                if(id > xtask->state->urlnode_id_max) xtask->state->urlnode_id_max = id;
                if(flag & REG_IS_FILE)
                    mmqueue_push(xtask->queue, xtask->state->qfile, id);
                else if(flag & REG_IS_LIST)
                    mmqueue_push(xtask->queue, xtask->state->qlist, id);
                if(urlnodelist[parent].childs_root == 0)
                    urlnodelist[parent].childs_root = vmap_new_tree(xtask->map);
                urlnodelist[id].childs_mid = vmap_try_insert(xtask->map, 
                        urlnodelist[parent].childs_root, id, k, &old);
                urlnodelist[parent].nchilds++;
                urlnodelist[parent].status = XT_STATUS_OK;
            }
            else 
            {
                id = old;
            }
            ret = id;
        }
        MUTEX_UNLOCK(xtask->urlnode_mutex);
        s = strstr(url, "http://") + 7;
        p = line;
        while(*s != '\0' && *s != '/') *p++ = *s++;
        *p = '\0';
        x = xtask_add_host_url(xtask, line, id, flag);
        x = xtask_add_node_url(xtask, nodeid, id);
    }
    return ret;
}

/* get url */
int xtask_get_url(XTASK *xtask, int urlid, char *url)
{
    XTURLNODE *urlnodelist = NULL;
    int ret = -1;

    if(xtask && urlid > 0 && url && (urlnodelist = (XTURLNODE *)(xtask->urlnodeio.map)))
    {
        MUTEX_LOCK(xtask->urlnode_mutex);
        if(urlid <= xtask->state->urlnode_id_max && urlnodelist[urlid].url_db_id > 0)
        {
            ret = db_read_data(PDB(xtask->mmdb), urlnodelist[urlid].url_db_id, url);
        }
        MUTEX_UNLOCK(xtask->urlnode_mutex);
    }
    return ret;
}

/* set urlnode status */
int xtask_set_urlnode_status(XTASK *xtask, int urlid, short err)
{
    XTURLNODE *urlnodelist = NULL;
    int ret = -1;

    if(xtask && urlid > 0 && (urlnodelist = (XTURLNODE *)xtask->urlnodeio.map))
    {
        MUTEX_LOCK(xtask->urlnode_mutex); 
        if(urlid <= xtask->state->urlnode_id_max)
        {
            urlnodelist[urlid].status = err;
            if((err & ERR_NEED_REPEAT))
            {
                if(urlnodelist[urlid].retry < XT_RETRY_MAX)
                {
                    if(urlnodelist[urlid].flag & XT_TASK_FILE)
                        mmqueue_push(xtask->queue, xtask->state->qfile, urlid);
                    else
                        mmqueue_push(xtask->queue, xtask->state->qretry, urlid);
                    urlnodelist[urlid].retry++;
                }
                else
                {
                    WARN_LOGGER(xtask->logger, "urlid:%d download err:%d", urlid, err);
                }
            }
            ret = urlid;
        }
        MUTEX_UNLOCK(xtask->urlnode_mutex); 
    }
    return ret;
}

/* set urlnode data */
int xtask_set_urlnode_data(XTASK *xtask, int urlid, int last_modified, char *data, int ndata)
{
    XTURLNODE *urlnodelist = NULL;
    int ret = -1;

    if(xtask && urlid > 0 && data && ndata > 0 
            &&(urlnodelist = (XTURLNODE *)xtask->urlnodeio.map))
    {
        MUTEX_LOCK(xtask->urlnode_mutex); 
        if(urlid <= xtask->state->urlnode_id_max)
        {
            urlnodelist[urlid].last_modified = last_modified;
            ret = db_set_data(PDB(xtask->db), urlid, data, ndata);
            if(ndata > xtask->state->page_size_max) xtask->state->page_size_max = ndata;
            //mmqueue_push(xtask->queue, xtask->state->qextract, urlid);
        }
        MUTEX_UNLOCK(xtask->urlnode_mutex); 
    }
    return ret;
}

/* xtask pop block */
char *xtask_pop_block(XTASK *xtask)
{
    char *block = NULL;
    int x = 0;

    if(xtask)
    {
        MUTEX_LOCK(xtask->block_mutex);
        if(xtask->nqblocks > 0)
        {
            x = --(xtask->nqblocks);
            block = xtask->qblocks[x];
            xtask->qblocks[x] = NULL;
        }
        else
        {
            block = (char *)xmm_new(XT_DOCUMENT_MAX);
        }
        MUTEX_UNLOCK(xtask->block_mutex);
    }
    return block;
}

/* push block */
void xtask_push_block(XTASK *xtask, char *block)
{
    int x = 0;

    if(xtask && block)
    {
        MUTEX_LOCK(xtask->block_mutex);
        if(xtask->nqblocks < XT_BLOCKS_MAX)
        {
            x = xtask->nqblocks++;
            xtask->qblocks[x] = block;
        }
        else
        {
            xmm_free(block, XT_DOCUMENT_MAX);
        }
        MUTEX_UNLOCK(xtask->block_mutex);
    }
    return ;
}

/* merge record */
int xtask_merge_record(XTASK *xtask, XTRECORD *record)
{
    int ret = -1, i = 0, n = 0, urlid = 0;
    char *mm = NULL, *p = NULL;
    XTRECORD *old = NULL;

    if(xtask && record && (urlid = record->id) > 0 
            && (p = ((char *)record + record->length))
            && (mm = xtask_pop_block(xtask)))
    {
        MM_MUTEX_LOCK(xtask, urlid);
        if((n = db_read_data(PDB(xtask->res), urlid, mm)) > 0)
        {
            old = (XTRECORD *)mm;
            for(i = 0; i < XT_FIELDS_MAX; i++)
            {
                if(record->mms[i].length == 0 && old->mms[i].length > 0)
                {
                    record->mms[i].off = p - (char *)record;
                    DEBUG_LOGGER(xtask->logger, "urlid:%d fieldid:%d flag:%d length:%d", urlid, i, old->mms[i].flag, old->mms[i].length);
                    memcpy(p, mm + old->mms[i].off, old->mms[i].length);
                    p += old->mms[i].length;
                    *p++ = '\0';
                    record->mms[i].length = old->mms[i].length;
                    record->mms[i].flag |= old->mms[i].flag;
                }
            }
        }
        record->length = p - (char *)record;
        DEBUG_LOGGER(xtask->logger, "urlid:%d length:%d", urlid, record->length);
        ret = db_set_data(PDB(xtask->res), urlid, (char *)record, record->length);
        DEBUG_LOGGER(xtask->logger, "urlid:%d length:%d ret:%d", urlid, record->length, ret);
        MM_MUTEX_UNLOCK(xtask, urlid);
        xtask_push_block(xtask, mm);
    }
    return ret;
}

/* convert item to  record */
int xtask_item_to_record(XTASK *xtask, int urlid, XTITEM *item, XTRECORD *record)
{
    int ret = -1, i = 0, k = 0, x = 0, left = 0;
    char *p = NULL, *head = NULL, *pp = NULL; 
    XTLIST lists[XT_FIELDS_MAX];

    if(xtask && urlid > 0 && (head = (char *)item) && record)
    {
        DEBUG_LOGGER(xtask->logger, "urlid:%d nold:%d tableid:%d", urlid, item->length, item->tableid);
        memset(lists, 0, sizeof(XTLIST) * XT_FIELDS_MAX);
        p = head + sizeof(XTITEM);
        for(i = 0; i < item->count; i++)
        {
            if((k = item->regs[i].fieldid) >= 0 && item->regs[i].length > 0)
            {
                //fprintf(stdout, "field:%d length:%d p:%s\n", k, item->regs[i].length, p);
                if((x = lists[k].count) < XT_REGX_MAX)
                {
                    lists[k].mms[x].reg_no = i;
                    lists[k].mms[x].offset = p - head;
                    lists[k].count++;
                }
            }
            p += item->regs[i].length + 1;
        }
        //TO record 
        memset(record, 0, sizeof(XTRECORD));
        p = (char *)record + sizeof(XTRECORD);
        record->id = urlid;
        record->tableid = item->tableid;
        for(i = 0; i < XT_FIELDS_MAX; i++)
        {
            left = XT_DOCUMENT_MAX - (p - (char *)record);
            if(lists[i].count > 0 && lists[i].length < left)
            {
                record->mms[i].off = p - (char *)record; 
                pp = p;
                for(x = 0; x < lists[i].count; x++)
                {
                    if(x > 0 && *(p-1) != 0x20) *p++ = 0x20;
                    k = lists[i].mms[x].reg_no;
                    record->mms[i].flag |= item->regs[k].flag;
                    memcpy(p, (char *)head + lists[i].mms[x].offset, item->regs[k].length);
                    p += item->regs[k].length;
                    lists[i].length += item->regs[k].length + 1;
                }
                record->mms[i].length = p - pp;
                *p++ = '\0';
            }
        }
        record->length = p - (char *)record;
        ret = xtask_merge_record(xtask, record);
    }
    return ret;
}

/* convert res to  record */
int xtask_res_to_record(XTASK *xtask, XTURLNODE *urlnode, XTRES *res, XTRECORD *record)
{
    int ret = -1, i = 0, k = 0, urlid = 0, x = 0, tableid = 0, 
        parent = 0, id = 0, left = 0;
    char *p = NULL, *head = NULL, *tail = NULL, *pp = NULL; 
    XTLIST lists[XT_FIELDS_MAX];
    XTITEM *item = NULL;

    if(xtask && (head = (char *)res) && (urlid = res->id) > 0
            && urlnode && record) 
    {
        memset(lists, 0, sizeof(XTLIST) * XT_FIELDS_MAX);
        p = head + sizeof(XTRES);
        tail = head + res->length;
        while(p < tail)
        {
            item = (XTITEM *)p;
            if(tableid == 0 && item->tableid > 0) tableid = item->tableid;
            //fprintf(stdout, "id:%d count:%d length:%d\n", res->id, item->count, item->length);
            p += sizeof(XTITEM);
            x = 0;
            while(x < item->count)
            {
                if(item->regs[x].flag & REG_IS_LINK)
                {
                    parent = xtask_check_parent(xtask, urlid, item->regs[x].nodeid);
                    id = xtask_add_url(xtask, parent,item->regs[x].nodeid,p,item->regs[x].flag); 
                    if((item->regs[x].flag & REG_IS_PAGE) && urlnode->brother > 0
                            && item->regs[x].nodeid == urlnode->nodeid)
                    {
                        xtask_set_brother(xtask, id, res->id);
                    }
                }
                if((k = item->regs[x].fieldid) >= 0 && item->regs[x].length > 0)
                {
                    if((i = lists[k].count) < XT_REGX_MAX)
                    {
                        lists[k].mms[i].item_no = (char *)item - (char *)head;
                        lists[k].mms[i].reg_no = x;
                        lists[k].mms[i].offset = p - head;
                        lists[k].count++;
                    }
                    lists[k].length += item->regs[x].length + 1;
                }
                p += item->regs[x].length + 1;
                ++x;
            }
        }
        //TO record 
        memset(record, 0, sizeof(XTRECORD));
        p = (char *)record + sizeof(XTRECORD);
        record->id = urlid;
        record->tableid = tableid;
        for(i = 0; i < XT_FIELDS_MAX; i++)
        {
            left = XT_DOCUMENT_MAX - (p - (char *)record);
            if(lists[i].count > 0 && lists[i].length < left)
            {
                record->mms[i].off = p - (char *)record; 
                pp = p;
                for(x = 0; x < lists[i].count; x++)
                {
                    if(x > 0 && *(p-1) != 0x20) *p++ = 0x20;
                    item = (XTITEM *)(head + lists[i].mms[x].item_no);
                    k = lists[i].mms[x].reg_no;
                    record->mms[i].flag |= item->regs[k].flag;
                    memcpy(p, head + lists[i].mms[x].offset, item->regs[k].length);
                    p += item->regs[k].length;
                }
                record->mms[i].length = p - pp;
                *p++ = '\0';
            }
        }
        record->length = p - (char *)record;
        if(record->tableid > 0 && record->length > 0)
        {
            ret = xtask_merge_record(xtask, record);
        }
        else
        {
            WARN_LOGGER(xtask->logger, "urlid:%d tableid:%d length:%d", record->id, record->tableid, record->length);
        }
    }
    return ret;
}

/* set urlnode res */
int xtask_update_record(XTASK *xtask, int urlid, char *data, int ndata)
{
    char *block = NULL, *mm = NULL, *p = NULL, *pp = NULL;
    XTRECORD *record = NULL, *rec = NULL;
    int ret = -1, i = 0, k = 0, n = 0;
    XTURLNODE *urlnodelist = NULL;
    XTITEM *item = NULL;

    if(xtask && urlid > 0 && (pp = data) && (item = (XTITEM *)data) && ndata > 0 
            && (urlnodelist = (XTURLNODE *)xtask->urlnodeio.map) 
            && (block = xtask_pop_block(xtask)))
    {
        MM_MUTEX_LOCK(xtask, urlid);
        //MUTEX_LOCK(xtask->record_mutex); 
        if(urlid <= xtask->state->urlnode_id_max && (mm = xtask_pop_block(xtask)))
        {
            record = (XTRECORD *)block;
            memset(block, 0, sizeof(XTRECORD));
            p = (char *)block + sizeof(XTRECORD);
            record->id = urlid;
            record->tableid = item->tableid;
            pp += sizeof(XTITEM);
            DEBUG_LOGGER(xtask->logger, "urlid:%d nold:%d tableid:%d", urlid, n, item->tableid);
            for(i = 0; i < item->count; i++)
            {
                if((k = item->regs[i].fieldid) >= 0 && item->regs[i].length > 0)
                {
                    record->mms[k].off = p - block;
                    record->mms[k].flag |= item->regs[i].flag;
                    DEBUG_LOGGER(xtask->logger, "urlid:%d i:%d fieldid:%d flag:%d length:%d", urlid, i, k, item->regs[i].flag, item->regs[i].length);
                    memcpy(p, pp, item->regs[i].length);
                    p += item->regs[i].length;
                    *p++ = '\0';
                    record->mms[k].length = item->regs[i].length;
                }
                pp += item->regs[i].length + 1;
            }
            if((n = db_read_data(PDB(xtask->res), urlid, mm)) > 0)
            {
                rec = (XTRECORD *)mm;
                for(i = 0; i < XT_FIELDS_MAX; i++)
                {
                    if(record->mms[i].length == 0 && rec->mms[i].length > 0)
                    {
                        record->mms[i].off = p - block;
                        DEBUG_LOGGER(xtask->logger, "urlid:%d i:%d fieldid:%d flag:%d length:%d", urlid, i, k, rec->mms[i].flag, rec->mms[i].length);
                        memcpy(p, mm + rec->mms[i].off, rec->mms[i].length);
                        p += rec->mms[i].length;
                        *p++ = '\0';
                        record->mms[i].length = rec->mms[i].length;
                        record->mms[i].flag |= rec->mms[i].flag;
                    }
                }
            }
            record->length = p - block;
            DEBUG_LOGGER(xtask->logger, "urlid:%d length:%d", urlid, record->length);
            ret = db_set_data(PDB(xtask->res), urlid, block, record->length);
            DEBUG_LOGGER(xtask->logger, "urlid:%d length:%d ret:%d", urlid, record->length, ret);
        }
        MM_MUTEX_UNLOCK(xtask, urlid);
        if(block)xtask_push_block(xtask, block);
        if(mm)xtask_push_block(xtask, mm);
    }
    return ret;
}

/* get record */
int xtask_get_record(XTASK *xtask, int urlid, char *mm)
{
    int n = 0, i = 0, ret = -1, parentid = 0;
    XTRECORD *record = NULL, *rec = NULL;
    char *p = NULL, *m = NULL;
    XTURLNODE urlnode = {0};

    if(xtask && urlid > 0 && mm && (m = xtask_pop_block(xtask)))
    {
        record = (XTRECORD *)mm;
        if(xtask_get_urlnode(xtask, urlid, &urlnode) > 0  
                && (n = db_read_data(PDB(xtask->res), urlid, mm)) > 0)
        {
            p = mm + record->length;
            while((parentid = urlnode.parent) > 0 && xtask_get_urlnode(xtask, parentid, &urlnode)>0)
            {
                rec = (XTRECORD *)m;
                if(db_read_data(PDB(xtask->res), urlnode.brother, m) > 0)
                {
                    for(i = 0; i < XT_FIELDS_MAX; i++) 
                    {
                        if(record->mms[i].length == 0 && rec->mms[i].length > 0)
                        {
                            //DEBUG_LOGGER(xtask->logger, "urlid:%d field:%s => %s", urlid, table.fields[i].name, m + rec->mms[i].off);
                            record->mms[i].off = p - mm;
                            memcpy(p, m + rec->mms[i].off, rec->mms[i].length);
                            p += rec->mms[i].length;
                            *p++ = 0;
                            record->mms[i].length = rec->mms[i].length;
                        }
                    }
                }
            }
            record->length = p - mm;
            ret = urlid;
        }
        xtask_push_block(xtask, m);
    }
    return ret;
}

/* view record */
int xtask_view_record(XTASK *xtask, int urlid, char *data, int ndata)
{
    char *mm = NULL, *m = NULL, *p = NULL, *pp = NULL;
    XTRECORD *record = NULL;
    int ret = -1, i = 0;
    XTTABLE table = {0};

    if(xtask && urlid > 0 && (p = data) && ndata > 0 && (mm = xtask_pop_block(xtask)))
    {
        record = (XTRECORD *)mm;
        memset(mm, 0, sizeof(XTRECORD));
        if((m = xtask_pop_block(xtask)) && xtask_get_record(xtask, urlid, mm) > 0 
                && record->length > 0 && xtask_get_table(xtask, record->tableid, &table) > 0)
        {
            p += sprintf(p, "({'id':'%d', 'nfields':'%d', 'fields':{", urlid, table.nfields);
            pp = p;
            for(i = 0; i < XT_FIELDS_MAX; i++)
            {
                if(table.fields[i].id > 0)
                {
                    p += sprintf(p, "'%s':'", table.fields[i].name);
                    if(record->mms[i].length > 0) 
                    {
                        p += base64_encode(p, (const unsigned char *)
                                (mm +record->mms[i].off) , record->mms[i].length);
                    }
                    *p++ = '\'';
                    *p++ = ',';
                }
            }
            if(p != pp)--p;
            p += sprintf(p, "}})");
            ret = p - data;
        }
        if(mm) xtask_push_block(xtask, mm);
        if(m)xtask_push_block(xtask, m);
    }
    return ret; 
}

/* set task log level */
void xtask_set_log_level(XTASK *xtask, int log_level)
{
    if(xtask)
    {
        LOGGER_SET_LEVEL(xtask->logger, log_level);
    }
    return ;
}

/* set dump dir */
void xtask_set_dumpdir(XTASK *xtask, char *dir)
{

    if(xtask && dir)
    {
        strcpy(xtask->state->dumpdir, dir);
    }
    return ;
}

/* output csv file */
int xtask_append_csv(XTASK *xtask, int id, char *data, int ndata)
{
    char path[XT_PATH_MAX], *p = NULL, *txt = NULL, *mm = NULL;
    int ret = -1, i = 0, n = 0;
    FHEADER *fheader = NULL;
    XTTABLE *tables = NULL;
    IFIELD *fields = NULL;
    struct stat st = {0};
    
    if(xtask && (fheader = (FHEADER *)data) && ndata > 0 
            && id > 0 && (tables = (XTTABLE *)(xtask->tableio.map))
            && (mm = xtask_pop_block(xtask)))
    {
        fields = (IFIELD *)(data + sizeof(FHEADER));
        txt = (char *)fields + sizeof(IFIELD) * fheader->nfields;
        MUTEX_LOCK(xtask->csv_mutex);
        if(xtask->outs[id].csv <= 0)
        {
            p = path;
            p += sprintf(p, "%s/%s/%s.csv", xtask->state->dumpdir, 
                    tables[id].name, tables[id].name);
            xtask_pmkdir(path);
            if((xtask->outs[id].csv = open(path, O_CREAT|O_WRONLY|O_APPEND, 0644)) <= 0)
            {
                FATAL_LOGGER(xtask->logger, "open csv file[%s] failed, %s", path, strerror(errno));
            }
        }
        if(xtask->outs[id].csv > 0) fstat(xtask->outs[id].csv, &st);
        if(xtask->outs[id].csv > 0) 
        {
            if(st.st_size == 0)
            {
                p = mm;
                for(i = 0; i < XT_FIELDS_MAX; i++)
                {
                    if(tables[id].fields[i].id > 0)
                    {
                        if(n > 0) *p++ = ',';
                        p += sprintf(p, "%s", tables[id].fields[i].name);
                        n++;
                    }
                }
                *p++ = '\n';;
                //header
                ret = write(xtask->outs[id].csv, mm, (p - mm));
            }
            p = mm;
            for(i = 0; i < fheader->nfields; i++)
            {
                if(i > 0) *p++ = ',';
                if(fields[i].length > 0)
                {
                    p += base64_encode(p, (const unsigned char *)(txt + fields[i].offset),
                            fields[i].length);
                }
            }
            *p++ = '\n';;
            //line
            ret = write(xtask->outs[id].csv, mm, (p - mm));
        }
        MUTEX_UNLOCK(xtask->csv_mutex);
        xtask_push_block(xtask, mm);
    }
    return ret;
}

/* append dump */
int xtask_append_dump(XTASK *xtask, int id, char *data, int ndata)
{
    char path[XT_PATH_MAX], *p = NULL;
    FHEADER *fheader = NULL;
    struct stat st = {0};
    int ret = -1, n = 0;
    XTTABLE *tables = NULL;
    
    if(xtask && data && ndata > 0 && id > 0 && (tables = (XTTABLE *)(xtask->tableio.map)))
    {
        MUTEX_LOCK(xtask->dump_mutex);
        if(xtask->outs[id].fd > 0) fstat(xtask->outs[id].fd, &st);
        if(st.st_size > XT_DUMP_MAX)
        {
            p = path + sizeof(FHEADER);
            fheader = (FHEADER *)path;
            fheader->size = n;
            fheader->flag = IB_DUMP_SET; 
            n = fheader->size = sprintf(p, "%s/%s/%d.doc",  
                xtask->state->dumpdir, tables[id].name, ++(tables[id].dump_id_max));
            n = write(xtask->outs[id].fd, path, sizeof(FHEADER) + n);
            close(xtask->outs[id].fd);
            xtask->outs[id].fd = 0;
        }
        if(xtask->outs[id].fd <= 0)
        {
            p = path;
            p += sprintf(p, "%s/%s/%d.doc", xtask->state->dumpdir, 
                    tables[id].name, tables[id].dump_id_max);
            xtask_pmkdir(path);
            if((xtask->outs[id].fd = open(path, O_CREAT|O_WRONLY|O_APPEND, 0644)) <= 0)
            {
                FATAL_LOGGER(xtask->logger, "open dump file[%s] failed, %s", path, strerror(errno));
            }
        }
        if(xtask->outs[id].fd > 0) 
        {
            ret = write(xtask->outs[id].fd, data, ndata);
        }
        MUTEX_UNLOCK(xtask->dump_mutex);
    }
    return ret;
}

#define OVER_DUMP_STATE(xtask,ret)                          \
do                                                          \
{                                                           \
    MUTEX_LOCK(xtask->dump_mutex);                          \
    if(ret > 0)xtask->state->over_dump++;                   \
    else xtask->state->err_dump++;                          \
    MUTEX_UNLOCK(xtask->dump_mutex);                        \
}while(0)
/* dump record */
int xtask_dump(XTASK *xtask)
{
    char *block = NULL, *mm = NULL, *s = NULL, *p = NULL, *pp = NULL, *ps = NULL;
    int ret = -1, id = 0, i = 0, x = 0, k = 0, n = 0, flag = 0, nrecord = 0, tableid = 0;
    XTRECORD *record = NULL;
    FHEADER *fheader = NULL;
    IFIELD *fields = NULL;
    XTTABLE table = {0};
    struct tm tm = {0};
    time_t dtime = 0;

    if(xtask)
    {
        if((block = xtask_pop_block(xtask)) && (mm = xtask_pop_block(xtask)))
        {
            fheader = (FHEADER *)block;
            while(x < XT_DUMP_TASK_MAX && mmqueue_pop(xtask->queue, xtask->state->qdump, &id) > 0)
            {
                record = (XTRECORD *)mm;
                memset(record, 0, sizeof(XTRECORD));
                tableid = 0;
                nrecord = 0;
                if((nrecord = xtask_get_record(xtask, id, mm)) > 0 
                        && (tableid = xtask_get_table(xtask, record->tableid, &table)) > 0)
                {
                    mm[record->length] = 0;
                    memset(block, 0, sizeof(FHEADER));
                    fields = (IFIELD *)(block + sizeof(FHEADER));
                    ps = p = (char *)fields + sizeof(IFIELD) * table.nfields;
                    memset(fields, 0, sizeof(IFIELD) * table.nfields);
                    DEBUG_LOGGER(xtask->logger, "id:%d length:%d", id, record->length);
                    fheader->nfields = table.nfields;
                    fheader->globalid = id;
                    fheader->category = 1;
                    fheader->rank = 1.0;
                    k = 0;i = 0;
                    while(i < XT_FIELDS_MAX) 
                    {
                        pp = p;
                        if(table.fields[i].id > 0)
                        {
                            DEBUG_LOGGER(xtask->logger, "id:%d length:%d", id, record->length);
                            flag = 0;
                            fields[k].offset = p - ps;
                            if(i >= XT_TEXT_FROM && i < XT_INT_FROM) 
                                flag = (IB_DATATYPE_TEXT|IB_IS_NEED_INDEX);
                            else if(i >= XT_INT_FROM && i < XT_LONG_FROM) 
                                flag = (IB_DATATYPE_INT|IB_IS_NEED_INDEX|IB_IS_NEED_UPDATE);
                            else if(i >= XT_LONG_FROM && i < XT_DOUBLE_FROM) 
                                flag = (IB_DATATYPE_LONG|IB_IS_NEED_INDEX|IB_IS_NEED_UPDATE);
                            else if(i >= XT_DOUBLE_FROM && i < XT_DISPLAY_FROM) 
                                flag = (IB_DATATYPE_DOUBLE|IB_IS_NEED_INDEX|IB_IS_NEED_UPDATE);
                            else 
                                flag = IB_DATATYPE_TEXT;
                            fields[k].flag = flag;
                            if(record->mms[i].length > 0)
                            {
                                s = mm + record->mms[i].off;
                                if(record->mms[i].flag & REG_IS_DATETIME)
                                {
                                    memset(&tm, 0, sizeof(struct tm));
                                    dtime = str2time(s);
                                    p += sprintf(p, "%u", (unsigned int)dtime);
                                }
                                else
                                {
                                    p += sprintf(p, "%s", s);
                                }
                            }
                            else
                            {
                                if(flag & IB_DATATYPE_NUMBRIC) *p++ = '0';
                                else *p++ = 0x20;
                            }
                            *p++ = '\n';
                            *p = '\0';
                            fields[k].length = p - pp;
                            //DEBUG_LOGGER(xtask->logger, "table:%s field:%s k:%d length:%d pp:%s", table.name, table.fields[i].name, k, fields[k].length, pp);
                            ++k;
                            ++i;
                        }
                        else
                        {
                            if(i>= XT_TEXT_FROM && i < XT_INT_FROM)
                            {
                                if((p - ps) > 0) 
                                    fheader->crc = (int)http_crc32((unsigned char *)ps, p - ps);
                                i = XT_INT_FROM;
                            }
                            else if(i>= XT_INT_FROM && i < XT_LONG_FROM) i = XT_LONG_FROM;
                            else if(i>= XT_LONG_FROM && i < XT_DOUBLE_FROM) i = XT_DOUBLE_FROM;
                            else if(i>= XT_DOUBLE_FROM && i < XT_DISPLAY_FROM) i = XT_DISPLAY_FROM;
                            else if(i >= XT_DISPLAY_FROM) break;
                            DEBUG_LOGGER(xtask->logger, "id:%d length:%d", id, record->length);
                        }
                    }
                    //fprintf(stdout, "content:%s \r\n", ps);
                    fheader->size = p - (char *)fields;
                    n = p - block;
                    if((p - ps) > 0)
                    {
                        ret = xtask_append_dump(xtask, record->tableid, block, n);
                        ret |= xtask_append_csv(xtask, record->tableid, block, n);
                    }
                    else
                    {
                        WARN_LOGGER(xtask->logger, "urlid:%d no_content", id);
                    }
                    OVER_DUMP_STATE(xtask, ret);
                    if(ret < 0) 
                    {
                        goto fail;
                    }
                    ++x;
                    DEBUG_LOGGER(xtask->logger, "x:%d urlid:%d fheader->size:%d qdump left:%d", x, id, fheader->size, mmqueue_total(xtask->queue, xtask->state->qdump));
                    continue;
                }
fail:
                WARN_LOGGER(xtask->logger, "dump urlid:%d record->id:%d record->tableid:%d/(%d) nrecord:%d failed, ret:%d", id, record->id, record->tableid, tableid, nrecord, ret);
                mmqueue_push(xtask->queue, xtask->state->qdump, id);
                break;
            }
            ret = x;
        }
        if(mm)xtask_push_block(xtask, mm);
        if(block)xtask_push_block(xtask, block);
    }
    return ret;
}

/* remove urlurlnode */
int xtask_remove_urlnode(XTASK *xtask, int urlnodeid)
{
    int ret = -1, x = 0, k = 0, id = 0;
    XTURLNODE *urlnodelist = NULL;

    if(xtask && urlnodeid > 0 && urlnodeid <= xtask->state->urlnode_id_max 
            && (urlnodelist = (XTURLNODE *)(xtask->urlnodeio.map)))
    {
        if(urlnodelist[urlnodeid].nchilds > 0 &&  urlnodelist[urlnodeid].childs_root > 0
            && (x = vmap_min(xtask->map, urlnodelist[urlnodeid].childs_root, &id, &k)) > 0)
        {
            do
            {
                if(urlnodelist[id].nchilds > 0)
                    xtask_remove_urlnode(xtask, id);
                else
                {
                    db_del_data(PDB(xtask->mmdb), urlnodelist[id].url_db_id);
                    kmap_remove(xtask->urlmap, xtask->state->urlmap_root, 
                            urlnodelist[id].urlmap_id, NULL, NULL);
                    memset(&(urlnodelist[id]), 0, sizeof(XTURLNODE));
                    urlnodelist[id].id = xtask->state->urlnode_id_left;
                    xtask->state->urlnode_id_left = id;
                    xtask->state->urlnode_total--;
                }
            }while((x = vmap_next(xtask->map, urlnodelist[urlnodeid].childs_root, x, &id, &k)) > 0);
        }
        vmap_remove_tree(xtask->map, urlnodelist[urlnodeid].childs_root);
        db_del_data(PDB(xtask->mmdb), urlnodelist[urlnodeid].url_db_id);
        kmap_remove(xtask->urlmap, xtask->state->urlmap_root, 
                urlnodelist[urlnodeid].urlmap_id, NULL, NULL);
        memset(&(urlnodelist[urlnodeid]), 0, sizeof(XTURLNODE));
        urlnodelist[urlnodeid].id = xtask->state->urlnode_id_left;
        xtask->state->urlnode_id_left = urlnodeid;
        xtask->state->urlnode_total--;
        ret = urlnodeid;
    }
    return ret;
}

/* delete urlnode */
int xtask_delete_urlnode(XTASK *xtask, int urlnodeid)
{
    int ret = -1, k = 0, hostid = 0, nodeid = 0, parent = 0;
    XTURLNODE *urlnodelist = NULL;

    if(xtask && urlnodeid >= 0  && (urlnodelist = (XTURLNODE *)(xtask->urlnodeio.map))) 
    {
        MUTEX_LOCK(xtask->urlnode_mutex);
        if(urlnodeid != 0 && (parent = urlnodelist[urlnodeid].parent) > 0)
        {
            vmap_remove(xtask->map, urlnodelist[parent].childs_root, 
                    urlnodelist[urlnodeid].childs_mid, NULL, NULL);
            kmap_remove(xtask->urlmap, xtask->state->urlmap_root,
                    urlnodelist[urlnodeid].urlmap_id, NULL, NULL);
            urlnodelist[parent].nchilds--;
        }
        hostid = urlnodelist[urlnodeid].hostid;
        nodeid = urlnodelist[urlnodeid].nodeid;
        ret = xtask_remove_urlnode(xtask, urlnodeid);
        ret = parent;
        MUTEX_UNLOCK(xtask->urlnode_mutex);
        k = xtask_remove_host_url(xtask, hostid, urlnodeid);
        k = xtask_remove_node_url(xtask, nodeid, urlnodeid);
    }
    return ret;
}

/* get urlnode */
int xtask_get_urlnode(XTASK *xtask, int urlnodeid, XTURLNODE *urlnode)
{
    XTURLNODE *urlnodelist = NULL;
    int ret = -1;

    if(xtask && urlnodeid > 0 && urlnode 
            && (urlnodelist = (XTURLNODE *)(xtask->urlnodeio.map))) 
    {
        MUTEX_LOCK(xtask->urlnode_mutex);
        if(urlnodeid <= xtask->state->urlnode_id_max)
        {
            memcpy(urlnode, &(urlnodelist[urlnodeid]), sizeof(XTURLNODE));
            ret = urlnodeid;
        }
        MUTEX_UNLOCK(xtask->urlnode_mutex);
    }
    return ret;
}

/* find parents uniqe id */
int xtask_parent_is_unique(XTASK *xtask, int urlnodeid, int nodeid)
{
    XTURLNODE *urlnodelist = NULL;
    XTNODE *nodelist = NULL;
    int ret = -1, id = 0;

    if(xtask && urlnodeid > 0 && nodeid > 0 && (nodelist = (XTNODE *)(xtask->nodeio.map)) 
            && (urlnodelist = (XTURLNODE *)(xtask->urlnodeio.map))) 
    {
        MUTEX_LOCK(xtask->node_mutex);
        MUTEX_LOCK(xtask->urlnode_mutex);
        if(nodeid <= xtask->state->node_id_max && urlnodeid <= xtask->state->urlnode_id_max)
        {
            if(urlnodelist[urlnodeid].nodeid == nodelist[nodeid].parent)
                ret = urlnodeid;
            else
            {
                id = urlnodeid;
                while(id > 0 && urlnodelist[id].nodeid != nodeid) 
                    id = urlnodelist[id].parent;
                if(id > 0 && urlnodelist[id].nodeid == nodeid)
                    ret = urlnodelist[id].parent;
            }
        }
        MUTEX_UNLOCK(xtask->urlnode_mutex);
        MUTEX_UNLOCK(xtask->node_mutex);
    }
    return ret;
}
/* find parent */
int xtask_check_parent(XTASK *xtask, int urlnodeid, int nodeid)
{
    XTURLNODE *urlnodelist = NULL;
    XTNODE *nodelist = NULL;
    int ret = -1, id = 0;

    if(xtask && urlnodeid > 0 && nodeid > 0 && (nodelist = (XTNODE *)(xtask->nodeio.map)) 
            && (urlnodelist = (XTURLNODE *)(xtask->urlnodeio.map))) 
    {
        MUTEX_LOCK(xtask->node_mutex);
        MUTEX_LOCK(xtask->urlnode_mutex);
        if(nodeid <= xtask->state->node_id_max && urlnodeid <= xtask->state->urlnode_id_max)
        {
            if(urlnodelist[urlnodeid].nodeid == nodelist[nodeid].parent)
                ret = urlnodeid;
            else
            {
                id = urlnodeid;
                while(id > 0 && urlnodelist[id].nodeid != nodeid) 
                    id = urlnodelist[id].parent;
                if(id > 0 && urlnodelist[id].nodeid == nodeid)
                    ret = urlnodelist[id].parent;
            }
        }
        MUTEX_UNLOCK(xtask->urlnode_mutex);
        MUTEX_UNLOCK(xtask->node_mutex);
    }
    return ret;
}

/* set brother */
int xtask_set_brother(XTASK *xtask, int urlnodeid, int brother)
{
    XTURLNODE *urlnodelist = NULL;
    int ret = -1;

    if(xtask && urlnodeid > 0 && urlnodeid <= xtask->state->urlnode_id_max 
            && (urlnodelist = (XTURLNODE *)(xtask->urlnodeio.map)))
    {
        MUTEX_LOCK(xtask->mutex);
        if(brother > 0 && brother <= xtask->state->urlnode_id_max 
                && urlnodelist[brother].nodeid == urlnodelist[urlnodeid].nodeid)
        {
            urlnodelist[urlnodeid].brother = urlnodelist[brother].brother;
            ret = urlnodeid;
        }
        MUTEX_UNLOCK(xtask->mutex);
    }
    return ret;
}

/* view urlurlnode childs */
int xtask_view_urlnodes(XTASK *xtask, int urlnodeid, char *out, int nout, int from, int to)
{
    int ret = -1, x = 0, k = 0, id = 0, i = 0, n = 0,
        npages = 0, parent = 0, ppnodeid = 0;
    XTURLNODE *urlnodelist = NULL;
    char *p = NULL, *end = NULL;

    if(xtask && urlnodeid > 0 && urlnodeid <= xtask->state->urlnode_id_max 
            && (p = out) > 0 && (end = (p + nout)) > out
            && (urlnodelist = (XTURLNODE *)(xtask->urlnodeio.map)))
    {
        MUTEX_LOCK(xtask->urlnode_mutex);
        if(urlnodelist[urlnodeid].nchilds > from &&  urlnodelist[urlnodeid].childs_root > 0
                && (x = vmap_min(xtask->map, urlnodelist[urlnodeid].childs_root, &id, &k)) > 0)
        {
            npages = (urlnodelist[urlnodeid].nchilds / XT_PAGE_NUM) 
                + (((urlnodelist[urlnodeid].nchilds % XT_PAGE_NUM)) > 0);
            if((parent = urlnodelist[urlnodeid].parent) == 0)
                ppnodeid = urlnodelist[urlnodeid].nodeid;
            p += sprintf(p, "({'id':'%d','parent':'%d','ppnode':'%d','nchilds':'%d','pages':'%d', 'childs':{", urlnodeid, parent, ppnodeid, urlnodelist[urlnodeid].nchilds, npages);
            do
            {
                if(i >= from && i < to)
                {
                    p += sprintf(p, "'%d':{'id':'%d','nchilds':'%d','level':'%d',"
                        "'nodeid':'%d','url':'", id, id, urlnodelist[id].nchilds, 
                        urlnodelist[id].level,  urlnodelist[id].nodeid);
                    if((n = db_read_data(PDB(xtask->mmdb), urlnodelist[id].url_db_id, p)) > 0)
                        p += n;
                    p += sprintf(p, "'},");
                }
                ++i;
            }while(i<to&&(x=vmap_next(xtask->map,urlnodelist[urlnodeid].childs_root,x,&id,&k))>0);
            --p;
            p += sprintf(p, "}})");
        }
        MUTEX_UNLOCK(xtask->urlnode_mutex);
        ret = p - out;
    }
    return ret;
}

/* new url task */
int xtask_new_task(XTASK *xtask, int urlid, char *out, int nout)
{
    char *p = NULL, *end = NULL, *ps = NULL, *host = NULL, *pp = NULL;
    int ret = -1, n = 0, hostid = 0, parent = 0;
    XTURLNODE *urlnodelist = NULL;
    struct hostent *hp = NULL;
    XTHOST *hostlist = NULL;
    XTMETA *meta = NULL;

    if(xtask && urlid > 0 && (p = out) && nout > HTTP_HEADERS_MAX 
            && (end = (out + nout)) > p && (hostlist = (XTHOST *)(xtask->hostio.map)) 
            && (urlnodelist = (XTURLNODE *)(xtask->urlnodeio.map))) 
    {
        meta = (XTMETA *)p; p += sizeof(XTMETA);
        meta->id = urlid;
        DEBUG_LOGGER(xtask->logger, "read-urlid:%d url", urlid);
        meta->nurl = db_read_data(PDB(xtask->mmdb), urlnodelist[urlid].url_db_id, p);
        DEBUG_LOGGER(xtask->logger, "over-read-urlid:%d url", urlid);
        meta->proxy_ip = 0;meta->proxy_port = 0;
        hostid = urlnodelist[urlid].hostid;
        DEBUG_LOGGER(xtask->logger, "read-urlid:%d hostid:%d", urlid, hostid);
        meta->ip = xtask_host_ip(xtask, hostid);
        DEBUG_LOGGER(xtask->logger, "over-read-urlid:%d hostid:%d", urlid, hostid);
        meta->nheaders = 0;
        p += meta->nurl;
        ps = p;
        p += sprintf(p, "Host: ");
        host = p;
        DEBUG_LOGGER(xtask->logger, "read-urlid:%d hostid:%d", urlid, hostid);
        if((n = db_read_data(PDB(xtask->mmdb), hostlist[hostid].db_host_id, p)) > 0)
            p += n;
        *p = '\0';
        DEBUG_LOGGER(xtask->logger, "over-read-urlid:%d hostid:%d host:%s", urlid, hostid, host);
        if(meta->ip == 0)
        {
            if((hp = gethostbyname(host)))
                meta->ip = inet_addr(inet_ntoa(*((struct in_addr *)(hp->h_addr))));
            WARN_LOGGER(xtask->logger, "over-read-urlid:%d hostid:%d gethostbyname(%s) => %d", urlid, hostid, host, meta->ip);
        }
        if(hostlist[hostid].db_cookie_id > 0)
        {
            pp = p;
            p += sprintf(p, "\r\nCookie: ");
            p += xtask_get_host_cookie(xtask, hostid, p);
            /*
            if((n = db_read_data(PDB(xtask->mmdb), hostlist[hostid].db_cookie_id, p)) > 0)
                p += n;
            else
                p = pp;
            */
        }
        if((parent = urlnodelist[urlid].parent) > 0 && urlnodelist[parent].url_db_id > 0)
        {
            pp = p;
            p += sprintf(p, "\r\nReferer: ");
            DEBUG_LOGGER(xtask->logger, "read-urlid:%d hostid:%d refer", urlid, hostid);
            if((n = db_read_data(PDB(xtask->mmdb), urlnodelist[parent].url_db_id, p)) > 0)
                p += n;
            else p = pp;
            DEBUG_LOGGER(xtask->logger, "over-read-urlid:%d hostid:%d refer", urlid, hostid);
        }
        if(urlnodelist[urlid].last_modified)
        {
            p += sprintf(p, "\r\nIf-Modified-Since: ");
            p += GMTstrdate(urlnodelist[urlid].last_modified, p);
        }
        MUTEX_LOCK(xtask->mutex);
        p += sprintf(p, "\r\n%s", xtask->state->http_headers);
        MUTEX_UNLOCK(xtask->mutex);
        if(*(p-1) != '\n'){*p++ = '\r'; *p++ = '\n'; *p = '\0';}
                //"Accept-Language: %s\r\nAccept-Encoding: %s\r\n"
                //"Accept-Charset: %s\r\nConnection: close\r\n",
                //HTTP_USER_AGENT, HTTP_ACCEPT_TYPE, HTTP_ACCEPT_LANGUAGE,
                //HTTP_ACCEPT_ENCODING, HTTP_ACCEPT_CHARSET);
        /*
        p += sprintf(p, "\r\nUser-Agent: %s\r\nAccept: %s\r\n"
                "Accept-Language: %s\r\n"
                "Accept-Charset: %s\r\nConnection: close\r\n",
                HTTP_USER_AGENT, HTTP_ACCEPT_TYPE, HTTP_ACCEPT_LANGUAGE,
                HTTP_ACCEPT_CHARSET);
        */
        meta->nheaders = p - ps;
        ret = p - out;
    }
    return ret;
}

/* new download task */
int xtask_download_task(XTASK *xtask, int flag, char *out, int nout)
{
    int ret = -1, urlid = 0, k = 0, n = 0, left = 0, hostid = 0;
    XTURLNODE *urlnodelist = NULL;
    char *p = NULL, *end = NULL;// *pp = NULL;
    XTHOST *hostlist = NULL;

    if(xtask && (p = out) && nout > HTTP_HEADERS_MAX && (end = (out + nout)) > p 
        && (urlnodelist = (XTURLNODE *)(xtask->urlnodeio.map)) 
        && (hostlist = (XTHOST *)(xtask->hostio.map))) 
    {
        left = nout;
        //MUTEX_LOCK(xtask->urlnode_mutex);
        if(flag & XT_TASK_FILE)
        {
            while(left > HTTP_HEADERS_MAX && k < HTTP_TASK_MAX && mmqueue_pop(xtask->queue, 
                        xtask->state->qfile, &urlid) > 0 && urlid > 0)
            {
                DEBUG_LOGGER(xtask->logger, "file:%d", urlid);
                if((n = xtask_new_task(xtask, urlid, p, left)) > 0)
                {
                    p += n;
                    ++k;
                }
                else
                {
                    mmqueue_push(xtask->queue, xtask->state->qfile, urlid);
                    break;
                }
                left = nout - (p - out);
            }
        }
        else if(flag & XT_TASK_REPEAT)
        {
            while(left > HTTP_HEADERS_MAX && k < HTTP_TASK_MAX && mmqueue_pop(xtask->queue, 
                        xtask->state->qretry, &urlid) > 0 && urlid > 0)
            {
                DEBUG_LOGGER(xtask->logger, "retry:%d", urlid);
                if((n = xtask_new_task(xtask, urlid, p, left)) > 0)
                {
                    p += n;
                    ++k;
                }
                else
                {
                    mmqueue_push(xtask->queue, xtask->state->qretry, urlid);
                    break;
                }
                left = nout - (p - out);
            }
        }
        else if(flag & XT_TASK_UPDATE)
        {
            while(left > HTTP_HEADERS_MAX && k < HTTP_TASK_MAX && mmqueue_pop(xtask->queue, 
                        xtask->state->qupdate, &urlid) > 0 && urlid > 0)
            {
                DEBUG_LOGGER(xtask->logger, "update:%d", urlid);
                if((n = xtask_new_task(xtask, urlid, p, left)) > 0)
                {
                    p += n;
                    ++k;
                }
                else
                {
                    mmqueue_push(xtask->queue, xtask->state->qupdate, urlid);
                    break;
                }
                left = nout - (p - out);
            }
        }
        else if(flag & XT_TASK_LIST)
        {
            while(left > HTTP_HEADERS_MAX && k < HTTP_TASK_MAX && mmqueue_pop(xtask->queue, 
                        xtask->state->qlist, &urlid) > 0 && urlid > 0)
            {
                DEBUG_LOGGER(xtask->logger, "qlist:%d", urlid);
                if((n = xtask_new_task(xtask, urlid, p, left)) > 0)
                {
                    p += n;
                    ++k;
                }
                else
                {
                    mmqueue_push(xtask->queue, xtask->state->qlist, urlid);
                    break;
                }
                left = nout - (p - out);
            }
        }
        else
        {
            DEBUG_LOGGER(xtask->logger, "read-urls flag:%d left:%d k:%d", flag, left, k);
            while(left > HTTP_HEADERS_MAX && k < HTTP_TASK_MAX 
                    && (urlid = xtask_host_poptask(xtask, &hostid)) > 0)
            {
                DEBUG_LOGGER(xtask->logger, "read-hostid:%d flag:%d urlid:%d",hostid,flag, urlid);
                DEBUG_LOGGER(xtask->logger, "page:%d", urlid);
                if((n = xtask_new_task(xtask, urlid, p, left)) > 0)
                {
                    DEBUG_LOGGER(xtask->logger, "over-read-hostid:%d flag:%d urlid:%d",hostid,flag, urlid);
                    p += n;
                    ++k;
                }
                else
                {
                    xtask_host_retask(xtask, hostid, urlid);
                    break;
                }
                left = nout - (p - out);
            }
            DEBUG_LOGGER(xtask->logger, "over-read-urls flag:%d left:%d k:%d", flag, left, k);
        }
        ret = p - out;
        //MUTEX_UNLOCK(xtask->urlnode_mutex);
    }
    return ret;
}

/* reupdate */
int xtask_reupdate(XTASK *xtask)
{
    int ret = -1, id = 0;

    if(xtask)
    {
        while(mmqueue_pop(xtask->queue, xtask->state->qupwait, &id) > 0)
            mmqueue_push(xtask->queue, xtask->state->qupdate, id);
        ret = 0;
    }
    return ret;
}

/* download task */
int xtask_new_download(XTASK *xtask, int flag, int *taskid, char *data, int ndata)
{
    int ret = -1, id = 0, n = 0, db_id = 0;
    char line[XT_PATH_MAX];

    if(xtask && data && ndata > 0)
    {
        MUTEX_LOCK(xtask->download_mutex);
        if(flag & XT_TASK_FILE)
        {
            if(mmqueue_pop(xtask->queue, xtask->state->qtask_file, &id) > 0)
            {
                n = sprintf(line, "file:%d", id);
                ret = db_xread_data(PDB(xtask->mmdb), line, n, data);
            }
            else
            {
                if(xtask->state->file_task_id < 0) xtask->state->file_task_id = 0;
                if((ret = xtask_download_task(xtask, flag, data, ndata)) > 0)
                {
                    if(mmqueue_pop(xtask->queue, xtask->state->qid_file, &id) <= 0)
                        id = ++xtask->state->file_task_id;
                    n = sprintf(line, "file:%d", id);
                    db_id = db_xchunk_data(PDB(xtask->mmdb), line, n, data, ret, ndata);
                }
                else id = 0;
            }
            if(ret > 0) vmap_try_insert(xtask->map, xtask->state->file_task_wait, id, id, NULL);
        }
        else if(flag & XT_TASK_UPDATE)
        {
            if(mmqueue_pop(xtask->queue, xtask->state->qtask_update, &id) > 0)
            {
                n = sprintf(line, "update:%d", id);
                ret = db_xread_data(PDB(xtask->mmdb), line, n, data);
            }
            else
            {
                if(xtask->state->update_task_id < 0) xtask->state->update_task_id = 0;
                if((ret = xtask_download_task(xtask, flag, data, ndata)) > 0)
                {
                    if(mmqueue_pop(xtask->queue, xtask->state->qid_update, &id) <= 0)
                        id = ++xtask->state->update_task_id;
                    n = sprintf(line, "update:%d", id);
                    db_id = db_xchunk_data(PDB(xtask->mmdb), line, n, data, ret, ndata);
                }
                else id = 0;
            }
            if(ret > 0) vmap_try_insert(xtask->map, xtask->state->update_task_wait, id, id, NULL);
        }
        else if(flag & XT_TASK_REPEAT)
        {
            if(mmqueue_pop(xtask->queue, xtask->state->qtask_retry, &id) > 0)
            {
                n = sprintf(line, "retry:%d", id);
                ret = db_xread_data(PDB(xtask->mmdb), line, n, data);
            }
            else
            {
                if(xtask->state->retry_task_id < 0) xtask->state->retry_task_id = 0;
                if((ret = xtask_download_task(xtask, flag, data, ndata)) > 0)
                {
                    if(mmqueue_pop(xtask->queue, xtask->state->qid_retry, &id) <= 0)
                        id = ++xtask->state->retry_task_id;
                    n = sprintf(line, "retry:%d", id);
                    db_id = db_xchunk_data(PDB(xtask->mmdb), line, n, data, ret, ndata);
                }
                else id = 0;
            }
            if(ret > 0) vmap_try_insert(xtask->map, xtask->state->retry_task_wait, id, id, NULL);
        }
        else if(flag & XT_TASK_LIST)
        {
            if(mmqueue_pop(xtask->queue, xtask->state->qtask_list, &id) > 0)
            {
                n = sprintf(line, "list:%d", id);
                ret = db_xread_data(PDB(xtask->mmdb), line, n, data);
            }
            else
            {
                if(xtask->state->list_task_id < 0) xtask->state->list_task_id = 0;
                if((ret = xtask_download_task(xtask, flag, data, ndata)) > 0)
                {
                    if(mmqueue_pop(xtask->queue, xtask->state->qid_list, &id) <= 0)
                        id = ++xtask->state->list_task_id;
                    n = sprintf(line, "list:%d", id);
                    db_id = db_xchunk_data(PDB(xtask->mmdb), line, n, data, ret, ndata);
                }
                else id = 0;
            }
            if(ret > 0) vmap_try_insert(xtask->map, xtask->state->list_task_wait, id, id, NULL);
        }
        else
        {
            if(mmqueue_pop(xtask->queue, xtask->state->qtask_page, &id) > 0)
            {
                DEBUG_LOGGER(xtask->logger, "uncomplete task:%d", id);
                n = sprintf(line, "page:%d", id);
                ret = db_xread_data(PDB(xtask->mmdb), line, n, data);
            }
            else
            {
                if(xtask->state->page_task_id < 0) xtask->state->page_task_id = 0;
                DEBUG_LOGGER(xtask->logger, "new-task-start");
                if((ret = xtask_download_task(xtask, flag, data, ndata)) > 0)
                {
                    if(mmqueue_pop(xtask->queue, xtask->state->qid_page, &id) <= 0)
                        id = ++xtask->state->page_task_id;
                    n = sprintf(line, "page:%d", id);
                    DEBUG_LOGGER(xtask->logger, "new-task:%d", id);
                    db_id = db_xchunk_data(PDB(xtask->mmdb), line, n, data, ret, ndata);
                    DEBUG_LOGGER(xtask->logger, "over-new-task:%d", id);
                }
                else id = 0;
            }
            if(ret > 0) vmap_try_insert(xtask->map, xtask->state->page_task_wait, id, id, NULL);
        }
        MUTEX_UNLOCK(xtask->download_mutex);
        *taskid = id;
    }
    return ret;
}

/* reset download */
int xtask_reset_download(XTASK *xtask, int flag, int taskid)
{
    int ret = -1, k = 0;

    if(xtask)
    {
        MUTEX_LOCK(xtask->download_mutex);
        if(flag & XT_TASK_FILE) 
        {
            if((k = vmap_find(xtask->map, xtask->state->file_task_wait, taskid, NULL)) > 0)
            {
                mmqueue_push(xtask->queue, xtask->state->qtask_file, taskid);
                vmap_remove(xtask->map, xtask->state->file_task_wait, k, NULL, NULL);
            }
            ret = 0;
        }
        else if(flag & XT_TASK_UPDATE) 
        {
            if((k = vmap_find(xtask->map, xtask->state->update_task_wait, taskid, NULL)) > 0)
            {
                mmqueue_push(xtask->queue, xtask->state->qtask_update, taskid);
                vmap_remove(xtask->map, xtask->state->update_task_wait, k, NULL, NULL);
            }
            ret = 0;
        }
        else if(flag & XT_TASK_REPEAT) 
        {
            if((k = vmap_find(xtask->map, xtask->state->retry_task_wait, taskid, NULL)) > 0)
            {
                mmqueue_push(xtask->queue, xtask->state->qtask_retry, taskid);
                vmap_remove(xtask->map, xtask->state->retry_task_wait, k, NULL, NULL);
            }
            ret = 0;
        }
        else if(flag & XT_TASK_LIST) 
        {
            if((k = vmap_find(xtask->map, xtask->state->list_task_wait, taskid, NULL)) > 0)
            {
                mmqueue_push(xtask->queue, xtask->state->qtask_list, taskid);
                vmap_remove(xtask->map, xtask->state->list_task_wait, k, NULL, NULL);
            }
            ret = 0;
        }
        else
        {
            if((k = vmap_find(xtask->map, xtask->state->page_task_wait, taskid, NULL)) > 0)
            {
                mmqueue_push(xtask->queue, xtask->state->qtask_page, taskid);
                vmap_remove(xtask->map, xtask->state->page_task_wait, k, NULL, NULL);
            }
            ret = 0;
        }
        MUTEX_UNLOCK(xtask->download_mutex);
    }
    return ret;
}

/* over download task */
int xtask__over__download(XTASK *xtask, int flag, int taskid)
{
    int ret = -1, k = 0, n = 0;
    char line[XT_PATH_MAX];

    if(xtask)
    {
        MUTEX_LOCK(xtask->download_mutex);    
        if(flag & XT_TASK_FILE)
        {
            if((k = vmap_find(xtask->map, xtask->state->file_task_wait, taskid, NULL)) > 0)
            {
                vmap_remove(xtask->map, xtask->state->file_task_wait, k, NULL, NULL);
                n = sprintf(line, "file:%d", taskid);
                n = db_xdel_data(PDB(xtask->mmdb), line, n);
                mmqueue_push(xtask->queue, xtask->state->qid_file, taskid);
            }
            ret = 0;
        }
        else if(flag & XT_TASK_UPDATE)
        {
            if((k = vmap_find(xtask->map, xtask->state->update_task_wait, taskid, NULL)) > 0)
            {
                vmap_remove(xtask->map, xtask->state->update_task_wait, k, NULL, NULL);
                n = sprintf(line, "update:%d", taskid);
                n = db_xdel_data(PDB(xtask->mmdb), line, n);
                mmqueue_push(xtask->queue, xtask->state->qid_update, taskid);
            }
            ret = 0;
        }
        else if(flag & XT_TASK_REPEAT)
        {
            if((k = vmap_find(xtask->map, xtask->state->retry_task_wait, taskid, NULL)) > 0)
            {
                vmap_remove(xtask->map, xtask->state->retry_task_wait, k, NULL, NULL);
                n = sprintf(line, "retry:%d", taskid);
                n = db_xdel_data(PDB(xtask->mmdb), line, n);
                mmqueue_push(xtask->queue, xtask->state->qid_retry, taskid);
            }
            ret = 0;
        }
        else if(flag & XT_TASK_LIST)
        {
            if((k = vmap_find(xtask->map, xtask->state->list_task_wait, taskid, NULL)) > 0)
            {
                vmap_remove(xtask->map, xtask->state->list_task_wait, k, NULL, NULL);
                n = sprintf(line, "list:%d", taskid);
                n = db_xdel_data(PDB(xtask->mmdb), line, n);
                mmqueue_push(xtask->queue, xtask->state->qid_list, taskid);
            }
            ret = 0;
        }
        else
        {
            if((k = vmap_find(xtask->map, xtask->state->page_task_wait, taskid, NULL)) > 0)
            {
                vmap_remove(xtask->map, xtask->state->page_task_wait, k, NULL, NULL);
                n = sprintf(line, "page:%d", taskid);
                n = db_xdel_data(PDB(xtask->mmdb), line, n);
                mmqueue_push(xtask->queue, xtask->state->qid_page, taskid);
            }
            ret = 0;
        }
        MUTEX_UNLOCK(xtask->download_mutex);    
    }
    return ret;
}
#define DOWNLOAD_BYTES_STATE(xtask, flag, len)                  \
do                                                              \
{                                                               \
    if(flag & XT_TASK_FILE)                                     \
    {                                                           \
        xtask->state->file_over_total++;                        \
        xtask->state->file_bytes_total += len;                  \
    }                                                           \
    else if(flag & XT_TASK_LIST)                                \
    {                                                           \
        xtask->state->list_over_total++;                        \
        xtask->state->list_bytes_total += len;                  \
    }                                                           \
    else if(flag & XT_TASK_UPDATE)                              \
    {                                                           \
        xtask->state->update_over_total++;                      \
        xtask->state->update_bytes_total += len;                \
    }                                                           \
    else if(flag & XT_TASK_REPEAT)                              \
    {                                                           \
        xtask->state->repeat_over_total++;                      \
        xtask->state->repeat_bytes_total += len;                \
    }                                                           \
    else                                                        \
    {                                                           \
        xtask->state->page_over_total++;                        \
        xtask->state->page_bytes_total += len;                  \
    }                                                           \
}while(0)
#define OVER_DOWNLOAD_STATE(xtask, flag, rec)               \
do                                                          \
{                                                           \
    MUTEX_LOCK(xtask->mutex);                               \
    if(rec->length)                                         \
    {                                                       \
        xtask->state->over_download++;                      \
        DOWNLOAD_BYTES_STATE(xtask, flag, rec->length);     \
    }                                                       \
    else                                                    \
    {                                                       \
        if(rec->err & ERR_NEED_REPEAT)                      \
             xtask->state->err_download++;                  \
        else                                                \
             xtask->state->unknown_download++;              \
    }                                                       \
    MUTEX_UNLOCK(xtask->mutex);                             \
}while(0)

/* over download */
int xtask_over_download(XTASK *xtask, int flag, int taskid, char *data, int ndata)
{
    char *p = NULL, *end = NULL;
    XTURLNODE urlnode = {0};
    XTREC *rec = NULL;
    int ret = -1, id = 0;

    if(xtask && (p = data) && (end = (data + ndata)) > data)
    {
        ret = xtask__over__download(xtask, flag, taskid);
        while(p < end)
        {
            rec = (XTREC *)p; p += sizeof(XTREC);
            if(xtask_get_urlnode(xtask, rec->id, &urlnode) > 0)
            {
                if(rec->nlocation > 0)
                {
                    id = xtask_add_url(xtask, urlnode.parent, urlnode.nodeid, p, urlnode.flag);
                    p += rec->nlocation + 1;
                }
                else 
                    id = rec->id;
                if(rec->ncookie > 0) 
                {
                    xtask_parse_cookie(xtask, urlnode.hostid, rec->date, p, rec->ncookie);
                    p += rec->ncookie + 1;
                }
                if(rec->length > 0)
                {
                    ret = xtask_set_urlnode_data(xtask, id, rec->last_modified, p, rec->length);
                    p += rec->length;
                    if(urlnode.flag & REG_IS_FILE)
                        mmqueue_push(xtask->queue, xtask->state->qfile_dump, id);
                    else
                        mmqueue_push(xtask->queue, xtask->state->qextract, id);
                    if((urlnode.flag & REG_IS_LIST))
                        mmqueue_push(xtask->queue, xtask->state->qupdate, id);
                }
                if(rec->err)
                {
                    xtask_set_urlnode_status(xtask, id, rec->err);
                }
                OVER_DOWNLOAD_STATE(xtask, flag, rec);
            }
            else 
            {
                WARN_LOGGER(xtask->logger, "urlid:%d not found", rec->id);
                break; 
            }
        }
    }
    return ret;
}

/* new extract task */
int xtask_extract_task(XTASK *xtask, char *out, int nout)
{
    int ret = -1, id = 0, n = 0, k = 0, left = 0, last = 0;
    char *p = NULL, *end = NULL, *pp = NULL, *s = NULL;
    XTURLNODE urlnode = {0};
    XTROW *row = NULL;
    XTMORE *more = NULL;

    if(xtask && (p = out) && (end = (p + nout)) > p)
    {
        more = (XTMORE *)p; p += sizeof(XTMORE);
        if((more->ntables = xtask_read_tables(xtask, p, nout - (p - out))) > 0)
            p += more->ntables * sizeof(XTTABLE);
        else return -1;
        if((more->ntemplates = xtask_read_templates(xtask, p, nout - (p - out))) > 0)
            p += more->ntemplates * sizeof(ITEMPLATE);
        else return -1;
        s = p;
        while((left = (nout - (p - out)))>(xtask->state->page_size_max+XT_ROW_MAX) 
                && k < XT_EXTRACT_MAX 
                && mmqueue_pop(xtask->queue, xtask->state->qextract, &id) > 0)
        {
            //if(id == last) continue;
            ACCESS_LOGGER(xtask->logger, "ready for extract urlid:%d", id);
            pp = p;
            if(xtask_get_urlnode(xtask, id, &urlnode) && urlnode.nodeid > 0)
            {
                memset(p, 0, sizeof(XTROW));
                row = (XTROW *)p;
                row->id = id;
                p += sizeof(XTROW);
                left -= sizeof(XTROW);
                if((row->ntemplates = xtask_get_node_templates_id(xtask, 
                                urlnode.nodeid, p, left)) <= 0) 
                {
                    ACCESS_LOGGER(xtask->logger, "urlid:%d nodeid:%d ntemplates:%d", id, urlnode.nodeid, row->ntemplates);
                    goto over;
                }
                p += sizeof(int) * row->ntemplates;
                /* read url */
                if((n = db_read_data(PDB(xtask->mmdb), urlnode.url_db_id, p)) <= 0)
                    goto over;
                p += n;
                *p++ = 0;
                row->nurl = n;
                /* read data */
                if((n = db_read_data(PDB(xtask->db), id, p)) <= 0)
                    goto over;
                p += n;
                row->length = n;
                ++k;
                last = id;
                ACCESS_LOGGER(xtask->logger, "urlid:%d left:%d nurl:%d length:%d page_max:%d", id, left, row->nurl, row->length, xtask->state->page_size_max);
                continue;
            }
over:
            mmqueue_push(xtask->queue, xtask->state->qextract, id);
            p = pp;
            break;
        }
        if(p != s) ret = p - out;
        ACCESS_LOGGER(xtask->logger, "extract length:%d total:%d", ret, k);
    }
    return ret;
}

/* extract task */
int xtask_new_extract(XTASK *xtask, int *taskid, char *data, int ndata)
{
    int ret = -1, id = 0, n = 0, db_id = 0;
    char line[XT_PATH_MAX];

    if(xtask && data && ndata > 0)
    {
        MUTEX_LOCK(xtask->extract_mutex);
        if(mmqueue_pop(xtask->queue, xtask->state->qtask_extract, &id) > 0)
        {
            n = sprintf(line, "extract:%d", id);
            ret = db_xread_data(PDB(xtask->mmdb), line, n, data);
            ACCESS_LOGGER(xtask->logger, "new_extract taskid:%d ret:%d", id, ret);
        }
        else
        {
            if(xtask->state->extract_task_id < 0) xtask->state->extract_task_id = 0;
            if((ret = xtask_extract_task(xtask, data, ndata)) > 0)
            {
                if(mmqueue_pop(xtask->queue, xtask->state->qid_extract, &id) <= 0)
                    id = ++xtask->state->extract_task_id;
                n = sprintf(line, "extract:%d", id);
                db_id = db_xchunk_data(PDB(xtask->mmdb), line, n, data, ret, ndata);
                ACCESS_LOGGER(xtask->logger, "new_extract taskid:%d db_id:%d", id, db_id);
            }
        }
        if(ret > 0) vmap_try_insert(xtask->map, xtask->state->extract_task_wait, id, id, NULL);
        MUTEX_UNLOCK(xtask->extract_mutex);
        *taskid = id;
    }
    return ret;
}

/* reset extract */
int xtask_reset_extract(XTASK *xtask, int taskid)
{
    int ret = -1, k = 0;

    if(xtask)
    {
        MUTEX_LOCK(xtask->extract_mutex);
        if((k = vmap_find(xtask->map, xtask->state->extract_task_wait, taskid, NULL)) > 0)
        {
            mmqueue_push(xtask->queue, xtask->state->qtask_extract, taskid);
            vmap_remove(xtask->map, xtask->state->extract_task_wait, k, NULL, NULL);
        }
        ret = 0;
        MUTEX_UNLOCK(xtask->extract_mutex);
    }
    return ret;
}

/* over extract task */
int xtask__over__extract(XTASK *xtask, int taskid)
{
    int ret = -1, k = 0, n = 0;
    char line[XT_PATH_MAX];

    if(xtask)
    {
        MUTEX_LOCK(xtask->extract_mutex);    
        ACCESS_LOGGER(xtask->logger, "extract__data_start taskid:%d", taskid);
        if((k = vmap_find(xtask->map, xtask->state->extract_task_wait, taskid, NULL)) > 0)
        {
            vmap_remove(xtask->map, xtask->state->extract_task_wait, k, NULL, NULL);
            n = sprintf(line, "extract:%d", taskid);
            n = db_xdel_data(PDB(xtask->mmdb), line, n);
            mmqueue_push(xtask->queue, xtask->state->qid_extract, taskid);
        }
        ret = 0;
        ACCESS_LOGGER(xtask->logger, "extract__data_over taskid:%d", taskid);
        MUTEX_UNLOCK(xtask->extract_mutex);    
    }
    return ret;
}
#define OVER_EXTRACT_STATE(xtask, res)                                          \
do                                                                              \
{                                                                               \
    MUTEX_LOCK(xtask->mutex);                                                   \
    if(res->length <= sizeof(XTRES))                                            \
    {                                                                           \
        WARN_LOGGER(xtask->logger, "err_extract{urlid:%d,nodeid:%d,length:%d}", \
                res->id, res->nodeid, res->length);                             \
        xtask->state->err_extract++;                                            \
    }                                                                           \
    xtask->state->over_extract++;                                               \
    MUTEX_UNLOCK(xtask->mutex);                                                 \
}while(0)
/* over extract task */
int xtask_over_extract(XTASK *xtask, int taskid, char *data, int ndata)
{
    char *p = NULL, *end = NULL, *tail = NULL, *mm = NULL;
    int ret = -1, x = 0, parentid = 0, id = 0;
    XTURLNODE urlnode = {0};
    XTRES *res = NULL;
    XTITEM *item = NULL;

    if(xtask && (p = data) && (end = (data + ndata)) > p 
            && (mm = xtask_pop_block(xtask)) )
    {
        while(p < end)
        {
            res = (XTRES *)p;        
            if((ret = xtask_get_urlnode(xtask, res->id, &urlnode)) > 0)
            {
                ACCESS_LOGGER(xtask->logger, "extract-data urlid:%d", res->id);
                //merge res
                if((urlnode.flag & REG_IS_UNIQE))// || xtask_parents_is_uniqe(xtask, res->id))
                {
                    DEBUG_LOGGER(xtask->logger, "res_to_record(urlid:%d) item->count:%d nodeid:%d ret:%d", res->id, item->count, urlnode.nodeid, ret);
                    if(res->length > sizeof(XTRES) && (ret = xtask_res_to_record(xtask, 
                                    &urlnode, res, (XTRECORD *)mm)) != -1)
                    {
                        mmqueue_push(xtask->queue, xtask->state->qdump, res->id);
                        ACCESS_LOGGER(xtask->logger, "qdump urlid:%d total:%d", res->id, mmqueue_total(xtask->queue, xtask->state->qdump));
                    }
                    p += res->length;
                    DEBUG_LOGGER(xtask->logger, "res_to_record(urlid:%d) item->count:%d nodeid:%d ret:%d", res->id, item->count, urlnode.nodeid, ret);
                }
                else
                {
                    tail = p + res->length;
                    p += sizeof(XTRES);
                    while(p < tail)
                    {
                        item = (XTITEM *)p;
                        p += sizeof(XTITEM);
                        x = 0;
                        while(x < item->count)
                        {
                            if(item->regs[x].flag & REG_IS_LINK)
                            {
                                parentid = xtask_check_parent(xtask, res->id, item->regs[x].nodeid);
                                id = xtask_add_url(xtask, parentid, item->regs[x].nodeid,p,item->regs[x].flag); 
                                //WARN_LOGGER(xtask->logger, "urlid:%d url:%s", id, p);
                                if((item->regs[x].flag & REG_IS_PAGE) && urlnode.brother > 0
                                        && item->regs[x].nodeid == urlnode.nodeid)
                                {
                                    xtask_set_brother(xtask, id, res->id);
                                }
                                if(!(item->regs[x].flag & REG_IS_PAGE) && id > 0 && item->regs[x].nodeid > 0)
                                {
                                    DEBUG_LOGGER(xtask->logger, "item_to_record(urlid:%d) id:%d nodeid:%d url:%s count:%d item[%d]->flag:%d", res->id, id, item->regs[x].nodeid, p, item->count, x, item->regs[x].flag);
                                    xtask_item_to_record(xtask, id, item, (XTRECORD *)mm);
                                    DEBUG_LOGGER(xtask->logger, "item_to_record(urlid:%d) id:%d nodeid:%d url:%s count:%d item[%d]->flag:%d", res->id, id, item->regs[x].nodeid, p, item->count, x, item->regs[x].flag);
                                }
                            }
                            p += item->regs[x].length + 1;
                            ++x;
                        }
                    }
                }
                OVER_EXTRACT_STATE(xtask, res);
                ACCESS_LOGGER(xtask->logger, "over-extract-data urlid:%d", res->id);
            }
            else
            { 
                WARN_LOGGER(xtask->logger, "invalid urlid:%d", res->id);
                p = tail;
            }
        }
        ret = xtask__over__extract(xtask, taskid);
        xtask_push_block(xtask, mm);
    }
    return ret;
}

/* check task wait */
void xtask_check_wait(XTASK *xtask)
{
    int mid = 0, taskid = 0;

    if(xtask)
    {
        while(vmap_total(xtask->map, xtask->state->host_task_wait) > 0
            && (mid = vmap_min(xtask->map, xtask->state->host_task_wait, &taskid, NULL)) > 0)
        {
            mmqueue_push(xtask->queue, xtask->state->qhost, taskid);
            vmap_remove(xtask->map, xtask->state->host_task_wait, mid, NULL, NULL);
        }
        while(vmap_total(xtask->map, xtask->state->page_task_wait) > 0
            && (mid = vmap_min(xtask->map, xtask->state->page_task_wait, &taskid, NULL)) > 0)
        {
            mmqueue_push(xtask->queue, xtask->state->qtask_page, taskid);
            vmap_remove(xtask->map, xtask->state->page_task_wait, mid, NULL, NULL);
        }
        while(vmap_total(xtask->map, xtask->state->list_task_wait) > 0
            && (mid = vmap_min(xtask->map, xtask->state->list_task_wait, &taskid, NULL)) > 0)
        {
            mmqueue_push(xtask->queue, xtask->state->qtask_list, taskid);
            vmap_remove(xtask->map, xtask->state->list_task_wait, mid, NULL, NULL);
        }
        while(vmap_total(xtask->map, xtask->state->file_task_wait) > 0
            && (mid = vmap_min(xtask->map, xtask->state->file_task_wait, &taskid, NULL)) > 0)
        {
            mmqueue_push(xtask->queue, xtask->state->qtask_file, taskid);
            vmap_remove(xtask->map, xtask->state->file_task_wait, mid, NULL, NULL);
        }
        while(vmap_total(xtask->map, xtask->state->update_task_wait) > 0
            && (mid = vmap_min(xtask->map, xtask->state->update_task_wait, &taskid, NULL)) > 0)
        {
            mmqueue_push(xtask->queue, xtask->state->qtask_update, taskid);
            vmap_remove(xtask->map, xtask->state->update_task_wait, mid, NULL, NULL);
        }
        while(vmap_total(xtask->map, xtask->state->retry_task_wait) > 0
            && (mid = vmap_min(xtask->map, xtask->state->retry_task_wait, &taskid, NULL)) > 0)
        {
            mmqueue_push(xtask->queue, xtask->state->qtask_retry, taskid);
            vmap_remove(xtask->map, xtask->state->retry_task_wait, mid, NULL, NULL);
        }
        while(vmap_total(xtask->map, xtask->state->extract_task_wait) > 0
            && (mid = vmap_min(xtask->map, xtask->state->extract_task_wait, &taskid, NULL)) > 0)
        {
            mmqueue_push(xtask->queue, xtask->state->qtask_extract, taskid);
            vmap_remove(xtask->map, xtask->state->extract_task_wait, mid, NULL, NULL);
        }
    }
    return ;
}
#define STATE_SPEED(xtask, now, total, interval, page_avg, page_speed,      \
        list_avg, list_speed, update_avg, update_speed,                     \
        repeat_avg, repeat_speed, file_avg, file_speed)                     \
do                                                                          \
{                                                                           \
    MUTEX_LOCK(xtask->mutex);                                               \
    now = time(NULL);                                                       \
    total = now - xtask->state->time_start;                                 \
    if((interval = (now - xtask->state->time_last)) > 0)                    \
    {                                                                       \
        page_avg = (xtask->state->page_bytes_total                          \
                - xtask->state->page_bytes_init)/ total;                    \
        page_speed = (xtask->state->page_bytes_total                        \
                - xtask->state->page_bytes_last)/interval;                  \
        xtask->state->page_bytes_last = xtask->state->page_bytes_total;     \
        list_avg = (xtask->state->list_bytes_total                          \
                - xtask->state->list_bytes_init)/ total;                    \
        list_speed = (xtask->state->list_bytes_total                        \
                - xtask->state->list_bytes_last)/interval;                  \
        xtask->state->list_bytes_last = xtask->state->list_bytes_total;     \
        update_avg = (xtask->state->update_bytes_total                      \
                - xtask->state->update_bytes_init)/ total;                  \
        update_speed = (xtask->state->update_bytes_total                    \
                - xtask->state->update_bytes_last)/interval;                \
        xtask->state->update_bytes_last = xtask->state->update_bytes_total; \
        repeat_avg = (xtask->state->repeat_bytes_total                      \
                - xtask->state->repeat_bytes_init)/ total;                  \
        repeat_speed = (xtask->state->repeat_bytes_total                    \
                - xtask->state->repeat_bytes_last)/interval;                \
        xtask->state->repeat_bytes_last = xtask->state->repeat_bytes_total; \
        file_avg = (xtask->state->file_bytes_total                          \
                - xtask->state->file_bytes_init)/ total;                    \
        file_speed = (xtask->state->file_bytes_total                        \
                - xtask->state->file_bytes_last)/interval;                  \
        xtask->state->file_bytes_last = xtask->state->file_bytes_total;     \
        xtask->state->time_last = now;                                      \
    }                                                                       \
    MUTEX_UNLOCK(xtask->mutex);                                             \
}while(0)
#define KSPEED(xxxx) (xxxx / 1024)
/* get state info */
int xtask_get_stateinfo(XTASK *xtask, char *out, int nout)
{
    int ret = -1, qhost, qpriority = 0, qwait = 0, qlist = 0, qfile = 0, 
        qupdate = 0, qextract = 0, qdump = 0, qretry = 0, page_avg = 0,
        page_speed = 0, file_avg = 0, file_speed = 0, list_avg = 0, 
        list_speed = 0, update_avg = 0, update_speed = 0, repeat_avg = 0,
        repeat_speed = 0, total = 0, interval = 0;
    time_t now = 0;
    char *p = NULL, *end = NULL;

    if(xtask && (p = out) && (end = (out + nout)) > out)
    {
        STATE_SPEED(xtask, now, total, interval, page_avg, page_speed, list_avg, list_speed, 
            update_avg, update_speed, repeat_avg, repeat_speed, file_avg, file_speed);

        qhost = mmqueue_total(xtask->queue, xtask->state->qhost);
        qpriority = mmqueue_total(xtask->queue, xtask->state->qpriority);
        qwait = mmqueue_total(xtask->queue, xtask->state->qwait);
        qlist = mmqueue_total(xtask->queue, xtask->state->qlist);
        qfile = mmqueue_total(xtask->queue, xtask->state->qfile);
        qupdate = mmqueue_total(xtask->queue, xtask->state->qupdate);
        qretry = mmqueue_total(xtask->queue, xtask->state->qretry);
        qextract = mmqueue_total(xtask->queue, xtask->state->qextract);
        qdump = mmqueue_total(xtask->queue, xtask->state->qdump);
        p += sprintf(p, "({'host_total':'%d','url_total':'%d','qlist_total':'%d',"
                "'qpriority_total':'%d','qwait_total':'%d','qfile_total':'%d',"
                "'qupdate_total':'%d','qextract_total':'%d','qdump_total':'%d',"
                "'err_download':'%d','unknown_download':'%d','over_download':'%d',"
                "'err_extract':'%d', 'over_extract':'%d', 'qhost_total':'%d', 'over_dump':'%d',"
                "'err_dump':'%d','qretry_total':'%d', 'time_total':'%d', 'interval':'%d',"
                "'page_avg':'%d', 'page_speed':'%d', 'list_avg':'%d', 'list_speed':'%d', "
                "'update_avg':'%d', 'update_speed':'%d','repeat_avg':'%d', 'repeat_speed':'%d', "
                "'file_avg':'%d', 'file_speed':'%d'})", 
                xtask->state->host_id_max, xtask->state->urlnode_id_max,
                qlist, qpriority, qwait, qfile, qupdate, qextract, qdump,
                xtask->state->err_download, xtask->state->unknown_download, 
                xtask->state->over_download, xtask->state->err_extract,
                xtask->state->over_extract, qhost, xtask->state->over_dump, 
                xtask->state->err_dump, qretry, total, interval,
                KSPEED(page_avg), KSPEED(page_speed), KSPEED(list_avg), KSPEED(list_speed), 
                KSPEED(update_avg), KSPEED(update_speed), KSPEED(repeat_avg), 
                KSPEED(repeat_speed), KSPEED(file_avg), KSPEED(file_speed));  
        ret = p - out;
    }
    return ret;
}
/* get headers */
int xtask_get_headers(XTASK *xtask, char *out)
{
    char *s = NULL;
    int ret = -1;

    if(xtask && (s = out))
    {
        MUTEX_LOCK(xtask->mutex);
        s += sprintf(s, "({'headers':'");
        s += base64_encode(s, (const unsigned char *)xtask->state->http_headers, 
                strlen(xtask->state->http_headers));
        s += sprintf(s, "'})");
        ret = s - out;
        MUTEX_UNLOCK(xtask->mutex);
    }
    return ret; 
}

/* set headers */
int xtask_set_headers(XTASK *xtask, char *headers)
{
    char *p = NULL, *pp = NULL, *s = NULL, *ss = NULL;
    int ret = -1, n = 0;
    if(xtask && (p = headers) && strlen(headers) > 0)
    {
        MUTEX_LOCK(xtask->mutex);
        s = xtask->state->http_headers;
        while(*p != '\0') 
        {
            pp = p;
            ss = s;
            while(*p != '|' && *p != '\0')++p;
            if((n = (p - pp)) > 0 && (s - xtask->state->http_headers) 
                    < (XT_HTTP_HEADER_MAX - n))
            {
                s += base64_decode((unsigned char *)s, (const char *)pp, n);
                *s++ = '\r';*s++ = '\n';
            }
            if(*p == '|') ++p;
        }
        *s = '\0';
        MUTEX_UNLOCK(xtask->mutex);
        ret = 0;
    }
    return ret;
}

/* auto set headers */
int xtask_auto_headers(XTASK *xtask, HTTP_REQ *http_req)
{
    char *s = NULL, *bs = NULL;
    int ret = -1, n = 0;

    if(xtask && http_req && (bs = http_req->hlines))
    {
        MUTEX_LOCK(xtask->mutex);
        s = xtask->state->http_headers;
        if((n = http_req->headers[HEAD_REQ_ACCEPT]) > 0)
        {
            s += sprintf(s, "%s %s\r\n", http_headers[HEAD_REQ_ACCEPT].e, bs + n);
        }
        if((n = http_req->headers[HEAD_REQ_ACCEPT_CHARSET]) > 0)
        {
            s += sprintf(s, "%s %s\r\n", http_headers[HEAD_REQ_ACCEPT_CHARSET].e, bs + n);
        }
        if((n = http_req->headers[HEAD_REQ_ACCEPT_ENCODING]) > 0)
        {
            s += sprintf(s, "%s %s\r\n", http_headers[HEAD_REQ_ACCEPT_ENCODING].e, bs + n);
        }
        if((n = http_req->headers[HEAD_REQ_ACCEPT_LANGUAGE]) > 0)
        {
            s += sprintf(s, "%s %s\r\n", http_headers[HEAD_REQ_ACCEPT_LANGUAGE].e, bs + n);
        }
        if((n = http_req->headers[HEAD_GEN_CONNECTION]) > 0)
        {
            s += sprintf(s, "%s %s\r\n", http_headers[HEAD_GEN_CONNECTION].e, bs + n);
        }
        if((n = http_req->headers[HEAD_REQ_USER_AGENT]) > 0)
        {
            s += sprintf(s, "%s %s\r\n", http_headers[HEAD_REQ_USER_AGENT].e, bs + n);
        }
        MUTEX_UNLOCK(xtask->mutex);
        ret = 0;
    }
    return ret;
}

/* initialize XTASK */
XTASK *xtask_init(char *dir)
{
    char path[XT_PATH_MAX], *p = NULL;
    struct stat st = {0};
    XTASK *xtask = NULL;
    int n = 0, i = 0;

    if(dir && (xtask = xmm_new(sizeof(XTASK))))
    {
        MUTEX_INIT(xtask->mutex);
        MUTEX_INIT(xtask->table_mutex);
        MUTEX_INIT(xtask->node_mutex);
        MUTEX_INIT(xtask->proxy_mutex);
        MUTEX_INIT(xtask->dns_mutex);
        MUTEX_INIT(xtask->host_mutex);
        MUTEX_INIT(xtask->urlnode_mutex);
        MUTEX_INIT(xtask->download_mutex);
        MUTEX_INIT(xtask->extract_mutex);
        MUTEX_INIT(xtask->record_mutex);
        MUTEX_INIT(xtask->block_mutex);
        MUTEX_INIT(xtask->dump_mutex);
        MUTEX_INIT(xtask->csv_mutex);
        /* initialize mutexs  */
#ifdef HAVE_PTHREAD
        for(i = 0; i < XT_MUTEX_MAX; i++)
        {
            pthread_mutex_init(&(xtask->mutexs[i]), NULL);
            pthread_mutex_init(&(xtask->hmutexs[i]), NULL);
        }
#endif
        strcpy(xtask->basedir, dir);
        n = sprintf(path, "%s/%s", dir, XTASK_LOG_NAME); 
        xtask_pmkdir(path);
        p = path;
        LOGGER_INIT(xtask->logger, p);
        n = sprintf(path, "%s/%s", dir, XTASK_STATE_NAME); 
        if((xtask->stateio.fd = open(path, O_CREAT|O_RDWR, 0644)) <= 0 
                || fstat(xtask->stateio.fd, &st) != 0)
        {
            if(xtask->stateio.fd > 0) close(xtask->stateio.fd);
            FATAL_LOGGER(xtask->logger, "open state file[%s] failed, %s", path, strerror(errno));
            _exit(-1);
        }
        else
        {
            if(st.st_size < sizeof(XTSTATE) && ftruncate(xtask->stateio.fd, sizeof(XTSTATE)) != 0)
            {
                _exit(-1);
            }
            if((xtask->state = (XTSTATE *)mmap(NULL, sizeof(XTSTATE), PROT_READ|PROT_WRITE,
                            MAP_SHARED, xtask->stateio.fd, 0)) == NULL || xtask->state == (void *)-1)
            {
                FATAL_LOGGER(xtask->logger, "mmap state failed, %s\n", strerror(errno));
                _exit(-1);
            }
            if(st.st_size < sizeof(XTSTATE)) 
                memset(((char *)xtask->state + st.st_size), 0, sizeof(XTSTATE) - st.st_size);
            if(xtask->state->http_headers[0] == '\0')
            {
                strcpy(xtask->state->http_headers, XT_HTTP_GEN_HEADER);
            }
        }
        n = sprintf(path, "%s/%s", dir, XTASK_MMDB_DIR); 
        if((xtask->mmdb = db_init(path, 1)) == NULL)
        {
            FATAL_LOGGER(xtask->logger, "initialize MMDB[%s] failed, %s", path, strerror(errno));
            _exit(-1);
        }
        n = sprintf(path, "%s/%s", dir, XTASK_TEMP_DIR); 
        if((xtask->temp = db_init(path, 0)) == NULL)
        {
            FATAL_LOGGER(xtask->logger, "initialize TEMPLATE[%s] failed, %s", path, strerror(errno));
            _exit(-1);
        }
        n = sprintf(path, "%s/%s", dir, XTASK_DB_DIR); 
        if((xtask->db = db_init(path, 0)) == NULL)
        {
            FATAL_LOGGER(xtask->logger, "initialize DB[%s] failed, %s", path, strerror(errno));
            _exit(-1);
        }
        n = sprintf(path, "%s/%s", dir, XTASK_RES_DIR); 
        if((xtask->res = db_init(path, 0)) == NULL)
        {
            FATAL_LOGGER(xtask->logger, "initialize RES[%s] failed, %s", path, strerror(errno));
            _exit(-1);
        }
        n = sprintf(path, "%s/%s", dir, XTASK_QUEUE_NAME); 
        if((xtask->queue = mmqueue_init(path)) == NULL)
        {
            FATAL_LOGGER(xtask->logger, "initialize mmqueue[%s] failed, %s", path, strerror(errno));
            _exit(-1);
        }
        if(xtask->state->qproxy == 0) xtask->state->qproxy = mmqueue_new(xtask->queue);
        if(xtask->state->qdns == 0) xtask->state->qdns = mmqueue_new(xtask->queue);
        if(xtask->state->qhost == 0) xtask->state->qhost = mmqueue_new(xtask->queue);
        if(xtask->state->qlist == 0) xtask->state->qlist = mmqueue_new(xtask->queue);
        if(xtask->state->qpriority == 0) xtask->state->qpriority = mmqueue_new(xtask->queue);
        if(xtask->state->qwait == 0) xtask->state->qwait = mmqueue_new(xtask->queue);
        if(xtask->state->qfile == 0) xtask->state->qfile = mmqueue_new(xtask->queue);
        if(xtask->state->qupdate == 0) xtask->state->qupdate = mmqueue_new(xtask->queue);
        if(xtask->state->qupwait == 0) xtask->state->qupwait = mmqueue_new(xtask->queue);
        if(xtask->state->qextract == 0) xtask->state->qextract = mmqueue_new(xtask->queue);
        if(xtask->state->qdump == 0) xtask->state->qdump = mmqueue_new(xtask->queue);
        if(xtask->state->qfile_dump == 0) xtask->state->qfile_dump = mmqueue_new(xtask->queue);
        if(xtask->state->qtask_page == 0)xtask->state->qtask_page=mmqueue_new(xtask->queue);
        if(xtask->state->qtask_list == 0)xtask->state->qtask_list=mmqueue_new(xtask->queue);
        if(xtask->state->qtask_file == 0)xtask->state->qtask_file = mmqueue_new(xtask->queue);
        if(xtask->state->qtask_update == 0) xtask->state->qtask_update = mmqueue_new(xtask->queue);
        if(xtask->state->qtask_retry == 0) xtask->state->qtask_retry = mmqueue_new(xtask->queue);
        if(xtask->state->qtask_extract == 0) xtask->state->qtask_extract = mmqueue_new(xtask->queue);
        if(xtask->state->qid_page == 0)xtask->state->qid_page=mmqueue_new(xtask->queue);
        if(xtask->state->qid_list == 0)xtask->state->qid_list=mmqueue_new(xtask->queue);
        if(xtask->state->qid_file == 0)xtask->state->qid_file = mmqueue_new(xtask->queue);
        if(xtask->state->qid_update == 0) xtask->state->qid_update = mmqueue_new(xtask->queue);
        if(xtask->state->qid_retry == 0) xtask->state->qid_retry = mmqueue_new(xtask->queue);
        if(xtask->state->qid_extract == 0) xtask->state->qid_extract = mmqueue_new(xtask->queue);
        if(xtask->state->qretry == 0) xtask->state->qretry = mmqueue_new(xtask->queue);
        n = sprintf(path, "%s/%s", dir, XTASK_KMAP_NAME); 
        if((xtask->kmap = mmtrie_init(path)) == NULL)
        {
            FATAL_LOGGER(xtask->logger, "initialize mmtrie[%s] failed, %s", path, strerror(errno));
            _exit(-1);
        }
        n = sprintf(path, "%s/%s", dir, XTASK_MAP_DIR); 
        if((xtask->map = vmap_init(path)) == NULL)
        {
            FATAL_LOGGER(xtask->logger, "initialize vmap[%s] failed, %s", path, strerror(errno));
            _exit(-1);
        }
        if(xtask->state->page_task_wait == 0) 
            xtask->state->page_task_wait = vmap_new_tree(xtask->map);
        if(xtask->state->list_task_wait == 0) 
            xtask->state->list_task_wait = vmap_new_tree(xtask->map);
        if(xtask->state->file_task_wait == 0) 
            xtask->state->file_task_wait = vmap_new_tree(xtask->map);
        if(xtask->state->update_task_wait == 0) 
            xtask->state->update_task_wait = vmap_new_tree(xtask->map);
        if(xtask->state->retry_task_wait == 0) 
            xtask->state->retry_task_wait = vmap_new_tree(xtask->map);
        if(xtask->state->extract_task_wait == 0) 
            xtask->state->extract_task_wait = vmap_new_tree(xtask->map);
        if(xtask->state->host_task_wait == 0) 
            xtask->state->host_task_wait = vmap_new_tree(xtask->map);
        n = sprintf(path, "%s/%s", dir, XTASK_QMAP_DIR); 
        if((xtask->qmap = qmap_init(path)) == NULL)
        {
            FATAL_LOGGER(xtask->logger, "initialize qmap[%s] failed, %s", path, strerror(errno));
            _exit(-1);
        }
        if(xtask->state->qhost_expire == 0)
        {
            xtask->state->qhost_expire = vmap_new_tree(xtask->qmap);
        }
        if(xtask->state->qupdate_cron == 0)
        {
            xtask->state->qupdate_cron = vmap_new_tree(xtask->qmap);
        }
        n = sprintf(path, "%s/%s", dir, XTASK_URLMAP_DIR); 
        if((xtask->urlmap = kmap_init(path)) == NULL)
        {
            FATAL_LOGGER(xtask->logger, "initialize kmap[%s] failed, %s", path, strerror(errno));
            _exit(-1);
        }
        /* db table */
        n = sprintf(path, "%s/%s", dir, XTASK_TABLE_NAME); 
        if((xtask->tableio.fd = open(path, O_CREAT|O_RDWR, 0644)) <= 0
                || fstat(xtask->tableio.fd, &st) != 0)
        {
            if(xtask->tableio.fd > 0) close(xtask->tableio.fd);
            xtask->tableio.fd = 0;
            FATAL_LOGGER(xtask->logger, "open table[%s] failed, %s", path, strerror(errno));
            _exit(-1);
        }
        else
        {
            xtask->tableio.size = sizeof(XTTABLE) * XT_TABLE_MAX;
            xtask->tableio.end = st.st_size;
            if((xtask->tableio.map = mmap(NULL, xtask->tableio.size, PROT_READ|PROT_WRITE,
                            MAP_SHARED, xtask->tableio.fd, 0)) == MAP_FAILED)
            {
                FATAL_LOGGER(xtask->logger, "mmap table[%s] size:%d failed, %s", path, xtask->tableio.size, strerror(errno));
                _exit(-1);
            }
        }
        /* node */
        n = sprintf(path, "%s/%s", dir, XTASK_NODE_NAME); 
        if((xtask->nodeio.fd = open(path, O_CREAT|O_RDWR, 0644)) <= 0
                || fstat(xtask->nodeio.fd, &st) != 0)
        {
            if(xtask->nodeio.fd > 0) close(xtask->nodeio.fd);
            xtask->nodeio.fd = 0;
            FATAL_LOGGER(xtask->logger, "open node[%s] failed, %s", path, strerror(errno));
            _exit(-1);
        }
        else
        {
            xtask->nodeio.size = sizeof(XTNODE) * XT_NODE_MAX;
            xtask->nodeio.end = st.st_size;
            if((xtask->nodeio.map = mmap(NULL, xtask->nodeio.size, PROT_READ|PROT_WRITE,
                            MAP_SHARED, xtask->nodeio.fd, 0)) == MAP_FAILED)
            {
                FATAL_LOGGER(xtask->logger, "mmap node[%s] size:%d failed, %s", path, xtask->nodeio.size, strerror(errno));
                _exit(-1);
            }
        }
        /* proxyio */
        n = sprintf(path, "%s/%s", dir, XTASK_PROXY_NAME); 
        if((xtask->proxyio.fd = open(path, O_CREAT|O_RDWR, 0644)) <= 0
                || fstat(xtask->proxyio.fd, &st) != 0)
        {
            if(xtask->proxyio.fd > 0) close(xtask->proxyio.fd);
            xtask->proxyio.fd = 0;
            FATAL_LOGGER(xtask->logger, "open proxy[%s] failed, %s", path, strerror(errno));
            _exit(-1);
        }
        else
        {
            xtask->proxyio.size = sizeof(XTPROXY) * XT_PROXY_MAX;
            xtask->proxyio.end = st.st_size;
            if((xtask->proxyio.map = mmap(NULL, xtask->proxyio.size, PROT_READ|PROT_WRITE,
                            MAP_SHARED, xtask->proxyio.fd, 0)) == MAP_FAILED)
            {
                FATAL_LOGGER(xtask->logger, "mmap proxy[%s] size:%d failed, %s", path, xtask->proxyio.size, strerror(errno));
                _exit(-1);
            }
        }
        /* dnsio */
        n = sprintf(path, "%s/%s", dir, XTASK_DNS_NAME); 
        if((xtask->dnsio.fd = open(path, O_CREAT|O_RDWR, 0644)) <= 0
                || fstat(xtask->dnsio.fd, &st) != 0)
        {
            if(xtask->dnsio.fd > 0) close(xtask->dnsio.fd);
            xtask->dnsio.fd = 0;
            FATAL_LOGGER(xtask->logger, "open dns[%s] failed, %s", path, strerror(errno));
            _exit(-1);
        }
        else
        {
            xtask->dnsio.size = sizeof(XTDNS) * XT_DNS_MAX;
            xtask->dnsio.end = st.st_size;
            if((xtask->dnsio.map = mmap(NULL, xtask->dnsio.size, PROT_READ|PROT_WRITE,
                            MAP_SHARED, xtask->dnsio.fd, 0)) == MAP_FAILED)
            {
                FATAL_LOGGER(xtask->logger, "mmap dns[%s] size:%d failed, %s", path, xtask->dnsio.size, strerror(errno));
                _exit(-1);
            }
        }
        xtask_queue_dns(xtask);
        /* hostio */
        n = sprintf(path, "%s/%s", dir, XTASK_HOST_NAME); 
        if((xtask->hostio.fd = open(path, O_CREAT|O_RDWR, 0644)) <= 0
                || fstat(xtask->hostio.fd, &st) != 0)
        {
            if(xtask->hostio.fd > 0) close(xtask->hostio.fd);
            xtask->hostio.fd = 0;
            FATAL_LOGGER(xtask->logger, "open host[%s] failed, %s", path, strerror(errno));
            _exit(-1);
        }
        else
        {
            xtask->hostio.size = sizeof(XTHOST) * XT_HOST_MAX;
            xtask->hostio.end = st.st_size;
            if((xtask->hostio.map = mmap(NULL, xtask->hostio.size, PROT_READ|PROT_WRITE,
                            MAP_SHARED, xtask->hostio.fd, 0)) == MAP_FAILED)
            {
                FATAL_LOGGER(xtask->logger, "mmap host[%s] size:%d failed, %s", path, xtask->hostio.size, strerror(errno));
                _exit(-1);
            }
        }
        /* urlnodeio */
        n = sprintf(path, "%s/%s", dir, XTASK_URLNODE_NAME); 
        if((xtask->urlnodeio.fd = open(path, O_CREAT|O_RDWR, 0644)) <= 0
                || fstat(xtask->urlnodeio.fd, &st) != 0)
        {
            if(xtask->urlnodeio.fd > 0) close(xtask->urlnodeio.fd);
            xtask->urlnodeio.fd = 0;
            FATAL_LOGGER(xtask->logger, "open url[%s] failed, %s", path, strerror(errno));
            _exit(-1);
        }
        else
        {
            xtask->urlnodeio.size = sizeof(XTURLNODE) * XT_URLNODE_MAX;
            xtask->urlnodeio.end = st.st_size;
            if((xtask->urlnodeio.map = mmap(NULL, xtask->urlnodeio.size, PROT_READ|PROT_WRITE,
                            MAP_SHARED, xtask->urlnodeio.fd, 0)) == MAP_FAILED)
            {
                FATAL_LOGGER(xtask->logger, "mmap url[%s] size:%d failed, %s", path, xtask->urlnodeio.size, strerror(errno));
                _exit(-1);
            }
        }
        xtask_check_wait(xtask);
        xtask->state->time_start = xtask->state->time_last = time(NULL);
        xtask->state->page_bytes_last = xtask->state->page_bytes_init 
            = xtask->state->page_bytes_total;
        xtask->state->file_bytes_last = xtask->state->file_bytes_init 
            = xtask->state->file_bytes_total;
        xtask->state->list_bytes_last = xtask->state->list_bytes_init 
            = xtask->state->list_bytes_total;
        xtask->state->update_bytes_last = xtask->state->update_bytes_init 
            = xtask->state->update_bytes_total;
        xtask->state->repeat_bytes_last = xtask->state->repeat_bytes_init
            = xtask->state->repeat_bytes_total;
    }
    return xtask;
}

/* close xtask */
void xtask_clean(XTASK *xtask)
{
    int i = 0;

    if(xtask)
    {
        for(i = 1; i <= xtask->state->table_id_max; i++)
        {
            if(xtask->outs[i].fd > 0)close(xtask->outs[i].fd);
            if(xtask->outs[i].csv > 0)close(xtask->outs[i].csv);
        }
        if(xtask->state) munmap(xtask->state, sizeof(XTSTATE));
        xtask->state = NULL;
        if(xtask->stateio.fd > 0) close(xtask->stateio.fd);
        xtask->stateio.fd = 0;
        if(xtask->tableio.map) munmap(xtask->tableio.map, xtask->tableio.size);
        if(xtask->tableio.fd > 0) close(xtask->tableio.fd);
        if(xtask->nodeio.map) munmap(xtask->nodeio.map, xtask->nodeio.size);
        if(xtask->nodeio.fd > 0) close(xtask->nodeio.fd);
        if(xtask->proxyio.map) munmap(xtask->proxyio.map, xtask->proxyio.size);
        if(xtask->proxyio.fd > 0) close(xtask->proxyio.fd);
        if(xtask->dnsio.map) munmap(xtask->dnsio.map, xtask->dnsio.size);
        if(xtask->dnsio.fd > 0) close(xtask->dnsio.fd);
        if(xtask->hostio.map) munmap(xtask->hostio.map, xtask->hostio.size);
        if(xtask->hostio.fd > 0) close(xtask->hostio.fd);
        if(xtask->urlnodeio.map) munmap(xtask->urlnodeio.map, xtask->urlnodeio.size);
        if(xtask->urlnodeio.fd > 0) close(xtask->urlnodeio.fd);
        db_clean(xtask->temp);
        db_clean(xtask->db);
        db_clean(xtask->mmdb);
        mmqueue_clean(xtask->queue);
        mmtrie_clean(xtask->kmap);
        vmap_close(xtask->map);
        qmap_close(xtask->qmap);
        kmap_close(xtask->urlmap);
        LOGGER_CLEAN(xtask->logger);
        MUTEX_DESTROY(xtask->mutex);
        MUTEX_DESTROY(xtask->table_mutex);
        MUTEX_DESTROY(xtask->node_mutex);
        MUTEX_DESTROY(xtask->proxy_mutex);
        MUTEX_DESTROY(xtask->dns_mutex);
        MUTEX_DESTROY(xtask->host_mutex);
        MUTEX_DESTROY(xtask->urlnode_mutex);
        MUTEX_DESTROY(xtask->download_mutex);
        MUTEX_DESTROY(xtask->extract_mutex);
        MUTEX_DESTROY(xtask->record_mutex);
        MUTEX_DESTROY(xtask->block_mutex);
        MUTEX_DESTROY(xtask->dump_mutex);
        MUTEX_DESTROY(xtask->csv_mutex);
#ifdef HAVE_PTHREAD
        for(i = 0; i < XT_MUTEX_MAX; i++)
        {
            pthread_mutex_destroy(&(xtask->mutexs[i]));
            pthread_mutex_destroy(&(xtask->hmutexs[i]));
        }
#endif
        xmm_free(xtask, sizeof(XTASK));
    }
    return ;
}

#ifdef _DEBUG_XTASK
#include "mtrie.h"
#include "http.h"
int main(int argc, char **argv)
{
    char line[XT_LINE_MAX], *host = NULL, name[XT_NAME_MAX], *url = NULL;
    int id = 0, i = 0, x = 0, n = 0;
    unsigned char *s = NULL;
    XTNODE nodes[XT_NODES_MAX];
    XTPROXY proxy = {0};
    XTASK *xtask = NULL;

    if((xtask = xtask_init("/tmp/xtask")))
    {
#ifdef _DEBUG_HOST
        x = 110;
        for(i = 1; i < 20; i++)
        {
            n = x + (i << 8);  
            s = (unsigned char *)&n;
            n = sprintf(line, "%d.%d.%d.%d:8080", s[0], s[1], s[2], s[3]);
            id = xtask_add_proxy(xtask, line);
            if(id != i)
            {
                fprintf(stdout, "%d=add_proxy(%s)\r\n", id, line); 
            }
        }
        xtask_set_proxy_status(xtask, 11, NULL, 2);
        if((n = xtask_view_proxylist(xtask, line, XT_LINE_MAX)) > 0)
        {
            fprintf(stdout, "%s\r\n", line);
        }
        n = x + (11 << 8);  
        s = (unsigned char *)&n;
        n = sprintf(line, "%d.%d.%d.%d:8080", s[0], s[1], s[2], s[3]);
        xtask_set_proxy_status(xtask, 0, line, -1);
        if((n = xtask_view_proxylist(xtask, line, XT_LINE_MAX)) > 0)
        {
            fprintf(stdout, "%s\r\n", line);
        }
        xtask_del_proxy(xtask, 12, NULL);
        if((n = xtask_view_proxylist(xtask, line, XT_LINE_MAX)) > 0)
        {
            fprintf(stdout, "%s\r\n", line);
        }
        n = x + (14 << 8);  
        s = (unsigned char *)&n;
        n = sprintf(line, "%d.%d.%d.%d:8080", s[0], s[1], s[2], s[3]);
        xtask_del_proxy(xtask, 0, line);
        if((n = xtask_view_proxylist(xtask, line, XT_LINE_MAX)) > 0)
        {
            fprintf(stdout, "%s\r\n", line);
        }
        /*
        while(1)
        {
            if((n = xtask_get_proxy(xtask, &proxy)))
            {
                s = (unsigned char *)&(proxy.ip);
                fprintf(stdout, "get_proxy(%d) => [%d.%d.%d.%d:%d](%d)\r\n", n, s[0], s[1], s[2], s[3], proxy.port, proxy.status);
            }
            sleep(1);
        }
        */
        host = "202.106.0.20";
        if((id = xtask_add_dns(xtask, host)) > 0) 
        {
            fprintf(stdout, "add_dns(%s) => %d/%d\r\n", host, id, xtask->state->dns_id_max);
        }
        id = xtask_set_dns_status(xtask, id, NULL, 2);
        if((n = xtask_view_dnslist(xtask, line, XT_LINE_MAX)) > 0)
        {
            fprintf(stdout, "%s\r\n", line);
        }
        host = "202.106.0.21";id = xtask_add_dns(xtask, host);
        host = "202.106.0.22";id = xtask_add_dns(xtask, host);
        host = "202.106.0.23";id = xtask_add_dns(xtask, host);
        host = "202.106.0.24";id = xtask_add_dns(xtask, host);
        host = "202.106.0.25";id = xtask_add_dns(xtask, host);
        host = "202.106.0.26";id = xtask_add_dns(xtask, host);
        host = "202.106.0.27";id = xtask_add_dns(xtask, host);
        host = "202.106.0.28";id = xtask_add_dns(xtask, host);
        host = "202.106.0.29";id = xtask_add_dns(xtask, host);
        host = "202.106.0.23"; id = xtask_set_dns_status(xtask, 0, host, -1);
        host = "202.106.0.26"; id = xtask_del_dns(xtask, 0, host);
        id = 2;id = xtask_del_dns(xtask, id, NULL);//202.106.0.21
        if((n = xtask_view_dnslist(xtask, line, XT_LINE_MAX)) > 0)
        {
            fprintf(stdout, "%s\r\n", line);
        }
#endif
#ifdef _DEBUG_TABLE
        /* add table */
        if((id = xtask_add_table(xtask, "info")) > 0) 
        {
            n = xtask_add_field(xtask, id, IB_DATATYPE_TEXT|IB_IS_NEED_INDEX, "title");
            n = xtask_add_field(xtask, id, IB_DATATYPE_TEXT|IB_IS_NEED_INDEX, "title");
            n = xtask_add_field(xtask, id, IB_DATATYPE_TEXT|IB_IS_NEED_INDEX, "content");
            n = xtask_add_field(xtask, id, IB_DATATYPE_TEXT|IB_IS_NEED_INDEX, "author");
            n = xtask_add_field(xtask, id, IB_DATATYPE_INT|IB_IS_NEED_INDEX, "time");
            n = xtask_add_field(xtask, id, IB_DATATYPE_LONG|IB_IS_NEED_INDEX, "size");
            n = xtask_add_field(xtask, id, IB_DATATYPE_DOUBLE|IB_IS_NEED_INDEX, "rank");
            n = xtask_add_field(xtask, id, IB_DATATYPE_TEXT, "url");
            n = xtask_add_field(xtask, id, IB_DATATYPE_TEXT, "img");
        }
        if((id = xtask_add_table(xtask, "back")) > 0) 
        {
            n = xtask_add_field(xtask, id, IB_DATATYPE_TEXT|IB_IS_NEED_INDEX, "title");
            n = xtask_add_field(xtask, id, IB_DATATYPE_TEXT|IB_IS_NEED_INDEX, "title");
            n = xtask_add_field(xtask, id, IB_DATATYPE_TEXT|IB_IS_NEED_INDEX, "content");
            n = xtask_add_field(xtask, id, IB_DATATYPE_TEXT|IB_IS_NEED_INDEX, "author");
            n = xtask_add_field(xtask, id, IB_DATATYPE_INT|IB_IS_NEED_INDEX, "time");
            n = xtask_add_field(xtask, id, IB_DATATYPE_LONG|IB_IS_NEED_INDEX, "size");
            n = xtask_add_field(xtask, id, IB_DATATYPE_DOUBLE|IB_IS_NEED_INDEX, "rank");
            n = xtask_add_field(xtask, id, IB_DATATYPE_TEXT, "url");
            n = xtask_add_field(xtask, id, IB_DATATYPE_TEXT, "img");
            if((n = xtask_view_database(xtask, id, line, XT_LINE_MAX)) > 0)
            {
                fprintf(stdout, "add_fields():\r\n%s\r\n", line);
            }
        }
        n = xtask_delete_field(xtask, 2, 64);
        if((n = xtask_view_database(xtask, line, XT_LINE_MAX)) > 0)
        {
            fprintf(stdout, "del_field(2,64):\r\n%s\r\n", line);
        }
        n = xtask_rename_table(xtask, 2, "item");
        if((n = xtask_view_database(xtask, line, XT_LINE_MAX)) > 0)
        {
            fprintf(stdout, "rename_table(2):\r\n%s\r\n", line);
        }
        n = xtask_delete_table(xtask, 2);
        if((n = xtask_view_database(xtask, line, XT_LINE_MAX)) > 0)
        {
            fprintf(stdout, "delete_table(2):\r\n%s\r\n", line);
        }
#endif
#ifdef _DEBUG_NODE
        //add node 
        for(i = 1; i < 10000; i++)
        {
            sprintf(name, "node_%d", i);
            x = random()%i;
            if((n = xtask_add_node(xtask, x, name)) >= 0)
            {
                fprintf(stdout, "%d::add node:%d to node:%d\n", n, i, x);
            }
        }
        xtask_list_nodes(xtask, 0, stdout);
        if((n = xtask_view_node_childs(xtask, 3, line, XT_LINE_MAX)) > 0)
        {
            fprintf(stdout, "view_childs(3):\r\n%s\r\n", line);
        }
        if((n = xtask_get_node_childs(xtask, 3, nodes)) > 1)
        {
            xtask_update_node(xtask, 3, nodes[0].id, "node_null");
            if((n = xtask_view_node_childs(xtask, 3, line, XT_LINE_MAX)) > 0)
            {
                fprintf(stdout, "update_node(3,%d):\r\n%s\r\n", nodes[0].id, line);
            }
            n = xtask_delete_node(xtask, nodes[1].id);
            if((n = xtask_view_node_childs(xtask, 3, line, XT_LINE_MAX)) > 0)
            {
                fprintf(stdout, "delete_node(3,%d):\r\n%s\r\n", nodes[1].id, line);
            }
        }
        //xtask_list_nodes(xtask, 0, stdout);
#endif
#ifdef _DEBUG_URL
//gcc -o task xtask.c utils/*.c -I utils/ -D_DEBUG_URL -D_DEBUG_XTASK -lz  -lchardet
        url = "http://www.newsmth.net/nForum/article/Modern_CHN/346525";
        id = xtask_add_url(xtask, 0, 1, url, REG_IS_LINK);
        fprintf(stdout, "url:%s => %d\r\n", url, id);
        id = xtask_add_url(xtask, 0, 1, url, REG_IS_LINK);
        fprintf(stdout, "url:%s => %d\r\n", url, id);
#endif
#ifdef _DEBUG_COOKIE
#include "stime.h"
    int hostid = xtask__hostid(xtask, "lt.cjdby.net");
    time_t now = 0, date = 0;
    int ncookie = 0;
    char cookie[XT_COOKIE_SIZE];
    HTTP_RESPONSE http_resp = {0};
    char headers[XT_LINE_MAX], *end = NULL, *p = NULL;
    strcpy(headers, "HTTP/1.0 403 Forbidden\r\nConnection:keep-alive\r\nContent-Encoding:gzip\r\nContent-Type:text/html; charset=gbk\r\nDate:Sun, 23 Dec 2012 12:51:50 GMT\r\nServer:Tengine\r\nSet-Cookie:miM4_faa5_lastact=1356267109%09forum.php%09forumdisplay; expires=Mon, 24-Dec-2012 12:51:49 GMT; path=/; domain=.cjdby.net\r\nSet-Cookie:miM4_faa5_stats_qc_reg=deleted; expires=Thu, 01-Jan-1970 00:00:01 GMT; path=/; domain=.cjdby.net\r\nSet-Cookie:miM4_faa5_cloudstatpost=deleted; expires=Thu, 01-Jan-1970 00:00:01 GMT; path=/; domain=.cjdby.net\r\nSet-Cookie:miM4_faa5_forum_lastvisit=sdfsdfdf; expires=Sun, 30-Dec-2012 12:51:49 GMT; path=/; domain=.cjdby.net\r\nSet-Cookie:miM4_faa5_forum_lastvisit=mmmmmmm; expires=Sun, 23-Dec-2012 12:51:50 GMT; path=/; domain=.cjdby.net\r\nTransfer-Encoding:chunked\r\nVary:Accept-Encoding\r\nVary:Accept-Encoding\r\nX-Powered-By:PHP/5.3.19\r\n\r\n");
    end = headers + strlen(headers);
    void *map = http_headers_map_init();
    if(http_response_parse(headers, end, &http_resp, map) != -1)
    {
        fprintf(stdout, "---------------------HEADERS---------------------\n");
        for(i = 0; i < HTTP_HEADER_NUM; i++)
        {
            if((n = http_resp.headers[i]) > 0)
            {
                fprintf(stdout, "%s %s\n", http_headers[i].e, http_resp.hlines + n);
            }
        }
        fprintf(stdout, "---------------------HEADERS END---------------------\n");
        /* date */
        if((n = http_resp.headers[HEAD_GEN_DATE]))
        {
            date = (unsigned int)str2time(http_resp.hlines + n);
        }
        fprintf(stdout, "hostid:%d %u\n", hostid, (unsigned int)date);
        /* cookie */
        if(http_resp.ncookies > 0)
        {
            ncookie = http_cookie_line(&http_resp, cookie);
            fprintf(stdout, "%s::%d cookie_line(%s)\n", __FILE__, __LINE__, cookie);
            id = xtask_parse_cookie(xtask, hostid, date, cookie, ncookie);
            fprintf(stdout, "%s::%d parse_cookie(%d) => %d\n", __FILE__, __LINE__, hostid, id);
            if((n = xtask_get_host_cookie(xtask, hostid, cookie)) > 0)
            {
                fprintf(stdout, "%s::%d cookies => %s\n", __FILE__, __LINE__, cookie);
            }
            xtask_del_host_cookie(xtask, hostid);
            if((n = xtask_get_host_cookie(xtask, hostid, cookie)) > 0)
            {
                fprintf(stdout, "%s::%d cookies => %s\n", __FILE__, __LINE__, cookie);
            }
        }
    }
    http_headers_map_clean(map);
#endif
        xtask_clean(xtask);
        return 0;
    }
    return -1;
}
#endif
