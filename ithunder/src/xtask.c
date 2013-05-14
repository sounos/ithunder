#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <ibase.h>
#include "stime.h"
#include "xtask.h"
#include "xmm.h"
#include "mutex.h"
#include "mmtrie.h"
#include "vmap.h"
#include "kmap.h"
#include "mmqueue.h"
#include "logger.h"
#include "mmtrie.h"
#include "base64.h"
#include "http.h"
#include "md5.h"
#include "db.h"
#include "http.h"
#define XTASK_MMDB_DIR      "mmdb" 
#define XTASK_DB_DIR        "db" 
#define XTASK_RES_DIR       "res" 
#define XTASK_TEMP_DIR      "temp" 
#define XTASK_URLMAP_DIR    "urlmap"
#define XTASK_MAP_DIR       "map"
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
            n = vmap_try_insert(xtask->map, nodelist[parent].childs_root, k, id, &old); 
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
            n = vmap_try_insert(xtask->map, nodelist[parent].childs_root, k, nodeid, &old); 
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
           template->mmid = vmap_insert(xtask->map, 
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

/* delete template */
int xtask_delete_template(XTASK *xtask, int nodeid, int templateid)
{
    XTNODE *nodelist = NULL;
    int ret = -1;

    if(xtask && templateid > 0 && nodeid > 0 
            && (nodelist = (XTNODE *)(xtask->nodeio.map)))
    {
        MUTEX_LOCK(xtask->node_mutex);
        if(nodeid <= xtask->state->node_id_max 
            && templateid <= xtask->state->template_id_max) 
        {
            vmap_remove(xtask->map, nodelist[nodeid].templates_root, templateid,NULL,NULL);
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
    int ret = -1, x = 0, k = 0, id = 0;
    XTNODE *nodelist = NULL;

    if(xtask && nodeid > 0 && nodeid <= xtask->state->node_id_max 
            && (nodelist = (XTNODE *)(xtask->nodeio.map)))
    {
        if(nodelist[nodeid].nchilds > 0 &&  nodelist[nodeid].childs_root > 0
                && (x = vmap_min(xtask->map, nodelist[nodeid].childs_root, &id, &k)))
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
            }while((x = vmap_next(xtask->map, nodelist[nodeid].childs_root, x, &id, &k)) > 0);
            vmap_remove_tree(xtask->map, nodelist[nodeid].childs_root);
            if((x = vmap_min(xtask->map, nodelist[nodeid].templates_root, &id, &k)) > 0)
            {
                do
                {
                    db_del_data(PDB(xtask->temp), id);
                }while((x = vmap_next(xtask->map, 
                                nodelist[nodeid].templates_root, x, &id, &k)) > 0);
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
    int ret = -1, k = 0, id = 0, parent = 0;
    XTNODE *nodelist = NULL;

    if(xtask && nodeid >= 0  && (nodelist = (XTNODE *)(xtask->nodeio.map))) 
    {
        MUTEX_LOCK(xtask->node_mutex);
        if(nodeid != 0 && (parent = nodelist[nodeid].parent) >= 0)
        {
            vmap_remove(xtask->map,nodelist[parent].childs_root, nodelist[nodeid].mapid, &k, &id);
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
    int ret = -1, n = 0, k = 0, x = 0, id = 0;
    XTNODE *nodelist = NULL, *p = NULL;

    if(xtask && nodeid >= 0 && (p = node) && (nodelist = (XTNODE *)(xtask->nodeio.map))) 
    {
        MUTEX_LOCK(xtask->node_mutex);
        if(nodeid <= xtask->state->node_id_max && xtask->state->node_total > 0
                && nodelist[nodeid].nchilds > 0 &&  nodelist[nodeid].childs_root > 0
                && (x = vmap_min(xtask->map, nodelist[nodeid].childs_root, &id, &k)) >0)
        {
            do
            {
                memcpy(p, &(nodelist[id]), sizeof(XTNODE));
                ++p;
                ++n;
            }while((x = vmap_next(xtask->map, nodelist[nodeid].childs_root, x, &id, &k)) > 0);
            ret = n;
        }
        MUTEX_UNLOCK(xtask->node_mutex);
    }
    return ret;
}

/* view childs node */
int xtask_view_node_childs(XTASK *xtask, int nodeid, char *out, int nout)
{
    int ret = -1,  k = 0, x = 0, id = 0;
    char *p = NULL, *end = NULL;
    XTNODE *nodelist = NULL;

    if(xtask && nodeid >= 0 &&  (p = out)  && (end = (out + nout)) > out 
            && (nodelist = (XTNODE *)(xtask->nodeio.map))) 
    {
        MUTEX_LOCK(xtask->node_mutex);
        if(nodeid <= xtask->state->node_id_max && xtask->state->node_total > 0
                && nodelist[nodeid].nchilds > 0 &&  nodelist[nodeid].childs_root > 0
                && (x = vmap_min(xtask->map, nodelist[nodeid].childs_root, &id, &k)) )
        {
            p += sprintf(p, "({'id':'%d', 'uid':'%d', 'parent':'%d', 'nchilds':'%d', 'childs':[",
                    nodeid,  nodelist[nodeid].uid, nodelist[nodeid].parent, nodelist[nodeid].nchilds);
            do
            {
                p += sprintf(p, "{'id':'%d', 'uid':'%d', 'name':'%s','nchilds':'%d'},",
                        id,  nodelist[id].uid, nodelist[id].name, nodelist[id].nchilds);
            }while((x = vmap_next(xtask->map, nodelist[nodeid].childs_root, x, &id, &k)) > 0);
            --p;
            p += sprintf(p, "]})");
            ret = p - out;
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
            if(old == 0)
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
                        nodelist[nodeid].urlroot, urlid, NULL) > 0) )
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
    int ret = -1,  k = 0, x = 0, id = 0, n = 0, npages = 0;
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
                && (x = vmap_min(xtask->map, nodelist[nodeid].urlroot, &id, &k)))
        {
            npages = (nodelist[nodeid].nurlchilds / XT_PAGE_NUM) 
                + ((nodelist[nodeid].nurlchilds%XT_PAGE_NUM)  > 0);
            p += sprintf(p, "({'id':'%d', 'nchilds':'%d', 'pages':'%d','childs':{", 
                nodeid, nodelist[nodeid].nurlchilds, npages);
            do
            {
                if(n >= from && n < to)
                {
                    p += sprintf(p, "'%d':{'id':'%d','nchilds':'%d','level':'%d',"
                        "'nodeid':'%d','url':'", id, id, urlnodelist[id].nchilds, 
                        urlnodelist[id].level, urlnodelist[id].nodeid);
                    if((n = db_read_data(PDB(xtask->mmdb), urlnodelist[id].url_db_id, p)) > 0)
                        p += n;
                    p += sprintf(p, "'},");
                }
                n++;
            }while(n<to&&(x=vmap_next(xtask->map,nodelist[nodeid].urlroot,x,&id,&k)) > 0);
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
        p += sprintf(p, "({\"proxylist\":{");  
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
        p += sprintf(p, "}})");
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
            dnslist[id].status = XT_STATUS_OK;
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
    int ret = -1, i = 0, x = 0, n = 0, id = 0;
    XTHOST *hostlist  = NULL;
    char line[XT_LINE_MAX];

    if(xtask && host && xtask->state && xtask->kmap 
            && (hostlist = (XTHOST *)(xtask->hostio.map)) 
            && (n = sprintf(line, "h:%s", host)) > 0)
    {
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
            hostlist[id].status = HOST_STATUS_READY;
            n = sprintf(line, "host:%d", id);
            hostlist[id].db_host_id = db_xadd_data(PDB(xtask->mmdb), line, n, host, strlen(host));
            mmqueue_push(xtask->queue, xtask->state->qhost, id);
            xtask->state->host_total++;
        }
        if(id > xtask->state->host_id_max) xtask->state->host_id_max = id;
        MUTEX_UNLOCK(xtask->host_mutex);
        //ret = id;
    }
    return ret;
}

/* pop host */
int xtask_pop_host(XTASK *xtask, char *host)
{
    int ret = -1, id = 0, n = 0;
    XTHOST *hostlist = NULL;

    if(xtask && host && (hostlist = (XTHOST *)(xtask->hostio.map)))
    {
        MUTEX_LOCK(xtask->host_mutex);  
        while(mmqueue_pop(xtask->queue, xtask->state->qhost, &id) > 0)
        {
            if(id > 0 && hostlist[id].status == HOST_STATUS_READY 
                    && (n = db_read_data(PDB(xtask->mmdb), hostlist[id].db_host_id, host)) > 0)
            {
                host[n] = '\0';
                vmap_try_insert(xtask->map, xtask->state->host_task_wait, id, id, NULL);
                //mmqueue_push(xtask->queue, xtask->state->qhost, id);
                ret = id;
                break;
            }
        }
        MUTEX_UNLOCK(xtask->host_mutex);
    }
    return ret;
}

/* set host iplist */
int xtask_set_host_ip(XTASK *xtask, char *host, int *iplist, int niplist)
{
    int ret = -1, hostid = 0, k = 0, n = 0;
    XTHOST *hostlist = NULL;
    char line[XT_LINE_MAX];

    if(xtask && (hostid = xtask__hostid(xtask, host))> 0 && iplist && niplist > 0
        && (hostlist = (XTHOST *)(xtask->hostio.map)))
    {
        MUTEX_LOCK(xtask->host_mutex);
        if(hostid <= xtask->state->host_id_max)
        {
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
            hostlist[hostid].status = HOST_STATUS_OK;
            if(hostlist[hostid].level > 0)
                mmqueue_push(xtask->queue, xtask->state->qpriority, hostid);
            else
                mmqueue_push(xtask->queue, xtask->state->qwait, hostid);
            if((k = vmap_find(xtask->map, xtask->state->host_task_wait, hostid, NULL)) > 0)
                vmap_remove(xtask->map, xtask->state->host_task_wait, k, NULL, NULL);
        }
        MUTEX_UNLOCK(xtask->host_mutex);
    }
    return ret;
}

/* reset host queue */
void xtask_reset_host(XTASK *xtask, int hostid)
{
    int mid = 0;

    if(xtask && hostid > 0)
    {
        if((mid = vmap_find(xtask->map, xtask->state->host_task_wait, hostid, NULL) > 0))
        {
            mmqueue_push(xtask->queue, xtask->state->qhost, hostid);
            vmap_remove(xtask->map, xtask->state->host_task_wait, mid, NULL, NULL);
        }
    }
    return ;
}

/* get host ip */
int xtask_get_host_ip(XTASK *xtask, char *host)
{
    int ret = -1, k = 0, n = 0, hostid = 0, iplist[XT_IP_MAX];
    XTHOST *hostlist = NULL;

    if(xtask && host && (hostlist = (XTHOST *)(xtask->hostio.map))
        && (hostid = xtask__hostid(xtask, host)) > 0)
    {
        MUTEX_LOCK(xtask->host_mutex);
        //fprintf(stdout, "%s::%d hostid:%d host:%s\r\n", __FILE__, __LINE__, hostid, host);
        if(hostid <= xtask->state->host_id_max && hostlist[hostid].status == HOST_STATUS_READY
            && (n = db_read_data(PDB(xtask->mmdb), hostlist[hostid].db_ip_id, (char *)iplist)) > 0)
        {
            k = random()%(n/sizeof(int));
            ret = iplist[k];
            //s = (unsigned char *)iplist[k];
            //n = sprintf(ip, "%d.%d.%d.%d", s[0], s[1], s[2], s[3]);
            ret = hostid;
        }
        MUTEX_UNLOCK(xtask->host_mutex);
    }
    return ret;
}


int xtask_host_ip(XTASK *xtask, int hostid)
{
    int ret = 0, k = 0, n = 0, iplist[XT_IP_MAX];
    XTHOST *hostlist = NULL;

    if(xtask && hostid > 0 && (hostlist = (XTHOST *)(xtask->hostio.map)))
    {
        MUTEX_LOCK(xtask->host_mutex);
        if(hostid <= xtask->state->host_id_max 
                && hostlist[hostid].status == HOST_STATUS_OK
                && (n = db_read_data(PDB(xtask->mmdb), 
                        hostlist[hostid].db_ip_id, (char *)iplist)) > 0)
        {
            k = random()%(n/sizeof(int));
            ret = iplist[k];
        }
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
            xtask_reset_host(xtask, hostid);
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

/* merge cookie */
int xtask_merge_cookie(XTASK *xtask, XTKV *cookies, int k, char *p, char *end)
{
    int i = 0, x = 0, old = -1, id = 0;

    if(xtask && cookies)
    {
        while(p < end)
        {
            while(*p == 0x20 || *p == '\t')++p;
            cookies[k].k = p;
            while(*p != '=' && p < end)++p;
            if(*p == '=')
            {
                cookies[k].nk = p - cookies[k].k;
                x = xtask->state->cookie_uid_max+1;
                old = -1;
                if((id = mmtrie_add(xtask->kmap, cookies[k].k, cookies[k].nk, x)) != x)
                {
                    i = 0;
                    while(i < k)
                    {
                        if(cookies[i].id == id){old = i;break;}
                        ++i;
                    }
                }
                else
                    x = xtask->state->cookie_uid_max++;
                cookies[k].v = ++p;
                while(p < end && *p != ';')++p;
                cookies[k].nv = p++ - cookies[k].v;
                if(old == -1 && id > 0) ++k;
            }
        }
    }
    return k;
}

/* set host cookie */
int xtask_add_host_cookie(XTASK *xtask, int hostid, char *cookie, int ncookie)
{
    char line[COOKIE_LINE_MAX], buf[COOKIE_LINE_MAX], *p = NULL, *end = NULL;
    XTKV cookies[XT_COOKIE_MAX];
    XTHOST *hostlist = NULL;
    int ret = -1, k = 0, n = 0, i = 0;

    if(xtask && hostid > 0 && (p = cookie) && (end = (p + ncookie)) > p 
            && (hostlist = (XTHOST *)(xtask->hostio.map)))
    {
        MUTEX_LOCK(xtask->host_mutex);
        if(hostid <= xtask->state->host_id_max)
        {
            if(hostlist[hostid].db_cookie_id == 0)
            {
                n = sprintf(line, "cookie:%d", hostid);
                hostlist[hostid].db_cookie_id = db_xadd_data(PDB(xtask->mmdb), line, n, 
                        cookie, ncookie); 
            }
            else
            {
                memset(cookies, 0, sizeof(XTKV) * XT_COOKIE_MAX);
                k = 0;
                k = xtask_merge_cookie(xtask, cookies, k, p, end);
                if((n = db_read_data(PDB(xtask->mmdb), hostlist[hostid].db_cookie_id, line)) > 0)
                {
                    k = xtask_merge_cookie(xtask, cookies, k, line, line + n);
                }
                p = buf;
                while(i < k)
                {
                    p += sprintf(p, "%.*s=%.*s;", cookies[i].nk, cookies[i].k, 
                            cookies[i].nv, cookies[i].v);
                    ++i;
                }
                n = db_set_data(PDB(xtask->mmdb), hostlist[hostid].db_cookie_id, 
                        buf, p - buf); 
            }
            ret = hostid;
        }
        MUTEX_UNLOCK(xtask->host_mutex);
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

/* get host cookie */
int xtask_get_host_cookie(XTASK *xtask, int hostid, char *out, int nout)
{
    XTHOST *hostlist = NULL;
    int ret = -1;

    if(xtask && hostid > 0 && (hostlist = (XTHOST *)(xtask->hostio.map)))
    {
        MUTEX_LOCK(xtask->host_mutex);
        if(hostid <= xtask->state->host_id_max && hostlist[hostid].db_cookie_id > 0 
                && db_get_data_len(PDB(xtask->mmdb), hostlist[hostid].db_cookie_id) < nout)
        {
            ret = db_read_data(PDB(xtask->mmdb), hostlist[hostid].db_cookie_id, out);
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
        MUTEX_LOCK(xtask->urlnode_mutex);
        if(hostlist[hostid].urlroot == 0) hostlist[hostid].urlroot = vmap_new_tree(xtask->map);
        k = vmap_try_insert(xtask->map, hostlist[hostid].urlroot, urlid, hostid, &old);   
        urlnodelist[urlid].hostid = hostid;
        urlnodelist[urlid].host_mid = k;
        if(!(flag & REG_IS_FILE) && old == 0)
        {
            if(hostlist[hostid].qwait == 0)
                hostlist[hostid].qwait = mmqueue_new(xtask->queue);
            if(hostlist[hostid].status == HOST_STATUS_OK
                && mmqueue_total(xtask->queue, hostlist[hostid].qwait) == 0)
            {
                if(hostlist[hostid].level > 0)
                    mmqueue_push(xtask->queue, xtask->state->qpriority, hostid);
                else
                    mmqueue_push(xtask->queue, xtask->state->qwait, hostid);
            }
            if(!(flag & REG_IS_LIST)) mmqueue_push(xtask->queue, hostlist[hostid].qwait, urlid);
            hostlist[hostid].nurlchilds++;
        }
        ret = urlid;
        MUTEX_UNLOCK(xtask->urlnode_mutex);
    }
    return ret;
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
        if((id = vmap_find(xtask->map, hostlist[hostid].urlroot, urlid, NULL) > 0) )
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

    if(xtask && parent >= 0 && nodeid && url && (n = strlen(url)) > 0
            && (urlnodelist = (XTURLNODE *)(xtask->urlnodeio.map)))
    {
        md5((unsigned char *)url, n, digest); 
        tmp = (int64_t *)digest; key = *tmp;
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
                //fprintf(stdout, "%s::%d url:%s k:%d old:%d\r\n", __FILE__, __LINE__, url, k, old);
                xtask->state->urlnode_total++;
                if(id > xtask->state->urlnode_id_max) xtask->state->urlnode_id_max = id;
                if(flag & REG_IS_FILE)
                    mmqueue_push(xtask->queue, xtask->state->qfile, id);
                else if(flag & REG_IS_LIST)
                    mmqueue_push(xtask->queue, xtask->state->qlist, id);
                //WARN_LOGGER(xtask->logger, "urlid:%d parent:%d", id, parent);
            }
            else 
            {
                id = old;
                //fprintf(stdout, "%s::%d url:%s old:%d\r\n", __FILE__, __LINE__, url, old);
            }
            if(urlnodelist[parent].childs_root == 0)
                urlnodelist[parent].childs_root = vmap_new_tree(xtask->map);
            urlnodelist[id].childs_mid = vmap_try_insert(xtask->map, 
                    urlnodelist[parent].childs_root, id, k, &old);
            if(old == 0)
            {
                urlnodelist[parent].nchilds++;
                urlnodelist[parent].status = XT_STATUS_OK;
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
        MUTEX_LOCK(xtask->record_mutex); 
        if(urlid <= xtask->state->urlnode_id_max && (mm = xtask_pop_block(xtask)))
        {
            record = (XTRECORD *)block;
            memset(block, 0, sizeof(XTRECORD));
            p = (char *)block + sizeof(XTRECORD);
            record->id = urlid;
            record->tableid = item->tableid;
            pp += sizeof(XTITEM);
            //WARN_LOGGER(xtask->logger, "urlid:%d nold:%d tableid:%d", urlid, n, item->tableid);
            for(i = 0; i < item->count; i++)
            {
                if((k = item->regs[i].fieldid) >= 0 && item->regs[i].length > 0)
                {
                    record->mms[k].off = p - block;
                    memcpy(p, pp, item->regs[i].length);
                    p += item->regs[i].length;
                    *p++ = '\0';
                    record->mms[k].length = item->regs[i].length;
                }
                //DEBUG_LOGGER(xtask->logger, "urlid:%d i:%d fieldid:%d flag:%d", urlid, i, k, item->regs[i].flag);
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
                        memcpy(p, mm + rec->mms[i].off, rec->mms[i].length);
                        p += rec->mms[i].length;
                        *p++ = '\0';
                        record->mms[i].length = rec->mms[i].length;
                    }
                }
            }
            record->length = p - block;
            ret = db_set_data(PDB(xtask->res), urlid, block, record->length);
        }
        MUTEX_UNLOCK(xtask->record_mutex); 
        if(block)xtask_push_block(xtask, block);
        if(mm)xtask_push_block(xtask, mm);
    }
    return ret;
}

/* get record */
int xtask_get_record(XTASK *xtask, int urlid, char *mm)
{
    int n = 0, i = 0, ret = -1, parent = 0;
    XTRECORD *record = NULL, *rec = NULL;
    char *p = NULL, *m = NULL;
    XTURLNODE urlnode = {0};
    XTTABLE table = {0};

    if(xtask && urlid > 0 && mm && (m = xtask_pop_block(xtask)))
    {
        record = (XTRECORD *)mm;
        if(xtask_get_urlnode(xtask, urlid, &urlnode) > 0  
                && (n = db_read_data(PDB(xtask->res), urlid, mm)) > 0
                && (xtask_get_table(xtask, record->tableid, &table)) > 0)
        {
            p = mm + record->length;
            while((parent = urlnode.parent) > 0 && xtask_get_urlnode(xtask, parent,&urlnode)>0)
            {
                rec = (XTRECORD *)m;
                if(db_read_data(PDB(xtask->res), urlnode.brother, m) > 0)
                {
                    for(i = 0; i < XT_FIELDS_MAX; i++) 
                    {
                        if(record->mms[i].length == 0 && rec->mms[i].length > 0)
                        {
                            //WARN_LOGGER(xtask->logger, "urlid:%d field:%s => %s", urlid, table.fields[i].name, m + rec->mms[i].off);
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
        if(m) xtask_push_block(xtask, m);
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

/* set dump dir */
void xtask_set_dumpdir(XTASK *xtask, char *dir)
{
    char path[XT_PATH_MAX];
    int n = 0;

    if(xtask && dir)
    {
        strcpy(xtask->state->dumpdir, dir);
        n = sprintf(path, "%s/%d.doc", dir, xtask->state->dump_id_max);
        xtask_pmkdir(path);
        if((xtask->dumpfd = open(path, O_CREAT|O_RDWR|O_APPEND, 644)) <= 0)
        {
            FATAL_LOGGER(xtask->logger, "open dump file[%s] failed, %s", path, strerror(errno));
        }
    }
    return ;
}

/* append dump */
int xtask_append_dump(XTASK *xtask, char *data, int ndata)
{
    char path[XT_PATH_MAX], *p = NULL;
    FHEADER *fheader = NULL;
    struct stat st = {0};
    int ret = -1, n = 0;
    
    if(xtask && data && ndata > 0)
    {
        MUTEX_LOCK(xtask->dump_mutex);
        if(xtask->dumpfd > 0 && fstat(xtask->dumpfd, &st) == 0)
        {
            if(st.st_size > XT_DUMP_MAX)
            {
                p = path + sizeof(FHEADER);
                fheader = (FHEADER *)path;
                fheader->size = n;
                fheader->flag = IB_DUMP_SET; 
                n = fheader->size = sprintf(p, "%s/%d.doc", 
                        xtask->state->dumpdir, ++(xtask->state->dump_id_max));
                n = write(xtask->dumpfd, path, sizeof(FHEADER) + n);
                close(xtask->dumpfd);
                xtask_pmkdir(path);
                if((xtask->dumpfd = open(p, O_CREAT|O_RDWR|O_APPEND, 644)) <= 0)
                {
                    FATAL_LOGGER(xtask->logger, "open dump file[%s] failed, %s", p, strerror(errno));
                }
            }
            if(xtask->dumpfd > 0) ret = write(xtask->dumpfd, data, ndata);
        }
        MUTEX_UNLOCK(xtask->dump_mutex);
    }
    return ret;
}

/* dump record */
int xtask_dump(XTASK *xtask)
{
    char *block = NULL, *mm = NULL, *p = NULL, *pp = NULL, *ps = NULL;
    int ret = -1, id = 0, i = 0, x = 0, k = 0, n = 0, flag = 0;
    XTRECORD *record = NULL;
    FHEADER *fheader = NULL;
    IFIELD *fields = NULL;
    XTTABLE table = {0};
    time_t dtime = 0;

    if(xtask)
    {
        if((block = xtask_pop_block(xtask)) && (mm = xtask_pop_block(xtask)))
        {
            fheader = (FHEADER *)block;
            while(x < XT_DUMP_TASK_MAX && mmqueue_pop(xtask->queue, xtask->state->qdump, &id) > 0)
            {
                record = (XTRECORD *)mm;
                if(xtask_get_record(xtask, id, mm) > 0 
                        && xtask_get_table(xtask, record->tableid, &table) > 0)
                {
                    memset(block, 0, sizeof(FHEADER));
                    fields = (IFIELD *)(block + sizeof(FHEADER));
                    ps = p = (char *)fields + sizeof(IFIELD) * table.nfields;
                    memset(fields, 0, sizeof(sizeof(IFIELD) * table.nfields));
                    fheader->nfields = table.nfields;
                    fheader->globalid = id;
                    fheader->category = 1;
                    fheader->rank = 1.0;
                    k = 0;
                    for(i = 0; i < XT_FIELDS_MAX; i++) 
                    {
                        if(table.fields[i].id > 0)
                        {
                            pp = p;
                            flag = 0;
                            fields[k].offset = p - block;
                            if(i >= XT_TEXT_FROM && i < XT_INT_FROM) 
                                flag = (IB_DATATYPE_TEXT|IB_IS_NEED_INDEX);
                            else if(i >= XT_INT_FROM && i < XT_LONG_FROM) 
                                flag = (IB_DATATYPE_INT|IB_IS_NEED_INDEX);
                            else if(i >= XT_LONG_FROM && i < XT_DOUBLE_FROM) 
                                flag = (IB_DATATYPE_LONG|IB_IS_NEED_INDEX);
                            else if(i >= XT_DOUBLE_FROM && i < XT_DISPLAY_FROM) 
                                flag = (IB_DATATYPE_DOUBLE|IB_IS_NEED_INDEX);
                            else 
                                flag = IB_DATATYPE_TEXT;
                            fields[k].flag = flag;
                            if(record->mms[i].length > 0)
                            {
                                if(record->mms[i].flag & REG_IS_DATETIME)
                                {
                                    dtime = str2time(mm + record->mms[i].off);
                                    p += sprintf(p, "%u", (unsigned int)dtime);
                                }
                                else
                                {
                                    p += sprintf(p, "%s", mm + record->mms[i].off);
                                }
                                //WARN_LOGGER(xtask->logger, "table:%s field:%s id:%d str:%s", record->tableid, table.fields[i].name, id, s);
                            }
                            else
                            {
                                if(flag & IB_DATATYPE_NUMBRIC) *p++ = '0';
                            }
                            fields[k].length = p - pp;
                            *p++ = '\r';*p++ = '\n';
                            ++k;
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
                            else break;
                        }
                    }
                    fheader->size = p - (char *)fields;
                    n = p - block;
                    if(xtask_append_dump(xtask, block, n) < 0) goto fail;
                    ++x;
                    continue;
                }
fail:
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
                && (x = vmap_min(xtask->map, urlnodelist[urlnodeid].childs_root, &id, &k)))
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
                && (x = vmap_min(xtask->map, urlnodelist[urlnodeid].childs_root, &id, &k)))
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
        meta->nurl = db_read_data(PDB(xtask->mmdb), urlnodelist[urlid].url_db_id, p);
        meta->proxy_ip = 0;meta->proxy_port = 0;
        hostid = urlnodelist[urlid].hostid;
        meta->ip = xtask_host_ip(xtask, hostid);
        meta->nheaders = 0;
        p += meta->nurl;
        ps = p;
        p += sprintf(p, "Host: ");
        host = p;
        if((n = db_read_data(PDB(xtask->mmdb), hostlist[hostid].db_host_id, p)) > 0)
            p += n;
        *p = '\0';
        if(meta->ip == 0)
        {
            if((hp = gethostbyname(host)))
                meta->ip = inet_addr(inet_ntoa(*((struct in_addr *)(hp->h_addr))));
        }
        /*
        if(hostlist[hostid].db_cookie_id > 0)
        {
            pp = p;
            p += sprintf(p, "\r\nCookie: ");
            if((n = db_read_data(PDB(xtask->mmdb), hostlist[hostid].db_cookie_id, p)) > 0)
                p += n;
            else
                p = pp;
        }
        */
        if((parent = urlnodelist[urlid].parent) > 0 && urlnodelist[parent].url_db_id > 0)
        {
            pp = p;
            p += sprintf(p, "\r\nReferer: ");
            if((n = db_read_data(PDB(xtask->mmdb), urlnodelist[parent].url_db_id, p)) > 0)
                p += n;
            else p = pp;
        }
        if(urlnodelist[urlid].last_modified)
        {
            p += sprintf(p, "\r\nIf-Modified-Since: ");
            p += GMTstrdate(urlnodelist[urlid].last_modified, p);
        }
        p += sprintf(p, "\r\nUser-Agent: %s\r\nAccept: %s\r\n"
                "Accept-Language: %s\r\nAccept-Encoding: %s\r\n"
                "Accept-Charset: %s\r\nConnection: close\r\n",
                HTTP_USER_AGENT, HTTP_ACCEPT_TYPE, HTTP_ACCEPT_LANGUAGE,
                HTTP_ACCEPT_ENCODING, HTTP_ACCEPT_CHARSET);
        meta->nheaders = p - ps;
        ret = p - out;
    }
    return ret;
}

/* new download task */
int xtask_download_task(XTASK *xtask, int flag, char *out, int nout)
{
    int ret = -1, urlid = 0, k = 0, n = 0, left = 0, hostid = 0;
    char *p = NULL, *end = NULL, *pp = NULL;
    XTURLNODE *urlnodelist = NULL;
    XTHOST *hostlist = NULL;

    if(xtask && (p = out) && nout > HTTP_HEADERS_MAX && (end = (out + nout)) > p 
        && (urlnodelist = (XTURLNODE *)(xtask->urlnodeio.map)) 
        && (hostlist = (XTHOST *)(xtask->hostio.map))) 
    {
        left = nout;
        MUTEX_LOCK(xtask->urlnode_mutex);
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
            while(left > HTTP_HEADERS_MAX && k < HTTP_TASK_MAX)
            {
                pp = p;
                if(mmqueue_pop(xtask->queue, xtask->state->qpriority, &hostid) > 0)
                {
                    if(hostlist[hostid].level != HOST_LEVEL_FIFO)
                        mmqueue_push(xtask->queue, xtask->state->qwait, hostid);
                }
                else if(mmqueue_pop(xtask->queue, xtask->state->qwait, &hostid) > 0)
                {
                    if(hostlist[hostid].level != HOST_LEVEL_RR)
                        mmqueue_push(xtask->queue, xtask->state->qpriority, hostid);
                }
                if(hostid > 0)
                {
                    while(left > HTTP_HEADERS_MAX && k < HTTP_TASK_MAX 
                        && mmqueue_pop(xtask->queue, hostlist[hostid].qwait, &urlid) > 0 && urlid > 0)
                    {
                        DEBUG_LOGGER(xtask->logger, "page:%d", urlid);
                        if((n = xtask_new_task(xtask, urlid, p, left)) > 0)
                        {
                            p += n;
                            ++k;
                        }
                        else
                        {
                            mmqueue_push(xtask->queue, hostlist[hostid].qwait, urlid);
                            break;
                        }
                        left = nout - (p - out);
                    }
                    /* push back */
                    if(mmqueue_total(xtask->queue, hostlist[hostid].qwait) > 0)
                    {
                        if(hostlist[hostid].level == HOST_LEVEL_FIFO)
                            mmqueue_push(xtask->queue, xtask->state->qpriority, hostid);
                        else
                            mmqueue_push(xtask->queue, xtask->state->qwait, hostid);
                    }
                }
                left = nout - (p - out);
                if(p == pp) break;
            }
        }
        ret = p - out;
        MUTEX_UNLOCK(xtask->urlnode_mutex);
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
                ret = db_xread_data(xtask->mmdb, line, n, data);
            }
            else
            {
                if(xtask->state->file_task_id < 0) xtask->state->file_task_id = 0;
                id = ++xtask->state->file_task_id;
                n = sprintf(line, "file:%d", id);
                if((ret = xtask_download_task(xtask, flag, data, ndata)) > 0)
                {
                    db_id = db_xset_data(PDB(xtask->mmdb), line, n, data, ret);
                }
            }
            if(ret > 0) vmap_try_insert(xtask->map, xtask->state->file_task_wait, id, id, NULL);
        }
        else if(flag & XT_TASK_UPDATE)
        {
            if(mmqueue_pop(xtask->queue, xtask->state->qtask_update, &id) > 0)
            {
                n = sprintf(line, "update:%d", id);
                ret = db_xread_data(xtask->mmdb, line, n, data);
            }
            else
            {
                if(xtask->state->update_task_id < 0) xtask->state->update_task_id = 0;
                id = ++xtask->state->update_task_id;
                n = sprintf(line, "update:%d", id);
                if((ret = xtask_download_task(xtask, flag, data, ndata)) > 0)
                {
                    db_id = db_xset_data(PDB(xtask->mmdb), line, n, data, ret);
                }
            }
            if(ret > 0) vmap_try_insert(xtask->map, xtask->state->update_task_wait, id, id, NULL);
        }
        else
        {
            if(mmqueue_pop(xtask->queue, xtask->state->qtask_download, &id) > 0)
            {
                n = sprintf(line, "download:%d", id);
                ret = db_xread_data(xtask->mmdb, line, n, data);
            }
            else
            {
                if(xtask->state->download_task_id < 0) xtask->state->download_task_id = 0;
                id = ++xtask->state->download_task_id;
                n = sprintf(line, "download:%d", id);
                if((ret = xtask_download_task(xtask, flag, data, ndata)) > 0)
                {
                    db_id = db_xset_data(PDB(xtask->mmdb), line, n, data, ret);
                }
            }
            if(ret > 0) vmap_try_insert(xtask->map, xtask->state->download_task_wait, id, id, NULL);
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
        else
        {
            if((k = vmap_find(xtask->map, xtask->state->download_task_wait, taskid, NULL)) > 0)
            {
                mmqueue_push(xtask->queue, xtask->state->qtask_download, taskid);
                vmap_remove(xtask->map, xtask->state->download_task_wait, k, NULL, NULL);
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
                n = db_xdel_data(xtask->mmdb, line, n);
            }
            ret = 0;
        }
        else if(flag & XT_TASK_UPDATE)
        {
            if((k = vmap_find(xtask->map, xtask->state->update_task_wait, taskid, NULL)) > 0)
            {
                vmap_remove(xtask->map, xtask->state->update_task_wait, k, NULL, NULL);
                n = sprintf(line, "update:%d", taskid);
                n = db_xdel_data(xtask->mmdb, line, n);
            }
            ret = 0;
        }
        else
        {
            if((k = vmap_find(xtask->map, xtask->state->download_task_wait, taskid, NULL)) > 0)
            {
                vmap_remove(xtask->map, xtask->state->download_task_wait, k, NULL, NULL);
                n = sprintf(line, "download:%d", taskid);
                n = db_xdel_data(xtask->mmdb, line, n);
            }
            ret = 0;
        }
        MUTEX_UNLOCK(xtask->download_mutex);    
    }
    return ret;
}

/* over download */
int xtask_over_download(XTASK *xtask, int flag, int taskid, char *data, int ndata)
{
    char *p = NULL, *end = NULL;
    XTURLNODE urlnode = {0};
    XTREC *rec = NULL;
    int ret = -1, id = 0;

    if(xtask && (p = data) && (end = (data + ndata)) > data)
    {
        //fprintf(stdout, "%s::%d taskid:%d\r\n", __FILE__, __LINE__, taskid);
        //WARN_LOGGER(xtask->logger, "taskid:%d", taskid);
        ret = xtask__over__download(xtask, flag, taskid);
        //WARN_LOGGER(xtask->logger, "taskid:%d", taskid);
        while(p < end)
        {
            rec = (XTREC *)p; p += sizeof(XTREC);
            if(xtask_get_urlnode(xtask, rec->id, &urlnode) > 0)
            {
                //WARN_LOGGER(xtask->logger, "rec->id:%d", rec->id);
                if(rec->nlocation > 0)
                {
                    //WARN_LOGGER(xtask->logger, "rec->id:%d nlocation:%d", rec->id, rec->nlocation);
                    id = xtask_add_url(xtask, urlnode.parent, urlnode.nodeid, p, urlnode.flag);
                    p += rec->nlocation + 1;
                }
                else 
                    id = rec->id;
                if(rec->ncookie > 0) 
                {
                    //WARN_LOGGER(xtask->logger, "rec->id:%d ncookie:%d", rec->id, rec->ncookie);
                    //xtask_add_host_cookie(xtask, urlnode.hostid, p, rec->ncookie);
                    p += rec->ncookie + 1;
                }
                if(rec->length > 0)
                {
                    //WARN_LOGGER(xtask->logger, "rec->id:%d length:%d", rec->id, rec->length);
                    ret = xtask_set_urlnode_data(xtask, id, rec->last_modified, p, rec->length);
                    p += rec->length;
                    if(urlnode.flag & REG_IS_FILE)
                        mmqueue_push(xtask->queue, xtask->state->qfile_dump, id);
                    else
                        mmqueue_push(xtask->queue, xtask->state->qextract, id);
                    if((urlnode.flag & REG_IS_LIST))
                        mmqueue_push(xtask->queue, xtask->state->qupdate, id);
                }
                else
                {
                    WARN_LOGGER(xtask->logger, "urlid:%d download error:%d", id, rec->err);
                    xtask_set_urlnode_status(xtask, id, rec->err);
                    if(rec->err & ERR_NEED_RETRY)
                        mmqueue_push(xtask->queue, xtask->state->qretry, id);
                }
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
    char *p = NULL, *end = NULL, *pp = NULL;
    ITEMPLATE *templates = NULL;
    XTURLNODE urlnode = {0};
    XTTABLE *table = NULL;
    XTROW *row = NULL;

    if(xtask && (p = out) && (end = (p + nout)) > p)
    {
        while((left = (nout - (p - out))) > XT_ROW_MAX && k < XT_EXTRACT_MAX 
                && mmqueue_pop(xtask->queue, xtask->state->qextract, &id) > 0)
        {
            //if(id == last) continue;
            //WARN_LOGGER(xtask->logger, "ready for extract urlid:%d", id);
            pp = p;
            if(xtask_get_urlnode(xtask, id, &urlnode) && urlnode.nodeid > 0)
            {
                memset(p, 0, sizeof(XTROW));
                row = (XTROW *)p;
                row->id = id;
                p += sizeof(XTROW);
                left -= sizeof(XTROW);
                templates = (ITEMPLATE *)p;
                /* templates */
                if((n = xtask_get_node_templates(xtask, urlnode.nodeid, p, left)) > 0)
                {
                    row->ntemplates = n;
                    p += n * sizeof(ITEMPLATE);
                    left -= n * sizeof(ITEMPLATE);
                    /* read table  */ 
                    if(templates[0].tableid > 0)
                    {
                        table = (XTTABLE *)p; 
                        if(xtask_get_table(xtask, templates[0].tableid, table) <= 0) goto over;
                        p += sizeof(XTTABLE);
                    }
                }
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
                continue;
            }
over:
            mmqueue_push(xtask->queue, xtask->state->qextract, id);
            p = pp;
            break;
        }
        ret = p - out;
        //fprintf(stdout, "%s::%d extract_task_ret:%d\r\n", __FILE__, __LINE__, ret);
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
        }
        else
        {
            if(xtask->state->extract_task_id < 0) xtask->state->extract_task_id = 0;
            if((ret = xtask_extract_task(xtask, data, ndata)) > 0)
            {
                id = ++xtask->state->extract_task_id;
                n = sprintf(line, "extract:%d", id);
                db_id = db_xset_data(PDB(xtask->mmdb), line, n, data, ret);
            }
        }
        if(ret > 0) vmap_try_insert(xtask->map, xtask->state->extract_task_wait, id, id, NULL);
        MUTEX_UNLOCK(xtask->extract_mutex);
        *taskid = id;
        //fprintf(stdout, "%s::%d id:%d ret:%d wait:%d\r\n", __FILE__, __LINE__, id, ret, vmap_total(xtask->map, xtask->state->extract_task_wait));
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
            //fprintf(stdout, "%s::%d taskid:%d\r\n", __FILE__, __LINE__, taskid);
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
        if((k = vmap_find(xtask->map, xtask->state->extract_task_wait, taskid, NULL)) > 0)
        {
            vmap_remove(xtask->map, xtask->state->extract_task_wait, k, NULL, NULL);
            n = sprintf(line, "file:%d", taskid);
            n = db_xdel_data(xtask->mmdb, line, n);
        }
        ret = 0;
        MUTEX_UNLOCK(xtask->extract_mutex);    
    }
    return ret;
}

/* over extract task */
int xtask_over_extract(XTASK *xtask, int taskid, char *data, int ndata)
{
    int ret = -1, x = 0, parent = 0, id = 0;
    char *p = NULL, *end = NULL, *tail = NULL;
    XTURLNODE urlnode = {0};
    XTRES *record = NULL;
    XTITEM *item = NULL;

    if(xtask && (p = data) && (end = (data + ndata)) > p)
    {
        while(p < end)
        {
            record = (XTRES *)p;        
            tail = p + record->length;
            ret = xtask_get_urlnode(xtask, record->id, &urlnode);
            p += sizeof(XTRES);
            while(p < tail)
            {
                item = (XTITEM *)p;
                if(urlnode.flag & REG_IS_UNIQE)
                {
                    ret = xtask_update_record(xtask, record->id, p, item->length);
                    //WARN_LOGGER(xtask->logger, "update_record(urlid:%d) item->count:%d nodeid:%d ret:%d", record->id, item->count, urlnode.nodeid, ret);
                }
                p += sizeof(XTITEM);
                x = 0;
                while(x < item->count)
                {
                    if(item->regs[x].flag & REG_IS_LINK)
                    {
                        parent = xtask_check_parent(xtask, record->id, item->regs[x].nodeid);
                        id = xtask_add_url(xtask, parent,item->regs[x].nodeid,p,item->regs[x].flag); 
                        if((item->regs[x].flag & REG_IS_PAGE) && urlnode.brother > 0
                                && item->regs[x].nodeid == urlnode.nodeid)
                        {
                            xtask_set_brother(xtask, id, record->id);
                        }
                        if(!(item->regs[x].flag & REG_IS_PAGE) && id > 0 && item->regs[x].nodeid > 0)
                        {
                            //WARN_LOGGER(xtask->logger, "update_record(urlid:%d) id:%d nodeid:%d url:%s count:%d item[%d]->flag:%d", record->id, id, item->regs[x].nodeid, p, item->count, x, item->regs[x].flag);
                            xtask_update_record(xtask, id, (char *)item, item->length);
                        }
                        //fprintf(stdout, "%s::%d x:%d parent:%d nodeid:%d newurl:%s id:%d\r\n", __FILE__, __LINE__, x, parent, item->regs[x].nodeid, p, id);
                    }
                    p += item->regs[x].length + 1;
                    ++x;
                }
            }
            if(urlnode.flag & REG_IS_UNIQE)
            {
                mmqueue_push(xtask->queue, xtask->state->qdump, record->id);
                //WARN_LOGGER(xtask->logger, "qdump urlid:%d total:%d", record->id, mmqueue_total(xtask->queue, xtask->state->qdump));
            }
        }
        ret = xtask__over__extract(xtask, taskid);
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
        while(vmap_total(xtask->map, xtask->state->download_task_wait) > 0
            && (mid = vmap_min(xtask->map, xtask->state->download_task_wait, &taskid, NULL)) > 0)
        {
            mmqueue_push(xtask->queue, xtask->state->qtask_download, taskid);
            vmap_remove(xtask->map, xtask->state->download_task_wait, mid, NULL, NULL);
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
        while(vmap_total(xtask->map, xtask->state->extract_task_wait) > 0
            && (mid = vmap_min(xtask->map, xtask->state->extract_task_wait, &taskid, NULL)) > 0)
        {
            mmqueue_push(xtask->queue, xtask->state->qtask_extract, taskid);
            vmap_remove(xtask->map, xtask->state->extract_task_wait, mid, NULL, NULL);
        }
    }
    return ;
}
/* initialize XTASK */
XTASK *xtask_init(char *dir)
{
    char path[XT_PATH_MAX], *p = NULL;
    struct stat st = {0};
    XTASK *xtask = NULL;
    int n = 0;

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
        if(xtask->state->qextract == 0) xtask->state->qextract = mmqueue_new(xtask->queue);
        if(xtask->state->qdump == 0) xtask->state->qdump = mmqueue_new(xtask->queue);
        if(xtask->state->qfile_dump == 0) xtask->state->qfile_dump = mmqueue_new(xtask->queue);
        if(xtask->state->qtask_download == 0)xtask->state->qtask_download=mmqueue_new(xtask->queue);
        if(xtask->state->qtask_file == 0)xtask->state->qtask_file = mmqueue_new(xtask->queue);
        if(xtask->state->qtask_update == 0) xtask->state->qtask_update = mmqueue_new(xtask->queue);
        if(xtask->state->qtask_extract == 0) xtask->state->qtask_extract = mmqueue_new(xtask->queue);
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
        if(xtask->state->download_task_wait == 0) 
            xtask->state->download_task_wait = vmap_new_tree(xtask->map);
        if(xtask->state->file_task_wait == 0) 
            xtask->state->file_task_wait = vmap_new_tree(xtask->map);
        if(xtask->state->update_task_wait == 0) 
            xtask->state->update_task_wait = vmap_new_tree(xtask->map);
        if(xtask->state->extract_task_wait == 0) 
            xtask->state->extract_task_wait = vmap_new_tree(xtask->map);
        if(xtask->state->host_task_wait == 0) 
            xtask->state->host_task_wait = vmap_new_tree(xtask->map);
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
    }
    return xtask;
}
/* close xtask */
void xtask_clean(XTASK *xtask)
{
    if(xtask)
    {
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
        xmm_free(xtask, sizeof(XTASK));
    }
    return ;
}

#ifdef _DEBUG_XTASK
int main(int argc, char **argv)
{
    char line[XT_LINE_MAX], *host = NULL, name[XT_NAME_MAX];
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
        xtask_clean(xtask);
        return 0;
    }
    return -1;
}
#endif
