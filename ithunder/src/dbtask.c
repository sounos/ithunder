#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
//#include <sys/socket.h>
//#include <netinet/in.h>
//#include <arpa/inet.h>
#include <sys/types.h>
#include <mtask.h>
#include <dbase.h>
#include "md5.h"
#include "crc32.h"
#include "timer.h"
#define XX_BUF_SIZE     65536
#define XX_URL_MAX      4096
#define XX_NS_MAX       256
#define XX_PACKET_MAX   1024
int main(int argc, char **argv)
{
    int i = 0, k = 0, x = 0, mid = 0, qid = 0, port = 0, flag = 0, dbport = 0, 
        isdaemon = 0, isout = 0, packetid = 0, ret = -1, from = 0, to = 0;
    char tmp[XX_BUF_SIZE], url[XX_URL_MAX],*ip = NULL, *dbhost = NULL, 
         block[XX_PACKET_MAX * sizeof(int64_t)], ch = 0;
    int64_t *list = NULL, key = 0, rand = 0;
    unsigned char digest[MD5_LEN];
    BJSON request = {0};
    void *timer = NULL;
    BREC record = {0};
    MTASK mtask = {0};
    pid_t pid = 0;
    DBASE db = {0};

    /* get configure file */
    while((ch = getopt(argc, argv, "h:p:m:q:i:x:n:c:f:t:od")) != -1)
    {
        switch(ch)
        {
            case 'h':
                ip = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'm':
                mid = atoi(optarg);
                break;
            case 'q':
                qid = atoi(optarg);
                break;
            case 'i':
                dbhost = optarg;
                break;
            case 'x':
                dbport = atoi(optarg);
                break;
            case 'f':
                from = atoi(optarg);
                break;
            case 't':
                to = atoi(optarg);
                break;
            case 'o':
                isout = 1;
                break;
            case 'd':
                isdaemon = 1;
                break;
            default:
                break;
            }
    }
    if(ip == NULL || port <= 0 || (qid == 0 && mid == 0) || dbhost == NULL || dbport <= 0)
    {
        fprintf(stderr, "Usage:%s -h qtask_host -p qtask_port -m commitid -q queueid "
                "-i dbhost -x dbport -f from -t to -o output -d is_working_daemon\n", argv[0]);
        _exit(-1);
    }
    /* daemon */
    if(isdaemon)
    {
        pid = fork();
        switch (pid) {
            case -1:
                perror("fork()");
                exit(EXIT_FAILURE);
                break;
            case 0: /* child process */
                if(setsid() == -1)
                    exit(EXIT_FAILURE);
                break;
            default:/* parent */
                _exit(EXIT_SUCCESS);
                break;
        }
    }
    if(mtask_set(&mtask, ip, port, mid, qid) == 0 
            && mtask_connect(&mtask) == 0)
    {
        if(dbase_set(&db, dbhost, dbport) != 0 || dbase_connect(&db) != 0)
        {
            fprintf(stderr, "connect to db[%s:%d] failed, %s\n", dbhost, dbport, strerror(errno));
            _exit(-1);
        }
        list = (int64_t *)block;
        //sprintf(ns, "%s.%s", dbname, collection);
        TIMER_INIT(timer);
        BJSON_INIT(request);
        k = from;
        if(to <= 0) to = from + 10000000;
        do
        {
            TIMER_SAMPLE(timer);
            rand = random();
            flag = 0;
            //if((rand%33) == 0) flag = MTASK_TO_QHEAD;
            x = 0;
            while(x < XX_PACKET_MAX)
            {
                //fprintf(stdout, "%s::%d ns:%s OK\n", __FILE__, __LINE__, ns);
                sprintf(url, "http://demo.com/%d.html", ++k);
                memset(digest, 0, MD5_LEN);
                md5((unsigned char *)url, strlen(url), digest);
                key = *((int64_t *)digest);
                sprintf(tmp, "title %d content %d comment %d", k, k, k);
                brequest_reset(&request);
                bjson_start(&request);
                bjson_append_long(&request, "id", key);
                bjson_new_object(&request, "property");
                bjson_append_int(&request, "status", 1);
                bjson_append_int(&request, "crc", crc32(tmp, strlen(tmp)));
                bjson_append_int(&request, "slevel", 1);
                bjson_append_long(&request, "category", (int64_t)1 << (rand%64));
                bjson_append_double(&request, "rank", 1.0);
                bjson_finish_object(&request);
                //fprintf(stdout, "%s::%d key:%lld\n", __FILE__, __LINE__, key);
                bjson_new_object(&request, "text_index");
                sprintf(tmp, "title %d", k);
                bjson_append_string(&request, "title", tmp);
                sprintf(tmp, "content %d", k);
                bjson_append_string(&request, "content", tmp);
                sprintf(tmp, "comment %d", k);
                bjson_append_string(&request, "comment", tmp);
                bjson_finish_object(&request);
                //fprintf(stdout, "%s::%d key:%lld\n", __FILE__, __LINE__, key);
                bjson_new_object(&request, "int_index");
                bjson_append_int(&request, "0", rand/3);
                bjson_append_int(&request, "1", rand/9);
                bjson_append_int(&request, "2", rand/31);
                bjson_finish_object(&request);
                bjson_new_object(&request, "long_index");
                bjson_append_long(&request, "0", rand/2);
                bjson_append_long(&request, "1", rand/7);
                bjson_append_long(&request, "2", rand/21);
                bjson_finish_object(&request);
                bjson_new_object(&request, "double_index");
                bjson_append_double(&request, "0", (double)rand/(double)33);
                bjson_append_double(&request, "1", (double)rand/(double)53);
                bjson_append_double(&request, "2", (double)rand/(double)73);
                bjson_finish_object(&request);
                bjson_new_object(&request, "display");
                bjson_append_string(&request, "url", url);
                sprintf(tmp, "http://demo.com/%d.jpg", k);
                bjson_append_string(&request, "img", tmp);
                bjson_finish_object(&request);
                bjson_finish(&request);
                if(isout)
                {
                    record.data = request.data;
                    record.ndata = request.current;
                    bjson_json(&record, tmp);
                    fprintf(stdout, "json:%s\n", tmp);
                }
                if((ret = dbase_set_record(&db, key, &request)) < 0)
                {
                    fprintf(stderr, "set_data(%lld) ndata:%d failed, ret:%d\n", (long long)key, request.current, ret);
                    _exit(-1);
                }
                //_exit(-1);
                list[x++] = key;
            }
            if(isout)
            {
                TIMER_SAMPLE(timer);
                fprintf(stdout, "db-time-used:%lld\n", PT_LU_USEC(timer));
                TIMER_SAMPLE(timer);
            }
            if((packetid = mtask_push(&mtask, flag, block, sizeof(int64_t) * XX_PACKET_MAX)) >= 0)
            {
                if(isout)
                {
                    TIMER_SAMPLE(timer);
                    fprintf(stdout, "mtask-time-used:%lld\n", PT_LU_USEC(timer));
                    fprintf(stdout, "1:{%d:{packetid:%d}}\n", i, packetid);
                }
                ++i;
            }
            else 
            {
                fprintf(stderr, "push mtask failed, %s\n", strerror(errno));
                sleep(1);
            }
        }while(k < to);
        bjson_clean(&request);
        dbase_close(&db);
        mtask_close(&mtask);
        TIMER_CLEAN(timer);
    }
    return 0;
}
