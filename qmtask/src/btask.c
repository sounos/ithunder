#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include "mtask.h"
int main(int argc, char **argv)
{
    int i = 0, x = 0, mid = 0, qid = 0, port = 0, flag = 0, 
        isdaemon = 0, isout = 0, len = 0, packetid = 0;
    char *ip = NULL, *packet = NULL, block[MTASK_PACKET_MAX * sizeof(int64_t)], ch = 0;
    int64_t old = 0, *list = NULL;
    MTASK mtask = {0};
    pid_t pid = 0;

    /* get configure file */
    while((ch = getopt(argc, argv, "h:p:m:q:d")) != (char)-1)
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
    if(ip == NULL || port <= 0 || (qid == 0 && mid == 0))
    {
        fprintf(stderr, "Usage:%s -h host -p port -m commitid -q queueid -o output -d working as daemon\n", argv[0]);
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
        list = (int64_t *)block;
        if(mid <= 0 && qid > 0)
        {
            do
            {
                old = random();
                flag = 0;
                if((old%33) == 0) flag = MTASK_TO_QHEAD;
                x = 0;
                while(x < MTASK_PACKET_MAX)
                {
                    list[x++] = (int64_t)random();
                }
                if((packetid = mtask_push(&mtask, flag, block, 
                                sizeof(int64_t) * MTASK_PACKET_MAX)) >= 0)
                {
                    if(isout)fprintf(stdout, "1:{%d:{packetid:%d}}\n", i, packetid);
                    ++i;
                }
                else 
                {
                    sleep(1);
                }
            }while(1);
        }
        else
        {
            /*
            packetid = 0;
            do
            {
                old = random();
                flag = 0;
                if((old%33) == 0) flag = MTASK_TO_QHEAD;
                if((packetid = mtask_commit(&mtask, flag, NULL, 0)) > 0)
                {
                    packet = mtask.packet;
                    len = mtask.length;
                    if(isout)fprintf(stdout, "2:{%d:{packetid:%d length:%d}}\n", i, packetid, len);
                    ++i;
                }
                else 
                {
                    packet = NULL;
                    len = 0;
                    sleep(1);
                }
            }while(1);
            */
            packetid = 0;
            do
            {
                old = random();
                flag = 0;
                if((old%33) == 0) flag = MTASK_TO_QHEAD;
                if((packetid = mtask_pop(&mtask)) > 0)
                {
                    packet = mtask.packet;
                    len = mtask.length;
                    if(isout)fprintf(stdout, "2:{%d:{packetid:%d length:%d}}\n", i, packetid, len);
                    ++i;
                    mtask_finish(&mtask, flag);
                }
                else 
                {
                    packet = NULL;
                    len = 0;
                    sleep(1);
                }
            }while(1);
        }
        mtask_close(&mtask);
    }
    return 0;
}
/* benchmark for qtask */
//gcc -o mtask mtask.c -D_DEBUG_MTASK && ./mtask 127.0.0.1 2066 1 2
