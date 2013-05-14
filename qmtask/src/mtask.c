#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <fcntl.h>
#include "mtask.h"
#include "xmm.h"
/* set message task */
int mtask_set(MTASK *mtask, char *ip, int port, int mtaskid, int qtaskid)
{
    int ret = -1;
    if(mtask && ip && port > 0 && (mtaskid > 0 || qtaskid > 0))
    {
        memset(mtask, 0, sizeof(MTASK));
        mtask->mtaskid = mtaskid;
        mtask->qtaskid = qtaskid;
        mtask->rsa.sin_family = AF_INET;
        mtask->rsa.sin_addr.s_addr = inet_addr(ip);
        mtask->rsa.sin_port = htons(port);
        ret = 0;
    }
    return ret;
}

/* mtask reset */
int mtask_reset(MTASK *mtask)
{
    if(mtask)
    {
        if(mtask->fd > 0)
        {
            shutdown(mtask->fd, SHUT_RD|SHUT_WR);
            close(mtask->fd);
        }
        mtask->fd = -1;
        if(mtask->packet && mtask->mm_size) xmm_free(mtask->packet, mtask->mm_size);
        mtask->packet = NULL;
        mtask->length = 0;
        mtask->packetid = 0;
        return 0;
    }
    return -1;
}

int mtask_packet_resize(MTASK *mtask, int size)
{
    int ret = -1, len = 0;

    if(mtask)
    {
        if(mtask->mm_size < size) len = ((size/MTASK_MM_BASE)+1) * MTASK_MM_BASE;
        if(len > 0)
        {
            if((mtask->packet = xmm_renew(mtask->packet, mtask->mm_size, len)))
            {
                mtask->mm_size = len;
                ret = 0;
            }
        }
        else ret = 0;
    }
    return ret;
}

/* connect to qtask */
int mtask_connect(MTASK *mtask)
{
    if(mtask)
    {
        if(mtask->fd <= 0 && (mtask->fd = socket(AF_INET, SOCK_STREAM, 0)) <= 0)
        {
            fprintf(stderr, "create socket() failed, %s\n", strerror(errno));
            return -2;
        }
        if(connect(mtask->fd, (struct sockaddr *)&(mtask->rsa), sizeof(struct sockaddr)) != 0)
        {
            fprintf(stderr, "connect() failed, %s\n", strerror(errno));
            close(mtask->fd);
            mtask->fd = -1;
            return -3;
        }
        return 0;
    }
    return -1;
}

/* push data id and get new data_id */
int mtask_commit(MTASK *mtask, int flag, char *packet, int packet_len)
{
    int ret = -1, n = 0;
    MHEAD mhead = {0};

    if(mtask)
    {
        if(mtask->fd <= 0 && mtask_connect(mtask) < 0) 
            return -2;
        if(mtask->qtaskid > 0)
        {
            mhead.cmd |= MTASK_CMD_PUSH;
            if(packet && packet_len > 0) mhead.length = packet_len;
        }
        if(mtask->mtaskid > 0) 
        {
            mhead.cmd |= MTASK_CMD_POP;
            if(mtask->packetid > 0) mhead.cmd |= MTASK_CMD_FINISH;
            if(mtask->qtaskid <= 0) mhead.cmd |= MTASK_CMD_OVER;
        }
        mhead.flag = flag;
        mhead.packetid = mtask->packetid;
        mhead.mtaskid = mtask->mtaskid;
        mhead.qtaskid = mtask->qtaskid;
        //fprintf(stdout, "%s::%d mid:%d qid:%d cmd:%d len:%d\n", __FILE__, __LINE__, mhead.mtaskid, mhead.qtaskid, mhead.cmd, mhead.length);
        /* send header */
        if(write(mtask->fd, &mhead, sizeof(MHEAD)) <= 0)
        {
            fprintf(stderr, "write task header failed, %s\n", strerror(errno));
            mtask_reset(mtask);
            return -3;
        }
        /* send packet */
        if(mhead.length > 0 && write(mtask->fd, packet, packet_len) <= 0)
        {
            fprintf(stderr, "write task list failed, %s\n", strerror(errno));
            mtask_reset(mtask);
            return -4;
        }
        /* recv header */
        if(recv(mtask->fd, &mhead, sizeof(MHEAD), MSG_WAITALL) != sizeof(MHEAD))
        {
            fprintf(stderr, "read task header failed, %s\n", strerror(errno));
            mtask_reset(mtask);
            return -5;
        }
        if(mtask->qtaskid > 0 && packet && packet_len > 0) mtask->qtotal++;
        if(mtask->mtaskid > 0 && mtask->packetid > 0) mtask->mtotal++;
        /*
        if(mhead.length > 1024 * 64)
        {
            fprintf(stderr, "Invalid packetid:%d length:%d mid:%d qid:%d\n", mhead.packetid, mhead.length, mtask->mtaskid, mtask->qtaskid);
            fflush(stderr);
            _exit(-1);
        }
        */
        /* malloc for packet */
        if(mtask_packet_resize(mtask, mhead.length) != 0)
        {
            fprintf(stderr, "renew(mtask->packet) failed, %s\n", strerror(errno));
            mtask_reset(mtask);
            return -6;
        }
        /* read packet */
        if(mhead.length > 0 && (n = recv(mtask->fd, mtask->packet, mhead.length, MSG_WAITALL)) != mhead.length)
        {
            fprintf(stderr, "read %d/%d task list failed, %s\n", n, mhead.length, strerror(errno));
            mtask_reset(mtask);
            return -7;
        }
        mtask->length = mhead.length;
        mtask->packetid = mhead.packetid;
        ret = mhead.packetid;
        //fprintf(stdout, "%s::%d data_id:%lld\n", __FILE__, __LINE__, (long long)mhead.data_id);
    }
    return ret;
}

/*
 * return value
 * -1 mtask is NULL
 * -2 connection is bad
 * -3 Invalid packet data
 * -4 write() header failed
 * -5 write() packet failed
 * -6 recv() header failed
 * */
/* push packet */
int mtask_push(MTASK *mtask, int flag, char *packet, int packet_len)
{
    MHEAD mhead = {0};
    int ret = -1;

    if(mtask)
    {
        if(mtask->fd <= 0 && mtask_connect(mtask) < 0) 
            return -2;
        if(mtask->qtaskid > 0 && packet && packet_len > 0)
        {
            mhead.cmd |= MTASK_CMD_PUSH;
            mhead.length = packet_len;
            mhead.qtaskid = mtask->qtaskid;
        }
        else
            return -3;
        mhead.flag = flag;
        //fprintf(stdout, "%s::%d mid:%d qid:%d cmd:%d len:%d\n", __FILE__, __LINE__, mhead.mtaskid, mhead.qtaskid, mhead.cmd, mhead.length);
        /* send header */
        if(write(mtask->fd, &mhead, sizeof(MHEAD)) <= 0)
        {
            fprintf(stderr, "write task header failed, %s\n", strerror(errno));
            mtask_reset(mtask);
            return -4;
        }
        /* send packet */
        if(mhead.length > 0 && write(mtask->fd, packet, packet_len) <= 0)
        {
            fprintf(stderr, "write task list failed, %s\n", strerror(errno));
            mtask_reset(mtask);
            return -5;
        }
        /* recv header */
        if(recv(mtask->fd, &mhead, sizeof(MHEAD), MSG_WAITALL) != sizeof(MHEAD))
        {
            fprintf(stderr, "read task header failed, %s\n", strerror(errno));
            mtask_reset(mtask);
            return -6;
        }
        mtask->packetid = 0;
        ret = mhead.packetid;
        //fprintf(stdout, "%s::%d data_id:%lld\n", __FILE__, __LINE__, (long long)mhead.data_id);
    }
    return ret;
}


/*
 * return value
 * -1 mtask is NULL
 * -2 connection is bad
 * -3 Invalid commitid
 * -4 write() header failed
 * -5 recv() header failed
 * -6 malloc() for packet failed
 * -7 recv() packet failed
 * */
/* pop packet */
int mtask_pop(MTASK *mtask)
{
    int ret = -1, n = 0;
    MHEAD mhead = {0};

    if(mtask)
    {
        if(mtask->fd <= 0 && mtask_connect(mtask) < 0) 
            return -2;
        if(mtask->mtaskid > 0)
        {
            mhead.cmd |= MTASK_CMD_POP;
            mhead.mtaskid = mtask->mtaskid;
        }
        else
        {
            return -3;
        }
        /* send header */
        if(write(mtask->fd, &mhead, sizeof(MHEAD)) <= 0)
        {
            fprintf(stderr, "write task header failed, %s\n", strerror(errno));
            mtask_reset(mtask);
            return -4;
        }
        /* recv header */
        if(recv(mtask->fd, &mhead, sizeof(MHEAD), MSG_WAITALL) != sizeof(MHEAD))
        {
            fprintf(stderr, "read task header failed, %s\n", strerror(errno));
            mtask_reset(mtask);
            return -5;
        }
        /* malloc for packet */
        if(mtask_packet_resize(mtask, mhead.length) != 0)
        {
            fprintf(stderr, "renew(mtask->packet) failed, %s\n", strerror(errno));
            mtask_reset(mtask);
            return -6;
        }
        /* read packet */
        if(mhead.length > 0 && (n = recv(mtask->fd, mtask->packet, mhead.length, MSG_WAITALL)) != mhead.length)
        {
            fprintf(stderr, "read %d/%d task list failed, %s\n", n, mhead.length, strerror(errno));
            mtask_reset(mtask);
            return -7;
        }
        mtask->length = mhead.length;
        mtask->packetid = mhead.packetid;
        ret = mhead.packetid;
    }
    return ret;
}

/*
 * return value
 * -1 mtask is NULL
 * -2 connection is bad
 * -3 Invalid packetid and commitid
 * -4 write() header failed
 * -5 recv() header failed
 * finish packet */
int mtask_finish(MTASK *mtask, int flag)
{
    MHEAD mhead = {0};
    int ret = -1;

    if(mtask)
    {
        if(mtask->fd <= 0 && mtask_connect(mtask) < 0) 
            return -2;
        if(mtask->packetid > 0 && mtask->mtaskid > 0)
        {
            mhead.packetid = mtask->packetid;
            mhead.mtaskid = mtask->mtaskid;
            mhead.cmd |= MTASK_CMD_FINISH;
        }
        else
            return -3;
        if(mtask->qtaskid > 0)
        {
            mhead.cmd |= MTASK_CMD_PUSH;
            mhead.flag = flag;
            mhead.qtaskid = mtask->qtaskid;
        }
        else
        {
             mhead.cmd |= MTASK_CMD_OVER;
        }
        /* send header */
        if(write(mtask->fd, &mhead, sizeof(MHEAD)) <= 0)
        {
            fprintf(stderr, "write task header failed, %s\n", strerror(errno));
            mtask_reset(mtask);
            return -4;
        }
        /* recv header */
        if(recv(mtask->fd, &mhead, sizeof(MHEAD), MSG_WAITALL) != sizeof(MHEAD))
        {
            fprintf(stderr, "read task header failed, %s\n", strerror(errno));
            mtask_reset(mtask);
            return -5;
        }
        mtask->packetid = 0;
        ret = mhead.packetid;
        //fprintf(stdout, "%s::%d data_id:%lld\n", __FILE__, __LINE__, (long long)mhead.data_id);
    }
    return ret;
}

/* close message task */
void mtask_close(MTASK *mtask)
{
    if(mtask->fd > 0)
    {
        shutdown(mtask->fd, SHUT_RD|SHUT_WR);
        close(mtask->fd);
        mtask->fd = -1;
        if(mtask->packet && mtask->mm_size > 0)
            xmm_free(mtask->packet, mtask->mm_size);
    }
    return ;
}
