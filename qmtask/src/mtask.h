#ifndef __MTASK__H__
#define __MTASK__H__
#ifdef __cplusplus
extern "C" {
#endif
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#define MTASK_CMD_PUSH      0x01
#define MTASK_CMD_POP       0x02
#define MTASK_CMD_FINISH    0x04
#define MTASK_CMD_OVER      0x08
#define MTASK_TO_QHEAD      0x01
#define MTASK_PACKET_MAX    8192
#define MTASK_MM_BASE       4096
typedef struct MHEAD
{
    int cmd;
    int flag;
    int mtaskid;
    int qtaskid;
    int packetid;
    int length;
}MHEAD;
typedef struct _MTASK
{
    int     fd;
    int     mtaskid;
    int     qtaskid;
    int     mm_size;
    int     length;
    int     packetid;
    off_t   mtotal;
    off_t   qtotal;
    char    *packet;
    struct  sockaddr_in rsa;
}MTASK;
/* set message task */
int mtask_set(MTASK *mtask, char *ip, int port, int mtaskid, int qtaskid);
/* connect to qtask 
 * -1 mtask is NULL
 * -2 socket() failed
 * -3 connect() failed
 * */
int mtask_connect(MTASK *mtask);

/* get new task count 
 * -1 mtask is NULL
 * -2 mtask->fd <= 0 and mtask_connect failed
 * -3 write() task header failed
 * -4 write() task list[] failed
 * -5 read task header failed
 * -6 malloc failed
 * -7 read task list[] failed
 * ret >= 0 mhead.packetid
 * */
int mtask_commit(MTASK *mtask, int flag, char *packet, int packet_len);

/* push packet 
 * return value
 * -1 mtask is NULL
 * -2 connection is bad
 * -3 Invalid packet data
 * -4 write() header failed
 * -5 write() packet failed
 * -6 recv() header failed
 * */
int mtask_push(MTASK *mtask, int flag, char *packet, int packet_len);

/* pop packet
 * return value
 * -1 mtask is NULL
 * -2 connection is bad
 * -3 Invalid commitid
 * -4 write() header failed
 * -5 recv() header failed
 * -6 malloc() for packet failed
 * -7 recv() packet failed
 */
int mtask_pop(MTASK *mtask);

/* over packet 
 * return value
 * -1 mtask is NULL
 * -2 connection is bad
 * -3 Invalid packetid and commitid
 * -4 write() header failed
 * -5 recv() header failed
 * */
int mtask_finish(MTASK *mtask, int flag);

/* close message task */
void mtask_close(MTASK *mtask);
#ifdef __cplusplus
 }
#endif
#endif
