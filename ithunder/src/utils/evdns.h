#ifndef _EVDNS_H
#define _EVDNS_H
/* Header format, from RFC 1035:
 *                                  1  1  1  1  1  1
 *    0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5
 *  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 *  |                      ID                       |
 *  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 *  |QR|   Opcode  |AA|TC|RD|RA|   Z    |   RCODE   |
 *  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 *  |                    QDCOUNT                    |
 *  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 *  |                    ANCOUNT                    |
 *  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 *  |                    NSCOUNT                    |
 *  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 *  |                    ARCOUNT                    |
 *  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 *
 * AA, TC, RA, and RCODE are only set in responses.  Brief description
 * of the remaining fields:
 *      ID      Identifier to match responses with queries
 *      QR      Query (0) or response (1)
 *      Opcode  For our purposes, always QUERY
 *      RD      Recursion desired
 *      Z       Reserved (zero)
 *      QDCOUNT Number of queries
 *      ANCOUNT Number of answers
 *      NSCOUNT Number of name server records
 *      ARCOUNT Number of additional records
 *
 * Question format, from RFC 1035:
 *                                  1  1  1  1  1  1
 *    0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5
 *  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 *  |                                               |
 *  /                     QNAME                     /
 *  /                                               /
 *  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 *  |                     QTYPE                     |
 *  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 *  |                     QCLASS                    |
 *  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 *
 * The query name is encoded as a series of labels, each represented
 * as a one-byte length (maximum 63) followed by the text of the
 * label.  The list is terminated by a label of length zero (which can
 * be thought of as the root domain).
 */
#define OFF_MASK  0xc0
#define MAXCDNAME 255
#define HFIXEDSZ 12
#define QFIXEDSZ 4
#define RRFIXEDSZ 10
#define TYPE_ANAME  1
#define TYPE_CNAME  5
#define TYPE_PTR    12
#define DNS__16BIT(p)                   (((p)[0] << 8) | (p)[1])
#define DNS__32BIT(p)                   (((p)[0] << 24) | ((p)[1] << 16) | \
                                         ((p)[2] << 8) | (p)[3])

#define DNS__SET16BIT(p, v)  (((p)[0] = (unsigned char)(((v) >> 8) & 0xff)), \
                              ((p)[1] = (unsigned char)((v) & 0xff)))
#define DNS__SET32BIT(p, v)  (((p)[0] = (unsigned char)(((v) >> 24) & 0xff)), \
                              ((p)[1] = (unsigned char)(((v) >> 16) & 0xff)), \
                              ((p)[2] = (unsigned char)(((v) >> 8) & 0xff)), \
                              ((p)[3] = (unsigned char)((v) & 0xff)))

#if 0
/* we cannot use this approach on systems where we can't access 16/32 bit
   data on un-aligned addresses */
#define DNS__16BIT(p)                   ntohs(*(unsigned short*)(p))
#define DNS__32BIT(p)                   ntohl(*(unsigned long*)(p))
#define DNS__SET16BIT(p, v)             *(unsigned short*)(p) = htons(v)
#define DNS__SET32BIT(p, v)             *(unsigned long*)(p) = htonl(v)
#endif

/* Macros for parsing a DNS header */
#define DNS_HEADER_QID(h)               DNS__16BIT(h)
#define DNS_HEADER_QR(h)                (((h)[2] >> 7) & 0x1)
#define DNS_HEADER_OPCODE(h)            (((h)[2] >> 3) & 0xf)
#define DNS_HEADER_AA(h)                (((h)[2] >> 2) & 0x1)
#define DNS_HEADER_TC(h)                (((h)[2] >> 1) & 0x1)
#define DNS_HEADER_RD(h)                ((h)[2] & 0x1)
#define DNS_HEADER_RA(h)                (((h)[3] >> 7) & 0x1)
#define DNS_HEADER_Z(h)                 (((h)[3] >> 4) & 0x7)
#define DNS_HEADER_RCODE(h)             ((h)[3] & 0xf)
#define DNS_HEADER_QDCOUNT(h)           DNS__16BIT((h) + 4)
#define DNS_HEADER_ANCOUNT(h)           DNS__16BIT((h) + 6)
#define DNS_HEADER_NSCOUNT(h)           DNS__16BIT((h) + 8)
#define DNS_HEADER_ARCOUNT(h)           DNS__16BIT((h) + 10)

/* Macros for constructing a DNS header */
#define DNS_HEADER_SET_QID(h, v)      DNS__SET16BIT(h, v)
#define DNS_HEADER_SET_QR(h, v)       ((h)[2] |= (unsigned char)(((v) & 0x1) << 7))
#define DNS_HEADER_SET_OPCODE(h, v)   ((h)[2] |= (unsigned char)(((v) & 0xf) << 3))
#define DNS_HEADER_SET_AA(h, v)       ((h)[2] |= (unsigned char)(((v) & 0x1) << 2))
#define DNS_HEADER_SET_TC(h, v)       ((h)[2] |= (unsigned char)(((v) & 0x1) << 1))
#define DNS_HEADER_SET_RD(h, v)       ((h)[2] |= (unsigned char)((v) & 0x1))
#define DNS_HEADER_SET_RA(h, v)       ((h)[3] |= (unsigned char)(((v) & 0x1) << 7))
#define DNS_HEADER_SET_Z(h, v)        ((h)[3] |= (unsigned char)(((v) & 0x7) << 4))
#define DNS_HEADER_SET_RCODE(h, v)    ((h)[3] |= (unsigned char)((v) & 0xf))
#define DNS_HEADER_SET_QDCOUNT(h, v)  DNS__SET16BIT((h) + 4, v)
#define DNS_HEADER_SET_ANCOUNT(h, v)  DNS__SET16BIT((h) + 6, v)
#define DNS_HEADER_SET_NSCOUNT(h, v)  DNS__SET16BIT((h) + 8, v)
#define DNS_HEADER_SET_ARCOUNT(h, v)  DNS__SET16BIT((h) + 10, v)

/* Macros for parsing the fixed part of a DNS question */
#define DNS_QUESTION_TYPE(q)            DNS__16BIT(q)
#define DNS_QUESTION_CLASS(q)           DNS__16BIT((q) + 2)

/* Macros for constructing the fixed part of a DNS question */
#define DNS_QUESTION_SET_TYPE(q, v)     DNS__SET16BIT(q, v)
#define DNS_QUESTION_SET_CLASS(q, v)    DNS__SET16BIT((q) + 2, v)

/* Macros for parsing the fixed part of a DNS resource record */
#define DNS_RR_TYPE(r)                  DNS__16BIT(r)
#define DNS_RR_CLASS(r)                 DNS__16BIT((r) + 2)
#define DNS_RR_TTL(r)                   DNS__32BIT((r) + 4)
#define DNS_RR_LEN(r)                   DNS__16BIT((r) + 8)

/* Macros for constructing the fixed part of a DNS resource record */
#define DNS_RR_SET_TYPE(r)              DNS__SET16BIT(r, v)
#define DNS_RR_SET_CLASS(r)             DNS__SET16BIT((r) + 2, v)
#define DNS_RR_SET_TTL(r)               DNS__SET32BIT((r) + 4, v)
#define DNS_RR_SET_LEN(r)               DNS__SET16BIT((r) + 8, v)
#define EVDNS_BUF_SIZE                  512
#define EVDNS_TIMEOUT 10000000
#define EVDNS_TIMEOUT_MAX 6
#define EVDNS_MAX_NUM   32
#ifndef EVDNS_NAME_MAX
#define EVDNS_NAME_MAX  256
#endif
#define DNS_DEFAULT_PORT 53
typedef struct _EVHOSTENT
{
    short nalias;
    short naddrs;
    int qid;
    int  addrs[EVDNS_MAX_NUM];
    unsigned char alias[EVDNS_MAX_NUM][EVDNS_NAME_MAX];
    unsigned char name[EVDNS_NAME_MAX];
}EVHOSTENT;
/* return buffer length*/
int evdns_make_query(char *hostname, int dnsclass, int type, unsigned short id, int rd, unsigned char *buf);
/* parse reply record */
int evdns_parse_reply(unsigned char *buf, int nbuf, EVHOSTENT *hostent);
#endif /* DNS_H */
