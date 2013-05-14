#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include "evdns.h"
/*
static const char *opcodes[] = {
  "QUERY", "IQUERY", "STATUS", "(reserved)", "NOTIFY",
  "(unknown)", "(unknown)", "(unknown)", "(unknown)",
  "UPDATEA", "UPDATED", "UPDATEDA", "UPDATEM", "UPDATEMA",
  "ZONEINIT", "ZONEREF"
};

static const char *rcodes[] = {
  "NOERROR", "FORMERR", "SERVFAIL", "NXDOMAIN", "NOTIMP", "REFUSED",
  "(unknown)", "(unknown)", "(unknown)", "(unknown)", "(unknown)",
  "(unknown)", "(unknown)", "(unknown)", "(unknown)", "NOCHANGE"
};
*/
int evdns_make_query(char *domain, int dnsclass, int type, 
        unsigned short id, int rd, unsigned char *buf)
{
    unsigned char *p = NULL, *q = NULL, *s = NULL, *name = NULL;
    int buflen = 0, len = 0;

    if(domain)
    {
        p = name = (unsigned char *)domain;
        while(*p != '\0')
        {
            if(*p++ != '\\')++len;
        }
        if(*(p-1) != '.') ++len;
        //add end \0 size
        len++;
        if(len > MAXCDNAME) return -1;
        buflen = len + HFIXEDSZ + QFIXEDSZ;
        q = buf;
        memset(q, 0, buflen);
        DNS_HEADER_SET_QID(q, id);
        DNS_HEADER_SET_OPCODE(q, 0);
        if(rd)
        {
            DNS_HEADER_SET_RD(q, 1);
        }
        else
        {
            DNS_HEADER_SET_RD(q, 0);
        }
        DNS_HEADER_SET_QDCOUNT(q, 1);
        if (*name == '.' && *(name + 1) == '\0') name++;
        /* Start writing out the name after the header. */
        q += HFIXEDSZ;
        p = name;
        //fprintf(stdout, "%d:dnsquery:%d:%d\n", __LINE__, (q - buf), buflen);
        do
        {
            s = q++;
            len = 0;
            while(*p != '.' && *p != '\0') 
            {
                if (*p == '\\') ++p;
                else{*q++ = *p++; len++;}
            }
            *s = (unsigned char)len;
            if(*p != '\0')++p; 
        }while(*p != '\0');
        *q++ = 0;
        /* Finish off the question with the type and class. */
        DNS_QUESTION_SET_TYPE(q, type);
        DNS_QUESTION_SET_CLASS(q, dnsclass);
        /*
        if((tmpfd = open("/tmp/buf.txt", O_CREAT|O_RDWR, 0644)) > 0)
        {
            write(tmpfd, buf, buflen);
            close(tmpfd);
        }
        fprintf(stdout, "%d:dnsquery:%d:%d\n", __LINE__, (q - buf), buflen);
        */
        return buflen;
    }
    return -1;
}

/* prase name */
unsigned char *evdns_expand_name(unsigned char *ptr, unsigned char *start, 
        unsigned char *end, unsigned char *name)
{
    unsigned char *p = ptr, *q = name, *ret = NULL;
    int n = 0, flag = 0, offset = 0;

    while(p < end && *p != '\0')
    {
        if(*p >= OFF_MASK)
        {
            if(flag == 0) {ret = p + 2; flag = 1;}
            offset = (((*p & ~OFF_MASK) << 8) | *(p+1));
            p = start + offset;
            //fprintf(stdout, "offset:%d:%d\n", offset, *p);
        }
        else
        {
            n = *p++;
            while(p < end && n-- > 0 && q < (name + EVDNS_NAME_MAX))
            {
                *q++ = *p++;
            }
            if(q < (name + EVDNS_NAME_MAX)) *q++ = '.';
        }
    }
    if(flag == 0) ret = p+1;
    *q = '\0';
    if(q > name && *(q-1) == '.') *(q-1) = '\0';
    //fprintf(stdout, "name:%s\n", name);
    return ret;
}

/* parse reply record */
int evdns_parse_reply(unsigned char *buf, int nbuf, EVHOSTENT *hostent)
{
    unsigned char name[EVDNS_NAME_MAX], *p = NULL, *end = NULL, *s = NULL, *ps = NULL; 
    int i = 0, qdcount = 0, ancount = 0, nscount = 0, arcount = 0, 
        qr = 0, opcode = 0, aa = 0, tc = 0, rd = 0, 
        ra = 0, rcode = 0, type = 0, dnsclass = 0, ttl = 0, rrlen = 0;

    if(buf && nbuf > HFIXEDSZ)
    {
        hostent->naddrs = 0;
        hostent->nalias = 0;
        p = buf;
        end = buf + nbuf;
        hostent->qid = DNS_HEADER_QID(p);
        qr = DNS_HEADER_QR(p);
        opcode = DNS_HEADER_OPCODE(p);
        aa = DNS_HEADER_AA(p);
        tc = DNS_HEADER_TC(p);
        rd = DNS_HEADER_RD(p);
        ra = DNS_HEADER_RA(p);
        rcode = DNS_HEADER_RCODE(p);
        qdcount = DNS_HEADER_QDCOUNT(p);
        ancount = DNS_HEADER_ANCOUNT(p);
        nscount = DNS_HEADER_NSCOUNT(p);
        arcount = DNS_HEADER_ARCOUNT(p);
        p += HFIXEDSZ;
        /* Display the answer header. */
        /*
        printf("id: %d\n", id);
        printf("flags: %s%s%s%s%s\n",
                qr ? "qr " : "",
                aa ? "aa " : "",
                tc ? "tc " : "",
                rd ? "rd " : "",
                ra ? "ra " : "");
        printf("opcode: %s\n", opcodes[opcode]);
        printf("rcode: %s\n", rcodes[rcode]);
        fprintf(stdout, "qdcount:%d\nancount:%d\nnscount:%d\narcount:%d\n", 
                qdcount, ancount, nscount, arcount);
        */
        /* parse question */
        if(p >= end) return -1;
        for(i = 0; i < qdcount; i++)
        {
            ps = hostent->name;
            s = evdns_expand_name(p, buf, end, ps);
            if(s == p || (s+QFIXEDSZ) > end) return -1;
            p = s;
            /* Parse the question type and class. */
            type = DNS_QUESTION_TYPE(p);
            dnsclass = DNS_QUESTION_CLASS(p);
            p  += QFIXEDSZ;
            if(p > end) return -1;
            /*
            fprintf(stdout, "qname:%-15s", name);
            fprintf(stdout, "\tqtype:%d", type);
            fprintf(stdout, "\tqclass:%d\r\n", dnsclass);
            */
        }
        if(p > end) return -1;
        /* parse A name */
        for(i = 0; i < ancount; i++)
        {
            ps = hostent->alias[hostent->nalias++];
            s = evdns_expand_name(p, buf, end, ps);
            if(s == p || (s+RRFIXEDSZ) > end) return -1;
            p = s;
            type = DNS_RR_TYPE(p);
            dnsclass = DNS_RR_CLASS(p);
            ttl = DNS_RR_TTL(p);
            rrlen = DNS_RR_LEN(p);
            p += RRFIXEDSZ;
            if(p > end || (p+rrlen) > end) return -1;
            /*
            fprintf(stdout, "name:%s type:%d dnsclass:%d ttl:%d rrlen:%d ", 
                    name, type, dnsclass, ttl, rrlen);
            */
            /* addr name */
            if(type == TYPE_ANAME)
            {
                if(rrlen <= sizeof(int))
                    memcpy(&(hostent->addrs[hostent->naddrs++]), p, rrlen);
            }
            /* Canonical name */
            else if(type == TYPE_CNAME)
            {
                ps = hostent->alias[hostent->nalias++];
                s = evdns_expand_name(p, buf, end, ps);
                //fprintf(stdout, "cname:%s ", cname);
            }
            /* pointer */
            else if(type == TYPE_PTR)
            {
                ps = hostent->alias[hostent->nalias++];
                s = evdns_expand_name(p, buf, end, ps);
                //fprintf(stdout, "pointer:%s ", cname);
            }
            //fprintf(stdout, "\r\n");
            p += rrlen;
        }
        if(p > end) return -1;
        /* parse name server  */
        for(i = 0; i < nscount; i++)
        {
            ps = name;
            s = evdns_expand_name(p, buf, end, ps);
            if(s == p || (s+RRFIXEDSZ) > end) return -1;
            p = s;
            type = DNS_RR_TYPE(p);
            dnsclass = DNS_RR_CLASS(p);
            ttl = DNS_RR_TTL(p);
            rrlen = DNS_RR_LEN(p);
            p += RRFIXEDSZ;
            if(p > end || (p+rrlen) > end) return -1;
            p += rrlen;
        }
        if(p > end) return -1;
        /* parse additional record  */
        for(i = 0; i < arcount; i++)
        {
            ps = name;
            s = evdns_expand_name(p, buf, end, ps);
            if(s == p || (s+RRFIXEDSZ) > end) return -1;
            p = s;
            type = DNS_RR_TYPE(p);
            dnsclass = DNS_RR_CLASS(p);
            ttl = DNS_RR_TTL(p);
            rrlen = DNS_RR_LEN(p);
            p += RRFIXEDSZ;
            if(p >= end || (p+rrlen) > end) return -1;
            p += rrlen;
        }
        return (p - buf);
    }
    return -1;
}

#ifdef _DEBUG_EVDNS
int main()
{
    unsigned char dnsbuf[EVDNS_BUF_SIZE];
    int ndnsbuf = 0;

    ndnsbuf = evdns_make_query("www.china.com", 1, 1, 0, 0, dnsbuf);
    return 0;
}
#endif
