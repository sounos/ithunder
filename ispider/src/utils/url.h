#ifndef _URL_H_
#define _URL_H_
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#define MATCHEURL(count, p, pp, epp, s, es, x, pres, content)                               \
do                                                                                          \
{                                                                                           \
    while(*p != '\0' && pp < epp)                                                           \
    {                                                                                       \
        if(*p == '<')                                                                       \
        {                                                                                   \
            ++p;                                                                            \
            if(*p == '\0') goto errbreak;                                                   \
            x = atoi(p);                                                                    \
            if(x > count && x < 0) goto errbreak;                                           \
            s = content + pres[x*2];                                                        \
            es = content + pres[x*2+1];                                                     \
            while(s < es && pp < epp)                                                       \
            {                                                                               \
                if(*s == '\r' || *s == '\n' || *s == '\t')++s;                              \
                else if(*((unsigned char *)s) > 127 || *s == 0x20)                          \
                {                                                                           \
                    if(pp > (epp - 3)) goto errbreak;                                       \
                    pp += sprintf(pp, "%%%02x", *((unsigned char *)s));                     \
                }else *pp++ = *s++;                                                         \
            }                                                                               \
            while(*p != '\0' && *p != '>')++p;                                              \
            if(*p == '>')++p;                                                               \
            else goto errbreak;                                                             \
        }                                                                                   \
        else *pp++ = *p++;                                                                  \
    }                                                                                       \
    *pp = '\0';break;                                                                       \
errbreak:                                                                                   \
    --pp;                                                                                   \
}while(0)

#define CPURL(s, es, p, e, pp, epp, end, host, path, last)                                  \
do                                                                                          \
{                                                                                           \
    if(strncasecmp(p, "http://", 7) == 0)                                                   \
    {                                                                                       \
        host = pp + 7;                                                                      \
        while(p < e && pp < epp)                                                            \
        {                                                                                   \
            if(pp > host && path == NULL && *p == '/') path = pp;                           \
            if(*p == '\r' || *p == '\n' || *p == '\t')++p;                                  \
            else if(*((unsigned char *)p) > 127 || *p == 0x20)                              \
            {                                                                               \
                if(pp > (epp - 3)) break;                                                   \
                pp += sprintf(pp, "%%%02x", *((unsigned char *)p));                         \
                ++p;                                                                        \
            }                                                                               \
            else *pp++ = *p++;                                                              \
        }                                                                                   \
    }                                                                                       \
    else                                                                                    \
    {                                                                                       \
        end = NULL;                                                                         \
        path = NULL;                                                                        \
        last = NULL;                                                                        \
        while(s < es && pp < epp)                                                           \
        {                                                                                   \
            if(*s == ':' && *(s+1) == '/' && *(s+2) == '/')                                 \
            {                                                                               \
                if(pp > (epp - 3)) break;                                                   \
                *pp++ = *s++;                                                               \
                *pp++ = *s++;                                                               \
                *pp++ = *s++;                                                               \
                host = pp;                                                                  \
            }                                                                               \
            else if(host && path == NULL && *s == '/')                                      \
            {                                                                               \
                last = path = pp;                                                           \
                *pp++ = *s++;                                                               \
            }                                                                               \
            else if(path && end == NULL && *s == '?')                                       \
            {                                                                               \
                end = pp;                                                                   \
                *pp++ = *s++;                                                               \
            }                                                                               \
            else if(path && end == NULL && *s == '/')                                       \
            {                                                                               \
                last = pp;                                                                  \
                *pp++ = *s++;                                                               \
            }                                                                               \
            else *pp++ = *s++;                                                              \
        }                                                                                   \
        *pp = '\0';                                                                         \
        if(*p == '/')                                                                       \
        {                                                                                   \
            if(path) pp = path;                                                             \
        }                                                                                   \
        else if(*p == '?')                                                                  \
        {                                                                                   \
            if(end)pp = end;                                                                \
        }                                                                                   \
        else                                                                                \
        {                                                                                   \
            if(path == NULL) {path = pp; *path = '/';}                                      \
            if(last == NULL) {last = path;}                                                 \
            if(*p == '.')                                                                   \
            {                                                                               \
                if(*(p+1) == '/') p += 2;                                                   \
                while(p < e && *p == '.' && *(p+1) == '.' && *(p+2) == '/')                 \
                {                                                                           \
                    p += 3;                                                                 \
                    while(p < e && *p == '/' && *(p+1) == '/') ++p;                         \
                    if(*last == '/') last--;                                                \
                    while(last > path && *last != '/') --last;                              \
                }                                                                           \
            }                                                                               \
            if(*last == '/')last++;                                                         \
            pp = last;                                                                      \
        }                                                                                   \
        while(p < e && pp < epp)                                                            \
        {                                                                                   \
            if(*p == '\r' || *p == '\n' || *p == '\t')++p;                                  \
            else if(*((unsigned char *)p) > 127 || *p == 0x20)                              \
            {                                                                               \
                if(pp > (epp - 3)) break;                                                   \
                pp += sprintf(pp, "%%%02x", *((unsigned char *)p));                         \
                ++p;                                                                        \
            }else *pp++ = *p++;                                                             \
        }                                                                                   \
        *pp = '\0';                                                                         \
    }                                                                                       \
}while(0)
#ifdef _DEBUG_URL
#define HTTP_URL_MAX 4096
/* url max */
int main()
{
    char *s = NULL, *es = NULL, *p = NULL, *e = NULL, *pp = NULL, *epp = NULL,
         *end = NULL, *host = NULL, *path = NULL, *last = NULL, newurl[HTTP_URL_MAX];
    int n = 0;

        pp = newurl; epp = newurl + HTTP_URL_MAX;
        s = "http://www.china.com/sdfkhksdf";
        es = s + strlen(s);
        p = "?sdd=sdjfdf";
        e = p + strlen(p);
        CPURL(s, es, p, e, pp, epp, end, host, path, last);
        fprintf(stdout, "%s::%d url:%s\n", __FILE__, __LINE__, newurl);
        return 0;

        pp = newurl; epp = newurl + HTTP_URL_MAX;
        s = "http://www.china.com";
        es = s + strlen(s);
        p = "http://sss.com/dsfadsfds/dfkljldsf.html";
        e = p + strlen(p);
        CPURL(s, es, p, e, pp, epp, end, host, path, last);
        fprintf(stdout, "%s::%d url:%s\n", __FILE__, __LINE__, newurl);

        pp = newurl; epp = newurl + HTTP_URL_MAX;
        s = "http://www.china.com";
        es = s + strlen(s);
        p = "/dsfadsfds/dfkljldsf.html";
        e = p + strlen(p);
        CPURL(s, es, p, e, pp, epp, end, host, path, last);
        fprintf(stdout, "%s::%d url:%s\n", __FILE__, __LINE__, newurl);

        pp = newurl; epp = newurl + HTTP_URL_MAX;
        s = "http://www.china.com";
        es = s + strlen(s);
        p = "dsfadsfds/dfkljldsf.html";
        e = p + strlen(p);
        CPURL(s, es, p, e, pp, epp, end, host, path, last);
        fprintf(stdout, "%s::%d url:%s\n", __FILE__, __LINE__, newurl);

        pp = newurl; epp = newurl + HTTP_URL_MAX;
        s = "http://www.china.com";
        es = s + strlen(s);
        p = "./dsfadsfds/dfkljldsf.html";
        e = p + strlen(p);
        CPURL(s, es, p, e, pp, epp, end, host, path, last);
        fprintf(stdout, "%s::%d url:%s\n", __FILE__, __LINE__, newurl);

        pp = newurl; epp = newurl + HTTP_URL_MAX;
        s = "http://www.china.com";
        es = s + strlen(s);
        p = "../dsfadsfds/dfkljldsf.html";
        e = p + strlen(p);
        CPURL(s, es, p, e, pp, epp, end, host, path, last);
        fprintf(stdout, "%s::%d url:%s\n", __FILE__, __LINE__, newurl);

        pp = newurl; epp = newurl + HTTP_URL_MAX;
        s = "http://www.china.com/sdklfjkls/sdfkljdslf/dklfjalf";
        es = s + strlen(s);
        p = "../dsfadsfds/dfkljldsf.html";
        e = p + strlen(p);
        CPURL(s, es, p, e, pp, epp, end, host, path, last);
        fprintf(stdout, "%s::%d url:%s\n", __FILE__, __LINE__, newurl);

         pp = newurl; epp = newurl + HTTP_URL_MAX;
        s = "http://www.china.com/dklsjfld/8389/dskfjasf/2111.html";
        es = s + strlen(s);
        p = "../../../dsfadsfds/dfkljldsf.html";
        e = p + strlen(p);
        CPURL(s, es, p, e, pp, epp, end, host, path, last);
        fprintf(stdout, "%s::%d url:%s\n", __FILE__, __LINE__, newurl);

}
#endif
#endif
