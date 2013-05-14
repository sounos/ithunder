#include "http.h"
#include "mtrie.h"
#ifdef _HTTP_CHARSET_CONVERT
#define _GNU_SOURCE
#include <iconv.h>
#include "chardet.h"
#include "stime.h"
#include "zstream.h"
#define CHARSET_MAX 256
#endif
#ifndef _STATIS_YMON
#define _STATIS_YMON
/*
static const char *wdays[]={"Sun","Mon","Tue","Wed","Thu","Fri","Sat"};
static const char *ymonths[]= {
    "Jan", "Feb", "Mar",
    "Apr", "May", "Jun",
    "Jul", "Aug", "Sep",
    "Oct", "Nov", "Dec"};
    */
#endif
#define HEX2CH(c, x) ( ((x = (c - '0')) >= 0 && x < 10) \
        || ((x = (c - 'a')) >= 0 && (x += 10) < 16) \
        || ((x = (c - 'A')) >= 0 && (x += 10) < 16) )
#define URLENCODE(s, e, src)                                                    \
do                                                                              \
{                                                                               \
    while(*src != '\0' && s < e)                                                \
    {                                                                           \
        if(*src == 0x20)                                                        \
        {                                                                       \
            *s++ = '+';                                                         \
            ++src;                                                              \
        }                                                                       \
        else if(*((unsigned char *)src) > 127 && s < (e - 2))                   \
        {                                                                       \
            s += sprintf(s, "%%%02X", *((unsigned char *)src));                 \
            ++src;                                                              \
        }                                                                       \
        else *s++ = *s++;                                                       \
    }                                                                           \
    if(s < e) *s = '\0';                                                        \
}while(0)
#define URLDECODE(s, end, high, low, pp)                                            \
do                                                                                  \
{                                                                                   \
    if(*s == '%' && s < (end - 2) && HEX2CH(*(s+1), high)  && HEX2CH(*(s+2), low))  \
    {                                                                               \
        *pp++ = (high << 4) | low;                                                  \
        s += 3;                                                                     \
    }                                                                               \
    else *pp++ = *s++;                                                              \
}while(0)
static const char *http_encodings[] = {"deflate", "gzip", "bzip2", "compress"}; 
static unsigned long crc32_tab[] = {
    0x00000000L, 0x77073096L, 0xee0e612cL, 0x990951baL, 0x076dc419L,
    0x706af48fL, 0xe963a535L, 0x9e6495a3L, 0x0edb8832L, 0x79dcb8a4L,
    0xe0d5e91eL, 0x97d2d988L, 0x09b64c2bL, 0x7eb17cbdL, 0xe7b82d07L,
    0x90bf1d91L, 0x1db71064L, 0x6ab020f2L, 0xf3b97148L, 0x84be41deL,
    0x1adad47dL, 0x6ddde4ebL, 0xf4d4b551L, 0x83d385c7L, 0x136c9856L,
    0x646ba8c0L, 0xfd62f97aL, 0x8a65c9ecL, 0x14015c4fL, 0x63066cd9L,
    0xfa0f3d63L, 0x8d080df5L, 0x3b6e20c8L, 0x4c69105eL, 0xd56041e4L,
    0xa2677172L, 0x3c03e4d1L, 0x4b04d447L, 0xd20d85fdL, 0xa50ab56bL,
    0x35b5a8faL, 0x42b2986cL, 0xdbbbc9d6L, 0xacbcf940L, 0x32d86ce3L,
    0x45df5c75L, 0xdcd60dcfL, 0xabd13d59L, 0x26d930acL, 0x51de003aL,
    0xc8d75180L, 0xbfd06116L, 0x21b4f4b5L, 0x56b3c423L, 0xcfba9599L,
    0xb8bda50fL, 0x2802b89eL, 0x5f058808L, 0xc60cd9b2L, 0xb10be924L,
    0x2f6f7c87L, 0x58684c11L, 0xc1611dabL, 0xb6662d3dL, 0x76dc4190L,
    0x01db7106L, 0x98d220bcL, 0xefd5102aL, 0x71b18589L, 0x06b6b51fL,
    0x9fbfe4a5L, 0xe8b8d433L, 0x7807c9a2L, 0x0f00f934L, 0x9609a88eL,
    0xe10e9818L, 0x7f6a0dbbL, 0x086d3d2dL, 0x91646c97L, 0xe6635c01L,
    0x6b6b51f4L, 0x1c6c6162L, 0x856530d8L, 0xf262004eL, 0x6c0695edL,
    0x1b01a57bL, 0x8208f4c1L, 0xf50fc457L, 0x65b0d9c6L, 0x12b7e950L,
    0x8bbeb8eaL, 0xfcb9887cL, 0x62dd1ddfL, 0x15da2d49L, 0x8cd37cf3L,
    0xfbd44c65L, 0x4db26158L, 0x3ab551ceL, 0xa3bc0074L, 0xd4bb30e2L,
    0x4adfa541L, 0x3dd895d7L, 0xa4d1c46dL, 0xd3d6f4fbL, 0x4369e96aL,
    0x346ed9fcL, 0xad678846L, 0xda60b8d0L, 0x44042d73L, 0x33031de5L,
    0xaa0a4c5fL, 0xdd0d7cc9L, 0x5005713cL, 0x270241aaL, 0xbe0b1010L,
    0xc90c2086L, 0x5768b525L, 0x206f85b3L, 0xb966d409L, 0xce61e49fL,
    0x5edef90eL, 0x29d9c998L, 0xb0d09822L, 0xc7d7a8b4L, 0x59b33d17L,
    0x2eb40d81L, 0xb7bd5c3bL, 0xc0ba6cadL, 0xedb88320L, 0x9abfb3b6L,
    0x03b6e20cL, 0x74b1d29aL, 0xead54739L, 0x9dd277afL, 0x04db2615L,
    0x73dc1683L, 0xe3630b12L, 0x94643b84L, 0x0d6d6a3eL, 0x7a6a5aa8L,
    0xe40ecf0bL, 0x9309ff9dL, 0x0a00ae27L, 0x7d079eb1L, 0xf00f9344L,
    0x8708a3d2L, 0x1e01f268L, 0x6906c2feL, 0xf762575dL, 0x806567cbL,
    0x196c3671L, 0x6e6b06e7L, 0xfed41b76L, 0x89d32be0L, 0x10da7a5aL,
    0x67dd4accL, 0xf9b9df6fL, 0x8ebeeff9L, 0x17b7be43L, 0x60b08ed5L,
    0xd6d6a3e8L, 0xa1d1937eL, 0x38d8c2c4L, 0x4fdff252L, 0xd1bb67f1L,
    0xa6bc5767L, 0x3fb506ddL, 0x48b2364bL, 0xd80d2bdaL, 0xaf0a1b4cL,
    0x36034af6L, 0x41047a60L, 0xdf60efc3L, 0xa867df55L, 0x316e8eefL,
    0x4669be79L, 0xcb61b38cL, 0xbc66831aL, 0x256fd2a0L, 0x5268e236L,
    0xcc0c7795L, 0xbb0b4703L, 0x220216b9L, 0x5505262fL, 0xc5ba3bbeL,
    0xb2bd0b28L, 0x2bb45a92L, 0x5cb36a04L, 0xc2d7ffa7L, 0xb5d0cf31L,
    0x2cd99e8bL, 0x5bdeae1dL, 0x9b64c2b0L, 0xec63f226L, 0x756aa39cL,
    0x026d930aL, 0x9c0906a9L, 0xeb0e363fL, 0x72076785L, 0x05005713L,
    0x95bf4a82L, 0xe2b87a14L, 0x7bb12baeL, 0x0cb61b38L, 0x92d28e9bL,
    0xe5d5be0dL, 0x7cdcefb7L, 0x0bdbdf21L, 0x86d3d2d4L, 0xf1d4e242L,
    0x68ddb3f8L, 0x1fda836eL, 0x81be16cdL, 0xf6b9265bL, 0x6fb077e1L,
    0x18b74777L, 0x88085ae6L, 0xff0f6a70L, 0x66063bcaL, 0x11010b5cL,
    0x8f659effL, 0xf862ae69L, 0x616bffd3L, 0x166ccf45L, 0xa00ae278L,
    0xd70dd2eeL, 0x4e048354L, 0x3903b3c2L, 0xa7672661L, 0xd06016f7L,
    0x4969474dL, 0x3e6e77dbL, 0xaed16a4aL, 0xd9d65adcL, 0x40df0b66L,
    0x37d83bf0L, 0xa9bcae53L, 0xdebb9ec5L, 0x47b2cf7fL, 0x30b5ffe9L,
    0xbdbdf21cL, 0xcabac28aL, 0x53b39330L, 0x24b4a3a6L, 0xbad03605L,
    0xcdd70693L, 0x54de5729L, 0x23d967bfL, 0xb3667a2eL, 0xc4614ab8L,
    0x5d681b02L, 0x2a6f2b94L, 0xb40bbe37L, 0xc30c8ea1L, 0x5a05df1bL,
    0x2d02ef8dL
};

/* Return a 32-bit CRC of the contents of the buffer. */
unsigned long http_crc32(unsigned char *s, unsigned int len)
{
    unsigned int i = 0;
    unsigned long crc32val = 0;

    for (i = 0;  i < len;  i ++)
    {
        crc32val = crc32_tab[(crc32val ^ s[i]) & 0xff] ^ (crc32val >> 8);
    }
    return crc32val;
}

int http_base64encode(char *src, int src_len, char *dst)
{
    int i = 0, j = 0;
    char base64_map[65] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    for (; i < src_len - src_len % 3; i += 3)
    {
        dst[j++] = base64_map[(src[i] >> 2) & 0x3F];
        dst[j++] = base64_map[((src[i] << 4) & 0x30) + ((src[i + 1] >> 4) & 0xF)];
        dst[j++] = base64_map[((src[i + 1] << 2) & 0x3C) + ((src[i + 2] >> 6) & 0x3)];
        dst[j++] = base64_map[src[i + 2] & 0x3F];
    }

    if (src_len % 3 == 1) 
    {
        dst[j++] = base64_map[(src[i] >> 2) & 0x3F];
        dst[j++] = base64_map[(src[i] << 4) & 0x30];
        dst[j++] = '=';
        dst[j++] = '=';
    }
    else if (src_len % 3 == 2) 
    {
        dst[j++] = base64_map[(src[i] >> 2) & 0x3F];
        dst[j++] = base64_map[((src[i] << 4) & 0x30) + ((src[i + 1] >> 4) & 0xF)];
        dst[j++] = base64_map[(src[i + 1] << 2) & 0x3C];
        dst[j++] = '=';
    }
    dst[j] = '\0';
    return j;
}

int http_base64decode(unsigned char *src, int src_len, unsigned char *dst)
{
    int i = 0, j = 0;
    char base64_decode_map[256] = {
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 62, 255, 255, 255, 63, 52, 53, 54, 
        55, 56, 57, 58, 59, 60, 61, 255, 255, 255, 0, 255, 
        255, 255, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 
        13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 
        255, 255, 255, 255, 255, 255, 26, 27, 28, 29, 30, 31, 
        32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 
        46, 47, 48, 49, 50, 51, 255, 255, 255, 255, 255, 255, 255, 
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 
        255, 255, 255, 255, 255, 255};
    for (; i < src_len; i += 4) 
    {
        dst[j++] = (base64_decode_map[src[i]] << 2) 
            | (base64_decode_map[src[i + 1]] >> 4);
        dst[j++] = (base64_decode_map[src[i + 1]] << 4) 
            | (base64_decode_map[src[i + 2]] >> 2);
        dst[j++] = (base64_decode_map[src[i + 2]] << 6) 
            | (base64_decode_map[src[i + 3]]);
    }
    dst[j] = '\0';
    return j;
}
void *http_headers_map_init()
{
    char *p = NULL, *s = NULL, line[HTTP_HEAD_MAX];
    void *map = NULL;
    int i = 0;

    if((map = mtrie_init()))
    {
        for(i = 0; i < HTTP_HEADER_NUM; i++)
        {
            p = http_headers[i].e;
            s = line;
            while(*p != '\0')
            {
                if(*p >= 'A' && *p <= 'Z')
                {
                    *s++ = *p++ - ('A' - 'a'); 
                }
                else *s++ = *p++;
            }
            *s = '\0';
            mtrie_add(map, line, http_headers[i].elen, i+1);
        }
        for(i = 0; i < HTTP_METHOD_NUM; i++)
        {
            p = http_methods[i].e;
            s = line;
            while(*p != '\0')
            {
                if(*p >= 'A' && *p <= 'Z')
                {
                    *s++ = *p++ - ('A' - 'a'); 
                }
                else *s++ = *p++;
            }
            *s = '\0';
            mtrie_add(map, line, http_methods[i].elen, i+1);
        }
        for(i = 0; i < HTTP_RESPONSE_NUM; i++)
        {
            mtrie_add(map, response_status[i].e, response_status[i].elen, i+1);
        }
    }
    return map;
}

void http_headers_map_clean(void *map)
{
    if(map) mtrie_clean(map);
}
/* parse argv line */
int http_argv_parse(char *p, char *end, HTTP_REQ *http_req)
{
    char *pp = NULL, *epp = NULL, *s = NULL;
    HTTP_KV *argv = NULL, *eargv = NULL;
    int n = 0, high = 0, low = 0;

    if(p && end && (s = p) < end && http_req)
    {
        argv = &(http_req->argvs[http_req->nargvs]);
        eargv = &(http_req->argvs[HTTP_ARGVS_MAX+1]);
        if(http_req->nline == 0) http_req->nline = 1;
        argv->k = http_req->nline;
        pp = http_req->line + http_req->nline;
        epp = http_req->line + HTTP_ARGV_LINE_MAX;
        while(s < end && *s != '\r' && *s != '\n' && *s != 0x20 && argv < eargv && pp < epp)
        {
            high = 0;low = 0;
            //if(*s == '?'){argv->k = pp - http_req->line; ++s;}
            if(*s == '+'){*pp++ = 0x20; ++s;}
            else if(*s == '=' && argv->k && !(argv->v))
            {
                if(argv->k > 0) argv->nk = pp - http_req->line - argv->k;
                if(pp >= epp) break;
                *pp++ = '\0';
                argv->v = pp - http_req->line;
                ++s;
            }
            else if(*s == '&' && argv->k && argv->v)
            {
                argv->nv = pp - http_req->line - argv->v;
                if(pp >= epp) break;
                *pp++ = '\0';
                http_req->nline = pp - http_req->line;
                http_req->nargvs++;
                argv++;
                argv->k = pp - http_req->line;
                argv->v = 0;
                ++s;
            }
            else if(*s == '%' && s < (end - 2) && HEX2CH(*(s+1), high)  
                    && HEX2CH(*(s+2), low))
            {
                if(pp >= epp) break;
                *pp++ = (high << 4) | low;
                s += 3;
            }
            else if(argv->k || argv->v)
            {
                if(pp >= epp) break;
                *pp++ = *s++;
            }
            else ++s;
        }
        if(argv < eargv && argv->k && argv->v)
        {
            argv->nv = pp - http_req->line - argv->v;
            if(pp >= epp) goto end;
            *pp++ = '\0';
            http_req->nline = pp - http_req->line;
            http_req->nargvs++;
            argv++;
        }
end:
        n = s - p;
    }
    return n;
}

/* HTTP cookie parser */
int http_cookie_parse(char *p, char *end, HTTP_REQ *http_req)
{    
    HTTP_KV *cookie = NULL, *ecookie = NULL;
    char *s = NULL, *pp = NULL, *epp = NULL;
    int n = 0, high = 0, low = 0;

    if(p && end && (s = p) < end && http_req)
    {
        cookie = &(http_req->cookies[http_req->ncookies]);
        ecookie = &(http_req->cookies[HTTP_COOKIES_MAX+1]);
        if(http_req->nhline == 0) http_req->nhline = 1;
        cookie->k = http_req->nhline;
        pp = http_req->hlines + http_req->nhline;
        epp = http_req->hlines + HTTP_HEADER_MAX;
        while(s < end && *s != '\r' && cookie < ecookie && pp < epp)
        {
            high = 0;low = 0;
            if(*s == '+'){*pp++ = 0x20; ++s;}
            else if(*s == '=' && cookie->k && !cookie->v)
            {
                if(cookie->k > 0) cookie->nk = pp - http_req->hlines - cookie->k;
                if(pp >= epp) break;
                *pp++ = *s++;
                cookie->v = pp - http_req->hlines;
            }
            else if((*s == ';' || *s == 0x20 || *s == '\t')
                    && cookie->k && cookie->v)
            {
                cookie->nv = pp - http_req->hlines - cookie->v;
                if(pp >= epp) break;
                *pp++ = *s;
                http_req->nhline = pp - http_req->hlines;
                http_req->ncookies++;
                cookie++;
                if(*s++ == ';') cookie->k = pp - http_req->hlines;
                while(s < end && *s == 0x20) ++s;
            }
            else if(*s == '%' && s < (end - 2) && HEX2CH(*(s+1), high)  && HEX2CH(*(s+2), low))
            {
                if(pp >= epp) break;
                *pp++ = (high << 4) | low;
                s += 3;
            }
            else if(cookie->k || cookie->v)
            {
                if(pp >= epp) break;
                *pp++ = *s++;
            }
            else ++s;
            if((s == end || *s == '\r') && cookie < ecookie && cookie->k && cookie->v)
            {
                cookie->nv = pp - http_req->hlines - cookie->v;
                http_req->nhline = pp - http_req->hlines;
                http_req->ncookies++;
                cookie++;
                break;
            }
        }
        n = s - p;

    }
    return n;
}

/* HTTP response cookie parser */
int http_resp_cookie_parse(char *p, char *end, HTTP_RESPONSE *http_resp)
{    
    char *s = NULL, *pp = NULL, *epp = NULL, *ss = NULL, *sp = NULL;
    HTTP_COOKIE *cookie = NULL;
    int n = 0;

    if(p && (s = p) < end && http_resp && http_resp->ncookies < HTTP_COOKIES_MAX)
    {
        cookie = &(http_resp->cookies[http_resp->ncookies]);
        if(http_resp->nhline == 0) http_resp->nhline = 1;
        pp = http_resp->hlines + http_resp->nhline;
        epp = http_resp->hlines + HTTP_HEADER_MAX;
        while(*s == 0x20 || *s == '\t')++s;
        if((sp = strchr(s, '='))  && (n = (sp - s)) > 0 && pp < epp) 
        {
            /* name */
            cookie->kv.k = pp - http_resp->hlines;
            while(s < sp)
            {
                *pp++ = *s++; 
            }
            cookie->kv.nk = pp - http_resp->hlines - cookie->kv.k; 
            *pp++ = '\0';
            s = ++sp;
            /* value */
            cookie->kv.v = pp - http_resp->hlines;
            while(pp < epp && *s != ';' && *s != '\r' && s < end)
            {
                *pp++ = *s++; 
            }
            if(*s == ';') ++s;
            cookie->kv.nv = pp - http_resp->hlines - cookie->kv.v;
            *pp++ = '\0';
            /* expire */
            sp = "expires=";
            n = strlen(sp);
            if((ss = strstr(s, sp)))
            {
                s = ss + n;
                sp = pp;
                cookie->expire_off = pp - http_resp->hlines;
                while(pp < epp && *s != ';' && *s != '\r' && s < end)
                {
                    *pp++ = *s++; 
                }
                if(*s == ';') ++s;
                cookie->expire_len = pp - sp; 
                *pp++ = '\0';
            }
            /* path */
            sp = "path=";
            n = strlen(sp);
            if((ss = strstr(s, sp)))
            {
                s = ss + n;
                sp = pp;
                cookie->path_off = pp - http_resp->hlines;
                while(pp < epp && *s != ';' && *s != '\r' && s < end)
                {
                    *pp++ = *s++; 
                }
                if(*s == ';') ++s;
                cookie->path_len = pp - sp; 
                *pp++ = '\0';
            }
            /* domain */
            sp = "domain=";
            n = strlen(sp);
            if((ss = strstr(s, sp)))
            {
                s = ss + n;
                sp = pp;
                cookie->domain_off = pp - http_resp->hlines;
                while(pp < epp && *s != ';' && *s != '\r' && s < end)
                {
                    *pp++ = *s++;
                }
                if(*s == ';') ++s;
                cookie->domain_len = pp - sp; 
                *pp++ = '\0';
            }
            http_resp->ncookies++;
        }
        http_resp->nhline = pp - http_resp->hlines;
        n = s - p;
    }
    return n;
}

/* cookie line */
int http_cookie_line(HTTP_RESPONSE *http_resp, char *cookie)
{
    char *p = NULL, *bs = NULL;
    int ret = 0, i = 0;

    if(http_resp && (p = cookie))
    {
        if(http_resp->ncookies > 0)
        {
            bs = http_resp->hlines;
            i = 0;
            do
            {
                p += sprintf(p, "%.*s=%.*s;", 
                        http_resp->cookies[i].kv.nk, bs + http_resp->cookies[i].kv.k, 
                        http_resp->cookies[i].kv.nv, bs + http_resp->cookies[i].kv.v); 
                if(http_resp->cookies[i].expire_len > 0)
                {
                    /* expire */
                    p += sprintf(p, "e=%u;", (unsigned int)str2time(bs+http_resp->cookies[i].expire_off));
                }
                if(http_resp->cookies[i].path_len > 0)
                {
                    /* path */
                    p += sprintf(p, "p=%.*s;", http_resp->cookies[i].path_len,
                            bs + http_resp->cookies[i].path_off);
                }
                if(http_resp->cookies[i].domain_len > 0)
                {
                    /* domain */
                    p += sprintf(p, "h=%.*s;", http_resp->cookies[i].domain_len,
                            bs + http_resp->cookies[i].domain_off);
                }
                *p++ = '\n';
            }while(++i < http_resp->ncookies);
            *p++ = '\0';
            ret  = p - cookie;
        }
    }
    return ret;
}

/* HTTP HEADER parser */
int http_request_parse(char *p, char *end, HTTP_REQ *http_req, void *map)
{
    char line[HTTP_HEAD_MAX], *x = NULL, *s = p, *es = NULL, *ps = NULL,
         *eps = NULL, *pp = NULL, *epp = NULL, *sp = NULL;
    int i  = 0, high = 0, low = 0, ret = -1, n = 0;

    if(p && end)
    {
        //request method
        //while(s < end && *s != 0x20 && *s != 0x09)++s;
        pp = http_req->hlines + 1;
        epp = pp + HTTP_HEADER_MAX;
        while(s < end && (*s == 0x20 || *s == 0x09))++s;
        x = pp;
        while(s < end && *s != 0x20 && pp < epp) 
        {
            if(*s >= 'A' && *s <= 'Z')
            {
                *pp++ = *s++ - ('A' - 'a');
            }
            else *pp++ = *s++;
        }
        if((n = (pp - x)) > 0 && (i = (mtrie_get(map, x, n) - 1)) >= 0)
        {
            http_req->reqid = i;
        }
        /*
        for(i = 0; i < HTTP_METHOD_NUM; i++)
        {
            if(strstr(s, http_methods[i].e))
            {
                http_req->reqid = i;
                s += http_methods[i].elen;
                break;
            }
        }
        */
        //path
        while(s < end && *s == 0x20)++s;
        ps = http_req->path;
        eps = ps + HTTP_URL_PATH_MAX;
        while(s < end && *s != 0x20 && *s != '\r' && *s != '?' && ps < eps)
	    {
		    URLDECODE(s, end, high, low, ps);
	    }
        if(ps >= eps ) goto end;
        *ps = '\0';
        if(*s == '?') 
        {
            http_req->argv_off = ++s - p;
            s += http_argv_parse(s, end, http_req);
            http_req->argv_len = s - p - http_req->argv_off; 
        }
        while(s < end && *s != '\n')s++;
        s++;
        while(s < end)
        {
            //parse response  code 
            /* ltrim */
            while(*s == 0x20)s++;
            es = line;
            while(s < end && *s != 0x20)
            {
                if(*s >= 'A' && *s <= 'Z')
                {
                    *es++ = *s++ - ('A' - 'a');
                }
                else *es++ = *s++;
            }
            *es = '\0';
            n = es - line;
            if(((i = mtrie_get(map, line, n) - 1)) >= 0)
            {
                http_req->headers[i] = pp - http_req->hlines;
                ret++;
            }
            while(s < end && *s == 0x20)s++;
            /*
            for(i = 0; i < HTTP_HEADER_NUM; i++)
            {
                if((end - s) >= http_headers[i].elen
                        && strncasecmp(s, http_headers[i].e, http_headers[i].elen) == 0)
                {
                    //fprintf(stdout, "%s:%d path:%s\n", __FILE__, __LINE__, s);
                    s +=  http_headers[i].elen;
                    http_req->headers[i] = pp - http_req->hlines;
                    ret++;
                    break;
                }
            }
            */
            if(i == HEAD_REQ_COOKIE)
            {
                s += http_cookie_parse(s, end, http_req);
                //fprintf(stdout, "cookie:%s\n", s);
                pp = http_req->hlines + http_req->nhline;
            }
            else if(i == HEAD_REQ_AUTHORIZATION && strncasecmp(s, "Basic", 5) == 0)
            {
                while(s < end && *s != 0x20 && *s != '\r' && pp < epp)*pp++ = *s++;
                while(s < end && *s == 0x20 && pp < epp) *pp++ = *s++;
                sp = s;
                while(s < end && *s != '\r' && *s != 0x20) ++s;
                http_req->auth.k = pp - http_req->hlines;
                pp += http_base64decode((unsigned char *)sp, (s - sp), (unsigned char *)pp);
                sp = http_req->hlines + http_req->auth.k;
                while(sp < pp && *sp != ':') sp++;
                if(*sp == ':')
                {
                    http_req->auth.nk = sp -  http_req->hlines - http_req->auth.k;
                    http_req->auth.v = sp + 1 - http_req->hlines;
                    http_req->auth.nv = pp -  http_req->hlines - http_req->auth.v;
                }
            }
            else
            {
                while(s < end && *s != '\r' && pp < epp)*pp++ = *s++;
            }
            if(pp >= epp) goto end;
            *pp++ = '\0';
            http_req->nhline = pp - http_req->hlines;
            ++s;
            while(s < end && *s != '\n')++s;
            ++s;
        }
end:
        ret++;
    }
    return ret;
}

/* HTTP response parser */
int http_response_parse(char *p, char *end, HTTP_RESPONSE *http_resp, void *map)
{
    char line[HTTP_HEAD_MAX], *x = NULL, *s = p, *es = NULL, *pp = NULL, *epp = NULL;
    int i  = 0, ret = -1, n = 0; //high = 0, low = 0;

    if(p && end)
    {
        pp = http_resp->hlines;
        epp = http_resp->hlines + HTTP_HEADER_MAX;
        while(s < end && *s != 0x20 && *s != 0x09 && pp < epp)*pp++ = *s++;
        while(s < end && (*s == 0x20 || *s == 0x09) && pp < epp)*pp++ = *s++;
        x = s; 
        while(s < end && *s >= '0' && *s <= '9')*pp++ = *s++;
        if((n = (s - x)) > 0 && (i = (mtrie_get(map, x, n) - 1)) >= 0)
        {
            http_resp->respid = i;
        }
        /*
        for(i = 0; i < HTTP_RESPONSE_NUM; i++ )
        {
            if(memcmp(response_status[i].e, s, response_status[i].elen) == 0)
            {
                http_resp->respid = i;
                                break;
            }
        }
        */
        while(s < end && *s != '\r' && *s != '\n' && *s != '\0' && pp < epp) *pp++ = *s++;
        while(s < end && (*s == '\r' || *s == '\n'))++s;
        *pp++ = '\0';
        while(s < end)
        {
            //parse response  code 
            /* ltrim */
            while(*s == 0x20)s++;
            es = line;
            while(s < end && *s != 0x20 && *s != ':')
            {
                if(*s >= 'A' && *s <= 'Z')
                {
                    *es++ = *s++ - ('A' - 'a');
                }
                else *es++ = *s++;
            }
            if(*s == ':') *es++ = *s++;
            *es = '\0';
            n = es - line;
            //fprintf(stdout, "%s::%d line:%s\n", __FILE__, __LINE__, line);
            if((i = (mtrie_get(map, line, n) - 1)) >= 0)
            {
                http_resp->headers[i] = pp - http_resp->hlines;
                ret++;
            }
            while(s < end && *s == 0x20)s++;
            /*
            for(i = 0; i < HTTP_HEADER_NUM; i++)
            {
                if( (end - s) >= http_headers[i].elen
                        && strncasecmp(s, http_headers[i].e, http_headers[i].elen) == 0)
                {
                    s +=  http_headers[i].elen;
                    while(s < end && *s == 0x20)s++;
                    http_resp->headers[i] = pp - http_resp->hlines;
                    ret++;
                    break;
                }
            }
            */
            if(i == HEAD_RESP_SET_COOKIE)
            {
                s += http_resp_cookie_parse(s, end, http_resp);
                pp = http_resp->hlines + http_resp->nhline;
            }
            while(s < end && *s != '\r' && pp < epp)*pp++ = *s++;
            if(pp >= epp) goto end;
            *pp++ = '\0';
            http_resp->nhline = pp - http_resp->hlines;
            ++s;
            while(s < end && *s != '\n')++s;
            ++s;
        }
end:
        ret++;
    }
    return ret;
}
/* return HTTP key/value */
int http_kv(HTTP_KV *kv, char *line, int nline, char **key, char **val)
{
    int ret = -1;

    if(kv && line && key && val)
    {
        if(kv->k > 0 && kv->k < nline && kv->nk > 0 && kv->nk < nline)
        {
            *key = &(line[kv->k]);
            line[kv->k + kv->nk] = '\0';
            ret++;
        }
        if(kv->v > 0 && kv->v < nline && kv->nv > 0 && kv->nv < nline)
        {
            *val = &(line[kv->v]);
            line[kv->v + kv->nv] = '\0';
            ret++;
        }
    }
    return ret;
}
/* HTTP charset convert */
int http_charset_convert(char *content_type, char *content_encoding, char *data, int len,   
                char *tocharset, int is_need_compress, char **out)
{
    int nout = 0; 
#ifdef _HTTP_CHARSET_CONVERT
    char charset[CHARSET_MAX], *rawdata = NULL, *txtdata = NULL, *todata = NULL, 
         *zdata = NULL, *p = NULL, *ps = NULL, *outbuf = NULL;
    size_t nrawdata = 0, ntxtdata = 0, ntodata = 0, nzdata = 0, 
           ninbuf = 0, noutbuf = 0, n = 0;
    chardet_t pdet = NULL;
    iconv_t cd = NULL;

    if(content_type && content_encoding && data && len > 0 && tocharset && out
        && strncasecmp(content_type, "text", 4) == 0)
    {
        *out = NULL;
        if(strncasecmp(content_encoding, "gzip", 4) == 0)
        {
            nrawdata =  len * 16 + Z_HEADER_SIZE;
            if((rawdata = (char *)calloc(1, nrawdata)))
            {
                if((httpgzdecompress((Bytef *)data, len, 
                    (Bytef *)rawdata, (uLong *)((void *)&nrawdata))) == 0)
                {
                    txtdata = rawdata;
                    ntxtdata = nrawdata;
                }
                else goto err_end;
            }
            else goto err_end;
        }
        else if(strncasecmp(content_encoding, "deflate", 7) == 0)
        {
            nrawdata =  len * 16 + Z_HEADER_SIZE;
            if((rawdata = (char *)calloc(1, nrawdata)))
            {
                if((zdecompress((Bytef *)data, len, (Bytef *)rawdata, 
                                (uLong*)((void *)&nrawdata))) == 0)
                {
                    txtdata = rawdata;
                    ntxtdata = nrawdata;
                    //fprintf(stdout, "%s::%d ndata:%d\r\n", __FILE__, __LINE__, nrawdata);
                }
                else goto err_end;
            }
            else goto err_end;
        }
        else 
        {
            txtdata = data;
            ntxtdata = len;
        }
        memset(charset, 0, CHARSET_MAX);
        //charset detactor
        if(txtdata && ntxtdata > 0 && chardet_create(&pdet) == 0)
        {
            if(chardet_handle_data(pdet, txtdata, ntxtdata) == 0
                    && chardet_data_end(pdet) == 0 )
            {
                chardet_get_charset(pdet, charset, CHARSET_MAX);
            }
            chardet_destroy(pdet);
        }
        //convert string charset 
        if(txtdata && ntxtdata > 0)
        {
            if(strcasecmp(charset, tocharset) != 0 
                    && (cd = iconv_open(tocharset, charset)) != (iconv_t)-1)
            {
                p = txtdata;
                ninbuf = ntxtdata;
                n = noutbuf = ninbuf * 8;
                if((ps = outbuf = (char *)calloc(1, noutbuf)))
                {
                    if(iconv(cd, &p, &ninbuf, &ps, (size_t *)&n) == (size_t)-1)
                    {
                        free(outbuf);
                        outbuf = NULL;
                    }
                    else
                    {
                        noutbuf -= n;
                        todata = outbuf;
                        ntodata = noutbuf;
                        //fprintf(stdout, "%s::%d ndata:%d\r\n", __FILE__, __LINE__, ntodata);
                    }
                }
                iconv_close(cd);
            }
            else
            {
                todata = txtdata;
                ntodata = ntxtdata;
                //fprintf(stdout, "nbuf:%d outbuf:\r\n", ntxtdata);
            }
        }else goto err_end;
        if(is_need_compress && todata && ntodata > 0)
        {
            nzdata = ntodata + Z_HEADER_SIZE;
            if((zdata = (char *)calloc(1, nzdata)))
            {
                if(zcompress((Bytef *)todata, ntodata, 
                    (Bytef *)zdata, (uLong *)((void *)&nzdata)) != 0)
                {
                    free(zdata);
                    zdata = NULL;
                    nzdata = 0;
                }
            }
        }
err_end:
        if(todata == data){todata = NULL;ntodata = 0;}
        if(zdata)
        {
            *out = zdata; nout = nzdata;
            if(outbuf){free(outbuf);outbuf = NULL;}
            if(rawdata){free(rawdata); rawdata = NULL;}
        }
        else if(todata)
        {
            if(rawdata && todata != rawdata)
            {
                free(rawdata); rawdata = NULL;
            }
            *out = todata; nout = ntodata;
        }
    }
#endif
    return nout;
}
/* HTTP charset convert data free*/
void http_charset_convert_free(char *data)
{
    if(data) free(data);
}

/* hextodec */
int http_hextodec(char *hex, int len)
{
    int k = 0, n = 0, x = 0;
    char s = 0;

    if(hex && len > 0)
    {
        k = len  - 1;
        x = 1;
        while(k >= 0)
        {
            s = hex[k]; 
            if(s >= 'A' && s <= 'F') n += (10 + (s - 'A')) * x;
            else if(s >= 'a' && s <= 'f') n += (10 + (s - 'a')) * x;
            else if(s >= '0' && s <= '9') n += (s - '0') * x;
            --s;
            x <<= 4;
            --k;
        }
    }
    return n;
}

/* HTTP chunkd */
int http_chunked_parse(HTTP_CHUNK *chunk, char *data, int ndata)
{
    char *p = NULL, *end = NULL, *pp = NULL;
    int ret = -1, n = 0, k = 0;

    if(chunk && (p = data) && (end = data + ndata) > p)
    {
        k = 0;
        while(p < end) 
        {
            pp = p;
            while(p < end && ((*p >= 'A' && *p <= 'Z') || (*p >= 'a' && *p <= 'z') 
                        || (*p >= '0' && *p <= '9')))++p;
            n = http_hextodec(pp, p - pp);
            while(p < end && *p != '\r' && *p != '\n')++p;
            p += 2;
            if(n == 0)
            {
                while(p < end && *p != '\r' && *p != '\n')++p;
                if(p < end && *p++ == '\r' && p < end && *p++ == '\n')
                    ret = p - data;
                //fprintf(stdout, "%s::%d ret:%d\r\n", __FILE__, __LINE__, ret);
                break;
            }
            chunk->chunks[k].off = p - data;
            chunk->chunks[k].len = n;
            p += n;
            p += 2;
            ++k;
        }
        chunk->nchunks = k;
    }
    return ret;
}

#ifdef _DEBUG_HTTP
int main(int argc, char **argv)
{
    HTTP_REQ http_req = {0};
    HTTP_RESPONSE http_resp = {0};
    char buf[HTTP_BUFFER_SIZE], block[HTTP_BUFFER_SIZE], *p = NULL, *end = NULL;
    int i = 0, n = 0;
    void *http_headers_map = NULL;
    if((http_headers_map = http_headers_map_init()) == NULL)
    {
        fprintf(stderr, "Initialize http_headers_map failed");
        _exit(-1);
    }
    /* test request parser */
    if((p = buf) && (n = sprintf(p, "%s %s HTTP/1.0\r\nHost: %s\r\n"
                    "Cookie: %s\r\nAuthorization: %s\r\nConnection: close\r\n\r\n", "GET",
                    "/search?hl=zh-CN&client=safari&rls=zh-cn"
                    "&newwindow=1&q=%E5%A5%BD&btnG=Google+%E6%90%9C%E7%B4%A2&meta=&aq=f&oq=",
                    "abc.com", "acddsd=dakhfksf; abc=dkjflasdff; "
                    "abcd=%E4%BD%A0%E5%A5%BD%E9%A9%AC; "
                    "你是水?=%E4%BD%A0%E5%A5%BD%E9%A9%AC", 
                    "Basic YWRtaW46YWtkamZsYWRzamZs"
                    "ZHNqZmxzZGpmbHNkamZsa3NkamZsZHNm")) > 0)
    {
        end = p + n;
        if(http_request_parse(p, end, &http_req, http_headers_map) != -1)
        {
            if((n = sprintf(buf, "%s", "client=safari&rls=zh-cn"
                            "&q=base64%E7%BC%96%E7%A0%81%E8%A7%84%E5%88%99"
                            "&ie=UTF-8&oe=UTF-8")) > 0)
            {
                end = buf + n;
                http_argv_parse(buf, end, &http_req);
            }
            fprintf(stdout, "---------------------STATE-----------------------\n"); 
            fprintf(stdout, "HTTP reqid:%d\npath:%s\nnargvs:%d\nncookie:%d\n", 
                    http_req.reqid, http_req.path, http_req.nargvs, http_req.ncookies);
            if(http_req.auth.nk > 0 || http_req.auth.nv > 0)
            {
                fprintf(stdout, "Authorization: %.*s => %.*s\n",
                        http_req.auth.nk,  http_req.hlines + http_req.auth.k, 
                        http_req.auth.nv, http_req.hlines + http_req.auth.v);
            }
            fprintf(stdout, "---------------------STATE END---------------------\n"); 
            fprintf(stdout, "---------------------HEADERS---------------------\n"); 
            for(i = 0; i < HTTP_HEADER_NUM; i++)
            {
                if((n = http_req.headers[i]) > 0)
                {
                    fprintf(stdout, "%s %s\n", http_headers[i].e, http_req.hlines + n);
                }
            }
            fprintf(stdout, "---------------------HEADERS END---------------------\n"); 
            fprintf(stdout, "---------------------COOKIES---------------------\n"); 
            for(i = 0; i < http_req.ncookies; i++)
            {
                fprintf(stdout, "%d: %.*s => %.*s\n", i, 
                        http_req.cookies[i].nk, http_req.hlines + http_req.cookies[i].k, 
                        http_req.cookies[i].nv, http_req.hlines + http_req.cookies[i].v);
            }
            fprintf(stdout, "---------------------COOKIES END---------------------\n"); 
            fprintf(stdout, "---------------------ARGVS-----------------------\n"); 
            for(i = 0; i < http_req.nargvs; i++)
            {
                fprintf(stdout, "%d: %s[%d]=>%s[%d]\n", i, 
                        http_req.line + http_req.argvs[i].k, http_req.argvs[i].nk,
                        http_req.line + http_req.argvs[i].v, http_req.argvs[i].nv);
            }
            fprintf(stdout, "---------------------ARGVS END---------------------\n"); 
        }
    }
    if((n = sprintf(buf, "HTTP/1.0 403 Forbidden\r\nContent-Type: text/html; charset=UTF-8\r\n"
                    "Date: Tue, 21 Apr 2009 01:32:56 GMT\r\nContent-Length: 0\r\n"
                    "Server: gws\r\nCache-Control: private, x-gzip-ok=\"\"\r\n"
                    "Connection: Close\r\n%s\r\n\r\n",
                    "Set-Cookie: 你是水?=%E4%BD%A0%E5%A5%BDa; "
                    "expires=Wed, 22-Apr-2009 03:12:37 GMT; path=/; domain=abc.com\r\n"
                    "Set-Cookie: 你是菜鸟=%E4%BD%A0%E5%A5%BDb; "
                    "expires=Wed, 22-Apr-2009 03:12:37 GMT; path=/; domain=abc.com\r\n"
                    "Set-Cookie: 你是?=%E4%BD%A0%E5%A5%BDc; "
                    "expires=Wed, 22-Apr-2009 03:12:37 GMT; path=/; domain=abc.com"
                   )) > 0)
    {
        end = buf + n;
        char headers[HTTP_BUF_SIZE];
        strcpy(headers, "HTTP/1.0 403 Forbidden\r\nConnection:keep-alive\r\nContent-Encoding:gzip\r\nContent-Type:text/html; charset=gbk\r\nDate:Sun, 23 Dec 2012 12:51:50 GMT\r\nServer:Tengine\r\nSet-Cookie:miM4_faa5_lastact=1356267109%09forum.php%09forumdisplay; expires=Mon, 24-Dec-2012 12:51:49 GMT; path=/; domain=.cjdby.net\r\nSet-Cookie:miM4_faa5_stats_qc_reg=deleted; expires=Thu, 01-Jan-1970 00:00:01 GMT; path=/; domain=.cjdby.net\r\nSet-Cookie:miM4_faa5_cloudstatpost=deleted; expires=Thu, 01-Jan-1970 00:00:01 GMT; path=/; domain=.cjdby.net\r\nSet-Cookie:miM4_faa5_forum_lastvisit=D_5_1353928356D_4_1356267109; expires=Sun, 30-Dec-2012 12:51:49 GMT; path=/; domain=.cjdby.net\r\nTransfer-Encoding:chunked\r\nVary:Accept-Encoding\r\nVary:Accept-Encoding\r\nX-Powered-By:PHP/5.3.19\r\n\r\n");
        end = headers + strlen(headers);
        if(http_response_parse(headers, end, &http_resp, http_headers_map) != -1)
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
            fprintf(stdout, "---------------------COOKIES---------------------\n"); 
            for(i = 0; i < http_resp.ncookies; i++)
            {
                fprintf(stdout, "%d: %.*s => %.*s;expire[%.*s];path[%.*s];domain[%.*s];\n", i, 
                        http_resp.cookies[i].kv.nk, http_resp.hlines + http_resp.cookies[i].kv.k, 
                        http_resp.cookies[i].kv.nv, http_resp.hlines + http_resp.cookies[i].kv.v,
                        http_resp.cookies[i].expire_len, http_resp.hlines + http_resp.cookies[i].expire_off,
                        http_resp.cookies[i].path_len, http_resp.hlines + http_resp.cookies[i].path_off,
                        http_resp.cookies[i].domain_len, http_resp.hlines + http_resp.cookies[i].domain_off
                        );
            }
            if((n = http_cookie_line(&http_resp, buf)) > 0)
                fprintf(stdout, "%s\n", buf);
            fprintf(stdout, "---------------------COOKIES END---------------------\n"); 

        }
    }
    return 0;
}
#endif
