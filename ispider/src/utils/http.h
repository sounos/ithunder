#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdarg.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#ifndef __HTTP_H__
#define __HTTP_H__
#define HTTP_IP_MAX 		    16
#define HTTP_VHOST_MAX     	    256
#define HTTP_INDEX_MAX     	    32
#define HTTP_INDEXES_MAX   	    1048576
#define HTTP_URL_PATH_MAX  	    1024
#define HTTP_PATH_MAX      	    1024
#define HTTP_BUF_SIZE      	    8192
#define HTTP_ARGV_LINE_MAX 	    32768
#define HTTP_HEAD_MAX 	        4096
#define HTTP_COOKIES_MAX   	    64
#define HTTP_ARGVS_MAX     	    256
#define HTTP_BUFFER_SIZE   	    32768
#define HTTP_HEADER_MAX    	    32768
#define HTTP_BYTE_K        	    1024
#define HTTP_BYTE_M        	    1048576
#define HTTP_BYTE_G        	    1073741824
#define HTTP_MMAP_MAX           157810688
#define HTTP_ENCODING_DEFLATE  	0x01
#define HTTP_ENCODING_GZIP 	    0x02
#define HTTP_ENCODING_BZIP2  	0x04
#define HTTP_ENCODING_COMPRESS  0x08
#define HTTP_ENCODING_NUM       4
#define HTTP_CHUNKED_MAX        256
typedef struct _HTTP_VHOST
{
    char *name;
    char *home;
    void *logger;
}HTTP_VHOST;
typedef struct _HTTPK
{
    int off;
    int len;
}HTTPK;
typedef struct _HTTP_ELEMENT
{
	int  id;
	int  elen;
	char *e;
	char *s;
}HTTP_ELEMENT;

/* HTTP/1.1 RESPONSE STATUS FROM RFC2616
   Status-Code    =
   "100"  ; Section 10.1.1: Continue
   | "101"  ; Section 10.1.2: Switching Protocols
   | "200"  ; Section 10.2.1: OK
   | "201"  ; Section 10.2.2: Created
   | "202"  ; Section 10.2.3: Accepted
   | "203"  ; Section 10.2.4: Non-Authoritative Information
   | "204"  ; Section 10.2.5: No Content
   | "205"  ; Section 10.2.6: Reset Content
   | "206"  ; Section 10.2.7: Partial Content
   | "300"  ; Section 10.3.1: Multiple Choices
   | "301"  ; Section 10.3.2: Moved Permanently
   | "302"  ; Section 10.3.3: Found
   | "303"  ; Section 10.3.4: See Other
   | "304"  ; Section 10.3.5: Not Modified
   | "305"  ; Section 10.3.6: Use Proxy
   | "307"  ; Section 10.3.8: Temporary Redirect
   | "400"  ; Section 10.4.1: Bad Request
   | "401"  ; Section 10.4.2: Unauthorized
   | "402"  ; Section 10.4.3: Payment Required
   | "403"  ; Section 10.4.4: Forbidden
   | "404"  ; Section 10.4.5: Not Found
   | "405"  ; Section 10.4.6: Method Not Allowed
   | "406"  ; Section 10.4.7: Not Acceptable

   | "407"  ; Section 10.4.8: Proxy Authentication Required
   | "408"  ; Section 10.4.9: Request Time-out
   | "409"  ; Section 10.4.10: Conflict
   | "410"  ; Section 10.4.11: Gone
   | "411"  ; Section 10.4.12: Length Required
   | "412"  ; Section 10.4.13: Precondition Failed
   | "413"  ; Section 10.4.14: Request Entity Too Large
   | "414"  ; Section 10.4.15: Request-URI Too Large
   | "415"  ; Section 10.4.16: Unsupported Media Type
   | "416"  ; Section 10.4.17: Requested range not satisfiable
   | "417"  ; Section 10.4.18: Expectation Failed
   | "500"  ; Section 10.5.1: Internal Server Error
   | "501"  ; Section 10.5.2: Not Implemented
   | "502"  ; Section 10.5.3: Bad Gateway
   | "503"  ; Section 10.5.4: Service Unavailable
   | "504"  ; Section 10.5.5: Gateway Time-out
   | "505"  ; Section 10.5.6: HTTP Version not supported
   | extension-code

   extension-code = 3DIGIT
   Reason-Phrase  = *<TEXT, excluding CR, LF>
 */
#define HTTP_RESPONSE_NUM 40
static const HTTP_ELEMENT response_status[] = 
{
#define RESP_CONTINUE		0
	{0, 3, "100", "Continue"},
#define RESP_SWITCH		1
	{1, 3, "101", "Switching Protocols"},
#define RESP_OK			2
	{2, 3, "200", "OK"},
#define RESP_CREATED		3
	{3, 3, "201", "Created"},
#define RESP_ACCEPTED		4
	{4, 3, "202", "Accepted"},
#define RESP_NONAUTHINFO	5
	{5, 3, "203", "Non-Authoritative Information"},
#define RESP_NOCONTENT		6
	{6, 3, "204", "No Content"},
#define RESP_RESETCONTENT	7
	{7, 3, "205", "Reset Content"},
#define RESP_PARTIALCONTENT	8
	{8, 3, "206", "Partial Content"},
#define RESP_MULTCHOICES	9
	{9, 3, "300", "Multiple Choices"},
#define RESP_MOVEDPERMANENTLY	10
	{10, 3, "301", "Moved Permanently"},
#define RESP_FOUND		11
	{11, 3, "302", "Found"},
#define RESP_SEEOTHER		12
	{12, 3, "303", "See Other"},
#define RESP_NOTMODIFIED	13
	{13, 3, "304", "Not Modified"},
#define RESP_USEPROXY		14
	{14, 3, "305", "Use Proxy"},
#define RESP_TEMPREDIRECORDT	15
	{15, 3, "307", "Temporary Redirect"},
#define RESP_BADREQUEST		16
	{16, 3, "400", "Bad Request"},
#define RESP_UNAUTHORIZED	17
	{17, 3, "401", "Unauthorized"},
#define RESP_PAYMENTREQUIRED	18
	{18, 3, "402", "Payment Required"},
#define RESP_FORBIDDEN		19
	{19, 3, "403", "Forbidden"},
#define RESP_NOTFOUND		20
	{20, 3, "404", "Not Found"},
#define RESP_METHODNOTALLOWED	21
	{21, 3, "405", "Method Not Allowed"},
#define RESP_NOTACCEPTABLE	22
	{22, 3, "406", "Not Acceptable"},
#define RESP_PROXYAUTHREQ	23
	{23, 3, "407", "Proxy Authentication Required"},
#define RESP_REQUESTTIMEOUT	24
	{24, 3, "408", "Request Time-out"},
#define RESP_CONFLICT		25
	{25, 3, "409", "Conflict"},
#define RESP_GONE		26
	{26, 3, "410", "Gone"},
#define RESP_LENGTHREQUIRED	27
	{27, 3, "411", "Length Required"},
#define RESP_PREFAILED		28
	{28, 3, "412", "Precondition Failed"},
#define RESP_REQENTTOOLARGE	29
	{29, 3, "413", "Request Entity Too Large"},
#define RESP_REQURLTOOLARGE	30
	{30, 3, "414", "Request-URI Too Large"},
#define RESP_UNSUPPORTEDMEDIA	31
	{31, 3, "415", "Unsupported Media Type"},
#define RESP_REQRANGENOTSAT	32
	{32, 3, "416", "Requested range not satisfiable"},
#define RESP_EXPECTFAILED	33
	{33, 3, "417", "Expectation Failed"},
#define RESP_INTERNALSERVERERROR	34
	{34, 3, "500", "Internal Server Error"},
#define RESP_NOTIMPLEMENT	35
	{35, 3, "501", "Not Implemented"},
#define RESP_BADGATEWAY		36
	{36, 3, "502", "Bad Gateway"},
#define RESP_SERVICEUNAVAILABLE	37
	{37, 3, "503", "Service Unavailable"},
#define RESP_GATEWAYTIMEOUT	38
	{38, 3, "504", "Gateway Time-out"},
#define RESP_HTTPVERNOTSUPPORT	39
	{39, 3, "505", "HTTP Version not supported"}
};

/* HTTP/1.1 REQUEST HEADERS FROM RFC2616 
   request-header = 
   | Accept                   ; Section 14.1
   | Accept-Charset           ; Section 14.2
   | Accept-Encoding          ; Section 14.3
   | Accept-Language          ; Section 14.4
   | Authorization            ; Section 14.8
   | Expect                   ; Section 14.20
   | From                     ; Section 14.22
   | Host                     ; Section 14.23
   | If-Match                 ; Section 14.24

   | If-Modified-Since        ; Section 14.25
   | If-None-Match            ; Section 14.26
   | If-Range                 ; Section 14.27
   | If-Unmodified-Since      ; Section 14.28
   | Max-Forwards             ; Section 14.31
   | Proxy-Authorization      ; Section 14.34
   | Range                    ; Section 14.35
   | Referer                  ; Section 14.36
   | TE                       ; Section 14.39
   | User-Agent               ; Section 14.43

  HTTP/1.1  RESPONSE HEADERS FROM RFC2616 
   response-header = 
   | Accept-Ranges           ; Section 14.5
   | Age                     ; Section 14.6
   | ETag                    ; Section 14.19
   | Location                ; Section 14.30
   | Proxy-Authenticate      ; Section 14.33

   | Retry-After             ; Section 14.37
   | Server                  ; Section 14.38
   | Vary                    ; Section 14.44
   | WWW-Authenticate        ; Section 14.47

  HTTP/1.1 GENERAL HEADERS FROM RFC2616
   general-header = 
   | Cache-Control            ; Section 14.9
   | Connection               ; Section 14.10
   | Date                     ; Section 14.18
   | Pragma                   ; Section 14.32
   | Trailer                  ; Section 14.40
   | Transfer-Encoding        ; Section 14.41
   | Upgrade                  ; Section 14.42
   | Via                      ; Section 14.45
   | Warning                  ; Section 14.46

   HTTP/1.1 ENTITY  HEADERS FROM RFC2616
   entity-header  = 
   | Allow                    ; Section 14.7
   | Content-Encoding         ; Section 14.11
   | Content-Language         ; Section 14.12
   | Content-Length           ; Section 14.13
   | Content-Location         ; Section 14.14
   | Content-MD5              ; Section 14.15
   | Content-Range            ; Section 14.16
   | Content-Type             ; Section 14.17
   | Expires                  ; Section 14.21
   | Last-Modified            ; Section 14.29
   | extension-header
    
   Additional headers
   | Cookies 
   | Set-Cookie 
 */
static const HTTP_ELEMENT http_headers[] = 
{
#define HEAD_REQ_ACCEPT 0
	{0, 7, "Accept:", NULL},
#define HEAD_REQ_ACCEPT_CHARSET 1
	{1, 15, "Accept-Charset:", NULL},
#define HEAD_REQ_ACCEPT_ENCODING 2
	{2, 16, "Accept-Encoding:", NULL},
#define HEAD_REQ_ACCEPT_LANGUAGE 3
	{3, 16, "Accept-Language:", NULL},
#define HEAD_RESP_ACCEPT_RANGE 4
	{4, 14, "Accept-Ranges:", NULL},
#define HEAD_RESP_AGE 5
	{5, 4, "Age:", NULL},
#define HEAD_ENT_ALLOW 6
	{6, 6, "Allow:", NULL},
#define HEAD_REQ_AUTHORIZATION 7
	{7, 14, "Authorization:", NULL},
#define HEAD_GEN_CACHE_CONTROL 8
	{8, 14, "Cache-Control:", NULL},
#define HEAD_GEN_CONNECTION 9
	{9, 11, "Connection:", NULL},
#define HEAD_ENT_CONTENT_ENCODING 10
	{10, 17, "Content-Encoding:", NULL},
#define HEAD_ENT_CONTENT_LANGUAGE 11
	{11, 17, "Content-Language:", NULL},
#define HEAD_ENT_CONTENT_LENGTH 12
	{12, 15, "Content-Length:", NULL},
#define HEAD_ENT_CONTENT_LOCATION 13
	{13, 17, "Content-Location:", NULL},
#define HEAD_ENT_CONTENT_MD5 14
	{14, 12, "Content-MD5:", NULL},
#define HEAD_ENT_CONTENT_RANGE 15
	{15, 14, "Content-Range:", NULL},
#define HEAD_ENT_CONTENT_TYPE 16
	{16, 13, "Content-Type:", NULL},
#define HEAD_GEN_DATE 17
	{17, 5, "Date:", NULL},
#define HEAD_RESP_ETAG 18
	{18, 5, "ETag:", NULL},
#define HEAD_REQ_EXPECT 19
	{19, 7, "Expect:", NULL},
#define HEAD_ENT_EXPIRES 20
	{20, 8, "Expires:", NULL},
#define HEAD_REQ_FROM 21
	{21, 5, "From:", NULL},
#define HEAD_REQ_HOST 22 
	{22, 5, "Host:", NULL},
#define HEAD_REQ_IF_MATCH 23 
	{23, 9, "If-Match:", NULL},
#define HEAD_REQ_IF_MODIFIED_SINCE 24 
	{24, 18, "If-Modified-Since:", NULL},
#define HEAD_REQ_IF_NONE_MATCH 25
	{25, 14, "If-None-Match:", NULL},
#define HEAD_REQ_IF_RANGE 26 
	{26, 9, "If-Range:", NULL},
#define HEAD_REQ_IF_UNMODIFIED_SINCE 27 
	{27, 20, "If-Unmodified-Since:", NULL},
#define HEAD_ENT_LAST_MODIFIED 28
	{28, 14, "Last-Modified:", NULL},
#define HEAD_RESP_LOCATION 29 
	{29, 9, "Location:", NULL},
#define HEAD_REQ_MAX_FORWARDS 30 
	{30, 13, "Max-Forwards:", NULL},
#define HEAD_GEN_PRAGMA 31 
	{31, 7, "Pragma:", NULL},
#define HEAD_RESP_PROXY_AUTHENTICATE 32 
	{32, 19, "Proxy-Authenticate:", NULL},
#define HEAD_REQ_PROXY_AUTHORIZATION 33
	{33, 20, "Proxy-Authorization:", NULL},
#define HEAD_REQ_RANGE 34 
	{34, 6, "Range:", NULL},
#define HEAD_REQ_REFERER 35
	{35, 8, "Referer:", NULL},
#define HEAD_RESP_RETRY_AFTER 36
	{36, 12, "Retry-After:", NULL},
#define HEAD_RESP_SERVER 37
	{37, 7, "Server:", NULL},
#define HEAD_REQ_TE 38
	{38, 3, "TE:", NULL},
#define HEAD_GEN_TRAILER 39
	{39, 8, "Trailer:", NULL},
#define HEAD_GEN_TRANSFER_ENCODING 40 
	{40, 18, "Transfer-Encoding:", NULL},
#define HEAD_GEN_UPGRADE 41 
	{41, 8, "Upgrade:", NULL},
#define HEAD_REQ_USER_AGENT 42
	{42, 11, "User-Agent:", NULL},
#define HEAD_RESP_VARY 43 
	{43, 5, "Vary:", NULL},
#define HEAD_GEN_VIA 44 
	{44, 4, "Via:", NULL},
#define HEAD_GEN_WARNING 45
	{45, 8, "Warning:", NULL},
#define HEAD_RESP_WWW_AUTHENTICATE 46 
	{46, 17, "WWW-Authenticate:", NULL},
#define HEAD_REQ_COOKIE 47
    {47, 7, "Cookie:", NULL},
#define HEAD_RESP_SET_COOKIE 48
    {48, 11, "Set-Cookie:", NULL},
#define HEAD_GEN_USERID     49
    {49, 7, "UserID:", NULL},
#define HEAD_GEN_UUID     50
    {50, 5, "UUID:", NULL},
#define HEAD_GEN_RAW_LENGTH   51
    {51, 11, "Raw-Length:", NULL},
#define HEAD_GEN_TASK_TYPE   52
    {52, 10, "Task-Type:", NULL},
#define HEAD_GEN_DOWNLOAD_LENGTH   53
    {53, 16, "Download-Length:", NULL}
};
#define HTTP_HEADER_NUM	54

/* HTTP/1.1 METHODS
   Method         = 
   | "OPTIONS"                ; Section 9.2
   | "GET"                    ; Section 9.3
   | "HEAD"                   ; Section 9.4
   | "POST"                   ; Section 9.5
   | "PUT"                    ; Section 9.6
   | "DELETE"                 ; Section 9.7
   | "TRACE"                  ; Section 9.8
   | "CONNECT"                ; Section 9.9
   | extension-method
   extension-method = token
 */
#define HTTP_METHOD_NUM 11
static const HTTP_ELEMENT http_methods[] = 
{
#define HTTP_OPTIONS	0
	{0, 7, "OPTIONS", NULL},
#define HTTP_GET	1
	{1, 3, "GET", NULL},
#define HTTP_HEAD	2
	{2, 4, "HEAD", NULL},
#define HTTP_POST	3
	{3, 4, "POST", NULL},
#define HTTP_PUT	4
	{4, 3, "PUT", NULL},
#define HTTP_DELETE	5
	{5, 6, "DELETE", NULL},
#define HTTP_TRACE	6
	{6, 5, "TRACE", NULL},
#define HTTP_CONNECT	7
	{7, 7, "CONNECT", NULL},
#define HTTP_TASK   8
    {8, 4, "TASK", NULL},
#define HTTP_DOWNLOAD 9
    {9, 8, "DOWNLOAD", NULL},
#define HTTP_EXTRACT 10
    {9, 7, "EXTRACT", NULL}
};

/* file ext support list */
#define HTTP_MIME_NUM 99
static const HTTP_ELEMENT http_mime_types[]=
{
    {0, 4, "html", "text/html"},
    {1, 3, "htm", "text/html"},
    {2, 5, "shtml", "text/html"},
    {3, 3, "css", "text/css"},
    {4, 3, "xml", "text/xml"},
    {5, 3, "gif", "image/gif"},
    {6, 4, "jpeg", "image/jpeg"},
    {7, 3, "jpg", "image/jpeg"},
    {8, 2, "js", "application/x-javascript"},
    {9, 4, "atom", "application/atom+xml"},
    {10, 3, "rss", "application/rss+xml"},
    {11, 3, "mml", "text/mathml"},
    {12, 3, "txt", "text/plain"},
    {13, 3, "jad", "text/vnd.sun.j2me.app-descriptor"},
    {14, 3, "wml", "text/vnd.wap.wml"},
    {15, 3, "htc", "text/x-component"},
    {16, 3, "png", "image/png"},
    {17, 3, "tif", "image/tiff"},
    {18, 4, "tiff", "image/tiff"},
    {19, 4, "wbmp", "image/vnd.wap.wbmp"},
    {20, 3, "ico", "image/x-icon"},
    {21, 3, "jng", "image/x-jng"},
    {22, 3, "bmp", "image/x-ms-bmp"},
    {23, 3, "svg", "image/svg+xml"},
    {24, 3, "jar", "application/java-archive"},
    {25, 3, "war", "application/java-archive"},
    {26, 3, "ear", "application/java-archive"},
    {27, 3, "hqx", "application/mac-binhex40"},
    {28, 3, "doc", "application/msterm"},
    {29, 3, "pdf", "application/pdf"},
    {30, 2, "ps", "application/postscript"},
    {31, 3, "eps", "application/postscript"},
    {32, 2, "ai", "application/postscript"},
    {33, 3, "rtf", "application/rtf"},
    {34, 3, "xls", "application/vnd.ms-excel"},
    {35, 3, "ppt", "application/vnd.ms-powerpoint"},
    {36, 4, "wmlc", "application/vnd.wap.wmlc"},
    {37, 5, "xhtml", "application/vnd.wap.xhtml+xml"},
    {38, 3, "kml", "application/vnd.google-earth.kml+xml"},
    {39, 3, "kmz", "application/vnd.google-earth.kmz"},
    {40, 3, "cco", "application/x-cocoa"},
    {41, 7, "jardiff", "application/x-java-archive-diff"},
    {42, 4, "jnlp", "application/x-java-jnlp-file"},
    {43, 3, "run", "application/x-makeself"},
    {44, 2, "pl", "application/x-perl"},
    {45, 2, "pm", "application/x-perl"},
    {46, 3, "prc", "application/x-pilot"},
    {47, 3, "pdb", "application/x-pilot"},
    {48, 3, "rar", "application/x-rar-compressed"},
    {49, 3, "rpm", "application/x-redhat-package-manager"},
    {50, 3, "sea", "application/x-sea"},
    {51, 3, "swf", "application/x-shockwave-flash"},
    {52, 3, "sit", "application/x-stuffit"},
    {53, 3, "tcl", "application/x-tcl"},
    {54, 2, "tk", "application/x-tcl"},
    {55, 3, "der", "application/x-x509-ca-cert"},
    {56, 3, "pem", "application/x-x509-ca-cert"},
    {57, 3, "crt", "application/x-x509-ca-cert"},
    {58, 3, "xpi", "application/x-xpinstall"},
    {59, 3, "zip", "application/zip"},
    {60, 3, "bin", "application/octet-stream"},
    {61, 3, "exe", "application/octet-stream"},
    {62, 3, "dll", "application/octet-stream"},
    {63, 3, "deb", "application/octet-stream"},
    {64, 3, "dmg", "application/octet-stream"},
    {65, 3, "eot", "application/octet-stream"},
    {66, 3, "img", "application/octet-stream"},
    {67, 3, "iso", "application/octet-stream"},
    {68, 3, "msi", "application/octet-stream"},
    {69, 3, "msp", "application/octet-stream"},
    {70, 3, "msm", "application/octet-stream"},
    {71, 3, "mid", "audio/midi"},
    {72, 4, "midi", "audio/midi"},
    {73, 3, "kar", "audio/midi"},
    {74, 3, "mp3", "audio/mpeg"},
    {75, 2, "ra", "audio/x-realaudio"},
    {76, 4, "3gpp", "video/3gpp"},
    {77, 3, "3gp", "video/3gpp"},
    {78, 4, "mpeg", "video/mpeg"},
    {79, 3, "mpg", "video/mpeg"},
    {80, 3, "mov", "video/quicktime"},
    {81, 3, "flv", "video/x-flv"},
    {82, 3, "mng", "video/x-mng"},
    {83, 3, "asx", "video/x-ms-asf"},
    {84, 3, "asf", "video/x-ms-asf"},
    {85, 3, "wmv", "video/x-ms-wmv"},
    {86, 3, "avi", "video/x-msvideo"},
    {87, 3, "dmg", "application/octet-stream"},
    {88, 3, "php", "text/plain"},
    {89, 2, "am", "text/plain"},
    {90, 2, "in", "text/plain"},
    {91, 3, "cpp", "text/plain"},
    {92, 1, "c", "text/plain"},
    {93, 1, "h", "text/plain"},
    {94, 2, "m4", "text/plain"},
    {95, 2, "sh", "text/plain"},
    {96, 5, "guess", "text/plain"},
    {97, 3, "sub", "text/plain"},
    {98, 3, "awk", "text/plain"}
};
/*
static const char *ftypes[] = {
	"UNKOWN",
	"FIFO",
	"CHR",
	"UNKOWN",
	"DIR",
	"UNKOWN",
	"BLK",
	"UNKOWN",
	"FILE",
	"UNKOWN",
	"LNK",
	"UNKOWN",
	"SOCK",
	"UNKOWN",
	"WHT"
};
*/
typedef struct _HTTP_KV
{
    unsigned short nk;
    unsigned short nv;
    unsigned short k;
    unsigned short v;
}HTTP_KV;
typedef struct _HTTP_COOKIE
{
    HTTP_KV kv;
    unsigned short expire_off;
    unsigned short expire_len;
    unsigned short path_off;
    unsigned short path_len;
    unsigned short domain_off;
    unsigned short domain_len;
}HTTP_COOKIE;
typedef struct _HTTP_CHUNK
{
    HTTPK chunks[HTTP_CHUNKED_MAX];
    int nchunks;
    int bits;
}HTTP_CHUNK;
typedef struct _HTTP_RESPONSE
{
    int respid;
    int ncookies;
    int header_size;
    int nhline;
    int headers[HTTP_HEADER_NUM];
    char hlines[HTTP_HEADER_MAX];
    HTTP_COOKIE cookies[HTTP_COOKIES_MAX];
}HTTP_RESPONSE;
typedef struct _HTTP_REQ
{
    int reqid;
    int nargvs;
    int nline;
    int ncookies;
    int argv_off;
    int argv_len;
    int header_size;
    int nhline;
    int  headers[HTTP_HEADER_NUM];
    HTTP_KV auth;
    HTTP_KV argvs[HTTP_ARGVS_MAX];
    HTTP_KV cookies[HTTP_COOKIES_MAX];
    char path[HTTP_URL_PATH_MAX];
    char hlines[HTTP_HEADER_MAX];
    char line[HTTP_ARGV_LINE_MAX];
}HTTP_REQ;
/* initialize headers map */
void *http_headers_map_init();
/* clean headers map */
void http_headers_map_clean(void *map);
/* HTTP request HEADER parser */
int http_request_parse(char *p, char *end, HTTP_REQ *http_req, void *map);
/* HTTP argvs  parser */
int http_argv_parse(char *p, char *end, HTTP_REQ *http_req);
/* parse cookie */
int http_cookie_parse(char *p, char *end, HTTP_REQ *http_req);
/* parse cookie */
int http_resp_cookie_parse(char *p, char *end, HTTP_RESPONSE *resp);
/* cookie line */
int http_cookie_line(HTTP_RESPONSE *http_resp, char *cookie);
/* HTTP response HEADER parser */
int http_response_parse(char *p, char *end, HTTP_RESPONSE *resp, void *map);
/* return HTTP key/value */
int http_kv(HTTP_KV *kv, char *line, int nline, char **name, char **key);
/* HTTP charset convert */
int http_charset_convert(char *content_type, char *content_encoding, char *data, int len, 
        char *tocharset, int is_need_compress, char **out);
/* HTTP charset convert data free*/
void http_charset_convert_free(char *data);
int http_base64encode(char *src, int src_len, char *dst);
int http_base64decode(unsigned char *src, int src_len, unsigned char *dst);
unsigned long http_crc32(unsigned char *in, unsigned int inlen);
int http_chunked_parse(HTTP_CHUNK *chunk, char *data, int ndata);
int http_hextodec(char *hex, int len);
#endif
