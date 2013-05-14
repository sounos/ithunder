#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "html.h"
#include "charcode.h"
#include "htmltags.h"
#include "mtrie.h"
#define ISALPHA(p) ((*p >= '0' && *p <= '9') ||(*p >= 'A' && *p <= 'Z')||(*p >= 'a' && *p <= 'z'))
#define RTRIM(p, end)                                                                           \
{                                                                                               \
        while( p < end && ( *p == ' ' || *p == '\r' || *p == '\n' || *p == '\t')) p++;          \
}
#ifdef _SHOW_DEL
#define PRINT_DEL(id, blck) fprintf(stdout, "%d::<%s> %d/%d pairs:%d {%s}\n\n", \
        id, htmltags[id].tag, blck->nlinks, blck->nbytes, blck->npairs, blck->start);            
#else
#define PRINT_DEL(id, blck)
#endif
                    //|| blck->npairs > HTML_PAIRS_MAX                                            
#define DELETE_BLOCK(id, blck, e, el)                                                           \
do                                                                                              \
{                                                                                               \
    if(htmltags[id].isblock && blck->start)                                                     \
    {                                                                                           \
        if((blck->nbytes = (e - blck->start)) > 0 && (blck->nbytes < HTML_TEXT_MIN              \
                    || ((double)(blck->nlinks) /(double)(blck->nbytes)) > HTML_LINK_SCALE))     \
        {                                                                                       \
            PRINT_DEL(id, blck);                                                                \
            while(e > blck->start) *e-- = '\0';                                                 \
            el = blck->last;                                                                    \
        }                                                                                       \
    }                                                                                           \
}while(0)
#define ENDING_BLOCK(_next_, _pp_, _start_)         \
do                                                  \
{                                                   \
    if(_pp_ > _start_ && *(_pp_ - 1) != '\n')       \
    {                                               \
        *_pp_++ = '\n';                             \
        _next_ = _pp_;                              \
    }                                               \
}while(0)

int html_add_field(HTML *html, char *text, int ntext)
{
    char *p = NULL, *ep = NULL, *s = NULL, *es = NULL;

    if(html && text && ntext > 0)
    {
        if(html->nfields < HTML_FIELD_MAX)
        {
            p = text;
            ep = text + ntext;
            s = html->content + html->ncontent;
            es = html->content + HTML_CONTENT_MAX;
            if(html->nfields == 0) html->nfields = 1;
            html->fieldlist[html->nfields].from = s - html->content;
            while(p < ep && s < es) *s++ = *p++;
            html->ncontent = s - html->content;
            html->fieldlist[html->nfields].len = 
                html->ncontent - html->fieldlist[html->nfields].from ;
            html->nfields++;
            return 0;
        }
    }
    return -1;
}

/* Parse HTML CODE */
int html_get_content(HTML *html, char *content, size_t len, int filter, int new_field)
{
	char *p = content, *start = content, *end = content + len, *from = NULL, 
         *pp = NULL, *head = NULL, *ps = NULL, *sp = NULL, 
         *last = NULL, *last_link = NULL;	
    int n = 0, id = 0, isupper = 0, isend = 0,
        isdelete = -1, ndelete = 0, x = 0, m = 0;
    HBLOCK *last_block = NULL;
	
	if(html && content && len > 0)	
	{
        if(html->content == NULL || html->block == NULL || len > html->nblock)
        {
            n = html->nblock;
            if(len > html->nblock) n = (len + 1); 
            html->content = (char *)realloc(html->content, n);
            html->block = (char *)realloc(html->block, n);
        }
        head = ps = html->block;
        while(p < end)
        {
            if(*p == '\t' || *p == '\r' || *p == '\n' || *p == '\t')
            {
                if(ps > head && *(ps-1) != 0x20) *ps++ = 0x20;
                p++;
            }
            else if(memcmp(p, "　", 3) == 0)
            {
                if(ps > head && *(ps-1) != 0x20) *ps++ = 0x20;
                p += 3;
            }
            else if(*p == '&')
            {
                m = end - p;
                x = mtrie_maxfind(html->table, p, m, &n);
                if(x == 1 && ps > head && *(ps-1) == 0x20)
                {
                    p += n;
                    continue;
                }
                else if(x > 0 && x < CHARCODE_NUM)

                {
                    ps += sprintf(ps, "%s", charcodelist[x].chr);
                    p += n;
                    continue;
                }
                else
                {
                    *ps++ = *p++;
                }
            }
            else if(p < (end - 1) && *p == 0x20 && *(p+1) == 0x20)
            {
                p++;
            }
            else
            {
                *ps++ = *p++;
            }
        }
        //fprintf(stdout, "%s\n", html->block);
        end = ps;
        start = p = html->block;
        /*
        */
        head = html->content;
        from = last = pp = html->content + html->ncontent;
		while(p < end)
        {
            id = -1;
            if(*p == '<')
            {
                if(p < (end - 3) && *(p+1) == '!' && *(p+2) == '-' && *(p+3) == '-')
                {
                    p += 4;
                    while(p < end)
                    {
                        if(p < (end - 2) && *p == '-' && *(p+1) == '-' && *(p+2) == '>')
                        {
                            p += 3;
                            break;
                        }
                        else p++;
                    }
                }
                else
                {
                    ps = NULL;
                    sp = p++;
                    isend = 0;
                    while(p < end && (*p == '\r' || *p == '\t' || *p == '\n' || *p == 0x20))p++; 
                    if(*p == '/'){isend = 1;p++;}
                    while(p < end && (*p == '\r' || *p == '\t' || *p == '\n' || *p == 0x20))p++; 
                    if(*p == '!' || (*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z'))
                    {
                        ps = pp;
                        while(p < end && (*p == '!' || (*p >= 'a' && *p <= 'z') 
                                    || (*p >= '0' && *p <= '9') || 
                                    (isupper = (*p >= 'A' && *p <= 'Z'))))
                        {
                            if(isupper) *pp++ = *p++ + ('a' - 'A');
                            else *pp++ = *p++;
                            isupper = 0;
                        }
                        if((n = (pp - ps)) > 0)
                        {
                            //fprintf(stdout, "%d::%d::%.*s\n", sp - html->block, ndelete, 12, sp);
                            memset(ps, 0, n);
                            id = mtrie_get(html->table, ps, n) - 1;
                            if(id >= 0)
                            {
                                while(p < end && *p != '>')++p;
                                if(ndelete == 0)
                                {
                                    if(id == HTMLTAG_TITLE_ID)
                                    {
                                        if(isend) 
                                        {
                                            //fprintf(stdout, "%d::%s\n", __LINE__, from);
                                            html->ntitle = html->fieldlist[0].len = (ps - head) 
                                                - html->fieldlist[0].from;
                                            ENDING_BLOCK(last, ps, head);
                                            if(html->nfields == 0) html->nfields = 1;
                                            last = from = ps;
                                        }
                                        else 
                                        {
                                            html->titleoff = html->fieldlist[0].from = ps - head;
                                        }
                                    }
                                    if(id == HTMLTAG_A_ID)
                                    {

                                    }
                                    if(id == HTMLTAG_IMG_ID)
                                    {

                                    }
                                    //BR
                                    if(id == HTMLTAG_BR_ID && (ps - last) > HTML_BLOCK_MIN)
                                    {
                                        ENDING_BLOCK(last, ps, head);
                                    }
                                    //blocks
                                    if(htmltags[id].ispair)
                                    {
                                        if(filter)
                                        {
                                            if(isend) 
                                            {
                                                //fprintf(stdout, "</%s>\n", htmltags[id].tag);
                                                if(last_block)
                                                {
                                                    if(id == HTMLTAG_A_ID)
                                                    {
                                                        if(last_link) 
                                                            last_block->nlinks += (ps - last_link);
                                                        last_link = NULL;
                                                    }
                                                    last_block->npairs++;
                                                    if(htmltags[id].isblock)
                                                    {
                                                        DELETE_BLOCK(id, last_block, ps, last);
                                                        last_block = last_block->parent;
                                                        html->nblocks--;
                                                    }
                                                }
                                            }
                                            else 
                                            {
                                                //fprintf(stdout, "<%s>\n", htmltags[id].tag);
                                                if(htmltags[id].isblock 
                                                        && (x = html->nblocks) < HTML_TAG_NMAX)
                                                {
                                                    memset(&(html->blocklist[x]), 0, sizeof(HBLOCK));
                                                    html->blocklist[x].last = last;
                                                    html->blocklist[x].id = id;
                                                    html->blocklist[x].parent = last_block;
                                                    html->blocklist[x].start = ps;
                                                    last_block = &(html->blocklist[x]);
                                                    html->nblocks++;
                                                }
                                                if(id == HTMLTAG_A_ID && last_link == NULL)
                                                    last_link = ps;
                                            }
                                        }
                                    }
                                    if((ps - last) > HTML_BLOCK_MIN)
                                    {
                                        ENDING_BLOCK(last, ps, head);
                                    }
                                }
                                if(htmltags[id].ispair && htmltags[id].isdelete)
                                {
                                    if(isend) 
                                    {
                                        if(isdelete == id)
                                        {
                                            ndelete--;
                                            isdelete = -1;
                                        }
                                    }
                                    else if(ndelete == 0) 
                                    {
                                        ndelete++;
                                        isdelete = id;
                                    }
                                }
                            }
                            ++p;
                            pp = ps;
                            continue;
                        }
                    }
//next:
                    if(ndelete == 0 && (n = (p - sp)) > 0) 
                    {
                        if(ps) pp = ps;
                        memcpy(pp, sp, n);
                        pp += n;
                    }
                    continue;
                }
            }
            else if(ndelete > 0 || *p == '\r' || *p == '\t' || *p == '\n' 
                    || (*p == 0x20 && pp > head && *(pp-1) == 0x20)) 
            {
                ++p;
            }
            else if(*p == '&')
            {
                m = end - p;
                x = mtrie_maxfind(html->table, p, m, &n);
                if(x == 1 && pp > head && *(pp-1) == 0x20)
                {
                    p += n;
                    continue;
                }
                else if(x > 0 && x < CHARCODE_NUM)

                {
                    pp += sprintf(pp, "%s", charcodelist[x].chr);
                    p += n;
                    continue;
                }
                else
                {
                    *pp++ = *p++;
                }
            }
            else if(*p == '?' || *p == '!' || *p == '.')
            {
                //split phrase
                *pp++ = *p++;
                while(p < end && (*p == '?' || *p == '!' || *p == '.'))
                {
                    *pp++ = *p++;
                }
                if(p < end && p > (start+1) && (ISALPHA(p) || ISALPHA(p-2))) 
                {
                    continue;
                }
                if((pp - last) > HTML_BLOCK_MIN)
                {
                    ENDING_BLOCK(last, pp, head);
                }
            }
            else if (p < (end - 2) && (memcmp(p, "。", 3) == 0 
                        || memcmp(p, "；", 3) == 0 
                        || memcmp(p, "！", 3) == 0 
                        || memcmp(p, "，", 3) == 0 
                        || memcmp(p, "？", 3) == 0))
            {
                //split phrase
                memcpy(pp, p, 3); pp += 3; p += 3;
                while(p < (end - 2) && (memcmp(p, "。", 3) == 0 
                            || memcmp(p, "；", 3) == 0 
                            || memcmp(p, "！", 3) == 0 
                            || memcmp(p, "，", 3) == 0 
                            || memcmp(p, "？", 3) == 0))
                {
                    memcpy(pp, p, 3); pp += 3; p += 3;
                }
                if((pp - last) > HTML_BLOCK_MIN)
                {
                    ENDING_BLOCK(last, pp, head);
                }
            }
            else if(p < (end - 2) && memcmp(p, "　", 3) == 0)
            {
                if(*pp != 0x20) *pp++ = 0x20;
                p += 3;
            }
            else if(((unsigned char )(*p)) > 0x80)
            {
                n = 0;
                if(((unsigned char )(*p)) >= 252) n = 6;
                else if(((unsigned char )(*p)) >= 248) n = 5;
                else if(((unsigned char )(*p)) >= 240) n = 4;
                else if(((unsigned char )(*p)) >= 224) n = 3;
                else if(((unsigned char )(*p)) >= 192) n = 2;
                else n = 1;
                if(n > 0) memcpy(pp, p, n);
                pp += n;
                p += n;
            }
            else 
            {
                *pp++ = *p++;
            }
        }
        html->ncontent = pp - html->content;
        if(new_field && html->nfields < HTML_FIELD_MAX && (n = (pp - from)) > 0)
        {
            if(html->nfields == 0) html->nfields = 1;
            html->fieldlist[html->nfields].from = from - html->content;
            html->fieldlist[html->nfields].len = n;
            html->nfields++;
        }
        return 0;
	}
    return -1;
}

/* Reset HTML */
void html_reset(HTML *html)
{
	if(html)
	{
		html->nblocks = 0;
        if(html->content) memset(html->content, 0, html->ncontent);
		html->ncontent = 0;
        if(html->block) memset(html->block, 0, html->nblock);
		html->nblock = 0;
        memset(html->fieldlist, 0,  sizeof(HFIELD) * HTML_FIELD_MAX);
        html->nfields = 0;
		html->titleoff = 0;
		html->ntitle = 0;
	}
}

/* Clean HTML */
void html_clean(HTML **html)
{
	if(html && *html)
	{
		if((*html)->block) free((*html)->block);
		if((*html)->content) free((*html)->content);
        if((*html)->table) mtrie_clean((*html)->table);
		free(*html);
		*html = NULL;
	}
}

/* Initialize HTML */
HTML *html_init()
{
    long i = 0;
	HTML *html = NULL;
	if((html = (HTML *)calloc(1, sizeof(HTML))))
	{
		html->get_content = html_get_content;
		html->add_field = html_add_field;
		html->reset = html_reset;
		html->clean = html_clean;
        html->table = mtrie_init();
        html->nblock = HTML_CONTENT_MAX;
        html->content = (char *)calloc(1, html->nblock);
        html->block = (char *)calloc(1, html->nblock);
        for(i = 0; i < HTMLTAGS_NUM; i++)
        {
            if(htmltags[i].tag)
            {
                mtrie_add(html->table, htmltags[i].tag, strlen(htmltags[i].tag), i+1);
            }
        }
        for(i = 1; i < CHARCODE_NUM; i++)
        {
            if(charcodelist[i].dec)
            {
                mtrie_add(html->table, charcodelist[i].dec, strlen(charcodelist[i].dec), i);
            }
            if(charcodelist[i].code)
            {
                mtrie_add(html->table, charcodelist[i].code, strlen(charcodelist[i].code), i);
            } 
        }
	}
	return html;
}

#ifdef _DEBUG_HTML
#include <sys/stat.h>
#include <fcntl.h>
#include "timer.h"
#define BUF_SIZE 4 * 1024 * 1024
int main(int argc, char **argv)
{
    int i = 0, n = 0, len = 0;
    char *s = NULL, *url = NULL;
    struct stat st = {0};
    void *timer = NULL;
    HTML *html = NULL;
    int fd = 0;

    if(argc < 2)
    {
        fprintf(stderr, "Usage:%s file ...\n", argv[0]);
        _exit(-1);
    }
    if((html = html_init()))
    {
        TIMER_INIT(timer);
        for(i = 1; i < argc; i++)
        {
            fprintf(stdout, "parse %s ...\n", argv[i]);
            if((fd = open(argv[i], O_RDONLY)) > 0)
            {
                if(fstat(fd, &st) == 0 && st.st_size > 0)
                {
                    if((s = calloc(1, st.st_size)))
                    {
                        read(fd, s, st.st_size);
                        html->reset(html);
                        TIMER_SAMPLE(timer);
                        html_get_content(html, s, st.st_size, 1, 1);
                        url = "http://www.china.com/abc.html";
                        html_add_field(html, url, strlen(url));
                        TIMER_SAMPLE(timer);
                        fprintf(stdout, "html->get_content(%s) time used:%lld\n", 
                                argv[i], PT_LU_USEC(timer));
                        free(s);
                        s = NULL;
                    }
                    fprintf(stdout, "len:%d strlen:%d ntitle:%d %s\n", html->ncontent, 
                            (int)strlen(html->content), html->ntitle, html->content);
                }
                close(fd);
            }
        }
        for(i = 0; i < html->nfields; i++)
        {
            fprintf(stdout, "%.*s\r\n", html->fieldlist[i].len, 
                    (html->content + html->fieldlist[i].from));
        }
        TIMER_CLEAN(timer);
        html->clean(&html);
    }
    return 0;
}
//gcc -o html *.c -D_DEBUG_HTML
#endif
