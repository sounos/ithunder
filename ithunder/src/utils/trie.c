#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include "trie.h"
void hnode_view(HNODE *hnode)
{
    int i = 0;

    if(hnode)
    {
        if(hnode->dptr)fprintf(stdout, "%ld\n", (long)hnode->dptr);
        if(hnode->list)
        {
            for(i = 0; i < hnode->count; i++)
            {
                hnode_view(&(hnode->list[i])); 
            }
        }
    }
}
void hnode_clean(HNODE *hnode)
{
    int i = 0;
    if(hnode)
    {
        if(hnode->list)
        {
            for(i = 0; i < hnode->count; i++)
            {
                hnode_clean(&(hnode->list[i]));
            }
            free(hnode->list);
            hnode->list = NULL;
        }
    }
}
void trietab_view(void *ptr)
{
    TRIETAB *trietab = (TRIETAB *)ptr;
    int i = 0;

    if(trietab)
    {
        for(i = 0; i < BYTE_SIZE; i++)
        {
            hnode_view(&(trietab->table[i]));
        }
    }
}

void trietab_clean(void *ptr)
{
    TRIETAB *trietab = (TRIETAB *)ptr;
    int i = 0;

    if(trietab)
    {
        for(i = 0; i < BYTE_SIZE; i++)
        {
            hnode_clean(&(trietab->table[i]));
        }
    }

}
#ifdef _DEBUG_TRIE
#include <fcntl.h>
#include <sys/stat.h>
#include "timer.h"
#define LINE_MAX 1024
typedef  struct _RECORD
{
    char *key;
    char *data;
}RECORD;
#define NRECORD 24
static RECORD recdlist[] = {
    {"abc", "kdajfkldjflkdsjflkd"},
    {"abca", "kdajfkldjflkdsjflkd"},
    {"abcd", "ldjfaldsjfkdslfjdslkfjlds"},
    {"abcde", "kjLJDAFJDALFJA;DLFJL;AJL;"},
    {"abcdeb", "kjLJDAFJDALFJA;DLFJL;AJL;"},
    {"abcdefd", "dklajfiuiosdufaodsufouo"},
    {"abcdedc", "dklajfiuiosdufaodsufouo"},
    {"abcdee", "dklajfiuiosdufaodsufouo"},
    {"abcdefg", "dlfkja;ldsfkl;dsfkl;daskfl;d"},
    {"abcdefgd", "dlfkja;ldsfkl;dsfkl;daskfl;d"},
    {"abcdefgh", "dsa.fkl;dsfkl;dskf;ldskfl;dkf"},
    {"abcdefghe", "dsa.fkl;dsfkl;dskf;ldskfl;dkf"},
    {"abcdefghi", "ld;kfa;lsdfk;ldskf;ldskf;ldskf;ld"},
    {"abcdefghif", "ld;kfa;lsdfk;ldskf;ldskf;ldskf;ld"},
    {"abcdefghij", "dslfjaldsfjdslfjkdslfjldsjfldsfj"},
    {"abcdefghijg", "dslfjaldsfjdslfjkdslfjldsjfldsfj"},
    {"abcdefghijk", "lkjdfakldsjflsdkjfldsjfeow[rowe[r"},
    {"abcdefghijkh", "lkjdfakldsjflsdkjfldsjfeow[rowe[r"},
    {"abcdefghijkl", "sdfu9OIUIOUIODFAUSIOFUAOISFUOI"},
    {"abcdefghijkli", "sdfu9OIUIOUIODFAUSIOFUAOISFUOI"},
    {"abcdefghijklm", "kdjfakldsjfldsjfldsjflajdslfjdslf"},
    {"abcdefghijklmj", "kdjfakldsjfldsjfldsjflajdslfjdslf"},
    {"abcdefghijklmn", "dafhjkldsjfld;sjfl;dsjfl;dsjfl;djsal;f"},
    {"abcdefghijklmnk", "dafhjkldsjfld;sjfl;dsjfl;dsjfl;djsal;f"}
};
#define INUM 250
static int ilist[] = {
232,19,158,38,175,197,114,68,188,109,120,80,102,47,102,143,142,79,160,52,3,124,114,32,70,18,189,123,116,190,247,56,17,157,230,3,139,79,204,66,22,167,208,141,155,125,158,16,54,157,56,53,118,49,163,35,84,116,30,193,22,211,24,89,251,223,42,123,228,43,13,211,160,178,15,154,233,126,200,14,30,19,234,106,32,185,15,104,6,228,183,173,125,202,177,131,16,162,158,159,216,57,71,143,122,143,112,87,189,144,239,236,95,180,30,98,232,214,53,197,135,82,39,148,205,228,83,235,181,162,217,40,97,52,67,171,90,217,201,56,80,186,53,12,76,140,215,199,170,121,47,13,0,39,144,8,4,80,190,216,123,146,8,155,183,109,225,194,46,19,206,189,163,213,34,70,185,39,238,87,40,232,122,39,47,60,46,190,208,250,209,173,145,180,180,44,67,206,69,192,219,115,68,101,87,180,76,218,74,187,198,37,140,95,122,36,213,172,196,195,10,203,125,126,72,4,64,186,51,121,202,80,29,79,245,189,142,75,238,239,230,205,0,231,63,217,101,208,204,96,39,128,123,227,126,39};
//usage
//gcc -o t trie.c timer.c -D_DEBUG_TRIE -D_FILE_OFFSET_BITS=64 && ./t ./wordict.txt ./temp.txt
int main(int argc, char **argv)
{
    void *trietab = NULL;
    char *key = NULL;
    void *skey = NULL;
    void *pdata = NULL;
    char *file = NULL;
    char buf[LINE_MAX];
    char *p = NULL, *end = NULL;
    int i = 0, n = 0;
    long wordid = 1;
    FILE *fp = NULL;
    TIMER *timer = NULL;
    long long insert_total = 0, query_total = 0, seg_total = 0;
    HNODE hnode , *pnode = NULL;
    unsigned char s = 0;
    int min = 0, max = 0, count = 0;
    long k = 0, m = 0;
    unsigned char slist[256];
    char *txtfile = NULL;
    char *content = NULL;
    int fd = -1, pos = 0, nforward = 0;
    struct stat st;
    long word_id = 0;

    /*
    pnode = &(hnode);
    memset(pnode, 0, sizeof(HNODE));
    memset(slist, 0, 256);
    for(k = 0; k < 65530; k++)
    {
        s = (rand()%256);
        //if(slist[s] == 0 ){fprintf(stdout, "%d,", s); slist[s]++;count++;}
        HN_FIND(pnode, s, min, max, n);
        fprintf(stdout, "k:%d[%d] n:%d\n", k, (unsigned char )s, n);
        if(n < 0 || n == pnode->count || pnode->list[n].chr != ((unsigned char )s))
        {
            HN_ADD(pnode, s, min, max, n);
            pnode->list[n].dptr = (void *)k;
        }
    }
    for(i = 0; i < pnode->count; i++) fprintf(stdout, "i:%d[%d]\n", i, pnode->list[i].chr);
    */
    //fprintf(stdout, "count:%d:%d\n", pnode->count, count);

    /*
    */
    if(argc < 2)
    {
        fprintf(stderr, "Usage:%s wordfile [contentfile]\n", argv[0]);
        _exit(-1);
    }
    file = argv[1];
    if(argc > 2) txtfile = argv[2];
    /*

    void *trietab = NULL, *dp = NULL;
    char *s = "adfdsfdsf";
    int len = 0, id = 1000;

    if(trietab = TRIETAB_INIT())
    {
        if((len = strlen(s)) > 0)
        {
            dp = (void *)((long)(id));
            TRIETAB_RADD(trietab, s, len, pdata);
        }
        TRIETAB_CLEAN(trietab);
    }
    */
    if((trietab = TRIETAB_INIT()))
    {
        if((fp = fopen(file, "r")))
        {
            TIMER_INIT(timer);
            while(fgets(buf, LINE_MAX, fp))
            {
                p = buf;
                while(*p != '\n' && *p != '\0')p++;
                *p = '\0';
                n = p - buf;
                //fprintf(stdout, "%d:%s\n", n, buf);
                TIMER_SAMPLE(timer);
                pdata = (void *)wordid++;
                TRIETAB_RADD(trietab, buf, n, pdata);
                //TRIETAB_RADD(trietab, buf, n, pdata);
                TIMER_SAMPLE(timer);
                /*
                   if(pdata)
                   {
                   fprintf(stdout, "%ld:%s\n", (long)pdata, buf);
                   }
                   */
                insert_total += PT_LU_USEC(timer);

            }
            fseek(fp, 0, SEEK_SET);
            word_id = 1;
            while(fgets(buf, LINE_MAX, fp))
            {
                p = buf;
                while(*p != '\n' && *p != '\0')p++;
                *p = '\0';
                n = p - buf;
                TIMER_SAMPLE(timer);
                TRIETAB_RGET(trietab, buf, n, pdata);
                //word_id = (long) pdata;
                //TRIETAB_RGET(trietab, buf, n, pdata);
                //if(((long)pdata) != word_id) fprintf(stdout, "%d:%d\n", word_id, (long)pdata);
                //word_id++;
                TIMER_SAMPLE(timer);
                query_total += PT_LU_USEC(timer);
            }
            fprintf(stdout, "wordid:%d count:%d size:%d\n"
                    "insert_usec:%lld insert_avg:%f\n"
                    "query_usec:%lld query_avg:%f\n", 
                    wordid, HBCNT(trietab), HBSIZE(trietab), 
                    insert_total, (double)(insert_total/wordid),
                    query_total, (double)(query_total/wordid));

            if(txtfile && (fd = open(txtfile, O_RDONLY, 0644)) > 0)
            {
                fstat(fd, &st);
                if(st.st_size > 0 && (content = (char *)malloc(st.st_size)) 
                        && read(fd, content, st.st_size) > 0)
                {
                    p = content;
                    end = content + st.st_size;
                    
                    TIMER_SAMPLE(timer);
                    while(p < end)
                    {
                        while(p < end && (*p == 0x20 || *p == '\r' 
                                    || *p == '\n' || *p == '\t'))++p;
                        if(((unsigned char )*p) >= 252) nforward = 6;
                        else if(((unsigned char )*p) >= 248) nforward = 5;
                        else if(((unsigned char )*p) >= 240) nforward = 4;
                        else if(((unsigned char )*p) >= 224) nforward = 3;
                        else if(((unsigned char )*p) >= 192) nforward = 2;
                        else nforward = 1;
                        TRIETAB_MAX_FIND(trietab, p, (end - p), pdata, pos);
                        //fprintf(stdout, "%d:%d:%d\n", (long)pdata, pos, st.st_size);
                        if(pdata && pos >= 0)
                        {
                            //fprintf(stdout, "%d:%d:%s", pos, (long *)pdata, p);
                            //break;
                            //memcpy(buf, p, pos);
                            //buf[pos] = '\0';
                            //fprintf(stdout, "%d:%s\n", (long *)pdata, buf);
                            p += (pos);
                        }
                        else
                        {
                            p += nforward;
                        }
                    }
                    TIMER_SAMPLE(timer);
                    fprintf(stdout, "segment text size:%lld time:%lld\n", 
                            st.st_size, (seg_total = PT_LU_USEC(timer)));

		   /*
                    fprintf(stdout, "segment text size:%lld time:%lld avg:%f\n", 
                            st.st_size, (seg_total = PT_LU_USEC(timer)), 
                            (double)(seg_total/st.st_size));
			*/
                    free(content);
                    content = NULL;
                }
                close(fd);
            }
            //trietab_view(trietab);
            //return ;
            /*
               fprintf(stdout, "wordid:%d count:%d size:%d\n"
               "insert_usec:%lld insert_avg:%f\n"
               "query_usec:%lld query_avg:%f\n", 
               wordid, HBCNT(trietab), HBSIZE(trietab), 
               insert_total, (double)(insert_total/wordid),
               query_total, (double)(query_total/wordid));
               fseek(fp, 0, SEEK_SET);
               while(fgets(buf, LINE_MAX, fp))
               {
               p = buf;
               while(*p != '\n' && *p != '\0')p++;
             *p = '\0';
             n = p - buf;
             TIMER_SAMPLE(timer);
             TRIETAB_GET(trietab, buf, n, pdata);
             TIMER_SAMPLE(timer);
             query_total += PT_LU_USEC(timer);
             if(pdata) fprintf(stdout, "%d\n", (long)pdata);
             }
             fprintf(stdout, "wordid:%d count:%d size:%d\n"
             "insert_usec:%lld insert_avg:%f\n"
             "query_usec:%lld query_avg:%f\n", 
             wordid, HBCNT(trietab), HBSIZE(trietab), 
             insert_total, (double)(insert_total/wordid),
             query_total, (double)(query_total/wordid));
             */
            TIMER_CLEAN(timer);
            fclose(fp);
        }
        TRIETAB_CLEAN(trietab);
    }
    /*
    if((trietab = TRIETAB_INIT()))
    {
        for(i = 0; i < NRECORD; i++)
        {
            key = recdlist[i].key;
            skey = recdlist[i].data;
            TRIETAB_ADD(trietab, key, strlen(key), skey);
        }
        for(i = 0; i < NRECORD; i++)
        {
            key = recdlist[i].key;
            skey = recdlist[i].data;
            TRIETAB_GET(trietab, key, strlen(key), pdata);
            if(pdata) fprintf(stdout, "get:%s\nold:%s\n", pdata, skey);
        }
        fprintf(stdout, "count:%d size:%d\n", HBCNT(trietab), HBSIZE(trietab));
    }
    */
    return 0;
}
#endif
