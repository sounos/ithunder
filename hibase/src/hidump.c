#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <time.h>
#include <errno.h>
#include <ibase.h>
int main(int argc, char **argv)
{
    char *outfile = NULL, *block = NULL, *content = NULL, 
         *p = NULL, *last = NULL;
    int fd = 0, i = 0, k = 0, count = 0, nfields = 7;
    FHEADER *fheader = NULL;
    IFIELD *fields = NULL;
    if(argc < 3) 
    {
        fprintf(stderr, "Usage:%s outfile count\r\n", argv[0]);
        _exit(-1);
    }
    outfile = argv[1];
    count = atoi(argv[2]);
    if((fd = open(outfile, O_CREAT|O_WRONLY|O_APPEND, 0644)) > 0)
    {
        if(count > 0 && (block = (char *)calloc(1, 1024 * 1024 * 8)) )
        {
            for(i = 0; i < count; i++)
            {
                fheader = (FHEADER *)block;
                memset(fheader, 0, sizeof(FHEADER));
                fields = (IFIELD *)(block + sizeof(FHEADER));
                memset(fields, 0, sizeof(IFIELD) * nfields);
                p = content = block + sizeof(FHEADER) + sizeof(IFIELD) * nfields;
                fheader->globalid = i + 100000000;
                fheader->nfields = nfields;
                fheader->rank = 1.0f;
                fheader->category = 1;
                fheader->nfields = nfields;
                fheader->flag = (IB_RANK_SET|IB_INT_SET|IB_LONG_SET|IB_DOUBLE_SET|IB_STATUS_SET);
                k = 0;
                fields[k].offset = p - content;last = p;
                p += sprintf(p, "title:%d\r\n", i);
                fields[k].length = p - last;
                fields[k].flag = IB_DATATYPE_TEXT|IB_IS_NEED_INDEX;k++;
                fields[k].offset = p - content;last = p;
                p += sprintf(p, "content:%d\r\n", i);
                fields[k].length = p - last;
                fields[k].flag = IB_DATATYPE_TEXT|IB_IS_NEED_INDEX;k++;
                fields[k].offset = p - content;last = p;
                p += sprintf(p, "author:%d\r\n", i);
                fields[k].length = p - last;
                fields[k].flag = IB_DATATYPE_TEXT|IB_IS_NEED_INDEX;k++;
                //fheader.crc = crc32(content);
                fields[k].offset = p - content;last = p;
                p += sprintf(p, "%u\r\n", (unsigned int)time(NULL));
                fields[k].length = p - last;
                fields[k].flag = IB_DATATYPE_INT|IB_IS_NEED_INDEX;k++;
                fields[k].offset = p - content;last = p;
                p += sprintf(p, "%lld\r\n", (long long int)fheader->globalid);
                fields[k].length = p - last;
                fields[k].flag = IB_DATATYPE_LONG|IB_IS_NEED_INDEX;k++;
                fields[k].offset = p - content;last = p;
                p += sprintf(p, "%f\r\n", (double)random()/(double)10000000);
                fields[k].length = p - last;
                fields[k].flag = IB_DATATYPE_DOUBLE|IB_IS_NEED_INDEX;k++;
                fields[k].offset = p - content;last = p;
                p += sprintf(p, "http://demo.com/%d.html\r\n", i);
                fields[k].length = p - last;
                fields[k].flag = IB_DATATYPE_TEXT;k++;
                fheader->size = p - (char *)fields;
                if(write(fd, block, (p - block)) < 0)
                {
                    fprintf(stderr, "write to %s failed, %s\r\n", outfile, strerror(errno));
                    _exit(-1);
                }
            }
            free(block);
        }
        close(fd);
    }
    return 0;
}
