#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include "zvbcode.h"
#define  VB_NUM_MAX  100000000
#ifdef _DEBUG_ZVBCODE
#include "timer.h"
#define __ZINT_OUT__ 0
#define __ZINT__ "%d\n"
typedef int ZINT;
int main(int argc, char **argv)
{
    int i = 0, n = 0, x = 0;
    ZINT rand = 0, *dp = NULL, *np = NULL;
    char *p = NULL, *ps = NULL, *ep = NULL;
    FILE *fp = NULL, *dfp = NULL;
    void *timer = NULL;

    if((ps = (char *)calloc(VB_NUM_MAX*2, sizeof(ZINT))))
    {
        TIMER_INIT(timer);
        n = VB_NUM_MAX  * sizeof(ZINT);
        if((fp = fopen("/tmp/double.txt", "a+")))
        {
            p = ps;
            np = &rand;
            for(i = 0; i < VB_NUM_MAX; i++)
            {
                rand = (ZINT)random()%100000000;
                if(__ZINT_OUT__)fprintf(fp, __ZINT__, rand);
                ZVBCODE(np, p);
            }
            TIMER_SAMPLE(timer);
            fflush(fp);
            fprintf(stdout, "compressed(sizeof(zint):%d) %d to %d time:%lld avg:%f\n", 
                    sizeof(ZINT), n, (p - ps), PT_LU_USEC(timer), 
                    (double)PT_LU_USEC(timer)/(double)VB_NUM_MAX);
            fclose(fp);
        }
        if((dfp = fopen("/tmp/double.text", "a+")))
        {
            ep = p;
            p = ps;
            np = &rand;  
            TIMER_SAMPLE(timer);
            i = 0;
            while(p < ep)
            {
                rand = 0;
                UZVBCODE(p, x, np);
                i++;
                if(__ZINT_OUT__) fprintf(dfp, __ZINT__, rand);
            }
            TIMER_SAMPLE(timer);
            fflush(fp);
            fprintf(stdout, "decompressed(typesizeof(ZINT):%d) %d to %d time:%lld avg:%f\n", 
                    sizeof(ZINT), (p - ps), i * sizeof(ZINT), PT_LU_USEC(timer), 
                    (double)PT_LU_USEC(timer)/(double)i);
            fclose(dfp);
        }
        free(ps);
        ps = NULL;
    }
}
#endif
