#include <chardet.h>
#ifndef __QCHARDET_H__
#define __QCHARDET_H__
#define Q_CHARDET_MAX 4096
typedef struct _QCHARDET
{
    void *mutex;
    chardet_t qpool[Q_CHARDET_MAX];
    int nqpool;
    int bits;
}QCHARDET;
void qchardet_init(QCHARDET *q);
chardet_t qchardet_pop(QCHARDET *q);
void qchardet_push(QCHARDET *q, chardet_t pdet);
void qchardet_close(QCHARDET *q);
#endif
