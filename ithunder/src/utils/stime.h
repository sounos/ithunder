#ifndef _STIME_H
#define _STIME_H
#include <sys/time.h>
#include <time.h>
//convert str datetime to time
time_t str2time(char *datestr);
/* time to GMT */
int GMTstrdate(time_t time, char *date);
/* strdate */
int strdate(time_t time, char *date);
/* local date time */
int datetime(time_t times, char *date);
/* timetospec */
void timetospec(struct timespec *ts, int usecs);
#endif
