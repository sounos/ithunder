#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#ifndef _ZSTREAM_H
#define _ZSTREAM_H
#define  Z_HEADER_SIZE 12
/* Compress data */
int zcompress(Bytef *data, uLong ndata, 
	Bytef *zdata, uLong *nzdata);
/* Uncompress data */
int zdecompress(Bytef *zdata, uLong nzdata,
        Bytef *data, uLong *ndata);
/* gzip Compress data */
int gzcompress(Bytef *data, uLong ndata, 
	Bytef *zdata, uLong *nzdata);
/* gzip Uncompress data */
int gzdecompress(Bytef *zdata, uLong nzdata,
        Bytef *data, uLong *ndata);
/* http gzip Uncompress data */
int httpgzdecompress(Bytef *zdata, uLong nzdata,
        Bytef *data, uLong *ndata);

#endif
