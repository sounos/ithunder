#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <string.h>
#include <sys/types.h>
#include <fcntl.h>
#include "md5.h"
#define MEMCPY(dstptr, srcptr, n) 					\
{									\
	char *_dst = (char *)dstptr; 					\
	char *_src = (char *)srcptr; 					\
	while((_dst - (char *)dstptr) != n ) *_dst++ = *_src++; 	\
}
#define MEMSET(ptr, c, n) 						\
{ 									\
	char *_p = (char *)ptr; 					\
	while((_p - (char *)ptr) != n) *_p++ = c;			\
}
#define DECODE(_x, _b, _nb) 						\
{ 									\
	u_int32_t _i , _j; 						\
	for (_i = 0, _j = 0; _j < _nb; _i++, _j += 4) {			\
 		_x[_i] = ( (u_int32_t) _b[_j] ) 			\
			| ( ((u_int32_t)_b[_j+1]) << 8) 		\
			| ( ((u_int32_t) _b[_j+2]) << 16) 		\
			| ( ((u_int32_t) _b[_j+3]) << 24);		\
	} 								\
}
#define ENCODE(_x, _b, _nb) 						\
{ 									\
	u_int32_t _i, _j; 						\
	for (_i = 0, _j = 0; _j < _nb; _i++, _j += 4) { 		\
		_x[_j] = (unsigned char)(_b[_i] & 0xff); 		\
		_x[_j+1] =  (unsigned char)((_b[_i] >> 8) & 0xff); 	\
		_x[_j+2] = (unsigned char)((_b[_i] >> 16) & 0xff); 	\
		_x[_j+3] = (unsigned char)((_b[_i] >> 24) & 0xff); 	\
	} 								\
}
static unsigned char PADDING[] = {
  0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};
#define S11 7
#define S12 12
#define S13 17
#define S14 22
#define S21 5
#define S22 9
#define S23 14
#define S24 20
#define S31 4
#define S32 11
#define S33 16
#define S34 23
#define S41 6
#define S42 10
#define S43 15
#define S44 21
#define ff(x, y, z) (((x) & (y)) | ((~x) & (z)))
#define gg(x, y, z) (((x) & (z)) | ((y) & (~z)))
#define hh(x, y, z) ((x) ^ (y) ^ (z))
#define ii(x, y, z) ((y) ^ ((x) | (~z)))
#define lrotate(x, n) (((x) << (n)) | ((x) >> (32-(n))))
#define FF(a, b, c, d, x, s, ac) {					\
	(a) += ff ((b), (c), (d)) + (x) + (u_int32_t)(ac); 		\
	(a) = lrotate ((a), (s)); 					\
	(a) += (b); 							\
}
#define GG(a, b, c, d, x, s, ac) { 					\
	(a) += gg ((b), (c), (d)) + (x) + (u_int32_t)(ac); 		\
	(a) = lrotate ((a), (s)); 					\
	(a) += (b); 							\
}
#define HH(a, b, c, d, x, s, ac) { 					\
	(a) += hh ((b), (c), (d)) + (x) + (u_int32_t)(ac); 		\
	(a) = lrotate ((a), (s)); 					\
	(a) += (b); 							\
}
#define II(a, b, c, d, x, s, ac) { 					\
	(a) += ii ((b), (c), (d)) + (x) + (u_int32_t)(ac); 		\
	(a) = lrotate ((a), (s)); 					\
	(a) += (b); 							\
}
#define MD5_CALCULATE(ctx, block, nblock) 				\
{ 									\
    u_int32_t x[MD5_LEN]; 						\
    u_int32_t a = ctx->state[0], b = ctx->state[1],  		\
    c = ctx->state[2], d = ctx->state[3]; 		\
    DECODE(x, block, nblock); 					\
    /* Round 1 */ 							\
    FF (a, b, c, d, x[ 0], S11, 0xd76aa478); /* 1 */ 		\
    FF (d, a, b, c, x[ 1], S12, 0xe8c7b756); /* 2 */ 		\
    FF (c, d, a, b, x[ 2], S13, 0x242070db); /* 3 */ 		\
    FF (b, c, d, a, x[ 3], S14, 0xc1bdceee); /* 4 */ 		\
    FF (a, b, c, d, x[ 4], S11, 0xf57c0faf); /* 5 */ 		\
    FF (d, a, b, c, x[ 5], S12, 0x4787c62a); /* 6 */ 		\
    FF (c, d, a, b, x[ 6], S13, 0xa8304613); /* 7 */ 		\
    FF (b, c, d, a, x[ 7], S14, 0xfd469501); /* 8 */ 		\
    FF (a, b, c, d, x[ 8], S11, 0x698098d8); /* 9 */ 		\
    FF (d, a, b, c, x[ 9], S12, 0x8b44f7af); /* 10 */ 		\
    FF (c, d, a, b, x[10], S13, 0xffff5bb1); /* 11 */ 		\
    FF (b, c, d, a, x[11], S14, 0x895cd7be); /* 12 */ 		\
    FF (a, b, c, d, x[12], S11, 0x6b901122); /* 13 */ 		\
    FF (d, a, b, c, x[13], S12, 0xfd987193); /* 14 */ 		\
    FF (c, d, a, b, x[14], S13, 0xa679438e); /* 15 */ 		\
    FF (b, c, d, a, x[15], S14, 0x49b40821); /* 16 */ 		\
    /* Round 2 */ 							\
    GG (a, b, c, d, x[ 1], S21, 0xf61e2562); /* 17 */ 		\
    GG (d, a, b, c, x[ 6], S22, 0xc040b340); /* 18 */ 		\
    GG (c, d, a, b, x[11], S23, 0x265e5a51); /* 19 */ 		\
    GG (b, c, d, a, x[ 0], S24, 0xe9b6c7aa); /* 20 */ 		\
    GG (a, b, c, d, x[ 5], S21, 0xd62f105d); /* 21 */ 		\
    GG (d, a, b, c, x[10], S22,  0x2441453); /* 22 */ 		\
    GG (c, d, a, b, x[15], S23, 0xd8a1e681); /* 23 */ 		\
    GG (b, c, d, a, x[ 4], S24, 0xe7d3fbc8); /* 24 */ 		\
    GG (a, b, c, d, x[ 9], S21, 0x21e1cde6); /* 25 */ 		\
    GG (d, a, b, c, x[14], S22, 0xc33707d6); /* 26 */ 		\
    GG (c, d, a, b, x[ 3], S23, 0xf4d50d87); /* 27 */ 		\
    GG (b, c, d, a, x[ 8], S24, 0x455a14ed); /* 28 */ 		\
    GG (a, b, c, d, x[13], S21, 0xa9e3e905); /* 29 */ 		\
    GG (d, a, b, c, x[ 2], S22, 0xfcefa3f8); /* 30 */ 		\
    GG (c, d, a, b, x[ 7], S23, 0x676f02d9); /* 31 */ 		\
    GG (b, c, d, a, x[12], S24, 0x8d2a4c8a); /* 32 */ 		\
    /* Round 3 */ 							\
    HH (a, b, c, d, x[ 5], S31, 0xfffa3942); /* 33 */ 		\
    HH (d, a, b, c, x[ 8], S32, 0x8771f681); /* 34 */ 		\
    HH (c, d, a, b, x[11], S33, 0x6d9d6122); /* 35 */ 		\
    HH (b, c, d, a, x[14], S34, 0xfde5380c); /* 36 */ 		\
    HH (a, b, c, d, x[ 1], S31, 0xa4beea44); /* 37 */ 		\
    HH (d, a, b, c, x[ 4], S32, 0x4bdecfa9); /* 38 */ 		\
    HH (c, d, a, b, x[ 7], S33, 0xf6bb4b60); /* 39 */ 		\
    HH (b, c, d, a, x[10], S34, 0xbebfbc70); /* 40 */ 		\
    HH (a, b, c, d, x[13], S31, 0x289b7ec6); /* 41 */ 		\
    HH (d, a, b, c, x[ 0], S32, 0xeaa127fa); /* 42 */ 		\
    HH (c, d, a, b, x[ 3], S33, 0xd4ef3085); /* 43 */ 		\
    HH (b, c, d, a, x[ 6], S34,  0x4881d05); /* 44 */ 		\
    HH (a, b, c, d, x[ 9], S31, 0xd9d4d039); /* 45 */ 		\
    HH (d, a, b, c, x[12], S32, 0xe6db99e5); /* 46 */ 		\
    HH (c, d, a, b, x[15], S33, 0x1fa27cf8); /* 47 */ 		\
    HH (b, c, d, a, x[ 2], S34, 0xc4ac5665); /* 48 */ 		\
    /* Round 4 */ 							\
    II (a, b, c, d, x[ 0], S41, 0xf4292244); /* 49 */ 		\
    II (d, a, b, c, x[ 7], S42, 0x432aff97); /* 50 */ 		\
    II (c, d, a, b, x[14], S43, 0xab9423a7); /* 51 */ 		\
    II (b, c, d, a, x[ 5], S44, 0xfc93a039); /* 52 */ 		\
    II (a, b, c, d, x[12], S41, 0x655b59c3); /* 53 */ 		\
    II (d, a, b, c, x[ 3], S42, 0x8f0ccc92); /* 54 */ 		\
    II (c, d, a, b, x[10], S43, 0xffeff47d); /* 55 */ 		\
    II (b, c, d, a, x[ 1], S44, 0x85845dd1); /* 56 */ 		\
    II (a, b, c, d, x[ 8], S41, 0x6fa87e4f); /* 57 */ 		\
    II (d, a, b, c, x[15], S42, 0xfe2ce6e0); /* 58 */ 		\
    II (c, d, a, b, x[ 6], S43, 0xa3014314); /* 59 */ 		\
    II (b, c, d, a, x[13], S44, 0x4e0811a1); /* 60 */ 		\
    II (a, b, c, d, x[ 4], S41, 0xf7537e82); /* 61 */ 		\
    II (d, a, b, c, x[11], S42, 0xbd3af235); /* 62 */ 		\
    II (c, d, a, b, x[ 2], S43, 0x2ad7d2bb); /* 63 */ 		\
    II (b, c, d, a, x[ 9], S44, 0xeb86d391); /* 64 */ 		\
    ctx->state[0] += a; 						\
    ctx->state[1] += b; 						\
    ctx->state[2] += c; 						\
    ctx->state[3] += d; 						\
}
//32768=32K 65536=64K 131072=128K 262144=256K 524288=512K 786432=768K 
////1048576=1M  2097152=2M 4194304=4M 8388608 = 8M 16777216=16M  33554432=32M
#define  MD5_BUF_SIZE   1048576
/* Initialize */
void md5_init(MD5_CTX *context)
{
	MEMSET(context, 0, sizeof(MD5_CTX));
	context->state[0] = 0x67452301;
	context->state[1] = 0xefcdab89;
	context->state[2] = 0x98badcfe;
	context->state[3] = 0x10325476;
}

/* Update */
void md5_update(MD5_CTX *context, unsigned char *data, u_int32_t ndata)
{
	//u_int32_t x[_MD5_BLOCK_N];
	u_int32_t i = 0, offset = 0, npart = 0;
	offset = (u_int32_t)((context->total[0] >> 3) & 0x3f);	
	if((context->total[0] += ((u_int32_t)ndata << 3)) < ((u_int32_t)ndata << 3))
		context->total[1]++;
	context->total[1] += ((u_int32_t)ndata >> 29);
	npart = _MD5_BLOCK_N - offset;
	if(ndata >= npart)
	{
		MEMCPY((context->buf + offset), data, npart);	
		MD5_CALCULATE(context, context->buf, _MD5_BLOCK_N);
		MEMSET(context->buf, 0, _MD5_BLOCK_N);
		for(i = npart; (i + _MD5_BLOCK_N - 1) < ndata; i += _MD5_BLOCK_N)
		{
			MD5_CALCULATE(context, (data + i), _MD5_BLOCK_N);
		}
		offset = 0;
	}
	else i = 0;
	MEMCPY((context->buf + offset), (data + i), (ndata - i));
	//fprintf(stdout, "buffer:%s\n", context->buf);
}

/* Final */
void md5_final(MD5_CTX *context)
{
	unsigned char bits[_MD5_BITS_N];
	u_int32_t  index = 0, npad = 0;	
	if(context)
	{
		ENCODE(bits, context->total, 8);
		index	= (u_int32_t) ((context->total[0] >> 3) & 0x3f);
		npad	= (index < _MD5_SET_N) ? (_MD5_SET_N - index ) 
				: (_MD5_SET_N + _MD5_BLOCK_N - index); 
		md5_update(context, PADDING, npad);	
		md5_update(context, bits, _MD5_BITS_N);
		ENCODE(context->digest, context->state, MD5_LEN);		
	}
}

/* md5 */
void md5(unsigned char *data, u_int32_t ndata, unsigned char *digest)
{
        MD5_CTX ctx;
        md5_init(&ctx);
        md5_update(&ctx, data, ndata);
        md5_final(&ctx);
        memcpy(digest, ctx.digest, MD5_LEN);
}


/* Cacalute FILE md5 */
int md5_file(const char *file, unsigned char *digest)
{
//32768=32K 65536=64K 131072=128K 262144=256K 524288=512K 786432=768K 
//1048576=1M  2097152=2M 4194304=4M 8388608 = 8M 16777216=16M  33554432=32M
	MD5_CTX ctx;
	//unsigned char buf[MD5_BUF_SIZE];
    unsigned char *p = NULL;
	int fd = 0, n = 0;
	struct stat st;
	if(file && stat(file, &st) == 0 && S_ISREG(st.st_mode) )
	{
		if((fd = open(file, O_RDONLY)) > 0)	
        {
            if((p = (unsigned char *)calloc(1, MD5_BUF_SIZE)))
            {
                md5_init(&ctx);
                while(( n = read(fd, p, MD5_BUF_SIZE)) > 0)
                {
                    md5_update(&ctx, p, (u_int32_t)n);	
                }
                md5_final(&ctx);
                memcpy(digest, ctx.digest, MD5_LEN);
                free(p);
            }
            close(fd);
            return 0;
        }
	}
	return -1;
}

#ifdef _DEBUG_MD5
int main(int argc, char **argv)
{
	int i = 0, j = 0;
	unsigned char digest[MD5_LEN];
	if(argc < 2)
	{
		fprintf(stderr, "Usage:%s string1 string2 ...\n", argv[0]);	
		_exit(-1);
	}	
	for(i = 1; i < argc; i++)
	{
		MD5(argv[i], strlen(argv[i]), digest);
		MD5OUT(digest, stdout);
		fprintf(stdout, " %s\n", argv[i]);
	}
}
#endif


#ifdef _DEBUG_MD5FILE
int main(int argc, char **argv)
{
	int i = 0, j = 0;
	unsigned char digest[MD5_LEN];
	if(argc < 2)
	{
		fprintf(stderr, "Usage:%s file1 file2 ...\n", argv[0]);	
		_exit(-1);
	}	
	for(i = 1; i < argc; i++)
	{
		if(md5_file(argv[i], digest) == 0)
		{
			MD5OUT(digest, stdout);
			fprintf(stdout, " %s\n", argv[i]);
		}
	}
}
#endif
