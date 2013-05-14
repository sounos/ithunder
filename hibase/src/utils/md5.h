#include <sys/types.h>
#ifndef _MD5_H
#define _MD5_H
#ifdef __cplusplus
extern "C" {
#endif
#define MD5_LEN 16
#define _MD5_BLOCK_N 64
#define _MD5_SET_N  56
#define _MD5_BITS_N  8
#ifndef _TYPEDEF_MD5_CTX
#define _TYPEDEF_MD5_CTX
typedef struct _MD5_CTX
{
	u_int32_t state[4];
	u_int32_t total[2];
	unsigned char digest[MD5_LEN];
	unsigned char buf[_MD5_BLOCK_N];		
}MD5_CTX;
/* Initialize */
void md5_init(MD5_CTX *context);
/* Update */
void md5_update(MD5_CTX *context, unsigned char *data, u_int32_t ndata);
/* Final */
void md5_final(MD5_CTX *context);
void md5(unsigned char *data, u_int32_t ndata, unsigned char *digest);
/* Cacalute FILE md5 */
int md5_file(const char *file, unsigned char *digest);
#endif
#define MD5(_data, _ndata, md) {					\
	MD5_CTX ctx;							\
        md5_init(&ctx);							\
        md5_update(&ctx, _data, _ndata);				\
        md5_final(&ctx);						\
        memcpy(md, ctx.digest, MD5_LEN);					\
}
#define MD5OUT(md, out)							\
{									\
	int i = 0;							\
	do{								\
		fprintf(out, "%02x", md[i++]);				\
	}while(i < MD5_LEN);						\
}
#ifdef __cplusplus
 }
#endif
#endif
