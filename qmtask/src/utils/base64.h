#ifndef _BASE64_H
#define _BASE64_H
#define BASE64_LEN(x) ((x + 2) / 3 * 4 + 1)
int base64_encode(char *out, const unsigned char *in, int inlen);
int base64_decode(unsigned char *out, const char *in, int inlen);
#endif
