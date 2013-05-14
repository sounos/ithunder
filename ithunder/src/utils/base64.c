#include <string.h>
#include "base64.h"
static const char base64char[64] = 
"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
static const char base64val[128] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63,
    52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1,
    -1,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
    15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1,
    -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
    41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1
};
#define IS_ASCII(c) (((unsigned char) c) <= 0177 ? 1 : 0)
#define BASE64VAL(c)    (IS_ASCII(c) ? base64val[(int) (c)] : -1)
int base64_encode(char *out, const unsigned char *in, int inlen)
{
    const unsigned char *inp = in;
    char *outp = out;

    while (inlen >= 3) {
        *outp++ = base64char[(inp[0] >> 2) & 0x3f];
        *outp++ = base64char[((inp[0] & 0x03) << 4) |
            ((inp[1] >> 4) & 0x0f)];
        *outp++ = base64char[((inp[1] & 0x0f) << 2) |
            ((inp[2] >> 6) & 0x03)];
        *outp++ = base64char[inp[2] & 0x3f];

        inp += 3;
        inlen -= 3;
    }

    if (inlen > 0) {
        *outp++ = base64char[(inp[0] >> 2) & 0x3f];
        if (inlen == 1) {
            *outp++ = base64char[(inp[0] & 0x03) << 4];
            *outp++ = '=';
        } else {
            *outp++ = base64char[((inp[0] & 0x03) << 4) |
                ((inp[1] >> 4) & 0x0f)];
            *outp++ = base64char[((inp[1] & 0x0f) << 2)];
        }
        *outp++ = '=';
    }
    *outp = '\0';
    return outp - out;
}

int base64_decode(unsigned char *out, const char *in, int inlen)
{
    const char *inp = in;
    unsigned char *outp = out;
    char buf[4];

    if (inlen < 0) return -1;

    while (inlen >= 4 && *inp != '\0')
    {
        buf[0] = *inp++;
        inlen--;
        if (BASE64VAL(buf[0]) == -1) break;

        buf[1] = *inp++;
        inlen--;
        if (BASE64VAL(buf[1]) == -1) break;

        buf[2] = *inp++;
        inlen--;
        if (buf[2] != '=' && BASE64VAL(buf[2]) == -1) break;

        buf[3] = *inp++;
        inlen--;
        if (buf[3] != '=' && BASE64VAL(buf[3]) == -1) break;

        *outp++ = ((BASE64VAL(buf[0]) << 2) & 0xfc) |
            ((BASE64VAL(buf[1]) >> 4) & 0x03);
        if (buf[2] != '=') {
            *outp++ = ((BASE64VAL(buf[1]) & 0x0f) << 4) |
                ((BASE64VAL(buf[2]) >> 2) & 0x0f);
            if (buf[3] != '=') {
                *outp++ = ((BASE64VAL(buf[2]) & 0x03) << 6) |
                    (BASE64VAL(buf[3]) & 0x3f);
            }
        }
    }
    return outp - out;
}
