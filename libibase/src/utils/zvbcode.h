#ifndef _ZVBCODE_H
#define _ZVBCODE_H
#define VBUC(p) ((unsigned char *)p)
#define LL(p) ((unsigned long long)p)
/* compress variable byte code 
 * (long/long long) int n = 100000, *np = &n;
 * char *p = buf;
 * ZVBCODE(np, p);
 *
 * */
#define ZVBCODE(np, p)                                                                      \
do                                                                                          \
{                                                                                           \
    do                                                                                      \
    {                                                                                       \
        *VBUC(p) = *VBUC(np);                                                               \
        *np >>= 7;                                                                          \
        if(*np) *VBUC(p) |= 0x80;                                                           \
        ++p;                                                                                \
    }while(*np > 0);                                                                        \
}while(0)

/* uncompress variable byte code 
 * (long/long long) int n = 0, *np = &n;
 * int x = 0;
 * char *p = buf;
 * UZVBCODE(p, x, np);
 *
 * */
#define UZVBCODE(p, x, np)                                                                  \
do                                                                                          \
{                                                                                           \
    x = 0;                                                                                  \
    do                                                                                      \
    {                                                                                       \
        *np |= ((*VBUC(p) & 0x7F) << x);                                                    \
        x += 7;                                                                             \
    }while((*VBUC(p++) & 0x80));                                                            \
}while(0)

/* compress z10 */
#define ZVB10(np, p)                                                                        \
do                                                                                          \
{                                                                                           \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
}while(0)

/* compress z9 */
#define ZVB9(np, p)                                                                         \
do                                                                                          \
{                                                                                           \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
}while(0)

/* compress z8 */
#define ZVB8(np, p)                                                                         \
do                                                                                          \
{                                                                                           \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
}while(0)

/* compress z7 */
#define ZVB7(np, p)                                                                         \
do                                                                                          \
{                                                                                           \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
}while(0)

/* compress z6 */
#define ZVB6(np, p)                                                                         \
do                                                                                          \
{                                                                                           \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
}while(0)

/* compress z5 */
#define ZVB5(np, p)                                                                         \
do                                                                                          \
{                                                                                           \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
}while(0)

/* compress z4 */
#define ZVB4(np, p)                                                                         \
do                                                                                          \
{                                                                                           \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
}while(0)

/* compress z3 */
#define ZVB3(np, p)                                                                         \
do                                                                                          \
{                                                                                           \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
}while(0)

/* compress z2 */
#define ZVB2(np, p)                                                                         \
do                                                                                          \
{                                                                                           \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
    *VBUC(p++) = (*VBUC(np) | 0x80); *np >>= 7;                                             \
}while(0)

/* compress */
#define ZVB(np, p)                                                                          \
do                                                                                          \
{                                                                                           \
   if(LL(*np) >= 0x8000000000000000llu){ZVB10(np, p);}                                      \
   else if(LL(*np) >= 0x100000000000000llu){ZVB9(np, p);}                                   \
   else if(LL(*np) >= 0x2000000000000llu){ZVB8(np, p);}                                     \
   else if(LL(*np) >= 0x40000000000llu){ZVB7(np, p);}                                       \
   else if(LL(*np) >= 0x800000000llu){ZVB6(np, p);}                                         \
   else if(LL(*np) >= 0x10000000llu){ZVB5(np, p);}                                          \
   else if(LL(*np) >= 0x200000llu){ZVB4(np, p);}                                            \
   else if(LL(*np) >= 0x4000llu){ZVB3(np, p);}                                              \
   else if(LL(*np) >= 0x80llu){ZVB2(np, p);}                                                \
   else {*VBUC(p++) = (*VBUC(np) | 0x80);}                                                  \
}while(0)

#define LL2DOUBLE(llp, dp)                                                                  \
do                                                                                          \
{                                                                                           \
    VBUC(dp)[0] = VBUC(llp)[7];                                                             \
    VBUC(dp)[1] = VBUC(llp)[6];                                                             \
    VBUC(dp)[2] = VBUC(llp)[5];                                                             \
    VBUC(dp)[3] = VBUC(llp)[4];                                                             \
    VBUC(dp)[4] = VBUC(llp)[3];                                                             \
    VBUC(dp)[5] = VBUC(llp)[2];                                                             \
    VBUC(dp)[6] = VBUC(llp)[1];                                                             \
    VBUC(dp)[7] = VBUC(llp)[0];                                                             \
}while(0)

#define DOUBLE2LL(dp, llp)                                                                  \
do                                                                                          \
{                                                                                           \
    VBUC(llp)[0] = VBUC(dp)[7];                                                             \
    VBUC(llp)[1] = VBUC(dp)[6];                                                             \
    VBUC(llp)[2] = VBUC(dp)[5];                                                             \
    VBUC(llp)[3] = VBUC(dp)[4];                                                             \
    VBUC(llp)[4] = VBUC(dp)[3];                                                             \
    VBUC(llp)[5] = VBUC(dp)[2];                                                             \
    VBUC(llp)[6] = VBUC(dp)[1];                                                             \
    VBUC(llp)[7] = VBUC(dp)[0];                                                             \
}while(0)
#endif
