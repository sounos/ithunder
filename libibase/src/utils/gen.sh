#!/bin/bash
#long
cp -f imap.h lmap.h
cp -f imap.c lmap.c
perl -i -p -e "s/int32_t/int64_t/g" lmap.h lmap.c
perl -i -p -e "s/IMM/LMM/g" lmap.h lmap.c
perl -i -p -e "s/IMAP/LMAP/g" lmap.h lmap.c
perl -i -p -e "s/imap/lmap/g" lmap.h lmap.c
perl -i -p -e "s/typedef uint64_t u32_t/typedef uint32_t u32_t/g" lmap.h lmap.c
#double
cp -f imap.h dmap.h
cp -f imap.c dmap.c
perl -i -p -e "s/int32_t/double/g" dmap.h dmap.c
perl -i -p -e "s/IMM/DMM/g" dmap.h dmap.c
perl -i -p -e "s/IMAP/DMAP/g" dmap.h dmap.c
perl -i -p -e "s/imap/dmap/g" dmap.h dmap.c
perl -i -p -e "s/typedef udouble u32_t/typedef uint32_t u32_t/g" dmap.h dmap.c
