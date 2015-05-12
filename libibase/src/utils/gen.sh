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
#cdb.c
cp -f db.h cdb.h
cp -f db.c cdb.c
perl -i -p -e "s/DB/CDB/g" cdb.h cdb.c
perl -i -p -e "s/db_/cdb_/g" cdb.h cdb.c
perl -i -p -e "s/XIO/XCIO/g" cdb.h cdb.c
perl -i -p -e "s/XBLOCK/XCBLOCK/g" cdb.h cdb.c 
perl -i -p -e "s/XSTATE/XCSTATE/g" cdb.h cdb.c 
perl -i -p -e 's|"db.h"|"cdb.h"|g' cdb.c 
perl -i -p -e "s/CDB_BASE_SIZE        64/CDB_BASE_SIZE        4096/g" cdb.h 
#mdb.c
cp -f db.h mdb.h
cp -f db.c mdb.c
perl -i -p -e "s/DB/MDB/g" mdb.h mdb.c
perl -i -p -e "s/db_/mdb_/g" mdb.h mdb.c
perl -i -p -e "s/XIO/XMIO/g" mdb.h mdb.c
perl -i -p -e "s/XBLOCK/XMBLOCK/g" mdb.h mdb.c 
perl -i -p -e "s/XSTATE/XMSTATE/g" mdb.h mdb.c 
perl -i -p -e 's|"db.h"|"mdb.h"|g' mdb.c 
perl -i -p -e "s|//#define __USE_X_TAG__|#define __USE_X_TAG__|g" mdb.c 

