#!/bin/bash
mkdir -p backup ithunder; datetime=`date +%Y%m%d%H%M%S`;tar -zcf backup/all-${datetime}.tar.gz rpms src srpms/ *.sh; tar -zcf backup/rpms-${datetime}.tar.gz rpms; tar -zcf backup/source-${datetime}.tar.gz src srpms *.sh; 
#sodo rpms
#tar -zcf sodo/rpms-${datetime}.tar.gz rpms/libevbase* rpms/libsbase* rpms/libscws* rpms/libsobase* rpms/sodo* debuginfo/libevbase* debuginfo/libsbase* debuginfo/libscws* debuginfo/libsobase* debuginfo/sodo*; 
#sodo srpms
#tar -zcf sodo/srpms-${datetime}.tar.gz srpms/libevbase* srpms/libsbase* srpms/libscws* srpms/libsobase* srpms/sodo* sodo.sh; 
#ithunder rpms
tar -zcf ithunder/rpms-${datetime}.tar.gz rpms/libevbase* rpms/libsbase*  rpms/libscws* rpms/libibase* rpms/libchardet* rpms/hibase* rpms/libdbase* rpms/libmtask* rpms/qmtask* rpms/hidbase* rpms/ithunder* 
#debuginfo/libevbase* debuginfo/libsbase*  debuginfo/libscws* debuginfo/libibase* debuginfo/libchardet* debuginfo/hibase* debuginfo/qmtask* debuginfo/hidbase* debuginfo/ithunder* ; 
#ithunder srpms
tar -zcf ithunder/srpms-${datetime}.tar.gz srpms/libevbase* srpms/libsbase* srpms/libscws* srpms/libibase* srpms/libchardet* srpms/hibase* srpms/qmtask* srpms/hidbase* srpms/ithunder* build.sh
#sowork rpms
#tar -zcf sowork/rpms-${datetime}.tar.gz rpms/libevbase* rpms/libsbase*  rpms/libscws* rpms/libsobase* rpms/sodo* rpms/libdbase* rpms/libmtask* rpms/qmtask* rpms/hidbase* rpms/soworker* debuginfo/libevbase* debuginfo/libsbase*  debuginfo/libscws* debuginfo/libsobase* debuginfo/sodo* debuginfo/qmtask* debuginfo/hidbase* debuginfo/soworker* ; 
#sowork srpms
#tar -zcf sowork/srpms-${datetime}.tar.gz srpms/libevbase* srpms/libsbase* srpms/libscws* srpms/libsobase* srpms/sodo* srpms/qmtask* srpms/hidbase* srpms/soworker* sowork.sh
#./remote.sh backup/rpms-${datetime}.tar.gz
