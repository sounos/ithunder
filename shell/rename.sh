#!/bin/bash
#rename libibase to libsobase 
ibver="0.5.21"
ibnewver="1.5.21"
ibname="libibase"
ibnew="libsobase"
#rename files
rm -rf ${ibname}-${ibver}
tar -zxf ${ibname}-${ibver}.tar.gz
mv ${ibname}-${ibver}/src/ibase.h ${ibname}-${ibver}/src/sobase.h
mv ${ibname}-${ibver}/src/ibase.c ${ibname}-${ibver}/src/sobase.c
mv ${ibname}-${ibver}/doc/libibase.spec ${ibname}-${ibver}/doc/libsobase.spec
perl -i -p -e "s/libibase/libsobase/g" ${ibname}-${ibver}/src/* ${ibname}-${ibver}/doc/* ${ibname}-${ibver}/configure.in
perl -i -p -e "s/ibase/sobase/g" ${ibname}-${ibver}/src/* ${ibname}-${ibver}/doc/* ${ibname}-${ibver}/configure.in
perl -i -p -e "s/IBASE/SOBASE/g" ${ibname}-${ibver}/src/* ${ibname}-${ibver}/doc/* ${ibname}-${ibver}/configure.in
perl -i -p -e "s/IB_/SOB_/g" ${ibname}-${ibver}/src/* ${ibname}-${ibver}/doc/* ${ibname}-${ibver}/configure.in
#perl -i -p -e "s/#ifdef SOB_GLOBALID_32BIT/#ifndef SOB_GLOBALID_32BIT/g" ${ibname}-${ibver}/src/sobase.h 
perl -i -p -e "s/${ibver}/${ibnewver}/g" ${ibname}-${ibver}/configure.in ${ibname}-${ibver}/doc/*.spec
cd ${ibname}-${ibver}; aclocal && autoheader && libtoolize --force && automake -a && autoconf && ./configure && make dist-gzip && mv ${ibnew}-${ibnewver}.tar.gz ../
cd ../ && rm -rf ${ibname}-${ibver}
#rename ithunder to sotime
rokver="0.0.3"
roknewver="1.0.3"
rokname="ithunder"
roknew="soworker"
rm -rf ${rokname}-${rokver}
tar -zxf ${rokname}-${rokver}.tar.gz
#src
mv ${rokname}-${rokver}/src/ithinkd.c ${rokname}-${rokver}/src/sothinkd.c
mv ${rokname}-${rokver}/src/idispatchd.c ${rokname}-${rokver}/src/sodispatchd.c
#doc
mv ${rokname}-${rokver}/doc/rc.ithinkd.ini ${rokname}-${rokver}/doc/rc.sothinkd.ini
mv ${rokname}-${rokver}/doc/rc.ithinkd.html ${rokname}-${rokver}/doc/rc.sothinkd.html
mv ${rokname}-${rokver}/doc/rc.ithinkd.init ${rokname}-${rokver}/doc/rc.sothinkd.init
mv ${rokname}-${rokver}/doc/rc.idispatchd.ini ${rokname}-${rokver}/doc/rc.sodispatchd.ini
mv ${rokname}-${rokver}/doc/rc.idispatchd.html ${rokname}-${rokver}/doc/rc.sodispatchd.html
mv ${rokname}-${rokver}/doc/rc.idispatchd.init ${rokname}-${rokver}/doc/rc.sodispatchd.init
mv ${rokname}-${rokver}/doc/rc.ithunder.spec ${rokname}-${rokver}/doc/rc.soworker.spec
perl -i -p -e "s/ithunder/soworker/g" ${rokname}-${rokver}/src/* ${rokname}-${rokver}/doc/* ${rokname}-${rokver}/configure.in
perl -i -p -e "s/ithink/sothink/g" ${rokname}-${rokver}/src/* ${rokname}-${rokver}/doc/* ${rokname}-${rokver}/configure.in
perl -i -p -e "s/ITHINK/SOTHINK/g" ${rokname}-${rokver}/src/* ${rokname}-${rokver}/doc/* ${rokname}-${rokver}/configure.in
perl -i -p -e "s/idispatch/sodispatch/g" ${rokname}-${rokver}/src/* ${rokname}-${rokver}/doc/* ${rokname}-${rokver}/configure.in
perl -i -p -e "s/IDISPATCH/SODISPATCH/g" ${rokname}-${rokver}/src/* ${rokname}-${rokver}/doc/* ${rokname}-${rokver}/configure.in
perl -i -p -e "s/libibase/libsobase/g" ${rokname}-${rokver}/src/* ${rokname}-${rokver}/doc/* ${rokname}-${rokver}/configure.in
perl -i -p -e "s/ibase/sobase/g" ${rokname}-${rokver}/src/* ${rokname}-${rokver}/doc/* ${rokname}-${rokver}/configure.in
perl -i -p -e "s/IBASE/SOBASE/g" ${rokname}-${rokver}/src/* ${rokname}-${rokver}/doc/* ${rokname}-${rokver}/configure.in
perl -i -p -e "s/IB_/SOB_/g" ${rokname}-${rokver}/src/* ${rokname}-${rokver}/doc/* ${rokname}-${rokver}/configure.in
perl -i -p -e "s/${rokver}/${roknewver}/g" ${rokname}-${rokver}/configure.in
perl -i -p -e "s/portlist\[/bkportlist\[/g" ${rokname}-${rokver}/doc/rc.sodispatchd.html
${rokname}-${rokver}/doc/base64.php ${rokname}-${rokver}/doc/rc.sodispatchd.html ${rokname}-${rokver}/src/base64dispatchdhtml.h 
cd ${rokname}-${rokver}; aclocal && autoheader && automake -a && autoconf && ./configure && make dist-gzip && mv ${roknew}-${roknewver}.tar.gz ../
cd ../ && rm -rf ${rokname}-${rokver}
#rename hibase to sodo 
hiver="0.4.21"
hinewver="1.4.21"
hiname='hibase'
hinew='sodo'
rm -rf ${hiname}-${hiver}
tar -zxf ${hiname}-${hiver}.tar.gz
#src
mv ${hiname}-${hiver}/src/hidoc.h ${hiname}-${hiver}/src/sodoc.h
mv ${hiname}-${hiver}/src/hidoc.c ${hiname}-${hiver}/src/sodoc.c
mv ${hiname}-${hiver}/src/hidocd.c ${hiname}-${hiver}/src/sodocd.c
mv ${hiname}-${hiver}/src/hindexd.c ${hiname}-${hiver}/src/soindexd.c
mv ${hiname}-${hiver}/src/himasterd.c ${hiname}-${hiver}/src/somasterd.c
#doc
mv ${hiname}-${hiver}/doc/hibase.spec ${hiname}-${hiver}/doc/sodo.spec
mv ${hiname}-${hiver}/doc/rc.hidocd ${hiname}-${hiver}/doc/rc.sodocd
mv ${hiname}-${hiver}/doc/rc.hidocd.ini ${hiname}-${hiver}/doc/rc.sodocd.ini
mv ${hiname}-${hiver}/doc/rc.hidocd.html ${hiname}-${hiver}/doc/rc.sodocd.html
mv ${hiname}-${hiver}/doc/rc.hindexd ${hiname}-${hiver}/doc/rc.soindexd
mv ${hiname}-${hiver}/doc/rc.hindexd.ini ${hiname}-${hiver}/doc/rc.soindexd.ini
mv ${hiname}-${hiver}/doc/rc.hindexd.html ${hiname}-${hiver}/doc/rc.soindexd.html
mv ${hiname}-${hiver}/doc/rc.himasterd ${hiname}-${hiver}/doc/rc.somasterd
mv ${hiname}-${hiver}/doc/rc.himasterd.ini ${hiname}-${hiver}/doc/rc.somasterd.ini
mv ${hiname}-${hiver}/doc/rc.himasterd.html ${hiname}-${hiver}/doc/rc.somasterd.html
mv ${hiname}-${hiver}/doc/hiservice.sh ${hiname}-${hiver}/doc/soservice.sh
#replace
perl -i -p -e "s/HI_/SO_/g" ${hiname}-${hiver}/doc/* ${hiname}-${hiver}/src/* ${hiname}-${hiver}/configure.in
perl -i -p -e "s/IB_/SOB_/g" ${hiname}-${hiver}/doc/* ${hiname}-${hiver}/src/* ${hiname}-${hiver}/configure.in
perl -i -p -e "s/HIBASE/SODO/g" ${hiname}-${hiver}/doc/* ${hiname}-${hiver}/src/* ${hiname}-${hiver}/configure.in
perl -i -p -e "s/IBASE/SOBASE/g" ${hiname}-${hiver}/doc/* ${hiname}-${hiver}/src/* ${hiname}-${hiver}/configure.in
perl -i -p -e "s/HIDOC/SODOC/g" ${hiname}-${hiver}/doc/* ${hiname}-${hiver}/src/* ${hiname}-${hiver}/configure.in
perl -i -p -e "s/HINDEX/SOINDEX/g" ${hiname}-${hiver}/doc/* ${hiname}-${hiver}/src/* ${hiname}-${hiver}/configure.in
perl -i -p -e "s/HIMASTER/SOMASTER/g" ${hiname}-${hiver}/doc/* ${hiname}-${hiver}/src/* ${hiname}-${hiver}/configure.in
perl -i -p -e "s/HIQDOC/SOQDOC/g" ${hiname}-${hiver}/doc/* ${hiname}-${hiver}/src/* ${hiname}-${hiver}/configure.in
perl -i -p -e "s/HIQPARSER/SOQPARSER/g" ${hiname}-${hiver}/doc/* ${hiname}-${hiver}/src/* ${hiname}-${hiver}/configure.in
perl -i -p -e "s/hiservice/soservice/g" ${hiname}-${hiver}/doc/* 
perl -i -p -e "s/hibase/sodo/g" ${hiname}-${hiver}/doc/* ${hiname}-${hiver}/src/* ${hiname}-${hiver}/configure.in 
perl -i -p -e "s/ibase/sobase/g" ${hiname}-${hiver}/doc/* ${hiname}-${hiver}/src/* ${hiname}-${hiver}/configure.in
perl -i -p -e "s/hidoc/sodoc/g" ${hiname}-${hiver}/doc/* ${hiname}-${hiver}/src/* ${hiname}-${hiver}/configure.in
perl -i -p -e "s/hindex/soindex/g" ${hiname}-${hiver}/doc/* ${hiname}-${hiver}/src/* ${hiname}-${hiver}/configure.in
perl -i -p -e "s/himaster/somaster/g" ${hiname}-${hiver}/doc/* ${hiname}-${hiver}/src/* ${hiname}-${hiver}/configure.in
perl -i -p -e "s/hiqdoc/soqdoc/g" ${hiname}-${hiver}/doc/* ${hiname}-${hiver}/src/* ${hiname}-${hiver}/configure.in
perl -i -p -e "s/hiqparser/soqparser/g" ${hiname}-${hiver}/doc/* ${hiname}-${hiver}/src/* ${hiname}-${hiver}/configure.in
perl -i -p -e "s/service_port = 2510/service_port = 2102/g" ${hiname}-${hiver}/doc/*.ini ${hiname}-${hiver}/doc/*.spec ${hiname}-${hiver}/doc/*.sh
perl -i -p -e "s/service_port = 3515/service_port = 3103/g" ${hiname}-${hiver}/doc/*.ini ${hiname}-${hiver}/doc/*.spec ${hiname}-${hiver}/doc/*.sh
perl -i -p -e "s/service_port = 4520/service_port = 4104/g" ${hiname}-${hiver}/doc/*.ini ${hiname}-${hiver}/doc/*.spec ${hiname}-${hiver}/doc/*.sh 
perl -i -p -e "s/service_port = 2612/service_port = 2204/g" ${hiname}-${hiver}/doc/*.ini ${hiname}-${hiver}/doc/*.sh
perl -i -p -e "s/service_port = 3618/service_port = 3206/g" ${hiname}-${hiver}/doc/*.ini ${hiname}-${hiver}/doc/*.sh
perl -i -p -e "s/service_port = 4624/service_port = 4208/g" ${hiname}-${hiver}/doc/*.ini ${hiname}-${hiver}/doc/*.sh
perl -i -p -e "s/service_port = 2714/service_port = 2306/g" ${hiname}-${hiver}/doc/*.ini ${hiname}-${hiver}/doc/*.spec ${hiname}-${hiver}/doc/*.sh
perl -i -p -e "s/service_port = 3721/service_port = 3309/g" ${hiname}-${hiver}/doc/*.ini ${hiname}-${hiver}/doc/*.spec ${hiname}-${hiver}/doc/*.sh
perl -i -p -e "s/service_port = 4728/service_port = 4312/g" ${hiname}-${hiver}/doc/*.ini ${hiname}-${hiver}/doc/*.spec ${hiname}-${hiver}/doc/*.sh
perl -i -p -e "s/service_port = 2816/service_port = 2408/g" ${hiname}-${hiver}/doc/*.ini ${hiname}-${hiver}/doc/*.spec ${hiname}-${hiver}/doc/*.sh
perl -i -p -e "s/service_port = 3824/service_port = 3412/g" ${hiname}-${hiver}/doc/*.ini ${hiname}-${hiver}/doc/*.spec ${hiname}-${hiver}/doc/*.sh
perl -i -p -e "s/service_port = 4832/service_port = 4416/g" ${hiname}-${hiver}/doc/*.ini ${hiname}-${hiver}/doc/*.spec ${hiname}-${hiver}/doc/*.sh
perl -i -p -e "s/service_port = 2918/service_port = 2000/g" ${hiname}-${hiver}/doc/*.ini ${hiname}-${hiver}/doc/*.sh
perl -i -p -e "s|\"4832\", \"4520\", \"4728\"|\"4416\", \"4104\", \"4312\"|g" ${hiname}-${hiver}/doc/rc.sodocd.html ${hiname}-${hiver}/doc/*.sh  
perl -i -p -e "s|\"0\", \"3515\", \"3824\", \"\", \"3721\"|\"0\", \"3103\", \"3412\", \"\", \"3309\"|g" ${hiname}-${hiver}/doc/rc.somasterd.html ${hiname}-${hiver}/doc/*.sh 
${hiname}-${hiver}/doc/base64.php ${hiname}-${hiver}/doc/rc.sodocd.html ${hiname}-${hiver}/src/base64docdhtml.h 
${hiname}-${hiver}/doc/base64.php ${hiname}-${hiver}/doc/rc.somasterd.html ${hiname}-${hiver}/src/base64masterdhtml.h 
perl -i -p -e "s/${hiver}/${hinewver}/g" ${hiname}-${hiver}/configure.in ${hiname}-${hiver}/doc/*.spec
perl -i -p -e "s/${ibver}/${ibnewver}/g" ${hiname}-${hiver}/doc/*.spec
cd ${hiname}-${hiver}; aclocal && autoheader && automake -a && autoconf && ./configure && make dist-gzip && mv ${hinew}-${hinewver}.tar.gz ../ && cd ../ && rm -rf ${hiname}-${hiver} 
