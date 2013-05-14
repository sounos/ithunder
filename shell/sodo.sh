#!/bin/bash
arch=`uname -p`
mkdir -p build; pushd build;rm -rf debuginfo/ rpms/ test/ /usr/src/redhat/SRPMS/* /usr/src/redhat/RPMS/${arch}/* /usr/src/redhat/BUILD/* ;popd;
rpm -Uvh srpms/*;
basedir="`pwd`";
pushd /usr/src/redhat/SPECS;
rpm -e sodo libsobase --nodeps; rm -f /etc/so*.ini*
rpm -e libscws --nodeps
rpm -e libsbase libevbase --nodeps;
rm -rf /usr/src/redhat/BUILD/* /usr/src/redhat/SRPMS/* /usr/src/redhat/RPMS/x86_64/* ${basedir}/debuginfo/* ${basedir}/test/*  ${basedir}/src/* ${basedir}/rpms/* ${basedir}/srpms/* /tmp/*;
buildrpm()
{
    name=$1;ver=$2;rel=$3;isinstall=$4;isdebug=$5;arch=`uname -p`;
    perl -i -p -e "s/^Version: .*/Version: ${ver}/" ${name}.spec \
    && perl -i -p -e "s/^Release: .*%/Release: ${rel}%/" ${name}.spec \
    && rpmbuild -ba ${name}.spec 
    [ "$?" != 0 ] && exit
    if [ "$isinstall" == "yes" ];then 
        rpm -Uvh /usr/src/redhat/RPMS/${arch}/${name}-${ver}-${rel}.${arch}.rpm --force 
        [ "$?" != 0 ] && exit
    fi
    if [ "$isdebug" ]; 
    then
        cp ${name}.spec  ${name}_4DB.spec \
        &&  perl -i -p -e "s/^%configure/%configure CFLAGS=\"-O0 -g\" CPPFLAGS=\"-O0 -g\" CXXFLAGS=\"-O0 -g\"/" ${name}_4DB.spec \
        &&  perl -i -p -e "s/^Release: .*%/Release: 4DB${rel}%/" ${name}_4DB.spec \
        && rpmbuild -ba ${name}_4DB.spec && rm -f ${name}_4DB.spec
        [ "$?" != 0 ] && exit
    fi
}
#libevbase
buildrpm libevbase 1.0.2 1 yes
#libsbase
buildrpm libsbase 1.0.4 5 yes
#libscws
buildrpm libscws 1.1.8 1 yes
#libsobase
buildrpm libsobase 1.5.21 4 yes
#sodo
buildrpm sodo 1.4.21 3 yes
popd
perl -i -p -e "s@/tmp@/index/tmp@g" /etc/sodocd.ini;
service sodocd restart
rm -rf /tmp/*
#tarball
mkdir -p ${basedir}/{debuginfo,test,srpms,rpms,src}
cp -f /usr/src/redhat/SOURCES/*.tar.gz ${basedir}/src/ 
mv -f /usr/src/redhat/SRPMS/* ${basedir}/srpms/ 
mv -f /usr/src/redhat/RPMS/x86_64/*debuginfo* ${basedir}/debuginfo/
#mv -f /usr/src/redhat/RPMS/x86_64/*debug* ${basedir}/test/
mv -f /usr/src/redhat/RPMS/x86_64/* ${basedir}/rpms/
pushd ${basedir}/;
mkdir -p backup sodo; datetime=`date +%Y%m%d%H%M%S`;tar -zcf backup/rpms-${datetime}.tar.gz rpms/*;tar -zcf backup/srpms-${datetime}.tar.gz srpms/libevbase* srpms/* sodo.sh
popd;
