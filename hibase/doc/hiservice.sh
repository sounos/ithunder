#!/bin/bash
genservice()
{
    name="$1";baseport=$2
    doc="d$name" index="i$name" parser="q$name" qdoc="s$name" master="m$name"
    docd="${doc}d";indexd="${index}d";parserd="${parser}d";qdocd="${qdoc}d";masterd="${master}d";
    pushd /usr/sbin/;
    ln -sf hidocd $docd
    ln -sf hindexd $indexd
    ln -sf hiqparserd $parserd
    ln -sf hiqdocd $qdocd
    ln -sf himasterd $masterd
    popd;
    /bin/cp -f /etc/init.d/hidocd /etc/init.d/$docd
    /bin/cp -f /etc/init.d/hindexd /etc/init.d/$indexd
    /bin/cp -f /etc/init.d/hiqparserd /etc/init.d/$parserd
    /bin/cp -f /etc/init.d/hiqdocd /etc/init.d/$qdocd
    /bin/cp -f /etc/init.d/himasterd /etc/init.d/$masterd
    docd_httpd_port=`expr $baseport + 10`
    masterd_http_port=`expr $baseport + 20`
    indexd_http_port=`expr $baseport + 30`
    indexd_query_port=`expr $baseport + 31`
    indexd_index_port=`expr $baseport + 32`
    parserd_http_port=`expr $baseport + 40`
    parserd_query_port=`expr $baseport + 41`
    parserd_index_port=`expr $baseport + 42`
    qdocd_http_port=`expr $baseport + 50`
    qdocd_query_port=`expr $baseport + 51`
    qdocd_index_port=`expr $baseport + 52`
    /bin/cp -f /etc/hidocd.ini /etc/"${docd}.ini"
    sportlist='"4832", "4520", "4728"'
    mportlist='"0", "3515", "3824", "", "3721"'
#doc
    perl -i -p -e "s|service_port = 2918|service_port = $docd_httpd_port|g" /etc/"${docd}.ini" 
    perl -i -p -e "s/is_inside_html = 1/is_inside_html = 0/g" /etc/"${docd}.ini"
    mkdir -p "/var/html/$doc"; cp -f /var/html/hidoc/index.html /var/html/$doc/
    perl -i -p -e "s|$sportlist|\"$qdocd_index_port\", \"$parserd_index_port\", \"$indexd_index_port\"|g"  /var/html/$doc/index.html
#master
    /bin/cp -f /etc/himasterd.ini /etc/"${masterd}.ini"
    perl -i -p -e "s|service_port = 2612|service_port = $masterd_http_port|g" /etc/"${masterd}.ini"
    perl -i -p -e "s/is_inside_html = 1/is_inside_html = 0/g" /etc/"${masterd}.ini"
    mkdir -p "/var/html/$master"; 
    [ -e /var/html/$master/index.html ] || cp -f /var/html/himaster/index.html /var/html/$master/
    perl -i -p -e "s|$mportlist|\"0\", \"$parserd_query_port\", \"$qdocd_query_port\", \"\", \"$indexd_query_port\"|g"  /var/html/$master/index.html
#index
    mkdir -p "/var/html/$index"; 
    [ -e /var/html/$index/index.html ] || cp -f /var/html/hindex/index.html /var/html/$index/
    /bin/cp -f /etc/hindexd.ini /etc/"${indexd}.ini"
    perl -i -p -e "s|service_port = 2714|service_port = $indexd_http_port|g" /etc/"${indexd}.ini"
    perl -i -p -e "s|service_port = 3721|service_port = $indexd_query_port|g" /etc/"${indexd}.ini"
    perl -i -p -e "s|service_port = 4728|service_port = $indexd_index_port|g" /etc/"${indexd}.ini"
    perl -i -p -e "s/is_inside_html = 1/is_inside_html = 0/g" /etc/"${indexd}.ini"
#parser
    /bin/cp -f /etc/hiqparserd.ini /etc/"${parserd}.ini"
    perl -i -p -e "s|service_port = 2510|service_port = $parserd_http_port|g" /etc/"${parserd}.ini"
    perl -i -p -e "s|service_port = 3515|service_port = $parserd_query_port|g" /etc/"${parserd}.ini"
    perl -i -p -e "s|service_port = 4520|service_port = $parserd_index_port|g" /etc/"${parserd}.ini"
#qdoc
    /bin/cp -f /etc/hiqdocd.ini /etc/"${qdocd}.ini"
    perl -i -p -e "s|service_port = 2816|service_port = $qdocd_http_port|g" /etc/"${qdocd}.ini"
    perl -i -p -e "s|service_port = 3824|service_port = $qdocd_query_port|g" /etc/"${qdocd}.ini"
    perl -i -p -e "s|service_port = 4832|service_port = $qdocd_index_port|g" /etc/"${qdocd}.ini"
    perl -i -p -e "s/hidoc/$doc/g" /etc/init.d/$docd /etc/"${docd}.ini"
    perl -i -p -e "s/HIDOC/`echo $doc|tr [a-z] [A-Z]`/g" /etc/init.d/$docd
    perl -i -p -e "s/hindex/$index/g" /etc/init.d/$indexd /etc/"${indexd}.ini"
    perl -i -p -e "s/HINDEX/`echo $index|tr [a-z] [A-Z]`/g" /etc/init.d/$indexd
    perl -i -p -e "s/hiqparser/$parser/g" /etc/init.d/$parserd /etc/"${parserd}.ini"
    perl -i -p -e "s/HIQPARSER/`echo $parser|tr [a-z] [A-Z]`/g" /etc/init.d/$parserd
    perl -i -p -e "s/hiqdoc/$qdoc/g" /etc/init.d/$qdocd /etc/"${qdocd}.ini"
    perl -i -p -e "s/HIQDOC/`echo $qdoc|tr [a-z] [A-Z]`/g" /etc/init.d/$qdocd
    perl -i -p -e "s/himaster/$master/g" /etc/init.d/$masterd /etc/"${masterd}.ini"
    perl -i -p -e "s/HIMASTER/`echo $master|tr [a-z] [A-Z]`/g" /etc/init.d/$masterd

}
if [ $# -lt 2 ];then

    echo "Usage:$0 service_name baseport"
else
    genservice  $1 $2
fi
