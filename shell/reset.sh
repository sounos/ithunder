#!/bin/sh
service hichunkd stop;
#rm -rf /data/tmp/hichunk;
#rm -rf /tmp/hichunk
#rm -rf /db*/.db
/bin/sed -e '88,88s/0/1/g' /etc/hitrackerd.ini > hitrackerd.ini ;
/bin/mv -f hitrackerd.ini /etc/hitrackerd.ini
/bin/sed -e '88,88s/0/1/g' -e '232,232s/0/2/g' /etc/hichunkd.ini > hichunkd.ini
/bin/mv -f hichunkd.ini /etc/hichunkd.ini
/sbin/chkconfig --level 3 hichunkd on;
/sbin/service hichunkd start;
while true;
do
    if [ "`/bin/netstat -anp|grep LISTEN|grep 2480`" ]; then
        echo "starting set dbase and multicast";
        x=0;
        for i in 1 2 3 4 5 6 ;
        do
            /usr/bin/curl -o "req.txt" -d "op=1&path=/db${i}/.db/mm&port=240${i}&limit=0&mode=0" "http://127.0.0.1:2480/q";
            diskid=`expr $i - 1`;
            for m in 0 1 2 3;
                do
                    mask="234.8.8.$x";
                    /usr/bin/curl -o "req.txt" -d "op=6&diskid=$diskid&mask=$mask" "http://127.0.0.1:2480/q";
                    ((x++));
                done
        done;
        break;
    fi;
    sleep 1;
done;
