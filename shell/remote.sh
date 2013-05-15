#!/bin/bash
packet=$1
#scp $packet root@s11:/data/software/rpms.tar.gz
#scp reinstall.sh root@s4:/data/software/
#ssh root@s11 "nohup /data/software/reinstall.sh &"
scp $packet root@worker1:/data/build/rpms.tar.gz
scp install.sh root@worker1:/data/build/
scp reset.sh root@worker1:/data/build/
ssh root@worker1  "nohup /data/build/install.sh &"

scp $packet root@worker2:/data/build/rpms.tar.gz
scp install.sh root@worker2:/data/build/
scp reset.sh root@worker2:/data/build/
ssh root@worker2  "nohup /data/build/install.sh &"

scp $packet root@worker3:/data/build/rpms.tar.gz
scp install.sh root@worker3:/data/build/
scp reset.sh root@worker3:/data/build/
ssh root@worker3  "nohup /data/build/install.sh &"
