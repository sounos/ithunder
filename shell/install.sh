#!/bin/bash
rpm -e libsbase  --nodeps;
rpm -e libevbase  --nodeps;
rpm -e libdbase  --nodeps;
rpm -e hidbase  --nodeps;
cd /data/build;
rm -f rpms/*;
tar -zxf rpms.tar.gz && rm -f rpms/*debuginfo* \
&& rpm -Uvh rpms/libevbase* rpms/libsbase-* rpms/hidbase-* rpms/libdbase-* \
&& ./reset.sh
