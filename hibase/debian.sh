#!/bin/bash
rm -rf debian
pkg=`grep -E "^PACKAGE_NAME=" configure |sed -e "s/PACKAGE_NAME='\(.*\)'/\1/"`
ver=`grep -E "^PACKAGE_VERSION=" configure |sed -e "s/PACKAGE_VERSION='\(.*\)'/\1/"`
dh_make -n -s -c bsd -e SounOS@gmail.com -p "${pkg}_${ver}"
cp control debian/control
sed -i "s/^Package:.*/Package: $pkg/" debian/control
sed -i "s/^Source:.*/Source: $pkg/" debian/control
sed -i "s/^License:.*/License: $pkg/" debian/control
sed -i "s/^Standards-Version:.*/Standards-Version: $ver/" debian/control
sed -i "s/root <SounOS@gmail.com>/SounOS <SounOS@gmail.com>/" debian/*
dpkg-buildpackage -rfakeroot -eSounOS -mSounOS
