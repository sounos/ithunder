# Authority: SounOS.org

Summary: internet Information Spider
Name: ispider
Version: 0.0.2
Release: 7%{?dist}
License: BSD
Group: System Environment/Libraries
URL: http://code.google.com/p/libibase/

Source: http://code.google.com/p/libibase/download/%{name}-%{version}.tar.gz
Packager: SounOS <SounOS@gmail.com>
Vendor: SounOS
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

BuildRequires: libevbase >= 1.0.0 libsbase >= 1.0.0 libibase >= 0.5.15 libchardet >= 0.0.1
Requires: libevbase >= 1.0.0 libsbase >= 1.0.0 libibase >= 0.5.15 libchardet >= 0.0.1

%description
Internet Infomation Spider

%prep
%setup

%build
%configure
%{__make}

%install
%{__rm} -rf %{buildroot}
%{__make} install DESTDIR="%{buildroot}"

%clean
%{__rm} -rf %{buildroot}

%post 

    /sbin/chkconfig --level 345 imonitord  off
    /sbin/chkconfig --level 345 ispider  off
    /sbin/chkconfig --level 345 ipager  off
    /sbin/chkconfig --level 345 ilister  off
    /sbin/chkconfig --level 345 ifiler  off
    /sbin/chkconfig --level 345 iupdater  off
    /sbin/chkconfig --level 345 irepeator  off
    /sbin/chkconfig --level 345 iextractor  off

%preun 
    [ "`pstree|grep imonitord|wc -l`" -gt "0" ] && /sbin/service imonitord stop
    [ "`pstree|grep ispider|wc -l`" -gt "0" ] && /sbin/service ispider stop
    [ "`pstree|grep ilister|wc -l`" -gt "0" ] && /sbin/service ilister stop
    [ "`pstree|grep ipager|wc -l`" -gt "0" ] && /sbin/service ipager stop
    [ "`pstree|grep ifiler|wc -l`" -gt "0" ] && /sbin/service ifiler stop
    [ "`pstree|grep iupdater|wc -l`" -gt "0" ] && /sbin/service iupdater stop
    [ "`pstree|grep irepeator|wc -l`" -gt "0" ] && /sbin/service irepeator stop
    [ "`pstree|grep iextractor|wc -l`" -gt "0" ] && /sbin/service iextractor stop
    /sbin/chkconfig --del imonitord
    /sbin/chkconfig --del ispider
    /sbin/chkconfig --del ipager
    /sbin/chkconfig --del ilister
    /sbin/chkconfig --del ifiler
    /sbin/chkconfig --del iupdater
    /sbin/chkconfig --del irepeator
    /sbin/chkconfig --del iextractor

%files
%defattr(-, root, root, 0755)
%{_bindir}/*
%{_sbindir}/*
%{_localstatedir}/*
%{_sysconfdir}/init.d/*
%config(noreplace) %{_sysconfdir}/*.ini

%changelog
* Wed Jun  8 2011 16:53:13 CST SounOS <sounos@gmail.com>
- added kvmap.* zvbcode.*
- added rename packet name to iworker
