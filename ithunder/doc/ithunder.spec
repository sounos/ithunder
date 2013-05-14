# Authority: SounOS.org

Summary: IndexWorker based-on qmtask & libibase
Name: ithunder
Version: 0.0.4
Release: 6%{?dist}
License: BSD
Group: System Environment/Libraries
URL: http://code.google.com/p/libibase/

Source: http://code.google.com/p/libibase/download/%{name}-%{version}.tar.gz
Packager: SounOS <SounOS@gmail.com>
Vendor: SounOS
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

BuildRequires: libevbase >= 1.0.0 libsbase >= 1.0.0 libibase >= 0.5.15 libscws >= 1.1.1 libdbase >= 0.0.3 libmtask >= 0.0.5
Requires: libevbase >= 1.0.0 libsbase >= 1.0.0 libibase >= 0.5.15 libscws >= 1.1.1 libdbase >= 0.0.3  libmtask >= 0.0.5

%description
IndexWorker based-on qmtask & libibase

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

    /sbin/chkconfig --level 345 ithinkd  off
    /sbin/chkconfig --level 345 idispatchd  off

%preun 
    [ "`pstree|grep ithinkd|wc -l`" -gt "0" ] && /sbin/service ithinkd stop
    [ "`pstree|grep idispatchd|wc -l`" -gt "0" ] && /sbin/service idispatchd stop
    /sbin/chkconfig --del ithinkd
    /sbin/chkconfig --del idispatchd

%files
%defattr(-, root, root, 0755)
%{_bindir}/*
%{_sbindir}/*
%{_localstatedir}/*
%{_sysconfdir}/rc.d/*
%config(noreplace) %{_sysconfdir}/*.ini

%changelog
* Wed Jun  8 2011 16:53:13 CST SounOS <sounos@gmail.com>
- added kvmap.* zvbcode.*
- added rename packet name to iworker
