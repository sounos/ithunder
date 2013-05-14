# Authority: SounOS.org

Summary: High speed/performence Search Engine Base Tools
Name: hibase
Version: 0.4.19
Release: 66%{?dist}
License: BSD
Group: System Environment/Libraries
URL: http://code.google.com/p/libibase/

Source: http://code.google.com/p/libibase/download/%{name}-%{version}.tar.gz
Packager: SounOS <SounOS@gmail.com>
Vendor: SounOS
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

BuildRequires: libevbase >= 1.0.0 libsbase >= 1.0.0 libibase >= 0.5.14
Requires: libevbase >= 1.0.0 libsbase >= 1.0.0 libibase >= 0.5.14

%description
Hibase is some tools set for search engine. 

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

    /sbin/chkconfig --level 345 hindexd  off
    /sbin/chkconfig --level 345 hidocd off
    /sbin/chkconfig --level 345 hiqdocd off
    /sbin/chkconfig --level 345 himasterd off
    /sbin/chkconfig --level 345 hiqparserd off

%preun 
    [ "`pstree|grep himasterd|wc -l`" -gt "0" ] && /sbin/service himasterd stop
    [ "`pstree|grep hindexd|wc -l`" -gt "0" ] && /sbin/service hindexd stop
    [ "`pstree|grep hidocd|wc -l`" -gt "0" ]  && /sbin/service hidocd stop
    [ "`pstree|grep hiqdocd|wc -l`" -gt "0" ] && /sbin/service hiqdocd stop
    [ "`pstree|grep hiqparserd|wc -l`" -gt "0" ] && /sbin/service hiqparserd stop
    /sbin/chkconfig --del hindexd
    /sbin/chkconfig --del hidocd
    /sbin/chkconfig --del hiqdocd
    /sbin/chkconfig --del himasterd
    /sbin/chkconfig --del hiqparserd

%files
%defattr(-, root, root, 0755)
%{_sbindir}/*
%{_localstatedir}/*
%{_sysconfdir}/rc.d/*
%config(noreplace) %{_sysconfdir}/*.ini

%changelog
* Thu Apr 22 2010 11:37:25 CST SounOS <sounos@gmail.com>
- fixed hidoc_set_basedir()::iqueue_push(taskid)
* Tue Mar 09 2010 15:02:39 CST SounOS <sounos@gmail.com>
- added hidoc_set_ccompress_status();
- added hidoc_set_phrase_status();
- added hidoc_add_task();
- added hidoc_del_task();
- added hidoc_pop_task();
- added hidoc_push_task();
- added hidoc_over_task();
