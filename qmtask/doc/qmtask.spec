# Authority: SounOS.org
Summary: Queue Monitor for Tasks used for distrubuted computing
Name: qmtask
Version: 0.0.5
Release: 39%{?dist}
License: BSD
Group: System Environment/Libraries
URL: http://code.google.com/p/libibase/

Source: http://code.google.com/p/libibase/download/%{name}-%{version}.tar.gz
Packager: SounOS <SounOS@gmail.com>
Vendor: SounOS
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
BuildRequires: libevbase >= 1.0.0 libsbase >= 1.0.0
Requires: libevbase >= 1.0.0 libsbase >= 1.0.0

%description
Queue Monitor for Tasks used for distrubuted computing.

%package -n libmtask
Group: Development/Libraries
Summary: Development tools for the qmtask server.

%description -n libmtask
The hidbase-devel package contains the API files libmtask and mtask.h

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

    /sbin/chkconfig --add qtaskd 

%preun 
    [ "`pstree|grep qtaskd|wc -l`" -gt "0" ] && /sbin/service qtaskd stop
    /sbin/chkconfig --del qtaskd

%files -n libmtask
%defattr(-, root, root)
%{_libdir}/*
%{_includedir}/*

%files
%defattr(-, root, root, 0755)
%{_sbindir}/*
%{_bindir}/*
%{_localstatedir}/*
%{_sysconfdir}/rc.d/*
%config(noreplace) %{_sysconfdir}/*.ini

%changelog
* Thu Jun  2 2011 11:27:49 CST SounOS <sounos@gmail.com>
- qmtask-0.0.3.tar.gz (qmtask　0.0.3 分布式计算任务调度) file uploaded by sounos@gmail.com
- r1902 ( fixed mmtree.c::mmtree_insert()/mmtree_insert()::mmtree...) committed by sounos.@gmail.com
- fixed mmtree.c::mmtree_insert()/mmtree_insert()::mmtree->state->roots[rootid].total;
- r1901 ( updated qtask.c added XTASK{total,over}; ) committed by sounos@gmail.com
- updated qtask.c added XTASK{total,over};
- r1900 ( updated qtaskd_request_handler(); ) committed by sounos@gmail.com
- updated qtaskd_request_handler();
