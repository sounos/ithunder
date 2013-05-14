# Authority: SounOS.org

Summary: Instant and Incremental Full-Text Search Engine
Name: libibase
Version: 0.3.8
Release: 1%{?dist}
License: BSD
Group: System Environment/Libraries
URL: http://code.google.com/p/libibase/

Source: http://code.google.com/p/libibase/download/%{name}-%{version}.tar.gz
Packager: SounOS <SounOS@gmail.com>
Vendor: SounOS
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

%description
Libibase is a library for Search Engine which is Instant and Incremental. 

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
	/sbin/ldconfig

%postun 
	/sbin/ldconfig

%files
%defattr(-, root, root, 0755)
%{_includedir}/*
%{_libdir}/*

%changelog
* Thu Apr 22 2010 11:38:18 CST SounOS <SounOS@gmail.com>
- updated idb.c added memory index
* Tue Mar 09 2010 14:48:22 CST SounOS <SounOS@gmail.com>
- added ibase_set_phrase_status();
- added ibase_set_index_status();
