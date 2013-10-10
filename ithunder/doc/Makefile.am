#automake 
EXTRA_DIST = rc.ithinkd.ini rc.ithinkd.html rc.ithinkd.init base64.php \
rc.idispatchd.ini rc.idispatchd.html rc.idispatchd.init ithunder.spec 

install: install-ini

conftodir = "`echo $(sysconfdir)|sed -e 's|/usr/etc|/etc|'`"

initdir = "$(DESTDIR)$(conftodir)/init.d"

sysconftodir = "$(DESTDIR)$(conftodir)"

htmldir = "`echo @prefix@"/var/html"|sed -e 's|/usr/var|/var|g'`"
htmltodir = "$(DESTDIR)$(htmldir)"

install-ini:
	@echo install *.html to $(htmldir)
	@test -d $(htmltodir)/think || mkdir -p $(htmltodir)/think
	@cp -p rc.ithinkd.html $(htmltodir)/think/index.html
	@test -d $(htmltodir)/dispatch || mkdir -p $(htmltodir)/dispatch
	@cp -p rc.idispatchd.html $(htmltodir)/dispatch/index.html
	@echo install *.ini to $(sysconftodir)
	@test -d $(sysconftodir) || mkdir -p $(sysconftodir)
	@test -f $(sysconftodir)/ithinkd.ini \
		|| cp -p rc.ithinkd.ini $(sysconftodir)/ithinkd.ini
	@test -f $(sysconftodir)/idispatchd.ini \
		|| cp -p rc.idispatchd.ini $(sysconftodir)/idispatchd.ini
	@perl -i -p -e "s|\"/var/html|\"$(htmldir)|g" \
		$(sysconftodir)/ithinkd.ini \
		$(sysconftodir)/idispatchd.ini
	@echo install *.init to $(initdir)
	@test -d $(initdir) || mkdir -p $(initdir)
	@test -f $(initdir)/ithinkd \
		|| cp -p rc.ithinkd.init $(initdir)/ithinkd
	@test -f $(initdir)/idispatchd \
		|| cp -p rc.idispatchd.init $(initdir)/idispatchd
	@perl -i -p -e "s@/usr/local/sbin@$(prefix)/sbin@g" $(initdir)/ithinkd \
		$(initdir)/idispatchd  
	@perl -i -p -e "s@/usr/local/etc@$(conftodir)@g" $(initdir)/ithinkd \
		$(initdir)/idispatchd 
