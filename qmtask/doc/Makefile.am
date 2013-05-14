#automake 
EXTRA_DIST = rc.qtaskd.ini rc.qtaskd.html rc.qtaskd.init base64.php qmtask.spec

install: install-ini

conftodir = "`echo $(sysconfdir)|sed -e 's|/usr/etc|/etc|'`"

initdir = "$(DESTDIR)$(conftodir)/rc.d/init.d"

sysconftodir = "$(DESTDIR)$(conftodir)"

htmldir = "`echo @prefix@"/var/html"|sed -e 's|/usr/var|/var|g'`"
htmltodir = "$(DESTDIR)$(htmldir)"

install-ini:
	@echo install *.html to $(htmldir)
	@test -d $(htmltodir)/qtask || mkdir -p $(htmltodir)/qtask
	@cp -p rc.qtaskd.html $(htmltodir)/qtask/index.html
	@echo install qtaskd.ini to $(sysconftodir)
	@test -d $(sysconftodir) || mkdir -p $(sysconftodir)
	@test -f $(sysconftodir)/qtaskd.ini \
		|| cp -p rc.qtaskd.ini $(sysconftodir)/qtaskd.ini
	@perl -i -p -e "s|\"/var/html|\"$(htmldir)|g" $(sysconftodir)/qtaskd.ini
	@echo install qtaskd.init to $(initdir)
	@test -d $(initdir) || mkdir -p $(initdir)
	@test -f $(initdir)/qtaskd \
		|| cp -p rc.qtaskd.init $(initdir)/qtaskd
	@perl -i -p -e "s@/usr/local/sbin@$(prefix)/sbin@g" $(initdir)/qtaskd 
	@perl -i -p -e "s@/usr/local/etc@$(conftodir)@g" $(initdir)/qtaskd 


