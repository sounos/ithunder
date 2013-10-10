#automake 
EXTRA_DIST = base64.php ispider.spec rc.imonitord.ini rc.imonitord.html \
rc.imonitord.init rc.ispider.ini rc.ispider.init rc.iextractor.ini rc.iextractor.init

install: install-ini

conftodir = "`echo $(sysconfdir)|sed -e 's|/usr/etc|/etc|'`"

initdir = "$(DESTDIR)$(conftodir)/init.d"

sysconftodir = "$(DESTDIR)$(conftodir)"

bintodir = "$(DESTDIR)/$(bindir)"

htmldir = "`echo @prefix@"/var/html"|sed -e 's|/usr/var|/var|g'`"
htmltodir = "$(DESTDIR)$(htmldir)"

install-ini:
	@echo install imonitord.html to $(htmltodir)
	@test -d $(htmltodir)/monitor || mkdir -p $(htmltodir)/monitor
	@cp -p rc.imonitord.html $(htmltodir)/monitor/index.html
	@echo "link $(bindir)/ispider to 'ilister,ipager,iupdater,irepeator'"
	@ln -sf $(bindir)/ispider $(bintodir)/ilister
	@ln -sf $(bindir)/ispider $(bintodir)/ipager
	@ln -sf $(bindir)/ispider $(bintodir)/ifiler
	@ln -sf $(bindir)/ispider $(bintodir)/iupdater
	@ln -sf $(bindir)/ispider $(bintodir)/irepeator
	@echo install *.ini to $(sysconftodir)
	@test -d $(sysconftodir) || mkdir -p $(sysconftodir)
	@test -f $(sysconftodir)/imonitord.ini \
		|| cp -p rc.imonitord.ini $(sysconftodir)/imonitord.ini
	@perl -i -p -e "s|\"/var/html|\"$(htmldir)|g" \
		$(sysconftodir)/imonitord.ini
	@test -f $(sysconftodir)/ispider.ini \
		|| cp -p rc.ispider.ini $(sysconftodir)/ispider.ini
	@test -f $(sysconftodir)/ipager.ini \
		|| cp -p rc.ispider.ini $(sysconftodir)/ipager.ini
	@test -f $(sysconftodir)/ilister.ini \
		|| cp -p rc.ispider.ini $(sysconftodir)/ilister.ini
	@test -f $(sysconftodir)/ifiler.ini \
		|| cp -p rc.ispider.ini $(sysconftodir)/ifiler.ini
	@test -f $(sysconftodir)/iupdater.ini \
		|| cp -p rc.ispider.ini $(sysconftodir)/iupdater.ini
	@test -f $(sysconftodir)/irepeator.ini \
		|| cp -p rc.ispider.ini $(sysconftodir)/irepeator.ini
	@test -f $(sysconftodir)/iextractor.ini \
		|| cp -p rc.iextractor.ini $(sysconftodir)/iextractor.ini
	@echo install *.init to $(initdir)
	@test -d $(initdir) || mkdir -p $(initdir)
	@test -f $(initdir)/imonitord \
		|| cp -p rc.imonitord.init $(initdir)/imonitord
	@test -f $(initdir)/ispider \
		|| cp -p rc.ispider.init $(initdir)/ispider
	@test -f $(initdir)/ipager \
		|| cp -p rc.ispider.init $(initdir)/ipager
	@test -f $(initdir)/ilister \
		|| cp -p rc.ispider.init $(initdir)/ilister
	@test -f $(initdir)/ifiler \
		|| cp -p rc.ispider.init $(initdir)/ifiler
	@test -f $(initdir)/iupdater \
		|| cp -p rc.ispider.init $(initdir)/iupdater
	@test -f $(initdir)/irepeator \
		|| cp -p rc.ispider.init $(initdir)/irepeator
	@test -f $(initdir)/iextractor \
		|| cp -p rc.iextractor.init $(initdir)/iextractor
	@perl -i -p -e "s@spider@pager@g" $(sysconftodir)/ipager.ini $(initdir)/ipager 
	@perl -i -p -e "s@task_type = 0@task_type = 0@g" $(sysconftodir)/ipager.ini
	@perl -i -p -e "s@spider@lister@g" $(sysconftodir)/ilister.ini $(initdir)/ilister
	@perl -i -p -e "s@task_type = 0@task_type = 1@g" $(sysconftodir)/ilister.ini
	@perl -i -p -e "s@spider@filer@g" $(sysconftodir)/ifiler.ini $(initdir)/ifiler
	@perl -i -p -e "s@task_type = 0@task_type = 2@g" $(sysconftodir)/ifiler.ini
	@perl -i -p -e "s@spider@updater@g" $(sysconftodir)/iupdater.ini $(initdir)/iupdater
	@perl -i -p -e "s@task_type = 0@task_type = 4@g" $(sysconftodir)/iupdater.ini
	@perl -i -p -e "s@spider@repeator@g" $(sysconftodir)/irepeator.ini $(initdir)/irepeator
	@perl -i -p -e "s@task_type = 0@task_type = 8@g" $(sysconftodir)/irepeator.ini
	@perl -i -p -e "s@/usr/local/sbin@$(prefix)/sbin@g" $(initdir)/imonitord 
	@perl -i -p -e "s@/usr/local/bin@$(prefix)/bin@g" $(initdir)/ispider \
		$(initdir)/iextractor  $(initdir)/ipager \
		$(initdir)/ilister $(initdir)/ifiler \
		$(initdir)/iupdater $(initdir)/irepeator
	@perl -i -p -e "s@/usr/local/etc@$(conftodir)@g" $(initdir)/imonitord \
		$(initdir)/ispider $(initdir)/iextractor \
		$(initdir)/ipager $(initdir)/ilister $(initdir)/ifiler \
		$(initdir)/iupdater $(initdir)/irepeator
