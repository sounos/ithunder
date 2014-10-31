#automake 
EXTRA_DIST = rc.hindexd.ini rc.hidocd.ini rc.himasterd.ini rc.dict.txt \
			 rc.hidocd rc.hindexd rc.himasterd base64.php  hibase.spec \
			 rc.hindexd.html rc.hidocd.html rc.himasterd.html hiservice.sh \
			 hidocd.init hindexd.init himasterd.init sinit


install: install-ini

conftodir = "`echo $(sysconfdir)|sed -e 's|/usr/etc|/etc|'`"

initdir = "$(DESTDIR)$(conftodir)/init.d"
rcdir = "$(conftodir)/rc.d/init.d"

sysconftodir = "$(DESTDIR)$(conftodir)"

sbintodir = $(DESTDIR)$(sbindir)

htmldir = "`echo @prefix@"/var/html"|sed -e 's|/usr/var|/var|g'`"
htmltodir = $(DESTDIR)$(htmldir)
isubuntu = "`uname -v|grep Ubuntu`"

install-ini:
	@echo install *.html to $(htmldir)
	@test -d $(htmltodir)/hidoc || mkdir -p $(htmltodir)/hidoc
	@cp -p rc.hidocd.html $(htmltodir)/hidoc/index.html
	@test -d $(htmltodir)/hindex || mkdir -p $(htmltodir)/hindex
	@cp -p rc.hindexd.html $(htmltodir)/hindex/index.html
	@test -d $(htmltodir)/himaster || mkdir -p $(htmltodir)/himaster
	@cp -p rc.himasterd.html $(htmltodir)/himaster/index.html
	@echo install genservice.sh to $(sbintodir)
	@test -d $(sbintodir) || mkdir -p $(sbintodir)
	@cp -p hiservice.sh $(sbintodir)
	@cp -p sinit $(sbintodir)
	@chmod 755 $(sbintodir)/hiservice.sh $(sbintodir)/sinit
	@echo "link $(sbintodir)/hindexd to $(sbintodir)/hiqdocd|hiqparserd"
	@ln -sf $(sbindir)/hindexd $(sbintodir)/hiqdocd
	@ln -sf $(sbindir)/hindexd $(sbintodir)/hiqparserd
	@echo install hindexd.ini hidocd.ini to $(sysconftodir)
	@test -d $(sysconftodir) || mkdir -p $(sysconftodir)
	@test -d $(initdir) || mkdir -p $(initdir)
	@test -f $(sysconftodir)/hindexd.ini \
		|| cp -p rc.hindexd.ini $(sysconftodir)/hindexd.ini
	@test -f $(sysconftodir)/hiqparserd.ini \
		|| cp -p rc.hindexd.ini $(sysconftodir)/hiqparserd.ini
	@perl -i -p -e  "s/hindex/hiqparser/g" $(sysconftodir)/hiqparserd.ini
	@perl -i -p -e  "s/HINDEX/HIQPARSER/g" $(sysconftodir)/hiqparserd.ini
	@perl -i -p -e "s/used_for = 0/used_for = 2/g" $(sysconftodir)/hiqparserd.ini
	@perl -i -p -e "s/index_status = 0/index_status = 1/g" $(sysconftodir)/hiqparserd.ini
	@perl -i -p -e "s/phrase_status = 0/phrase_status = 1/g" $(sysconftodir)/hiqparserd.ini
	@perl -i -p -e "s/service_port = 2714/service_port = 2510/g" $(sysconftodir)/hiqparserd.ini
	@perl -i -p -e "s/service_port = 3721/service_port = 3515/g" $(sysconftodir)/hiqparserd.ini
	@perl -i -p -e "s/service_port = 4728/service_port = 4520/g" $(sysconftodir)/hiqparserd.ini
	@test -f $(sysconftodir)/hiqdocd.ini \
		|| cp -p rc.hindexd.ini $(sysconftodir)/hiqdocd.ini
	@perl -i -p -e  "s/hindex/hiqdoc/g" $(sysconftodir)/hiqdocd.ini
	@perl -i -p -e  "s/HINDEX/HIQDOC/g" $(sysconftodir)/hiqdocd.ini
	@perl -i -p -e "s/used_for = 0/used_for = 1/g" $(sysconftodir)/hiqdocd.ini
	@perl -i -p -e "s/index_status = 0/index_status = 1/g" $(sysconftodir)/hiqdocd.ini
	@perl -i -p -e "s/phrase_status = 0/phrase_status = 1/g" $(sysconftodir)/hiqdocd.ini
	@perl -i -p -e "s/service_port = 2714/service_port = 2816/g" $(sysconftodir)/hiqdocd.ini
	@perl -i -p -e "s/service_port = 3721/service_port = 3824/g" $(sysconftodir)/hiqdocd.ini
	@perl -i -p -e "s/service_port = 4728/service_port = 4832/g" $(sysconftodir)/hiqdocd.ini
	@test -f $(sysconftodir)/hidocd.ini \
		|| cp -p rc.hidocd.ini $(sysconftodir)/hidocd.ini
	@test -f $(sysconftodir)/himasterd.ini \
		|| cp -p rc.himasterd.ini $(sysconftodir)/himasterd.ini
	@perl -i -p -e "s|\"/var/html|\"$(htmldir)|g" \
		$(sysconftodir)/hidocd.ini \
		$(sysconftodir)/hindexd.ini \
		$(sysconftodir)/himasterd.ini \
		$(sysconftodir)/hiqparserd.ini \
		$(sysconftodir)/hiqdocd.ini
	@echo "install init scripts"
	@if [ $(isubuntu) != "" ]; \
	then \
		test -f $(initdir)/hindexd \
			|| cp -p hindexd.init $(initdir)/hindexd;\
		test -f $(initdir)/hidocd \
			|| cp -p hidocd.init $(initdir)/hidocd;\
		test -f $(initdir)/himasterd \
			|| cp -p himasterd.init $(initdir)/himasterd;\
		test -f $(initdir)/hiqparserd 	\
			|| cp -p hindexd.init $(initdir)/hiqparserd;\
		test -f $(initdir)/hiqdocd \
			|| cp -p hindexd.init $(initdir)/hiqdocd ;\
	else \
		test -f $(initdir)/hindexd \
			|| cp -p rc.hindexd $(initdir)/hindexd;\
		test -f $(initdir)/hidocd \
			|| cp -p rc.hidocd $(initdir)/hidocd;\
		test -f $(initdir)/himasterd \
			|| cp -p rc.himasterd $(initdir)/himasterd;\
		test -f $(initdir)/hiqparserd 	\
			|| cp -p rc.hindexd $(initdir)/hiqparserd;\
		test -f $(initdir)/hiqdocd \
			|| cp -p rc.hindexd $(initdir)/hiqdocd;\
	fi
	@perl -i -p -e  "s/hindex/hiqparser/g" $(initdir)/hiqparserd
	@perl -i -p -e  "s/HINDEX/HIQPARSER/g" $(initdir)/hiqparserd
	@perl -i -p -e  "s/hindex/hiqdoc/g" $(initdir)/hiqdocd
	@perl -i -p -e  "s/HINDEX/HIQDOC/g" $(initdir)/hiqdocd
	@perl -i -p -e "s@/usr/local/sbin@$(prefix)/sbin@g" $(initdir)/hindexd \
		 $(initdir)/hidocd $(initdir)/himasterd $(initdir)/hiqdocd $(initdir)/hiqparserd
	@perl -i -p -e "s@/usr/local/etc@$(conftodir)@g" $(initdir)/hindexd \
		 $(initdir)/hidocd $(initdir)/himasterd $(initdir)/hiqdocd $(initdir)/hiqparserd
