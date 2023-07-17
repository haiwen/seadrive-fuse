
AM_CFLAGS = -DPKGDATADIR=\"$(pkgdatadir)\" \
	-DPACKAGE_DATA_DIR=\""$(pkgdatadir)"\" \
	@GLIB2_CFLAGS@ \
	@MSVC_CFLAGS@ \
	@CURL_CFLAGS@ \
	@BPWRAPPER_CFLAGS@ \
	@FUSE_CFLAGS@ \
	@SEARPC_CFLAGS@ \
	-Wall

if WIN32
AM_CFLAGS += -I/usr/local/include
endif

bin_PROGRAMS = seadrive

noinst_HEADERS = \
	http-tx-mgr.h \
	sync-mgr.h \
	seafile-session.h \
	rpc-service.h \
	fuse-ops.h \
	repo-tree.h \
	journal-mgr.h \
	change-set.h \
	fs-mgr.h \
	repo-mgr.h \
	commit-mgr.h \
	branch-mgr.h \
	filelock-mgr.h \
	obj-store.h \
	block-mgr.h \
	block.h \
	block-backend.h \
	seafile-crypt.h \
	diff-simple.h \
	seafile-config.h \
	seafile-error.h \
	log.h \
	curl-init.h \
	cdc.h \
	rabin-checksum.h \
	db.h \
	job-mgr.h \
	timer.h \
	utils.h \
	file-cache-mgr.h \
	sync-status-tree.h \
	mq-mgr.h \
	notif-mgr.h \
	common.h \
	obj-backend.h \
	searpc-signature.h \
	searpc-marshal.h

if COMPILE_WS
ws_src = notif-mgr.c
endif

seadrive_SOURCES = \
	seadrive.c \
	http-tx-mgr.c \
	sync-mgr.c \
	seafile-session.c \
	rpc-service.c \
	fuse-ops.c \
	repo-tree.c \
	journal-mgr.c \
	change-set.c \
	fs-mgr.c \
	repo-mgr.c \
	commit-mgr.c \
	branch-mgr.c \
	filelock-mgr.c \
	obj-store.c \
	obj-backend-fs.c \
	block-mgr.c \
	block-backend.c \
	block-backend-fs.c \
	seafile-crypt.c \
	diff-simple.c \
	seafile-config.c \
	log.c \
	curl-init.c \
	cdc.c \
	rabin-checksum.c \
	db.c \
	job-mgr.c \
	timer.c \
	utils.c \
	file-cache-mgr.c \
	sync-status-tree.c \
	mq-mgr.c \
	$(ws_src)

if WIN32
seadrive_LDADD = @LIB_INTL@ @GLIB2_LIBS@ @GOBJECT_LIBS@ \
	@SSL_LIBS@ @LIB_RT@ @LIB_UUID@ -lsqlite3 @LIBEVENT_LIBS@ \
	@LIB_WS32@ @LIB_CRYPT32@ \
	@JANSSON_LIBS@ @LIB_MAC@ @ZLIB_LIBS@ @CURL_LIBS@ @BPWRAPPER_LIBS@ \
	@FUSE_LIBS@ @SEARPC_LIBS@ @WS_LIBS@
else
seadrive_LDADD = @LIB_INTL@ @GLIB2_LIBS@ @GOBJECT_LIBS@ \
	@SSL_LIBS@ @LIB_RT@ @LIB_UUID@ -lsqlite3 @LIBEVENT_LIBS@ \
	@LIB_WS32@ @LIB_CRYPT32@ \
	@JANSSON_LIBS@ @LIB_MAC@ @ZLIB_LIBS@ @CURL_LIBS@ @BPWRAPPER_LIBS@ \
	@FUSE_LIBS@ @SEARPC_LIBS@ @WS_LIBS@
endif

seadrive_LDFLAGS = @CONSOLE@
