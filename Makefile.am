SUBDIRS = src python

DIST_SUBDIRS = src python

INTLTOOL = \
	intltool-extract.in \
	intltool-merge.in \
	intltool-update.in

# EXTRA_DIST = install-sh $(INTLTOOL) README.markdown scripts debian msi LICENSE.txt
EXTRA_DIST = install-sh $(INTLTOOL) scripts msi debian dmg

ACLOCAL_AMFLAGS = -I m4

dist-hook:
	git log --format='%H' -1 > $(distdir)/latest_commit
