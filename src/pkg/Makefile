include $(GOROOT)/src/Make.inc

TARG=doozer
GOFILES=\
	doozer.go\
	version.go\

.PHONY: version.go
version.go:
	printf 'package doozer\nconst Version = `%s`\n' '$(VERSION)' > $@

include $(GOROOT)/src/Make.pkg
