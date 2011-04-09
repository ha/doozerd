include ../Make.inc

VERSION ?= $(shell git describe | sed s/^v// | tr - +)

TARG=doozer
GOFILES=\
	doozer.go\
	liveness.go\
	version.go\

include $(GOROOT)/src/Make.pkg

.PHONY: version.go
version.go:
	printf 'package doozer\nconst Version = `%s`\n' '$(VERSION)' > $@
