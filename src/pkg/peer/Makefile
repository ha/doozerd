include ../../Make.inc

VERSION ?= $(shell git describe | sed s/^v// | tr - +)

TARG=doozer/peer
GOFILES=\
	peer.go\
	liveness.go\
	version.go\

include $(GOROOT)/src/Make.pkg

.PHONY: version.go
version.go:
	printf 'package peer\n\nconst Version = `%s`\n' '$(VERSION)' > $@
