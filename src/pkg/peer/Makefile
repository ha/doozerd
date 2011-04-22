include ../../Make.inc

TARG=doozer/peer
GOFILES=\
	peer.go\
	liveness.go\
	version.go\

include $(GOROOT)/src/Make.pkg

VERS ?= $(shell ./version.sh)
GVERS := $(shell sed 's/[^`]*`//' <version.go | sed 's/`.*//')
version.go:
	printf 'package peer;const Version = `%s`\n' '$(VERS)' > $@

ifneq ($(VERS),$(GVERS))
.PHONY: version.go
endif

CLEANFILES+=version.go
